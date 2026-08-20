"""Unit tests for the Checkpoint_Manager periodic snapshot task.

Task 8.4 of the *m1-operational-baseline* spec adds ``start`` / ``stop`` /
``_snapshot_loop``. This file pins the three behaviours the requirements ask
for: one write per period with the loop surviving a single failure, the overrun
WARNING, and the "cancel then write the final snapshot" shutdown order.

The tests drive the coroutines with ``asyncio.run`` (this project has no
pytest-asyncio) and set ``manager.interval`` directly after construction so a
sub-second period can be used without going through ``_resolve_interval``,
which legitimately rejects anything below 10 seconds.

Log assertions go through a captured console sink rather than ``caplog``,
following ``test_checkpoint_write_unit.py``.

Validates: Requirements 5.1, 5.9, 5.10
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
from pathlib import Path

import takler.logging
from takler.core.bunch import Bunch
from takler.core.flow import Flow
from takler.server.checkpoint import TEMP_SUFFIX, CheckpointManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bunch() -> Bunch:
    bunch = Bunch(host="login01", port="33083")
    flow = Flow(name="flow1")
    flow.add_task("task1")
    bunch.add_flow(flow)
    return bunch


def _make_manager(
        tmp_path: Path,
        interval: float = 0.02,
) -> CheckpointManager:
    """A manager whose period is short enough for a test to observe."""
    manager = CheckpointManager(
        bunch=_make_bunch(), checkpoint_file=tmp_path / "takler.check"
    )
    manager.interval = interval
    return manager


def _capturing_stderr(func):
    """Run ``func`` while capturing the console log output."""
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="DEBUG", console=True)
            result = func()
    finally:
        takler.logging.configure(console=True)
    return result, buffer.getvalue()


# ---------------------------------------------------------------------------
# Periodic writing (Requirement 5.1)
# ---------------------------------------------------------------------------

def test_start_holds_the_periodic_task_and_writes_once_per_period(tmp_path):
    manager = _make_manager(tmp_path)
    writes = []
    original = manager.write_checkpoint_async

    async def counting_write():
        writes.append(1)
        return await original()

    manager.write_checkpoint_async = counting_write

    async def main():
        await manager.start()
        assert manager._snapshot_task is not None
        assert not manager._snapshot_task.done()
        # Wait for the periods to elapse rather than for a fixed duration, so
        # the assertion does not depend on how fast this machine writes.
        while len(writes) < 2:
            await asyncio.sleep(manager.interval)
        task = manager._snapshot_task
        await manager.stop()
        return task

    task = asyncio.run(main())

    assert len(writes) >= 2
    assert task.cancelled() or task.done()
    assert manager._snapshot_task is None
    assert json.loads(manager.checkpoint_file.read_text(encoding="utf-8"))


def test_start_twice_keeps_a_single_task(tmp_path):
    manager = _make_manager(tmp_path, interval=5.0)

    async def main():
        await manager.start()
        first = manager._snapshot_task
        await manager.start()
        second = manager._snapshot_task
        await manager.stop()
        return first, second

    (first, second), captured = _capturing_stderr(lambda: asyncio.run(main()))

    assert first is second
    assert "WARNING" in captured
    assert "already running" in captured


def test_loop_keeps_running_after_a_failing_write(tmp_path):
    """A single failure is an ERROR, not the end of all snapshots."""
    manager = _make_manager(tmp_path)
    calls = []
    original = manager.write_checkpoint_async

    async def flaky_write():
        calls.append(1)
        if len(calls) == 1:
            raise OSError("disk gone")
        return await original()

    manager.write_checkpoint_async = flaky_write

    async def main():
        await manager.start()
        while len(calls) < 3:
            await asyncio.sleep(manager.interval)
        await manager.stop()

    _, captured = _capturing_stderr(lambda: asyncio.run(main()))

    assert len(calls) >= 3
    errors = [line for line in captured.splitlines() if "ERROR" in line]
    assert len(errors) == 1
    assert str(manager.checkpoint_file) in errors[0]
    assert "disk gone" in errors[0]
    # The later periods really did produce a snapshot.
    assert json.loads(manager.checkpoint_file.read_text(encoding="utf-8"))


def test_cancelling_the_loop_task_is_not_swallowed(tmp_path):
    manager = _make_manager(tmp_path, interval=5.0)

    async def main():
        await manager.start()
        task = manager._snapshot_task
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return task

    task = asyncio.run(main())

    assert task.cancelled()


# ---------------------------------------------------------------------------
# Overrun warning (Requirement 5.10)
# ---------------------------------------------------------------------------

def test_write_slower_than_the_period_logs_a_warning_with_both_values(tmp_path):
    manager = _make_manager(tmp_path, interval=0.05)
    original = manager.write_checkpoint_async

    async def slow_write():
        result = await original()
        await asyncio.sleep(manager.interval * 3)
        return result

    manager.write_checkpoint_async = slow_write

    async def main():
        await manager.start()
        await asyncio.sleep(manager.interval * 5)
        await manager.stop()

    _, captured = _capturing_stderr(lambda: asyncio.run(main()))

    warnings = [line for line in captured.splitlines() if "WARNING" in line]
    assert warnings
    first = warnings[0]
    assert str(manager.checkpoint_file) in first
    # Both the measured duration and the configured period are reported.
    assert "0.05" in first
    assert "seconds, which is longer" in first


def test_write_faster_than_the_period_logs_no_warning(tmp_path):
    # A period comfortably above the cost of one write on any disk, so this
    # test cannot report an overrun that is really just slow IO.
    manager = _make_manager(tmp_path, interval=0.5)
    writes = []
    original = manager.write_checkpoint_async

    async def counting_write():
        writes.append(1)
        return await original()

    manager.write_checkpoint_async = counting_write

    async def main():
        await manager.start()
        while not writes:
            await asyncio.sleep(0.05)
        await manager.stop()

    _, captured = _capturing_stderr(lambda: asyncio.run(main()))

    assert "WARNING" not in captured


# ---------------------------------------------------------------------------
# Shutdown snapshot (Requirement 5.9)
# ---------------------------------------------------------------------------

def test_stop_writes_a_final_snapshot_after_cancelling_the_task(tmp_path):
    manager = _make_manager(tmp_path, interval=5.0)

    async def main():
        await manager.start()
        # Changed after the task was created and before any period elapsed, so
        # only the final snapshot can contain it.
        manager.bunch.add_flow(Flow(name="flow2"))
        await manager.stop()

    asyncio.run(main())

    snapshot = json.loads(manager.checkpoint_file.read_text(encoding="utf-8"))
    assert [f["name"] for f in snapshot["bunch"]["flows"]] == ["flow1", "flow2"]


def test_stop_cancels_before_it_writes(tmp_path):
    """Ordering matters: no periodic write may overlap the final one."""
    manager = _make_manager(tmp_path, interval=5.0)
    events = []
    original_write = manager.write_checkpoint

    def recording_write():
        events.append(("write", manager._snapshot_task))
        return original_write()

    manager.write_checkpoint = recording_write

    async def main():
        await manager.start()
        task = manager._snapshot_task
        await manager.stop()
        return task

    task = asyncio.run(main())

    assert [name for name, _ in events] == ["write"]
    # The task was already cancelled and released when the final write ran.
    assert events[0][1] is None
    assert task.cancelled()


def test_stop_drains_a_write_the_periodic_task_left_in_flight(tmp_path):
    """Cancelling the loop must not leave a write racing the final snapshot.

    ``write_checkpoint_async`` runs the file IO on a worker thread, which
    cancelling the periodic task does not stop. Both that write and the final
    one go through the same pid-suffixed temporary files, so if ``stop`` did not
    wait, one of them would lose its temporary file to the other's
    ``os.replace``.
    """
    manager = _make_manager(tmp_path, interval=0.01)
    events = []
    original_async = manager.write_checkpoint_async
    original_sync = manager.write_checkpoint

    async def slow_async_write():
        events.append("async-start")
        await asyncio.sleep(0.2)
        result = await original_async()
        events.append("async-end")
        return result

    def recording_sync_write():
        events.append("sync-write")
        return original_sync()

    manager.write_checkpoint_async = slow_async_write
    manager.write_checkpoint = recording_sync_write

    async def main():
        await manager.start()
        # Cancel exactly while a write is in flight.
        while "async-start" not in events:
            await asyncio.sleep(0.005)
        await manager.stop()

    _, captured = _capturing_stderr(lambda: asyncio.run(main()))

    assert events == ["async-start", "async-end", "sync-write"]
    assert "ERROR" not in captured
    assert [p.name for p in tmp_path.iterdir() if TEMP_SUFFIX in p.name] == []
    assert json.loads(manager.checkpoint_file.read_text(encoding="utf-8"))


def test_stop_without_start_still_writes_a_snapshot(tmp_path):
    manager = _make_manager(tmp_path, interval=5.0)

    asyncio.run(manager.stop())

    assert manager.checkpoint_file.is_file()


def test_stop_is_safe_to_call_twice(tmp_path):
    manager = _make_manager(tmp_path, interval=5.0)

    async def main():
        await manager.start()
        await manager.stop()
        await manager.stop()

    asyncio.run(main())

    assert manager._snapshot_task is None
    assert json.loads(manager.checkpoint_file.read_text(encoding="utf-8"))


def test_stop_reports_a_failed_final_snapshot_without_raising(tmp_path):
    manager = _make_manager(tmp_path, interval=5.0)

    def fail_after(index, name):
        if name == "write_temp_file":
            raise OSError("no space left")

    manager._after_write_step = fail_after

    _, captured = _capturing_stderr(lambda: asyncio.run(manager.stop()))

    assert "ERROR" in captured
    assert not manager.checkpoint_file.exists()
