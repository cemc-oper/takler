"""Requirement-level boundary tests for the Checkpoint_Manager.

Task 8.8 of the *m1-operational-baseline* spec asks for one file that guards the
four checkpoint boundary behaviours an operator notices first, each from the
angle of the requirement rather than of the method that implements it:

* a snapshot write that takes longer than the configured period (5.10),
* a snapshot file without a ``format_version`` (6.15),
* a first start with no snapshot file on disk (6.9),
* a clean shutdown writing the last snapshot (5.9).

The mechanics of the same behaviours are pinned next door -- ``start`` / ``stop``
/ ``_snapshot_loop`` in ``test_checkpoint_periodic_unit.py`` and
``_load_snapshot`` / ``restore`` in ``test_checkpoint_restore_unit.py``. What
this file adds is the operator-visible contract on top of them: the overrun
WARNING really reports the measured duration and does not trigger a catch-up
write, a versionless snapshot restores to exactly what the earliest supported
version restores to, an absent snapshot is an INFO and not an error, and the
snapshot ``stop`` leaves behind is one a fresh process can actually restore.

Periods are set by assigning ``manager.interval`` after construction:
``_resolve_interval`` legitimately refuses anything below 10 seconds, which no
test can wait for.

Log assertions go through a captured console sink rather than ``caplog``: the
logging backend does not route records into pytest's handler, so the sink has to
be configured inside the redirection block (same approach as
``test_checkpoint_write_unit.py``).

Validates: Requirements 5.9, 5.10, 6.9, 6.15
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import re
from pathlib import Path

import takler.logging
from takler.core.bunch import Bunch
from takler.core.flow import Flow
from takler.core.state import NodeStatus
from takler.server.checkpoint import (
    CHECKPOINT_FORMAT_VERSION,
    EARLIEST_SUPPORTED_FORMAT_VERSION,
    CheckpointManager,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bunch(host: str = "login01", port: str = "33083") -> Bunch:
    """A bunch with one flow and one task, enough to be recognizable."""
    bunch = Bunch(host=host, port=port)
    flow = Flow(name="flow1")
    flow.add_task("task1")
    bunch.add_flow(flow)
    return bunch


def _make_manager(tmp_path: Path, interval: float) -> CheckpointManager:
    manager = CheckpointManager(
        bunch=_make_bunch(), checkpoint_file=tmp_path / "takler.check"
    )
    manager.interval = interval
    return manager


def _restoring_manager(tmp_path: Path) -> CheckpointManager:
    """A manager over an empty bunch, i.e. a freshly started server process.

    Deliberately started on the same host and port the snapshots were written
    with: the address checks of requirements 6.18 - 6.21 are pinned in their own
    file, and a mismatch here would only add noise to the log assertions below.
    """
    return CheckpointManager(
        bunch=Bunch(host="login01", port="33083"),
        checkpoint_file=tmp_path / "takler.check",
    )


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


def _lines(captured: str, level: str) -> list:
    return [line for line in captured.splitlines() if level in line]


def _write_snapshot(tmp_path: Path, name: str) -> Path:
    """Write one real snapshot of :func:`_make_bunch` and return its path."""
    path = tmp_path / name
    writer = CheckpointManager(bunch=_make_bunch(), checkpoint_file=path)
    assert writer.write_checkpoint() is True
    return path


# ---------------------------------------------------------------------------
# A write slower than the period (Requirement 5.10)
# ---------------------------------------------------------------------------

def test_a_write_slower_than_the_period_is_reported_with_duration_and_period(
        tmp_path,
):
    """Requirement 5.10: the WARNING carries the measured cost and the period.

    The operator's question is "how far behind is the snapshot?", so the number
    in the message has to be the duration that was actually measured, not the
    period it exceeded.
    """
    manager = _make_manager(tmp_path, interval=0.05)
    overrun = manager.interval * 3
    writes = []
    original = manager.write_checkpoint_async

    async def slow_write():
        result = await original()
        await asyncio.sleep(overrun)
        writes.append(1)
        return result

    manager.write_checkpoint_async = slow_write

    async def main():
        await manager.start()
        # Wait for two slow periods to have happened rather than for a fixed
        # duration, so the assertions do not depend on machine speed.
        while len(writes) < 2:
            await asyncio.sleep(manager.interval)
        await manager.stop()

    _, captured = _capturing_stderr(lambda: asyncio.run(main()))

    warnings = [line for line in _lines(captured, "WARNING") if "took" in line]
    assert warnings
    for line in warnings:
        assert str(manager.checkpoint_file) in line
        assert "interval of 0.05 seconds" in line
        measured = re.search(r"took ([0-9.]+) seconds", line)
        assert measured is not None
        assert float(measured.group(1)) >= overrun
    # A period the manager could not keep up with produces a report, never an
    # extra write to catch up: at most one warning per write.
    assert len(warnings) <= len(writes)
    assert "ERROR" not in captured


def test_a_slow_write_does_not_stop_the_snapshots(tmp_path):
    """Requirement 5.10 is a report, not a shutdown: the file keeps moving."""
    manager = _make_manager(tmp_path, interval=0.05)
    writes = []
    original = manager.write_checkpoint_async

    async def slow_write():
        result = await original()
        await asyncio.sleep(manager.interval * 3)
        writes.append(1)
        return result

    manager.write_checkpoint_async = slow_write

    async def main():
        await manager.start()
        while len(writes) < 2:
            await asyncio.sleep(manager.interval)
        assert manager._snapshot_task is not None
        assert not manager._snapshot_task.done()
        await manager.stop()

    asyncio.run(main())

    assert len(writes) >= 2
    assert json.loads(manager.checkpoint_file.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A snapshot without a format version (Requirement 6.15)
# ---------------------------------------------------------------------------

def test_a_snapshot_without_a_format_version_is_read_as_the_earliest_version(
        tmp_path,
):
    """Requirement 6.15: the missing field means "the oldest format we read"."""
    path = _write_snapshot(tmp_path, "takler.check")
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    del snapshot["format_version"]
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    manager = _restoring_manager(tmp_path)

    result, captured = _capturing_stderr(manager.restore)

    assert result is True
    assert list(manager.bunch.flows) == ["flow1"]
    assert "ERROR" not in captured
    # The version the file was read as is named, so the operator can tell this
    # apart from a snapshot that really carried version 1.
    version_info = [
        line for line in _lines(captured, "INFO") if "format version" in line
    ]
    assert len(version_info) == 1
    assert str(path) in version_info[0]
    assert str(EARLIEST_SUPPORTED_FORMAT_VERSION) in version_info[0]


def test_a_versionless_snapshot_restores_exactly_as_the_earliest_version_does(
        tmp_path,
):
    """"按最早支持版本处理" means the same restore, not merely a restore."""
    source = _write_snapshot(tmp_path, "takler.check")
    snapshot = json.loads(source.read_text(encoding="utf-8"))

    without_version = dict(snapshot)
    del without_version["format_version"]
    earliest = dict(snapshot)
    earliest["format_version"] = EARLIEST_SUPPORTED_FORMAT_VERSION

    restored = []
    for payload in (without_version, earliest):
        source.write_text(json.dumps(payload), encoding="utf-8")
        manager = _restoring_manager(tmp_path)
        assert manager.restore() is True
        restored.append(
            [flow.to_dict() for _, flow in manager.bunch.flows.items()]
        )

    assert restored[0] == restored[1]


def test_the_earliest_supported_version_is_not_above_what_is_written(tmp_path):
    """A versionless snapshot can only be usable while this holds."""
    assert EARLIEST_SUPPORTED_FORMAT_VERSION <= CHECKPOINT_FORMAT_VERSION


# ---------------------------------------------------------------------------
# No snapshot file on disk (Requirement 6.9)
# ---------------------------------------------------------------------------

def test_a_first_start_without_any_snapshot_is_an_info_and_an_empty_bunch(
        tmp_path,
):
    """Requirement 6.9: a first start is normal, so nothing may be an ERROR."""
    manager = _restoring_manager(tmp_path)
    assert not manager.checkpoint_file.exists()
    assert not manager.backup_file.exists()

    result, captured = _capturing_stderr(manager.restore)

    assert result is False
    assert manager.bunch.flows == {}
    assert "ERROR" not in captured
    assert "WARNING" not in captured
    empty_bunch = [
        line for line in _lines(captured, "INFO") if "empty bunch" in line
    ]
    assert len(empty_bunch) == 1
    assert str(manager.checkpoint_file) in empty_bunch[0]
    # Every path that was looked for is named, so the operator can check
    # whether the server is reading the location they configured.
    looked_for = [
        line
        for line in _lines(captured, "INFO")
        if "does not exist" in line
    ]
    assert len(looked_for) == 2
    assert str(manager.checkpoint_file) in looked_for[0]
    assert str(manager.backup_file) in looked_for[1]


def test_the_startup_flow_continues_after_an_empty_start(tmp_path):
    """The empty bunch is a working starting point, not a dead one."""
    manager = _restoring_manager(tmp_path)

    manager.restore()
    manager.bunch.add_flow(Flow(name="flow1"))

    assert manager.write_checkpoint() is True
    snapshot = json.loads(manager.checkpoint_file.read_text(encoding="utf-8"))
    assert [f["name"] for f in snapshot["bunch"]["flows"]] == ["flow1"]


def test_an_absent_checkpoint_file_still_restores_from_the_backup(tmp_path):
    """The absent file is skipped with an INFO, not treated as a failure."""
    path = _write_snapshot(tmp_path, "takler.check")
    manager = _restoring_manager(tmp_path)
    path.replace(manager.backup_file)

    result, captured = _capturing_stderr(manager.restore)

    assert result is True
    assert list(manager.bunch.flows) == ["flow1"]
    assert "ERROR" not in captured
    missing = [line for line in _lines(captured, "INFO") if "does not exist" in line]
    assert len(missing) == 1
    assert str(manager.checkpoint_file) in missing[0]


# ---------------------------------------------------------------------------
# The clean shutdown snapshot (Requirement 5.9)
# ---------------------------------------------------------------------------

def test_stop_leaves_a_snapshot_of_the_state_it_shut_down_with(tmp_path):
    """Requirement 5.9 through the public ``stop`` contract.

    The period is long enough that no periodic write can happen, so the file on
    disk after ``stop`` can only be the shutdown snapshot -- and it has to be a
    snapshot a fresh process can restore, otherwise "wrote a snapshot" buys the
    operator nothing.
    """
    manager = _make_manager(tmp_path, interval=5.0)

    async def main():
        await manager.start()
        # State the server accumulated after the periodic task was created.
        flow1 = manager.bunch.find_flow("flow1")
        flow1.begin()
        manager.bunch.find_node("/flow1/task1").run()
        assert not manager.checkpoint_file.exists()
        await manager.stop()
        # Written before ``stop`` returned, not scheduled for later.
        return manager.checkpoint_file.is_file()

    written_when_stop_returned, captured = _capturing_stderr(
        lambda: asyncio.run(main())
    )

    assert written_when_stop_returned is True
    assert "ERROR" not in captured

    reloaded = _restoring_manager(tmp_path)
    assert reloaded.restore() is True
    flow1 = reloaded.bunch.find_flow("flow1")
    assert flow1.begun is True
    task1 = reloaded.bunch.find_node("/flow1/task1")
    assert task1.state.node_status == NodeStatus.submitted
    assert task1.try_no == 1


def test_stop_writes_the_shutdown_snapshot_even_with_nothing_to_cancel(tmp_path):
    """A server shut down before its first period still leaves a snapshot."""
    manager = _make_manager(tmp_path, interval=5.0)

    _, captured = _capturing_stderr(lambda: asyncio.run(manager.stop()))

    assert manager.checkpoint_file.is_file()
    assert "ERROR" not in captured
    reloaded = _restoring_manager(tmp_path)
    assert reloaded.restore() is True
    assert list(reloaded.bunch.flows) == ["flow1"]
