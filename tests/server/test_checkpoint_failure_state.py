"""Checkpoint publication order and failure pause semantics."""

import asyncio
import errno
import json
import threading

import pytest
import typer

from takler.core import Bunch, HaltCause, NodeStatus
from takler.core.bunch import ServerState
from takler.protocol.commands import CompleteCommand, ServerResumeCommand
from takler.server import TaklerServer
from takler.server.checkpoint import CheckpointManager
from takler.server.cli import serve_forever
from takler.server.scheduler import Scheduler


def _manager(tmp_path):
    bunch = Bunch()
    flow = bunch.add_flow("flow")
    flow.add_task("task")
    flow.begin()
    return CheckpointManager(bunch, checkpoint_file=tmp_path / "checkpoint")


@pytest.mark.parametrize(
    ("error", "category"),
    [
        (OSError(errno.ENOSPC, "secret path or password"), "storage full"),
        (OSError(errno.EACCES, "secret path or password"), "permission denied"),
        (OSError(errno.EIO, "secret path or password"), "storage I/O error"),
    ],
)
def test_failed_write_pauses_and_reports_safe_reason(tmp_path, error, category):
    manager = _manager(tmp_path)
    assert manager.write_checkpoint()
    last_success = manager.last_checkpoint_at

    def fail(index, name):
        if name == "replace_checkpoint":
            raise error

    manager._after_write_step = fail
    assert manager.write_checkpoint() is False
    assert manager.bunch.server_state.status.value == "halted"
    assert manager.bunch.server_state.halt_causes == [HaltCause.checkpoint_failure]
    assert manager.last_checkpoint_at == last_success
    scheduler = Scheduler(manager.bunch)
    scheduler.checkpoint_manager = manager
    response = scheduler.server_status_response()
    assert response.message == f"checkpoint write failed: {category}"
    assert "secret" not in response.model_dump_json()
    assert scheduler.run_command_server_resume(ServerResumeCommand()).flag == 33

    manager._after_write_step = lambda index, name: None
    assert manager.write_checkpoint() is True
    assert manager.bunch.server_state.status.value == "running"
    assert manager.checkpoint_failure_reason is None
    assert manager.last_checkpoint_at is not None


def test_success_clears_only_failure_and_child_reports_while_halted(tmp_path):
    manager = _manager(tmp_path)
    manager.bunch.server_state = ServerState.from_causes(
        [HaltCause.manual, HaltCause.recovery]
    )
    task = manager.bunch.find_node("/flow/task")
    # The task was already submitted before the storage fault.
    manager.bunch.server_state = ServerState()
    task.run()
    manager.bunch.server_state = ServerState.from_causes(
        [HaltCause.manual, HaltCause.recovery]
    )

    manager._write_temp_file = lambda path, payload: (_ for _ in ()).throw(
        OSError(errno.ENOSPC, "secret")
    )
    assert manager.write_checkpoint() is False
    assert set(manager.bunch.server_state.halt_causes) == {
        HaltCause.manual,
        HaltCause.recovery,
        HaltCause.checkpoint_failure,
    }
    scheduler = Scheduler(manager.bunch)
    scheduler.run_command_complete(
        CompleteCommand(node_path="/flow/task", attempt_id=task.attempt_id)
    )
    assert task.state.node_status is NodeStatus.complete

    del manager._write_temp_file
    assert manager.write_checkpoint() is True
    assert manager.bunch.server_state.halt_causes == [
        HaltCause.manual,
        HaltCause.recovery,
    ]
    assert manager.bunch.server_state.status.value == "halted"
    saved = json.loads(manager.checkpoint_file.read_text())
    assert saved["runtime"]["server_state"]["halt_causes"] == [
        "manual",
        "recovery",
        "checkpoint_failure",
    ]


def test_newer_capture_cannot_be_overwritten_by_late_older_worker(tmp_path):
    manager = _manager(tmp_path)
    first_waiting = threading.Event()
    release_first = threading.Event()
    original_publish = manager._publish_captured

    def delayed_publish(sequence, payload):
        if sequence == 1:
            first_waiting.set()
            assert release_first.wait(5)
        return original_publish(sequence, payload)

    manager._publish_captured = delayed_publish

    async def run():
        older = asyncio.create_task(manager.write_checkpoint_async())
        assert await asyncio.to_thread(first_waiting.wait, 5)
        manager.bunch.add_flow("newer")
        newer = asyncio.create_task(manager.write_checkpoint_async())
        assert await asyncio.wait_for(newer, 5) is True
        release_first.set()
        assert await asyncio.wait_for(older, 5) is True

    asyncio.run(run())
    saved = json.loads(manager.checkpoint_file.read_text())
    assert {flow["name"] for flow in saved["definition"]["root"]["flows"]} == {
        "flow",
        "newer",
    }


def test_failed_newer_capture_does_not_publish_late_stale_view(tmp_path):
    manager = _manager(tmp_path)
    first_waiting = threading.Event()
    release_first = threading.Event()
    original_publish = manager._publish_captured

    def delayed_publish(sequence, payload):
        if sequence == 1:
            first_waiting.set()
            assert release_first.wait(5)
        return original_publish(sequence, payload)

    manager._publish_captured = delayed_publish
    manager._after_write_step = lambda index, name: (_ for _ in ()).throw(
        OSError(errno.ENOSPC, "full")
    )

    async def run():
        older = asyncio.create_task(manager.write_checkpoint_async())
        assert await asyncio.to_thread(first_waiting.wait, 5)
        manager.bunch.add_flow("newer")
        assert await manager.write_checkpoint_async() is False
        release_first.set()
        assert await older is False

    asyncio.run(run())
    assert not manager.checkpoint_file.exists()
    assert HaltCause.checkpoint_failure in manager.bunch.server_state.halt_causes


def test_final_write_failure_is_reported_to_server_cli(tmp_path, monkeypatch):
    server = TaklerServer(checkpoint_file=tmp_path / "checkpoint")
    manager = server.checkpoint_manager
    manager._write_temp_file = lambda path, payload: (_ for _ in ()).throw(
        OSError(errno.ENOSPC, "full")
    )
    assert asyncio.run(manager.stop()) is False
    assert HaltCause.checkpoint_failure in server.bunch.server_state.halt_causes

    async def stopped_server(instance):
        instance.shutdown_checkpoint_ok = False

    monkeypatch.setattr("takler.server.cli._serve", stopped_server)
    with pytest.raises(typer.Exit) as exc:
        serve_forever(server)
    assert exc.value.exit_code == 1


def test_periodic_write_retries_and_recovers_fault_pause(tmp_path):
    manager = _manager(tmp_path)
    manager.interval = 0.02
    failures = 0

    def fail_once(index, name):
        nonlocal failures
        if name == "write_temp_file" and failures == 0:
            failures += 1
            raise OSError(errno.ENOSPC, "full")

    manager._after_write_step = fail_once

    async def run():
        await manager.start()
        async with asyncio.timeout(3):
            while (
                HaltCause.checkpoint_failure
                not in manager.bunch.server_state.halt_causes
            ):
                await asyncio.sleep(0.001)
            assert manager.checkpoint_failure_reason == "storage full"
            while (
                HaltCause.checkpoint_failure in manager.bunch.server_state.halt_causes
            ):
                await asyncio.sleep(0.001)
        assert await manager.stop() is True

    asyncio.run(run())
    assert failures == 1
    assert manager.bunch.server_state.status.value == "running"
    assert manager.last_checkpoint_at is not None


def test_cancelled_async_caller_still_records_worker_failure(tmp_path):
    manager = _manager(tmp_path)
    started = threading.Event()
    release = threading.Event()
    original_publish = manager._publish_captured

    def delayed_publish(sequence, payload):
        started.set()
        assert release.wait(5)
        return original_publish(sequence, payload)

    manager._publish_captured = delayed_publish
    manager._write_temp_file = lambda path, payload: (_ for _ in ()).throw(
        OSError(errno.ENOSPC, "full")
    )

    async def run():
        caller = asyncio.create_task(manager.write_checkpoint_async())
        assert await asyncio.to_thread(started.wait, 5)
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        release.set()
        async with asyncio.timeout(3):
            while (
                HaltCause.checkpoint_failure
                not in manager.bunch.server_state.halt_causes
            ):
                await asyncio.sleep(0.001)

    asyncio.run(run())
    assert manager.checkpoint_failure_reason == "storage full"
