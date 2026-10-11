"""R3-07: background checkpoint work consumes only a pinned state version."""

import asyncio
import errno
import json
import pickle
import stat
from dataclasses import replace
from io import StringIO

import pytest
from pydantic import Field

from takler.core import Bunch, Flow, HaltCause, NodeStatus, Task
from takler.schema.definition import DefinitionModel
from takler.serialization.checkpoint import restore_checkpoint
from takler.serialization.registry import (
    NodeRegistration,
    builtin_registry,
    using_registry,
)
from takler.serialization.version import CheckpointTransfer
from takler.server.checkpoint import CheckpointManager
from takler.server.scheduler import Scheduler


def _manager(tmp_path):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    flow.add_task(Task("t"))
    flow.begin()
    scheduler = Scheduler(bunch)
    manager = CheckpointManager(bunch, checkpoint_file=tmp_path / "checkpoint")
    manager.query_scheduler = scheduler
    scheduler.checkpoint_manager = manager
    return manager, scheduler, flow


def test_background_checkpoint_uses_fixed_version_and_restores(tmp_path, monkeypatch):
    manager, scheduler, flow = _manager(tmp_path)
    expected = scheduler.version_store.pin()
    entered = asyncio.Event()
    release = asyncio.Event()
    real_write = manager._write_fixed_background

    async def delayed(sequence, version, saved_at):
        entered.set()
        await release.wait()
        return await real_write(sequence, version, saved_at)

    manager._write_fixed_background = delayed
    monkeypatch.setattr(
        "takler.server.checkpoint.capture_checkpoint",
        lambda bunch: (_ for _ in ()).throw(AssertionError("read live tree")),
    )

    async def run():
        pending = asyncio.create_task(manager.write_checkpoint_async())
        await entered.wait()
        flow.add_task(Task("later"))
        assert "/f/later" in scheduler.version_store.pin().index
        release.set()
        assert await pending is True
        await manager._drain_in_flight_write()
        if manager._background_pool is not None:
            manager._background_pool.shutdown(wait=True)
            manager._background_pool = None

    asyncio.run(run())
    saved = json.loads(manager.checkpoint_file.read_text())
    assert saved["saved_at"] >= expected.as_of
    assert saved["saved_at"] == manager.last_checkpoint_at
    assert "/f/later" not in saved["runtime"]["nodes"]
    restored = restore_checkpoint(manager.checkpoint_file.read_bytes())
    assert restored.find_node("/f/t") is not None
    assert restored.find_node("/f/later") is None


def test_invalid_newer_fixed_view_blocks_late_older_publication(tmp_path):
    manager, scheduler, flow = _manager(tmp_path)
    waiting = asyncio.Event()
    release = asyncio.Event()
    real_write = manager._write_fixed_background

    async def delayed(sequence, version, saved_at):
        if sequence == 1:
            waiting.set()
            await release.wait()
        return await real_write(sequence, version, saved_at)

    manager._write_fixed_background = delayed

    async def run():
        older = asyncio.create_task(manager.write_checkpoint_async())
        await waiting.wait()
        task = manager.bunch.find_node("/f/t")
        task.state.node_status = NodeStatus.submitted
        task.notify_query_change("runtime")
        assert await manager.write_checkpoint_async() is False
        release.set()
        assert await older is False
        await manager._drain_in_flight_write()

    asyncio.run(run())
    assert not manager.checkpoint_file.exists()
    assert HaltCause.checkpoint_failure in manager.bunch.server_state.halt_causes


def test_newer_fixed_view_wins_out_of_order_completion(tmp_path):
    manager, scheduler, flow = _manager(tmp_path)
    waiting = asyncio.Event()
    release = asyncio.Event()
    real_write = manager._write_fixed_background

    async def delayed(sequence, version, saved_at):
        if sequence == 1:
            waiting.set()
            await release.wait()
        return await real_write(sequence, version, saved_at)

    manager._write_fixed_background = delayed

    async def run():
        older = asyncio.create_task(manager.write_checkpoint_async())
        await waiting.wait()
        flow.add_task(Task("newer"))
        assert "/f/newer" in scheduler.version_store.pin().index
        assert await manager.write_checkpoint_async() is True
        release.set()
        assert await older is True
        await manager._drain_in_flight_write()
        if manager._background_pool is not None:
            manager._background_pool.shutdown(wait=True)
            manager._background_pool = None

    asyncio.run(run())
    assert (
        restore_checkpoint(manager.checkpoint_file.read_bytes()).find_node("/f/newer")
        is not None
    )


def test_cancelled_caller_is_drained_before_final_write(tmp_path):
    manager, _, _ = _manager(tmp_path)
    waiting = asyncio.Event()
    release = asyncio.Event()
    real_write = manager._write_fixed_background

    async def delayed(sequence, version, saved_at):
        waiting.set()
        await release.wait()
        return await real_write(sequence, version, saved_at)

    manager._write_fixed_background = delayed

    async def run():
        caller = asyncio.create_task(manager.write_checkpoint_async())
        await waiting.wait()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        stopping = asyncio.create_task(manager.stop())
        await asyncio.sleep(0)
        assert not stopping.done()
        release.set()
        assert await stopping is True

    asyncio.run(run())
    assert (
        restore_checkpoint(manager.checkpoint_file.read_bytes()).find_node("/f/t")
        is not None
    )


def test_background_queue_has_explicit_capacity(tmp_path):
    manager, _, _ = _manager(tmp_path)
    started = asyncio.Event()
    release = asyncio.Event()

    async def delayed(sequence, version, saved_at):
        started.set()
        await release.wait()
        return True, False, None

    manager._write_fixed_background = delayed

    async def run():
        pending = [
            asyncio.create_task(manager.write_checkpoint_async()) for _ in range(3)
        ]
        await started.wait()
        assert await manager.write_checkpoint_async() is False
        assert manager.checkpoint_failure_reason == "resource exhausted"
        release.set()
        await asyncio.gather(*pending)
        await manager._drain_in_flight_write()

    asyncio.run(run())
    assert manager._background_reserved == 0
    assert not manager._background_writes


def test_background_worker_exception_reports_failure(tmp_path):
    manager, _, _ = _manager(tmp_path)

    async def broken(sequence, version, saved_at):
        raise RuntimeError("worker crashed")

    manager._write_fixed_background = broken

    async def run():
        assert await manager.write_checkpoint_async() is False
        await manager._drain_in_flight_write()

    asyncio.run(run())
    assert not manager.checkpoint_file.exists()
    assert manager.checkpoint_failure_reason == "snapshot validation error"
    assert HaltCause.checkpoint_failure in manager.bunch.server_state.halt_causes


def test_stream_validation_rejects_invalid_fixed_record_before_output(tmp_path):
    _, scheduler, _ = _manager(tmp_path)
    version = scheduler.version_store.pin()
    row = version.node("/f/t")
    runtime = json.loads(row.runtime)
    runtime["kind"] = "flow"
    shard_no, offset = version.index["/f/t"]
    shard = list(version.nodes[shard_no])
    shard[offset] = replace(row, runtime=json.dumps(runtime).encode())
    shards = list(version.nodes)
    shards[shard_no] = tuple(shard)
    damaged = replace(version, nodes=tuple(shards))
    output = StringIO()

    with pytest.raises(Exception):
        damaged.checkpoint_stream(output.write)
    assert output.getvalue() == ""


def test_process_transfer_excludes_public_records_and_keeps_secrets(tmp_path):
    manager, scheduler, _ = _manager(tmp_path)
    task = manager.bunch.find_node("/f/t")
    assert task.run()
    version = scheduler.version_store.pin()
    transferred = pickle.loads(pickle.dumps(CheckpointTransfer(version)))
    assert all(row.public is None for row in transferred.iter_nodes())
    output = StringIO()
    transferred.checkpoint_stream(output.write)
    assert (
        restore_checkpoint(output.getvalue()).find_node("/f/t").job_password
        == task.job_password
    )


def test_saved_at_advances_for_repeated_fixed_writes(tmp_path):
    manager, _, _ = _manager(tmp_path)
    assert manager.write_checkpoint()
    first_bytes = manager.checkpoint_file.read_bytes()
    first = json.loads(manager.checkpoint_file.read_text())["saved_at"]
    assert manager.write_checkpoint()
    second = json.loads(manager.checkpoint_file.read_text())["saved_at"]
    assert second > first
    assert manager.backup_file.read_bytes() == first_bytes
    assert stat.S_IMODE(manager.backup_file.stat().st_mode) == 0o600


def test_fixed_backup_falls_back_to_copy_without_hardlinks(tmp_path, monkeypatch):
    manager, _, _ = _manager(tmp_path)
    assert manager.write_checkpoint()
    first_bytes = manager.checkpoint_file.read_bytes()

    def unavailable(*args, **kwargs):
        raise OSError(errno.EOPNOTSUPP, "no hard links")

    monkeypatch.setattr("takler.server.checkpoint.os.link", unavailable)
    assert manager.write_checkpoint()
    assert manager.backup_file.read_bytes() == first_bytes
    assert stat.S_IMODE(manager.backup_file.stat().st_mode) == 0o600


def test_prepared_write_failure_preserves_primary_and_cleans_temp(tmp_path):
    manager, _, _ = _manager(tmp_path)
    assert manager.write_checkpoint()
    primary = manager.checkpoint_file.read_bytes()

    def fail(prepared):
        raise OSError(errno.ENOSPC, "full")

    manager._replace_checkpoint = fail
    assert manager.write_checkpoint() is False
    assert manager.checkpoint_file.read_bytes() == primary
    assert manager.checkpoint_failure_reason == "storage full"
    assert not [path for path in tmp_path.iterdir() if ".tmp." in path.name]


def test_trusted_plugin_uses_fixed_thread_path_and_restores(tmp_path):
    class PluginTask(Task):
        def __init__(self, name, command):
            super().__init__(name)
            self.command = command
            self.payload = {"nested": [1, 2]}

    class PluginDefinition(DefinitionModel):
        command: str

    class PluginRuntime(DefinitionModel):
        payload: dict[str, list[int]] = Field(default_factory=dict)

    registry = builtin_registry()
    registry.register(
        NodeRegistration(
            type_id="example.checkpoint",
            python_type=PluginTask,
            kind="task",
            definition_schema=PluginDefinition,
            export_definition=lambda node: {"command": node.command},
            construct=lambda name, data: PluginTask(name, data.command),
            runtime_schema=PluginRuntime,
            export_runtime=lambda node: {"payload": node.payload},
            restore_runtime=lambda node, data: setattr(node, "payload", data.payload),
        )
    )
    with using_registry(registry):
        bunch = Bunch()
        bunch.add_flow(Flow("f")).add_task(PluginTask("p", "echo"))
        scheduler = Scheduler(bunch)
        manager = CheckpointManager(bunch, checkpoint_file=tmp_path / "checkpoint")
        manager.query_scheduler = scheduler
        scheduler.checkpoint_manager = manager
        assert asyncio.run(manager.write_checkpoint_async()) is True
        assert manager._background_pool is None
        restored = restore_checkpoint(
            manager.checkpoint_file.read_bytes(), registry=registry
        )
        assert restored.find_node("/f/p").payload == {"nested": [1, 2]}


def test_fixed_output_budget_fails_before_publication(tmp_path, monkeypatch):
    manager, _, _ = _manager(tmp_path)
    monkeypatch.setattr("takler.server.checkpoint.MAX_CHECKPOINT_OUTPUT_BYTES", 100)
    assert manager.write_checkpoint() is False
    assert manager.checkpoint_failure_reason == "resource exhausted"
    assert not manager.checkpoint_file.exists()
    assert not list(tmp_path.iterdir())


def test_background_timeout_terminates_worker_without_publication(
    tmp_path, monkeypatch
):
    manager, _, _ = _manager(tmp_path)
    monkeypatch.setattr("takler.server.checkpoint.MAX_BACKGROUND_SECONDS", 0.000001)
    assert asyncio.run(manager.write_checkpoint_async()) is False
    assert manager._background_pool is None
    assert not manager.checkpoint_file.exists()
    assert not list(tmp_path.iterdir())


def test_writer_transaction_cannot_be_split_by_checkpoint(tmp_path):
    manager, scheduler, _ = _manager(tmp_path)
    scheduler.query_changes.begin()
    try:
        assert manager.write_checkpoint() is False
        assert manager.checkpoint_failure_reason is None
        assert not manager.checkpoint_file.exists()
    finally:
        scheduler.query_changes.end()
    assert manager.write_checkpoint() is True
