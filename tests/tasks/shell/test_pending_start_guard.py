"""R3-02: a deferred shell submission must still own its attempt at process start."""

import asyncio
import json
from pathlib import Path

import pytest

from takler.core import Bunch, Flow, NodeStatus
from takler.protocol.commands import ForceCommand, ReplaceCommand, ServerHaltCommand
from takler.serialization import export_definition
from takler.serialization.checkpoint import capture_checkpoint, restore_checkpoint
from takler.server import TaklerServer
from takler.server.scheduler import Scheduler
from takler.tasks.shell import ShellScriptTask, shell_runner


def _pending_task(tmp_path, monkeypatch):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    flow.add_parameter("TAKLER_HOME", str(tmp_path))
    task = flow.add_task(ShellScriptTask("t"))
    monkeypatch.setattr(task, "create_job_script", lambda: "submit")
    flow.begin()
    return Scheduler(bunch), flow, task


@pytest.mark.parametrize(
    "invalidation",
    [
        "force_queued",
        "force_complete",
        "attempt",
        "generation",
        "flow_generation",
        "token",
        "active",
        "unbegun",
        "detached",
        "replace",
        "halt",
        "shutdown",
    ],
)
def test_invalidated_pending_command_never_starts(tmp_path, monkeypatch, invalidation):
    async def exercise():
        scheduler, flow, task = _pending_task(tmp_path, monkeypatch)
        calls = []

        async def fake_process(argv):
            calls.append(argv)

        monkeypatch.setattr(shell_runner, "run_process", fake_process)
        assert task.run()
        original_attempt = task.attempt_id
        if invalidation.startswith("force_"):
            state = invalidation.removeprefix("force_")
            assert (
                scheduler.run_command_force(
                    ForceCommand(paths=[task.node_path], state=state)
                ).flag
                == 0
            )
        elif invalidation == "attempt":
            task.attempt_id = "another-attempt"
        elif invalidation == "generation":
            task.definition_generation = "another-generation"
        elif invalidation == "flow_generation":
            flow.definition_generation = "another-generation"
        elif invalidation == "token":
            task._submission_token = object()
        elif invalidation == "active":
            task.set_node_status(NodeStatus.active)
        elif invalidation == "unbegun":
            flow.begun = False
        elif invalidation == "detached":
            scheduler.bunch.delete_flow("f")
            replacement = scheduler.bunch.add_flow(Flow("f"))
            replacement.add_task("t")
        elif invalidation == "replace":
            scheduler.run_command_force(
                ForceCommand(paths=[task.node_path], state="complete")
            )
            candidate = Flow("f")
            candidate.add_task("t")
            document = export_definition(candidate).model_dump(mode="json")
            scheduler.run_command_replace(
                ReplaceCommand(
                    target_path="/f", flow_bytes=json.dumps(document).encode()
                )
            )
        elif invalidation == "halt":
            scheduler.run_command_server_halt(ServerHaltCommand())
        elif invalidation == "shutdown":
            scheduler.begin_shutdown()
            assert not scheduler.should_stop
        for _ in range(4):
            await asyncio.sleep(0)
        assert calls == []
        if invalidation in {"detached", "replace"}:
            current = scheduler.bunch.find_node("/f/t")
            assert current is not task
            assert current.attempt_id is None
            assert current.state.node_status is not NodeStatus.aborted
        if invalidation in {"halt", "shutdown"}:
            assert task.attempt_id == original_attempt
            assert task.state.node_status is NodeStatus.aborted
            assert task.aborted_reason == "submission skipped before process start"

    asyncio.run(exercise())


def test_server_stop_closes_gate_before_transport_yields(tmp_path, monkeypatch):
    async def exercise():
        server = TaklerServer(checkpoint_file=tmp_path / "checkpoint")
        flow = server.bunch.add_flow(Flow("f"))
        flow.add_parameter("TAKLER_HOME", str(tmp_path))
        task = flow.add_task(ShellScriptTask("t"))
        monkeypatch.setattr(task, "create_job_script", lambda: "submit")
        flow.begin()
        calls = []

        async def fake_process(argv):
            calls.append(argv)

        async def stop_transport():
            assert server.bunch._service_stopping
            await asyncio.sleep(0)

        async def finished():
            return True

        monkeypatch.setattr(shell_runner, "run_process", fake_process)
        monkeypatch.setattr(server.grpc_transport, "stop", stop_transport)
        monkeypatch.setattr(server.scheduler, "stop", finished)
        monkeypatch.setattr(server.checkpoint_manager, "stop", finished)
        assert task.run()
        await server.stop()
        for _ in range(4):
            await asyncio.sleep(0)
        assert calls == []

    asyncio.run(exercise())


def test_shutdown_gate_can_precede_scheduler_stop(tmp_path, monkeypatch):
    async def exercise():
        scheduler, _, task = _pending_task(tmp_path, monkeypatch)
        scheduler.interval_main_loop = 0.01
        calls = []

        async def fake_process(argv):
            calls.append(argv)

        monkeypatch.setattr(shell_runner, "run_process", fake_process)
        assert task.run()
        loop_task = asyncio.create_task(scheduler.run())
        scheduler.begin_shutdown()
        await asyncio.sleep(0.03)  # Transport shutdown can yield for several scans.
        assert not scheduler.should_stop
        await asyncio.wait_for(scheduler.stop(), 1)
        await asyncio.wait_for(loop_task, 1)
        assert calls == []

    asyncio.run(exercise())


def test_shutdown_gate_is_process_local(tmp_path, monkeypatch):
    scheduler, _, _ = _pending_task(tmp_path, monkeypatch)
    scheduler.begin_shutdown()
    checkpoint = capture_checkpoint(scheduler.bunch)
    assert "_service_stopping" not in str(checkpoint)
    restored = restore_checkpoint(checkpoint)
    assert not restored._service_stopping


def test_local_command_starts_only_current_attempt(tmp_path, monkeypatch):
    async def exercise():
        _, _, task = _pending_task(tmp_path, monkeypatch)
        marker = tmp_path / "started.txt"

        def command():
            return f"printf '%s\\n' '{task.attempt_id}' >> '{marker}'"

        monkeypatch.setattr(task, "create_job_script", command)
        assert task.run()
        old_attempt = task.attempt_id
        task.requeue()
        assert task.run()
        new_attempt = task.attempt_id
        deadline = asyncio.get_running_loop().time() + 5
        while not marker.exists():
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        assert Path(marker).read_text().splitlines() == [new_attempt]
        assert new_attempt != old_attempt

    asyncio.run(exercise())
