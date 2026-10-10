"""Current attempt artifacts and shell failure ownership."""

from pathlib import Path

import pytest

from takler.core import Bunch, Flow, HaltCause, NodeStatus, ServerStatus
from takler.core.bunch import ServerState
from takler.exceptions import FlowStateError
from takler.tasks.shell import ShellScriptTask
from takler.tasks.shell.shell_runner import ShellRunner
from takler.tasks.shell.shell_render import ShellRender


@pytest.fixture
def shell_flow(tmp_path, monkeypatch):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    flow.add_parameter("TAKLER_HOME", str(tmp_path))
    script = tmp_path / "source.takler"
    script.write_text("echo {{ TAKLER_ATTEMPT_ID }}\n")
    task = flow.add_task(ShellScriptTask("t", script))
    calls = []
    monkeypatch.setattr(ShellRunner, "spawn", lambda self, **kw: calls.append(kw))
    flow.begin()
    return bunch, flow, task, calls


def test_requeue_with_same_try_number_keeps_old_files(shell_flow):
    _, _, task, _ = shell_flow
    assert task.run()
    first_id, first_path = task.attempt_id, Path(task.job_path)
    first_content = first_path.read_text()
    assert first_id in first_content
    assert task.try_no == 1

    task.requeue()
    assert task.run()
    assert task.try_no == 1
    assert task.attempt_id != first_id
    assert Path(task.job_path) != first_path
    assert first_path.read_text() == first_content
    assert task.attempt_id in Path(task.job_path).read_text()
    assert task.jobout_path.endswith(task.attempt_id)


def test_failed_command_render_keeps_written_reference_until_requeue(
    shell_flow, monkeypatch
):
    bunch, _, task, calls = shell_flow

    def fail_command(_):
        raise RuntimeError("command render failed")

    monkeypatch.setattr(ShellRender, "render_job_command", fail_command)
    assert task.run() is False
    assert calls == []
    path = Path(task.job_path)
    assert path.exists()
    assert bunch._artifact_path_index.by_path[path.resolve()] == (task, "job_path")
    task.requeue()
    assert path.resolve() not in bunch._artifact_path_index.by_path


@pytest.mark.parametrize("template", ["output", "{attempt_id}/{attempt_id}", 3])
def test_invalid_override_rejected_before_write(shell_flow, template):
    _, _, task, calls = shell_flow
    task.add_parameter("TAKLER_JOB", template)
    assert task.run() is False
    assert calls == []
    assert task.state.node_status is NodeStatus.aborted
    assert not list(
        Path(task.find_parent_parameter("TAKLER_HOME").value).rglob("*.job*")
    )


def test_override_resolves_and_checks_current_artifact_collision(
    shell_flow, tmp_path, monkeypatch
):
    _, flow, task, calls = shell_flow
    job_template = str(tmp_path / "jobs" / "{attempt_id}.job")
    out_template = str(tmp_path / "output" / "{attempt_id}.txt")
    flow.add_parameter("TAKLER_JOB", job_template)
    flow.add_parameter("TAKLER_JOBOUT", out_template)
    assert task.run()
    assert Path(task.job_path).exists()
    assert task.job_path == str((tmp_path / "jobs" / f"{task.attempt_id}.job"))
    assert task.jobout_path == str(tmp_path / "output" / f"{task.attempt_id}.txt")
    assert job_template == flow.find_parameter("TAKLER_JOB").value

    other = flow.add_task(ShellScriptTask("other", task.script_path))
    other.add_parameter("TAKLER_HOME", str(tmp_path))
    # A generated UUID does not prevent a path clash caused by a user path.
    other.add_parameter(
        "TAKLER_JOB", str(task.job_path).replace(task.attempt_id, "{attempt_id}")
    )
    from uuid import UUID

    from takler.core import task_node

    monkeypatch.setattr(task_node, "uuid4", lambda: UUID(task.attempt_id))
    assert other.run() is False
    assert len(calls) == 1
    assert Path(task.job_path).exists()


def test_callback_checks_attempt_and_generation(shell_flow):
    _, _, task, calls = shell_flow
    assert task.run()
    old = calls[0]["on_failure"]
    before = task.to_dict()
    task.definition_generation = "changed"
    old(RuntimeError("late"))
    assert task.state.node_status is NodeStatus.submitted
    task.definition_generation = before["definition_generation"]
    task.attempt_id = "new-attempt"
    old(RuntimeError("late"))
    assert task.state.node_status is NodeStatus.submitted


def test_orvix_sidecar_and_uncertain_submit_no_resubmission(shell_flow):
    _, flow, task, calls = shell_flow
    flow.add_parameter("TAKLER_SHELL_JOB_CMD", 'orvix submit "{{ TAKLER_JOB }}"')
    assert task.run()
    assert task.orvix_info_path == task.job_path + ".info.yaml"
    assert len(calls) == 1
    calls[0]["on_failure"](RuntimeError("submit returned nonzero"))
    assert task.state.node_status is NodeStatus.submitted
    assert task.aborted_reason is None
    assert "submit returned nonzero" in task.submission_unknown_reason
    assert len(calls) == 1


def test_halted_does_not_allocate_or_write(shell_flow, tmp_path):
    bunch, _, task, calls = shell_flow
    bunch.server_state = ServerState(
        status=ServerStatus.halted,
        status_reason="manual pause",
        halt_causes=[HaltCause.manual],
    )
    with pytest.raises(FlowStateError, match="halted"):
        task.run()
    assert task.attempt_id is None
    assert task.try_no == 0
    assert calls == []
    assert not list(tmp_path.rglob("*.job*"))
    with pytest.raises(FlowStateError, match="halted"):
        task.create_job_script()


def test_halt_before_deferred_process_start_prevents_submit(tmp_path, monkeypatch):
    import asyncio

    from takler.tasks.shell import shell_runner

    async def scenario():
        bunch = Bunch()
        flow = bunch.add_flow(Flow("f"))
        flow.add_parameter("TAKLER_HOME", str(tmp_path))
        source = tmp_path / "source.takler"
        source.write_text("echo hello\n")
        task = flow.add_task(ShellScriptTask("t", source))
        flow.begin()
        called = []

        async def process(*args):
            called.append(args)

        monkeypatch.setattr(shell_runner, "run_process", process)
        assert task.run()
        bunch.server_state = ServerState(
            status=ServerStatus.halted,
            status_reason="manual pause",
            halt_causes=[HaltCause.manual],
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert called == []
        assert task.state.node_status is NodeStatus.aborted

    asyncio.run(scenario())


def test_halted_scheduler_skips_auto_and_rejects_manual_force(shell_flow, tmp_path):
    import datetime

    from takler.protocol.commands import RunCommand
    from takler.server.scheduler import Scheduler

    bunch, flow, task, calls = shell_flow
    bunch.server_state = ServerState(
        status=ServerStatus.halted,
        status_reason="manual pause",
        halt_causes=[HaltCause.manual],
    )
    scheduler = Scheduler(bunch)
    scheduler._process_flow(flow.name, flow, datetime.datetime.now())
    result = scheduler.run_command_run(
        RunCommand(node_paths=[task.node_path], force=True)
    )
    assert result.results[0].effect == "none"
    assert result.results[0].flag != 0
    assert task.try_no == 0
    assert task.attempt_id is None
    assert calls == []
    assert not list(tmp_path.rglob("*.job*"))
