"""Service halt gates every new execution while leaving reports and state edits live."""

import datetime

import pytest

from takler.client.exit_code import exit_code_for_error_code
from takler.core import Bunch, HaltCause, NodeStatus, ServerStatus, Task, task
from takler.core.bunch import ServerState
from takler.exceptions import FlowStateError
from takler.protocol.commands import (
    BeginCommand,
    CompleteCommand,
    ForceCommand,
    RequeueCommand,
    ResumeCommand,
    RunCommand,
)
from takler.server.scheduler import Scheduler


def halt(bunch):
    bunch.server_state = ServerState(
        status=ServerStatus.halted,
        status_reason="manual pause",
        halt_causes=[HaltCause.manual],
    )


@pytest.fixture
def scheduler():
    bunch = Bunch()
    flow = bunch.add_flow("f")
    first = flow.add_task("a")
    second = flow.add_task("b")
    flow.begin()
    return Scheduler(bunch), flow, first, second


def test_halted_auto_scan_and_direct_task_entry_do_not_allocate(scheduler):
    runner, flow, first, second = scheduler
    halt(runner.bunch)
    runner._process_flow(flow.name, flow, datetime.datetime.now())
    runner.travel_bunch()
    for node in (first, second):
        with pytest.raises(FlowStateError, match="service is halted"):
            node.run()
        with pytest.raises(FlowStateError, match="service is halted"):
            node.before_run()
        assert node.state.node_status is NodeStatus.queued
        assert node.try_no == 0
        assert node.attempt_id is None
        assert node.job_password is None


@pytest.mark.parametrize("force", [False, True])
def test_halted_manual_batch_reports_each_rejection(scheduler, force):
    runner, _, first, second = scheduler
    halt(runner.bunch)
    result = runner.run_command_run(
        RunCommand(node_paths=[first.node_path, second.node_path], force=force)
    )
    assert result.flag == 16
    assert exit_code_for_error_code(result.flag) == 1
    assert [item.flag for item in result.results] == [14, 14]
    assert [item.effect for item in result.results] == ["none", "none"]
    assert [node.try_no for node in (first, second)] == [0, 0]
    assert all(node.attempt_id is None for node in (first, second))


def test_batch_rechecks_status_for_each_target(scheduler, monkeypatch):
    runner, _, first, second = scheduler

    def start_then_halt():
        halt(runner.bunch)
        return True

    monkeypatch.setattr(first, "do_run", start_then_halt)
    result = runner.run_command_run(
        RunCommand(node_paths=[first.node_path, second.node_path])
    )
    assert result.flag == 16
    assert [item.flag for item in result.results] == [0, 14]
    assert [item.effect for item in result.results] == ["applied", "none"]
    assert first.attempt_id is not None
    assert second.attempt_id is None
    assert second.try_no == 0


def test_calendar_halt_is_rechecked_before_dependency_resolution(
    scheduler, monkeypatch
):
    runner, flow, first, _ = scheduler
    called = []

    def update(_):
        halt(runner.bunch)

    monkeypatch.setattr(flow, "update_calendar", update)
    monkeypatch.setattr(flow, "resolve_dependencies", lambda: called.append(True))
    runner._process_flow(flow.name, flow, datetime.datetime.now())
    assert called == []
    assert first.attempt_id is None


def test_travel_stops_after_halt_between_flows(scheduler, monkeypatch):
    runner, flow, _, _ = scheduler
    second_flow = runner.bunch.add_flow("second")
    second_flow.add_task("t")
    second_flow.begin()
    called = []

    def first_resolve():
        called.append("first")
        halt(runner.bunch)

    monkeypatch.setattr(flow, "resolve_dependencies", first_resolve)
    monkeypatch.setattr(
        second_flow, "resolve_dependencies", lambda: called.append("second")
    )
    runner.travel_bunch()
    assert called == ["first"]


def test_halt_in_trusted_hook_restores_unused_attempt(scheduler):
    runner, flow, _, _ = scheduler

    class HookTask(Task):
        def before_run(self):
            super().before_run()
            halt(runner.bunch)

        def do_run(self):
            pytest.fail("do_run must not start after halt")

    node = flow.add_task(HookTask("hook"))
    with pytest.raises(FlowStateError, match="service is halted"):
        node.run()
    assert node.try_no == 0
    assert node.attempt_id is None
    assert node.job_password is None
    assert node.state.node_status is NodeStatus.unknown


def test_halted_inline_function_does_not_execute_or_allocate(scheduler):
    runner, flow, _, _ = scheduler
    calls = []
    inline = task("inline")(lambda self: calls.append(self.attempt_id))()
    flow.add_task(inline)
    flow.begin(force=True)
    halt(runner.bunch)
    with pytest.raises(FlowStateError, match="service is halted"):
        inline.run()
    assert calls == []
    assert inline.try_no == 0
    assert inline.attempt_id is None


def test_halted_state_edits_and_child_completion_do_not_resume_service(scheduler):
    runner, flow, first, second = scheduler
    first.run()
    attempt_id = first.attempt_id
    halt(runner.bunch)

    runner.run_command_complete(
        CompleteCommand(node_path=first.node_path, attempt_id=attempt_id)
    )
    assert first.state.node_status is NodeStatus.complete
    assert (
        runner.run_command_resume(ResumeCommand(node_paths=[second.node_path])).flag
        == 0
    )
    assert (
        runner.run_command_force(
            ForceCommand(paths=[second.node_path], state="complete")
        ).flag
        == 0
    )
    assert flow.state.node_status is NodeStatus.complete
    assert runner.bunch.server_state.status is ServerStatus.halted
    runner.run_command_requeue(RequeueCommand(node_paths=[second.node_path]))
    assert second.try_no == 0
    assert second.attempt_id is None
    assert runner.bunch.server_state.status is ServerStatus.halted


def test_halted_begin_requeues_without_starting(scheduler):
    runner, flow, first, second = scheduler
    halt(runner.bunch)
    result = runner.run_command_begin(BeginCommand(flow_name=flow.name, force=True))
    assert result.flag == 0
    assert runner.bunch.server_state.status is ServerStatus.halted
    assert [node.try_no for node in (first, second)] == [0, 0]
    assert all(node.attempt_id is None for node in (first, second))


def test_already_started_shell_process_continues_while_halted(tmp_path, monkeypatch):
    import asyncio

    from takler.tasks.shell import ShellScriptTask, shell_runner

    async def scenario():
        bunch = Bunch()
        flow = bunch.add_flow("f")
        flow.add_parameter("TAKLER_HOME", str(tmp_path))
        source = tmp_path / "source.takler"
        source.write_text("echo hello\n")
        node = flow.add_task(ShellScriptTask("t", source))
        flow.begin()
        started = asyncio.Event()
        finish = asyncio.Event()
        ended = asyncio.Event()

        async def process(*_):
            started.set()
            await finish.wait()
            ended.set()

        monkeypatch.setattr(shell_runner, "run_process", process)
        assert node.run()
        await started.wait()
        halt(bunch)
        assert node.state.node_status is NodeStatus.submitted
        finish.set()
        await ended.wait()
        await asyncio.sleep(0)
        assert node.state.node_status is NodeStatus.submitted
        Scheduler(bunch).run_command_complete(
            CompleteCommand(node_path=node.node_path, attempt_id=node.attempt_id)
        )
        assert node.state.node_status is NodeStatus.complete
        assert bunch.server_state.status is ServerStatus.halted

    asyncio.run(scenario())


def test_running_suspended_task_keeps_manual_run_semantics(scheduler):
    runner, _, first, _ = scheduler
    first.suspend()
    result = runner.run_command_run(RunCommand(node_paths=[first.node_path]))
    assert result.flag == 0
    assert first.state.suspended
    assert first.state.node_status is NodeStatus.submitted
    assert first.attempt_id is not None
