"""R3-10 limits and fairness under burst and uncertain submission outcomes."""

import asyncio
from subprocess import DEVNULL

import pytest

from takler.core import Bunch, Flow, NodeStatus, Task
from takler.core.node import compute_most_significant_status
from takler.exceptions import JobSubmissionError
from takler.protocol.commands import ServerHaltCommand
from takler.server.scheduler import Scheduler, FLOW_SCAN_SLICE_NODES
from takler.tasks.shell import ShellScriptTask
from takler.tasks.shell import shell_runner
from takler.tasks.shell.shell_render import ShellRender


def test_submit_capacity_is_shared_and_rejects_before_start(monkeypatch):
    monkeypatch.setattr(shell_runner, "MAX_PENDING_COMMANDS", 3)
    monkeypatch.setattr(shell_runner, "MAX_RUNNING_COMMANDS", 1)

    async def scenario():
        gate = asyncio.Event()
        first_started = asyncio.Event()
        started = []

        async def slow(command, **kwargs):
            started.append(command)
            assert kwargs == {"stdout": DEVNULL, "stderr": DEVNULL}
            first_started.set()
            await gate.wait()

        monkeypatch.setattr(shell_runner, "run_process", slow)
        runners = [shell_runner.ShellRunner() for _ in range(4)]
        tasks = [runner.spawn("sleep 1") for runner in runners[:3]]
        with pytest.raises(JobSubmissionError, match="capacity exhausted"):
            runners[3].spawn("must-not-start")
        await asyncio.wait_for(first_started.wait(), 1)
        assert len(started) == 1
        gate.set()
        await asyncio.gather(*tasks)
        await asyncio.sleep(0)
        assert len(started) == 3
        assert all(not runner._job_tasks for runner in runners)

    asyncio.run(scenario())


def test_failure_storm_is_bounded_and_releases_pending_slots(monkeypatch):
    async def fail(command, **kwargs):
        raise RuntimeError("submit failed")

    monkeypatch.setattr(shell_runner, "run_process", fail)
    monkeypatch.setattr(shell_runner.logger, "error", lambda message: None)

    async def scenario():
        runner = shell_runner.ShellRunner()
        failures = []
        accepted = []
        rejected = 0
        for _ in range(200):
            try:
                accepted.append(
                    runner.spawn("failing submit", on_failure=failures.append)
                )
            except JobSubmissionError:
                rejected += 1
        assert len(accepted) == shell_runner.MAX_PENDING_COMMANDS
        assert rejected == 200 - shell_runner.MAX_PENDING_COMMANDS
        results = await asyncio.gather(*accepted, return_exceptions=True)
        await asyncio.sleep(0)
        assert all(isinstance(result, RuntimeError) for result in results)
        assert len(failures) == len(accepted)
        assert not runner._job_tasks
        assert asyncio.get_running_loop()._takler_command_budget.pending == 0

    asyncio.run(scenario())


def test_timeout_of_external_submit_preserves_attempt_and_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(shell_runner, "COMMAND_TIMEOUT_SECONDS", 0.01)

    async def hung(command, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(shell_runner, "run_process", hung)

    async def scenario():
        bunch = Bunch()
        flow = bunch.add_flow(Flow("f"))
        flow.add_parameter("TAKLER_HOME", str(tmp_path))
        limit = flow.add_limit("slots", 1)
        task = flow.add_task(ShellScriptTask("t"))
        task.add_in_limit("slots", "/f")
        monkeypatch.setattr(task, "create_job_script", lambda: "external submit")
        monkeypatch.setattr(task, "_uses_orvix", lambda: True)
        flow.begin()
        assert task.run()
        attempt = task.attempt_id
        for _ in range(100):
            if task.submission_unknown_reason is not None:
                break
            await asyncio.sleep(0.002)
        assert task.state.node_status is NodeStatus.submitted
        assert task.attempt_id == attempt
        assert task.submission_unknown_reason.startswith("TimeoutError:")
        assert limit.value == 1

    asyncio.run(scenario())


def test_queue_wait_timeout_is_definite_nonstart(tmp_path, monkeypatch):
    monkeypatch.setattr(shell_runner, "MAX_RUNNING_COMMANDS", 1)
    monkeypatch.setattr(shell_runner, "QUEUE_WAIT_TIMEOUT_SECONDS", 0.01)
    gate = asyncio.Event()

    async def slow(command, **kwargs):
        await gate.wait()

    monkeypatch.setattr(shell_runner, "run_process", slow)

    async def scenario():
        bunch = Bunch()
        flow = bunch.add_flow(Flow("f"))
        flow.add_parameter("TAKLER_HOME", str(tmp_path))
        first = flow.add_task(ShellScriptTask("first"))
        second = flow.add_task(ShellScriptTask("second"))
        for task in (first, second):
            monkeypatch.setattr(task, "create_job_script", lambda: "external submit")
            monkeypatch.setattr(task, "_uses_orvix", lambda: True)
        flow.begin()
        assert first.run()
        assert second.run()
        for _ in range(100):
            if second.state.node_status is NodeStatus.aborted:
                break
            await asyncio.sleep(0.002)
        assert second.state.node_status is NodeStatus.aborted
        assert second.submission_unknown_reason is None
        assert "queue wait exceeded" in second.aborted_reason
        gate.set()
        await asyncio.sleep(0)

    asyncio.run(scenario())


def test_halt_discards_queued_start_but_keeps_running_wrapper(tmp_path, monkeypatch):
    monkeypatch.setattr(shell_runner, "MAX_RUNNING_COMMANDS", 1)

    async def scenario():
        started = asyncio.Event()
        finish = asyncio.Event()

        async def slow(command, **kwargs):
            started.set()
            await finish.wait()

        monkeypatch.setattr(shell_runner, "run_process", slow)
        bunch = Bunch()
        flow = bunch.add_flow(Flow("f"))
        flow.add_parameter("TAKLER_HOME", str(tmp_path))
        first = flow.add_task(ShellScriptTask("first"))
        second = flow.add_task(ShellScriptTask("second"))
        for task in (first, second):
            monkeypatch.setattr(task, "create_job_script", lambda: "external submit")
            monkeypatch.setattr(task, "_uses_orvix", lambda: True)
        flow.begin()
        assert first.run()
        await started.wait()
        assert second.run()
        await asyncio.sleep(0)
        Scheduler(bunch).run_command_server_halt(ServerHaltCommand())
        assert first.state.node_status is NodeStatus.submitted
        assert second.state.node_status is NodeStatus.aborted
        assert second.submission_unknown_reason is None
        finish.set()
        for _ in range(100):
            if asyncio.get_running_loop()._takler_command_budget.pending == 0:
                break
            await asyncio.sleep(0.002)
        budget = asyncio.get_running_loop()._takler_command_budget
        assert budget.pending == 0
        assert budget.processes._value == 1

    asyncio.run(scenario())


def test_stop_wakes_idle_scheduler_promptly():
    async def scenario():
        scheduler = Scheduler(Bunch(), interval_main_loop=60)
        loop = asyncio.create_task(scheduler.run())
        await asyncio.sleep(0)
        await asyncio.wait_for(scheduler.stop(), 0.5)
        await asyncio.wait_for(loop, 0.5)

    asyncio.run(scenario())


def test_shutdown_gate_keeps_idle_scheduler_yielding():
    async def scenario():
        scheduler = Scheduler(Bunch(), interval_main_loop=0.01)
        loop = asyncio.create_task(scheduler.run())
        try:
            await asyncio.sleep(0)
            scheduler.begin_shutdown()
            # Transports must still get a turn before Scheduler.stop is called.
            await asyncio.sleep(0.03)
            assert scheduler.bunch._service_stopping
            assert not loop.done()
            await asyncio.wait_for(scheduler.stop(), 0.5)
            await asyncio.wait_for(loop, 0.5)
        finally:
            if not loop.done():
                loop.cancel()
                await asyncio.gather(loop, return_exceptions=True)

    asyncio.run(scenario())


def test_large_submit_output_is_discarded():
    async def scenario():
        runner = shell_runner.ShellRunner()
        task = runner.spawn("yes output | head -c 8388608")
        await asyncio.wait_for(task, 5)
        await asyncio.sleep(0)
        assert not runner._job_tasks

    asyncio.run(scenario())


def test_builtin_flows_take_turns_during_runnable_burst():
    starts = []

    class RecordingTask(Task):
        def do_run(self):
            starts.append(self.get_flow().name)
            return True

    async def scenario():
        bunch = Bunch()
        for name in ("first", "second"):
            flow = bunch.add_flow(Flow(name))
            for index in range(2 * FLOW_SCAN_SLICE_NODES):
                flow.add_task(RecordingTask(f"t{index}"))
            flow.begin()
        scheduler = Scheduler(bunch, interval_main_loop=0.01)
        loop = asyncio.create_task(scheduler.main_loop())
        for _ in range(500):
            if len(starts) == 4 * FLOW_SCAN_SLICE_NODES:
                break
            await asyncio.sleep(0.002)
        scheduler.should_stop = True
        await asyncio.wait_for(loop, 1)
        assert len(starts) == 4 * FLOW_SCAN_SLICE_NODES
        assert starts.index("second") <= FLOW_SCAN_SLICE_NODES
        assert (
            starts.index("first", starts.index("second")) <= 2 * FLOW_SCAN_SLICE_NODES
        )

    asyncio.run(scenario())


def test_parent_status_counts_follow_updates_and_structure_changes():
    flow = Flow("f")
    children = [flow.add_task(Task(f"t{i}")) for i in range(1000)]
    flow.begin()
    assert flow.computed_status(True) is NodeStatus.queued
    for node in children:
        node.set_node_status_only(NodeStatus.complete)
        assert flow.computed_status(True) is compute_most_significant_status(
            flow.children, True
        )
    replacement = Task("replacement")
    replacement.set_node_status_only(NodeStatus.queued)
    flow.update_child(children[0], replacement)
    assert flow.computed_status(True) is NodeStatus.queued
    flow.delete_child(replacement)
    assert flow.computed_status(True) is NodeStatus.complete


def test_render_limits_script_and_submit_command(tmp_path, monkeypatch):
    from takler.tasks.shell import shell_render

    monkeypatch.setattr(shell_render, "MAX_JOB_SCRIPT_BYTES", 8)
    monkeypatch.setattr(shell_render, "MAX_SUBMIT_COMMAND_BYTES", 8)
    source = tmp_path / "source.takler"
    source.write_text("{{ VALUE }}")
    task = ShellScriptTask("t", source)
    task.add_parameter("TAKLER_HOME", str(tmp_path))
    task.add_parameter("VALUE", "long output")
    task.update_generated_parameters()
    render = ShellRender(task)
    with pytest.raises(ValueError, match="rendered job script exceeds"):
        render.render_script(source)
    assert not task.find_parameter("TAKLER_JOB").value.exists()
    with pytest.raises(ValueError, match="submit command exceeds"):
        render.render_command("{{ VALUE }}")


def test_render_rejects_oversized_include_before_loading(tmp_path, monkeypatch):
    from takler.tasks.shell import shell_render

    monkeypatch.setattr(shell_render, "MAX_TEMPLATE_BYTES", 32)
    source = tmp_path / "source.takler"
    source.write_text("{% include 'i' %}")
    (tmp_path / "i").write_text("x" * 33)
    task = ShellScriptTask("t", source)
    task.add_parameter("TAKLER_HOME", str(tmp_path))
    task.update_generated_parameters()
    with pytest.raises(ValueError, match="job template exceeds"):
        ShellRender(task).render_script(source)
