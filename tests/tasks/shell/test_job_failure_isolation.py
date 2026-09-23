"""Exercise the actual callback captured at submission, without external jobs."""

from subprocess import CalledProcessError
from unittest.mock import Mock

import pytest

from takler.core import Bunch, Flow, NodeStatus
from takler.tasks import ShellScriptTask
from takler.tasks.shell.shell_runner import ShellRunner


@pytest.fixture
def submission(monkeypatch):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task(ShellScriptTask("t"))
    monkeypatch.setattr(task, "create_job_script", lambda: "submit-job")
    callbacks = []
    monkeypatch.setattr(
        ShellRunner, "spawn", lambda self, **kw: callbacks.append(kw["on_failure"])
    )
    flow.begin()
    task.run()
    return bunch, flow, task, callbacks


@pytest.mark.parametrize(
    "transition",
    ["requeue", "force_run", "complete", "abort", "force_complete", "force_abort"],
)
def test_old_callback_cannot_abort_new_or_finished_instance(submission, transition):
    _, _, task, callbacks = submission
    old = callbacks[0]
    if transition == "requeue":
        old_try_no = task.try_no
        task.requeue()
        task.run()
        assert task.try_no == old_try_no == 1
    elif transition == "force_run":
        task.run()  # The force-run handler calls run even while in flight.
        assert task.try_no == 2
    elif transition == "complete":
        task.complete()
    elif transition == "abort":
        task.abort("child reason")
    else:
        task.set_node_status_only(
            NodeStatus.complete
            if transition == "force_complete"
            else NodeStatus.aborted
        )
        # A later forced state must not revive the old submission callback.
        task.set_node_status_only(NodeStatus.active)
    before = task.to_dict()
    task.abort = Mock(wraps=task.abort)
    old(RuntimeError("old wrapper failed"))
    task.abort.assert_not_called()
    assert task.to_dict() == before


@pytest.mark.parametrize("status", [NodeStatus.submitted, NodeStatus.active])
def test_current_failure_still_aborts_and_duplicate_callback_is_inert(
    submission, status
):
    _, _, task, callbacks = submission
    task.set_node_status_only(status)
    task.abort = Mock(wraps=task.abort)
    callbacks[0](RuntimeError("current wrapper failed"))
    assert task.state.node_status is NodeStatus.aborted
    assert "current wrapper failed" in task.aborted_reason
    callbacks[0](RuntimeError("duplicate"))
    assert task.abort.call_count == 1


def test_detached_old_tree_cannot_mutate_after_flow_swap(submission):
    bunch, flow, task, callbacks = submission
    task.complete()  # Legal replacement requires no active/submitted nodes.
    replacement = Flow("f")
    replacement.add_task(ShellScriptTask("t"))
    replacement.begin()
    bunch.add_flow(replacement)  # R0-13 supplies the guarded replace operation.
    # Even if an old object is forced/reused, it no longer owns its online path.
    task.run()
    task.abort = Mock(wraps=task.abort)
    before = bunch.to_dict()
    for callback in callbacks:
        callback(RuntimeError("detached"))
    task.abort.assert_not_called()
    assert bunch.to_dict() == before
    assert task.get_flow() is flow


def test_instance_marker_is_not_serialized(submission):
    _, _, task, _ = submission
    serialized = task.to_dict()
    restored = ShellScriptTask.from_dict(serialized)
    assert restored.to_dict() == serialized
    assert task._submission_token is not None
    assert restored._submission_token is None
    assert "submission_token" not in str(serialized)


def test_actual_runner_delivers_old_failure_after_requeue(monkeypatch):
    import asyncio

    from takler.tasks.shell import shell_runner

    async def scenario():
        gates = [asyncio.Event(), asyncio.Event()]
        attempts = []

        async def process(*args):
            index = len(attempts)
            attempts.append(asyncio.current_task())
            await gates[index].wait()
            raise RuntimeError(f"attempt {index} failed")

        monkeypatch.setattr(shell_runner, "run_process", process)
        flow = Flow("f")
        flow.add_parameter("TAKLER_HOME", ".")
        task = flow.add_task(ShellScriptTask("t"))
        monkeypatch.setattr(task, "create_job_script", lambda: "submit")
        flow.begin()
        task.run()
        await asyncio.sleep(0)
        task.requeue()
        task.run()
        await asyncio.sleep(0)
        assert task.try_no == 1
        gates[0].set()
        with pytest.raises(RuntimeError):
            await attempts[0]
        await asyncio.sleep(0)
        assert task.state.node_status is NodeStatus.submitted
        gates[1].set()
        with pytest.raises(RuntimeError):
            await attempts[1]
        await asyncio.sleep(0)
        assert task.state.node_status is NodeStatus.aborted
        assert task.aborted_reason == "RuntimeError: attempt 1 failed"

    asyncio.run(scenario())


@pytest.mark.parametrize("process_error", [False, True])
def test_runner_redacts_captured_submission_password(monkeypatch, process_error):
    import asyncio

    from takler.tasks.shell import shell_runner

    password = "FIXED_OLD_JOB_PASSWORD"
    logger = Mock()
    monkeypatch.setattr(shell_runner, "logger", logger)
    runner = ShellRunner(job_password=password)

    async def scenario():
        future = asyncio.get_running_loop().create_future()
        future.set_exception(
            CalledProcessError(3, f"submit {password}")
            if process_error
            else RuntimeError(f"failed using {password}")
        )
        runner._on_job_done(
            future, command=f"submit --pass={password}", node_path="/f/t"
        )

    asyncio.run(scenario())
    output = str(logger.mock_calls)
    assert password not in output
    assert "<redacted>" in output
    assert ("returncode=3" if process_error else "RuntimeError") in output


def test_render_and_callback_logs_do_not_disclose_job_password(tmp_path, monkeypatch):
    from takler.core import task_node
    from takler.tasks.shell import shell_script_task

    password = "FIXED_CURRENT_JOB_PASSWORD"
    monkeypatch.setattr(task_node.secrets, "token_urlsafe", lambda size: password)
    logger = Mock()
    monkeypatch.setattr(task_node, "logger", logger)
    monkeypatch.setattr(shell_script_task, "logger", logger)
    bunch = Bunch()
    flow = bunch.add_flow("f")
    script = tmp_path / "script"
    script.write_text("echo test\n")
    task = flow.add_task(ShellScriptTask("t", script))
    task.add_parameter("TAKLER_HOME", str(tmp_path))
    task.add_parameter("TAKLER_SHELL_JOB_CMD", "submit --pass={{ TAKLER_PASS }}")
    callbacks = []
    monkeypatch.setattr(
        ShellRunner, "spawn", lambda self, **kw: callbacks.append(kw["on_failure"])
    )
    flow.begin()
    task.run()
    callbacks[0](RuntimeError(f"wrapper received {password}"))
    assert task.state.node_status is NodeStatus.aborted
    assert password not in task.aborted_reason
    assert password not in str(logger.mock_calls)
    assert "<redacted>" in str(logger.mock_calls)


def test_spawn_failure_does_not_echo_password():
    from takler.exceptions import JobSubmissionError

    password = "FIXED_SPAWN_PASSWORD"
    runner = ShellRunner(job_password=password)
    with pytest.raises(JobSubmissionError) as caught:
        runner.spawn(command=f"submit --pass={password}")
    assert password not in str(caught.value)
    assert "<redacted>" in str(caught.value)
