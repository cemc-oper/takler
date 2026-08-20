"""Unit tests for ``takler.tasks.shell.shell_runner.ShellRunner``.

There is no ``pytest-asyncio`` in this project (see ``pyproject.toml``), so the
loop is driven with :func:`asyncio.run`, mirroring the convention of
``tests/server/test_exception_resilience_bug_condition.py``.
"""

import asyncio
from subprocess import CalledProcessError
from unittest import mock

import pytest

from takler.exceptions import JobSubmissionError
from takler.tasks.shell import shell_runner as shell_runner_mod
from takler.tasks.shell.shell_runner import ShellRunner


def test_spwan_without_running_loop_raises_job_submission_error():
    """Requirement 12.7: a failed spawn surfaces as JobSubmissionError."""
    runner = ShellRunner()
    command = "/bin/true some-job"

    with pytest.raises(JobSubmissionError) as exc_info:
        runner.spwan(command=command)

    message = str(exc_info.value)
    assert command in message
    assert "no running event loop" in message


def test_spwan_keeps_reference_until_task_finishes():
    """Requirements 12.1, 12.2: the runner holds the task and hooks a callback."""
    runner = ShellRunner()

    async def scenario():
        task = runner.spwan(command="exit 0", node_path="/flow1/task1")
        # Reference held while the task is in flight.
        assert task in runner._job_tasks
        await asyncio.sleep(0)
        await task
        # Let the done callbacks run.
        await asyncio.sleep(0)
        return task

    task = asyncio.run(scenario())

    assert task.done()
    assert task not in runner._job_tasks
    assert task not in runner._job_context


def test_failed_job_logs_error_then_triggers_state_change():
    """Requirements 12.3, 12.5: log first, then trigger the state change."""
    runner = ShellRunner()
    events = []

    def on_failure(exc):
        events.append(("on_failure", type(exc).__name__))

    async def scenario():
        task = runner.spwan(
            command="exit 3",
            node_path="/flow1/task1",
            on_failure=on_failure,
        )
        with pytest.raises(CalledProcessError):
            await task
        await asyncio.sleep(0)

    with mock.patch.object(shell_runner_mod, "logger") as mock_logger:
        mock_logger.error.side_effect = lambda message: events.append(
            ("error", message)
        )
        asyncio.run(scenario())

    assert [kind for kind, _ in events] == ["error", "on_failure"]
    log_message = events[0][1]
    assert "/flow1/task1" in log_message
    assert "exit 3" in log_message
    assert "returncode=3" in log_message
    assert events[1][1] == "CalledProcessError"


def test_generic_exception_log_contains_type_and_description():
    """Requirement 12.3: non CalledProcessError failures name the type."""
    runner = ShellRunner()
    seen = []

    async def scenario():
        async def boom():
            raise PermissionError("permission denied")

        task = asyncio.get_running_loop().create_task(boom())
        runner._job_tasks.add(task)
        runner._job_context[task] = ("submit.sh", "/flow1/task2", seen.append)
        task.add_done_callback(runner._on_job_done)
        with pytest.raises(PermissionError):
            await task
        await asyncio.sleep(0)

    with mock.patch.object(shell_runner_mod, "logger") as mock_logger:
        asyncio.run(scenario())
        messages = [call.args[0] for call in mock_logger.error.call_args_list]

    assert len(messages) == 1
    assert "/flow1/task2" in messages[0]
    assert "submit.sh" in messages[0]
    assert "PermissionError" in messages[0]
    assert "permission denied" in messages[0]
    assert [type(exc).__name__ for exc in seen] == ["PermissionError"]


def test_cancelled_job_does_not_log_or_trigger_state_change():
    runner = ShellRunner()
    calls = []

    async def scenario():
        task = runner.spwan(
            command="sleep 30",
            node_path="/flow1/task3",
            on_failure=calls.append,
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)

    with mock.patch.object(shell_runner_mod, "logger") as mock_logger:
        asyncio.run(scenario())
        assert mock_logger.error.call_count == 0

    assert calls == []


def test_spwan_v2_is_removed():
    """Requirement 12.6: only one public spawn path remains."""
    assert not hasattr(ShellRunner, "spwan_v2")
