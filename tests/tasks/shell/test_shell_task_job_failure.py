"""Unit tests for job submission failure visibility of ``ShellScriptTask``.

Covers Requirements 12.4, 12.8 and 12.9: a rendering failure surfaces as
``JobSubmissionError``, ``do_run`` logs it and aborts the task, and the
``on_failure`` callback handed to ``ShellRunner`` only aborts a task which is
still in flight.
"""

from pathlib import Path
import sys
from subprocess import CalledProcessError
from unittest import mock

import pytest

from takler.core import Bunch, Flow, NodeStatus
from takler.exceptions import JobSubmissionError
from takler.tasks import ShellScriptTask
from takler.tasks.shell import shell_script_task as shell_script_task_mod
from takler.tasks.shell.shell_runner import ShellRunner


pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="tests for linux only")


@pytest.fixture
def scripts_directory():
    return Path(Path(__file__).parent, "scripts")


def build_task(takler_home: Path, script_path) -> ShellScriptTask:
    with Bunch("test_bunch") as bunch:
        with Flow("flow1") as flow1:
            bunch.add_flow(flow1)
            with flow1.add_task(ShellScriptTask("task1", script_path)) as task1:
                task1.add_parameter("TAKLER_HOME", str(takler_home))
                task1.add_parameter("SLEEP", 1)
    return task1


def test_create_job_script_render_failure_raises_job_submission_error(tmp_path):
    """Requirement 12.9: rendering failure names the node path and the reason."""
    missing_script = Path(tmp_path, "scripts", "no_such_task.takler")
    task1 = build_task(tmp_path, str(missing_script))

    with pytest.raises(JobSubmissionError) as exc_info:
        task1.create_job_script()

    message = str(exc_info.value)
    assert "/flow1/task1" in message
    assert "no_such_task.takler" in message


def test_create_job_script_missing_script_param_raises_job_submission_error(tmp_path):
    """Requirement 12.9: an empty script parameter is a submission failure too."""
    task1 = build_task(tmp_path, None)

    with pytest.raises(JobSubmissionError) as exc_info:
        task1.create_job_script()

    assert "/flow1/task1" in str(exc_info.value)


def test_do_run_logs_error_and_aborts_on_job_submission_error(tmp_path):
    """Requirement 12.8: ``do_run`` logs ERROR with node path and aborts."""
    task1 = build_task(tmp_path, str(Path(tmp_path, "scripts", "missing.takler")))

    with mock.patch.object(shell_script_task_mod, "logger") as mock_logger:
        result = task1.do_run()
        messages = [call.args[0] for call in mock_logger.error.call_args_list]

    assert result is False
    assert len(messages) == 1
    assert "/flow1/task1" in messages[0]
    assert "missing.takler" in messages[0]

    assert task1.state.node_status == NodeStatus.aborted
    assert "JobSubmissionError" in task1.aborted_reason


def test_submit_passes_node_path_and_on_failure(tmp_path, scripts_directory):
    """Requirement 12.4: the spawn call carries the node path and the callback."""
    task1 = build_task(tmp_path, str(Path(scripts_directory, "task1.takler")))

    with mock.patch.object(ShellRunner, "spwan") as mock_spwan:
        assert task1.submit() is True

    assert mock_spwan.call_count == 1
    kwargs = mock_spwan.call_args.kwargs
    assert kwargs["node_path"] == "/flow1/task1"
    assert kwargs["on_failure"] == task1.on_job_failure
    assert kwargs["command"]


@pytest.mark.parametrize("status", [NodeStatus.submitted, NodeStatus.active])
def test_on_job_failure_aborts_in_flight_task(tmp_path, status):
    """Requirement 12.4: aborted_reason contains the exception type name."""
    task1 = build_task(tmp_path, None)
    task1.set_node_status(node_status=status)

    task1.on_job_failure(CalledProcessError(3, "/bin/sh"))

    assert task1.state.node_status == NodeStatus.aborted
    assert "CalledProcessError" in task1.aborted_reason


@pytest.mark.parametrize(
    "status", [NodeStatus.complete, NodeStatus.aborted, NodeStatus.queued, NodeStatus.unknown]
)
def test_on_job_failure_keeps_status_reported_by_child_command(tmp_path, status):
    """A task which already reported its own status must not be overwritten."""
    task1 = build_task(tmp_path, None)
    task1.set_node_status(node_status=status)
    task1.aborted_reason = None

    task1.on_job_failure(RuntimeError("boom"))

    assert task1.state.node_status == status
    assert task1.aborted_reason is None
