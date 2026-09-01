"""
Unit tests for the one-time job password of a ``Task``.

Covers requirements 4.1, 4.2, 4.3, 4.4, 4.6, 4.7, 4.8 and 4.10 of the
``m2-security`` spec: generation on ``increment_try_no``, clearing on
``requeue``, the ``TAKLER_PASS`` generated parameter, the job script rendering
path, and the unchanged key set of ``Task.to_dict()``.

No test prints a job password or puts one in a test name or an assertion
message: passwords are only ever read inside an assertion expression.
"""

import json
import sys
from pathlib import Path

import pytest

from takler.core import Flow, Task
from takler.core.parameter import TAKLER_PASS
from takler.tasks import ShellScriptTask
from takler.tasks.shell.shell_render import ShellRender


# Requirement 4.2 demands a length of at least 32. The implementation uses
# ``secrets.token_urlsafe(32)``, which encodes 32 random bytes as base64url and
# therefore yields 43 characters, not 32. The tests assert the requirement's
# bound rather than the implementation's exact length, so switching to another
# cryptographically secure source of at least this strength stays green.
MIN_JOB_PASSWORD_LENGTH = 32

# The key set of ``Task.to_dict()`` as of M1, written out literally.
#
# ``show`` and the checkpoint file share the same ``Bunch.to_dict()``, so this
# key set is the only barrier that keeps every in-flight job password out of
# both. It is asserted against this literal set - not against a snapshot and
# not against a computed set - precisely so that adding any key to the node
# serialization fails here and has to be argued for.
M1_TASK_DICT_KEYS = {
    "aborted_reason",
    "class_type",
    "name",
    "state",
    "task_id",
    "try_no",
}


# Generation and lifecycle ------------------------------------------------


def test_new_task_has_empty_job_password():
    task = Task("task1")

    assert task.job_password is None
    assert task.try_no == 0


def test_increment_try_no_generates_job_password():
    """Requirements 4.1, 4.2."""
    task = Task("task1")

    task.increment_try_no()

    assert task.job_password
    assert len(task.job_password) >= MIN_JOB_PASSWORD_LENGTH
    assert task.try_no == 1


def test_increment_try_no_twice_generates_different_job_passwords():
    """Requirement 4.3."""
    task = Task("task1")

    task.increment_try_no()
    first = task.job_password
    task.increment_try_no()
    second = task.job_password

    assert first != second
    assert len(second) >= MIN_JOB_PASSWORD_LENGTH


def test_requeue_clears_job_password():
    """Requirement 4.4."""
    flow1 = Flow("flow1")
    task1 = flow1.add_task("task1")
    task1.increment_try_no()
    assert task1.job_password

    task1.requeue()

    assert task1.job_password is None
    assert task1.try_no == 0


# Serialization ------------------------------------------------------------


def test_to_dict_key_set_is_unchanged_by_job_password():
    """Requirement 4.10."""
    flow1 = Flow("flow1")
    task1 = flow1.add_task("task1")
    task1.increment_try_no()
    assert task1.job_password

    d = task1.to_dict()

    assert set(d.keys()) == M1_TASK_DICT_KEYS
    assert task1.job_password not in json.dumps(d)


# TAKLER_PASS generated parameter -----------------------------------------


def test_generated_parameters_only_contains_takler_pass():
    """Requirements 4.6, 4.7."""
    flow1 = Flow("flow1")
    task1 = flow1.add_task("task1")

    # ``increment_try_no`` also calls ``update_generated_parameters``.
    task1.increment_try_no()

    params = task1.generated_parameters_only()
    assert TAKLER_PASS in params
    assert params[TAKLER_PASS].value == task1.job_password
    assert params[TAKLER_PASS].value


def test_takler_pass_parameter_is_empty_after_requeue():
    """Requirement 4.7 with an empty job password."""
    flow1 = Flow("flow1")
    task1 = flow1.add_task("task1")
    task1.increment_try_no()

    task1.requeue()
    task1.update_generated_parameters()

    assert task1.generated_parameters_only()[TAKLER_PASS].value is None


def test_takler_pass_is_not_a_user_parameter():
    """Requirement 4.10: TAKLER_PASS must stay a generated parameter."""
    flow1 = Flow("flow1")
    task1 = flow1.add_task("task1")
    task1.increment_try_no()

    # ``Node.to_dict()`` serializes ``user_parameters`` only, so TAKLER_PASS
    # being absent from there is what keeps the password out of ``show`` and
    # the checkpoint while still reaching the job script.
    assert TAKLER_PASS not in task1.user_parameters_only()
    assert task1.find_parameter(TAKLER_PASS).value == task1.job_password


# Job script rendering ----------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="tests for linux only")
def test_job_script_render_uses_non_empty_takler_pass(tmp_path):
    """
    Requirement 4.8.

    ``increment_try_no`` runs from ``before_run``, i.e. before ``do_run``
    renders the script, so the password exists by rendering time. The job
    script is rendered through ``check_job_creation`` instead of a real
    submission.
    """
    script_path = Path(tmp_path, "task1.takler")
    script_path.write_text('export TAKLER_PASS="{{TAKLER_PASS}}"\necho done\n')

    flow1 = Flow("flow1")
    flow1.add_parameter("TAKLER_HOME", str(tmp_path))
    task1 = flow1.add_task(ShellScriptTask("task1", str(script_path)))

    task1.before_run()
    assert task1.job_password

    template_params = ShellRender(node=task1).template_params()
    assert template_params[TAKLER_PASS] == task1.job_password
    assert template_params[TAKLER_PASS]

    assert task1.check_job_creation()
    job_script_path = Path(tmp_path, "flow1", f"task1.job{task1.try_no}")
    assert task1.job_password in job_script_path.read_text()
