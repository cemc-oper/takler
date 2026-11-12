"""Unit tests for the Zombie_Policy disposition of a detected zombie.

Task 8.2 of the *m2-security* spec adds :func:`dispose_zombie` and
:meth:`ZombieDetector.guard` on top of the detection of task 8.1. This file
pins the three policies against the five node attributes requirements 10.2 and
10.3 name (status, ``task_id``, ``try_no``, ``aborted_reason`` and the
Job_Password), the adoption rules of ``adopt``, and the content of the WARNING
record.

Log assertions go through a captured console sink rather than ``caplog``: the
logging backend does not route records into pytest's handler (same approach as
``test_checkpoint_restore_unit.py``).

No test prints a password or puts one into a test name or an assertion message:
passwords are only ever read inside an assertion expression.

Validates: Requirements 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9, 10.11,
10.12
"""

from __future__ import annotations

import contextlib
import io

import pytest

import takler.logging
from takler.core import Flow
from takler.core.state import NodeStatus
from takler.exceptions import ZombieError
from takler.server.auth import CallCredentials
from takler.server.connect_config import AuthMode, ZombiePolicy
from takler.server.zombie import (
    ChildAction,
    ZombieCondition,
    ZombieDetector,
    dispose_zombie,
)

# A recognizable stand-in for the ``takler-pass`` of a stale job. Long enough to
# be an unlikely substring of any log format, so "the record does not contain
# the password" cannot pass by accident.
CALL_PASSWORD = "call-password-0123456789abcdef"

# The password the server records for the current run of the task.
NODE_PASSWORD = "node-password-0123456789abcdef"


@pytest.fixture
def task():
    """An active ``/flow1/task1`` holding a job id and a Job_Password.

    Active plus a recorded id is the state in which all three conditions are
    reachable, so one fixture serves every policy test.
    """
    flow1 = Flow("flow1")
    with flow1:
        flow1.add_task("task1")
    flow1.begin()

    task1 = flow1.find_node("/flow1/task1")
    task1.run()  # -> submitted, try_no 1, password generated
    task1.init(task_id="job-1")
    task1.job_password = NODE_PASSWORD
    return task1


def _snapshot(node):
    """The five attributes ``fail`` and ``fob`` must leave untouched."""
    return (
        node.state.node_status,
        node.task_id,
        node.try_no,
        node.aborted_reason,
        node.job_password,
    )


def _capturing_stderr(func):
    """Run ``func`` while capturing the console log output."""
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="DEBUG", console=True)
            result = func()
    finally:
        takler.logging.configure(console=True)
    return result, buffer.getvalue()


# fail ----------------------------------------------------------------------


def test_fail_raises_and_changes_nothing(task):
    """Requirement 10.2."""
    before = _snapshot(task)

    with pytest.raises(ZombieError):
        dispose_zombie(
            task,
            "complete",
            ZombieCondition.Z2,
            policy=ZombiePolicy.FAIL,
            credentials=CallCredentials(job_password=CALL_PASSWORD),
        )

    assert _snapshot(task) == before


def test_fail_message_names_the_condition_and_the_node(task):
    with pytest.raises(ZombieError) as excinfo:
        dispose_zombie(
            task,
            "complete",
            ZombieCondition.Z1,
            policy=ZombiePolicy.FAIL,
            credentials=CallCredentials(job_password=CALL_PASSWORD),
        )

    message = str(excinfo.value)
    assert "Z1" in message
    assert "/flow1/task1" in message
    assert "complete" in message
    assert CALL_PASSWORD not in message
    assert NODE_PASSWORD not in message


def test_fail_is_the_default_policy(task):
    """The safest of the three policies applies when none is given."""
    with pytest.raises(ZombieError):
        dispose_zombie(
            task,
            "complete",
            ZombieCondition.Z2,
            credentials=CallCredentials.empty(),
        )


# fob -----------------------------------------------------------------------


def test_fob_skips_and_changes_nothing(task):
    """Requirement 10.3."""
    before = _snapshot(task)

    action = dispose_zombie(
        task,
        "complete",
        ZombieCondition.Z2,
        policy=ZombiePolicy.FOB,
        credentials=CallCredentials(job_password=CALL_PASSWORD),
    )

    assert action is ChildAction.SKIP
    assert _snapshot(task) == before


# adopt ---------------------------------------------------------------------


def test_adopt_proceeds_and_takes_over_the_password(task):
    """Requirements 10.4, 10.5."""
    action = dispose_zombie(
        task,
        "complete",
        ZombieCondition.Z1,
        policy=ZombiePolicy.ADOPT,
        credentials=CallCredentials(job_password=CALL_PASSWORD),
    )

    assert action is ChildAction.PROCEED
    assert task.job_password == CALL_PASSWORD


@pytest.mark.parametrize("carried", [None, "", "   "])
def test_adopt_without_a_password_leaves_it_unchanged(task, carried):
    """Requirement 10.6."""
    action = dispose_zombie(
        task,
        "complete",
        ZombieCondition.Z2,
        policy=ZombiePolicy.ADOPT,
        credentials=CallCredentials(job_password=carried),
    )

    assert action is ChildAction.PROCEED
    assert task.job_password == NODE_PASSWORD


def test_adopt_of_a_z3_init_takes_over_the_task_id(task):
    """Requirement 10.7: the ``init`` itself records the new job id."""
    detector = ZombieDetector(
        auth_mode=AuthMode.DISABLED, zombie_policy=ZombiePolicy.ADOPT
    )

    action = detector.guard(
        task,
        "init",
        task_id="job-2",
        credentials=CallCredentials(job_password=CALL_PASSWORD),
    )
    assert action is ChildAction.PROCEED

    task.init(task_id="job-2")
    assert task.task_id == "job-2"
    assert task.job_password == CALL_PASSWORD


# guard ---------------------------------------------------------------------


def test_guard_proceeds_silently_when_no_condition_is_hit(task):
    """Requirements 10.11, 10.12."""
    detector = ZombieDetector(
        auth_mode=AuthMode.ENABLED, zombie_policy=ZombiePolicy.FAIL
    )
    before = _snapshot(task)

    action, captured = _capturing_stderr(
        lambda: detector.guard(
            task,
            "complete",
            credentials=CallCredentials(job_password=NODE_PASSWORD),
        )
    )

    assert action is ChildAction.PROCEED
    assert _snapshot(task) == before
    assert "zombie" not in captured


def test_guard_applies_the_policy_of_the_detector(task):
    detector = ZombieDetector(
        auth_mode=AuthMode.ENABLED, zombie_policy=ZombiePolicy.FOB
    )

    action = detector.guard(
        task,
        "complete",
        credentials=CallCredentials(job_password=CALL_PASSWORD),
    )

    assert action is ChildAction.SKIP


# logging -------------------------------------------------------------------


def test_disposition_logs_one_warning_without_any_password(task):
    """Requirements 10.8, 10.9."""
    detector = ZombieDetector(
        auth_mode=AuthMode.ENABLED, zombie_policy=ZombiePolicy.FOB
    )

    _, captured = _capturing_stderr(
        lambda: detector.guard(
            task,
            "complete",
            credentials=CallCredentials(job_password=CALL_PASSWORD),
        )
    )

    warnings = [line for line in captured.splitlines() if "WARNING" in line]
    assert len(warnings) == 1

    record = warnings[0]
    assert "/flow1/task1" in record  # node path
    assert "complete" in record  # command name
    assert "Z1" in record  # condition identifier
    assert ZombiePolicy.FOB.value in record  # policy in force
    assert NodeStatus.active.name in record  # current status
    assert CALL_PASSWORD not in record
    assert NODE_PASSWORD not in record
