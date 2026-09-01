"""Unit tests for the Zombie_Detector itself.

This file is the detector's own level: the three Zombie_Conditions as
predicates, the order they are evaluated in, and what each Zombie_Policy makes
of a hit expressed as the ``flag`` the client will see. The two neighbouring
files stay at their own level and are not duplicated here:
``test_scheduler_child_guard.py`` is about the call sites inside the
``Scheduler``, and ``test_zombie_after_requeue.py`` is the end-to-end
requeue scenario.

The ``flag`` assertions go through ``_command_error_response``, the same
function ``TaklerService._handle_command`` uses, rather than through a literal
31: the requirement is about what the client receives, and reading the mapping
from the boundary keeps the test honest if the code ever moves.

Log assertions read a captured console sink rather than ``caplog``, because the
logging backend does not route records into pytest's handler.

No test prints a password, puts one into a test name or into an assertion
message: passwords are only ever read inside an assertion expression or handed
to the code under test.

Requirements 9.9 and 9.10 -- a missing or non-task target keeps its M1
exception, and no Operator_Command is judged -- live at the call sites and are
asserted in ``test_scheduler_child_guard.py``.

Validates: Requirements 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8, 10.2, 10.3, 10.4,
10.5, 10.6, 10.7, 10.8, 10.9, 10.11, 10.12, 16.5
"""

from __future__ import annotations

import asyncio
import contextlib
import io
from typing import Optional

import pytest

import takler.logging
from takler.core import Bunch, Flow
from takler.core.state import NodeStatus
from takler.core.task_node import Task
from takler.exceptions import ZombieError
from takler.server.auth import (
    CallCredentials,
    reset_call_credentials,
    set_call_credentials,
)
from takler.server.connect_config import AuthMode, ZombiePolicy
from takler.server.network_service import _command_error_response
from takler.server.protocol import error_code
from takler.server.scheduler import Scheduler
from takler.server.zombie import (
    IN_FLIGHT_STATUSES,
    ChildAction,
    ZombieCondition,
    ZombieDetector,
    detect_zombie_condition,
    hits_z1,
    hits_z2,
    hits_z3,
)

TASK_PATH = "/flow1/task1"

#: The ``takler-pass`` of a stale job, and the Job_Password the server records
#: for the current run. Both are long and recognizable, so "the text does not
#: contain the password" cannot pass by accident.
CALL_PASSWORD = "call-password-0123456789abcdef"
NODE_PASSWORD = "node-password-0123456789abcdef"

CHILD_COMMANDS = ["init", "complete", "abort", "event", "meter"]

#: Every status a task can be in which is not in flight, i.e. the whole of
#: ``Z2`` (Requirement 9.5).
OUT_OF_FLIGHT_STATUSES = [
    NodeStatus.unknown,
    NodeStatus.queued,
    NodeStatus.complete,
    NodeStatus.aborted,
]


def make_task(
    status: NodeStatus = NodeStatus.active,
    job_password: Optional[str] = NODE_PASSWORD,
    task_id: Optional[str] = "job-1",
) -> Task:
    """A ``/flow1/task1`` put into the state a test needs.

    The status is set directly rather than driven through ``run`` / ``init``,
    because ``Z2`` is about statuses no sequence of job operations produces
    while a job is running -- ``complete`` and ``aborted`` in particular.
    """
    flow1 = Flow("flow1")
    with flow1:
        flow1.add_task("task1")
    flow1.begin()

    task1: Task = flow1.find_node(TASK_PATH)
    task1.set_node_status(node_status=status)
    task1.task_id = task_id
    task1.job_password = job_password
    return task1


def snapshot(node: Task):
    """The five attributes ``fail`` and ``fob`` must leave untouched."""
    return (
        node.state.node_status,
        node.task_id,
        node.try_no,
        node.aborted_reason,
        node.job_password,
    )


def capturing_stderr(func):
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


def guarded_flag(detector: ZombieDetector, node: Task, command: str, **kwargs):
    """The ``flag`` and the :class:`ChildAction` one guarded command produces.

    ``fail`` leaves the guard by raising, and the RPC boundary turns that
    exception into the response, so the flag of that case is read from the same
    mapping the boundary uses. ``fob`` and ``adopt`` return, and the handler
    then builds its ordinary success response.
    """
    try:
        action = detector.guard(node, command, **kwargs)
    except ZombieError as exc:
        return _command_error_response(exc).flag, None
    return error_code.SUCCESS, action


# ---------------------------------------------------------------------------
# Z1: the credentials do not belong to the current run
# ---------------------------------------------------------------------------


def test_z1_hits_when_the_call_carries_another_password():
    """Requirement 9.2."""
    task = make_task()

    assert hits_z1(task, CallCredentials(job_password=CALL_PASSWORD)) is True


def test_z1_misses_when_the_call_carries_the_recorded_password():
    task = make_task()

    assert hits_z1(task, CallCredentials(job_password=NODE_PASSWORD)) is False


def test_z1_hits_when_the_task_holds_no_password():
    """Requirement 9.3: a requeued task has nothing a job could present."""
    task = make_task(job_password=None)

    assert hits_z1(task, CallCredentials(job_password=CALL_PASSWORD)) is True


@pytest.mark.parametrize("carried", [None, ""])
def test_z1_hits_when_the_call_carries_no_password(carried):
    task = make_task()

    assert hits_z1(task, CallCredentials(job_password=carried)) is True


def test_z1_hits_when_neither_side_has_a_password():
    """Two absent passwords are not a match: nothing authenticates nothing."""
    task = make_task(job_password=None)

    assert hits_z1(task, CallCredentials.empty()) is True


def test_z1_of_a_non_ascii_password_is_an_ordinary_mismatch():
    """A ``takler-pass`` off the wire may hold anything; it must not raise."""
    task = make_task()

    assert hits_z1(task, CallCredentials(job_password="口令\ud800")) is True


def test_z1_is_evaluated_when_auth_is_enabled():
    task = make_task()

    condition = detect_zombie_condition(
        task,
        "complete",
        auth_mode=AuthMode.ENABLED,
        credentials=CallCredentials(job_password=CALL_PASSWORD),
    )

    assert condition is ZombieCondition.Z1


def test_z1_is_skipped_when_auth_is_disabled():
    """Requirement 9.4: without authentication no client sends a password."""
    task = make_task()

    condition = detect_zombie_condition(
        task,
        "complete",
        auth_mode=AuthMode.DISABLED,
        credentials=CallCredentials(job_password=CALL_PASSWORD),
    )

    assert condition is None


# ---------------------------------------------------------------------------
# Z2: the task is in no state to be reported on
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", OUT_OF_FLIGHT_STATUSES, ids=lambda s: s.name)
def test_z2_hits_outside_the_in_flight_statuses(status):
    """Requirement 9.5."""
    task = make_task(status=status)

    assert hits_z2(task) is True


@pytest.mark.parametrize("status", IN_FLIGHT_STATUSES, ids=lambda s: s.name)
def test_z2_misses_while_the_task_is_in_flight(status):
    task = make_task(status=status)

    assert hits_z2(task) is False


@pytest.mark.parametrize("command", CHILD_COMMANDS)
def test_z2_is_reported_for_every_child_command(command):
    """No Child_Command is expected while the task is queued."""
    task = make_task(status=NodeStatus.queued, job_password=None)

    condition = detect_zombie_condition(
        task,
        command,
        task_id="job-1",
        auth_mode=AuthMode.DISABLED,
        credentials=CallCredentials.empty(),
    )

    assert condition is ZombieCondition.Z2


# ---------------------------------------------------------------------------
# Z3: a second job claims a task that is already running
# ---------------------------------------------------------------------------


def test_z3_hits_when_an_init_names_another_job_id():
    """Requirement 9.6."""
    task = make_task(task_id="job-1")

    assert hits_z3(task, "init", "job-2") is True


def test_z3_misses_when_the_init_names_the_recorded_job_id():
    task = make_task(task_id="job-1")

    assert hits_z3(task, "init", "job-1") is False


@pytest.mark.parametrize("recorded", [None, ""])
@pytest.mark.parametrize("carried", [None, ""])
def test_z3_treats_a_blank_job_id_as_an_absent_one(recorded, carried):
    task = make_task(task_id=recorded)

    assert hits_z3(task, "init", carried) is False


@pytest.mark.parametrize("command", ["complete", "abort", "event", "meter"])
def test_z3_applies_to_init_only(command):
    """The other four Child_Commands carry no job id at all."""
    task = make_task(task_id="job-1")

    assert hits_z3(task, command, "job-2") is False


@pytest.mark.parametrize("status", [NodeStatus.submitted] + OUT_OF_FLIGHT_STATUSES)
def test_z3_misses_unless_the_task_is_active(status):
    """Before ``init`` there is no recorded id for a second one to contradict."""
    task = make_task(status=status, task_id="job-1")

    assert hits_z3(task, "init", "job-2") is False


def test_z3_is_reported_by_the_detection_of_an_init():
    task = make_task()

    condition = detect_zombie_condition(
        task,
        "init",
        "job-2",
        auth_mode=AuthMode.ENABLED,
        credentials=CallCredentials(job_password=NODE_PASSWORD),
    )

    assert condition is ZombieCondition.Z3


def test_a_command_of_the_current_run_hits_nothing():
    """The control case: without it every assertion above could pass on a
    detector that reports a condition unconditionally."""
    task = make_task()

    condition = detect_zombie_condition(
        task,
        "init",
        "job-1",
        auth_mode=AuthMode.ENABLED,
        credentials=CallCredentials(job_password=NODE_PASSWORD),
    )

    assert condition is None


# ---------------------------------------------------------------------------
# The order of evaluation (Requirement 9.7)
# ---------------------------------------------------------------------------


def test_z1_is_reported_before_z2():
    """A requeued task hits both, and the more specific diagnosis wins."""
    task = make_task(status=NodeStatus.queued, job_password=None)
    credentials = CallCredentials(job_password=CALL_PASSWORD)

    assert hits_z1(task, credentials) is True
    assert hits_z2(task) is True

    condition = detect_zombie_condition(
        task,
        "complete",
        auth_mode=AuthMode.ENABLED,
        credentials=credentials,
    )

    assert condition is ZombieCondition.Z1


def test_z2_is_reported_when_z1_is_not_evaluated():
    """The same task under ``Auth_Mode=disabled`` falls through to ``Z2``."""
    task = make_task(status=NodeStatus.queued, job_password=None)

    condition = detect_zombie_condition(
        task,
        "complete",
        auth_mode=AuthMode.DISABLED,
        credentials=CallCredentials(job_password=CALL_PASSWORD),
    )

    assert condition is ZombieCondition.Z2


def test_z1_is_reported_before_z3():
    """An ``init`` of a stale job hits both; ``Z1`` names the run instance."""
    task = make_task(task_id="job-1")
    credentials = CallCredentials(job_password=CALL_PASSWORD)

    assert hits_z1(task, credentials) is True
    assert hits_z3(task, "init", "job-2") is True

    condition = detect_zombie_condition(
        task,
        "init",
        "job-2",
        auth_mode=AuthMode.ENABLED,
        credentials=credentials,
    )

    assert condition is ZombieCondition.Z1


# ---------------------------------------------------------------------------
# The three policies: flag, node state and Job_Password (Requirement 16.5)
# ---------------------------------------------------------------------------


def test_fail_answers_flag_31_and_writes_nothing():
    """Requirement 10.2."""
    task = make_task(status=NodeStatus.queued)
    before = snapshot(task)
    detector = ZombieDetector(
        auth_mode=AuthMode.ENABLED, zombie_policy=ZombiePolicy.FAIL
    )

    flag, action = guarded_flag(
        detector,
        task,
        "complete",
        credentials=CallCredentials(job_password=CALL_PASSWORD),
    )

    assert flag == 31
    assert error_code.error_name_for_code(flag) == "zombie"
    assert action is None
    assert snapshot(task) == before


def test_fob_answers_flag_0_and_writes_nothing():
    """Requirement 10.3."""
    task = make_task(status=NodeStatus.queued)
    before = snapshot(task)
    detector = ZombieDetector(
        auth_mode=AuthMode.ENABLED, zombie_policy=ZombiePolicy.FOB
    )

    flag, action = guarded_flag(
        detector,
        task,
        "complete",
        credentials=CallCredentials(job_password=CALL_PASSWORD),
    )

    assert flag == error_code.SUCCESS
    assert action is ChildAction.SKIP
    assert snapshot(task) == before


def test_adopt_answers_flag_0_and_takes_over_the_password():
    """Requirements 10.4, 10.5."""
    task = make_task(status=NodeStatus.queued)
    detector = ZombieDetector(
        auth_mode=AuthMode.ENABLED, zombie_policy=ZombiePolicy.ADOPT
    )

    flag, action = guarded_flag(
        detector,
        task,
        "complete",
        credentials=CallCredentials(job_password=CALL_PASSWORD),
    )

    assert flag == error_code.SUCCESS
    assert action is ChildAction.PROCEED
    assert task.job_password == CALL_PASSWORD


def test_adopt_without_a_carried_password_keeps_the_recorded_one():
    """Requirement 10.6."""
    task = make_task(status=NodeStatus.queued)
    detector = ZombieDetector(
        auth_mode=AuthMode.DISABLED, zombie_policy=ZombiePolicy.ADOPT
    )

    flag, action = guarded_flag(
        detector, task, "complete", credentials=CallCredentials.empty()
    )

    assert flag == error_code.SUCCESS
    assert action is ChildAction.PROCEED
    assert task.job_password == NODE_PASSWORD


# ---------------------------------------------------------------------------
# adopt of a ``Z3`` init, through the Scheduler
# ---------------------------------------------------------------------------


def make_scheduler(detector: Optional[ZombieDetector]) -> Scheduler:
    """A scheduler over one begun flow holding ``/flow1/task1``."""
    scheduler = Scheduler(bunch=Bunch(name="bunch"), zombie_detector=detector)
    flow1 = Flow("flow1")
    with flow1:
        flow1.add_task("task1")
    flow1.begin()
    scheduler.bunch.add_flow(flow1)
    return scheduler


@contextlib.contextmanager
def call_credentials(credentials: CallCredentials):
    """Publish ``credentials`` for the duration of the block.

    The scheduler reads the Credential_Metadata of the call from the ContextVar
    the Auth_Interceptor writes, so a test driving it directly has to put them
    there itself.
    """
    token = set_call_credentials(credentials)
    try:
        yield
    finally:
        reset_call_credentials(token)


def test_adopt_of_a_z3_init_adopts_the_job_id_and_the_password():
    """Requirement 10.7: the ``init`` runs, so it records the new job id."""
    # ``Auth_Mode=disabled`` is what isolates ``Z3`` here: with authentication
    # on, the stale password of the second job would hit ``Z1`` first, which is
    # the order test above.
    scheduler = make_scheduler(
        ZombieDetector(auth_mode=AuthMode.DISABLED, zombie_policy=ZombiePolicy.ADOPT)
    )
    task: Task = scheduler.bunch.find_node(TASK_PATH)
    task.run()
    task.init(task_id="job-1")
    task.job_password = NODE_PASSWORD

    with call_credentials(CallCredentials(job_password=CALL_PASSWORD)):
        assert (
            scheduler.zombie_detector.detect(task, "init", "job-2")
            is ZombieCondition.Z3
        )
        asyncio.run(scheduler.run_command_init(TASK_PATH, "job-2"))

    assert task.task_id == "job-2"
    assert task.job_password == CALL_PASSWORD
    assert task.state.node_status is NodeStatus.active


# ---------------------------------------------------------------------------
# No condition hit: the command runs and nothing extra happens
# ---------------------------------------------------------------------------


def test_a_command_of_the_current_run_executes_and_keeps_the_password():
    """Requirements 10.11, 10.12."""
    scheduler = make_scheduler(
        ZombieDetector(auth_mode=AuthMode.ENABLED, zombie_policy=ZombiePolicy.FAIL)
    )
    task: Task = scheduler.bunch.find_node(TASK_PATH)
    task.run()
    task.init(task_id="job-1")
    task.job_password = NODE_PASSWORD

    with call_credentials(CallCredentials(job_password=NODE_PASSWORD)):
        # No exception: the RPC boundary answers a returning handler ``flag=0``.
        scheduler.run_command_complete(TASK_PATH)

    assert task.state.node_status is NodeStatus.complete
    assert task.job_password == NODE_PASSWORD


# ---------------------------------------------------------------------------
# The WARNING of a disposition carries no password (Requirements 10.8, 10.9)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "policy", [ZombiePolicy.FAIL, ZombiePolicy.FOB, ZombiePolicy.ADOPT]
)
def test_the_disposition_warning_never_carries_a_password(policy):
    task = make_task(status=NodeStatus.queued)
    detector = ZombieDetector(auth_mode=AuthMode.ENABLED, zombie_policy=policy)

    _, captured = capturing_stderr(
        lambda: guarded_flag(
            detector,
            task,
            "complete",
            credentials=CallCredentials(job_password=CALL_PASSWORD),
        )
    )

    warnings = [line for line in captured.splitlines() if "WARNING" in line]
    assert len(warnings) == 1
    assert TASK_PATH in warnings[0]
    assert policy.value in warnings[0]
    assert CALL_PASSWORD not in captured
    assert NODE_PASSWORD not in captured
