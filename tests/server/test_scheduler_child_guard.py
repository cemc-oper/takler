"""The zombie guard inside the five Child_Commands of ``Scheduler``.

This module is about the *call sites*, not about the judgement itself: that the
guard is consulted once per Child_Command, after the node is located and
type-checked and before anything is written to the node (Requirement 9.1), that
``SKIP`` makes the command a no-op which still returns normally, that a missing
or non-task target keeps its M1 outcome without the guard being consulted at all
(Requirement 9.9), and that no Control_Command or Query_Command consults it
(Requirement 9.10).

The conditions and the policies themselves are covered by the zombie module's
own tests; the two end-to-end cases here only check that the scheduler and a
real :class:`ZombieDetector` fit together, including that a command which hits
nothing runs and leaves the Job_Password alone (Requirements 10.11, 10.12).

Validates: Requirements 9.1, 9.9, 9.10, 10.11, 10.12
"""

from __future__ import annotations

import asyncio
from typing import List, Optional, Tuple

import pytest

from takler.core import Bunch, Flow, NodeStatus
from takler.core.task_node import Task
from takler.exceptions import NodeNotFoundError, NodeTypeError, ZombieError
from takler.server.connect_config import ZombiePolicy
from takler.server.scheduler import Scheduler
from takler.server.zombie import ChildAction, ZombieDetector

TASK1 = "/flow1/container1/task1"
CONTAINER1 = "/flow1/container1"


class RecordingDetector:
    """A detector which records what it was asked and answers a fixed action.

    Stands in for :class:`ZombieDetector` at the scheduler's call sites: the
    scheduler only ever calls ``guard``, so recording that one method is enough
    to see whether a command consulted the guard, with which command name, and
    with which ``task_id``.
    """

    def __init__(self, action: ChildAction = ChildAction.PROCEED) -> None:
        self.action = action
        self.calls: List[Tuple[str, str, Optional[str]]] = []

    def guard(
        self,
        node: Task,
        command: str,
        task_id: Optional[str] = None,
        credentials=None,
    ) -> ChildAction:
        self.calls.append((node.node_path, command, task_id))
        return self.action

    @property
    def commands(self) -> List[str]:
        return [command for _, command, _ in self.calls]


def build_flow(name: str = "flow1") -> Flow:
    flow = Flow(name)
    container1 = flow.add_container("container1")
    container1.add_event("event_c")
    container1.add_meter("meter_c", 0, 100)
    task1 = container1.add_task("task1")
    task1.add_event("event1")
    task1.add_meter("meter1", 0, 100)
    container1.add_task("task2")
    flow.add_task("task3")
    return flow


def make_scheduler(detector=None) -> Scheduler:
    scheduler = Scheduler(bunch=Bunch(name="bunch"), zombie_detector=detector)
    scheduler.bunch.add_flow(build_flow("flow1"))
    return scheduler


def status_map(node) -> dict:
    """Status of the node and of all its descendants, keyed by node path."""
    result = {node.node_path: node.state.node_status}
    for child in node.children:
        result.update(status_map(child))
    return result


def run_child_command(scheduler: Scheduler, command: str, node_path: str = TASK1):
    """Issue one Child_Command by its short name, with a fixed payload."""
    if command == "init":
        return asyncio.run(scheduler.run_command_init(node_path, "job-42"))
    if command == "complete":
        return scheduler.run_command_complete(node_path)
    if command == "abort":
        return scheduler.run_command_abort(node_path, "boom")
    if command == "event":
        return scheduler.run_command_event(node_path, "event1")
    if command == "meter":
        return scheduler.run_command_meter(node_path, "meter1", "50")
    raise AssertionError(f"unknown child command: {command}")


CHILD_COMMANDS = ["init", "complete", "abort", "event", "meter"]


# the guard is consulted once per Child_Command ---------------------------


@pytest.mark.parametrize("command", CHILD_COMMANDS)
def test_every_child_command_consults_the_guard(command):
    detector = RecordingDetector(ChildAction.PROCEED)
    scheduler = make_scheduler(detector)

    run_child_command(scheduler, command)

    assert detector.calls == [(TASK1, command, "job-42" if command == "init" else None)]


def test_guard_runs_before_any_state_is_written():
    """The node the guard sees is still in its pre-command state."""
    seen: List[NodeStatus] = []
    scheduler = make_scheduler()
    task1 = scheduler.bunch.find_node(TASK1)

    class StatusRecordingDetector(RecordingDetector):
        def guard(self, node, command, task_id=None, credentials=None):
            seen.append(node.state.node_status)
            return super().guard(node, command, task_id, credentials)

    scheduler.zombie_detector = StatusRecordingDetector(ChildAction.PROCEED)
    status_before = task1.state.node_status
    assert status_before is not NodeStatus.complete

    run_child_command(scheduler, "complete")

    assert seen == [status_before]
    assert task1.state.node_status is NodeStatus.complete


# SKIP: no state change, no exception -------------------------------------


@pytest.mark.parametrize("command", CHILD_COMMANDS)
def test_skip_leaves_the_node_untouched(command):
    detector = RecordingDetector(ChildAction.SKIP)
    scheduler = make_scheduler(detector)
    flow = scheduler.bunch.find_flow("flow1")
    task1: Task = scheduler.bunch.find_node(TASK1)
    task1.job_password = "password-of-the-current-run"
    before = status_map(flow)

    # ``fob`` answers success: the command returns normally.
    assert run_child_command(scheduler, command) is None

    assert status_map(flow) == before
    assert task1.task_id is None
    assert task1.try_no == 0
    assert task1.aborted_reason is None
    assert task1.job_password == "password-of-the-current-run"
    assert task1.find_variable("event1").value is False
    assert task1.find_variable("meter1").value == 0


@pytest.mark.parametrize("command", CHILD_COMMANDS)
def test_proceed_runs_the_command(command):
    scheduler = make_scheduler(RecordingDetector(ChildAction.PROCEED))
    task1: Task = scheduler.bunch.find_node(TASK1)

    run_child_command(scheduler, command)

    if command == "init":
        assert task1.state.node_status is NodeStatus.active
        assert task1.task_id == "job-42"
    elif command == "complete":
        assert task1.state.node_status is NodeStatus.complete
    elif command == "abort":
        assert task1.state.node_status is NodeStatus.aborted
        assert task1.aborted_reason == "boom"
    elif command == "event":
        assert task1.find_variable("event1").value is True
    else:
        assert task1.find_variable("meter1").value == 50


# a missing or non-task target is not a zombie ---------------------------


@pytest.mark.parametrize("command", CHILD_COMMANDS)
def test_missing_node_raises_without_consulting_the_guard(command):
    detector = RecordingDetector(ChildAction.SKIP)
    scheduler = make_scheduler(detector)

    with pytest.raises(NodeNotFoundError):
        run_child_command(scheduler, command, "/flow1/container1/nope")

    assert detector.calls == []


@pytest.mark.parametrize("command", ["init", "complete", "abort"])
def test_non_task_node_raises_without_consulting_the_guard(command):
    detector = RecordingDetector(ChildAction.SKIP)
    scheduler = make_scheduler(detector)

    with pytest.raises(NodeTypeError):
        run_child_command(scheduler, command, CONTAINER1)

    assert detector.calls == []


@pytest.mark.parametrize(
    "command, variable, expected",
    [("event", "event_c", True), ("meter", "meter_c", 50)],
)
def test_event_and_meter_on_a_non_task_keep_m1_behaviour(command, variable, expected):
    """A container has no job instance, so it is judged by nothing."""
    detector = RecordingDetector(ChildAction.SKIP)
    scheduler = make_scheduler(detector)
    container1 = scheduler.bunch.find_node(CONTAINER1)

    if command == "event":
        scheduler.run_command_event(CONTAINER1, "event_c")
    else:
        scheduler.run_command_meter(CONTAINER1, "meter_c", "50")

    assert container1.find_variable(variable).value == expected
    assert detector.calls == []


# operator commands are never judged -------------------------------------


def test_control_and_query_commands_do_not_consult_the_guard():
    detector = RecordingDetector(ChildAction.SKIP)
    scheduler = make_scheduler(detector)

    scheduler.run_command_begin("flow1")
    scheduler.run_command_suspend(TASK1)
    scheduler.run_command_resume(TASK1)
    scheduler.run_command_run(TASK1, force=True)
    scheduler.run_command_force(f"{TASK1}:event1", "set")
    scheduler.run_command_force(TASK1, NodeStatus.complete.name)
    scheduler.run_command_free_dep(TASK1, "all")
    scheduler.run_command_requeue(TASK1)
    scheduler.handle_request_show(False, False, False, False, False)

    assert detector.calls == []


# no detector configured -------------------------------------------------


def test_without_a_detector_child_commands_keep_m1_behaviour():
    scheduler = make_scheduler()
    task1 = scheduler.bunch.find_node(TASK1)

    assert scheduler.zombie_detector is None
    assert scheduler._guard_child_command(task1, "complete") is ChildAction.PROCEED

    scheduler.run_command_complete(TASK1)
    assert task1.state.node_status is NodeStatus.complete


# with a real detector ---------------------------------------------------


def test_fail_policy_rejects_a_command_against_a_queued_task():
    """Requeued-then-reporting is the ``Z2`` case, and nothing is written."""
    scheduler = make_scheduler(
        ZombieDetector(zombie_policy=ZombiePolicy.FAIL),
    )
    flow = scheduler.bunch.find_flow("flow1")
    task1: Task = scheduler.bunch.find_node(TASK1)
    before = status_map(flow)

    with pytest.raises(ZombieError):
        scheduler.run_command_complete(TASK1)

    assert status_map(flow) == before
    assert task1.job_password is None


def test_a_command_of_the_current_run_proceeds_and_keeps_the_password():
    scheduler = make_scheduler(
        ZombieDetector(zombie_policy=ZombiePolicy.FAIL),
    )
    task1: Task = scheduler.bunch.find_node(TASK1)
    # The state a task is in while its job runs: active, holding the password of
    # the current try.
    task1.init("job-42")
    task1.job_password = "password-of-the-current-run"

    scheduler.run_command_complete(TASK1)

    assert task1.state.node_status is NodeStatus.complete
    assert task1.job_password == "password-of-the-current-run"
