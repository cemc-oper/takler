"""Unit tests for the ``Scheduler._require_begun`` guard.

Covers Requirement 8.10: requeue / run / force / free-dep on a node of a flow
which has not begun raise a ``TaklerError`` subclass whose ``str()`` contains
the flow name, and leave the node and all its descendants unchanged.
"""
import pytest

from takler.core import Bunch, Flow, NodeContainer, NodeStatus
from takler.exceptions import FlowStateError, TaklerError
from takler.server.scheduler import Scheduler


def build_flow(name: str = "flow1") -> Flow:
    flow = Flow(name)
    container1 = flow.add_container("container1")
    task1 = container1.add_task("task1")
    task1.add_event("event1")
    task1.add_meter("meter1", 0, 100)
    container1.add_task("task2")
    flow.add_task("task3")
    return flow


@pytest.fixture
def scheduler() -> Scheduler:
    return Scheduler(bunch=Bunch(name="bunch"))


@pytest.fixture
def flow(scheduler) -> Flow:
    return scheduler.bunch.add_flow(build_flow("flow1"))


def status_map(node) -> dict:
    """Status of the node and of all its descendants, keyed by node path."""
    result = {node.node_path: node.state.node_status}
    for child in node.children:
        result.update(status_map(child))
    return result


# guarded commands ------------------------------------------------------


@pytest.mark.parametrize(
    "node_path", ["/flow1", "/flow1/container1", "/flow1/container1/task1"]
)
def test_requeue_rejected_on_not_begun_flow(scheduler, flow, node_path):
    node = scheduler.bunch.find_node(node_path)
    before = status_map(node)

    with pytest.raises(FlowStateError) as exc_info:
        scheduler.run_command_requeue(node_path)

    assert isinstance(exc_info.value, TaklerError)
    assert "flow1" in str(exc_info.value)
    assert exc_info.value.flow_name == "flow1"
    assert status_map(node) == before


def test_run_rejected_on_not_begun_flow(scheduler, flow):
    task1 = scheduler.bunch.find_node("/flow1/container1/task1")
    before = status_map(task1)

    with pytest.raises(FlowStateError) as exc_info:
        scheduler.run_command_run("/flow1/container1/task1")

    assert "flow1" in str(exc_info.value)
    assert status_map(task1) == before


def test_run_force_rejected_on_not_begun_flow(scheduler, flow):
    """Even ``run --force`` is guarded: the flow gate comes first."""
    with pytest.raises(FlowStateError):
        scheduler.run_command_run("/flow1/container1/task1", force=True)


def test_run_on_non_task_rejected_before_type_check(scheduler, flow):
    """The guard runs before the "not a Task" branch."""
    with pytest.raises(FlowStateError):
        scheduler.run_command_run("/flow1/container1")


@pytest.mark.parametrize("recursive", [False, True])
def test_force_node_rejected_on_not_begun_flow(scheduler, flow, recursive):
    container1 = scheduler.bunch.find_node("/flow1/container1")
    before = status_map(container1)

    with pytest.raises(FlowStateError) as exc_info:
        scheduler.run_command_force(
            "/flow1/container1", NodeStatus.complete.name, recursive=recursive
        )

    assert "flow1" in str(exc_info.value)
    assert status_map(container1) == before


def test_force_event_rejected_on_not_begun_flow(scheduler, flow):
    """A variable path is guarded through its host node, and the event is untouched."""
    task1 = scheduler.bunch.find_node("/flow1/container1/task1")
    event1 = task1.find_variable("event1")

    with pytest.raises(FlowStateError) as exc_info:
        scheduler.run_command_force("/flow1/container1/task1:event1", "set")

    assert "flow1" in str(exc_info.value)
    assert event1.value is False


def test_force_meter_rejected_on_not_begun_flow(scheduler, flow):
    task1 = scheduler.bunch.find_node("/flow1/container1/task1")
    meter1 = task1.find_variable("meter1")
    value_before = meter1.value

    with pytest.raises(FlowStateError):
        scheduler.run_command_force("/flow1/container1/task1:meter1", "set")

    assert meter1.value == value_before


def test_force_invalid_state_still_rejected_by_guard(scheduler, flow):
    """The flow gate is reported before the unsupported state value."""
    with pytest.raises(FlowStateError):
        scheduler.run_command_force("/flow1/container1", "no_such_status")


@pytest.mark.parametrize("dep_type", ["all", "trigger", "time"])
def test_free_dep_rejected_on_not_begun_flow(scheduler, flow, dep_type):
    task1 = scheduler.bunch.find_node("/flow1/container1/task1")
    before = status_map(task1)

    with pytest.raises(FlowStateError) as exc_info:
        scheduler.run_command_free_dep("/flow1/container1/task1", dep_type)

    assert "flow1" in str(exc_info.value)
    assert status_map(task1) == before


# guarded commands pass once the flow has begun --------------------------


def test_guarded_commands_work_after_begin(scheduler, flow):
    scheduler.run_command_begin("flow1")

    scheduler.run_command_requeue("/flow1/container1")
    scheduler.run_command_free_dep("/flow1/container1/task1", "all")
    scheduler.run_command_force("/flow1/container1/task1:event1", "set")
    assert scheduler.run_command_run("/flow1/container1/task1") is True
    scheduler.run_command_force("/flow1/container1/task2", NodeStatus.complete.name)

    task1 = scheduler.bunch.find_node("/flow1/container1/task1")
    assert task1.find_variable("event1").value is True
    task2 = scheduler.bunch.find_node("/flow1/container1/task2")
    assert task2.state.node_status is NodeStatus.complete


# unguarded commands ----------------------------------------------------


def test_suspend_and_resume_not_guarded(scheduler, flow):
    """"suspend then begin" is a legitimate operator order, so it is not guarded."""
    scheduler.run_command_suspend("/flow1")
    assert flow.state.suspended is True

    scheduler.run_command_resume("/flow1")
    assert flow.state.suspended is False


def test_child_commands_not_guarded(scheduler, flow):
    """Child commands stay accepted: zombie semantics belong to M2."""
    scheduler.run_command_complete("/flow1/container1/task1")
    task1 = scheduler.bunch.find_node("/flow1/container1/task1")
    assert task1.state.node_status is NodeStatus.complete

    scheduler.run_command_abort("/flow1/container1/task2", "some reason")
    task2 = scheduler.bunch.find_node("/flow1/container1/task2")
    assert task2.state.node_status is NodeStatus.aborted

    scheduler.run_command_event("/flow1/container1/task1", "event1")
    assert task1.find_variable("event1").value is True

    scheduler.run_command_meter("/flow1/container1/task1", "meter1", "50")
    assert task1.find_variable("meter1").value == 50


# nodes outside of a flow ----------------------------------------------


def test_bare_node_tree_is_not_guarded():
    """``get_flow()`` returning ``None`` means no guard (Requirement 8.10 scope)."""
    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    container1 = NodeContainer("container1")
    task1 = container1.add_task("task1")

    scheduler._require_begun(container1)
    scheduler._require_begun(task1)


def test_require_begun_accepts_none(scheduler):
    """``None`` is a no-op so callers may pass an optional lookup result."""
    scheduler._require_begun(None)
