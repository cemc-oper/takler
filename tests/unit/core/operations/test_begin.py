import pytest

from takler.core import NodeStatus
from takler.exceptions import FlowStateError


def test_begin_starts_calendar_and_resets_node_tree(simple_flow_for_operation):
    """Requirements 8.2, 8.3: begin starts the calendar, resets the tree, sets ``begun``."""
    flow1 = simple_flow_for_operation.flow1

    assert flow1.begun is False
    assert flow1.calendar.initial_time is None

    flow1.begin()

    assert flow1.begun is True
    assert flow1.calendar.initial_time is not None
    for _, node in vars(simple_flow_for_operation).items():
        assert node.state.node_status == NodeStatus.queued


def test_begin_twice_raises_flow_state_error(simple_flow_for_operation):
    """Requirement 8.11: beginning an already begun flow without force fails."""
    flow1 = simple_flow_for_operation.flow1
    flow1.begin()

    task1 = simple_flow_for_operation.task1
    initial_time = flow1.calendar.initial_time
    task1.init("job-1")
    status_before = task1.state.node_status

    with pytest.raises(FlowStateError) as exc_info:
        flow1.begin()

    assert flow1.name in str(exc_info.value)
    assert exc_info.value.flow_name == flow1.name
    assert flow1.calendar.initial_time == initial_time
    assert task1.state.node_status == status_before


def test_begin_with_force_restarts_calendar(simple_flow_for_operation):
    """Requirement 8.12: force begin restarts the calendar and resets the tree."""
    flow1 = simple_flow_for_operation.flow1
    task1 = simple_flow_for_operation.task1
    flow1.begin()
    first_initial_time = flow1.calendar.initial_time

    task1.init("job-1")
    assert task1.state.node_status == NodeStatus.active

    flow1.begin(force=True)

    assert flow1.begun is True
    assert flow1.calendar.initial_time >= first_initial_time
    assert task1.state.node_status == NodeStatus.queued


def test_requeue_does_not_touch_calendar(simple_flow_for_operation):
    """Requirements 8.4, 8.5: requeue leaves every calendar field unchanged."""
    flow1 = simple_flow_for_operation.flow1
    flow1.begin()

    before = (
        flow1.calendar.initial_time,
        flow1.calendar.flow_time,
        flow1.calendar.duration,
        flow1.calendar.increment,
        flow1.calendar.initial_real_time,
        flow1.calendar.last_real_time,
    )

    flow1.requeue()

    after = (
        flow1.calendar.initial_time,
        flow1.calendar.flow_time,
        flow1.calendar.duration,
        flow1.calendar.increment,
        flow1.calendar.initial_real_time,
        flow1.calendar.last_real_time,
    )
    assert after == before


def test_requeue_on_not_begun_flow_keeps_calendar_unstarted(simple_flow_for_operation):
    """Requirements 8.5, 8.6: only begin may start the calendar, invariant holds."""
    flow1 = simple_flow_for_operation.flow1

    flow1.requeue()

    assert flow1.calendar.initial_time is None
    assert flow1.begun is False
