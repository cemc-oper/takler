import pytest

from takler.core import NodeStatus, State


def test_node_status_create():
    node_status = NodeStatus.complete
    assert node_status == NodeStatus.complete

    node_status = NodeStatus["complete"]
    assert node_status == NodeStatus.complete

    node_status = NodeStatus(2)
    assert node_status == NodeStatus.complete

    with pytest.raises(ValueError):
        node_status = NodeStatus(100)

    with pytest.raises(KeyError):
        node_status = NodeStatus["this_is_no_status"]


def test_node_status_order():
    assert NodeStatus.unknown < NodeStatus.complete
    assert NodeStatus.complete > NodeStatus.unknown

    assert NodeStatus.complete < NodeStatus.queued
    assert NodeStatus.queued > NodeStatus.complete

    assert NodeStatus.queued < NodeStatus.submitted

    assert NodeStatus.submitted < NodeStatus.active
    assert NodeStatus.complete < NodeStatus.active
    assert NodeStatus.active > NodeStatus.complete

    assert NodeStatus.active < NodeStatus.aborted


def test_state_create():
    node_state = State()
    assert node_state.node_status == NodeStatus.unknown
    assert node_state.suspended is False

    node_state = State(NodeStatus.complete)
    assert node_state.node_status == NodeStatus.complete
    assert node_state.suspended is False


def test_state_set():
    node_state = State(NodeStatus.complete)
    assert node_state.node_status == NodeStatus.complete
    assert node_state.suspended is False

    node_state.node_status = NodeStatus.queued
    node_state.suspended = True
    assert node_state.node_status == NodeStatus.queued
    assert node_state.suspended is True
