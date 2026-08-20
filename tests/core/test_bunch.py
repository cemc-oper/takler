import pytest

from takler.core import Bunch, NodeStatus
from takler.exceptions import InvalidNodePathError, NodeNotFoundError


@pytest.fixture
def simple_bunch(simple_flow, simple_flow_2):
    bunch = Bunch()
    flow1 = simple_flow.flow1
    bunch.add_flow(flow1)
    flow2 = simple_flow_2.flow2
    bunch.add_flow(flow2)
    return bunch


def test_bunch_add_flow(simple_flow, simple_flow_2):
    bunch = Bunch()
    flow1 = simple_flow.flow1
    bunch.add_flow(flow1)
    assert bunch.flows == {"flow1": flow1}

    flow2 = simple_flow_2.flow2
    bunch.add_flow(flow2)
    assert bunch.flows == {
        "flow1": flow1,
        "flow2": flow2,
    }


def test_bunch_find_flow(simple_bunch, simple_flow, simple_flow_2):
    bunch = simple_bunch
    flow1 = simple_flow.flow1
    flow2 = simple_flow_2.flow2

    f = bunch.find_flow("flow1")
    assert f == flow1

    f = bunch.find_flow("flow2")
    assert f == flow2

    f = bunch.find_flow("not_exist_flow")
    assert f is None


def test_bunch_find_node(simple_bunch, simple_flow, simple_flow_2):
    bunch = simple_bunch

    flow1_task3 = simple_flow.task3
    node = bunch.find_node("/flow1/container1/container2/task3")
    assert node == flow1_task3

    node = bunch.find_node("/flow1/not_exist_container/not_exist_task")
    assert node is None


def test_bunch_delete_flow(simple_bunch, simple_flow, simple_flow_2):
    bunch = simple_bunch
    flow1 = simple_flow.flow1
    flow2 = simple_flow_2.flow2

    f = bunch.delete_flow("flow1")
    assert f == flow1
    assert bunch.flows == {"flow2": flow2}


def test_bunch_find_node_with_relative_path(simple_bunch):
    """A path which does not start with "/" is malformed."""
    bunch = simple_bunch
    relative_path = "flow1/container1/task2"

    with pytest.raises(InvalidNodePathError) as exc_info:
        bunch.find_node(relative_path)

    assert exc_info.value.node_path == relative_path
    assert relative_path in str(exc_info.value)
    # transitional ValueError compatibility
    assert isinstance(exc_info.value, ValueError)


def test_bunch_delete_flow_not_exist(simple_bunch):
    bunch = simple_bunch

    with pytest.raises(NodeNotFoundError) as exc_info:
        bunch.delete_flow("not_exist_flow")

    assert exc_info.value.node_path == "/not_exist_flow"
    assert "not_exist_flow" in str(exc_info.value)
    assert isinstance(exc_info.value, ValueError)


def test_node_get_bunch(simple_bunch, simple_flow, simple_flow_2):
    bunch = simple_bunch
    flow1 = simple_flow.flow1
    flow2 = simple_flow_2.flow2

    assert flow1.get_bunch() == bunch
    assert flow2.get_bunch() == bunch

    task1 = simple_flow.task1
    assert task1.get_bunch() == bunch

    container2 = simple_flow_2.container2
    assert container2.get_bunch() == bunch


def test_bunch_get_node_status(simple_flow):
    bunch = Bunch()
    flow1 = simple_flow.flow1
    bunch.add_flow(flow1)
    flow1.requeue()
    assert bunch.get_node_status() == NodeStatus.queued

    task1 = simple_flow.task1
    task1.init("1001")
    assert bunch.get_node_status() == NodeStatus.active

    task1.complete()
    assert bunch.get_node_status() == NodeStatus.queued

    task2 = simple_flow.task2
    task2.abort()
    assert bunch.get_node_status() == NodeStatus.aborted
