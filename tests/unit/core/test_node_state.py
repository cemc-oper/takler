import pytest

from takler.core.node import Node, NodeStatus


def test_node_is_suspended_single():
    node1 = Node("node1")
    assert node1.is_suspended() is False

    node1.state.suspended = True
    assert node1.is_suspended() is True


def test_node_is_suspended_tree():
    node1 = Node("node1")
    node2 = node1.append_child("node2")
    node2_1 = node2.append_child("node2_1")
    node3 = node1.append_child("node3")

    assert node1.is_suspended() is False
    assert node2.is_suspended() is False
    assert node2_1.is_suspended() is False
    assert node3.is_suspended() is False

    node2.state.suspended = True
    assert node1.is_suspended() is False
    assert node2.is_suspended() is True
    assert node2_1.is_suspended() is False
    assert node3.is_suspended() is False


@pytest.fixture
def simple_flow_with_queued_status(simple_flow):
    """
    Flow:
        |- flow1 [queued]
          |- container1 [queued]
            |- task1 [queued]
            |- container2 [queued]
              |- task2 [queued]
              |- task3 [queued]
          |- task4 [queued]
          |- container3 [queued]
            |- task5 [queued]
          |- task6 [queued]
    """
    simple_flow.flow1.state.node_status = NodeStatus.queued
    simple_flow.container1.state.node_status = NodeStatus.queued
    simple_flow.task1.state.node_status = NodeStatus.queued
    simple_flow.container2.state.node_status = NodeStatus.queued
    simple_flow.task2.state.node_status = NodeStatus.queued
    simple_flow.task3.state.node_status = NodeStatus.queued
    simple_flow.task4.state.node_status = NodeStatus.queued
    simple_flow.container3.state.node_status = NodeStatus.queued
    simple_flow.task5.state.node_status = NodeStatus.queued
    simple_flow.task6.state.node_status = NodeStatus.queued

    return simple_flow


def test_node_set_node_status_only(simple_flow_with_queued_status):
    task1 = simple_flow_with_queued_status.task1

    assert task1.state.node_status == NodeStatus.queued
    task1.set_node_status_only(NodeStatus.queued)
    assert task1.state.node_status == NodeStatus.queued

    task1.set_node_status_only(NodeStatus.complete)
    assert task1.state.node_status == NodeStatus.complete


def test_node_container_sink_status_change_only(simple_flow_with_queued_status):
    container1 = simple_flow_with_queued_status.container1
    container1.sink_status_change_only(NodeStatus.complete)

    assert container1.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task1.state.node_status == NodeStatus.complete
    assert (
        simple_flow_with_queued_status.container2.state.node_status
        == NodeStatus.complete
    )
    assert simple_flow_with_queued_status.task2.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task3.state.node_status == NodeStatus.complete

    assert simple_flow_with_queued_status.flow1.state.node_status == NodeStatus.queued
    assert simple_flow_with_queued_status.task4.state.node_status == NodeStatus.queued
    assert (
        simple_flow_with_queued_status.container3.state.node_status == NodeStatus.queued
    )
    assert simple_flow_with_queued_status.task5.state.node_status == NodeStatus.queued
    assert simple_flow_with_queued_status.task6.state.node_status == NodeStatus.queued


def test_task_swim_status_change_only(simple_flow_with_queued_status):
    task2 = simple_flow_with_queued_status.task2

    assert task2.state.node_status == NodeStatus.queued
    task2.set_node_status_only(NodeStatus.active)
    assert task2.state.node_status == NodeStatus.active

    task2.swim_status_change_only()
    assert (
        simple_flow_with_queued_status.container2.state.node_status == NodeStatus.active
    )
    assert (
        simple_flow_with_queued_status.container1.state.node_status == NodeStatus.active
    )
    assert simple_flow_with_queued_status.flow1.state.node_status == NodeStatus.active

    assert simple_flow_with_queued_status.task1.state.node_status == NodeStatus.queued
    assert simple_flow_with_queued_status.task3.state.node_status == NodeStatus.queued
    assert simple_flow_with_queued_status.task4.state.node_status == NodeStatus.queued
    assert (
        simple_flow_with_queued_status.container3.state.node_status == NodeStatus.queued
    )


def test_task_set_node_status(simple_flow_with_queued_status):
    task2 = simple_flow_with_queued_status.task2

    task2.set_node_status(NodeStatus.active)
    assert (
        simple_flow_with_queued_status.container2.state.node_status == NodeStatus.active
    )
    assert (
        simple_flow_with_queued_status.container1.state.node_status == NodeStatus.active
    )
    assert simple_flow_with_queued_status.flow1.state.node_status == NodeStatus.active

    assert simple_flow_with_queued_status.task1.state.node_status == NodeStatus.queued
    assert simple_flow_with_queued_status.task3.state.node_status == NodeStatus.queued
    assert simple_flow_with_queued_status.task4.state.node_status == NodeStatus.queued
    assert (
        simple_flow_with_queued_status.container3.state.node_status == NodeStatus.queued
    )


def test_container_sink_status_change(simple_flow_with_queued_status):
    """
    NodeContainer's node status is computed from its children's status.
    so set_node_status has no effect for NodeContainer.

    Use sink_status_change to update all nodes under a NodeContainer.
    """
    container1 = simple_flow_with_queued_status.container1
    task4 = simple_flow_with_queued_status.task4
    container3 = simple_flow_with_queued_status.container3
    task6 = simple_flow_with_queued_status.task6

    container1.sink_status_change(NodeStatus.complete)

    assert simple_flow_with_queued_status.flow1.state.node_status == NodeStatus.queued
    assert container1.state.node_status == NodeStatus.complete
    assert (
        simple_flow_with_queued_status.container2.state.node_status
        == NodeStatus.complete
    )
    assert simple_flow_with_queued_status.task1.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task2.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task3.state.node_status == NodeStatus.complete
    assert task4.state.node_status == NodeStatus.queued
    assert container3.state.node_status == NodeStatus.queued
    assert simple_flow_with_queued_status.task5.state.node_status == NodeStatus.queued
    assert task6.state.node_status == NodeStatus.queued

    task4.set_node_status(NodeStatus.complete)

    assert simple_flow_with_queued_status.flow1.state.node_status == NodeStatus.queued
    assert (
        simple_flow_with_queued_status.container1.state.node_status
        == NodeStatus.complete
    )
    assert (
        simple_flow_with_queued_status.container2.state.node_status
        == NodeStatus.complete
    )
    assert simple_flow_with_queued_status.task1.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task2.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task3.state.node_status == NodeStatus.complete
    assert task4.state.node_status == NodeStatus.complete
    assert container3.state.node_status == NodeStatus.queued
    assert simple_flow_with_queued_status.task5.state.node_status == NodeStatus.queued
    assert task6.state.node_status == NodeStatus.queued

    container3.sink_status_change(NodeStatus.complete)

    assert simple_flow_with_queued_status.flow1.state.node_status == NodeStatus.queued
    assert (
        simple_flow_with_queued_status.container1.state.node_status
        == NodeStatus.complete
    )
    assert (
        simple_flow_with_queued_status.container2.state.node_status
        == NodeStatus.complete
    )
    assert simple_flow_with_queued_status.task1.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task2.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task3.state.node_status == NodeStatus.complete
    assert task4.state.node_status == NodeStatus.complete
    assert container3.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task5.state.node_status == NodeStatus.complete
    assert task6.state.node_status == NodeStatus.queued

    task6.set_node_status(NodeStatus.complete)

    assert simple_flow_with_queued_status.flow1.state.node_status == NodeStatus.complete
    assert (
        simple_flow_with_queued_status.container1.state.node_status
        == NodeStatus.complete
    )
    assert (
        simple_flow_with_queued_status.container2.state.node_status
        == NodeStatus.complete
    )
    assert simple_flow_with_queued_status.task1.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task2.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task3.state.node_status == NodeStatus.complete
    assert task4.state.node_status == NodeStatus.complete
    assert container3.state.node_status == NodeStatus.complete
    assert simple_flow_with_queued_status.task5.state.node_status == NodeStatus.complete
    assert task6.state.node_status == NodeStatus.complete
