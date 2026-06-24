import pytest

from takler.core import NodeStatus

def test_default_node_status_on_task(simple_flow_for_operation):
    """
    |- flow1 [queued]
      |- task1 [complete]
      |- container1 [queued]
        |- task2 [queued]
        |- container2 [queued]
          |- task3 [queued]
          |- task4 [queued]
        |- container3 [queued]
          |- task5 [queued]
          |- task6 [queued]
      |- task7 [queued]
      |- container4 [queued]
        |- task8 [queued]
        |- task9 [queued]
      |- task10 [queued]
    """

    flow1 = simple_flow_for_operation.flow1
    task1 = simple_flow_for_operation.task1
    task1.set_default_node_status(NodeStatus.complete)
    flow1.requeue()
    assert task1.state.node_status == NodeStatus.complete


def test_default_node_status_on_container(simple_flow_for_operation):
    """
    |- flow1 [queued]
      |- task1 [queue]
      |- container1 [complete]
        |- task2 [complete]
        |- container2 [complete]
          |- task3 [complete]
          |- task4 [complete]
        |- container3 [complete]
          |- task5 [complete]
          |- task6 [complete]
      |- task7 [queued]
      |- container4 [queued]
        |- task8 [queued]
        |- task9 [queued]
      |- task10 [queued]
    """

    flow1 = simple_flow_for_operation.flow1
    container1 = simple_flow_for_operation.container1
    task2 = simple_flow_for_operation.task2
    task3 = simple_flow_for_operation.task3
    container3 = simple_flow_for_operation.container3

    container1.set_default_node_status(NodeStatus.complete)

    flow1.requeue()

    assert container1.state.node_status == NodeStatus.complete
    assert task2.state.node_status == NodeStatus.complete
    assert task3.state.node_status == NodeStatus.complete
    assert container3.state.node_status == NodeStatus.complete



def test_set_default_node_status_on_task_with_error_status(simple_flow_for_operation):
    """
    |- flow1 [queued]
      |- task1 [complete]
      |- container1 [queued]
        |- task2 [queued]
        |- container2 [queued]
          |- task3 [queued]
          |- task4 [queued]
        |- container3 [queued]
          |- task5 [queued]
          |- task6 [queued]
      |- task7 [queued]
      |- container4 [queued]
        |- task8 [queued]
        |- task9 [queued]
      |- task10 [queued]
    """

    task1 = simple_flow_for_operation.task1
    with pytest.raises(ValueError):
        task1.set_default_node_status(NodeStatus.aborted)