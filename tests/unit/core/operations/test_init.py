from takler.core.state import NodeStatus


def test_init_on_task1(simple_flow_for_operation):
    flow1 = simple_flow_for_operation.flow1
    task1 = simple_flow_for_operation.task1
    container1 = simple_flow_for_operation.container1

    flow1.requeue()
    assert flow1.state.node_status == NodeStatus.queued
    assert task1.state.node_status == NodeStatus.queued
    assert container1.state.node_status == NodeStatus.queued

    task1.init(task_id="1111")
    assert flow1.state.node_status == NodeStatus.active
    assert task1.state.node_status == NodeStatus.active
    assert container1.state.node_status == NodeStatus.queued


def test_init_on_task3(simple_flow_for_operation):
    flow1 = simple_flow_for_operation.flow1
    container1 = simple_flow_for_operation.container1
    task2 = simple_flow_for_operation.task2
    container2 = simple_flow_for_operation.container2
    task3 = simple_flow_for_operation.task3
    task4 = simple_flow_for_operation.task4

    flow1.requeue()

    task3.init(task_id="3333")
    assert flow1.state.node_status == NodeStatus.active
    assert container1.state.node_status == NodeStatus.active
    assert task2.state.node_status == NodeStatus.queued
    assert container2.state.node_status == NodeStatus.active
    assert task3.state.node_status == NodeStatus.active
    assert task4.state.node_status == NodeStatus.queued
