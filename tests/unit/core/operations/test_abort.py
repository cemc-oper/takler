from takler.core import NodeStatus


def test_abort_on_task(simple_flow_for_operation):
    flow1 = simple_flow_for_operation.flow1
    task1 = simple_flow_for_operation.task1

    flow1.requeue()
    assert task1.state.node_status == NodeStatus.queued
    assert flow1.state.node_status == NodeStatus.queued

    task1.init("1111")
    assert flow1.state.node_status == NodeStatus.active
    assert task1.state.node_status == NodeStatus.active

    task1.abort()
    assert flow1.state.node_status == NodeStatus.aborted
    assert task1.state.node_status == NodeStatus.aborted
