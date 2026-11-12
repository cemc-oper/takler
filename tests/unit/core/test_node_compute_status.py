from pydantic import BaseModel, ConfigDict

import pytest

from takler.core import Flow, NodeContainer, Task, NodeStatus
from takler.core.node import compute_most_significant_status


class StatusFlow(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    flow1: Flow
    container1: NodeContainer
    task1: Task
    container2: NodeContainer
    task2: Task
    task3: Task
    task4: Task
    task5: Task
    task6: Task


@pytest.fixture
def status_flow() -> StatusFlow:
    """

    Flow:
        |- flow1 [unknown]
          |- container1 [unknown]
            |- task1 [unknown]
            |- container2 [unknown]
              |- task2 [unknown]
              |- task3 [unknown]
              |- task4 [unknown]
              |- task5 [unknown]
            |- task6 [unknown]
    """
    flow1 = Flow("flow1")
    container1 = flow1.add_container("container1")
    task1 = container1.add_task("task1")
    container2 = container1.add_container("container2")
    task2 = container2.add_task("task2")
    task3 = container2.add_task("task3")
    task4 = container2.add_task("task4")
    task5 = container2.add_task("task5")
    task6 = container1.add_task("task6")

    flow_object = StatusFlow(
        flow1=flow1,
        container1=container1,
        task1=task1,
        container2=container2,
        task2=task2,
        task3=task3,
        task4=task4,
        task5=task5,
        task6=task6,
    )

    return flow_object


@pytest.fixture
def status_flow_queued(status_flow) -> StatusFlow:
    """
    Flow:
        |- flow1 [queued]
          |- container1 [queued]
            |- task1 [queued]
            |- container2 [queued]
              |- task2 [queued]
              |- task3 [queued]
              |- task4 [queued]
              |- task5 [queued]
            |- task6 [queued]
    """
    for field_name in StatusFlow.model_fields:
        node = getattr(status_flow, field_name)
        node.state.node_status = NodeStatus.queued
    return status_flow


def test_compute_status_unknown(status_flow):
    assert (
        compute_most_significant_status(status_flow.container1.children, immediate=True)
        == NodeStatus.unknown
    )
    assert (
        compute_most_significant_status(
            status_flow.container1.children, immediate=False
        )
        == NodeStatus.unknown
    )


def test_compute_status_submitted(status_flow_queued):
    flow1 = status_flow_queued.flow1
    container1 = status_flow_queued.container1
    container2 = status_flow_queued.container2
    task2 = status_flow_queued.task2

    task2.state.node_status = NodeStatus.submitted

    assert (
        compute_most_significant_status(container2.children, immediate=True)
        == NodeStatus.submitted
    )
    assert (
        compute_most_significant_status(container1.children, immediate=True)
        == NodeStatus.queued
    )
    assert (
        compute_most_significant_status(flow1.children, immediate=True)
        == NodeStatus.queued
    )

    assert (
        compute_most_significant_status(flow1.children, immediate=False)
        == NodeStatus.submitted
    )
    assert flow1.state.node_status == NodeStatus.queued
    assert container1.state.node_status == NodeStatus.queued
    assert container2.state.node_status == NodeStatus.queued

    assert (
        compute_most_significant_status(container1.children, immediate=False)
        == NodeStatus.submitted
    )
    assert (
        compute_most_significant_status(container2.children, immediate=False)
        == NodeStatus.submitted
    )


def test_compute_status_submitted_and_active(status_flow_queued):
    flow1 = status_flow_queued.flow1
    container1 = status_flow_queued.container1
    container2 = status_flow_queued.container2
    task2 = status_flow_queued.task2
    task3 = status_flow_queued.task3

    task2.state.node_status = NodeStatus.submitted
    task3.state.node_status = NodeStatus.active

    assert (
        compute_most_significant_status(container2.children, immediate=True)
        == NodeStatus.active
    )
    assert (
        compute_most_significant_status(container1.children, immediate=True)
        == NodeStatus.queued
    )
    assert (
        compute_most_significant_status(flow1.children, immediate=True)
        == NodeStatus.queued
    )

    assert (
        compute_most_significant_status(flow1.children, immediate=False)
        == NodeStatus.active
    )
    assert flow1.state.node_status == NodeStatus.queued
    assert container1.state.node_status == NodeStatus.queued
    assert container2.state.node_status == NodeStatus.queued

    assert (
        compute_most_significant_status(container1.children, immediate=False)
        == NodeStatus.active
    )
    assert (
        compute_most_significant_status(container2.children, immediate=False)
        == NodeStatus.active
    )


def test_compute_status_submitted_and_active_and_complete(status_flow_queued):
    flow1 = status_flow_queued.flow1
    container1 = status_flow_queued.container1
    container2 = status_flow_queued.container2
    task2 = status_flow_queued.task2
    task3 = status_flow_queued.task3
    task4 = status_flow_queued.task4

    task2.state.node_status = NodeStatus.submitted
    task3.state.node_status = NodeStatus.active
    task4.state.node_status = NodeStatus.complete

    assert (
        compute_most_significant_status(container2.children, immediate=True)
        == NodeStatus.active
    )
    assert (
        compute_most_significant_status(container1.children, immediate=True)
        == NodeStatus.queued
    )
    assert (
        compute_most_significant_status(flow1.children, immediate=True)
        == NodeStatus.queued
    )

    assert (
        compute_most_significant_status(flow1.children, immediate=False)
        == NodeStatus.active
    )
    assert flow1.state.node_status == NodeStatus.queued
    assert container1.state.node_status == NodeStatus.queued
    assert container2.state.node_status == NodeStatus.queued


def test_compute_status_submitted_and_active_and_complete_and_aborted(
    status_flow_queued,
):
    flow1 = status_flow_queued.flow1
    container1 = status_flow_queued.container1
    container2 = status_flow_queued.container2
    task2 = status_flow_queued.task2
    task3 = status_flow_queued.task3
    task4 = status_flow_queued.task4
    task5 = status_flow_queued.task5

    task2.state.node_status = NodeStatus.submitted
    task3.state.node_status = NodeStatus.active
    task4.state.node_status = NodeStatus.complete
    task5.state.node_status = NodeStatus.aborted

    assert (
        compute_most_significant_status(container2.children, immediate=True)
        == NodeStatus.aborted
    )
    assert (
        compute_most_significant_status(container1.children, immediate=True)
        == NodeStatus.queued
    )
    assert (
        compute_most_significant_status(flow1.children, immediate=True)
        == NodeStatus.queued
    )

    assert (
        compute_most_significant_status(flow1.children, immediate=False)
        == NodeStatus.aborted
    )
    assert flow1.state.node_status == NodeStatus.queued
    assert container1.state.node_status == NodeStatus.queued
    assert container2.state.node_status == NodeStatus.queued

    assert (
        compute_most_significant_status(container1.children, immediate=False)
        == NodeStatus.aborted
    )
    assert (
        compute_most_significant_status(container2.children, immediate=False)
        == NodeStatus.aborted
    )


def test_compute_status_complete_and_queued(status_flow_queued):
    flow1 = status_flow_queued.flow1
    container1 = status_flow_queued.container1
    container2 = status_flow_queued.container2
    task2 = status_flow_queued.task2
    task3 = status_flow_queued.task3
    task4 = status_flow_queued.task4
    task5 = status_flow_queued.task5

    task2.state.node_status = NodeStatus.complete
    task3.state.node_status = NodeStatus.complete
    task4.state.node_status = NodeStatus.complete
    task5.state.node_status = NodeStatus.complete

    assert (
        compute_most_significant_status(container2.children, immediate=True)
        == NodeStatus.complete
    )
    assert (
        compute_most_significant_status(container1.children, immediate=True)
        == NodeStatus.queued
    )
    assert (
        compute_most_significant_status(flow1.children, immediate=True)
        == NodeStatus.queued
    )

    assert (
        compute_most_significant_status(flow1.children, immediate=False)
        == NodeStatus.queued
    )
    assert flow1.state.node_status == NodeStatus.queued
    assert container1.state.node_status == NodeStatus.queued
    assert container2.state.node_status == NodeStatus.queued

    assert (
        compute_most_significant_status(container1.children, immediate=False)
        == NodeStatus.queued
    )
    assert (
        compute_most_significant_status(container2.children, immediate=False)
        == NodeStatus.complete
    )


def test_node_container_compute_status_empty_children():
    container1 = NodeContainer("container1")
    assert container1.state.node_status == NodeStatus.unknown
    assert container1.computed_status(True) == NodeStatus.unknown
    assert container1.computed_status(False) == NodeStatus.unknown

    container1.set_node_status_only(NodeStatus.queued)
    assert container1.state.node_status == NodeStatus.queued
    assert container1.computed_status(True) == NodeStatus.queued
    assert container1.computed_status(False) == NodeStatus.queued
