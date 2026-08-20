import pytest

from takler.core import Flow
from takler.core.node import Node

from .util import get_node_tree_print_string


class ObjectContainer:
    pass


@pytest.fixture
def child_case():
    """

    |- flow1
        |- container1
            |- task1
            |- task2
    """
    result = ObjectContainer()

    flow1 = Flow("flow1")
    result.flow1 = flow1

    with flow1.add_container("container1") as container1:
        result.container1 = container1
        with container1.add_task("task1") as task1:
            result.task1 = task1
        with container1.add_task("task2") as task2:
            result.task2 = task2

    flow1.requeue()

    return result


def test_container_node(child_case):
    container1 = child_case.container1
    assert container1.to_dict() == dict(
        name="container1",
        class_type=dict(module="takler.core.node_container", name="NodeContainer"),
        state=dict(status=3, suspended=False),
        children=[
            dict(
                name="task1",
                class_type=dict(module="takler.core.task_node", name="Task"),
                state=dict(status=3, suspended=False),
                task_id=None,
                aborted_reason=None,
                try_no=0,
            ),
            dict(
                name="task2",
                class_type=dict(module="takler.core.task_node", name="Task"),
                state=dict(status=3, suspended=False),
                task_id=None,
                aborted_reason=None,
                try_no=0,
            ),
        ],
    )


def test_from_dict(child_case):
    container1 = child_case.container1
    d = dict(
        name="container1",
        class_type=dict(module="takler.core.node_container", name="NodeContainer"),
        state=dict(status=3, suspended=False),
        children=[
            dict(
                name="task1",
                class_type=dict(module="takler.core.task_node", name="Task"),
                state=dict(status=3, suspended=False),
                task_id=None,
                aborted_reason=None,
                try_no=0,
            ),
            dict(
                name="task2",
                class_type=dict(module="takler.core.task_node", name="Task"),
                state=dict(status=3, suspended=False),
                task_id=None,
                aborted_reason=None,
                try_no=0,
            ),
        ],
    )
    node = Node.from_dict(d)

    node_text = get_node_tree_print_string(node)
    expected_node_text = get_node_tree_print_string(container1)
    assert node_text == expected_node_text
    return
