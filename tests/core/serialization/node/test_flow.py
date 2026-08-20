import pytest

from takler.core import Flow, SerializationType

from .util import get_node_tree_print_string


class ObjectContainer:
    pass


@pytest.fixture
def flow_case():
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


def test_flow_node(flow_case):
    flow1 = flow_case.flow1
    assert flow1.to_dict() == dict(
        name="flow1",
        class_type=dict(
            module="takler.core.flow",
            name="Flow"
        ),
        state=dict(status=3, suspended=False),
        children=[
            dict(
                name="container1",
                class_type=dict(
                    module="takler.core.node_container",
                    name="NodeContainer"
                ),
                state=dict(status=3, suspended=False),
                children=[
                    dict(
                        name="task1",
                        class_type=dict(
                            module="takler.core.task_node",
                            name="Task"
                        ),
                        state=dict(status=3, suspended=False),
                        task_id=None,
                        aborted_reason=None,
                        try_no=0,
                    ),
                    dict(
                        name="task2",
                        class_type=dict(
                            module="takler.core.task_node",
                            name="Task"
                        ),
                        state=dict(status=3, suspended=False),
                        task_id=None,
                        aborted_reason=None,
                        try_no=0,
                    )
                ]
            )
        ],
        begun=False,
        calendar=dict(
            initial_time=None,
            flow_time=None,
            duration=None,
            increment=None,
            initial_real_time=None,
            last_real_time=None,
        ),
    )


def test_flow_from_dict(flow_case):
    flow1 = flow_case.flow1
    d = dict(
        name="flow1",
        class_type=dict(
            module="takler.core.flow",
            name="Flow"
        ),
        state=dict(status=3, suspended=False),
        children=[
            dict(
                name="container1",
                class_type=dict(
                    module="takler.core.node_container",
                    name="NodeContainer"
                ),
                state=dict(status=3, suspended=False),
                children=[
                    dict(
                        name="task1",
                        class_type=dict(
                            module="takler.core.task_node",
                            name="Task"
                        ),
                        state=dict(status=3, suspended=False),
                        task_id=None,
                        aborted_reason=None,
                        try_no=0,
                    ),
                    dict(
                        name="task2",
                        class_type=dict(
                            module="takler.core.task_node",
                            name="Task"
                        ),
                        state=dict(status=3, suspended=False),
                        task_id=None,
                        aborted_reason=None,
                        try_no=0,
                    )
                ]
            )
        ]
    )

    flow = Flow.from_dict(d, method=SerializationType.Status)

    flow_text = get_node_tree_print_string(flow)
    expected_flow_text = get_node_tree_print_string(flow1)
    assert flow_text == expected_flow_text
    return
