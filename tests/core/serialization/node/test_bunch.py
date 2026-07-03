import pytest

from takler.core import Flow, Bunch

from .util import get_node_tree_print_string


class ObjectContainer:
    pass


@pytest.fixture
def bunch_case():
    """

    |- flow1
        |- container1
            |- task1
            |- task2
    """
    result = ObjectContainer()

    bunch = Bunch("bunch1", host="host1", port="port1")
    result.bunch = bunch

    flow1 = Flow("flow1")
    result.flow1 = flow1
    bunch.add_flow(flow1)

    with flow1.add_container("container1") as container1:
        result.container1 = container1
        with container1.add_task("task1") as task1:
            result.task1 = task1
        with container1.add_task("task2") as task2:
            result.task2 = task2

    flow2 = Flow("flow2")
    result.flow2 = flow2
    bunch.add_flow(flow2)

    with flow2.add_container("container2") as container2:
        result.container2 = container1
        with container2.add_task("task3") as task3:
            result.task3 = task3
        with container2.add_task("task4") as task4:
            result.task4 = task4

    flow1.requeue()
    flow2.requeue()

    return result


def test_bunch_to_dict(bunch_case):
    bunch = bunch_case.bunch
    assert bunch.to_dict() == dict(
        name="bunch1",
        class_type=dict(
            module="takler.core.bunch",
            name="Bunch"
        ),
        state=dict(status=1, suspended=False),
        server_state=dict(
            host="host1",
            port="port1",
            parameters=[
                dict(name="TAKLER_HOST", value="host1"),
                dict(name="TAKLER_PORT", value="port1"),
                dict(name="TAKLER_HOME", value=".")
            ]
        ),
        flows=[
            dict(
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
            ),
            dict(
                name="flow2",
                class_type=dict(
                    module="takler.core.flow",
                    name="Flow"
                ),
                state=dict(status=3, suspended=False),
                children=[
                    dict(
                        name="container2",
                        class_type=dict(
                            module="takler.core.node_container",
                            name="NodeContainer"
                        ),
                        state=dict(status=3, suspended=False),
                        children=[
                            dict(
                                name="task3",
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
                                name="task4",
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
        ]
    )


def test_bunch_from_dict(bunch_case):
    bunch = bunch_case.bunch
    d = dict(
        name="bunch1",
        class_type=dict(
            module="takler.core.bunch",
            name="Bunch"
        ),
        state=dict(status=1, suspended=False),
        server_state=dict(
            host="host1",
            port="port1",
            parameters=[
                dict(name="TAKLER_HOST", value="host1"),
                dict(name="TAKLER_PORT", value="port1"),
                dict(name="TAKLER_HOME", value=".")
            ]
        ),
        flows=[
            dict(
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
            ),
            dict(
                name="flow2",
                class_type=dict(
                    module="takler.core.flow",
                    name="Flow"
                ),
                state=dict(status=3, suspended=False),
                children=[
                    dict(
                        name="container2",
                        class_type=dict(
                            module="takler.core.node_container",
                            name="NodeContainer"
                        ),
                        state=dict(status=3, suspended=False),
                        children=[
                            dict(
                                name="task3",
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
                                name="task4",
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
        ]
    )

    test_bunch = Bunch.from_dict(d)

    bunch_text = get_node_tree_print_string(test_bunch)
    expected_bunch_text = get_node_tree_print_string(bunch)
    assert bunch_text == expected_bunch_text
