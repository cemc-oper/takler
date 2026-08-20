import pytest

from takler.core import Flow, Bunch
from takler.core.parameter import TAKLER_HOST, TAKLER_PORT

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


def test_bunch_from_dict_flow_back_reference(bunch_case):
    """
    ``Bunch.from_dict`` must register flows through ``add_flow`` so restored flows
    keep the back reference to the bunch and can resolve the server parameters.
    """
    bunch = bunch_case.bunch

    restored_bunch = Bunch.from_dict(bunch.to_dict())

    for flow_name in ("flow1", "flow2"):
        flow = restored_bunch.find_flow(flow_name)
        assert flow is not None
        assert flow.get_bunch() is restored_bunch

        host_param = flow.find_parent_parameter(TAKLER_HOST)
        assert host_param is not None
        assert host_param.value == "host1"

        port_param = flow.find_parent_parameter(TAKLER_PORT)
        assert port_param is not None
        assert port_param.value == "port1"

    # descendant nodes reach the server parameters through the inheritance chain too
    task1 = restored_bunch.find_node("/flow1/container1/task1")
    assert task1 is not None
    assert task1.get_bunch() is restored_bunch
    assert task1.find_parent_parameter(TAKLER_HOST).value == "host1"
    assert task1.find_parent_parameter(TAKLER_PORT).value == "port1"
