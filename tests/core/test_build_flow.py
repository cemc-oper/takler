import pytest
from pydantic import BaseModel, ConfigDict

from takler.core import Flow, NodeContainer, Task


class SomeFlow(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    flow1: Flow
    container1: NodeContainer
    task1: Task
    container2: NodeContainer
    task2: Task
    task3: Task
    task4: Task
    container3: NodeContainer
    task5: Task
    task6: Task


def verify_flow_structure(some_flow: SomeFlow):
    flow1 = some_flow.flow1
    container1 = some_flow.container1
    task1 = some_flow.task1
    container2 = some_flow.container2
    task2 = some_flow.task2
    task3 = some_flow.task3
    task4 = some_flow.task4
    container3 = some_flow.container3
    task5 = some_flow.task5
    task6 = some_flow.task6

    assert flow1.name == "flow1"
    assert flow1.children == [container1, task4, container3, task6]
    assert isinstance(flow1, Flow)

    assert container1.name == "container1"
    assert container1.children == [task1, container2]
    assert container1.parent == flow1
    assert isinstance(container1, NodeContainer)

    assert task1.name == "task1"
    assert task1.children == []
    assert task1.parent == container1
    assert isinstance(task1, Task)

    assert container2.name == "container2"
    assert container2.children == [task2, task3]
    assert container2.parent == container1
    assert isinstance(container2, NodeContainer)

    assert task2.name == "task2"
    assert task2.children == []
    assert task2.parent == container2
    assert isinstance(task2, Task)
    assert task3.name == "task3"
    assert task3.children == []
    assert task3.parent == container2
    assert isinstance(task3, Task)

    assert task4.name == "task4"
    assert task4.children == []
    assert task4.parent == flow1
    assert isinstance(task4, Task)

    assert container3.name == "container3"
    assert container3.children == [task5]
    assert container3.parent == flow1
    assert isinstance(container3, NodeContainer)

    assert task5.name == "task5"
    assert task5.children == []
    assert task5.parent == container3
    assert isinstance(task5, Task)

    assert task6.name == "task6"
    assert task6.children == []
    assert task6.parent == flow1
    assert isinstance(task6, Task)


def test_build_flow():
    """
    Flow:

        |- flow1 [unknown]
          |- container1 [unknown]
            |- task1 [unknown]
            |- container2 [unknown]
              |- task2 [unknown]
              |- task3 [unknown]
          |- task4 [unknown]
          |- container3 [unknown]
            |- task5 [unknown]
          |- task6 [unknown]

    """
    flow1 = Flow("flow1")
    container1 = flow1.add_container("container1")
    task1 = container1.add_task("task1")
    container2 = NodeContainer("container2")
    container1.add_container(container2)
    task2 = container2.add_task("task2")
    task3 = Task("task3")
    task3 = container2.add_task(task3)
    task4 = flow1.add_task("task4")
    container3 = flow1.add_container("container3")
    task5 = container3.add_task("task5")
    task6 = flow1.add_task("task6")

    some_flow = SomeFlow(
        flow1=flow1,
        container1=container1,
        task1=task1,
        container2=container2,
        task2=task2,
        task3=task3,
        task4=task4,
        container3=container3,
        task5=task5,
        task6=task6,
    )

    verify_flow_structure(some_flow)


def test_build_flow_using_with():
    with Flow("flow1") as flow1:
        with flow1.add_container("container1") as container1:
            with container1.add_task("task1") as task1:
                pass
            with container1.add_container("container2") as container2:
                with container2.add_task("task2") as task2:
                    pass
                with container2.add_task("task3") as task3:
                    pass
            with flow1.add_task("task4") as task4:
                pass
        with flow1.add_container("container3") as container3:
            with container3.add_task("task5") as task5:
                pass
        with flow1.add_task("task6") as task6:
            pass

    some_flow = SomeFlow(
        flow1=flow1,
        container1=container1,
        task1=task1,
        container2=container2,
        task2=task2,
        task3=task3,
        task4=task4,
        container3=container3,
        task5=task5,
        task6=task6,
    )

    verify_flow_structure(some_flow)


# -----------------
# NodeContainer
# -----------------


def test_node_container_add_container_error_type():
    container1 = NodeContainer("container1")

    with pytest.raises(TypeError):
        container1.add_container(20)


def test_node_container_add_task_error_type():
    container1 = NodeContainer("container1")
    with pytest.raises(TypeError):
        container1.add_task(20)
