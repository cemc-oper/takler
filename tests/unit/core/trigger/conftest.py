from pydantic import BaseModel, ConfigDict
import pytest

from takler.core import Flow, Task, NodeContainer


class StatusSimpleFlow(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    flow1: Flow
    task1: Task
    task2: Task
    container1: NodeContainer
    task3: Task
    task4: Task
    container2: NodeContainer
    task5: Task
    container3: NodeContainer
    task6: Task
    task7: Task
    task8: Task
    task9: Task
    task10: Task


@pytest.fixture
def trigger_simple_flow():
    """

    |- flow1
        |- task1
        |- task2
        |- container1
             |- task3
             |- task4
        |- container2
            |- task5
            |- container3
                |- task6
                |- task7
            |- task8
        |- task9
        |- task10

    """
    flow1 = Flow("flow1")
    with flow1.add_task("task1") as task1:
        pass
    with flow1.add_task("task2") as task2:
        pass
    with flow1.add_container("container1") as container1:
        with container1.add_task("task3") as task3:
            pass
        with container1.add_task("task4") as task4:
            pass
    with flow1.add_container("container2") as container2:
        with container2.add_task("task5") as task5:
            pass
        with container2.add_container("container3") as container3:
            with container3.add_task("task6") as task6:
                pass
            with container3.add_task("task7") as task7:
                pass
        with container2.add_task("task8") as task8:
            pass
    with flow1.add_task("task9") as task9:
        pass
    with flow1.add_task("task10") as task10:
        pass

    flow1.requeue()

    simple_flow = StatusSimpleFlow(
        flow1=flow1,
        task1=task1,
        task2=task2,
        container1=container1,
        task3=task3,
        task4=task4,
        container2=container2,
        task5=task5,
        container3=container3,
        task6=task6,
        task7=task7,
        task8=task8,
        task9=task9,
        task10=task10,
    )

    return simple_flow
