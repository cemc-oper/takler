import pytest
from pydantic import BaseModel, ConfigDict

from takler.core import Flow, NodeContainer, Task


class FlowWithParameter(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    flow1: Flow
    task1: Task
    container1: NodeContainer
    task2: Task
    task3: Task
    container2: NodeContainer
    task4: Task
    task5: Task
    task6: Task
    container3: NodeContainer
    task7: Task
    task8: Task


@pytest.fixture
def flow_with_parameter() -> FlowWithParameter:
    """
    |- flow1
        param FORECAST_DAYS 3.5
        param NODES 4
        param DATA_PREFIX global
        param FLAG_UPLOAD True
        param PARTITION serial
        |- task1
            param DATA_SOURCE local
        |- container1
            param TIME_INTERVAL 10
            |- task2
                param AN_OPTION 2
            |- task3
            |- container2
                |- task4
                |- task5
            |- task6
        |- container3
            |- task7
                param PARTITION operation
        |- task8


    Returns
    -------
    FlowWithParameter
    """
    flow1 = Flow("flow1")
    task1 = flow1.add_task("task1")
    container1 = flow1.add_container("container1")
    task2 = container1.add_task("task2")
    task3 = container1.add_task("task3")
    container2 = container1.add_container("container2")
    task4 = container2.add_task("task4")
    task5 = container2.add_task("task5")
    task6 = container1.add_task("task6")
    container3 = flow1.add_container("container3")
    task7 = container3.add_task("task7")
    task8 = flow1.add_task("task8")

    flow1.add_parameter("FORECAST_DAYS", 3.5)
    flow1.add_parameter("NODES", 4)
    flow1.add_parameter("DATA_PREFIX", "global")
    flow1.add_parameter("FLAG_UPLOAD", True)
    flow1.add_parameter("PARTITION", "serial")
    container1.add_parameter("TIME_INTERVAL", 10)
    task2.add_parameter("AN_OPTION", 2)
    task1.add_parameter("DATA_SOURCE", "local")
    task7.add_parameter("PARTITION", "operation")

    f = FlowWithParameter(
        flow1=flow1,
        task1=task1,
        container1=container1,
        task2=task2,
        task3=task3,
        container2=container2,
        task4=task4,
        task5=task5,
        task6=task6,
        container3=container3,
        task7=task7,
        task8=task8,
    )

    return f
