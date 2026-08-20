import pytest

from takler.core import Task


@pytest.fixture
def task_with_variables() -> Task:
    task = Task("task1")
    task.add_parameter("param1", "param value1")
    task.add_event("event1", False)
    task.add_meter("meter1", 0, 100)
    return task


def test_task_find_variable_parameter(task_with_variables: Task):
    param = task_with_variables.find_variable("param1")
    assert param is not None
    assert param.name == "param1"
    assert param.value == "param value1"


def test_task_find_variable_parameter_non_exist(task_with_variables: Task):
    param = task_with_variables.find_variable("param2")
    assert param is None


def test_task_find_variable_event(task_with_variables: Task):
    event = task_with_variables.find_variable("event1")
    assert event is not None
    assert event.name == "event1"
    assert event.value is False


def test_task_find_variable_event_non_exist(task_with_variables: Task):
    event = task_with_variables.find_variable("event2")
    assert event is None


def test_task_find_variable_meter(task_with_variables: Task):
    meter = task_with_variables.find_variable("meter1")
    assert meter is not None
    assert meter.name == "meter1"
    assert meter.min_value == 0
    assert meter.max_value == 100


def test_task_find_variable_meter_non_exist(task_with_variables: Task):
    meter = task_with_variables.find_variable("meter2")
    assert meter is None
