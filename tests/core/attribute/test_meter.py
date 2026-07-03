import pytest

from takler.core import Meter


#-----------------------
# Meter
#-----------------------

def test_meter_create():
    meter = Meter("forecast_hour", -1, 240)
    assert meter.name == "forecast_hour"
    assert meter.min_value == -1
    assert meter.max_value == 240
    assert meter._value == -1


def test_meter_value():
    meter = Meter("forecast_hour", -1, 240)
    assert meter.value == -1

    meter.value = 10
    assert meter.value == 10

    meter.value = 240
    assert meter.value == 240

    meter.value = -1
    assert meter.value == -1


def test_meter_value_invalid():
    meter = Meter("forecast_hour", -1, 240)

    with pytest.raises(ValueError):
        meter.value = -2

    with pytest.raises(ValueError):
        meter.value = 241


def test_meter_reset():
    meter = Meter("forecast_hour", -1, 240)
    assert meter.value == -1
    meter.value = 10
    assert meter.value == 10
    meter.reset()
    assert meter.value == -1


#-----------------------
# Flow
#-----------------------


def test_task_add_meter(simple_flow):
    task1 = simple_flow.task1
    meter = task1.add_meter("forecast_hour", 0, 240)
    assert task1.meters == [Meter("forecast_hour", 0, 240)]
    assert meter.min_value == 0
    assert meter.max_value == 240
    assert meter.value == 0


def test_task_set_meter(simple_flow):
    task1 = simple_flow.task1
    meter1 = task1.add_meter("meter1", 0, 24)
    meter2 = task1.add_meter("meter2", 10, 100)

    assert task1.set_meter("meter1", 0)
    assert meter1.value == 0
    assert task1.set_meter("meter1", 10)
    assert meter1.value == 10
    assert task1.set_meter("meter1", 24)
    assert meter1.value == 24
    with pytest.raises(ValueError):
        task1.set_meter("meter1", 50)

    assert task1.set_meter("meter2", 10)
    assert meter2.value == 10
    assert task1.set_meter("meter2", 50)
    assert meter2.value == 50
    assert task1.set_meter("meter2", 100)
    assert meter2.value == 100


def test_task_set_meter_on_non_exist_meter(simple_flow):
    task1 = simple_flow.task1
    # meter1 = task1.add_meter("meter1", 0, 24)
    # meter2 = task1.add_meter("meter2", 10, 100)

    assert not task1.set_meter("not_exist_meter", 10)


def test_task_find_meter(simple_flow):
    task1 = simple_flow.task1
    meter1 = task1.add_meter("meter1", 0, 24)
    meter2 = task1.add_meter("meter2", 10, 100)

    assert task1.find_meter("meter1") == meter1
    assert task1.find_meter("meter2") == meter2

    assert task1.find_meter("not_exist_meter") is None


def test_task_reset_event(simple_flow):
    task1 = simple_flow.task1
    meter1 = task1.add_meter("meter1", 0, 24)
    meter2 = task1.add_meter("meter2", 10, 100)

    assert task1.set_meter("meter1", 10)
    assert meter1.value == 10
    assert task1.reset_meter("meter1")
    assert meter1.value == 0

    assert task1.set_meter("meter2", 50)
    assert meter2.value == 50
    assert task1.reset_meter("meter2")
    assert meter2.value == 10


def test_task_reset_event_on_non_exist_meter(simple_flow):
    task1 = simple_flow.task1
    # meter1 = task1.add_meter("meter1", 0, 24)
    # meter2 = task1.add_meter("meter2", 10, 100)
    assert not task1.reset_meter("not_exist_meter")
