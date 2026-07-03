from datetime import date, timedelta
from takler.core import RepeatDate, Flow, NodeStatus, Parameter

import pytest

#------------------
# RepeatDate
#------------------

@pytest.fixture
def start_date_int():
    return 20220601


@pytest.fixture
def end_date_int():
    return 20220607


def test_repeat_date_create(start_date_int, end_date_int):
    r = RepeatDate("TAKLER_DATE", start_date_int, end_date_int)
    assert r.start_date == date(2022, 6, 1)
    assert r.end_date == date(2022, 6, 7)
    assert r.step_day == timedelta(days=1)
    assert r._value == date(2022, 6, 1)

    assert r.start == 20220601
    assert r.end == 20220607
    assert r.step == 1
    assert r.value == 20220601


def test_repeat_date_value(start_date_int, end_date_int):
    r = RepeatDate("TAKLER_DATE", start_date_int, end_date_int)
    assert r.value == 20220601

    r.value = 20220602
    assert r.value == 20220602

    r.value = "20220603"
    assert r.value == 20220603

    r.value = date(2022, 6, 4)
    assert r.value == 20220604


def test_repeat_date_valid():
    r = RepeatDate("TAKLER_DATE", 20220601, 20220607)

    assert r.valid()
    r.value = 20220603
    assert r.valid()
    r.value = 20220607
    assert r.valid()

    r.value = 20220608
    assert not r.valid()
    r.value = 20220530
    assert not r.valid()


def test_repeat_date_increment(start_date_int, end_date_int):
    # step = 1
    r = RepeatDate("TAKLER_DATE", start_date_int, end_date_int)
    assert r.value == 20220601
    assert r.increment()
    assert r.value == 20220602
    assert r.increment()
    assert r.value == 20220603
    assert r.increment()
    assert r.value == 20220604
    assert r.increment()
    assert r.value == 20220605
    assert r.increment()
    assert r.value == 20220606
    assert r.increment()
    assert r.value == 20220607

    assert not r.increment()
    assert r.value == 20220607

    # step = 2
    r = RepeatDate("TAKLER_DATE", start_date_int, end_date_int, 2)
    assert r.increment()
    assert r.value == 20220603
    assert r.increment()
    assert r.value == 20220605
    assert r.increment()
    assert r.value == 20220607

    assert not r.increment()
    assert r.value == 20220607


def test_repeat_date_change(start_date_int, end_date_int):
    r = RepeatDate("TAKLER_DATE", start_date_int, end_date_int)
    r.change(20220602)
    assert r.value == 20220602

    r.change("20220603")
    assert r.value == 20220603

    r.change(date(2022, 6, 4))
    assert r.value == 20220604


def test_repeat_date_change_with_step_1_and_invalid_value(start_date_int, end_date_int):
    r = RepeatDate("TAKLER_DATE", start_date_int, end_date_int)
    with pytest.raises(ValueError):
        r.change(date(2022, 6, 8))
    with pytest.raises(ValueError):
        r.change(20220609)
    with pytest.raises(ValueError):
        r.change("20220530")


def test_repeat_date_change_with_step_2(start_date_int, end_date_int):
    r = RepeatDate("TAKLER_DATE", start_date_int, end_date_int, 2)
    r.change(20220603)
    assert r.value == 20220603

    r.change("20220605")
    assert r.value == 20220605

    r.change(date(2022, 6, 7))
    assert r.value == 20220607


def test_repeat_date_change_with_step_2_and_invalid_value(start_date_int, end_date_int):
    r = RepeatDate("TAKLER_DATE", start_date_int, end_date_int, 2)
    with pytest.raises(ValueError):
        r.change(date(2022, 5, 8))

    with pytest.raises(ValueError):
        r.change(20220608)

    with pytest.raises(ValueError):
        r.change("20220602")


def test_repeat_date_reset(start_date_int, end_date_int):
    r = RepeatDate("TAKLER_DATE", start_date_int, end_date_int)
    assert r.increment()
    assert r.increment()
    assert r.increment()
    assert r.value == 20220604

    r.reset()
    assert r.value == 20220601


def test_repeat_date_generated_params(start_date_int, end_date_int):
    r = RepeatDate("TAKLER_DATE", start_date_int, end_date_int)
    assert r.generated_parameters() == {
        "TAKLER_DATE": Parameter("TAKLER_DATE", 20220601)
    }

    r.increment()
    assert r.generated_parameters() == {
        "TAKLER_DATE": Parameter("TAKLER_DATE", 20220602)
    }


#---------------
# Flow
#---------------

def test_flow_add_repeat_repeat_date(start_date_int, end_date_int):
    flow1 = Flow("flow1")
    task1 = flow1.add_task("task1")
    task1.add_repeat(RepeatDate("YMD", start_date_int, end_date_int))


def test_flow_repeat_date_run(start_date_int, end_date_int):
    # create flow
    flow1 = Flow("flow1")
    task1 = flow1.add_task("task1")
    task1.add_repeat(RepeatDate("YMD", start_date_int, end_date_int))

    # requeue
    task1.requeue()

    # run 1
    task1.resolve_dependencies()
    assert task1.find_generated_parameter("YMD").value == 20220601

    # run 2
    task1.complete()
    task1.resolve_dependencies()
    assert task1.find_generated_parameter("YMD").value == 20220602

    # run 3
    task1.complete()
    task1.resolve_dependencies()
    assert task1.find_generated_parameter("YMD").value == 20220603

    # run 4
    task1.complete()
    task1.resolve_dependencies()
    assert task1.find_generated_parameter("YMD").value == 20220604

    # run 5
    task1.complete()
    task1.resolve_dependencies()
    assert task1.find_generated_parameter("YMD").value == 20220605

    # run 6
    task1.complete()
    task1.resolve_dependencies()
    assert task1.find_generated_parameter("YMD").value == 20220606

    # run 7
    task1.complete()
    task1.resolve_dependencies()
    assert task1.find_generated_parameter("YMD").value == 20220607

    # full complete
    task1.complete()
    task1.resolve_dependencies()

    assert task1.state.node_status == NodeStatus.complete
    assert task1.find_generated_parameter("YMD").value == 20220607

    # requeue
    task1.requeue()
    assert task1.find_generated_parameter("YMD").value == 20220601
