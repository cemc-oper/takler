import datetime

import pytest
from pydantic import BaseModel, ConfigDict

from takler.core import Flow, Task


# -------------------
# Flow
# -------------------


class OneTaskFlow(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    flow1: Flow
    task1: Task


@pytest.fixture
def one_task_time_flow() -> OneTaskFlow:
    """
    |- flow1
        |- task1
            time 12:00
    """
    with Flow("flow1") as flow1:
        with flow1.add_task("task1") as task1:
            task1.add_time(datetime.time(12, 0))

    flow1 = OneTaskFlow(flow1=flow1, task1=task1)
    return flow1


TEST_TIME = datetime.datetime(2022, 9, 12, 10, 0, 0)


@pytest.fixture
def patch_datetime_now(monkeypatch):
    """
    set ``datetime.datetime.now`` to a fixed time, 2022-09-12 10:00:01
    """

    class TestDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return TEST_TIME

    monkeypatch.setattr(datetime, "datetime", TestDateTime)


def test_time_attr_catch_time_point(one_task_time_flow, patch_datetime_now):
    flow1: Flow = one_task_time_flow.flow1
    task1: Task = one_task_time_flow.task1

    start_time = datetime.datetime(2022, 9, 12, 10, 0, 0)
    flow1.calendar.begin(start_time)
    # flow.calendar.flow_time = start_time
    # flow.calendar.last_real_time = start_time

    flow1.update_calendar(datetime.datetime(2022, 9, 12, 11, 0, 0))
    assert not task1.times[0].free
    assert not task1.resolve_time_dependencies()

    flow1.update_calendar(datetime.datetime(2022, 9, 12, 12, 0, 0))
    assert task1.times[0].free
    assert task1.resolve_time_dependencies()

    flow1.update_calendar(datetime.datetime(2022, 9, 12, 12, 1, 10))
    assert task1.times[0].free

    assert task1.resolve_time_dependencies()


def test_time_attr_miss_time_point(one_task_time_flow, patch_datetime_now):
    flow1: Flow = one_task_time_flow.flow1
    task1: Task = one_task_time_flow.task1

    start_time = datetime.datetime(2022, 9, 12, 10, 0, 0)
    flow1.calendar.begin(start_time)
    # flow1.calendar.flow_time = start_time
    # flow1.calendar.last_real_time = start_time

    flow1.update_calendar(datetime.datetime(2022, 9, 12, 11, 59, 0))
    assert not task1.times[0].free
    assert not task1.resolve_time_dependencies()

    flow1.update_calendar(datetime.datetime(2022, 9, 12, 12, 1, 0))
    assert not task1.times[0].free
    assert not task1.resolve_time_dependencies()

    flow1.update_calendar(datetime.datetime(2022, 9, 12, 13, 0, 10))
    assert not task1.times[0].free
    assert not task1.resolve_time_dependencies()


def test_time_attr_requeue(one_task_time_flow, patch_datetime_now):
    """
    ``requeue`` clears the ``free`` latch of the time attributes and leaves the calendar alone.

    Restarting the calendar belongs to ``begin`` only, so this test does not roll the calendar back
    after the requeue. Catching the time point again in a new run cycle is covered by
    ``test_time_attr_begin_again_catch_time_point``.
    """
    flow1: Flow = one_task_time_flow.flow1
    task1: Task = one_task_time_flow.task1

    # begin starts the calendar with the (patched) current time, 2022-09-12 10:00:00
    flow1.begin()
    assert flow1.calendar.initial_time == TEST_TIME

    flow1.update_calendar(datetime.datetime(2022, 9, 12, 11, 59, 0))
    assert not task1.times[0].free
    assert not task1.resolve_time_dependencies()

    flow1.update_calendar(datetime.datetime(2022, 9, 12, 12, 0, 0))
    assert task1.times[0].free
    assert task1.resolve_time_dependencies()

    flow1.update_calendar(datetime.datetime(2022, 9, 12, 13, 0, 10))
    assert task1.times[0].free
    assert task1.resolve_time_dependencies()

    # SECTION: requeue clears the free latch without touching the calendar
    calendar_before_requeue = flow1.calendar.to_dict()

    flow1.requeue()

    assert not task1.times[0].free
    assert not task1.resolve_time_dependencies()
    assert flow1.calendar.to_dict() == calendar_before_requeue


def _next_time_point(
    after: datetime.datetime, time_point: datetime.time
) -> datetime.datetime:
    """
    The first datetime later than ``after`` whose HH:MM equals ``time_point``.
    """
    candidate = datetime.datetime.combine(after.date(), time_point)
    while candidate <= after:
        candidate += datetime.timedelta(days=1)
    return candidate


def _advance_flow_time(flow: Flow, target_flow_time: datetime.datetime):
    """
    Advance ``flow``'s calendar so that its ``flow_time`` becomes exactly ``target_flow_time``.

    ``Calendar.update`` moves ``flow_time`` by the increment of the real time, so the real time fed
    into ``update_calendar`` is derived from the calendar itself instead of a hardcoded clock.
    """
    calendar = flow.calendar
    real_time = calendar.last_real_time + (target_flow_time - calendar.flow_time)
    flow.update_calendar(real_time)
    assert calendar.flow_time == target_flow_time


def test_time_attr_begin_again_catch_time_point(one_task_time_flow):
    """
    A new run cycle started by ``begin`` catches the time point again.

    ``begin`` starts the calendar with the real current time, so every expected time point is
    derived from the fresh ``initial_time`` instead of a fixed clock.
    """
    flow1: Flow = one_task_time_flow.flow1
    task1: Task = one_task_time_flow.task1
    time_point = task1.times[0].time

    # SECTION: first run cycle
    flow1.begin()
    assert flow1.begun
    initial_time = flow1.calendar.initial_time
    assert initial_time is not None
    assert flow1.calendar.flow_time == initial_time
    assert not task1.times[0].free

    first_hit = _next_time_point(
        initial_time + datetime.timedelta(minutes=2), time_point
    )

    _advance_flow_time(flow1, first_hit - datetime.timedelta(minutes=1))
    assert not task1.times[0].free
    assert not task1.resolve_time_dependencies()

    _advance_flow_time(flow1, first_hit)
    assert task1.times[0].free
    assert task1.resolve_time_dependencies()

    _advance_flow_time(flow1, first_hit + datetime.timedelta(hours=1))
    assert task1.times[0].free
    assert task1.resolve_time_dependencies()

    # SECTION: second run cycle, begin again with a fresh calendar
    flow1.begin(force=True)
    assert flow1.begun
    new_initial_time = flow1.calendar.initial_time
    assert new_initial_time >= initial_time
    assert flow1.calendar.flow_time == new_initial_time
    assert not task1.times[0].free

    second_hit = _next_time_point(
        new_initial_time + datetime.timedelta(minutes=2), time_point
    )

    _advance_flow_time(flow1, second_hit - datetime.timedelta(minutes=1))
    assert not task1.times[0].free
    assert not task1.resolve_time_dependencies()

    _advance_flow_time(flow1, second_hit)
    assert task1.times[0].free
    assert task1.resolve_time_dependencies()


def test_time_attr_free_dependencies(one_task_time_flow, patch_datetime_now):
    flow1: Flow = one_task_time_flow.flow1
    task1: Task = one_task_time_flow.task1

    start_time = datetime.datetime(2022, 9, 12, 10, 0, 0)
    flow1.calendar.begin(start_time)
    # flow1.calendar.flow_time = start_time
    # flow1.calendar.last_real_time = start_time

    flow1.update_calendar(datetime.datetime(2022, 9, 12, 11, 50, 0))
    assert not task1.times[0].free
    assert not task1.resolve_time_dependencies()

    task1.free_dependencies(dep_type="time")
    assert task1.times[0].free
    assert task1.resolve_time_dependencies()


def test_task_resolve_time_dependencies_single_task():
    task = Task("task1")
    task.add_time("12:00")

    with pytest.raises(RuntimeError):
        task.resolve_time_dependencies()
