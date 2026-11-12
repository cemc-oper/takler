import datetime

import pytest

from takler.core.calendar import Calendar


@pytest.fixture
def current_time():
    return datetime.datetime(2025, 8, 29, 10, 0, 10)


@pytest.fixture
def begin_time():
    return datetime.datetime(2025, 8, 29, 10, 0, 0)


@pytest.fixture
def patch_datetime_now(monkeypatch, current_time):
    """
    set ``datetime.datetime.now`` to a fixed time, 2025-08-29 10:00:10
    """

    class TestDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return current_time

    monkeypatch.setattr(datetime, "datetime", TestDateTime)


def test_calendar_create():
    calendar = Calendar()
    assert calendar.initial_time is None
    assert calendar.flow_time is None
    assert calendar.duration is None
    assert calendar.increment is None
    assert calendar.initial_real_time is None
    assert calendar.last_real_time is None


def test_calendar_begin(patch_datetime_now, begin_time, current_time):
    calendar = Calendar()
    calendar.begin(begin_time)
    assert calendar.initial_time == begin_time
    assert calendar.flow_time == begin_time
    assert calendar.duration == datetime.timedelta()
    assert calendar.increment == datetime.timedelta()
    assert calendar.initial_real_time == current_time
    assert calendar.last_real_time == current_time


def test_calendar_update(patch_datetime_now, begin_time, current_time):
    calendar = Calendar()
    calendar.begin(begin_time)

    update_time = begin_time + datetime.timedelta(minutes=30)

    calendar.update(update_time)
    assert calendar.initial_time == begin_time
    assert calendar.flow_time == update_time - datetime.timedelta(seconds=10)
    assert calendar.duration == datetime.timedelta(minutes=29, seconds=50)
    assert calendar.increment == datetime.timedelta(minutes=29, seconds=50)
    assert calendar.initial_real_time == current_time
    assert calendar.last_real_time == update_time
