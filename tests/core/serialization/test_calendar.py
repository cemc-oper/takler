import datetime

from takler.core import SerializationType
from takler.core.calendar import Calendar


def create_calendar() -> Calendar:
    """
    A calendar with all the six fields set, avoiding real clock reading.
    """
    calendar = Calendar()
    calendar.initial_time = datetime.datetime(2026, 7, 1, 8, 0, 0)
    calendar.flow_time = datetime.datetime(2026, 7, 1, 9, 14, 50)
    calendar.duration = datetime.timedelta(seconds=4490)
    calendar.increment = datetime.timedelta(seconds=10)
    calendar.initial_real_time = datetime.datetime(2026, 7, 1, 8, 0, 0)
    calendar.last_real_time = datetime.datetime(2026, 7, 1, 9, 14, 50)
    return calendar


def assert_calendar_equal(left: Calendar, right: Calendar):
    assert left.initial_time == right.initial_time
    assert left.flow_time == right.flow_time
    assert left.duration == right.duration
    assert left.increment == right.increment
    assert left.initial_real_time == right.initial_real_time
    assert left.last_real_time == right.last_real_time


def test_calendar_to_dict():
    calendar = create_calendar()

    assert calendar.to_dict() == dict(
        initial_time="2026-07-01T08:00:00",
        flow_time="2026-07-01T09:14:50",
        duration=4490.0,
        increment=10.0,
        initial_real_time="2026-07-01T08:00:00",
        last_real_time="2026-07-01T09:14:50",
    )


def test_calendar_to_dict_none():
    calendar = Calendar()

    assert calendar.to_dict() == dict(
        initial_time=None,
        flow_time=None,
        duration=None,
        increment=None,
        initial_real_time=None,
        last_real_time=None,
    )


def test_calendar_from_dict():
    d = dict(
        initial_time="2026-07-01T08:00:00",
        flow_time="2026-07-01T09:14:50",
        duration=4490.0,
        increment=10.0,
        initial_real_time="2026-07-01T08:00:00",
        last_real_time="2026-07-01T09:14:50",
    )

    calendar = Calendar.from_dict(d)
    assert_calendar_equal(calendar, create_calendar())

    calendar = Calendar.from_dict(d, method=SerializationType.Status)
    assert_calendar_equal(calendar, create_calendar())


def test_calendar_from_dict_none():
    d = dict(
        initial_time=None,
        flow_time=None,
        duration=None,
        increment=None,
        initial_real_time=None,
        last_real_time=None,
    )

    assert_calendar_equal(Calendar.from_dict(d), Calendar())


def test_calendar_from_dict_missing_keys():
    assert_calendar_equal(Calendar.from_dict(dict()), Calendar())


def test_calendar_from_dict_tree():
    d = create_calendar().to_dict()

    # all the six fields are runtime state, tree method keeps the initial values
    assert_calendar_equal(
        Calendar.from_dict(d, method=SerializationType.Tree), Calendar()
    )


def test_calendar_round_trip():
    calendar = create_calendar()

    restored = Calendar.from_dict(calendar.to_dict())
    assert_calendar_equal(restored, calendar)
    assert restored.to_dict() == calendar.to_dict()


def test_calendar_round_trip_none():
    calendar = Calendar()

    restored = Calendar.from_dict(calendar.to_dict())
    assert_calendar_equal(restored, calendar)
    assert restored.to_dict() == calendar.to_dict()


def test_calendar_round_trip_after_begin_and_update():
    calendar = Calendar()
    calendar.begin(datetime.datetime(2026, 7, 1, 8, 0, 0))
    calendar.update(datetime.datetime(2026, 7, 1, 8, 0, 30, 500000))

    restored = Calendar.from_dict(calendar.to_dict())
    assert_calendar_equal(restored, calendar)
    assert restored.to_dict() == calendar.to_dict()
