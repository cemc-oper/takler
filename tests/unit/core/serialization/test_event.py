import pytest

from takler.core import Event, SerializationType


def test_event_to_dict():
    event = Event("some_event")
    assert event.to_dict() == dict(
        name="some_event",
        initial_value=False,
        value=False,
    )

    event.value = True
    assert event.to_dict() == dict(
        name="some_event",
        initial_value=False,
        value=True,
    )


def test_event_from_dict():
    d = dict(
        name="event1",
        initial_value=False,
    )
    assert Event.from_dict(d, method=SerializationType.Tree) == Event(
        name="event1",
        initial_value=False,
    )

    with pytest.raises(KeyError):
        Event.from_dict(d)

    with pytest.raises(KeyError):
        Event.from_dict(d, method=SerializationType.Status)

    d = dict(
        name="event1",
        initial_value=False,
        value=True,
    )
    event = Event(name="event1", initial_value=False)
    event.value = True
    assert Event.from_dict(d) == event
    assert Event.from_dict(d, method=SerializationType.Status) == event

    assert Event.from_dict(d, method=SerializationType.Tree) != event
    assert Event.from_dict(d, method=SerializationType.Tree) == Event(
        name="event1",
        initial_value=False,
    )
