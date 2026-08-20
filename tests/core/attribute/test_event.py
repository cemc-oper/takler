import pytest

from takler.core import Event

# ----------------
# Event
# ----------------


def test_event_create():
    event = Event("event1")
    assert event.name == "event1"
    assert event.initial_value is False
    assert event._value is False

    event = Event("event2", True)
    assert event.name == "event2"
    assert event.initial_value is True
    assert event._value is True


def test_event_value():
    event = Event("event1", False)
    assert event.value is False
    assert event._value is False
    assert event.initial_value is False

    event.value = True
    assert event.value is True
    assert event._value is True
    assert event.initial_value is False


def test_event_reset():
    event = Event("event1", False)
    assert event.value is False
    assert event.initial_value is False

    event.value = True
    assert event.value is True
    assert event.initial_value is False

    event.reset()
    assert event.value is False
    assert event.initial_value is False

    event = Event("event1", True)
    assert event.value is True
    assert event.initial_value is True

    event.value = False
    assert event.value is False
    assert event.initial_value is True

    event.reset()
    assert event.value is True
    assert event.initial_value is True


# ----------------
# Event in flow
# ----------------


def test_task_add_event(simple_flow):
    task1 = simple_flow.task1
    event = task1.add_event("arrived")
    assert task1.events == [Event("arrived")]
    assert event.initial_value is False
    assert event.value is False


def test_task_add_event_duplicate(simple_flow):
    task1 = simple_flow.task1
    _ = task1.add_event("arrived")

    with pytest.raises(RuntimeError):
        task1.add_event("arrived")


def test_task_set_event(simple_flow):
    task1 = simple_flow.task1
    event1 = task1.add_event("event1")
    event2 = task1.add_event("event2")

    assert task1.set_event("event1", True)
    assert event1.value is True
    assert task1.set_event("event1", False)
    assert event1.value is False

    assert task1.set_event("event2", True)
    assert event2.value is True
    assert task1.set_event("event2", False)
    assert event2.value is False


def test_task_set_event_on_non_exist_event(simple_flow):
    task1 = simple_flow.task1
    assert task1.set_event("not_exist_event", True) is False


def test_task_find_event(simple_flow):
    task1 = simple_flow.task1
    event1 = task1.add_event("event1")

    assert task1.find_event("event1") == event1

    assert task1.find_event("not_exist_event") is None


def test_task_reset_event(simple_flow):
    task1 = simple_flow.task1
    event1 = task1.add_event("event1")
    event2 = task1.add_event("event2", True)

    assert task1.set_event("event1", True)
    assert event1.value is True
    assert task1.reset_event("event1") is True
    assert event1.value is False

    assert task1.set_event("event2", False)
    assert event2.value is False
    assert task1.reset_event("event2") is True
    assert event2.value is True

    assert task1.reset_event("not_exist_event") is False
