import datetime
import pytest

from takler.core import NodeStatus


TEST_TIME = datetime.datetime(2026, 1, 27, 10, 0, 0)


@pytest.fixture
def simple_flow_queued(simple_flow):
    simple_flow.flow1.requeue()
    return simple_flow


def test_node_check_dependencies_suspend(simple_flow_queued):
    task1 = simple_flow_queued.task1
    task1.suspend()

    assert not task1.check_dependencies()


@pytest.fixture
def patch_datetime_now(monkeypatch):
    class TestDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return TEST_TIME

    monkeypatch.setattr(datetime, "datetime", TestDateTime)


def test_node_check_dependencies_time(simple_flow_queued, patch_datetime_now):
    flow1 = simple_flow_queued.flow1
    task1 = simple_flow_queued.task1
    task2 = simple_flow_queued.task2
    task3 = simple_flow_queued.task3

    task1.add_time("09:00")
    task2.add_time("10:00")
    task3.add_time("11:00")
    start_time = datetime.datetime(2026, 1, 27, 10, 0, 0)
    flow1.calendar.begin(start_time)
    flow1.update_calendar(start_time)

    assert not task1.check_dependencies()
    assert task2.check_dependencies()
    assert not task3.check_dependencies()


def test_node_check_dependencies_complete(simple_flow_queued):
    task1 = simple_flow_queued.task1
    task1.add_event("event_a")
    task2 = simple_flow_queued.task2
    task2.add_complete_trigger("../task1:event_a == set")
    task2.add_trigger("../task1 == complete")

    assert not task2.check_dependencies()

    task1.set_event("event_a", True)
    assert not task2.check_dependencies()

    assert task2.is_complete_triggered
    assert task2.state.node_status == NodeStatus.complete


def test_node_check_dependencies_trigger(simple_flow_queued):
    task1 = simple_flow_queued.task1
    task2 = simple_flow_queued.task2
    task2.add_trigger("../task1 == complete")

    assert not task2.check_dependencies()

    task1.complete()
    assert task2.check_dependencies()


# ----------------
# Task
# ----------------


def test_task_check_dependencies_node_status(simple_flow_queued):
    task1 = simple_flow_queued.task1
    task2 = simple_flow_queued.task2
    task2.add_trigger("../task1 == complete")
    assert not task2.check_dependencies()

    task1.complete()
    assert task2.check_dependencies()

    task2.init("111")
    assert task2.state.node_status == NodeStatus.active
    assert not task2.check_dependencies()

    task2.abort("trap")
    assert task2.state.node_status == NodeStatus.aborted
    assert not task2.check_dependencies()


# ---------------
# complex
# ---------------


@pytest.fixture
def simple_flow_without_time(simple_flow_queued, patch_datetime_now):
    flow1 = simple_flow_queued.flow1
    task1 = simple_flow_queued.task1
    task2 = simple_flow_queued.task2

    task1.add_event("event_a")
    task2.add_trigger("../task1 == complete")
    task2.add_complete_trigger("../task1:event_a == set")
    start_time = TEST_TIME
    flow1.calendar.begin(start_time)
    flow1.update_calendar(start_time)

    return simple_flow_queued


def test_node_check_dependencies_combine_before_time(
    simple_flow_without_time, patch_datetime_now
):
    task1 = simple_flow_without_time.task1
    task2 = simple_flow_without_time.task2

    task1.complete()
    task2.add_time("09:00")
    assert not task2.check_dependencies()


def test_node_check_dependencies_combine_at_time(
    simple_flow_without_time, patch_datetime_now
):
    task1 = simple_flow_without_time.task1
    task2 = simple_flow_without_time.task2

    task1.complete()
    task2.add_time("10:00")
    assert task2.check_dependencies()


def test_node_check_dependencies_combine_after_time(
    simple_flow_without_time, patch_datetime_now
):
    task1 = simple_flow_without_time.task1
    task2 = simple_flow_without_time.task2

    task1.complete()
    task2.add_time("11:00")
    assert not task2.check_dependencies()


def test_node_check_dependencies_combine_complete(
    simple_flow_without_time, patch_datetime_now
):
    task1 = simple_flow_without_time.task1
    task2 = simple_flow_without_time.task2

    task1.set_event("event_a", True)
    task2.add_time("10:00")
    assert task2.state.node_status == NodeStatus.queued
    assert not task2.is_complete_triggered

    assert not task2.check_dependencies()

    assert task2.state.node_status == NodeStatus.complete
    assert task2.is_complete_triggered


@pytest.fixture
def simple_flow_with_limit(simple_flow_queued):
    container2 = simple_flow_queued.container2
    container2.add_limit("limit1", 1)
    container2.add_in_limit("limit1")

    return simple_flow_queued


def test_node_check_dependencies_limit(simple_flow_with_limit):
    task2 = simple_flow_with_limit.task2
    task3 = simple_flow_with_limit.task3

    assert task2.check_dependencies()
    assert task3.check_dependencies()

    task2.run()
    # WARNING: task2.init not affect limit.
    assert not task3.check_dependencies()
