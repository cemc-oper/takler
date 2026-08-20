"""Unit tests for ``Scheduler.run_command_begin``, load semantics and main loop skipping.

Covers Requirements 8.1, 8.8, 8.9, 8.13.
"""
import datetime
import json

import pytest

from takler.core import Bunch, Flow, NodeStatus
from takler.exceptions import FlowStateError, NodeNotFoundError
from takler.server.scheduler import Scheduler


def build_flow(name: str) -> Flow:
    flow = Flow(name)
    with flow:
        flow.add_task("task1")
        flow.add_task("task2")
    return flow


@pytest.fixture
def scheduler() -> Scheduler:
    return Scheduler(bunch=Bunch(name="bunch"))


# begin: single flow ----------------------------------------------------


def test_begin_single_flow(scheduler):
    """begin on a named flow starts its calendar and marks it begun."""
    flow = scheduler.bunch.add_flow(build_flow("flow1"))
    assert flow.begun is False

    scheduler.run_command_begin("flow1")

    assert flow.begun is True
    assert flow.calendar.initial_time is not None
    assert flow.find_node("/flow1/task1").state.node_status is NodeStatus.queued


def test_begin_unknown_flow_raises_node_not_found(scheduler):
    """Requirement 8.13: an unknown flow name is a NodeNotFoundError."""
    scheduler.bunch.add_flow(build_flow("flow1"))

    with pytest.raises(NodeNotFoundError) as exc_info:
        scheduler.run_command_begin("no_such_flow")

    assert exc_info.value.node_path == "/no_such_flow"


def test_begin_already_begun_flow_rejected(scheduler):
    """An already begun flow is rejected without force, and keeps its state."""
    flow = scheduler.bunch.add_flow(build_flow("flow1"))
    scheduler.run_command_begin("flow1")
    initial_time = flow.calendar.initial_time
    flow.find_node("/flow1/task1").set_node_status(NodeStatus.complete)

    with pytest.raises(FlowStateError) as exc_info:
        scheduler.run_command_begin("flow1")

    assert "flow1" in str(exc_info.value)
    assert flow.calendar.initial_time == initial_time
    assert flow.find_node("/flow1/task1").state.node_status is NodeStatus.complete


def test_begin_force_restarts_begun_flow(scheduler):
    """force begins again: calendar restarted and node tree reset."""
    flow = scheduler.bunch.add_flow(build_flow("flow1"))
    scheduler.run_command_begin("flow1")
    initial_time = flow.calendar.initial_time
    flow.find_node("/flow1/task1").set_node_status(NodeStatus.complete)

    scheduler.run_command_begin("flow1", force=True)

    assert flow.begun is True
    assert flow.calendar.initial_time >= initial_time
    assert flow.find_node("/flow1/task1").state.node_status is NodeStatus.queued


# begin: all flows ------------------------------------------------------


@pytest.mark.parametrize("flow_name", [None, ""])
def test_begin_all_flows(scheduler, flow_name):
    """Requirement 8.1: None or empty string means all flows."""
    flows = [scheduler.bunch.add_flow(build_flow(f"flow{i}")) for i in range(3)]

    scheduler.run_command_begin(flow_name)

    for flow in flows:
        assert flow.begun is True
        assert flow.calendar.initial_time is not None


def test_begin_all_flows_is_all_or_nothing(scheduler):
    """One already begun flow fails the whole command without touching the others."""
    scheduler.bunch.add_flow(build_flow("flow1"))
    flow2 = scheduler.bunch.add_flow(build_flow("flow2"))
    scheduler.run_command_begin("flow1")

    with pytest.raises(FlowStateError) as exc_info:
        scheduler.run_command_begin(None)

    assert "flow1" in str(exc_info.value)
    assert flow2.begun is False
    assert flow2.calendar.initial_time is None


def test_begin_all_flows_with_force(scheduler):
    """With force, a mix of begun and un-begun flows all end up begun."""
    flow1 = scheduler.bunch.add_flow(build_flow("flow1"))
    flow2 = scheduler.bunch.add_flow(build_flow("flow2"))
    scheduler.run_command_begin("flow1")

    scheduler.run_command_begin("", force=True)

    assert flow1.begun is True
    assert flow2.begun is True


def test_begin_all_on_empty_bunch_is_noop(scheduler):
    """begin over an empty bunch does nothing and does not raise."""
    scheduler.run_command_begin(None)

    assert scheduler.bunch.flows == {}


# load ------------------------------------------------------------------


def test_load_leaves_flow_not_begun(scheduler):
    """Requirement 8.8: load registers the flow but leaves it un-begun."""
    flow_dict = build_flow("flow1").to_dict()

    scheduler.run_command_load("json", json.dumps(flow_dict).encode("utf-8"))

    loaded = scheduler.bunch.find_flow("flow1")
    assert loaded is not None
    assert loaded.begun is False
    assert loaded.calendar.initial_time is None
    assert loaded.find_node("/flow1/task1").state.node_status is NodeStatus.unknown


def test_load_then_begin_starts_flow(scheduler):
    """A loaded flow becomes runnable once begin is called."""
    flow_dict = build_flow("flow1").to_dict()
    scheduler.run_command_load("json", json.dumps(flow_dict).encode("utf-8"))

    scheduler.run_command_begin("flow1")

    loaded = scheduler.bunch.find_flow("flow1")
    assert loaded.begun is True
    assert loaded.find_node("/flow1/task1").state.node_status is NodeStatus.queued


# main loop skipping ----------------------------------------------------


def test_process_flow_skips_not_begun_flow(scheduler):
    """Requirement 8.9: an un-begun flow gets neither calendar update nor solving."""
    flow = scheduler.bunch.add_flow(build_flow("flow1"))

    scheduler._process_flow("flow1", flow, datetime.datetime.now())

    # Calendar untouched: all fields still None (updating them would have
    # raised TypeError instead).
    assert flow.calendar.initial_time is None
    assert flow.calendar.flow_time is None
    # Dependencies not resolved: no task was submitted.
    assert flow.find_node("/flow1/task1").state.node_status is NodeStatus.unknown


def test_process_flow_processes_begun_flow(scheduler):
    """A begun flow is processed: calendar advances and dependencies resolve."""
    flow = scheduler.bunch.add_flow(build_flow("flow1"))
    scheduler.run_command_begin("flow1")
    flow_time_before = flow.calendar.flow_time

    scheduler._process_flow(
        "flow1", flow, datetime.datetime.now() + datetime.timedelta(seconds=1)
    )

    assert flow.calendar.flow_time > flow_time_before
    assert flow.find_node("/flow1/task1").state.node_status is NodeStatus.submitted
