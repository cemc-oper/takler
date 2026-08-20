"""Self-tests for the shared hypothesis generators in ``tests/strategies.py``.

These are not the property tests of the feature; they only check that the
generators produce usable objects and that the requirement 8.6 invariant
(``begun`` is true iff ``calendar.initial_time`` is not ``None``) can never be
violated by a generated value.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from takler.core import Bunch, Flow, NodeStatus, SerializationType
from takler.core.node import Node
from takler.exceptions import ExpressionSyntaxError, TaklerError
from takler.core.expression_parser import parse_trigger

from tests.strategies import (
    MAX_TREE_DEPTH,
    apply_flow_operations,
    bunches,
    flow_operation_sequences,
    flows,
    foreign_exception_types,
    invalid_node_paths,
    invalid_trigger_expressions,
    node_paths_of,
    nonexistent_node_paths,
    takler_exception_types,
)


def _depth_of(node: Node, depth: int = 1) -> int:
    if not node.children:
        return depth
    return max(_depth_of(child, depth + 1) for child in node.children)


def _check_begun_invariant(flow: Flow) -> None:
    assert flow.begun == (flow.calendar.initial_time is not None)
    if not flow.begun:
        assert flow.calendar.to_dict() == dict(
            initial_time=None,
            flow_time=None,
            duration=None,
            increment=None,
            initial_real_time=None,
            last_real_time=None,
        )
    else:
        assert flow.calendar.flow_time is not None
        assert flow.calendar.flow_time >= flow.calendar.initial_time


@settings(max_examples=100, deadline=None)
@given(bunches())
def test_bunches_are_valid(bunch: Bunch):
    assert isinstance(bunch, Bunch)
    assert 1 <= len(bunch.flows) <= 3

    for name, flow in bunch.flows.items():
        assert flow.name == name
        assert flow.get_bunch() is bunch
        assert _depth_of(flow) <= MAX_TREE_DEPTH
        _check_begun_invariant(flow)

        # every node keeps a valid status and a reachable path
        for path in node_paths_of(flow):
            node = bunch.find_node(path)
            assert node is not None
            assert isinstance(node.state.node_status, NodeStatus)

    # the whole bunch survives a snapshot round trip
    restored = Bunch.from_dict(bunch.to_dict())
    assert restored.to_dict() == bunch.to_dict()


@settings(max_examples=50, deadline=None)
@given(flows())
def test_flows_round_trip_both_methods(flow: Flow):
    status_restored = Flow.from_dict(flow.to_dict(), method=SerializationType.Status)
    _check_begun_invariant(status_restored)
    assert status_restored.begun == flow.begun

    tree_restored = Flow.from_dict(flow.to_dict(), method=SerializationType.Tree)
    assert tree_restored.begun is False
    _check_begun_invariant(tree_restored)


@settings(max_examples=100, deadline=None)
@given(flow_operation_sequences())
def test_full_operation_sequences_keep_begun_invariant(pair):
    flow, operations = pair
    _check_begun_invariant(flow)
    for operation in operations:
        flow = apply_flow_operations(flow, [operation])
        _check_begun_invariant(flow)


@settings(max_examples=50, deadline=None)
@given(flow_operation_sequences(include_begin=False))
def test_begin_free_operation_sequences_never_start_calendar(pair):
    flow, operations = pair
    assert all(operation.name != "begin" for operation in operations)
    before = flow.calendar.to_dict()
    was_begun = flow.begun

    flow = apply_flow_operations(flow, operations)

    if any(operation.name == "roundtrip_tree" for operation in operations):
        # a Tree round trip deliberately drops the runtime state
        _check_begun_invariant(flow)
    else:
        assert flow.begun == was_begun
        assert flow.calendar.to_dict() == before


@settings(max_examples=50, deadline=None)
@given(invalid_node_paths())
def test_invalid_node_paths_are_rejected(path: str):
    assert not Node.check_absolute_node_path(path)


@settings(max_examples=50, deadline=None)
@given(st.data(), bunches(max_flows=1))
def test_nonexistent_node_paths_are_not_found(data, bunch: Bunch):
    existing = [p for flow in bunch.flows.values() for p in node_paths_of(flow)]
    path = data.draw(nonexistent_node_paths(existing))
    assert Node.check_absolute_node_path(path)
    assert path not in existing
    assert bunch.find_node(path) is None


@settings(max_examples=50, deadline=None)
@given(takler_exception_types(), foreign_exception_types())
def test_exception_type_generators(takler_type, foreign_type):
    assert issubclass(takler_type, TaklerError)
    assert not issubclass(foreign_type, TaklerError)
    assert str(takler_type("boom")).find("boom") >= 0


@settings(max_examples=100, deadline=None)
@given(invalid_trigger_expressions())
def test_invalid_trigger_expressions_really_fail(expression: str):
    try:
        parse_trigger(expression)
    except ExpressionSyntaxError as exc:
        assert exc.expression == expression
    else:
        raise AssertionError(f"expression unexpectedly parsed: {expression!r}")
