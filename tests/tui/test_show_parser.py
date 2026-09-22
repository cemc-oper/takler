"""Unit tests for :mod:`takler.tui.show_parser`.

``parse_show`` reconstructs a real :class:`~takler.core.Bunch` from the
``show`` JSON payload; these tests pin the projection into
:class:`NodeInfo` / :class:`ShowSnapshot` that the tree and the tabs
read from.
"""

from __future__ import annotations

import json

import pytest

from takler.core import Bunch, Flow
from takler.core.task_node import Task
from takler.tui.show_parser import NodeInfo, ShowSnapshot, parse_show

from .conftest import show_payload


def test_parse_builds_snapshot_over_every_node(snapshot: ShowSnapshot) -> None:
    assert set(snapshot.nodes) == {
        "/flow1",
        "/flow1/family1",
        "/flow1/family1/task1",
        "/flow1/family1/task2",
        "/flow1/task3",
    }
    assert snapshot.roots == ["/flow1"]


def test_node_info_identity_and_level(snapshot: ShowSnapshot) -> None:
    flow = snapshot.get("/flow1")
    assert flow is not None
    assert flow.is_root is True
    assert flow.level == 0
    assert flow.parent_path is None
    assert flow.class_name == "Flow"

    task = snapshot.get("/flow1/family1/task1")
    assert task is not None
    assert task.is_root is False
    assert task.level == 2
    assert task.parent_path == "/flow1/family1"
    assert task.class_name == "Task"
    assert task.children == []

    family = snapshot.get("/flow1/family1")
    assert family is not None
    assert family.children == ["/flow1/family1/task1", "/flow1/family1/task2"]


def test_get_unknown_path_returns_none(snapshot: ShowSnapshot) -> None:
    assert snapshot.get("/nope") is None


def test_state_projection_and_suspended_display(snapshot: ShowSnapshot) -> None:
    task = snapshot.get("/flow1/family1/task1")
    assert task is not None
    assert task.state == "unknown"
    assert task.suspended is True
    assert task.display_state == "suspend (unknown)"

    plain = snapshot.get("/flow1/task3")
    assert plain is not None
    assert plain.suspended is False
    assert plain.display_state == "unknown"


def test_parameter_projection(snapshot: ShowSnapshot) -> None:
    flow = snapshot.get("/flow1")
    assert flow is not None
    assert flow.user_parameters == {"FLOW_HOME": "/flow"}
    # all_parameters is a defensive copy of the node-local parameters.
    params = flow.all_parameters
    params["FLOW_HOME"] = "mutated"
    assert flow.user_parameters["FLOW_HOME"] == "/flow"

    task = snapshot.get("/flow1/family1/task1")
    assert task is not None
    assert task.user_parameters["TAKLER_HOME"] == "/tmp/takler_home"


def test_dependency_and_attribute_projection(snapshot: ShowSnapshot) -> None:
    task = snapshot.get("/flow1/family1/task1")
    assert task is not None
    assert task.trigger == "./task2 == complete"
    assert task.complete_trigger == "./task2:event_done"
    assert task.times == ["12:00"]
    assert task.repeat == "YMD 20240101 [2024-01-01, 2024-01-31]"
    assert task.events == [("evt", "unset")]
    assert task.meters == [("mtr", "0", "10", "0")]

    family = snapshot.get("/flow1/family1")
    assert family is not None
    assert family.limits == [("big", "0/2")]

    task2 = snapshot.get("/flow1/family1/task2")
    assert task2 is not None
    assert task2.in_limits == [("big", 1, None)]


def test_nodes_without_attributes_project_empty(snapshot: ShowSnapshot) -> None:
    task3 = snapshot.get("/flow1/task3")
    assert task3 is not None
    assert task3.trigger is None
    assert task3.complete_trigger is None
    assert task3.repeat is None
    assert task3.times == []
    assert task3.events == []
    assert task3.meters == []
    assert task3.limits == []
    assert task3.in_limits == []
    assert task3.user_parameters == {}


def test_find_node_returns_domain_objects(snapshot: ShowSnapshot) -> None:
    node = snapshot.find_node("/flow1/family1/task1")
    assert isinstance(node, Task)
    assert snapshot.find_node("/missing") is None


def test_parents_of_walks_immediate_parent_first(snapshot: ShowSnapshot) -> None:
    parents = snapshot.parents_of("/flow1/family1/task1")
    assert [p.path for p in parents] == ["/flow1/family1", "/flow1"]
    assert snapshot.parents_of("/flow1") == []
    assert snapshot.parents_of("/missing") == []


def test_lookup_parameter_follows_the_inheritance_chain(
    snapshot: ShowSnapshot,
) -> None:
    # Node-local parameter wins.
    assert (
        snapshot.lookup_parameter("/flow1/family1/task1", "TAKLER_HOME")
        == "/tmp/takler_home"
    )
    # Inherited from an ancestor.
    assert snapshot.lookup_parameter("/flow1/family1/task2", "FLOW_HOME") == "/flow"
    # Unknown name / unknown path both resolve to None.
    assert snapshot.lookup_parameter("/flow1/family1/task2", "NOPE") is None
    assert snapshot.lookup_parameter("/missing", "FLOW_HOME") is None


def test_server_parameters_projection(snapshot: ShowSnapshot) -> None:
    # ``server_parameters`` mirrors the bunch node's own user parameters;
    # the rich fixture defines none at bunch level.
    assert snapshot.server_parameters == {}


def test_parse_rejects_invalid_json() -> None:
    with pytest.raises(json.JSONDecodeError):
        parse_show("not json")


def test_parse_empty_bunch_has_no_roots() -> None:
    snapshot = parse_show(show_payload(Bunch(name="empty")))
    assert snapshot.roots == []
    assert snapshot.nodes == {}


def test_multiple_flows_become_multiple_roots() -> None:
    bunch = Bunch(name="multi")
    bunch.add_flow(Flow("a"))
    bunch.add_flow(Flow("b"))
    snapshot = parse_show(show_payload(bunch))
    assert snapshot.roots == ["/a", "/b"]


def test_event_set_state_is_projected(rich_payload: str) -> None:
    # Flip the event on the reconstructed domain node, re-serialise and
    # confirm the projection reports "set".
    snapshot = parse_show(rich_payload)
    task = snapshot.find_node("/flow1/family1/task1")
    assert isinstance(task, Task)
    task.set_event("evt", True)
    reparsed = parse_show(show_payload(snapshot.bunch))
    info = reparsed.get("/flow1/family1/task1")
    assert info is not None
    assert ("evt", "set") in info.events


def test_snapshot_is_pure_view_over_reused_bunch(snapshot: ShowSnapshot) -> None:
    # NodeInfo wraps the very nodes of the reconstructed bunch -- the
    # tabs rely on this for Task type checks.
    info = snapshot.get("/flow1/family1/task1")
    assert info is not None
    assert info.node is snapshot.bunch.find_node("/flow1/family1/task1")
    assert isinstance(info, NodeInfo)


def test_build_rich_bunch_fixture_is_self_consistent(rich_bunch: Bunch) -> None:
    # Guard the fixture itself: a silent rename here would turn every
    # downstream assertion into a false pass/fail.
    assert rich_bunch.find_node("/flow1/family1/task1") is not None
    assert rich_bunch.find_node("/flow1/family1/task2") is not None
    assert rich_bunch.find_node("/flow1/task3") is not None
