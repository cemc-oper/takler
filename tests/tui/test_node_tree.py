"""Tests for :class:`takler.tui.widgets.node_tree.NodeTree`.

The row-label builders are pure functions and tested directly; rebuild /
cursor / selection bookkeeping goes through the pilot.
"""

from __future__ import annotations

import pytest
from textual.app import App, ComposeResult

from takler.tui.widgets.node_tree import (
    NodeTree,
    _attribute_rows,
    _event_label,
    _label_for,
    _meter_label,
)


# ---------------------------------------------------------------------------
# Label builders (pure)
# ---------------------------------------------------------------------------


def test_label_for_prefixes_state_block(snapshot) -> None:
    node = snapshot.get("/flow1/task3")
    label = _label_for(node)
    assert label.plain == "■■ task3"


def test_label_for_suspended_node_is_two_tone(snapshot) -> None:
    node = snapshot.get("/flow1/family1/task1")
    label = _label_for(node)
    assert label.plain == "■■ task1"
    assert len(label.spans) == 2  # suspended overlay + state colour


def test_attribute_rows_cover_every_kind(snapshot) -> None:
    task = snapshot.get("/flow1/family1/task1")
    kinds = [kind for kind, _ in _attribute_rows(task)]
    assert kinds == ["trigger", "complete", "repeat", "time", "event", "meter"]

    family = snapshot.get("/flow1/family1")
    assert [kind for kind, _ in _attribute_rows(family)] == ["limit"]

    task2 = snapshot.get("/flow1/family1/task2")
    assert [kind for kind, _ in _attribute_rows(task2)] == ["inlimit"]

    bare = snapshot.get("/flow1/task3")
    assert _attribute_rows(bare) == []


def test_attribute_row_bodies_carry_the_values(snapshot) -> None:
    task = snapshot.get("/flow1/family1/task1")
    bodies = {kind: label.plain for kind, label in _attribute_rows(task)}
    assert "./task2 == complete" in bodies["trigger"]
    assert "./task2:event_done" in bodies["complete"]
    assert "YMD" in bodies["repeat"]
    assert "12:00" in bodies["time"]
    assert "evt" in bodies["event"] and "[unset]" in bodies["event"]
    assert "mtr" in bodies["meter"] and "0..10" in bodies["meter"]


def test_event_label_marks_set_and_unset() -> None:
    assert "[set]" in _event_label("e", "set").plain
    assert "[unset]" in _event_label("e", "unset").plain


def test_meter_label_layout() -> None:
    assert _meter_label("m", "0", "10", "5").plain == "• meter  m  0..10  [5]"


def test_inlimit_row_shows_tokens_and_ref_when_present() -> None:
    class FakeNode:
        trigger = None
        complete_trigger = None
        repeat = None
        times = []
        limits = []
        events = []
        meters = []
        in_limits = [("big", 3, "/flow1/family1")]

    rows = _attribute_rows(FakeNode())
    assert [kind for kind, _ in rows] == ["inlimit"]
    body = rows[0][1].plain
    assert "tokens=3" in body
    assert "via /flow1/family1" in body


# ---------------------------------------------------------------------------
# Widget behaviour (pilot)
# ---------------------------------------------------------------------------


class TreeHost(App[None]):
    def __init__(self) -> None:
        super().__init__()
        self.node_tree = NodeTree()

    def compose(self) -> ComposeResult:
        yield self.node_tree


ALL_PATHS = {
    "/flow1",
    "/flow1/family1",
    "/flow1/family1/task1",
    "/flow1/family1/task2",
    "/flow1/task3",
}


@pytest.mark.anyio
async def test_rebuild_indexes_every_node_and_expands_on_first_build(
    snapshot,
) -> None:
    app = TreeHost()
    async with app.run_test(size=(100, 40)) as pilot:
        app.node_tree.rebuild(snapshot)
        await pilot.pause()
        assert set(app.node_tree._path_nodes) == ALL_PATHS
        for path in ("/flow1", "/flow1/family1", "/flow1/family1/task1"):
            tree_node = app.node_tree._path_nodes[path]
            assert tree_node.is_expanded, f"{path} should start expanded"
        # A bare node with neither children nor attributes is a leaf.
        assert app.node_tree._path_nodes["/flow1/task3"].allow_expand is False


@pytest.mark.anyio
async def test_attribute_rows_share_their_owners_path(snapshot) -> None:
    app = TreeHost()
    async with app.run_test(size=(100, 40)) as pilot:
        app.node_tree.rebuild(snapshot)
        await pilot.pause()
        tree_node = app.node_tree._path_nodes["/flow1/family1/task1"]
        attribute_leaves = list(tree_node.children)
        assert len(attribute_leaves) == 6  # trigger..meter
        for leaf in attribute_leaves:
            # Selection / right-click on an attribute row must target
            # the owning node, hence the shared data path.
            assert leaf.data == "/flow1/family1/task1"
            assert app.node_tree.is_real_node(leaf) is False
        assert app.node_tree.is_real_node(tree_node) is True


@pytest.mark.anyio
async def test_rebuild_preserves_collapse_state(snapshot) -> None:
    app = TreeHost()
    async with app.run_test(size=(100, 40)) as pilot:
        app.node_tree.rebuild(snapshot)
        await pilot.pause()
        app.node_tree._path_nodes["/flow1/family1"].collapse()
        app.node_tree.rebuild(snapshot)
        await pilot.pause()
        assert app.node_tree._path_nodes["/flow1/family1"].is_expanded is False
        # Untouched nodes stay expanded.
        assert app.node_tree._path_nodes["/flow1"].is_expanded is True


@pytest.mark.anyio
async def test_rebuild_preserves_the_cursor(snapshot) -> None:
    app = TreeHost()
    async with app.run_test(size=(100, 40)) as pilot:
        app.node_tree.rebuild(snapshot)
        await pilot.pause()
        app.node_tree.select_path("/flow1/task3")
        await pilot.pause()
        app.node_tree.rebuild(snapshot)
        await pilot.pause()
        cursor_node = app.node_tree.node_at_line(app.node_tree.cursor_line)
        assert cursor_node is not None
        assert cursor_node.data == "/flow1/task3"


@pytest.mark.anyio
async def test_line_lookups_are_bounds_safe(snapshot) -> None:
    app = TreeHost()
    async with app.run_test(size=(100, 40)) as pilot:
        app.node_tree.rebuild(snapshot)
        await pilot.pause()
        assert app.node_tree.node_at_line(-1) is None
        assert app.node_tree.node_at_line(10**6) is None
        assert app.node_tree.node_at_hover() is None  # no hover in a pilot test
        assert app.node_tree.path_at_hover() is None


@pytest.mark.anyio
async def test_select_path_moves_cursor_and_ignores_unknown_paths(snapshot) -> None:
    app = TreeHost()
    async with app.run_test(size=(100, 40)) as pilot:
        app.node_tree.rebuild(snapshot)
        await pilot.pause()
        app.node_tree.select_path("/missing")
        await pilot.pause()

        app.node_tree.select_path("/flow1/family1/task2")
        await pilot.pause()
        cursor_node = app.node_tree.node_at_line(app.node_tree.cursor_line)
        assert cursor_node is not None
        assert cursor_node.data == "/flow1/family1/task2"


@pytest.mark.anyio
async def test_anchor_for_cursor_stays_inside_the_tree(snapshot) -> None:
    app = TreeHost()
    async with app.run_test(size=(100, 40)) as pilot:
        app.node_tree.rebuild(snapshot)
        await pilot.pause()
        anchor = app.node_tree.anchor_for_cursor()
        assert anchor.x >= 0
        assert anchor.y >= 0
