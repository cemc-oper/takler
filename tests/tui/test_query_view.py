"""Compact query view and lazy TUI interaction regressions."""

from __future__ import annotations

from types import MappingProxyType, SimpleNamespace

import pytest

from takler.client.query_client import QueryNode, QuerySnapshot
from takler.serialization.query_projection import (
    iter_query_summaries,
    project_query_detail,
)
from takler.tui.app import TaklerTuiApp
from takler.tui.query_view import QueryTuiView
from takler.tui.widgets.node_tree import NodeTree
from textual.widgets import Static

from .conftest import FakeTuiService, text_of, wait_until


def compact_snapshot(bunch, scope_path="/", depth=None) -> QuerySnapshot:
    rows = [
        QueryNode.from_wire(row)
        for row, _ in iter_query_summaries(bunch, scope_path=scope_path, depth=depth)
    ]
    paths = tuple(row.path for row in rows)
    return QuerySnapshot(
        session_id="session",
        query_id="query",
        snapshot_id="snapshot",
        as_of="2026-10-08T00:00:00Z",
        expires_at="2026-10-08T00:01:00Z",
        root_name=bunch.name,
        scope_path=scope_path,
        depth=depth,
        nodes=MappingProxyType({row.path: row for row in rows}),
        ordered_paths=paths,
        flow_generations=MappingProxyType(
            {
                "/" + name: flow.definition_generation
                for name, flow in bunch.flows.items()
            }
        ),
        estimated_bytes=1024 + 512 * len(rows),
    )


class QueryFakeService(FakeTuiService):
    def __init__(self, bunch):
        super().__init__("")
        self.bunch = bunch
        self.snapshot_calls = 0
        self.detail_calls: list[tuple[str, tuple[str, ...]]] = []
        self.fail_next_snapshot = False

    def read_snapshot(self, *, scope_path=None, depth=None, cancelled=None):
        self.snapshot_calls += 1
        if self.fail_next_snapshot:
            self.fail_next_snapshot = False
            raise ConnectionError("offline")
        return compact_snapshot(self.bunch, scope_path or "/", depth)

    def read_detail(self, snapshot, path, groups, current=None):
        self.detail_calls.append((path, tuple(groups)))
        return SimpleNamespace(groups=project_query_detail(self.bunch, path, groups))


@pytest.mark.anyio
async def test_query_tui_loads_detail_only_for_selection(rich_bunch) -> None:
    service = QueryFakeService(rich_bunch)
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: "/flow1" in app._tree._path_nodes, pilot)
        assert service.snapshot_calls == 1
        assert service.show_calls == []
        assert service.detail_calls == [("/", ("service", "parameters"))]
        assert "/flow1/family1" not in app._tree._path_nodes
        app._tree._path_nodes["/flow1"].expand()
        await wait_until(lambda: "/flow1/family1" in app._tree._path_nodes, pilot)
        app._tree._path_nodes["/flow1/family1"].expand()
        await wait_until(lambda: "/flow1/family1/task1" in app._tree._path_nodes, pilot)
        app._tree.focus()
        app._tree.select_path("/flow1/family1/task1")
        await pilot.press("enter")
        await wait_until(lambda: len(service.detail_calls) == 2, pilot)
        await wait_until(
            lambda: app._snapshot.get("/flow1/family1/task1").trigger is not None,
            pilot,
        )
        assert (
            app._snapshot.inherited_parameters("/flow1/family1/task1")["FLOW_HOME"]
            == "/flow"
        )
        assert app._selected_path == "/flow1/family1/task1"
        tree_node = app._tree._path_nodes["/flow1/family1/task1"]
        before_rows = len(tree_node.children)
        assert before_rows > 0
        assert tree_node.allow_expand
        app._tree.update_query_detail("/flow1/family1/task1")
        assert len(tree_node.children) == before_rows
        stale_view = app._snapshot
        stale_seq = app._detail_seq
        app.action_refresh()
        await wait_until(lambda: service.snapshot_calls == 2, pilot)
        await wait_until(lambda: app._tree._path_nodes["/flow1"].is_expanded, pilot)
        assert app._selected_path == "/flow1/family1/task1"
        app._apply_detail(
            stale_view,
            "/flow1/family1/task1",
            stale_seq,
            service.read_detail(
                stale_view.query, "/flow1/family1/task1", ["parameters"]
            ),
        )
        assert app._snapshot is not stale_view
        rich_bunch.find_node("/flow1/family1").delete_child("task1")
        app.action_refresh()
        await wait_until(
            lambda: app._snapshot.query.get("/flow1/family1/task1") is None,
            pilot,
        )
        assert app._selected_path is None


@pytest.mark.anyio
async def test_query_tui_subtree_keeps_root_service_status(rich_bunch) -> None:
    service = QueryFakeService(rich_bunch)
    app = TaklerTuiApp(service, scope_path="/flow1/family1")
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: "/flow1/family1" in app._tree._path_nodes, pilot)
        assert app._snapshot.query.scope_path == "/flow1/family1"
        assert app._snapshot.service.status == "running"
        assert service.snapshot_calls == 2  # subtree plus root status
        assert len(app._tree._path_nodes) == 1
        app.action_scope_parent()
        await wait_until(lambda: app._snapshot.query.scope_path == "/flow1", pilot)
        assert app._snapshot.service.status == "running"
        app.action_scope_parent()
        await wait_until(lambda: app._snapshot.query.scope_path == "/", pilot)


@pytest.mark.anyio
async def test_query_tui_marks_failed_refresh_stale_and_keeps_view(rich_bunch) -> None:
    service = QueryFakeService(rich_bunch)
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: app._snapshot is not None, pilot)
        old = app._snapshot
        service.fail_next_snapshot = True
        app.action_refresh()
        await wait_until(
            lambda: (
                "stale" in text_of(app._status_bar.query_one("#status-left", Static))
            ),
            pilot,
        )
        assert app._snapshot is old
        app.action_refresh()
        await wait_until(lambda: app._snapshot is not old, pilot)
        assert "stale" not in text_of(app._status_bar.query_one("#status-left", Static))


def test_query_view_materializes_only_requested_nodes() -> None:
    nodes = {"/": QueryNode("/", "bunch", "takler.bunch", "unknown", False, None)}
    paths = ["/"]
    for flow_index in range(100):
        flow_path = f"/flow{flow_index:03d}"
        nodes[flow_path] = QueryNode(
            flow_path, "flow", "takler.flow", "unknown", False, None
        )
        paths.append(flow_path)
        for task_index in range(1000):
            task_path = f"{flow_path}/task{task_index:04d}"
            nodes[task_path] = QueryNode(
                task_path, "task", "takler.task", "unknown", False, None
            )
            paths.append(task_path)
    snapshot = QuerySnapshot(
        "session",
        "query",
        "snapshot",
        "2026-10-08T00:00:00Z",
        "2026-10-08T00:01:00Z",
        "large",
        "/",
        None,
        MappingProxyType(nodes),
        tuple(paths),
        MappingProxyType({}),
        1024 + 512 * len(paths),
    )
    view = QueryTuiView(snapshot)
    assert len(view.query.ordered_paths) == 100101
    assert len(view.roots) == 100
    assert view._nodes == {}
    tree = NodeTree()
    tree.rebuild_query(view)
    assert len(tree._path_nodes) == 100
    tree._load_query_children("/flow000")
    assert len(tree._path_nodes) == 356
    assert len(view._nodes) == 356
    assert tree._query_loaded["/flow000"] == 256
    tree._load_query_children("/flow000")
    assert len(tree._path_nodes) == 612
    assert tree._query_loaded["/flow000"] == 512


def test_query_view_pages_flat_root_rows() -> None:
    rows = {"/": QueryNode("/", "bunch", "takler.bunch", "unknown", False, None)}
    for index in range(600):
        path = f"/flow{index:03d}"
        rows[path] = QueryNode(path, "flow", "takler.flow", "unknown", False, None)
    snapshot = QuerySnapshot(
        "session",
        "query",
        "snapshot",
        "2026-10-08T00:00:00Z",
        "2026-10-08T00:01:00Z",
        "flat",
        "/",
        None,
        MappingProxyType(rows),
        tuple(rows),
        MappingProxyType({}),
        1024 + 512 * len(rows),
    )
    tree = NodeTree()
    tree.rebuild_query(QueryTuiView(snapshot))
    assert len(tree._path_nodes) == 256
    assert tree._query_root_loaded == 256
    tree._load_query_roots()
    assert len(tree._path_nodes) == 512
    tree._load_query_roots()
    assert len(tree._path_nodes) == 600
