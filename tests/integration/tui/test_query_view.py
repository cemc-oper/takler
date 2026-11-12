"""Compact query view and lazy TUI interaction regressions."""

from __future__ import annotations

from collections import ChainMap
from dataclasses import replace
from types import MappingProxyType, SimpleNamespace

import pytest

from takler.exceptions import QueryResponseError
from takler.serialization.query_projection import (
    project_query_detail,
)
from takler.tui.app import TaklerTuiApp
from takler.tui.query_view import QueryTuiView
from textual.widgets import Static

from tests.tui_fixtures import (
    FakeTuiService,
    compact_snapshot,
    move_tree_cursor,
    text_of,
    wait_until,
)


class QueryFakeService(FakeTuiService):
    def __init__(self, bunch):
        super().__init__("")
        self.bunch = bunch
        self.snapshot_calls = 0
        self.detail_calls: list[tuple[str, tuple[str, ...]]] = []
        self.root_calls = 0
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

    def read_root_detail(self, cancelled=None):
        self.root_calls += 1
        return self.read_detail(None, "/", ["service", "parameters"])


class DeltaQueryFakeService(QueryFakeService):
    def __init__(self, bunch):
        super().__init__(bunch)
        self.since_calls = 0
        self.next_delta = None

    def read_snapshot(self, *, scope_path=None, depth=None, cancelled=None):
        return replace(
            super().read_snapshot(
                scope_path=scope_path, depth=depth, cancelled=cancelled
            ),
            revision=self.snapshot_calls,
        )

    def read_since(self, snapshot, cancelled=None):
        self.since_calls += 1
        result = self.next_delta
        self.next_delta = None
        if isinstance(result, Exception):
            raise result
        return snapshot if result is None else result


@pytest.mark.anyio
@pytest.mark.integration
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
        await move_tree_cursor(pilot, app._tree, "/flow1/family1/task1")
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
@pytest.mark.integration
async def test_query_tui_subtree_keeps_root_service_status(rich_bunch) -> None:
    service = QueryFakeService(rich_bunch)
    app = TaklerTuiApp(service, scope_path="/flow1/family1")
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: "/flow1/family1" in app._tree._path_nodes, pilot)
        assert app._snapshot.query.scope_path == "/flow1/family1"
        assert app._snapshot.service.status == "running"
        assert service.snapshot_calls == 1
        assert service.root_calls == 1
        assert len(app._tree._path_nodes) == 1
        app.action_scope_parent()
        await wait_until(lambda: app._snapshot.query.scope_path == "/flow1", pilot)
        assert app._snapshot.service.status == "running"
        app.action_scope_parent()
        await wait_until(lambda: app._snapshot.query.scope_path == "/", pilot)


@pytest.mark.anyio
@pytest.mark.integration
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


@pytest.mark.anyio
@pytest.mark.integration
async def test_query_tui_no_change_skips_tree_and_detail(rich_bunch) -> None:
    service = DeltaQueryFakeService(rich_bunch)
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: isinstance(app._snapshot, QueryTuiView), pilot)
        view = app._snapshot
        flow = app._tree._path_nodes["/flow1"]
        children = view.children
        app.action_refresh()
        await wait_until(lambda: service.since_calls == 1, pilot)
        await pilot.pause()
        assert service.snapshot_calls == 1
        assert app._snapshot is view
        assert view.children is children
        assert app._tree._path_nodes["/flow1"] is flow
        assert service.detail_calls == [("/", ("service", "parameters"))]


@pytest.mark.anyio
@pytest.mark.integration
async def test_query_tui_root_delta_refreshes_service_without_selection(
    rich_bunch,
) -> None:
    service = DeltaQueryFakeService(rich_bunch)
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: isinstance(app._snapshot, QueryTuiView), pilot)
        view = app._snapshot
        root = view.query.nodes["/"]
        service.next_delta = replace(
            view.query,
            nodes=ChainMap({"/": replace(root, status="active")}, view.query.nodes),
            revision=view.query.revision + 1,
            changed_paths=("/",),
        )
        app.action_refresh()
        await wait_until(lambda: view.query.revision > 1, pilot)
        assert app._snapshot is view
        assert service.detail_calls == [
            ("/", ("service", "parameters")),
            ("/", ("service", "parameters")),
        ]


@pytest.mark.anyio
@pytest.mark.integration
async def test_query_tui_sparse_delta_updates_only_visible_row(rich_bunch) -> None:
    service = DeltaQueryFakeService(rich_bunch)
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: isinstance(app._snapshot, QueryTuiView), pilot)
        view = app._snapshot
        flow = app._tree._path_nodes["/flow1"]
        flow.expand()
        await wait_until(lambda: "/flow1/task3" in app._tree._path_nodes, pilot)
        untouched = app._tree._path_nodes["/flow1/family1"]
        task = app._tree._path_nodes["/flow1/task3"]
        app._selected_path = "/flow1/task3"
        app._request_detail("/flow1/task3")
        await wait_until(lambda: len(service.detail_calls) == 2, pilot)
        before = view.query
        path = "/flow1/task3"
        changed = replace(before.nodes[path], status="queued")
        service.next_delta = replace(
            before,
            nodes=ChainMap({path: changed}, before.nodes),
            revision=before.revision + 1,
            changed_paths=(path,),
        )
        app.action_refresh()
        await wait_until(lambda: view.query.revision == before.revision + 1, pilot)
        assert app._snapshot is view
        assert service.snapshot_calls == 1
        assert app._tree._path_nodes["/flow1"] is flow
        assert app._tree._path_nodes["/flow1/family1"] is untouched
        assert app._tree._path_nodes[path] is task
        assert view.get(path).state == "queued"
        assert task.label.plain.endswith("task3")
        await wait_until(lambda: len(service.detail_calls) == 3, pilot)


@pytest.mark.anyio
@pytest.mark.integration
async def test_query_tui_removal_keeps_other_rows_and_clears_selection(
    rich_bunch,
) -> None:
    service = DeltaQueryFakeService(rich_bunch)
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: isinstance(app._snapshot, QueryTuiView), pilot)
        app._tree._path_nodes["/flow1"].expand()
        await wait_until(lambda: "/flow1/family1" in app._tree._path_nodes, pilot)
        family = app._tree._path_nodes["/flow1/family1"]
        family.expand()
        path = "/flow1/family1/task1"
        await wait_until(lambda: path in app._tree._path_nodes, pilot)
        app._selected_path = path
        before = app._snapshot.query
        removed = tuple(
            item
            for item in before.ordered_paths
            if item == path or item.startswith(path + "/")
        )
        service.next_delta = replace(
            before,
            nodes=MappingProxyType(
                {
                    key: value
                    for key, value in before.nodes.items()
                    if key not in removed
                }
            ),
            ordered_paths=tuple(
                item for item in before.ordered_paths if item not in removed
            ),
            revision=before.revision + 1,
            removed_paths=removed,
        )
        app.action_refresh()
        await wait_until(lambda: path not in app._tree._path_nodes, pilot)
        assert app._selected_path is None
        assert app._tree._path_nodes["/flow1/family1"] is family
        assert app._snapshot.get(path) is None


@pytest.mark.anyio
@pytest.mark.integration
async def test_query_tui_reset_and_disconnect_rebuild(rich_bunch) -> None:
    service = DeltaQueryFakeService(rich_bunch)
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: isinstance(app._snapshot, QueryTuiView), pilot)
        first = app._snapshot
        service.next_delta = QueryResponseError("reset_required", "window expired")
        app.action_refresh()
        await wait_until(lambda: app._snapshot is not first, pilot)
        assert service.snapshot_calls == 2
        second = app._snapshot
        service.next_delta = ConnectionError("offline")
        app.action_refresh()
        await wait_until(
            lambda: (
                "stale" in text_of(app._status_bar.query_one("#status-left", Static))
            ),
            pilot,
        )
        assert app._snapshot is second
        service.next_delta = QueryResponseError("stale_identity", "reconnect")
        app.action_refresh()
        await wait_until(lambda: app._snapshot is not second, pilot)
        assert service.snapshot_calls == 3
        assert "stale" not in text_of(app._status_bar.query_one("#status-left", Static))


@pytest.mark.anyio
@pytest.mark.integration
async def test_query_tui_expired_detail_rebuilds_selected_scope(rich_bunch) -> None:
    class ExpiringService(DeltaQueryFakeService):
        expire_once = True

        def read_detail(self, snapshot, path, groups, current=None):
            if path == "/flow1/task3" and self.expire_once:
                self.expire_once = False
                raise QueryResponseError("snapshot_expired", "query unavailable")
            return super().read_detail(snapshot, path, groups, current)

    service = ExpiringService(rich_bunch)
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: isinstance(app._snapshot, QueryTuiView), pilot)
        first = app._snapshot
        app._selected_path = "/flow1/task3"
        app._request_detail("/flow1/task3")
        await wait_until(lambda: app._snapshot is not first, pilot)
        assert service.snapshot_calls == 2
        assert service.since_calls == 0
        assert app._selected_path == "/flow1/task3"
        await wait_until(
            lambda: any(path == "/flow1/task3" for path, _ in service.detail_calls),
            pilot,
        )


@pytest.mark.anyio
@pytest.mark.integration
async def test_query_tui_unchanged_polls_preserve_detail_access(rich_bunch) -> None:
    service = DeltaQueryFakeService(rich_bunch)
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: isinstance(app._snapshot, QueryTuiView), pilot)
        first = app._snapshot
        # The fixture's page expiry is already more than two TTLs in the past.
        for count in range(1, 4):
            app.action_refresh()
            await wait_until(lambda: service.since_calls == count, pilot)
        assert app._snapshot is first
        assert service.snapshot_calls == 1
        app._selected_path = "/flow1/task3"
        app._request_detail("/flow1/task3")
        await wait_until(
            lambda: any(path == "/flow1/task3" for path, _ in service.detail_calls),
            pilot,
        )


@pytest.mark.anyio
@pytest.mark.integration
async def test_query_tui_disconnect_resumes_same_revision(rich_bunch) -> None:
    service = DeltaQueryFakeService(rich_bunch)
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: isinstance(app._snapshot, QueryTuiView), pilot)
        view = app._snapshot
        service.next_delta = ConnectionError("offline")
        app.action_refresh()
        await wait_until(
            lambda: (
                "stale" in text_of(app._status_bar.query_one("#status-left", Static))
            ),
            pilot,
        )
        app.action_refresh()
        await wait_until(lambda: service.since_calls == 2, pilot)
        await wait_until(
            lambda: (
                "stale"
                not in text_of(app._status_bar.query_one("#status-left", Static))
            ),
            pilot,
        )
        assert app._snapshot is view
        assert service.snapshot_calls == 1


@pytest.mark.anyio
@pytest.mark.integration
async def test_query_tui_local_invalidation_rebuilds_selected_scope(rich_bunch) -> None:
    service = DeltaQueryFakeService(rich_bunch)
    app = TaklerTuiApp(service, scope_path="/flow1")
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: isinstance(app._snapshot, QueryTuiView), pilot)
        first = app._snapshot
        service.next_delta = replace(
            first.query,
            revision=first.query.revision + 1,
            stale_scopes=("/flow1/family1",),
        )
        app.action_refresh()
        await wait_until(lambda: app._snapshot is not first, pilot)
        assert service.snapshot_calls == 2
        assert app._snapshot.query.scope_path == "/flow1"


@pytest.mark.anyio
@pytest.mark.integration
async def test_query_tui_rejects_late_delta(rich_bunch) -> None:
    service = DeltaQueryFakeService(rich_bunch)
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: isinstance(app._snapshot, QueryTuiView), pilot)
        view = app._snapshot
        before = view.query
        updated = replace(before, revision=before.revision + 1)
        app._apply_delta(view, updated, None, app._refresh_seq - 1)
        assert view.query is before
