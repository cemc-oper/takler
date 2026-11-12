"""Project compact query state into TUI view models."""

from dataclasses import replace
from types import MappingProxyType, SimpleNamespace

from takler.client.query_client import QueryNode, QuerySnapshot
from takler.core import Bunch, Flow
from takler.serialization.query_projection import project_query_detail
from takler.tui.query_view import QueryTuiView
from takler.tui.widgets.node_tree import NodeTree, _label_for
from tests.tui_fixtures import compact_snapshot


def test_unknown_submission_visible_in_lazy_tui_view():
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task("t")
    flow.begin()
    task.run()
    task.mark_submission_unknown("submit reply lost")
    view = QueryTuiView(compact_snapshot(bunch))
    node = view.get("/f/t")
    assert node.submission_unknown
    assert node.display_state == "submitted; submission unknown"
    assert "submission unknown" in _label_for(node).plain
    view.apply_detail(
        "/f/t",
        SimpleNamespace(groups=project_query_detail(bunch, "/f/t", ["runtime"])),
    )
    assert node.submission_unknown_reason == "submit reply lost"


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


def test_query_delta_refreshes_visible_rows_and_defers_hidden_rows(monkeypatch):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    visible = flow.add_task("first").node_path
    hidden = flow.add_task("second").node_path
    snapshot = compact_snapshot(bunch)
    view = QueryTuiView(snapshot)
    tree = NodeTree()
    monkeypatch.setattr(tree, "QUERY_CHILD_BATCH", 1)
    tree.rebuild_query(view)
    tree._load_query_children("/f")
    row = tree._path_nodes[visible]
    previous_style = list(row.label.spans)

    nodes = dict(snapshot.nodes)
    for path in (visible, hidden):
        nodes[path] = replace(nodes[path], status="queued")
    updated = replace(
        snapshot,
        nodes=MappingProxyType(nodes),
        revision=1,
        changed_paths=(visible, hidden),
    )
    view.apply_delta(updated)
    tree.apply_query_delta(view, updated.changed_paths, ())

    assert tree._path_nodes[visible] is row
    assert row.label.spans != previous_style
    assert view.get(visible).display_state == "queued"
    assert hidden not in tree._path_nodes
    assert hidden not in view._nodes

    tree._load_query_children("/f")
    assert tree._path_nodes[hidden].label.spans == _label_for(view.get(hidden)).spans
    assert view.get(hidden).display_state == "queued"
