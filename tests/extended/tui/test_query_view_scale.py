"""Exercise lazy TUI row materialization at a 100,101-node scale."""

from collections import ChainMap
from dataclasses import replace
from types import MappingProxyType

from takler.client.query_client import QueryNode, QuerySnapshot
from takler.tui.query_view import QueryTuiView
from takler.tui.widgets.node_tree import NodeTree


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
    visible = tree._path_nodes["/flow000/task0000"]
    hidden = "/flow099/task0999"
    updated = replace(
        snapshot,
        nodes=ChainMap(
            {
                "/flow000/task0000": replace(
                    nodes["/flow000/task0000"], status="queued"
                ),
                hidden: replace(nodes[hidden], status="queued"),
            },
            snapshot.nodes,
        ),
        revision=1,
        changed_paths=("/flow000/task0000", hidden),
    )
    view.apply_delta(updated)
    tree.apply_query_delta(view, updated.changed_paths, ())
    assert len(tree._path_nodes) == 612
    assert tree._path_nodes["/flow000/task0000"] is visible
    assert hidden not in view._nodes
