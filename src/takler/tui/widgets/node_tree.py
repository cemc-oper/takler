"""The bunch tree widget for the takler TUI.

:class:`NodeTree` renders the bunch hierarchy as a Textual ``Tree`` and
folds the per-node attribute summaries (triggers, repeats, limits,
events, meters, …) in as synthetic leaf rows beneath their owning node.

The widget owns the path → :class:`~textual.widgets.tree.TreeNode`
index and all of the rebuild bookkeeping (preserving expand / collapse
state and the cursor across refreshes), so the app only has to call
:meth:`rebuild` with a fresh snapshot and use the small lookup helpers
(:meth:`path_at_hover`, :meth:`node_at_line`, :meth:`select_path`,
:meth:`anchor_for_cursor`) for menu / selection wiring.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from rich.text import Text
from textual.geometry import Offset
from textual.widgets import Tree
from textual.widgets.tree import TreeNode

from ..show_parser import NodeInfo, ShowSnapshot
from ..query_view import QueryTuiView
from ..state_style import state_block


# Styling for attribute leaves shown inline under each node. Tweak here
# rather than in the rendering function so all kinds line up.
_ATTR_STYLES = {
    "trigger": "yellow",
    "complete": "yellow",
    "repeat": "cyan",
    "limit": "magenta",
    "inlimit": "magenta",
    "time": "blue",
    "event": "green",
    "meter": "green",
}


def _attr_label(kind: str, body: str) -> Text:
    """Tree label for a synthetic attribute row.

    Looks like ``  • trigger  ./pre == complete`` with the kind tag
    coloured by :data:`_ATTR_STYLES` and the body in dim.
    """
    style = _ATTR_STYLES.get(kind, "white")
    label = Text("• ", style="dim")
    label.append(kind, style=style)
    label.append("  ")
    label.append(body[:256] + ("…" if len(body) > 256 else ""), style="italic dim")
    return label


def _event_label(name: str, value: str) -> Text:
    style = _ATTR_STYLES["event"]
    label = Text("• ", style="dim")
    label.append("event", style=style)
    label.append("  ")
    label.append(name, style="italic")
    label.append("  [")
    label.append(value, style="green" if value == "set" else "dim")
    label.append("]")
    return label


def _meter_label(name: str, mn: str, mx: str, value: str) -> Text:
    style = _ATTR_STYLES["meter"]
    label = Text("• ", style="dim")
    label.append("meter", style=style)
    label.append("  ")
    label.append(name, style="italic")
    label.append(f"  {mn}..{mx}  [")
    label.append(value, style="bold")
    label.append("]")
    return label


def _attribute_rows(node: NodeInfo) -> List[Tuple[str, Text]]:
    """Return ``[(kind, label), ...]`` for the inline attribute rows.

    Order follows roughly: dependencies → repeats / time → limits →
    in-limits → events → meters. Pure leaves with none of these stay
    leaves in the tree.
    """
    rows: List[Tuple[str, Text]] = []
    if node.trigger:
        rows.append(("trigger", _attr_label("trigger", node.trigger)))
    if node.complete_trigger:
        rows.append(("complete", _attr_label("complete", node.complete_trigger)))
    if node.repeat:
        rows.append(("repeat", _attr_label("repeat", node.repeat)))
    for time_str in node.times:
        rows.append(("time", _attr_label("time", time_str)))
    for name, value in node.limits:
        rows.append(("limit", _attr_label("limit", f"{name} [{value}]")))
    for name, tokens, ref in node.in_limits:
        body = f"{name}"
        if tokens != 1:
            body += f"  tokens={tokens}"
        if ref:
            body += f"  via {ref}"
        rows.append(("inlimit", _attr_label("inlimit", body)))
    for name, value in node.events:
        rows.append(("event", _event_label(name, value)))
    for name, mn, mx, value in node.meters:
        rows.append(("meter", _meter_label(name, mn, mx, value)))
    return rows


def _label_for(node: NodeInfo) -> Text:
    """Build a tree label like ``██ producer`` with the leading block coloured by state."""
    label = state_block(node.state, suspended=node.suspended)
    label.append(node.name)
    if node.submission_unknown:
        label.append(" [submission unknown]", style="bold red")
    return label


class NodeTree(Tree[str]):
    """Bunch hierarchy tree with inline attribute rows.

    Tree nodes carry the owning node's path string as their ``data``;
    synthetic attribute rows reuse the parent's path so selection /
    right-click / menu always target a real node.
    """

    DEFAULT_CSS = """
    NodeTree { height: 1fr; }
    """

    def __init__(self) -> None:
        super().__init__("bunch", id="node-tree")
        # path -> tree node, for the *real* nodes only (not attribute rows).
        # Named ``_path_nodes`` rather than ``_tree_nodes``: the ``Tree``
        # base class already owns a ``_tree_nodes`` id -> node index, and
        # shadowing it silently broke ``Tree`` internals that rely on it
        # (caught by the TUI test skeleton).
        self._path_nodes: Dict[str, TreeNode[str]] = {}
        self._query_view: QueryTuiView | None = None
        self._query_loaded: dict[str, int] = {}
        self._query_attr_nodes: dict[str, list[TreeNode[str]]] = {}
        self._query_root_loaded = 0

    QUERY_CHILD_BATCH = 256

    def rebuild_query(self, view: QueryTuiView) -> None:
        """Replace the compact view and recreate only expanded tree rows."""
        expanded = self._expanded_paths()
        cursor = self._cursor_path()
        loaded = self._query_loaded.copy()
        root_loaded = self._query_root_loaded
        self.clear()
        self._path_nodes.clear()
        self._query_loaded.clear()
        self._query_attr_nodes.clear()
        self._query_root_loaded = 0
        self._query_view = view
        target = min(max(root_loaded, self.QUERY_CHILD_BATCH), len(view.roots))
        while self._query_root_loaded < target:
            self._load_query_roots()
        for path in view.query.ordered_paths:
            if path in expanded and path in self._path_nodes:
                node = self._path_nodes[path]
                target = min(
                    loaded.get(path, self.QUERY_CHILD_BATCH),
                    len(view.children.get(path, [])),
                )
                while self._query_loaded.get(path, 0) < target:
                    self._load_query_children(path)
                node.expand()
        self._restore_cursor(cursor)

    def _load_query_roots(self) -> None:
        view = self._query_view
        if view is None:
            return
        if self._query_root_loaded and self.root.children:
            last = self.root.children[-1]
            if last.data is None:
                last.remove()
        start = self._query_root_loaded
        end = min(start + self.QUERY_CHILD_BATCH, len(view.roots))
        for path in view.roots[start:end]:
            self._add_query_node(self.root, path)
        self._query_root_loaded = end
        if end < len(view.roots):
            self.root.add_leaf(f"… more ({len(view.roots) - end})", data=None)

    def _add_query_node(self, parent: TreeNode[str], path: str) -> None:
        assert self._query_view is not None
        info = self._query_view.get(path)
        if info is None:
            return
        children = self._query_view.children.get(path, [])
        node = (
            parent.add(_label_for(info), data=path, expand=False)
            if children
            else parent.add_leaf(_label_for(info), data=path)
        )
        self._path_nodes[path] = node

    def _load_query_children(self, path: str) -> None:
        view = self._query_view
        if view is None or path not in self._path_nodes:
            return
        parent = self._path_nodes[path]
        children = view.children.get(path, [])
        start = self._query_loaded.get(path, 0)
        if start:
            for child in tuple(parent.children):
                if child.data is None:
                    child.remove()
        end = min(start + self.QUERY_CHILD_BATCH, len(children))
        for child in children[start:end]:
            self._add_query_node(parent, child)
        self._query_loaded[path] = end
        if end < len(children):
            parent.add_leaf(f"… more ({len(children) - end})", data=None)

    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        if self._query_view is None:
            return
        path = event.node.data
        if isinstance(path, str) and path not in self._query_loaded:
            self._load_query_children(path)

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        if self._query_view is None or event.node.data is not None:
            return
        parent = event.node.parent
        if parent is self.root:
            self._load_query_roots()
            event.stop()
            return
        if parent is not None and isinstance(parent.data, str):
            self._load_query_children(parent.data)
            event.stop()

    def update_query_detail(self, path: str) -> None:
        """Add inline attributes only for a detail that was actually fetched."""
        if self._query_view is None or path not in self._path_nodes:
            return
        node = self._path_nodes[path]
        info = self._query_view.get(path)
        if info is None:
            return
        node.set_label(_label_for(info))
        for previous in self._query_attr_nodes.pop(path, []):
            previous.remove()
        added: list[TreeNode[str]] = []
        for _, label in _attribute_rows(info):
            added.append(node.add_leaf(label, data=path))
        self._query_attr_nodes[path] = added
        node.allow_expand = bool(added or self._query_view.children.get(path))
        if added:
            node.expand()

    def apply_query_delta(
        self, view: QueryTuiView, changed: tuple[str, ...], removed: tuple[str, ...]
    ) -> None:
        """Refresh visible rows without clearing the tree or its cursor."""
        assert self._query_view is view
        removed_set = set(removed)
        for path in removed:
            parent_path = path.rsplit("/", 1)[0] or "/"
            if parent_path in removed_set:
                continue
            node = self._path_nodes.get(path)
            if node is None:
                continue
            parent = node.parent
            node.remove()
            if parent is self.root:
                self._query_root_loaded -= 1
                self._refresh_query_more(
                    self.root, self._query_root_loaded, len(view.roots)
                )
            elif parent_path in self._query_loaded:
                self._query_loaded[parent_path] -= 1
                self._refresh_query_more(
                    parent,
                    self._query_loaded[parent_path],
                    len(view.children.get(parent_path, ())),
                )
                parent.allow_expand = bool(
                    view.children.get(parent_path)
                    or self._query_attr_nodes.get(parent_path)
                )
        for path in removed:
            self._path_nodes.pop(path, None)
            self._query_loaded.pop(path, None)
            self._query_attr_nodes.pop(path, None)
        for path in changed:
            node = self._path_nodes.get(path)
            if node is None:
                continue
            info = view.get(path)
            if info is None:
                continue
            node.set_label(_label_for(info))
            for previous in self._query_attr_nodes.pop(path, []):
                previous.remove()
            node.allow_expand = bool(view.children.get(path))

    @staticmethod
    def _refresh_query_more(parent: TreeNode[str], loaded: int, total: int) -> None:
        for child in tuple(parent.children):
            if child.data is None:
                child.remove()
        if loaded < total:
            parent.add_leaf(f"… more ({total - loaded})", data=None)

    def reveal_query_path(self, path: str) -> None:
        """Expand only the ancestors needed to restore the selected row."""
        view = self._query_view
        if view is None or path not in view.query.nodes:
            return
        root_path = path if path in view.roots else "/" + path.split("/", 2)[1]
        if view.query.scope_path != "/":
            root_path = view.query.scope_path
        while root_path not in self._path_nodes and self._query_root_loaded < len(
            view.roots
        ):
            self._load_query_roots()
        parent = path.rsplit("/", 1)[0] or "/"
        chain: list[str] = []
        while parent != "/" and parent != view.query.scope_path:
            chain.append(parent)
            parent = parent.rsplit("/", 1)[0] or "/"
        if view.query.scope_path != "/":
            chain.append(view.query.scope_path)
        for ancestor in reversed(chain):
            node = self._path_nodes.get(ancestor)
            if node is None:
                break
            next_index = chain.index(ancestor) - 1
            child = chain[next_index] if next_index >= 0 else path
            while child not in self._path_nodes and self._query_loaded.get(
                ancestor, 0
            ) < len(view.children.get(ancestor, [])):
                self._load_query_children(ancestor)
            node.expand()
        self.select_path(path)

    def on_mount(self) -> None:
        self.show_root = False
        # Disable single-click / Enter auto-toggle on non-leaf nodes;
        # the app wires double-click to toggle instead.
        self.auto_expand = False

    # -- Rebuild ----------------------------------------------------

    def rebuild(self, snapshot: ShowSnapshot) -> None:
        """Rebuild the whole tree from ``snapshot``.

        Preserves the user's view across refreshes: which nodes are
        expanded and where the cursor sits. Without this every refresh
        would snap the whole tree back to fully-expanded and drop the
        selection, which is jarring during monitoring.
        """
        first_build = not self._path_nodes
        self._query_view = None
        self._query_loaded.clear()
        self._query_attr_nodes.clear()
        self._query_root_loaded = 0
        previous_paths = set(self._path_nodes)
        expanded_paths = self._expanded_paths()
        cursor_path = self._cursor_path()

        self.clear()
        self._path_nodes.clear()

        for root_path in snapshot.roots:
            self._add_subtree(self.root, snapshot, root_path)

        if first_build:
            # First time we see the bunch: expand everything so users
            # get the full picture without manual drilling.
            self.root.expand_all()
        else:
            self._restore_expanded(previous_paths, expanded_paths)

        self._restore_cursor(cursor_path)

    def _expanded_paths(self) -> set[str]:
        """Paths whose tree node is currently expanded."""
        return {path for path, node in self._path_nodes.items() if node.is_expanded}

    def _cursor_path(self) -> Optional[str]:
        """Path under the tree cursor, if it maps to a real node."""
        tree_node = self.node_at_line(self.cursor_line)
        if tree_node is None:
            return None
        data = tree_node.data
        return data if isinstance(data, str) else None

    def _restore_expanded(
        self, previous_paths: set[str], expanded_paths: set[str]
    ) -> None:
        """Re-apply a previously-captured expand/collapse state.

        Nodes are added expanded by :meth:`_add_subtree`, so we only
        need to collapse those the user had explicitly collapsed last
        time. Brand-new nodes (not in ``previous_paths``) keep the
        default-open behaviour.
        """
        for path, node in self._path_nodes.items():
            if not node.allow_expand:
                continue
            was_known = path in previous_paths
            if was_known and path not in expanded_paths:
                node.collapse()

    def _restore_cursor(self, cursor_path: Optional[str]) -> None:
        if cursor_path is None:
            return
        tree_node = self._path_nodes.get(cursor_path)
        if tree_node is None:
            return
        line = tree_node.line
        if line is not None and line >= 0:
            self.cursor_line = line

    def _add_subtree(
        self,
        parent: TreeNode[str],
        snapshot: ShowSnapshot,
        path: str,
    ) -> None:
        node = snapshot.nodes[path]
        label = _label_for(node)
        attribute_rows = _attribute_rows(node)
        # A node renders as a leaf iff it has no children AND no inline
        # attributes; otherwise users want it expandable so they can
        # see the rows.
        renders_as_leaf = not node.children and not attribute_rows
        if renders_as_leaf:
            tree_node = parent.add_leaf(label, data=path)
        else:
            tree_node = parent.add(label, data=path, expand=True)
        self._path_nodes[path] = tree_node

        # Inline attribute rows come first so they sit visually closer
        # to the owning node. Their ``data`` is the owning node's path,
        # so selection / right-click / menu always target the parent.
        for _kind, attr_label in attribute_rows:
            tree_node.add_leaf(attr_label, data=path)

        for child_path in node.children:
            self._add_subtree(tree_node, snapshot, child_path)

    # -- Lookups ----------------------------------------------------

    def node_at_line(self, line: int) -> Optional[TreeNode[str]]:
        """Return the tree node displayed on ``line``, or ``None``."""
        if line is None or line < 0:
            return None
        try:
            return self.get_node_at_line(line)
        except Exception:
            return None

    def node_at_hover(self) -> Optional[TreeNode[str]]:
        """Return the tree node directly under the mouse, or ``None``."""
        return self.node_at_line(self.hover_line)

    def path_at_hover(self) -> Optional[str]:
        """Node path under the mouse, if it maps to a real node row."""
        tree_node = self.node_at_hover()
        if tree_node is None:
            return None
        data = tree_node.data
        return data if isinstance(data, str) else None

    def is_real_node(self, tree_node: TreeNode[str]) -> bool:
        """True when ``tree_node`` is a real node row (not an attribute leaf).

        Attribute rows reuse their owning node's path as ``data`` but are
        not the node itself; this distinguishes the two.
        """
        data = tree_node.data
        return isinstance(data, str) and self._path_nodes.get(data) is tree_node

    def select_path(self, path: str) -> None:
        """Move the cursor to the tree row for ``path`` (if visible)."""
        tree_node = self._path_nodes.get(path)
        if tree_node is None:
            return
        # ``TreeNode.line`` is the node's displayed line (or -1 when the
        # node is hidden inside a collapsed ancestor); use it directly
        # instead of scanning every line.
        line = tree_node.line
        if line is not None and line >= 0:
            self.cursor_line = line

    def anchor_for_cursor(self) -> Offset:
        """Screen offset to anchor a popup menu near the cursor row."""
        try:
            tree_region = self.region
        except Exception:
            return Offset(0, 0)
        cursor_y = tree_region.y + max(0, self.cursor_line) + 1
        cursor_x = tree_region.x + 4
        return Offset(cursor_x, cursor_y)
