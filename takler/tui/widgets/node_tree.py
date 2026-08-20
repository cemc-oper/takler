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
    label.append(body, style="italic dim")
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
        self._tree_nodes: Dict[str, TreeNode[str]] = {}

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
        first_build = not self._tree_nodes
        previous_paths = set(self._tree_nodes)
        expanded_paths = self._expanded_paths()
        cursor_path = self._cursor_path()

        self.clear()
        self._tree_nodes.clear()

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
        return {path for path, node in self._tree_nodes.items() if node.is_expanded}

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
        for path, node in self._tree_nodes.items():
            if not node.allow_expand:
                continue
            was_known = path in previous_paths
            if was_known and path not in expanded_paths:
                node.collapse()

    def _restore_cursor(self, cursor_path: Optional[str]) -> None:
        if cursor_path is None:
            return
        tree_node = self._tree_nodes.get(cursor_path)
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
        self._tree_nodes[path] = tree_node

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
        return isinstance(data, str) and self._tree_nodes.get(data) is tree_node

    def select_path(self, path: str) -> None:
        """Move the cursor to the tree row for ``path`` (if visible)."""
        tree_node = self._tree_nodes.get(path)
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
