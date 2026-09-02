"""Parameters tab: list user parameters defined here and inherited from parents."""

from __future__ import annotations

from typing import Optional

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import DataTable, Static

from ..show_parser import NodeInfo, ShowSnapshot


class ParametersTab(Vertical):
    """Show user parameters local to the node and inherited from ancestors."""

    DEFAULT_CSS = """
    ParametersTab { padding: 1 2; }
    ParametersTab Static.title { padding-bottom: 1; text-style: bold; }
    ParametersTab DataTable { height: auto; max-height: 1fr; }
    """

    def __init__(self) -> None:
        super().__init__(id="tab-parameters")
        self._title = Static("(no node selected)", classes="title")
        self._table: DataTable = DataTable(zebra_stripes=True, cursor_type="row")

    def compose(self) -> ComposeResult:
        yield self._title
        yield self._table

    def on_mount(self) -> None:
        self._table.add_columns("kind", "name", "value")

    def show_node(
        self,
        node: Optional[NodeInfo],
        snapshot: Optional[ShowSnapshot] = None,
    ) -> None:
        self._table.clear()
        if node is None:
            self._title.update("(no node selected)")
            return

        local = node.user_parameters

        inherited: dict[str, str] = {}
        if snapshot is not None:
            for ancestor in snapshot.parents_of(node.path):
                for k, v in ancestor.user_parameters.items():
                    if k in local:
                        continue
                    inherited.setdefault(k, v)

        self._title.update(
            f"Parameters of {node.path}  "
            f"(local: {len(local)}, inherited: {len(inherited)})"
        )

        for name, value in sorted(local.items()):
            self._table.add_row("local", name, value)
        for name, value in sorted(inherited.items()):
            self._table.add_row("inherited", name, value)
