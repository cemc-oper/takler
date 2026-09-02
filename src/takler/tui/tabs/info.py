"""Info tab: show every attribute the snapshot has for the selected node."""

from __future__ import annotations

from typing import Optional

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Static

from ..show_parser import NodeInfo, ShowSnapshot
from ..state_style import state_block, style_for


def _state_text(node: NodeInfo) -> Text:
    text = state_block(node.state, suspended=node.suspended)
    style = style_for(node.state)
    if node.suspended:
        text.append(f"suspend ({node.state})", style=f"italic {style}")
    else:
        text.append(node.state, style=style)
    return text


class InfoTab(VerticalScroll):
    """Render a node's full set of attributes (mirrors ``show --show-all``)."""

    DEFAULT_CSS = """
    InfoTab { padding: 1 2; }
    InfoTab Static { width: 100%; }
    """

    def __init__(self) -> None:
        super().__init__(id="tab-info")
        self._body = Static(Text("(no node selected)", style="dim"), id="info-body")

    def compose(self) -> ComposeResult:
        yield self._body

    def show_node(
        self,
        node: Optional[NodeInfo],
        snapshot: Optional[ShowSnapshot] = None,
    ) -> None:
        if node is None:
            self._body.update(Text("(no node selected)", style="dim"))
            return

        text = Text()
        text.append("Path:    ", style="bold")
        text.append(f"{node.path}\n")
        text.append("Class:   ", style="bold")
        text.append(f"{node.class_name or '?'}\n", style="dim")
        text.append("State:   ", style="bold")
        text.append(_state_text(node))
        text.append("\n")
        text.append("Children: ", style="bold")
        text.append(f"{len(node.children)}\n")

        if node.trigger:
            text.append("\nTrigger\n", style="bold underline")
            text.append(f"  {node.trigger}\n")

        if node.complete_trigger:
            text.append("\nComplete trigger\n", style="bold underline")
            text.append(f"  {node.complete_trigger}\n")

        if node.repeat:
            text.append("\nRepeat\n", style="bold underline")
            text.append(f"  {node.repeat}\n")

        if node.times:
            text.append("\nTime\n", style="bold underline")
            for t in node.times:
                text.append(f"  {t}\n")

        if node.limits:
            text.append("\nLimits\n", style="bold underline")
            for name, value in node.limits:
                text.append(f"  {name} [{value}]\n")

        if node.in_limits:
            text.append("\nIn-limits\n", style="bold underline")
            for name, tokens, ref in node.in_limits:
                line = f"  {name}"
                if tokens != 1:
                    line += f"  tokens={tokens}"
                if ref:
                    line += f"  via {ref}"
                text.append(line + "\n")

        if node.events:
            text.append("\nEvents\n", style="bold underline")
            for name, value in node.events:
                style = "green" if value == "set" else "dim"
                text.append(f"  {name} [")
                text.append(value, style=style)
                text.append("]\n")

        if node.meters:
            text.append("\nMeters\n", style="bold underline")
            for name, mn, mx, value in node.meters:
                text.append(f"  {name} {mn}..{mx} [")
                text.append(value, style="bold")
                text.append("]\n")

        if node.user_parameters:
            text.append("\nUser parameters\n", style="bold underline")
            for k, v in sorted(node.user_parameters.items()):
                text.append(f"  {k} = ")
                text.append(v, style="cyan")
                text.append("\n")

        # Show inherited parameters (parents) so the picture matches the
        # actual runtime lookup.
        if snapshot is not None:
            inherited: dict[str, str] = {}
            for ancestor in snapshot.parents_of(node.path):
                for k, v in ancestor.user_parameters.items():
                    inherited.setdefault(k, v)
            if inherited:
                text.append("\nInherited parameters\n", style="bold underline")
                for k, v in sorted(inherited.items()):
                    text.append(f"  {k} = ")
                    text.append(v, style="dim cyan")
                    text.append("\n")

        self._body.update(text)
