"""Bottom status bar for the takler TUI.

:class:`StatusBar` shows the selected node on the left and short status
messages on the right. It also maps the legacy status palette
(``green`` / ``yellow`` / ``bold red``) onto a Rich style for the
right-hand label; the matching toast severity is handled by the app.
"""
from __future__ import annotations

from typing import Dict, Optional

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widgets import Static

from ..show_parser import NodeInfo
from ..state_style import state_block


class StatusBar(Horizontal):
    """Selection (left) + last status message (right)."""

    DEFAULT_CSS = """
    StatusBar {
        height: 1;
        background: $boost;
        padding: 0 1;
    }

    StatusBar #status-left {
        width: auto;
    }

    StatusBar #status-spacer {
        width: 1fr;
    }

    StatusBar #status-right {
        width: auto;
        color: $text-muted;
    }
    """

    _RIGHT_STYLES: Dict[str, str] = {
        "green": "green",
        "yellow": "yellow",
        "bold red": "bold red",
        "red": "red",
        "": "",
    }

    def __init__(self) -> None:
        super().__init__(id="status-bar")
        self._left: Static = Static("", id="status-left")
        self._right: Static = Static("", id="status-right")

    def compose(self) -> ComposeResult:
        yield self._left
        yield Static("", id="status-spacer")
        yield self._right

    def on_mount(self) -> None:
        self.set_selection(None)

    def set_message(self, message: str, style: str = "") -> None:
        """Write the right-hand status message in the legacy palette."""
        right_style = self._RIGHT_STYLES.get(style, "")
        text = Text(message, style=right_style) if right_style else Text(message)
        self._right.update(text)

    def set_selection(self, node: Optional[NodeInfo]) -> None:
        """Write the left side: the selected node's state badge + path."""
        if node is None:
            self._left.update(Text("—", style="dim"))
            return
        text = state_block(node.state, suspended=node.suspended)
        text.append(node.path)
        self._left.update(text)
