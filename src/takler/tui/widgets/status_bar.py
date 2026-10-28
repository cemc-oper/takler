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

from takler.query import ServiceInfo

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
        self._service: Optional[ServiceInfo] = None
        self._selection: Optional[NodeInfo] = None
        self._stale = False

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
        self._selection = node
        text = Text()
        if self._stale:
            text.append("stale  |  ", style="bold yellow")
        if self._service is not None:
            style = "bold red" if self._service.status == "halted" else "green"
            text.append(f"service {self._service.status}", style=style)
            if self._service.reason:
                text.append(f" ({self._service.reason})", style=style)
            text.append("  |  ", style="dim")
        if node is None:
            text.append("—", style="dim")
        else:
            text.append(state_block(node.state, suspended=node.suspended))
            text.append(node.path)
            if node.submission_unknown:
                text.append(" [submission unknown]", style="bold red")
        self._left.update(text)

    def set_service(self, service: Optional[ServiceInfo]) -> None:
        """Keep service state visible when any tree node is selected."""
        self._service = service
        self.set_selection(self._selection)

    def set_stale(self, stale: bool) -> None:
        self._stale = stale
        self.set_selection(self._selection)
