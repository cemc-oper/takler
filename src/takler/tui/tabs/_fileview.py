"""Shared base for tabs that show the contents of a single file.

Both the ``script`` and ``job`` tabs resolve a path for the selected
task and then display that file as a syntax-highlighted shell script.
The resolution rules differ (``TAKLER_SCRIPT`` parameter lookup vs.
scanning ``TAKLER_HOME`` for ``.job<n>`` files), but the widget layout
and the read-on-a-worker-thread rendering are identical; this base
captures the common parts.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from rich.syntax import Syntax
from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Static


class _FileViewTab(Vertical):
    """A title + body pair that renders a file as highlighted bash.

    Subclasses set :attr:`_TAB_ID` / :attr:`_BODY_ID` / :attr:`_GROUP`
    and drive rendering through :meth:`_show_empty`, :meth:`_show_message`
    and :meth:`_render_file`.
    """

    _TAB_ID: str = "tab-file"
    _BODY_ID: str = "file-body"

    def __init__(self) -> None:
        super().__init__(id=self._TAB_ID)
        self._title = Static("(no node selected)", classes="title")
        self._body = Static("", id=self._BODY_ID, expand=True)

    def compose(self) -> ComposeResult:
        yield self._title
        yield self._body

    # -- Rendering primitives ---------------------------------------

    def _show_empty(self) -> None:
        """No node selected."""
        self._title.update("(no node selected)")
        self._body.update("")

    def _show_message(self, title: str, message: str) -> None:
        """Show a dim informational message (e.g. "no script")."""
        self._title.update(title)
        self._body.update(Text(message, style="dim"))

    def _render_file(self, node_path: str, path: Path) -> None:
        """Read ``path`` on a worker thread and render it.

        Sets a transient "loading…" title immediately so the UI reflects
        the pending read.
        """
        self._title.update(f"{node_path}: loading…")
        self._read_file(node_path, path)

    @work(thread=True, exclusive=True, group="fileview")
    def _read_file(self, node_path: str, path: Path) -> None:
        text: Optional[str] = None
        error: Optional[str] = None
        if not path.exists():
            error = f"file not found: {path}"
        else:
            try:
                text = path.read_text(errors="replace")
            except OSError as exc:
                error = f"read error: {exc}"
        self.app.call_from_thread(self._apply_file, node_path, path, text, error)

    def _apply_file(
        self,
        node_path: str,
        path: Path,
        text: Optional[str],
        error: Optional[str],
    ) -> None:
        self._title.update(f"{node_path}: {path}")
        if error is not None:
            self._body.update(Text(error, style="bold red"))
            return
        self._body.update(
            Syntax(
                text or "",
                lexer="bash",
                line_numbers=True,
                word_wrap=False,
                theme="monokai",
            )
        )
