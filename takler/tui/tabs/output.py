"""Output tab: tail the job output for the selected task and list related files.

The server-side ``Bunch.to_dict()`` payload does not include the
generated ``TAKLER_JOBOUT`` parameter (those live only on the runtime
``ShellScriptTaskGeneratedParameters``). We derive the path the same
way :class:`ShellScriptTaskGeneratedParameters` does:

    TAKLER_HOME + node_path + "." + try_no

If we can't determine ``try_no`` (we never can from JSON alone), pick
the most recently modified ``TAKLER_HOME + node_path.*`` file, which
matches every output / job script for this node.

The tab also renders a sortable table of every file in the same
directory whose name starts with ``<node_name>.`` (output files, job
scripts, stderr, etc.), so users can inspect previous tries or
ancillary files without leaving the TUI. Clicking a row tails that
file; clicking a column header toggles the sort.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import DataTable, RichLog, Static

from ..show_parser import NodeInfo, ShowSnapshot
from ._artifacts import artifact_prefix


_TAIL_LINES = 1000

# Block size for the reverse tail reader. We read the file backwards in
# chunks of this size until we've gathered enough newlines, so a 1 GB
# log costs a few reads instead of loading the whole file.
_TAIL_BLOCK = 64 * 1024

# (display label, key). The key is the stable identifier used both as
# the ``DataTable`` column key and the sort selector.
_COLUMNS: List[Tuple[str, str]] = [
    ("name", "name"),
    ("path", "path"),
    ("modified", "mtime"),
    ("created", "ctime"),
]

# Columns whose default sort direction is "newest first".
_DESC_BY_DEFAULT = {"mtime", "ctime"}


def _format_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _tail_lines(path: Path, max_lines: int) -> List[str]:
    """Return the last ``max_lines`` lines of ``path`` efficiently.

    Reads the file backwards in :data:`_TAIL_BLOCK`-sized chunks so we
    never load more than roughly ``max_lines`` worth of data into
    memory, regardless of how large the file is. Lines are returned
    without trailing newlines.
    """
    with path.open("rb") as fp:
        fp.seek(0, os.SEEK_END)
        end = fp.tell()
        if end == 0:
            return []
        blocks: List[bytes] = []
        newline_count = 0
        pos = end
        while pos > 0 and newline_count <= max_lines:
            read_size = min(_TAIL_BLOCK, pos)
            pos -= read_size
            fp.seek(pos)
            chunk = fp.read(read_size)
            blocks.append(chunk)
            newline_count += chunk.count(b"\n")
        data = b"".join(reversed(blocks))
    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines()
    return lines[-max_lines:]


@dataclass
class _FileRow:
    """One file matching the node's prefix, with cached stat fields."""

    path: Path
    mtime: float
    ctime: float

    @property
    def name(self) -> str:
        return self.path.name

    def sort_value(self, key: str):
        if key == "name":
            return self.name
        if key == "path":
            return str(self.path)
        if key == "mtime":
            return self.mtime
        if key == "ctime":
            return self.ctime
        return ""


class OutputTab(Vertical):
    """Show the latest job output file plus a sortable list of related files."""

    DEFAULT_CSS = """
    OutputTab { padding: 1 2; }
    OutputTab Static.title { padding-bottom: 1; text-style: bold; }
    OutputTab Static.subtitle {
        padding-top: 1;
        padding-bottom: 1;
        text-style: bold;
    }
    OutputTab RichLog { height: 2fr; border: tall $accent; }
    OutputTab DataTable { height: 1fr; min-height: 8; }
    """

    def __init__(self) -> None:
        super().__init__(id="tab-output")
        self._title = Static("(no node selected)", classes="title")
        self._log: RichLog = RichLog(highlight=True, markup=False, wrap=False)
        self._files_title = Static("", classes="subtitle")
        self._table: DataTable = DataTable(zebra_stripes=True, cursor_type="row")
        self._rows: List[_FileRow] = []
        # Default: most recently modified first.
        self._sort_key: str = "mtime"
        self._sort_reverse: bool = True

    def compose(self) -> ComposeResult:
        yield self._title
        yield self._log
        yield self._files_title
        yield self._table

    def on_mount(self) -> None:
        self._add_columns()

    # -- Public API -------------------------------------------------

    def show_node(
        self,
        node: Optional[NodeInfo],
        snapshot: Optional[ShowSnapshot] = None,
    ) -> None:
        self._log.clear()
        self._rows = []

        if node is None:
            self._title.update("(no node selected)")
            self._files_title.update("")
            self._refresh_table()
            return

        prefix = self._prefix_for(node, snapshot)
        if prefix is None:
            self._title.update(f"{node.path}: no output yet")
            self._log.write(
                Text(
                    "No output file found. Either the task hasn't run or "
                    "TAKLER_HOME is not visible to the TUI host.",
                    style="dim",
                )
            )
            self._files_title.update("")
            self._refresh_table()
            return

        # Directory scanning + tailing touch the filesystem, which can
        # block (slow / network mounts). Run them on a worker thread and
        # apply results back on the main thread.
        self._title.update(f"{node.path}: loading…")
        self._files_title.update(Text("Related files (…)", style="dim"))
        self._load_output(node.path, prefix)

    @work(thread=True, exclusive=True, group="output")
    def _load_output(self, node_path: str, prefix: Path) -> None:
        rows = self._collect_files(prefix)
        target = self._pick_output(prefix, rows)
        log_lines: Optional[List[str]] = None
        log_error: Optional[str] = None
        log_path: Optional[Path] = None
        if target is not None:
            log_path = target
            try:
                log_lines = _tail_lines(target, _TAIL_LINES)
            except OSError as exc:
                log_error = f"read error: {exc}"
        self.app.call_from_thread(
            self._apply_output,
            node_path,
            rows,
            log_path,
            log_lines,
            log_error,
        )

    def _apply_output(
        self,
        node_path: str,
        rows: List["_FileRow"],
        log_path: Optional[Path],
        log_lines: Optional[List[str]],
        log_error: Optional[str],
    ) -> None:
        """Main-thread sink for :meth:`_load_output` results."""
        self._rows = rows
        self._log.clear()
        if log_error is not None:
            self._title.update(f"{node_path}: {log_path}")
            self._log.write(Text(log_error, style="bold red"))
        elif log_path is None:
            self._title.update(f"{node_path}: no output yet")
            self._log.write(
                Text(
                    "No output file found. Either the task hasn't run or "
                    "TAKLER_HOME is not visible to the TUI host.",
                    style="dim",
                )
            )
        else:
            self._title.update(f"{node_path}: {log_path} (last {_TAIL_LINES} lines)")
            for line in log_lines or []:
                self._log.write(line)
        self._files_title.update(
            Text(f"Related files ({len(self._rows)})", style="bold")
        )
        self._refresh_table()

    # -- DataTable handlers -----------------------------------------

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        key = event.column_key.value
        if key is None or key not in {k for _, k in _COLUMNS}:
            return
        if key == self._sort_key:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_key = key
            self._sort_reverse = key in _DESC_BY_DEFAULT
        self._refresh_table()
        event.stop()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        key = event.row_key.value
        if not key:
            return
        path = Path(key)
        self._title.update(f"{path}: loading…")
        self._tail_file(path)
        event.stop()

    @work(thread=True, exclusive=True, group="output")
    def _tail_file(self, path: Path) -> None:
        lines: Optional[List[str]] = None
        error: Optional[str] = None
        try:
            lines = _tail_lines(path, _TAIL_LINES)
        except OSError as exc:
            error = f"read error: {exc}"
        self.app.call_from_thread(self._apply_tail, path, lines, error)

    def _apply_tail(
        self, path: Path, lines: Optional[List[str]], error: Optional[str]
    ) -> None:
        self._log.clear()
        if error is not None:
            self._title.update(str(path))
            self._log.write(Text(error, style="bold red"))
            return
        self._title.update(f"{path} (last {_TAIL_LINES} lines)")
        for line in lines or []:
            self._log.write(line)

    # -- Rendering helpers ------------------------------------------

    def _add_columns(self) -> None:
        for label, key in _COLUMNS:
            display = label
            if key == self._sort_key:
                display = f"{label} {'▼' if self._sort_reverse else '▲'}"
            self._table.add_column(display, key=key)

    def _refresh_table(self) -> None:
        # Re-add columns so the sort indicator on the header label
        # stays in sync with the active sort.
        self._table.clear(columns=True)
        self._add_columns()

        rows = list(self._rows)
        if self._sort_key:
            rows.sort(
                key=lambda r: r.sort_value(self._sort_key),
                reverse=self._sort_reverse,
            )
        for row in rows:
            self._table.add_row(
                row.name,
                str(row.path),
                _format_ts(row.mtime),
                _format_ts(row.ctime),
                key=str(row.path),
            )

    def _render_log(self, path: Path, *, prefix: Optional[str] = None) -> None:
        title = f"{prefix}: {path}" if prefix else str(path)
        self._title.update(f"{title} (last {_TAIL_LINES} lines)")
        self._log.clear()
        try:
            with path.open("r", errors="replace") as fp:
                lines = fp.readlines()
        except OSError as exc:
            self._log.write(Text(f"read error: {exc}", style="bold red"))
            return
        for line in lines[-_TAIL_LINES:]:
            self._log.write(line.rstrip("\n"))

    def _render_log(self, path: Path, *, prefix: Optional[str] = None) -> None:
        title = f"{prefix}: {path}" if prefix else str(path)
        self._title.update(f"{title} (last {_TAIL_LINES} lines)")
        self._log.clear()
        try:
            with path.open("r", errors="replace") as fp:
                lines = fp.readlines()
        except OSError as exc:
            self._log.write(Text(f"read error: {exc}", style="bold red"))
            return
        for line in lines[-_TAIL_LINES:]:
            self._log.write(line.rstrip("\n"))

    # -- File discovery ---------------------------------------------

    @staticmethod
    def _prefix_for(node: NodeInfo, snapshot: Optional[ShowSnapshot]) -> Optional[Path]:
        return artifact_prefix(node, snapshot)

    @staticmethod
    def _collect_files(prefix: Path) -> List[_FileRow]:
        """Return every file in ``prefix.parent`` named ``<prefix.name>.*``.

        The leading-dot constraint scopes us to takler's own output
        artefacts (``.0``, ``.1``, ``.job0``, ``.err`` ...) and avoids
        matching unrelated siblings like ``job_01`` when the task is
        named ``job_0``.
        """
        parent = prefix.parent
        if not parent.exists():
            return []
        stem_dot = prefix.name + "."
        rows: List[_FileRow] = []
        try:
            for entry in parent.iterdir():
                if not entry.is_file():
                    continue
                if not entry.name.startswith(stem_dot):
                    continue
                try:
                    stat = entry.stat()
                except OSError:
                    continue
                # st_birthtime is only present on macOS / FreeBSD; on
                # Linux fall back to st_ctime (inode change time),
                # which is what `ls -lc` reports.
                ctime = getattr(stat, "st_birthtime", stat.st_ctime)
                rows.append(_FileRow(entry, stat.st_mtime, ctime))
        except OSError:
            return []
        return rows

    @staticmethod
    def _pick_output(prefix: Optional[Path], rows: List[_FileRow]) -> Optional[Path]:
        """Choose the file whose contents to tail in the log pane.

        Mirrors the previous behaviour: prefer ``<prefix>.0`` (try 0),
        otherwise the most recently modified ``<prefix>.<digits>``
        file. Job scripts (``.job<n>``) and stderr (``.err``) are
        excluded so we always show *output*, not generated source.
        """
        if prefix is None or not rows:
            return None
        stem_dot = prefix.name + "."

        target_zero = stem_dot + "0"
        for row in rows:
            if row.path.name == target_zero:
                return row.path

        digit_rows = [r for r in rows if r.path.name[len(stem_dot) :].isdigit()]
        if not digit_rows:
            return None
        return max(digit_rows, key=lambda r: r.mtime).path
