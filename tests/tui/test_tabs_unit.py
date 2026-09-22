"""Unit tests for the file-oriented tab helpers.

These cover the pure / filesystem-only pieces of the ``script`` /
``job`` / ``output`` tabs: reverse tailing, related-file discovery,
output / job file picking and the ``TAKLER_HOME`` prefix derivation.
The widget-level rendering is covered by the pilot tests.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from takler.tui.show_parser import parse_show
from takler.tui.tabs._artifacts import artifact_prefix
from takler.tui.tabs.job import JobTab
from takler.tui.tabs.output import (
    OutputTab,
    _FileRow,
    _format_ts,
    _tail_lines,
)

from .conftest import show_payload

_collect_files = OutputTab._collect_files
_pick_output = OutputTab._pick_output


# ---------------------------------------------------------------------------
# _tail_lines
# ---------------------------------------------------------------------------


def test_tail_lines_returns_last_n(tmp_path: Path) -> None:
    log = tmp_path / "task.0"
    log.write_text("".join(f"line {i}\n" for i in range(10)))
    assert _tail_lines(log, 3) == ["line 7", "line 8", "line 9"]


def test_tail_lines_short_file_returns_everything(tmp_path: Path) -> None:
    log = tmp_path / "task.0"
    log.write_text("only\n")
    assert _tail_lines(log, 1000) == ["only"]


def test_tail_lines_empty_file(tmp_path: Path) -> None:
    log = tmp_path / "task.0"
    log.write_text("")
    assert _tail_lines(log, 10) == []


def test_tail_lines_handles_missing_trailing_newline(tmp_path: Path) -> None:
    log = tmp_path / "task.0"
    log.write_text("a\nb\nc")
    assert _tail_lines(log, 2) == ["b", "c"]


def test_tail_lines_spans_block_boundaries(tmp_path: Path) -> None:
    # More than one 64 KiB block of content: the reverse reader must
    # stitch chunks back together in order.
    log = tmp_path / "task.0"
    marker = "begin-marker\n"
    log.write_text(marker + ("x" * 70000 + "\n") * 3 + "tail\n")
    lines = _tail_lines(log, 2)
    assert lines[-1] == "tail"


def test_tail_lines_replaces_undecodable_bytes(tmp_path: Path) -> None:
    log = tmp_path / "task.0"
    log.write_bytes(b"ok\n\xff\xfe broken\n")
    lines = _tail_lines(log, 10)
    assert lines[0] == "ok"
    assert "broken" in lines[1]


# ---------------------------------------------------------------------------
# _collect_files / _pick_output
# ---------------------------------------------------------------------------


def _touch(path: Path, content: str = "x") -> Path:
    path.write_text(content)
    return path


def test_collect_files_matches_only_prefixed_siblings(tmp_path: Path) -> None:
    prefix = tmp_path / "task1"
    wanted = [
        _touch(tmp_path / "task1.0"),
        _touch(tmp_path / "task1.job0"),
        _touch(tmp_path / "task1.err"),
    ]
    _touch(tmp_path / "task10.0")  # different task whose name starts alike
    _touch(tmp_path / "other.0")
    (tmp_path / "task1.dir").mkdir()  # directories never match

    rows = _collect_files(prefix)
    assert {row.path for row in rows} == set(wanted)
    assert all(isinstance(row, _FileRow) for row in rows)
    assert all(row.mtime > 0 for row in rows)


def test_collect_files_missing_directory_returns_empty(tmp_path: Path) -> None:
    assert _collect_files(tmp_path / "nope" / "task1") == []


def test_pick_output_prefers_try_zero(tmp_path: Path) -> None:
    prefix = tmp_path / "task1"
    first = _touch(tmp_path / "task1.0")
    newer = _touch(tmp_path / "task1.3")
    future = time.time() + 1000
    os.utime(newer, (future, future))  # make try 3 unmistakably newer
    rows = _collect_files(prefix)
    assert _pick_output(prefix, rows) == first


def test_pick_output_falls_back_to_newest_numbered_try(tmp_path: Path) -> None:
    prefix = tmp_path / "task1"
    _touch(tmp_path / "task1.1")
    newest = _touch(tmp_path / "task1.2")
    future = time.time() + 1000
    os.utime(newest, (future, future))
    rows = _collect_files(prefix)
    assert _pick_output(prefix, rows) == newest


def test_pick_output_ignores_job_scripts_and_stderr(tmp_path: Path) -> None:
    prefix = tmp_path / "task1"
    _touch(tmp_path / "task1.job0")
    _touch(tmp_path / "task1.err")
    rows = _collect_files(prefix)
    assert _pick_output(prefix, rows) is None


def test_pick_output_without_rows_or_prefix(tmp_path: Path) -> None:
    assert _pick_output(tmp_path / "task1", []) is None
    assert _pick_output(None, [_FileRow(tmp_path / "task1.0", 0.0, 0.0)]) is None


def test_file_row_sort_values(tmp_path: Path) -> None:
    row = _FileRow(tmp_path / "task1.0", 1.0, 2.0)
    assert row.sort_value("name") == "task1.0"
    assert row.sort_value("path") == str(tmp_path / "task1.0")
    assert row.sort_value("mtime") == 1.0
    assert row.sort_value("ctime") == 2.0
    assert row.sort_value("unknown-key") == ""


def test_format_ts() -> None:
    assert _format_ts(0.0).endswith(":00:00")
    assert len(_format_ts(0.0)) == len("1970-01-01 00:00:00")


# ---------------------------------------------------------------------------
# JobTab._pick_job
# ---------------------------------------------------------------------------


def test_pick_job_prefers_job_zero(tmp_path: Path) -> None:
    prefix = tmp_path / "task1"
    job0 = _touch(tmp_path / "task1.job0")
    job7 = _touch(tmp_path / "task1.job7")
    os.utime(job7, (1 << 30, 1 << 30))
    assert JobTab._pick_job(prefix) == job0


def test_pick_job_falls_back_to_newest_job_n(tmp_path: Path) -> None:
    prefix = tmp_path / "task1"
    _touch(tmp_path / "task1.job1")
    newest = _touch(tmp_path / "task1.job2")
    future = time.time() + 1000
    os.utime(newest, (future, future))
    assert JobTab._pick_job(prefix) == newest


def test_pick_job_ignores_non_job_files(tmp_path: Path) -> None:
    prefix = tmp_path / "task1"
    _touch(tmp_path / "task1.0")
    _touch(tmp_path / "task1.jobx")  # non-numeric suffix
    assert JobTab._pick_job(prefix) is None


def test_pick_job_missing_directory(tmp_path: Path) -> None:
    assert JobTab._pick_job(tmp_path / "nope" / "task1") is None


# ---------------------------------------------------------------------------
# artifact_prefix
# ---------------------------------------------------------------------------


def test_artifact_prefix_concatenates_home_and_path(snapshot) -> None:
    node = snapshot.get("/flow1/family1/task1")
    assert node is not None
    assert artifact_prefix(node, snapshot) == Path(
        "/tmp/takler_home/flow1/family1/task1"
    )


def test_artifact_prefix_inherits_home_from_ancestors() -> None:
    payload = show_payload(_bunch_with_home_on_flow())
    snapshot = parse_show(payload)
    node = snapshot.get("/flow1/task1")
    assert node is not None
    assert artifact_prefix(node, snapshot) == Path("/srv/home/flow1/task1")


def _bunch_with_home_on_flow():
    from takler.core import Bunch, Flow

    bunch = Bunch(name="b")
    flow = bunch.add_flow(Flow("flow1"))
    flow.add_parameter("TAKLER_HOME", "/srv/home")
    flow.add_task("task1")
    return bunch


def test_artifact_prefix_without_snapshot_or_home(snapshot, rich_payload) -> None:
    node = snapshot.get("/flow1/family1/task1")
    assert node is not None
    assert artifact_prefix(node, None) is None

    # The server always ships a default TAKLER_HOME ("."), so exercise the
    # "not resolvable" branch with a payload whose server state omits it.
    data = json.loads(rich_payload)
    data["server_state"]["parameters"] = [
        p for p in data["server_state"]["parameters"] if p["name"] != "TAKLER_HOME"
    ]
    homeless = parse_show(json.dumps(data))
    bare = homeless.get("/flow1/family1")
    assert bare is not None
    assert artifact_prefix(bare, homeless) is None
