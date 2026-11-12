"""Exercise file-backed tabs and their asynchronous reads."""

from __future__ import annotations

from pathlib import Path

import pytest
from rich.text import Text
from textual.visual import RichVisual
from textual.widgets import DataTable
from textual.widgets._data_table import ColumnKey, RowKey

from takler.core import Bunch, Flow
from takler.tui.show_parser import parse_show
from takler.tui.tabs import JobTab, OutputTab, ScriptTab

from tests.tui_fixtures import TabHost, show_payload, text_of, wait_until


pytestmark = pytest.mark.integration


def _bunch_with_artifacts(home: Path) -> Bunch:
    """A one-task bunch whose TAKLER_HOME points into ``home``."""
    bunch = Bunch(name="b")
    flow = bunch.add_flow(Flow("flow1"))
    flow.add_parameter("TAKLER_HOME", str(home))
    task = flow.add_task("task1")
    task.add_parameter("TAKLER_SCRIPT", str(home / "flow1" / "task1.takler"))
    return bunch


@pytest.mark.anyio
async def test_script_tab_without_selection_or_script(snapshot) -> None:
    app = TabHost(ScriptTab())
    async with app.run_test(size=(100, 40)):
        app.tab.show_node(None)
        assert text_of(app.tab._title) == "(no node selected)"

        app.tab.show_node(snapshot.get("/flow1/family1"), snapshot)
        assert "no TAKLER_SCRIPT" in text_of(app.tab._title)


@pytest.mark.anyio
async def test_script_tab_renders_existing_file(tmp_path: Path) -> None:
    script = tmp_path / "flow1" / "task1.takler"
    script.parent.mkdir(parents=True)
    script.write_text("#!/bin/bash\necho hello\n")
    snapshot = parse_show(show_payload(_bunch_with_artifacts(tmp_path)))

    app = TabHost(ScriptTab())
    async with app.run_test(size=(100, 40)) as pilot:
        app.tab.show_node(snapshot.get("/flow1/task1"), snapshot)
        await wait_until(
            lambda: str(script) in text_of(app.tab._title),
            pilot,
            message="script tab never finished loading",
        )
        assert "loading" not in text_of(app.tab._title)
        # The body holds the syntax-highlighted script (a RichVisual
        # wrapping the Syntax), not an error Content.
        assert isinstance(app.tab._body.render(), RichVisual)


@pytest.mark.anyio
async def test_script_tab_reports_missing_file(snapshot) -> None:
    # The fixture's TAKLER_SCRIPT points nowhere real.
    app = TabHost(ScriptTab())
    async with app.run_test(size=(100, 40)) as pilot:
        app.tab.show_node(snapshot.get("/flow1/family1/task1"), snapshot)
        await wait_until(
            lambda: "loading" not in text_of(app.tab._title),
            pilot,
            message="script tab stuck in loading state",
        )
        assert "file not found" in text_of(app.tab._body)


@pytest.mark.anyio
async def test_job_tab_without_selection_or_job(snapshot) -> None:
    app = TabHost(JobTab())
    async with app.run_test(size=(100, 40)) as pilot:
        app.tab.show_node(None)
        assert text_of(app.tab._title) == "(no node selected)"

        # task3 has no TAKLER_HOME of its own; the fixture's server
        # default "." yields a relative prefix that exists nowhere.
        app.tab.show_node(snapshot.get("/flow1/task3"), snapshot)
        await wait_until(lambda: "no job script yet" in text_of(app.tab._title), pilot)


@pytest.mark.anyio
async def test_job_tab_loads_generated_job(tmp_path: Path) -> None:
    job = tmp_path / "flow1" / "task1.job0"
    job.parent.mkdir(parents=True)
    job.write_text("#!/bin/bash\necho generated\n")
    snapshot = parse_show(show_payload(_bunch_with_artifacts(tmp_path)))

    app = TabHost(JobTab())
    async with app.run_test(size=(100, 40)) as pilot:
        app.tab.show_node(snapshot.get("/flow1/task1"), snapshot)
        await wait_until(
            lambda: "task1.job0" in text_of(app.tab._title),
            pilot,
            message="job tab never picked up task1.job0",
        )
        assert "loading" not in text_of(app.tab._title)


# ---------------------------------------------------------------------------
# OutputTab
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_output_tab_without_selection() -> None:
    app = TabHost(OutputTab())
    async with app.run_test(size=(100, 40)):
        app.tab.show_node(None)
        assert text_of(app.tab._title) == "(no node selected)"
        assert app.tab._table.row_count == 0


@pytest.mark.anyio
async def test_output_tab_without_output(snapshot) -> None:
    app = TabHost(OutputTab())
    async with app.run_test(size=(100, 40)) as pilot:
        # task3 has no TAKLER_HOME of its own; the fixture's server
        # default "." yields a relative prefix that exists nowhere.
        app.tab.show_node(snapshot.get("/flow1/task3"), snapshot)
        await wait_until(lambda: "no output yet" in text_of(app.tab._title), pilot)


def _populate_output_home(home: Path) -> None:
    node_dir = home / "flow1"
    node_dir.mkdir(parents=True)
    (node_dir / "task1.0").write_text("first\noutput\n")
    (node_dir / "task1.1").write_text("second try\n")
    (node_dir / "task1.job0").write_text("#!/bin/bash\n")


@pytest.mark.anyio
async def test_output_tab_tails_latest_and_lists_related(tmp_path: Path) -> None:
    _populate_output_home(tmp_path)
    snapshot = parse_show(show_payload(_bunch_with_artifacts(tmp_path)))

    app = TabHost(OutputTab())
    async with app.run_test(size=(120, 40)) as pilot:
        app.tab.show_node(snapshot.get("/flow1/task1"), snapshot)
        await wait_until(
            lambda: "task1.0" in text_of(app.tab._title),
            pilot,
            message="output tab never picked up task1.0",
        )
        assert "(last 1000 lines)" in text_of(app.tab._title)
        assert "Related files (3)" in text_of(app.tab._files_title)
        table = app.tab._table
        assert table.row_count == 3
        # Default sort: newest mtime first; the exact order depends on
        # creation time, so just check the set.
        names = {str(table.get_row_at(i)[0]) for i in range(3)}
        assert names == {"task1.0", "task1.1", "task1.job0"}


@pytest.mark.anyio
async def test_output_tab_header_click_changes_sort(tmp_path: Path) -> None:
    _populate_output_home(tmp_path)
    snapshot = parse_show(show_payload(_bunch_with_artifacts(tmp_path)))

    app = TabHost(OutputTab())
    async with app.run_test(size=(120, 40)) as pilot:
        tab = app.tab
        tab.show_node(snapshot.get("/flow1/task1"), snapshot)
        await wait_until(lambda: tab._table.row_count == 3, pilot)

        def header(key: str) -> None:
            event = DataTable.HeaderSelected(tab._table, ColumnKey(key), 0, Text(key))
            tab.on_data_table_header_selected(event)

        header("name")
        assert tab._sort_key == "name"
        assert tab._sort_reverse is False
        first_names = [str(tab._table.get_row_at(i)[0]) for i in range(3)]
        assert first_names == ["task1.0", "task1.1", "task1.job0"]

        # Same column again toggles direction.
        header("name")
        assert tab._sort_reverse is True
        assert str(tab._table.get_row_at(0)[0]) == "task1.job0"

        # Time-like columns default to newest-first.
        header("mtime")
        assert tab._sort_reverse is True

        # Unknown columns are ignored.
        header("bogus")
        assert tab._sort_key == "mtime"


@pytest.mark.anyio
async def test_output_tab_row_selection_tails_that_file(tmp_path: Path) -> None:
    _populate_output_home(tmp_path)
    snapshot = parse_show(show_payload(_bunch_with_artifacts(tmp_path)))

    app = TabHost(OutputTab())
    async with app.run_test(size=(120, 40)) as pilot:
        tab = app.tab
        tab.show_node(snapshot.get("/flow1/task1"), snapshot)
        await wait_until(lambda: tab._table.row_count == 3, pilot)

        target = str(tmp_path / "flow1" / "task1.1")
        event = DataTable.RowSelected(tab._table, 0, RowKey(target))
        tab.on_data_table_row_selected(event)
        await wait_until(
            lambda: (
                "task1.1" in text_of(tab._title)
                and "loading" not in text_of(tab._title)
            ),
            pilot,
            message="row selection never tailed task1.1",
        )
        assert "(last 1000 lines)" in text_of(tab._title)


@pytest.mark.anyio
async def test_output_tab_row_selection_reports_read_errors(tmp_path: Path) -> None:
    snapshot = parse_show(show_payload(_bunch_with_artifacts(tmp_path)))

    app = TabHost(OutputTab())
    async with app.run_test(size=(120, 40)) as pilot:
        tab = app.tab
        tab.show_node(snapshot.get("/flow1/task1"), snapshot)
        await wait_until(lambda: "no output yet" in text_of(tab._title), pilot)

        missing = str(tmp_path / "flow1" / "task1.9")
        event = DataTable.RowSelected(tab._table, 0, RowKey(missing))
        tab.on_data_table_row_selected(event)
        await wait_until(
            lambda: text_of(tab._title) == missing,
            pilot,
            message="read error path never applied",
        )
        assert "read error" in _richlog_text(tab._log)


def _richlog_text(log) -> str:
    return "\n".join(strip.text for strip in log.lines)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "tab_type,parameter", [(JobTab, "TAKLER_JOB"), (OutputTab, "TAKLER_JOBOUT")]
)
async def test_artifact_tabs_use_explicit_view_parameter(tmp_path, tab_type, parameter):
    chosen = tmp_path / "chosen.txt"
    chosen.write_text("chosen artifact\n")
    bunch = Bunch("b")
    task = bunch.add_flow(Flow("f")).add_task("t")
    task.add_parameter(parameter, str(chosen))
    snapshot = parse_show(show_payload(bunch))
    app = TabHost(tab_type())
    async with app.run_test(size=(100, 40)) as pilot:
        app.tab.show_node(snapshot.get("/f/t"), snapshot)
        await wait_until(lambda: str(chosen) in text_of(app.tab._title), pilot)
        if isinstance(app.tab, OutputTab):
            assert app.tab._table.row_count == 1
        else:
            assert "chosen artifact" in app.tab._body.render()._renderable.code
