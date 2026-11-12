"""Render the synchronous Info and Parameters tabs in isolation."""

import pytest

from takler.core import Bunch, Flow, HaltCause
from takler.core.bunch import ServerState
from takler.tui.show_parser import parse_show
from takler.tui.tabs import InfoTab, ParametersTab
from tests.tui_fixtures import TabHost, show_payload, text_of


@pytest.mark.anyio
async def test_info_tab_without_selection() -> None:
    app = TabHost(InfoTab())
    async with app.run_test(size=(100, 40)):
        assert "(no node selected)" in text_of(app.tab._body)


@pytest.mark.anyio
async def test_info_tab_renders_every_section(snapshot) -> None:
    app = TabHost(InfoTab())
    async with app.run_test(size=(100, 40)):
        app.tab.show_node(snapshot.get("/flow1/family1/task1"), snapshot)
        body = text_of(app.tab._body)
        assert "Path:" in body and "/flow1/family1/task1" in body
        assert "Class:" in body and "Task" in body
        assert "suspend (unknown)" in body
        assert "Trigger" in body and "./task2 == complete" in body
        assert "Complete trigger" in body and "./task2:event_done" in body
        assert "Repeat" in body and "YMD" in body
        assert "Time" in body and "12:00" in body
        assert "Events" in body and "evt" in body
        assert "Meters" in body and "mtr 0..10" in body
        assert "User parameters" in body and "TAKLER_HOME" in body
        # FLOW_HOME is defined on the flow, inherited by the task.
        assert "Inherited parameters" in body and "FLOW_HOME" in body


@pytest.mark.anyio
async def test_info_tab_container_shows_limits_and_in_limits(snapshot) -> None:
    app = TabHost(InfoTab())
    async with app.run_test(size=(100, 40)):
        app.tab.show_node(snapshot.get("/flow1/family1"), snapshot)
        body = text_of(app.tab._body)
        assert "Limits" in body and "big [0/2]" in body
        assert "Children: 2" in body

        app.tab.show_node(snapshot.get("/flow1/family1/task2"), snapshot)
        body = text_of(app.tab._body)
        assert "In-limits" in body and "big" in body


@pytest.mark.anyio
async def test_info_tab_bare_node_has_no_attribute_sections(snapshot) -> None:
    app = TabHost(InfoTab())
    async with app.run_test(size=(100, 40)):
        app.tab.show_node(snapshot.get("/flow1/task3"), snapshot)
        body = text_of(app.tab._body)
        assert "/flow1/task3" in body
        assert "Trigger" not in body
        assert "Events" not in body
        assert "User parameters" not in body


@pytest.mark.anyio
async def test_info_tab_without_snapshot_skips_inherited_section(snapshot) -> None:
    app = TabHost(InfoTab())
    async with app.run_test(size=(100, 40)):
        app.tab.show_node(snapshot.get("/flow1/family1/task1"), None)
        body = text_of(app.tab._body)
        assert "User parameters" in body
        assert "Inherited parameters" not in body


@pytest.mark.anyio
async def test_info_tab_shows_service_and_current_attempt_without_guessing_rid() -> (
    None
):
    import json

    from takler.serialization.query import project_show

    bunch = Bunch(name="b")
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task("t")
    flow.begin()
    task.run()
    task.job_path = "/jobs/current.job"
    task.jobout_path = "/jobs/current.out"
    bunch.server_state = ServerState.from_causes([HaltCause.recovery])
    payload = project_show(
        bunch,
        server_status={
            **bunch.server_state.to_dict(),
            "last_checkpoint_at": "2026-10-05T00:00:00+00:00",
            "restore_summary": {
                "source": "backup",
                "fallback": True,
                "saved_at": "2026-10-05T00:00:00+00:00",
                "flow_count": 1,
                "node_count": 2,
                "in_flight_count": 1,
            },
            "message": "",
        },
    )
    snapshot = parse_show(json.dumps(payload))
    app = TabHost(InfoTab())
    async with app.run_test(size=(100, 40)):
        app.tab.show_node(snapshot.get("/f/t"), snapshot)
        body = text_of(app.tab._body)
        assert "Service: halted (recovery pending)" in body
        assert "Last checkpoint: 2026-10-05T00:00:00+00:00" in body
        assert "Halt causes: recovery" in body
        assert "Recovery: backup snapshot (fallback)" in body
        assert "Snapshot saved: 2026-10-05T00:00:00+00:00" in body
        assert "Restored: 1 flows, 2 nodes, 1 in flight" in body
        assert f"Attempt ID: {task.attempt_id}" in body
        assert "RID: unknown" in body
        assert "Job script: /jobs/current.job" in body
        assert "Job output: /jobs/current.out" in body


@pytest.mark.anyio
async def test_parameters_tab_without_selection() -> None:
    app = TabHost(ParametersTab())
    async with app.run_test(size=(100, 40)):
        assert text_of(app.tab._title) == "(no node selected)"


@pytest.mark.anyio
async def test_parameters_tab_lists_local_and_inherited(snapshot) -> None:
    app = TabHost(ParametersTab())
    async with app.run_test(size=(100, 40)):
        app.tab.show_node(snapshot.get("/flow1/family1/task1"), snapshot)
        assert text_of(app.tab._title) == (
            "Parameters of /flow1/family1/task1  (local: 2, inherited: 5)"
        )
        table = app.tab._table
        assert table.row_count == 7
        assert list(table.get_row_at(0)) == ["local", "TAKLER_HOME", "/tmp/takler_home"]
        assert list(table.get_row_at(1))[:2] == ["local", "TAKLER_SCRIPT"]
        assert list(table.get_row_at(3)) == ["inherited", "FLOW_HOME", "/flow"]


@pytest.mark.anyio
async def test_parameters_tab_local_value_shadows_inherited() -> None:
    bunch = Bunch(name="b")
    flow = bunch.add_flow(Flow("flow1"))
    flow.add_parameter("SHARED", "from-flow")
    task = flow.add_task("task1")
    task.add_parameter("SHARED", "from-task")
    snapshot = parse_show(show_payload(bunch))

    app = TabHost(ParametersTab())
    async with app.run_test(size=(100, 40)):
        app.tab.show_node(snapshot.get("/flow1/task1"), snapshot)
        assert "(local: 1, inherited: 5)" in text_of(app.tab._title)
        assert list(app.tab._table.get_row_at(0)) == ["local", "SHARED", "from-task"]
