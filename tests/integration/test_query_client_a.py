"""Real HTTP/gRPC query reads through the Python client."""

import pytest

from takler.protocol.query_v1 import ResetReply, SinceRequest
from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server
from takler.tui.app import TaklerTuiApp
from takler.tui.service import TaklerTuiService
from tests.tui.conftest import text_of, wait_until
from textual.widgets import Static


@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_query_snapshot_detail_blob_and_show(
    transport, server_runner_factory, free_port_fixture, capsys
):
    grpc_port, http_port = free_port_fixture(), free_port_fixture()
    runner = server_runner_factory(
        port=grpc_port,
        interval_main_loop=1.0,
        connect_config=ConnectConfig(
            server=Server(
                address=Address(
                    hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)
                ),
                http=HttpSettings(host="127.0.0.1", port=str(http_port)),
            )
        ),
    )
    flow = runner.server.bunch.add_flow("flow")
    task = flow.add_task("task")
    task.attempt_id = "test-attempt"
    task.add_parameter("WIDE", "x" * 300000)
    client = runner.make_client(transport_name=transport)
    if transport == "http":
        client.port = http_port
    client.start()
    try:
        assert client.query_client.capabilities().snapshot is True
        snapshot = client.run_query_snapshot(scope_path="/flow")
        since_reply = client.query_client.transport.query(
            SinceRequest(
                kind="since_request",
                schema_version=1,
                scope_path="/flow",
                field_groups=["summary"],
                session_id=snapshot.session_id,
                query_id=snapshot.query_id,
                since_revision=1,
            )
        )
        assert isinstance(since_reply, ResetReply)
        assert since_reply.reason == "structure_changed"
        assert snapshot.ordered_paths == ("/flow", "/flow/task")
        assert snapshot.flow_generations["/flow"]
        assert snapshot.get("/flow/task").attempt_id == "test-attempt"
        detail = client.run_query_detail(snapshot, "/flow/task", ["parameters"])
        assert detail.groups.parameters.user[0].value == "x" * 300000
        assert detail.blob_refs == []
        client.run_query_show(scope_path="/flow", depth=0)
        output = capsys.readouterr().out
        assert "flow [" in output
        assert "task [" not in output
        assert "WIDE" not in output
    finally:
        client.close_channel()


@pytest.mark.anyio
@pytest.mark.parametrize("transport", ["grpc", "http"])
async def test_tui_reads_compact_query_and_selected_detail(
    transport, server_runner_factory, free_port_fixture
):
    grpc_port, http_port = free_port_fixture(), free_port_fixture()
    runner = server_runner_factory(
        port=grpc_port,
        interval_main_loop=1.0,
        connect_config=ConnectConfig(
            server=Server(
                address=Address(
                    hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)
                ),
                http=HttpSettings(host="127.0.0.1", port=str(http_port)),
            )
        ),
    )
    flow = runner.server.bunch.add_flow("flow")
    task = flow.add_task("task")
    task.add_parameter("WIDE", "x" * 300000)
    service = TaklerTuiService(
        "127.0.0.1",
        http_port if transport == "http" else grpc_port,
        transport_name=transport,
    )
    app = TaklerTuiApp(service)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(lambda: "/flow" in app._tree._path_nodes, pilot)
        assert app._snapshot.service.status in {"running", "halted"}
        assert "/flow/task" not in app._tree._path_nodes
        app._tree._path_nodes["/flow"].expand()
        await wait_until(lambda: "/flow/task" in app._tree._path_nodes, pilot)
        app._tree.focus()
        app._tree.select_path("/flow/task")
        await pilot.press("enter")
        await wait_until(
            lambda: (
                app._snapshot.get("/flow/task").user_parameters.get("WIDE")
                == "x" * 300000
            ),
            pilot,
            timeout=15,
        )
        assert app._snapshot.query.get("/flow/task") is not None
        app.action_scope_selected()
        await wait_until(
            lambda: (
                app._snapshot.query.scope_path == "/flow/task"
                or "refresh failed"
                in text_of(app._status_bar.query_one("#status-right", Static))
            ),
            pilot,
        )
        assert app._snapshot.query.scope_path == "/flow/task", text_of(
            app._status_bar.query_one("#status-right", Static)
        )
        assert app._snapshot.service.status in {"running", "halted"}
        app.action_scope_parent()
        await wait_until(
            lambda: (
                app._snapshot.query.scope_path == "/flow"
                or "refresh failed"
                in text_of(app._status_bar.query_one("#status-right", Static))
            ),
            pilot,
        )
        assert app._snapshot.query.scope_path == "/flow", text_of(
            app._status_bar.query_one("#status-right", Static)
        )
