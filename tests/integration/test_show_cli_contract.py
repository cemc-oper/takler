"""Paired CLI and TUI queries consume safe views over real HTTP and gRPC."""

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from takler.core import Task
from takler.query import parse_show
from takler.protocol import ShowRequest
from takler.server.connect_config import (
    Address,
    ConnectConfig,
    HttpSettings,
    SecuritySettings,
    Server,
)

CLIENTS = ["python", "tui"] + (
    ["go"] if os.environ.get("TAKLER_SHOW_GO_CLIENT") else []
)


@pytest.mark.parametrize("client", CLIENTS)
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_show_cli_contract(client, transport, server_runner_factory, free_port_fixture):
    grpc_port, http_port = free_port_fixture(), free_port_fixture()
    runner = server_runner_factory(
        port=grpc_port,
        connect_config=ConnectConfig(
            server=Server(
                address=Address(
                    hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)
                ),
                http=HttpSettings(host="127.0.0.1", port=str(http_port)),
            ),
            security=SecuritySettings(query_redacted_parameters=["BUSINESS_TOKEN"]),
        ),
    )
    bunch = runner.server.bunch
    bunch.add_parameter("ROOT_SETTING", "root-value")
    bunch.add_parameter("TAKLER_PASS", "ROOT_SECRET")
    flow = bunch.add_flow("flow")
    flow.add_parameter("BUSINESS_TOKEN", "CUSTOM_SECRET")

    class PluginTask(Task):
        def to_dict(self):
            raise AssertionError("execution exporter must not run")

    task = PluginTask("custom")
    task.add_parameter("Takler-Secret", "TASK_SECRET")
    task.suspend()
    flow.append_child(task)
    command = (
        [str(Path(sys.executable).with_name("takler-client-py"))]
        if client in ("python", "tui")
        else [os.environ["TAKLER_SHOW_GO_CLIENT"]]
    )
    if client == "tui":
        # A fresh process has no access to the server's local PluginTask class.
        command = [
            sys.executable,
            "-c",
            """
import os
import json
from takler.tui.service import TaklerTuiService
from takler.tui.query_view import QueryTuiView
with TaklerTuiService(os.environ['TAKLER_HOST'], os.environ['TAKLER_PORT'],
                     transport_name=os.environ['TAKLER_TRANSPORT']) as service:
    snapshot = service.read_snapshot()
    view = QueryTuiView(snapshot)
    root = service.read_detail(snapshot, '/', ['parameters', 'service'])
    view.apply_root_detail(root)
    detail = service.read_detail(snapshot, '/flow/custom', ['parameters'])
    view.apply_detail('/flow/custom', detail)
assert view.get('/flow/custom').suspended is True
assert view.lookup_parameter('/flow/custom', 'ROOT_SETTING') == 'root-value'
assert view.resolve_parameter('/flow/custom', 'BUSINESS_TOKEN').redacted
print(json.dumps({
    'path': '/flow/custom',
    'suspended': view.get('/flow/custom').suspended,
    'root_setting': view.lookup_parameter('/flow/custom', 'ROOT_SETTING'),
    'redacted': view.resolve_parameter('/flow/custom', 'BUSINESS_TOKEN').text,
}))
""",
        ]
    env = {k: v for k, v in os.environ.items() if not k.startswith("TAKLER_")}
    env.update(
        TAKLER_HOST="127.0.0.1",
        TAKLER_PORT=str(http_port if transport == "http" else grpc_port),
        TAKLER_TRANSPORT=transport,
        TAKLER_TIMEOUT="0",
        NO_PROXY="127.0.0.1,localhost",
        no_proxy="127.0.0.1,localhost",
    )
    result = subprocess.run(
        [*command, *([] if client == "tui" else ["show", "--show-all"])],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "root-value" in result.stdout and "custom" in result.stdout
    assert "<redacted>" in result.stdout
    for value in ("ROOT_SECRET", "CUSTOM_SECRET", "TASK_SECRET"):
        assert value not in result.stdout + result.stderr
    if client == "tui":
        payload = result.stdout
        data = json.loads(payload)
        assert data["path"] == "/flow/custom"
        assert data["suspended"] is True
        assert data["root_setting"] == "root-value"
        assert data["redacted"] == "<redacted>"
    else:
        assert "|- custom [suspend (unknown)]" in result.stdout
    view = parse_show(
        runner.server.scheduler.handle_request_show(
            ShowRequest(
                show_trigger=True,
                show_parameter=True,
                show_limit=True,
                show_event=True,
                show_meter=True,
            )
        )
    )
    assert view.lookup_parameter("/flow/custom", "ROOT_SETTING") == "root-value"
    assert view.resolve_parameter("/flow/custom", "BUSINESS_TOKEN").redacted


@pytest.mark.skipif(
    not os.environ.get("TAKLER_SHOW_GO_CLIENT"),
    reason="paired Go client is unavailable",
)
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_go_show_reads_chunked_detail(
    transport, server_runner_factory, free_port_fixture
):
    grpc_port, http_port = free_port_fixture(), free_port_fixture()
    runner = server_runner_factory(
        port=grpc_port,
        connect_config=ConnectConfig(
            server=Server(
                address=Address(
                    hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)
                ),
                http=HttpSettings(host="127.0.0.1", port=str(http_port)),
            )
        ),
    )
    task = runner.server.bunch.add_flow("flow").add_task("task")
    task.add_parameter("WIDE", "x" * 300000)
    env = {k: v for k, v in os.environ.items() if not k.startswith("TAKLER_")}
    env.update(
        TAKLER_TRANSPORT=transport,
        NO_PROXY="127.0.0.1,localhost",
        no_proxy="127.0.0.1,localhost",
    )
    result = subprocess.run(
        [
            os.environ["TAKLER_SHOW_GO_CLIENT"],
            "show",
            "--host",
            "127.0.0.1",
            "--port",
            str(http_port if transport == "http" else grpc_port),
            "--scope",
            "/flow/task",
            "--show-parameter",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "param WIDE '" + "x" * 300000 + "'" in result.stdout
