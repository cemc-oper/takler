"""Python client applies real summary deltas on both query transports."""

import pytest
import os
from pathlib import Path
import subprocess

from takler.core import NodeStatus
from takler.exceptions import QueryResponseError
from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server
from tests.integration.test_query_a_matrix import _environment


@pytest.fixture(scope="module")
def go_sync_binary(tmp_path_factory):
    paired = os.environ.get("TAKLER_QUERY_GO_CLIENT")
    if paired:
        binary = Path(paired)
        assert binary.is_file(), "paired Go binary is missing"
        return binary
    repo = Path(__file__).resolve().parents[3] / "takler-client"
    if not (repo / "go.mod").exists():
        pytest.skip("paired Go checkout is unavailable")
    binary = tmp_path_factory.mktemp("go-query-b") / "takler_client"
    subprocess.run(
        ["go", "build", "-o", str(binary), "main.go"], cwd=repo, check=True, timeout=90
    )
    return binary


@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_query_since_sparse_and_structure_reset(
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
    client = runner.make_client(transport_name=transport)
    if transport == "http":
        client.port = http_port
    client.start()
    try:
        first = client.run_query_snapshot(scope_path="/flow")
        assert first.revision is not None
        task.set_node_status(NodeStatus.queued)
        second = client.run_query_since(first)
        assert second.revision > first.revision
        assert second.get("/flow/task").status == "queued"
        assert first.get("/flow/task").status != "queued"
        flow.add_task("new")
        with pytest.raises(QueryResponseError) as exc:
            client.run_query_since(second)
        assert exc.value.code == "reset_required"
        assert client.query_client.current.is_stale()
        rebuilt = client.run_query_snapshot(scope_path="/flow")
        assert rebuilt.get("/flow/new") is not None
        assert not rebuilt.stale_scopes
        root = client.run_query_snapshot()
        flow.add_task("last")
        locally_stale = client.run_query_since(root)
        assert locally_stale.revision > root.revision
        assert locally_stale.stale_scopes == ("/flow",)
        assert not locally_stale.is_stale("/")
        selected = client.run_query_snapshot(scope_path="/flow")
        assert selected.get("/flow/last") is not None
    finally:
        client.close_channel()


@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_go_sync_cli_reads_real_since_route(
    transport, go_sync_binary, server_runner_factory, free_port_fixture, monkeypatch
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
    task = runner.server.bunch.add_flow("flow").add_task("task")
    service = runner.server.scheduler.query_service
    original_since = service.since
    calls = []

    async def changed_since(request):
        calls.append(request.since_revision)
        task.set_node_status(NodeStatus.queued)
        return await original_since(request)

    monkeypatch.setattr(service, "since", changed_since)
    port = http_port if transport == "http" else grpc_port
    selector = ["--flow", "flow"] if transport == "http" else ["--scope", "/flow"]
    result = subprocess.run(
        [str(go_sync_binary), "sync", *selector, "--polls", "1"],
        env=_environment(transport, port),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "# revision=" in result.stdout
    assert "task [queued]" in result.stdout
    assert len(calls) == 1
