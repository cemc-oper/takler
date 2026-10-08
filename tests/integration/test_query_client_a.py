"""Real HTTP/gRPC query reads through the Python client."""

import pytest

from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server


@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_query_snapshot_detail_blob_and_show(
    transport, server_runner_factory, free_port_fixture, capsys
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
