"""Python client applies real summary deltas on both query transports."""

import pytest

from takler.core import NodeStatus
from takler.exceptions import QueryResponseError
from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server


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
