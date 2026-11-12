"""Independent Python clients share summary capture over real sockets."""

from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server


@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_concurrent_baselines_since_detail_and_shutdown(
    transport, server_runner_factory, free_port_fixture, monkeypatch
):
    # Keep the socket test paginated without building a full-size page.
    monkeypatch.setattr("takler.server.query_service.PAGE_TARGET_BYTES", 1200)
    grpc_port, http_port = free_port_fixture(), free_port_fixture()
    runner = server_runner_factory(
        port=grpc_port,
        interval_main_loop=10,
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
    flow.add_task("t0")
    flow.add_task("t1")
    query = runner.server.scheduler.query_service
    begin_page = query._begin_page
    original = query._capture
    client_count = 3
    readers_joined = threading.Event()
    calls = []

    def track_readers(*args):
        result = begin_page(*args)
        if any(pending.readers == client_count for pending in query._captures.values()):
            readers_joined.set()
        return result

    def capture(*args):
        calls.append(args[6])
        assert readers_joined.wait(10), "clients did not join the same capture"
        return original(*args)

    monkeypatch.setattr(query, "_begin_page", track_readers)
    monkeypatch.setattr(query, "_capture", capture)
    clients = [
        runner.make_client(transport_name=transport) for _ in range(client_count)
    ]
    barrier = threading.Barrier(client_count, timeout=10)

    def read(client):
        barrier.wait()
        view = client.run_query_snapshot(scope_path="/flow")
        assert view.ordered_paths == ("/flow", "/flow/t0", "/flow/t1")
        updated = client.run_query_since(view)
        assert updated.query_id == view.query_id
        assert (
            client.run_query_detail(updated, "/flow/t0", ["runtime"]).path == "/flow/t0"
        )
        return view

    try:
        for client in clients:
            if transport == "http":
                client.port = http_port
            client.start()
        with ThreadPoolExecutor(max_workers=client_count) as executor:
            results = [executor.submit(read, client) for client in clients]
            views = [result.result(timeout=15) for result in results]
        assert len({view.snapshot_id for view in views}) == 1
        assert len(query._snapshots[views[0].snapshot_id].content.pages) > 1
        assert len(calls) == 1
        assert not query._captures and query._reserved == 0
    finally:
        for client in clients:
            client.close_channel()
        runner.stop()
    assert query._closed and not query._snapshots
    assert not query._query_leases and query._snapshot_expiry is None
