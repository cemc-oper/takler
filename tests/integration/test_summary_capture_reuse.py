"""Independent Python clients share summary capture over real sockets."""

from concurrent.futures import ThreadPoolExecutor
import threading
import time

import pytest

from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server


@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_concurrent_baselines_since_detail_and_shutdown(
    transport, server_runner_factory, free_port_fixture, monkeypatch
):
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
    for index in range(2300):
        flow.add_task(f"t{index}")
    query = runner.server.scheduler.query_service
    original = query._capture
    entered, proceed = threading.Event(), threading.Event()
    calls = []

    def capture(*args):
        calls.append(args[6])
        entered.set()
        assert proceed.wait(5)
        return original(*args)

    monkeypatch.setattr(query, "_capture", capture)
    clients = [runner.make_client(transport_name=transport) for _ in range(3)]
    barrier = threading.Barrier(3)

    def read(client):
        barrier.wait(3)
        view = client.run_query_snapshot(scope_path="/flow")
        assert len(view.ordered_paths) == 2301
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
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = [executor.submit(read, client) for client in clients]
            try:
                assert entered.wait(3)
                time.sleep(0.05)
            finally:
                proceed.set()
            views = [result.result(timeout=15) for result in results]
        assert len({view.snapshot_id for view in views}) == 1
        assert len(calls) == 1
        assert not query._captures and query._reserved == query._capture_readers == 0
    finally:
        proceed.set()
        for client in clients:
            client.close_channel()
        runner.stop()
    assert query._closed and not query._snapshots
    assert not query._query_leases and query._snapshot_expiry is None
