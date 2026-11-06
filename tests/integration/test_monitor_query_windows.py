"""Twenty independently authenticated monitor sessions on a 34-node gRPC tree."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import getpass
import os
import secrets
from threading import Barrier

import grpc
import pytest

from takler.client.query_client import QueryClient
from takler.core import Flow
from takler.exceptions import QueryResponseError
from takler.serialization import export_definition
from takler.server.connect_config import (
    Address,
    ConnectConfig,
    SecuritySettings,
    Server,
)
from takler.server.grpc_transport import GrpcTransport


WINDOWS = 20
TASK = "/demo/task00"


@pytest.fixture
def monitors(server_runner_factory, free_port_fixture, tmp_path, monkeypatch):
    for key in tuple(os.environ):
        if key.startswith("TAKLER_"):
            monkeypatch.delenv(key)
    for key in ("NO_PROXY", "no_proxy"):
        monkeypatch.setenv(key, "127.0.0.1,localhost,::1")

    # Separate processes have separate pools. Avoid gRPC sharing one TCP
    # connection among all twenty channels inside this test process.
    insecure_channel = grpc.insecure_channel

    def independent_channel(target, options):
        return insecure_channel(
            target, [*options, ("grpc.use_local_subchannel_pool", 1)]
        )

    monkeypatch.setattr(grpc, "insecure_channel", independent_channel)
    peers = set()
    read_page = GrpcTransport.ReadPage

    async def record_peer(transport, request, context):
        peers.add(context.peer())
        return await read_page(transport, request, context)

    monkeypatch.setattr(GrpcTransport, "ReadPage", record_peer)
    tokens = [secrets.token_hex(16) for _ in range(WINDOWS)]
    allowed = tmp_path / "operators"
    allowed.touch(mode=0o600)
    allowed.write_text("\n".join(tokens) + "\n")
    whitelist = tmp_path / "whitelist"
    whitelist.write_text(getpass.getuser() + "\n")
    port = free_port_fixture()
    runner = server_runner_factory(
        port=port,
        connect_config=ConnectConfig(
            server=Server(
                address=Address(hostname="127.0.0.1", ip="127.0.0.1", port=str(port))
            ),
            security=SecuritySettings(
                auth_mode="enabled",
                operator_secret_file=str(allowed),
                operator_whitelist_file=str(whitelist),
                audit_file=str(tmp_path / "audit.jsonl"),
            ),
        ),
    )
    with ExitStack() as stack:
        clients = []
        for index, token in enumerate(tokens):
            secret = tmp_path / f"window-{index}"
            secret.touch(mode=0o600)
            secret.write_text(token + "\n")
            client = runner.make_client(
                transport_name="grpc", secret_file=str(secret), retry_window=0
            )
            stack.callback(client.close_channel)
            client.start()
            clients.append(client)
        flow = Flow("demo")
        for index in range(32):
            name = f"task{index:02d}"
            flow.add_task(name).add_parameter("LABEL", name)
        definition = tmp_path / "demo.json"
        definition.write_text(export_definition(flow).model_dump_json())
        assert clients[0].run_command_load(str(definition)).flag == 0
        yield clients
        assert len(peers) == WINDOWS


def simultaneous(clients, operation):
    barrier = Barrier(len(clients), timeout=10)

    def run(index, client):
        barrier.wait()
        return operation(index, client)

    with ThreadPoolExecutor(max_workers=len(clients)) as executor:
        futures = [
            executor.submit(run, index, client) for index, client in enumerate(clients)
        ]
    return [future.result() for future in futures]


def refresh_and_check(client, path, *, suspended):
    before = client.query_client.current
    updated = client.run_query_since(before)
    assert updated.query_id == before.query_id
    assert updated.revision > before.revision
    assert path in updated.changed_paths
    assert not updated.stale_scopes
    assert updated.get(path).suspended is suspended
    detail = client.run_query_detail(updated, path, ["parameters"])
    assert detail.path == path
    assert [(item.name, item.value) for item in detail.groups.parameters.user] == [
        ("LABEL", path.rsplit("/", 1)[1])
    ]
    return updated


@pytest.mark.parametrize("cold_open", ["sequential", "simultaneous"])
def test_independent_monitor_windows(monitors, cold_open):
    if cold_open == "sequential":
        views = [client.run_query_snapshot() for client in monitors]
    else:
        views = simultaneous(monitors, lambda _, client: client.run_query_snapshot())
    assert all(len(view.ordered_paths) == 34 for view in views)
    assert all(not view.get(TASK).suspended for view in views)
    assert len({view.query_id for view in views}) == WINDOWS
    assert len({view.snapshot_id for view in views}) == WINDOWS
    with pytest.raises(QueryResponseError, match="snapshot_expired"):
        monitors[1].run_query_detail(views[0], TASK, ["parameters"])

    idle = simultaneous(
        monitors, lambda index, client: client.run_query_since(views[index])
    )
    for before, after in zip(views, idle):
        assert after.query_id == before.query_id
        assert after.revision == before.revision
        assert not after.changed_paths and not after.stale_scopes

    assert monitors[0].run_command_suspend([TASK]).flag == 0
    updated = simultaneous(
        monitors, lambda _, client: refresh_and_check(client, TASK, suspended=True)
    )
    assert len({view.revision for view in updated}) == 1

    # Keep each monitor's last revision across a transport interruption.
    for client in monitors:
        client.close_channel()
    monitors[0].start()
    assert monitors[0].run_command_resume([TASK]).flag == 0
    for index, client in enumerate(monitors):
        if index:
            client.start()
        # Staggered requests must also catch up from the pre-disconnect revision.
        refresh_and_check(client, TASK, suspended=False)


@pytest.mark.parametrize("shared_subtree", [True, False], ids=["same", "distinct"])
def test_independent_scoped_windows_and_service_details(monitors, shared_subtree):
    paths = [f"/demo/task{index:02d}" for index in range(WINDOWS)]

    # The TUI keeps its selected subtree and a separate depth-zero status query.
    def open_subtree(index, client):
        scope = "/demo" if shared_subtree else paths[index]
        subtree = client.run_query_snapshot(scope_path=scope)
        assert subtree.scope_path == scope and subtree.get(paths[index])
        assert len(subtree.ordered_paths) == (33 if shared_subtree else 1)
        root_client = QueryClient(client.transport)
        root = root_client.read_snapshot(depth=0)
        assert root.ordered_paths == ("/",)
        return root_client, root

    roots = simultaneous(monitors, open_subtree)

    assert monitors[0].run_command_suspend(paths).flag == 0

    def refresh(index, client):
        refresh_and_check(client, paths[index], suspended=True)
        root_client, root = roots[index]
        service = root_client.read_detail(root, "/", ["service", "parameters"])
        assert service.groups.service.status == "running"

    simultaneous(monitors, refresh)
