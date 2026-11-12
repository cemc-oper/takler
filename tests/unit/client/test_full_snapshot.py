"""Check the full snapshot reader with a small in-process transport."""

import asyncio

import pytest

from takler.client.full_snapshot import FullSnapshotClient
from takler.core import Bunch, Flow
from takler.exceptions import QueryResponseError, ServerResponseError
from takler.protocol.query_v1 import FullSnapshotRequest
from takler.server.scheduler import Scheduler
from tests.full_snapshot_fixtures import _LocalTransport, _service


def test_full_snapshot_client_resolves_blob_and_checks_completion():
    scheduler, _ = _service(payload="x" * 300_000)
    transport = _LocalTransport(scheduler)
    client = FullSnapshotClient(transport)
    view = client.read_full_snapshot(scope_path="/f")
    assert view.completion.total_nodes == 2
    assert (
        view.nodes_by_path["/f/t0000"]["groups"]["parameters"]["user"][0]["value"]
        == "x" * 300_000
    )
    assert not scheduler.query_service.full_snapshots._leases

    def corrupt(value):
        if value.kind == "full_snapshot_page" and value.complete:
            return value.model_copy(update={"sha256": "0" * 64})
        return value

    transport.mutate = corrupt
    with pytest.raises(ServerResponseError, match="completion mismatch"):
        client.read_full_snapshot()
    assert not scheduler.query_service.full_snapshots._leases

    transport.mutate = lambda value: (
        value.model_copy(update={"service": {**value.service, "message": "tampered"}})
        if value.kind == "full_snapshot_page" and value.page_index == 0
        else value
    )
    with pytest.raises(ServerResponseError, match="completion mismatch"):
        client.read_full_snapshot()


def test_full_snapshot_worker_fault_and_client_capability_failure():
    async def run():
        scheduler, _ = _service()
        service = scheduler.query_service.full_snapshots
        service._render_page = lambda lease: (_ for _ in ()).throw(
            RuntimeError("fault")
        )
        reply = await service.page(
            FullSnapshotRequest(
                kind="full_snapshot_request",
                schema_version=1,
            )
        )
        assert reply.kind == "error" and reply.code == "internal_error"
        assert not service._leases

    asyncio.run(run())

    scheduler, _ = _service()
    transport = _LocalTransport(scheduler)
    transport.mutate = lambda value: (
        value.model_copy(update={"full_snapshot_v1": False})
        if value.kind == "capabilities"
        else value
    )
    with pytest.raises(QueryResponseError) as error:
        FullSnapshotClient(transport).read_full_snapshot()
    assert error.value.code == "unsupported_capability"


def test_full_snapshot_subtree_large_ancestor_parameter_uses_blob():
    bunch = Bunch("root")
    bunch.add_parameter("PARENT_VALUE", "z" * 300_000)
    bunch.add_flow(Flow("f")).add_task("t")
    scheduler = Scheduler(bunch)
    view = FullSnapshotClient(_LocalTransport(scheduler)).read_full_snapshot(
        scope_path="/f/t"
    )
    assert view.completion.total_nodes == 1
    assert view.completion.ancestor_context[-1]["user"][0]["value"] == "z" * 300_000
    assert not scheduler.query_service.full_snapshots._leases


def test_full_snapshot_large_root_service_field_uses_blob():
    scheduler, _ = _service()
    status = scheduler.version_store.pin().public_node("/")["server_status"]
    scheduler.version_store.status_provider = lambda: {
        **status,
        "message": "m" * 300_000,
    }
    scheduler.version_store.note("/", False)
    view = FullSnapshotClient(_LocalTransport(scheduler)).read_full_snapshot()
    assert view.completion.service["message"] == "m" * 300_000
    assert not scheduler.query_service.full_snapshots._leases
