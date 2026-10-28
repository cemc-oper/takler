"""Fixed full-detail lease, streaming proof, and failure boundaries."""

from __future__ import annotations

import asyncio
import base64

import pytest

from takler.client.full_snapshot import FullSnapshotClient
from takler.core import Bunch, Flow, Task
from takler.exceptions import QueryResponseError, ServerResponseError
from takler.protocol.query_v1 import (
    CancelFullSnapshotRequest,
    FullSnapshotChunkRequest,
    FullSnapshotRequest,
)
from takler.server.scheduler import Scheduler
import takler.server.full_snapshot as snapshot_module


def _service(tasks=1, payload=""):
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    for index in range(tasks):
        task = flow.add_task(f"t{index:04d}")
        if index == 0 and payload:
            task.add_parameter("PAYLOAD", payload)
    return Scheduler(bunch), flow


class _LocalTransport:
    def __init__(self, scheduler):
        self.service = scheduler.query_service
        self.mutate = lambda value: value

    def query(self, request):
        if isinstance(request, FullSnapshotRequest):
            result = asyncio.run(self.service.full_snapshots.page(request))
        elif isinstance(request, FullSnapshotChunkRequest):
            result = asyncio.run(self.service.full_snapshots.chunk(request))
        elif isinstance(request, CancelFullSnapshotRequest):
            result = asyncio.run(self.service.full_snapshots.cancel(request))
        else:
            result = self.service.capabilities()
        return self.mutate(result)


def test_large_snapshot_gc_setting_is_restored_on_release(monkeypatch):
    current = [(2000, 10, 0)]
    monkeypatch.setattr(snapshot_module, "_has_incremental_gc", lambda: True)
    monkeypatch.setattr(snapshot_module, "GC_TUNE_MIN_PATHS", 1)
    monkeypatch.setattr(snapshot_module.gc, "isenabled", lambda: True)
    monkeypatch.setattr(snapshot_module.gc, "get_threshold", lambda: current[0])
    monkeypatch.setattr(
        snapshot_module.gc,
        "set_threshold",
        lambda *values: current.__setitem__(0, values),
    )

    async def run():
        scheduler, _ = _service(tasks=1000)
        service = scheduler.query_service.full_snapshots
        request = FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
        try:
            first = await service.page(request)
            second = await service.page(request)
            assert not first.complete and not second.complete
            assert current[0] == (2000, 1000, 0)

            cancelled = await service.cancel(
                CancelFullSnapshotRequest(
                    kind="cancel_full_snapshot_request",
                    schema_version=1,
                    session_id=first.session_id,
                    full_snapshot_id=first.full_snapshot_id,
                )
            )
            assert cancelled.cancelled
            assert current[0] == (2000, 1000, 0)
            service.close()
            assert current[0] == (2000, 10, 0)

            page = await service.page(request)
            assert not page.complete
            while not page.complete:
                page = await service.page(
                    request.model_copy(
                        update={
                            "full_snapshot_id": page.full_snapshot_id,
                            "cursor": page.next_cursor,
                        }
                    )
                )
            assert current[0] == (2000, 10, 0)

            expired = await service.page(request)
            assert not expired.complete
            service._leases[
                expired.full_snapshot_id
            ].used -= snapshot_module.IDLE_SECONDS
            service._purge()
            assert current[0] == (2000, 10, 0)

            small_scheduler, _ = _service()
            small_service = small_scheduler.query_service.full_snapshots
            complete = await small_service.page(request)
            assert complete.complete
            assert current[0] == (2000, 10, 0)
            small_service.close()

            third = await service.page(request)
            assert not third.complete
            current[0] = (2000, 400, 0)
            service.close()
            assert current[0] == (2000, 400, 0)
        finally:
            service.close()

    asyncio.run(run())


def test_full_snapshot_keeps_one_version_across_pages_and_cancellation():
    async def run():
        scheduler, flow = _service(tasks=1000)
        service = scheduler.query_service.full_snapshots
        request = FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
        first = await service.page(request)
        assert first.kind == "full_snapshot_page" and not first.complete
        version = first.state_version
        flow.add_task("later")
        pages = [first]
        while not pages[-1].complete:
            prior = pages[-1]
            pages.append(
                await service.page(
                    request.model_copy(
                        update={
                            "full_snapshot_id": prior.full_snapshot_id,
                            "cursor": prior.next_cursor,
                        }
                    )
                )
            )
        assert all(page.state_version == version for page in pages)
        assert pages[-1].total_nodes == 1002
        assert not any(row[0] == "/f/later" for page in pages for row in page.nodes)
        cancel = CancelFullSnapshotRequest(
            kind="cancel_full_snapshot_request",
            schema_version=1,
            session_id=first.session_id,
            full_snapshot_id=first.full_snapshot_id,
        )
        assert (await service.cancel(cancel)).cancelled
        assert not (await service.cancel(cancel)).cancelled
        assert not service._leases

    asyncio.run(run())


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


def test_full_snapshot_rejects_tampered_blob_and_expires_idle_lease():
    scheduler, _ = _service(payload="y" * 100_000)
    transport = _LocalTransport(scheduler)
    client = FullSnapshotClient(transport)

    def corrupt(value):
        if value.kind == "full_snapshot_chunk":
            return value.model_copy(
                update={"data_base64": base64.b64encode(b"bad").decode()}
            )
        return value

    transport.mutate = corrupt
    with pytest.raises(
        ServerResponseError,
        match="chunk completion mismatch|chunk length|digest mismatch",
    ):
        client.read_full_snapshot()
    assert not scheduler.query_service.full_snapshots._leases

    async def expire():
        service = scheduler.query_service.full_snapshots
        request = FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
        first = await service.page(request)
        service._leases[first.full_snapshot_id].used -= 301
        expired = await service.page(
            request.model_copy(
                update={
                    "full_snapshot_id": first.full_snapshot_id,
                    "cursor": first.next_cursor or "cursor",
                }
            )
        )
        assert expired.kind == "error" and expired.code == "snapshot_expired"
        assert not service._leases

    asyncio.run(expire())


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


def test_idle_full_snapshot_releases_root_without_another_request(monkeypatch):
    from takler.server import full_snapshot

    monkeypatch.setattr(full_snapshot, "IDLE_SECONDS", 0.02)

    async def run():
        scheduler, _ = _service()
        service = scheduler.query_service.full_snapshots
        reply = await service.page(
            FullSnapshotRequest(
                kind="full_snapshot_request",
                schema_version=1,
            )
        )
        assert reply.full_snapshot_id in service._leases
        await asyncio.sleep(0.06)
        assert not service._leases

    asyncio.run(run())


def test_full_snapshot_scope_owner_cursor_and_secret_projection():
    async def run():
        scheduler, flow = _service(tasks=1000)
        flow.add_parameter("TAKLER_SECRET", "never-publish-this-secret")
        service = scheduler.query_service.full_snapshots
        scheduler.query_service._owner = lambda: "owner-a"
        request = FullSnapshotRequest(
            kind="full_snapshot_request",
            schema_version=1,
            scope_path="/f",
            field_groups=["summary", "parameters"],
        )
        first = await service.page(request)
        assert first.kind == "full_snapshot_page"
        assert not first.complete
        assert b"never-publish-this-secret" not in first.model_dump_json().encode()
        continuation = request.model_copy(
            update={
                "full_snapshot_id": first.full_snapshot_id,
                "cursor": first.next_cursor,
            }
        )
        scheduler.query_service._owner = lambda: "owner-b"
        denied = await service.page(continuation)
        assert denied.kind == "error" and denied.code == "permission_denied"
        scheduler.query_service._owner = lambda: "owner-a"
        replay = await service.page(continuation.model_copy(update={"cursor": "bad"}))
        assert replay.kind == "error" and replay.code == "invalid_request"
        valid = await service.page(continuation)
        assert valid.kind == "full_snapshot_page"

        missing = await service.page(
            FullSnapshotRequest(
                kind="full_snapshot_request",
                schema_version=1,
                scope_path="/missing",
            )
        )
        assert missing.kind == "error" and missing.code == "invalid_request"

    asyncio.run(run())


def test_unregistered_plugin_is_rejected_without_partial_completion():
    class LocalTask(Task):
        pass

    async def run():
        bunch = Bunch()
        bunch.add_flow(Flow("f")).add_task(LocalTask("plugin"))
        service = Scheduler(bunch).query_service.full_snapshots
        reply = await service.page(
            FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
        )
        assert reply.kind == "error"
        assert reply.code == "unsupported_capability"
        assert not service._leases

    asyncio.run(run())


def test_full_snapshot_enforces_owner_and_global_lease_limits():
    async def run():
        scheduler, _ = _service()
        service = scheduler.query_service.full_snapshots
        owner = ["a"]
        scheduler.query_service._owner = lambda: owner[0]
        request = FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
        created = {}
        for name in ("a", "b"):
            owner[0] = name
            created[name] = [await service.page(request) for _ in range(2)]
            assert all(reply.kind == "full_snapshot_page" for reply in created[name])
            denied = await service.page(request)
            assert denied.kind == "error" and denied.code == "resource_exhausted"
        owner[0] = "c"
        denied = await service.page(request)
        assert denied.kind == "error" and denied.code == "resource_exhausted"
        owner[0] = "a"
        released = await service.cancel(
            CancelFullSnapshotRequest(
                kind="cancel_full_snapshot_request",
                schema_version=1,
                session_id=created["a"][0].session_id,
                full_snapshot_id=created["a"][0].full_snapshot_id,
            )
        )
        assert released.cancelled
        owner[0] = "c"
        assert (await service.page(request)).kind == "full_snapshot_page"

    asyncio.run(run())


def test_full_snapshot_encoded_budget_counts_blob_payload(monkeypatch):
    from takler.server import full_snapshot

    monkeypatch.setattr(full_snapshot, "MAX_ENCODED_BYTES", 50_000)

    async def run():
        scheduler, _ = _service(payload="x" * 100_000)
        service = scheduler.query_service.full_snapshots
        reply = await service.page(
            FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
        )
        assert reply.kind == "error" and reply.code == "resource_exhausted"
        assert not service._leases

    asyncio.run(run())


def test_full_snapshot_concurrent_same_cursor_replays_one_page():
    async def run():
        scheduler, _ = _service(tasks=1000)
        service = scheduler.query_service.full_snapshots
        request = FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
        first = await service.page(request)
        assert first.kind == "full_snapshot_page" and first.next_cursor
        next_request = request.model_copy(
            update={
                "full_snapshot_id": first.full_snapshot_id,
                "cursor": first.next_cursor,
            }
        )
        left, right = await asyncio.gather(
            service.page(next_request), service.page(next_request)
        )
        assert left.kind == right.kind == "full_snapshot_page"
        assert left.model_dump() == right.model_dump()
        assert left.page_index == 1

    asyncio.run(run())


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


def test_full_snapshot_unknown_group_reports_capability_failure():
    async def run():
        scheduler, _ = _service()
        reply = await scheduler.query_service.full_snapshots.page(
            FullSnapshotRequest(
                kind="full_snapshot_request",
                schema_version=1,
                field_groups=["summary", "unknown_group"],
            )
        )
        assert reply.kind == "error" and reply.code == "unsupported_capability"

    asyncio.run(run())
