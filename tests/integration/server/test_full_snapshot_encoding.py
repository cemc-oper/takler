"""Encoded full pages keep the lease's wire and fixed-version guarantees."""

import asyncio

import pytest

from takler.client.full_snapshot import FullSnapshotClient
from takler.protocol.query_v1 import (
    FullSnapshotRequest,
    decode_query_document,
    encode_query_document,
)
from takler.server import full_snapshot
from tests.full_snapshot_fixtures import _LocalTransport, _service


def test_encoded_pages_encode_once_and_replay_identical_bytes(monkeypatch):
    encoded = []
    original = full_snapshot._encode_full_page

    def encode(page, rows):
        raw = original(page, rows)
        encoded.append(raw)
        return raw

    monkeypatch.setattr(full_snapshot, "_encode_full_page", encode)

    async def run():
        scheduler, flow = _service(tasks=1000)
        service = scheduler.query_service.full_snapshots
        request = FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
        try:
            first_raw = await service.page_encoded(request)
            first = decode_query_document(first_raw, expected="full_snapshot_page")
            lease = service._leases[first.full_snapshot_id]
            assert lease.last_page is first_raw is encoded[0]
            assert not first.complete
            flow.add_task("later")
            continuation = request.model_copy(
                update={
                    "full_snapshot_id": first.full_snapshot_id,
                    "cursor": first.next_cursor,
                }
            )
            second_raw, structured = await asyncio.gather(
                service.page_encoded(continuation), service.page(continuation)
            )
            assert second_raw is encoded[1] is lease.last_page
            assert len(encoded) == 2
            assert encode_query_document(structured) == second_raw
            structured.nodes[0][0] = "/tampered"
            assert await service.page_encoded(continuation) is second_raw
            assert (await service.page(continuation)).nodes[0][0] != "/tampered"
            assert len(encoded) == 2
            pages = [first, decode_query_document(second_raw)]
            while not pages[-1].complete:
                prior = pages[-1]
                continuation = request.model_copy(
                    update={
                        "full_snapshot_id": prior.full_snapshot_id,
                        "cursor": prior.next_cursor,
                    }
                )
                raw = await service.page_encoded(continuation)
                assert raw is encoded[-1] is lease.last_page
                page = decode_query_document(raw)
                assert encode_query_document(page) == raw
                pages.append(page)
            assert len(encoded) == len(pages)
            assert pages[-1].total_nodes == 1002
            assert all(page.state_version == first.state_version for page in pages)
            assert not any(row[0] == "/f/later" for page in pages for row in page.nodes)
            assert await service.page_encoded(continuation) is encoded[-1]
        finally:
            service.close()
        assert not service._leases

    asyncio.run(run())


def test_encoded_replay_checks_owner_selection_cursor_and_expiry():
    async def run():
        scheduler, _ = _service(tasks=1000)
        service = scheduler.query_service.full_snapshots
        owner = ["a"]
        scheduler.query_service._owner = lambda: owner[0]
        request = FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
        try:
            first = decode_query_document(await service.page_encoded(request))
            continuation = request.model_copy(
                update={
                    "full_snapshot_id": first.full_snapshot_id,
                    "cursor": first.next_cursor,
                }
            )
            raw = await service.page_encoded(continuation)
            owner[0] = "b"
            denied = decode_query_document(await service.page_encoded(continuation))
            assert denied.code == "permission_denied"
            owner[0] = "a"
            for changes, code in (
                ({"scope_path": "/f"}, "permission_denied"),
                ({"field_groups": ["summary"]}, "permission_denied"),
                ({"cursor": "bad"}, "invalid_request"),
            ):
                error = decode_query_document(
                    await service.page_encoded(continuation.model_copy(update=changes))
                )
                assert error.code == code
            assert await service.page_encoded(continuation) is raw
            service._leases[first.full_snapshot_id].used -= full_snapshot.IDLE_SECONDS
            expired = decode_query_document(await service.page_encoded(continuation))
            assert expired.code == "snapshot_expired" and not service._leases
        finally:
            service.close()

    asyncio.run(run())


@pytest.mark.parametrize("payload", ["", "α" * 100_000])
def test_encoded_client_checks_complete_fixed_read_and_blob(payload):
    scheduler, flow = _service(tasks=1000, payload=payload)
    flow.add_parameter("TAKLER_SECRET", "never-publish-this-secret")
    changed = False

    class WireTransport(_LocalTransport):
        def query(self, request):
            nonlocal changed
            if not isinstance(request, FullSnapshotRequest):
                return super().query(request)
            raw = asyncio.run(self.service.full_snapshots.page_encoded(request))
            assert b"never-publish-this-secret" not in raw
            if not changed:
                changed = True
                flow.add_task("later")
                flow.children[0].add_parameter("PAYLOAD", "changed")
            return decode_query_document(raw)

    view = FullSnapshotClient(WireTransport(scheduler)).read_full_snapshot()
    assert view.completion.total_nodes == 1002
    assert "/f/later" not in view.nodes_by_path
    if payload:
        assert (
            view.nodes_by_path["/f/t0000"]["groups"]["parameters"]["user"][0]["value"]
            == payload
        )
    assert not scheduler.query_service.full_snapshots._leases
