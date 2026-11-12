"""Exercise the full-snapshot routes through in-process adapters."""

import asyncio

import httpx
import pytest

from takler.protocol.query_v1 import (
    CancelFullSnapshotRequest,
    FullSnapshotRequest,
    decode_query_document,
    encode_query_document,
)
from takler.server import full_snapshot
from takler.server.grpc_transport import GrpcTransport
from takler.server.handlers import CommandHandlers
from takler.server.http_transport import create_app
from takler.server.protocol import query_adapter
from tests.full_snapshot_fixtures import _service


@pytest.mark.parametrize("transport", ["http", "grpc"])
def test_transports_send_worker_bytes_without_reencoding(transport, monkeypatch):
    async def run():
        scheduler, _ = _service()
        service = scheduler.query_service.full_snapshots
        produced = []
        original = full_snapshot._encode_full_page

        def encode(page, rows):
            raw = original(page, rows)
            produced.append(raw)
            return raw

        def forbidden(*args, **kwargs):
            raise AssertionError("wire full page must reuse the worker encoding")

        monkeypatch.setattr(full_snapshot, "_encode_full_page", encode)
        monkeypatch.setattr(service, "page", forbidden)
        request = FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
        try:
            if transport == "http":
                import takler.server.http_transport as http_transport

                monkeypatch.setattr(http_transport, "encode_query_document", forbidden)
                app = create_app(CommandHandlers(scheduler))
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://test"
                ) as client:
                    response = await client.post(
                        "/v1/query/full-snapshots",
                        content=encode_query_document(request),
                        headers={"content-type": "application/json"},
                    )
                assert response.status_code == 200
                raw = response.content
            else:
                message = query_adapter.query_to_pb2(request)
                monkeypatch.setattr(query_adapter, "encode_query_document", forbidden)
                response = await GrpcTransport(scheduler).ReadFullSnapshot(
                    message, None
                )
                raw = response.json
            assert len(produced) == 1 and raw == produced[0]
            page = decode_query_document(raw, expected="full_snapshot_page")
            assert page.complete and page.total_nodes == 3
            cancelled = await service.cancel(
                CancelFullSnapshotRequest(
                    kind="cancel_full_snapshot_request",
                    schema_version=1,
                    session_id=page.session_id,
                    full_snapshot_id=page.full_snapshot_id,
                )
            )
            assert cancelled.cancelled and not service._leases
        finally:
            service.close()

    asyncio.run(run())
