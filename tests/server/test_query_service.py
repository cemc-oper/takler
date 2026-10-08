"""R2-07A snapshot, identity, and chunk invariants."""

import asyncio
import base64
import hashlib

from takler.core import Bunch, Flow
from takler.protocol.query_v1 import (
    CapabilitiesRequest,
    ChunkRequest,
    DetailRequest,
    PageRequest,
    SinceRequest,
    encode_query_document,
)
from takler.server.auth import (
    CallCredentials,
    reset_call_credentials,
    set_call_credentials,
)
from takler.server.query_changes import MAX_CHANGE_BATCHES
from takler.server.scheduler import Scheduler


def page_request(**changes):
    data = dict(
        kind="page_request",
        schema_version=1,
        scope_path=None,
        flow_name=None,
        depth=None,
        field_groups=["summary"],
        cursor=None,
    )
    data.update(changes)
    return PageRequest(**data)


def test_snapshot_pages_survive_live_mutation_and_replay():
    asyncio.run(_snapshot_pages_survive_live_mutation_and_replay())


def test_owner_replacement_expires_old_cursor():
    async def exercise():
        bunch = Bunch("root")
        flow = bunch.add_flow(Flow("f"))
        for index in range(2300):
            flow.add_task(f"t{index:05}")
        service = Scheduler(bunch).query_service
        first = await service.page(page_request())
        assert first.next_cursor
        await service.page(page_request())
        newest = await service.page(page_request())
        assert newest.kind == "page"
        assert (
            await service.page(page_request(cursor=first.next_cursor))
        ).code == "snapshot_expired"
        assert (
            await service.page(page_request(cursor=newest.next_cursor))
        ).kind == "page"

    asyncio.run(exercise())


async def _snapshot_pages_survive_live_mutation_and_replay():
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    for index in range(4000):
        flow.add_task(f"t{index:05}")
    scheduler = Scheduler(bunch)
    service = scheduler.query_service
    flow.add_task("before")
    base_revision = scheduler.query_changes.revision
    first = await service.page(page_request())
    assert first.kind == "page" and first.next_cursor
    assert first.base_revision == base_revision
    assert service._snapshots[first.snapshot_id].base_revision == base_revision
    assert (
        await service.page(page_request(field_groups=["summary", "runtime"]))
    ).code == "unsupported_capability"
    assert len(first.nodes) <= 2048
    assert len(encode_query_document(first)) <= 262144
    # This mutation is newer than the captured view.
    flow.add_task("late")
    cursor = first.next_cursor
    seen = [node.path for node in first.nodes]
    index = 1
    while cursor:
        request = page_request(cursor=cursor)
        next_page = await service.page(request)
        replay = await service.page(request)
        assert encode_query_document(next_page) == encode_query_document(replay)
        assert next_page.snapshot_id == first.snapshot_id
        assert next_page.base_revision == base_revision
        assert next_page.as_of == first.as_of
        assert next_page.page_index == index
        assert len(next_page.nodes) <= 2048
        assert len(encode_query_document(next_page)) <= 262144
        seen.extend(node.path for node in next_page.nodes)
        cursor = next_page.next_cursor
        index += 1
    assert len(seen) == 4003 and "/f/late" not in seen
    assert len(set(seen)) == len(seen)
    batches = scheduler.query_changes.since(base_revision)
    assert batches is not None
    assert any(
        change.path == "/f" and change.invalidate
        for batch in batches
        for change in batch.changes
    )
    assert (
        await service.page(page_request(cursor=first.next_cursor + "x"))
    ).code == "invalid_request"
    service._snapshots.clear()
    assert (
        await service.page(page_request(cursor=first.next_cursor))
    ).code == "snapshot_expired"


def test_detail_scope_identity_redaction_and_chunks():
    asyncio.run(_detail_scope_identity_redaction_and_chunks())


async def _detail_scope_identity_redaction_and_chunks():
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task("t")
    task.add_parameter("SECRET", "private")
    task.add_parameter("PAYLOAD", "x" * 300_000)
    scheduler = Scheduler(bunch, query_redacted_parameters=("secret",))
    service = scheduler.query_service
    first = await service.page(page_request(scope_path="/f", depth=1))
    request = DetailRequest(
        kind="detail_request",
        schema_version=1,
        session_id=first.session_id,
        query_id=first.query_id,
        path="/f/t",
        flow_generation=flow.definition_generation,
        attempt_id=None,
        field_groups=["parameters"],
    )
    detail = await service.detail(request)
    assert detail.kind == "detail" and detail.sample_mode == "live"
    assert len(encode_query_document(detail)) <= 262144
    assert len(detail.blob_refs) == 1
    ref = detail.blob_refs[0]
    chunks = []
    offset = 0
    while True:
        chunk = await service.chunk(
            ChunkRequest(
                kind="chunk_request",
                schema_version=1,
                blob_id=ref.blob_id,
                offset=offset,
            )
        )
        raw = base64.b64decode(chunk.data_base64)
        assert len(raw) <= 65536
        chunks.append(raw)
        offset += len(raw)
        if chunk.complete:
            break
    value = b"".join(chunks)
    assert hashlib.sha256(value).hexdigest() == ref.sha256
    assert len(value) == ref.total_bytes
    assert "x" * 300_000 in value.decode()
    assert "private" not in value.decode()
    assert (
        await service.detail(request.model_copy(update={"flow_generation": "old"}))
    ).code == "stale_identity"
    assert (
        await service.detail(
            request.model_copy(update={"path": "/", "flow_generation": None})
        )
    ).code == "permission_denied"
    token = set_call_credentials(CallCredentials(user="another", secret="different"))
    try:
        assert (
            await service.page(page_request(cursor=first.next_cursor or "bad"))
        ).kind == "error"
        assert (
            await service.chunk(
                ChunkRequest(
                    kind="chunk_request",
                    schema_version=1,
                    blob_id=ref.blob_id,
                    offset=0,
                )
            )
        ).code == "permission_denied"
    finally:
        reset_call_credentials(token)


def test_capture_rejects_concurrent_mutation_without_blocking_writes():
    asyncio.run(_capture_rejects_concurrent_mutation())


async def _capture_rejects_concurrent_mutation():
    import threading

    bunch = Bunch("root")
    bunch.add_flow(Flow("f")).add_task("t")
    scheduler = Scheduler(bunch)
    service = scheduler.query_service
    entered = threading.Event()
    release = threading.Event()
    capture = service._capture

    def delayed(*args):
        entered.set()
        release.wait(timeout=5)
        return capture(*args)

    service._capture = delayed
    pending = asyncio.create_task(service.page(page_request()))
    assert await asyncio.to_thread(entered.wait, 5)
    async with scheduler.query_state_lock:
        scheduler.query_revision += 1
        bunch.find_flow("f").add_task("new")
        scheduler.query_revision += 1
    release.set()
    result = await pending
    assert result.kind == "error" and result.code == "resource_exhausted"
    assert not service._snapshots


def test_capture_rejects_direct_core_write_without_command_guard():
    asyncio.run(_capture_rejects_direct_core_write())


async def _capture_rejects_direct_core_write():
    import threading

    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    scheduler = Scheduler(bunch)
    service = scheduler.query_service
    entered = threading.Event()
    release = threading.Event()
    capture = service._capture

    def delayed(*args):
        entered.set()
        release.wait(timeout=5)
        return capture(*args)

    service._capture = delayed
    pending = asyncio.create_task(service.page(page_request()))
    assert await asyncio.to_thread(entered.wait, 5)
    flow.add_task("new")
    assert scheduler.query_revision == 0
    release.set()
    result = await pending
    assert result.kind == "error" and result.code == "resource_exhausted"
    assert not service._snapshots


def test_snapshot_publication_rejects_expired_change_window():
    asyncio.run(_snapshot_publication_rejects_expired_change_window())


async def _snapshot_publication_rejects_expired_change_window():
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task("t")
    task.add_meter("m", 0, MAX_CHANGE_BATCHES + 1)
    scheduler = Scheduler(bunch)
    service = scheduler.query_service
    purge = service._purge
    calls = 0

    def expire_at_publication():
        nonlocal calls
        calls += 1
        purge()
        if calls == 2:
            for value in range(1, MAX_CHANGE_BATCHES + 2):
                task.set_meter("m", value)

    service._purge = expire_at_publication
    result = await service.page(page_request())
    assert result.kind == "error" and result.code == "resource_exhausted"
    assert not service._snapshots
    assert scheduler.query_changes.oldest_revision > 1


def test_http_and_grpc_query_routes():
    asyncio.run(_http_and_grpc_query_routes())


async def _http_and_grpc_query_routes():
    import socket

    import grpc
    import httpx

    from takler.protocol.query_v1 import decode_query_document
    from takler.server.grpc_transport import GrpcTransport
    from takler.server.handlers import CommandHandlers
    from takler.server.http_transport import create_app
    from takler.server.protocol import takler_pb2, takler_pb2_grpc
    from takler.server.protocol.query_adapter import query_from_pb2, query_to_pb2

    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    flow.add_task("t").add_parameter("PAYLOAD", "x" * 300_000)
    scheduler = Scheduler(bunch)
    since_request = SinceRequest(
        kind="since_request",
        schema_version=1,
        scope_path="/f",
        field_groups=["summary"],
        session_id="session-1",
        query_id="query-1",
        since_revision=1,
    )
    app = create_app(CommandHandlers(scheduler))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://query"
    ) as client:
        response = await client.post(
            "/v1/query/pages",
            content=encode_query_document(page_request()),
            headers={"content-type": "application/json"},
        )
        assert response.status_code == 200
        http_page = decode_query_document(response.content, expected="page")
        assert [node.path for node in http_page.nodes] == ["/", "/f", "/f/t"]
        assert http_page.base_revision == scheduler.query_changes.revision
        capabilities = await client.post(
            "/v1/query/capabilities",
            json={"kind": "capabilities_request", "schema_version": 1},
        )
        assert capabilities.json()["since"] is False
        since_response = await client.post(
            "/v1/query/since",
            content=encode_query_document(since_request),
            headers={"content-type": "application/json"},
        )
        assert since_response.status_code == 200
        assert decode_query_document(since_response.content, expected="error").code == (
            "unsupported_capability"
        )
        malformed_since = await client.post(
            "/v1/query/since",
            json={"kind": "since_request", "schema_version": 1},
        )
        assert (
            decode_query_document(malformed_since.content, expected="error").code
            == "invalid_request"
        )
        detail_request = DetailRequest(
            kind="detail_request",
            schema_version=1,
            session_id=http_page.session_id,
            query_id=http_page.query_id,
            path="/f/t",
            flow_generation=flow.definition_generation,
            attempt_id=None,
            field_groups=["parameters"],
        )
        detail_response = await client.post(
            "/v1/query/details",
            content=encode_query_document(detail_request),
            headers={"content-type": "application/json"},
        )
        http_detail = decode_query_document(detail_response.content, expected="detail")
        assert http_detail.blob_refs
        chunk_request = ChunkRequest(
            kind="chunk_request",
            schema_version=1,
            blob_id=http_detail.blob_refs[0].blob_id,
            offset=0,
        )
        chunk_response = await client.post(
            "/v1/query/chunks",
            content=encode_query_document(chunk_request),
            headers={"content-type": "application/json"},
        )
        assert (
            decode_query_document(chunk_response.content, expected="chunk").offset == 0
        )
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    transport = GrpcTransport(scheduler, host="127.0.0.1", port=port)
    await transport.start()
    try:
        async with grpc.aio.insecure_channel(
            f"127.0.0.1:{port}", options=(("grpc.enable_http_proxy", 0),)
        ) as channel:
            stub = takler_pb2_grpc.TaklerQueryStub(channel)
            response = await stub.ReadPage(query_to_pb2(page_request()))
            grpc_page = query_from_pb2(response, expected="page")
            assert [node.path for node in grpc_page.nodes] == ["/", "/f", "/f/t"]
            assert grpc_page.session_id == http_page.session_id
            assert grpc_page.base_revision == http_page.base_revision
            assert (
                query_from_pb2(
                    await stub.GetCapabilities(
                        query_to_pb2(
                            CapabilitiesRequest(
                                kind="capabilities_request",
                                schema_version=1,
                            )
                        )
                    ),
                    expected="capabilities",
                ).since
                is False
            )
            assert (
                query_from_pb2(
                    await stub.ReadSince(query_to_pb2(since_request)),
                    expected="error",
                ).code
                == "unsupported_capability"
            )
            assert (
                query_from_pb2(
                    await stub.ReadSince(
                        takler_pb2.QueryDocumentPayload(
                            json=b'{"kind":"since_request","schema_version":1}'
                        )
                    ),
                    expected="error",
                ).code
                == "invalid_request"
            )
            grpc_detail = query_from_pb2(
                await stub.ReadDetail(query_to_pb2(detail_request)),
                expected="detail",
            )
            assert grpc_detail.blob_refs
            grpc_chunk = query_from_pb2(
                await stub.ReadDetailChunk(
                    query_to_pb2(
                        ChunkRequest(
                            kind="chunk_request",
                            schema_version=1,
                            blob_id=grpc_detail.blob_refs[0].blob_id,
                            offset=0,
                        )
                    )
                ),
                expected="chunk",
            )
            assert grpc_chunk.offset == 0
    finally:
        await transport.stop()


def test_http_query_requires_operator_credentials(tmp_path):
    asyncio.run(_http_query_requires_operator_credentials(tmp_path))


async def _http_query_requires_operator_credentials(tmp_path):
    import socket

    import grpc
    import httpx

    from takler.server.auth import AuthGate, AuthInterceptor, CredentialStore
    from takler.server.connect_config import AuthMode
    from takler.server.grpc_transport import GrpcTransport
    from takler.server.handlers import CommandHandlers
    from takler.server.http_transport import create_app
    from takler.server.protocol import takler_pb2_grpc
    from takler.server.protocol.query_adapter import query_from_pb2, query_to_pb2

    secret_file = tmp_path / "secret"
    secret_file.write_text("operator-secret\n")
    whitelist_file = tmp_path / "whitelist"
    whitelist_file.write_text("alice\n")
    store = CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)
    gate = AuthGate(auth_mode=AuthMode.ENABLED, credential_store=store)
    scheduler = Scheduler(Bunch("root"))
    app = create_app(CommandHandlers(scheduler), gate=gate)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://query"
    ) as client:
        body = encode_query_document(page_request())
        missing = await client.post(
            "/v1/query/pages",
            content=body,
            headers={"content-type": "application/json"},
        )
        assert missing.status_code == 401
        allowed = await client.post(
            "/v1/query/pages",
            content=body,
            headers={
                "content-type": "application/json",
                "takler-user": "alice",
                "takler-secret": "operator-secret",
            },
        )
        assert allowed.status_code == 200 and allowed.json()["kind"] == "page"
        denied = await client.post(
            "/v1/query/pages",
            content=body,
            headers={
                "content-type": "application/json",
                "takler-user": "alice",
                "takler-secret": "wrong",
            },
        )
        assert denied.status_code == 403
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    transport = GrpcTransport(
        scheduler,
        host="127.0.0.1",
        port=port,
        interceptors=(AuthInterceptor(gate=gate),),
    )
    await transport.start()
    try:
        async with grpc.aio.insecure_channel(
            f"127.0.0.1:{port}", options=(("grpc.enable_http_proxy", 0),)
        ) as channel:
            stub = takler_pb2_grpc.TaklerQueryStub(channel)
            try:
                await stub.ReadPage(query_to_pb2(page_request()))
            except grpc.aio.AioRpcError as exc:
                assert exc.code() is grpc.StatusCode.UNAUTHENTICATED
            else:
                raise AssertionError("query accepted missing credentials")
            reply = await stub.ReadPage(
                query_to_pb2(page_request()),
                metadata=(
                    ("takler-user", "alice"),
                    ("takler-secret", "operator-secret"),
                ),
            )
            assert query_from_pb2(reply, expected="page").kind == "page"
    finally:
        await transport.stop()


def test_snapshot_and_blob_budgets_and_expiry():
    asyncio.run(_snapshot_and_blob_budgets_and_expiry())


async def _snapshot_and_blob_budgets_and_expiry():
    from dataclasses import replace
    import time

    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task("t")
    task.add_parameter("PAYLOAD", "x" * (12 * 1024 * 1024))
    service = Scheduler(bunch).query_service
    first = await service.page(page_request())
    second = await service.page(page_request())
    assert first.kind == second.kind == "page"
    third = await service.page(page_request())
    assert third.kind == "page"
    assert first.snapshot_id not in service._snapshots
    assert second.snapshot_id in service._snapshots
    request = DetailRequest(
        kind="detail_request",
        schema_version=1,
        session_id=first.session_id,
        query_id=first.query_id,
        path="/f/t",
        flow_generation=flow.definition_generation,
        attempt_id=None,
        field_groups=["parameters"],
    )
    one = await service.detail(request)
    two = await service.detail(request)
    assert one.kind == two.kind == "detail"
    assert (await service.detail(request)).code == "resource_exhausted"
    task.add_parameter("PAYLOAD", "y" * (17 * 1024 * 1024))
    assert (await service.detail(request)).code == "field_too_large"
    blob_id = one.blob_refs[0].blob_id
    service._blobs[blob_id] = replace(
        service._blobs[blob_id], expires=time.monotonic() - 1
    )
    assert (
        await service.chunk(
            ChunkRequest(
                kind="chunk_request", schema_version=1, blob_id=blob_id, offset=0
            )
        )
    ).code == "snapshot_expired"
    for snapshot_id, snapshot in list(service._snapshots.items()):
        service._snapshots[snapshot_id] = replace(
            snapshot, expires=time.monotonic() - 1
        )
    assert (await service.detail(request)).code == "snapshot_expired"


def test_root_service_detail_uses_scheduler_status():
    asyncio.run(_root_service_detail_uses_scheduler_status())


async def _root_service_detail_uses_scheduler_status():
    scheduler = Scheduler(Bunch("root"))
    scheduler.last_checkpoint_at = "2026-10-07T12:00:00Z"
    scheduler.restore_summary = {"restored": True}
    service = scheduler.query_service
    page = await service.page(page_request())
    detail = await service.detail(
        DetailRequest(
            kind="detail_request",
            schema_version=1,
            session_id=page.session_id,
            query_id=page.query_id,
            path="/",
            flow_generation=None,
            attempt_id=None,
            field_groups=["service"],
        )
    )
    assert detail.kind == "detail"
    assert detail.groups.service.last_checkpoint_at == scheduler.last_checkpoint_at
    assert detail.groups.service.restore_summary == scheduler.restore_summary


def test_root_depth_zero_rejects_child_detail():
    asyncio.run(_root_depth_zero_rejects_child_detail())


async def _root_depth_zero_rejects_child_detail():
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    service = Scheduler(bunch).query_service
    page = await service.page(page_request(depth=0))
    assert [node.path for node in page.nodes] == ["/"]
    detail = await service.detail(
        DetailRequest(
            kind="detail_request",
            schema_version=1,
            session_id=page.session_id,
            query_id=page.query_id,
            path="/f",
            flow_generation=flow.definition_generation,
            attempt_id=None,
            field_groups=["parameters"],
        )
    )
    assert detail.code == "permission_denied"
