"""Small transport-boundary checks, without performance measurements."""

import builtins
import json
from pathlib import Path
import runpy
from types import SimpleNamespace

import grpc
import pytest

from takler.client import _summary_receiver as native
from takler.client.grpc_transport import GrpcTransport
from takler.client.query_client import QueryClient
from takler.exceptions import (
    InvalidRequestError,
    PermissionDeniedError,
    ServerResponseError,
    TransportError,
)
from takler.protocol.query_v1 import (
    CapabilitiesRequest,
    PageRequest,
    decode_query_document,
)
from takler.server.protocol.takler_pb2 import QueryDocumentPayload


RAW = json.dumps(
    dict(
        kind="page",
        schema_version=1,
        session_id="s",
        query_id="q",
        snapshot_id="snap",
        as_of="2026-10-08T00:00:00Z",
        expires_at="2026-10-08T00:02:00Z",
        page_index=0,
        complete=True,
        next_cursor=None,
        root_name="",
        flow_generations={},
        total_nodes=1,
        nodes=[
            dict(
                path="/",
                node_kind="bunch",
                type_id="bunch",
                status="queued",
                suspended=False,
            )
        ],
    )
).encode()


class RpcFailure(grpc.RpcError):
    def __init__(self, status):
        self.status = status

    def code(self):
        return self.status

    def details(self):
        return "test error"


@pytest.fixture(params=["grpc", "http"])
def wire(request, fake_clock, monkeypatch):
    calls, answers = [], [RAW]
    credentials = [("x-takler-user", "reader")]
    if request.param == "grpc":
        transport = GrpcTransport(
            host="localhost",
            port=33083,
            retry_window=2,
            clock=fake_clock,
            sleep=fake_clock.sleep,
        )
        monkeypatch.setattr(transport, "_build_metadata", lambda kind: credentials)

        def rpc(payload, timeout, metadata):
            calls.append((payload.json, timeout, metadata))
            answer = answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return QueryDocumentPayload(json=answer)

        transport.query_stub = SimpleNamespace(ReadPage=rpc)
    else:
        httpx = pytest.importorskip("httpx")
        from takler.client import http_transport

        transport = http_transport.HttpTransport(
            host="localhost",
            port=33084,
            retry_window=2,
            clock=fake_clock,
            sleep=fake_clock.sleep,
        )
        monkeypatch.setattr(
            http_transport, "build_credential_pairs", lambda *a: credentials
        )

        def handle(req):
            assert req.url.path == "/v1/query/pages"
            calls.append(
                (
                    req.content,
                    req.extensions["timeout"]["read"],
                    [("x-takler-user", req.headers["x-takler-user"])],
                )
            )
            answer = answers.pop(0)
            if isinstance(answer, httpx.Response):
                return answer
            return httpx.Response(
                200, content=answer, headers={"content-type": "application/json"}
            )

        transport.client = httpx.Client(
            transport=httpx.MockTransport(handle), base_url="http://localhost"
        )
    yield SimpleNamespace(
        transport=transport, calls=calls, answers=answers, kind=request.param
    )
    transport.close()


def request():
    return PageRequest(kind="page_request", schema_version=1, field_groups=["summary"])


def test_callback_preserves_request_credentials_timeout_and_result(wire):
    seen = []

    def decode(raw):
        seen.append(raw)
        return decode_query_document(raw)

    expected = decode_query_document(RAW)
    assert wire.transport.query_summary(request(), decode) == expected
    wire.answers.append(RAW)
    assert wire.transport.query(request()) == expected
    assert wire.calls[0] == wire.calls[1]
    assert seen == [RAW]


def test_retry_calls_decoder_only_after_success(wire):
    if wire.kind == "grpc":
        failure = RpcFailure(grpc.StatusCode.UNAVAILABLE)
    else:
        import httpx

        failure = httpx.Response(503)
    wire.answers.insert(0, failure)
    seen = []

    def decode(raw):
        seen.append(raw)
        return decode_query_document(raw)

    wire.transport.query_summary(request(), decode)
    assert seen == [RAW] and len(wire.calls) == 2
    assert wire.calls[0] == wire.calls[1]


def test_permission_failure_never_enters_decoder(wire):
    if wire.kind == "grpc":
        failure = RpcFailure(grpc.StatusCode.PERMISSION_DENIED)
    else:
        import httpx

        failure = httpx.Response(403)
    wire.answers[:] = [failure]
    with pytest.raises(PermissionDeniedError):
        wire.transport.query_summary(
            request(), lambda _: pytest.fail("decoded denied response")
        )
    assert len(wire.calls) == 1


@pytest.mark.parametrize("raw", [b'{"kind":"page","kind":"page"}', b"{}", b"\xff"])
def test_malformed_response_mapping_and_atomicity(wire, raw):
    client = QueryClient(wire.transport)
    previous = client.read_snapshot()
    wire.answers[:] = [raw]
    with pytest.raises(ServerResponseError, match="invalid query response"):
        client.read_snapshot()
    assert client.current is previous
    assert len(wire.calls) == 2  # Decode failures do not retry.


def test_content_type_checked_before_decode(wire):
    if wire.kind != "http":
        pytest.skip("HTTP-only content type")
    import httpx

    wire.answers[:] = [
        httpx.Response(200, content=RAW, headers={"content-type": "text/plain"})
    ]
    with pytest.raises(ServerResponseError, match="content type"):
        wire.transport.query_summary(
            request(), lambda _: pytest.fail("decoded non-JSON")
        )


def test_summary_rejects_other_request_kinds(wire):
    with pytest.raises(InvalidRequestError):
        wire.transport.query_summary(
            CapabilitiesRequest(kind="capabilities_request", schema_version=1),
            lambda _: pytest.fail("unexpected decode"),
        )
    assert not wire.calls


def test_replaced_query_is_not_bypassed(wire, monkeypatch):
    seen = []

    def custom(document):
        seen.append(document)
        return decode_query_document(RAW)

    monkeypatch.setattr(wire.transport, "query", custom)
    assert QueryClient(wire.transport).read_snapshot().ordered_paths == ("/",)
    assert len(seen) == 1 and not wire.calls


def test_subclass_query_is_not_bypassed(wire):
    base = type(wire.transport)
    seen = []

    class Custom(base):
        def query(self, document):
            seen.append(document)
            return decode_query_document(RAW)

    transport = Custom(host="localhost", port=1234)
    assert QueryClient(transport).read_snapshot().ordered_paths == ("/",)
    assert len(seen) == 1


@pytest.mark.parametrize("module", [None, SimpleNamespace(API_VERSION=999)])
def test_unavailable_backend_uses_original_path(wire, monkeypatch, module):
    monkeypatch.setattr(native, "_native", module)
    monkeypatch.setattr(
        wire.transport, "query_summary", lambda *a: pytest.fail("native callback used")
    )
    assert QueryClient(wire.transport).read_snapshot().ordered_paths == ("/",)


@pytest.mark.parametrize("exception", [ImportError, OSError])
def test_unloadable_optional_module(exception, monkeypatch):
    original = builtins.__import__

    def imported(name, *args, **kwargs):
        if name == "takler_summary_kernel":
            raise exception("unavailable extension")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", imported)
    module = runpy.run_path(str(Path(native.__file__)))
    assert module["_native"] is None


def test_absent_native_does_not_probe_optional_transport_attributes(monkeypatch):
    monkeypatch.setattr(native, "_native", None)

    class Custom:
        @property
        def query_summary(self):
            raise RuntimeError("not an opted-in callback")

        def query(self, document):
            return decode_query_document(RAW)

    assert QueryClient(Custom()).read_snapshot().ordered_paths == ("/",)


def test_closed_transport(wire):
    wire.transport.close()
    with pytest.raises(TransportError, match="not open"):
        wire.transport.query_summary(request(), decode_query_document)
