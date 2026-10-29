"""Small correctness cases only. No archived 100k replay or performance assertions."""

import importlib.util
import json
from pathlib import Path
import threading

import pytest
from hypothesis import given, settings, strategies as st

import takler.client.query_client as qc
from takler.exceptions import QueryResponseError, ServerResponseError, TransportError
from takler.protocol.query_v1 import decode_query_document, encode_query_document
from takler.protocol.wire import WireError

pytest.importorskip("takler_summary_kernel")


class PureClient(qc.QueryClient):
    def __init__(self, transport):
        from types import SimpleNamespace

        super().__init__(SimpleNamespace(query=transport.query))


RustQueryClient = qc.QueryClient

ROOT = Path(__file__).resolve().parents[2]


def node(path, **changes):
    v = dict(
        path=path,
        node_kind="task",
        type_id="extension.任意",
        status="queued",
        suspended=False,
    )
    v.update(changes)
    return v


def page(nodes=None, **changes):
    v = dict(
        kind="page",
        schema_version=1,
        session_id="s",
        query_id="q",
        snapshot_id="snap",
        base_revision=5,
        as_of="2026-10-08T00:00:00Z",
        expires_at="2026-10-08T00:02:00Z",
        page_index=0,
        complete=True,
        next_cursor=None,
        root_name="根",
        flow_generations={"/流": "世代"},
        nodes=nodes
        if nodes is not None
        else [
            node("/", node_kind="bunch"),
            node("/流", node_kind="flow"),
            node("/流/任务"),
        ],
    )
    v.update(changes)
    return v


def raw(v):
    return (
        v
        if isinstance(v, (bytes, Exception))
        else json.dumps(v, ensure_ascii=True, separators=(",", ":")).encode()
    )


class RawTransport:
    def __init__(self, *replies):
        self.replies = [raw(r) for r in replies]
        self.requests = []

    def query_summary(self, request, decode):
        self.requests.append(request)
        if not self.replies:
            raise TransportError("missing page")
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        result = decode(reply)
        receiver = getattr(decode, "__self__", None)
        if receiver is not None and hasattr(receiver, "kernel"):
            self.kernel_counts = receiver.kernel.counts()
        return result

    def query(self, request):
        return self.query_summary(request, decode_query_document)


def outcome(cls, pages, **selection):
    client = cls(RawTransport(*pages))
    try:
        return ("ok", client.read_snapshot(**selection))
    except Exception as e:
        assert client.current is None
        return (type(e), getattr(e, "code", None), getattr(e, "reset_scope", None))


def equal(pages, **selection):
    a, b = (outcome(cls, pages, **selection) for cls in (PureClient, RustQueryClient))
    assert a == b
    return a


@pytest.mark.parametrize("key", list(page()))
def test_missing_header_field(key):
    p = page()
    del p[key]
    equal([p])


@pytest.mark.parametrize("key", list(page()))
@pytest.mark.parametrize("value", [None, True, False, 0, -1, 1.0, "", [], {}, "𐀀é"])
def test_strict_header_types(key, value):
    p = page()
    p[key] = value
    equal([p])


@pytest.mark.parametrize(
    "key",
    [
        "path",
        "node_kind",
        "type_id",
        "status",
        "suspended",
        "attempt_id",
        "submission_unknown",
        "extra",
    ],
)
@pytest.mark.parametrize(
    "value", [None, True, False, 0, 1, 1.0, "", "valid", [], {}, "\ud800"]
)
def test_strict_node_types(key, value):
    p = page()
    p["nodes"][2][key] = value
    equal([p])


@pytest.mark.parametrize(
    "key",
    [
        "path",
        "node_kind",
        "type_id",
        "status",
        "suspended",
        "attempt_id",
        "submission_unknown",
    ],
)
def test_node_defaults(key):
    p = page()
    p["nodes"][2].pop(key, None)
    equal([p])


@pytest.mark.parametrize(
    "path",
    [
        "",
        "relative",
        "//",
        "/流/",
        "/流//任务",
        "/流/.",
        "/流/..",
        "/流/\x00",
        "/流/\n",
        "/流/\x1f",
        "/流/\x7f",
        "/流/任务𐀀",
        "/流/é",
        "/流/é",
        "/流/缺少/任务",
        "/其他",
    ],
)
def test_paths(path):
    p = page()
    p["nodes"][2]["path"] = path
    equal([p])


@pytest.mark.parametrize(
    "data",
    [
        b'{"kind":"page","kind":"page"}',
        b'{"kind":"page","k\\u0069nd":"page"}',
        b'{"nested":{"a":1,"\\u0061":2}}',
        b'{"kind":"page","bad":NaN}',
        b'{"bad":Infinity}',
        b'{"bad":"\\ud800"}',
        b'{"bad":"\xff"}',
        b"[]",
        b"null",
        b"{",
        b"{} {}",
        b"\xef\xbb\xbf{}",
        b" " * 262145,
    ],
)
def test_strict_json(data):
    assert equal([data])[0] is WireError


def test_duplicate_keys_in_valid_documents():
    data = raw(page())
    for changed in [
        data.replace(b'"path":"/"', b'"path":"/","p\\u0061th":"/"'),
        data.replace(b'"/\\u6d41":"\\u4e16\\u4ee3"', b'"/\\u6d41":"x","/\\u6d41":"y"'),
    ]:
        assert changed != data
        assert equal([changed])[0] is WireError


def split():
    p = page()
    return [
        page(p["nodes"][:2], complete=False, next_cursor="cursor", total_nodes=3),
        page(p["nodes"][2:], page_index=1, total_nodes=3),
    ]


@pytest.mark.parametrize(
    "field",
    [
        "session_id",
        "query_id",
        "snapshot_id",
        "base_revision",
        "as_of",
        "expires_at",
        "root_name",
        "page_index",
        "total_nodes",
        "flow_generations",
    ],
)
def test_cross_page_identity(field):
    pages = split()
    pages[1][field] = {
        "base_revision": 6,
        "page_index": 2,
        "total_nodes": 4,
        "as_of": "2026-10-08T00:01:00Z",
        "expires_at": "2026-10-08T00:03:00Z",
        "flow_generations": {"/流": "changed"},
    }.get(field, "changed")
    assert equal(pages)[0] is ServerResponseError


def test_page_sequence_and_scope():
    pages = split()
    equal(pages)
    equal(pages[::-1])
    equal(pages[:1])
    equal([pages[0], pages[0]])
    equal([page(nodes=[])])
    equal([page(total_nodes=4)])
    equal([page()], depth=0)
    equal([page()], scope_path="/流")
    equal([page(nodes=page()["nodes"][1:])], flow_name="流")
    equal([page(nodes=[node("/流")])], scope_path="/流", depth=0)
    equal([page(nodes=[node("/流/任务")])], scope_path="/流")


def test_node_page_limits_and_generations():
    equal([page(nodes=[node("/")] * 2)])
    equal([page(nodes=[node("/")] + [node(f"/流/t{i}") for i in range(2048)])])
    for generations in ({}, {"/": "g"}, {"/流/x": "g"}, {"/流": ""}, {"/流": 1}):
        equal([page(flow_generations=generations)])
    for kind in ("task", "flow"):
        for attempt in (None, "", "attempt"):
            for unknown in (None, False, True):
                p = page()
                p["nodes"][2].update(
                    node_kind=kind,
                    status="submitted",
                    attempt_id=attempt,
                    submission_unknown=unknown,
                )
                equal([p])


def test_header_numeric_and_unicode_compatibility_fallback():
    for v in (True, 1.0, 2):
        equal([page(schema_version=v)])
    for field in ("total_nodes", "page_index", "base_revision"):
        equal([page(**{field: 2**100})])
    equal([page(expires_at="2026-10-08T00:00:00Z")])  # original permits this
    equal([raw(page()).replace(b'"page_index":0', b'"page_index":-0')])
    for path in ("/流/𐀀", "/流/é"):
        p = page()
        p["nodes"][2]["path"] = path
        equal([json.dumps(p, ensure_ascii=False).encode()])


def test_exact_budget_boundary_and_old_view(monkeypatch):
    size = outcome(PureClient, [page()])[1].estimated_bytes
    for limit in (size - 1, size, 2 * size - 1, 2 * size):
        monkeypatch.setattr(qc, "MAX_CLIENT_VIEW_BYTES", limit)
        equal([page()])
        for cls in (PureClient, RustQueryClient):
            c = cls(RawTransport(page(), page()))
            if limit < size:
                continue
            old = c.read_snapshot()
            if limit < size * 2:
                with pytest.raises(QueryResponseError, match="memory budget"):
                    c.read_snapshot()
                assert c.current is old
            else:
                assert c.read_snapshot() == old


@pytest.mark.parametrize("cls", [PureClient, RustQueryClient])
@pytest.mark.parametrize("cancel_after", [0, 1, 2])
def test_cancellation_and_no_partial_publish(cls, cancel_after):
    c = cls(RawTransport(page(), *split()))
    old = c.read_snapshot()
    count = 0

    def cancelled():
        nonlocal count
        count += 1
        return count > cancel_after

    with pytest.raises(QueryResponseError, match="cancelled"):
        c.read_snapshot(cancelled=cancelled)
    assert c.current is old


@pytest.mark.parametrize("cls", [PureClient, RustQueryClient])
def test_supersession_and_error_cleanup(cls):
    c = cls(RawTransport(page(), page()))
    old = c.read_snapshot()

    def cancelled():
        c.current = None
        return False

    with pytest.raises(QueryResponseError, match="cancelled"):
        c.read_snapshot(cancelled=cancelled)
    assert c.current is None
    assert old.get("/流/任务")
    c = cls(RawTransport(page(), split()[0], TransportError("disconnect"), page()))
    old = c.read_snapshot()
    with pytest.raises(TransportError):
        c.read_snapshot()
    assert c.current is old
    assert c.read_snapshot() == old


def test_three_clients_independent_original_interfaces():
    clients = [RustQueryClient(RawTransport(*split())) for _ in range(3)]
    views = [None] * 3

    def work(i):
        views[i] = clients[i].read_snapshot()

    threads = [threading.Thread(target=work, args=(i,)) for i in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
    assert not any(thread.is_alive() for thread in threads)
    assert views[0] == views[1] == views[2]
    assert len({id(v.nodes) for v in views}) == 3
    assert len({id(v.get("/")) for v in views}) == 3
    assert all(
        type(v) is qc.QuerySnapshot and type(v.get("/")) is qc.QueryNode for v in views
    )
    assert all(c.transport.kernel_counts == (2, 0) for c in clients)
    with pytest.raises(TypeError):
        views[0].nodes["/"] = None


@settings(max_examples=120, deadline=None, derandomize=True)
@given(
    st.text(max_size=20),
    st.text(max_size=20),
    st.sampled_from([None, True, False, 1, "attempt"]),
)
def test_differential_node_fuzz(path, type_id, attempt):
    p = page()
    p["nodes"][2].update(path="/流/" + path, type_id=type_id, attempt_id=attempt)
    equal([p])


def test_shared_wire_vectors():
    vectors = json.loads(
        (ROOT / "tests/protocol/fixtures/query_v1_vectors.json").read_text()
    )["cases"]
    for case in vectors:
        equal([case["raw"].encode()])


# Run the existing small summary/since/detail regression cases against the
# native receiver by adapting only their in-memory transport at the byte boundary.
MODULES = []
for name in ("test_query_client", "test_query_since"):
    spec = importlib.util.spec_from_file_location(
        "original_" + name, ROOT / f"tests/client/{name}.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    MODULES.append(mod)
    for key, value in vars(mod).items():
        if key.startswith("test_") and callable(value):
            globals()["test_existing_" + name + "_" + key[5:]] = value


@pytest.fixture(autouse=True)
def adapt_existing_tests(monkeypatch):
    for mod in MODULES:
        monkeypatch.setattr(mod, "QueryClient", RustQueryClient)

        def query_summary(self, request, decode):
            return decode(encode_query_document(self.query(request)))

        monkeypatch.setattr(
            mod.ReplayTransport, "query_summary", query_summary, raising=False
        )


def test_extension_and_plain_transport_paths(monkeypatch):
    class TypedTransport:
        def query(self, request):
            return decode_query_document(raw(page()))

    assert (
        RustQueryClient(TypedTransport()).read_snapshot()
        == PureClient(TypedTransport()).read_snapshot()
    )
    original = qc.QueryNode.from_wire.__func__
    seen = []

    def custom(cls, value):
        seen.append(value.path)
        return original(cls, value)

    monkeypatch.setattr(qc.QueryNode, "from_wire", classmethod(custom))
    c = RustQueryClient(RawTransport(page()))
    c.read_snapshot()
    assert len(seen) == 3 and not hasattr(c.transport, "kernel_counts")


def test_generation_iteration_order():
    p = page(flow_generations={"/z": "z", "/流": "g", "/a": "a"})
    a, b = [outcome(cls, [p])[1] for cls in (PureClient, RustQueryClient)]
    assert tuple(a.flow_generations) == tuple(b.flow_generations) == ("/z", "/流", "/a")


def test_registered_page_extension_uses_original_path(monkeypatch):
    import takler.protocol.query_v1 as wire

    class ExtendedPage(wire.PageReply):
        extension: str = "default"

    monkeypatch.setitem(wire.MODEL_BY_KIND, "page", ExtendedPage)
    p = page(extension="custom")
    client = RustQueryClient(RawTransport(p))
    assert client.read_snapshot() == PureClient(RawTransport(p)).read_snapshot()
    assert not hasattr(client.transport, "kernel_counts")


@pytest.mark.parametrize(
    "extension", ["path", "kinds", "statuses", "byte_limit", "node_limit"]
)
def test_modified_validation_rules_use_original_decoder(monkeypatch, extension):
    import takler.protocol.query_v1 as wire

    if extension == "path":
        original = wire.valid_path

        def custom(path):
            if path == "/流/任务":
                raise ValueError("extension rejects this node")
            return original(path)

        monkeypatch.setattr(wire, "valid_path", custom)
    elif extension == "kinds":
        monkeypatch.setattr(wire, "KINDS", wire.KINDS - {"task"})
    elif extension == "statuses":
        monkeypatch.setattr(wire, "STATUSES", wire.STATUSES - {"queued"})
    elif extension == "byte_limit":
        monkeypatch.setattr(wire, "MAX_PAGE_BYTES", 1)
    else:
        monkeypatch.setattr(wire, "MAX_PAGE_NODES", 1)
    client = RustQueryClient(RawTransport(page()))
    # The max-node field is bound into Pydantic's schema at class creation;
    # changing only its constant does not alter Python's accepted input.
    if extension == "node_limit":
        assert (
            client.read_snapshot() == PureClient(RawTransport(page())).read_snapshot()
        )
    else:
        with pytest.raises(WireError):
            client.read_snapshot()
        assert client.current is None
    assert not hasattr(client.transport, "kernel_counts")


def test_page_byte_boundary():
    p = raw(page())
    assert equal([p + b" " * (262144 - len(p))])[0] == "ok"
    assert equal([p + b" " * (262145 - len(p))])[0] is WireError


def test_native_maximum_valid_node_count():
    p = page(
        nodes=[node("/", type_id="x"), node("/f", type_id="x")]
        + [node(f"/f/n{i}", type_id="x") for i in range(2046)],
        flow_generations={"/f": "g"},
    )
    client = RustQueryClient(RawTransport(p))
    view = client.read_snapshot()
    assert len(view.nodes) == 2048 and client.transport.kernel_counts == (1, 0)
    assert view == PureClient(RawTransport(p)).read_snapshot()


def test_constructor_extension_failure_keeps_previous(monkeypatch):
    client = RustQueryClient(RawTransport(page(), page()))
    previous = client.read_snapshot()

    def fail(*args, **kwargs):
        raise RuntimeError("injected constructor failure")

    monkeypatch.setattr(qc.QueryNode, "__init__", fail)
    with pytest.raises(RuntimeError, match="injected"):
        client.read_snapshot()
    assert client.current is previous


def test_rejected_partial_views_are_released():
    import gc

    gc.collect()
    before = sum(type(o) is qc.QueryNode for o in gc.get_objects())
    pages = split()
    pages[1]["snapshot_id"] = "wrong"
    for _ in range(30):
        client = RustQueryClient(RawTransport(*pages))
        with pytest.raises(ServerResponseError):
            client.read_snapshot()
        assert client.current is None
    del client
    gc.collect()
    assert sum(type(o) is qc.QueryNode for o in gc.get_objects()) == before


@settings(max_examples=40, deadline=None, derandomize=True)
@given(
    st.integers(0, 25),
    st.integers(1, 7),
    st.sampled_from(["task", "任务𐀀", "a~b", "é"]),
)
def test_valid_cross_page_layouts(count, chunk, stem):
    nodes = [node("/"), node("/流")] + [node(f"/流/{stem}{i}") for i in range(count)]
    pages = [
        page(
            nodes=nodes[i : i + chunk],
            page_index=i // chunk,
            complete=i + chunk >= len(nodes),
            next_cursor=None if i + chunk >= len(nodes) else "cursor",
            total_nodes=len(nodes),
        )
        for i in range(0, len(nodes), chunk)
    ]
    assert equal(pages)[0] == "ok"
