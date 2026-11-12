"""Initial query client must publish only complete, validated views."""

import base64
import hashlib
import io

import pytest

import takler.client.query_client as query_module
from takler.client.query_client import QueryClient, print_query_show
from takler.client.service_client import TaklerServiceClient
from takler.exceptions import QueryResponseError, ServerResponseError
from takler.protocol.query_v1 import (
    ChunkReply,
    DetailBlobRef,
    DetailGroups,
    DetailReply,
    FailureReply,
    PageReply,
    SummaryNode,
)

NOW = "2026-10-08T00:00:00Z"
LATER = "2026-10-08T00:02:00Z"


class ReplayTransport:
    def __init__(self, *replies):
        self.replies = list(replies)
        self.requests = []

    def query(self, request):
        self.requests.append(request)
        return self.replies.pop(0)


def node(path, kind="task", attempt_id=None):
    if kind == "root":
        kind = "bunch"
    return SummaryNode(
        path=path,
        node_kind=kind,
        type_id=kind,
        status="queued",
        suspended=False,
        attempt_id=attempt_id,
    )


def page(paths, *, index=0, complete=True, snapshot="snap", total=3):
    return PageReply(
        kind="page",
        schema_version=1,
        session_id="session",
        query_id="query",
        snapshot_id=snapshot,
        as_of=NOW,
        expires_at=LATER,
        page_index=index,
        total_nodes=total,
        complete=complete,
        next_cursor=None if complete else "cursor",
        root_name="root",
        flow_generations={"/flow": "generation", "/other": "generation2"},
        nodes=paths,
    )


def test_publishes_only_after_all_pages_and_preserves_old_view_on_failure():
    old = page([node("/", "root"), node("/flow", "flow")], total=2)
    first = page([node("/", "root"), node("/flow", "flow")], complete=False)
    second = page([node("/flow/task")], index=1)
    transport = ReplayTransport(old, first, second)
    client = QueryClient(transport)
    previous = client.read_snapshot()
    result = client.read_snapshot()
    assert result.ordered_paths == ("/", "/flow", "/flow/task")
    assert client.current is result
    assert previous.ordered_paths == ("/", "/flow")
    assert transport.requests[-1].cursor == "cursor"

    transport.replies.extend(
        [first, page([node("/flow/task")], index=1, snapshot="other")]
    )
    with pytest.raises(ServerResponseError, match="identity"):
        client.read_snapshot()
    assert client.current is result


def test_cancelled_read_stops_before_next_page_and_keeps_old_view():
    old = page([node("/", "root"), node("/flow", "flow")], total=2)
    first = page([node("/", "root"), node("/flow", "flow")], complete=False)
    transport = ReplayTransport(old, first)
    client = QueryClient(transport)
    previous = client.read_snapshot()
    with pytest.raises(QueryResponseError, match="cancelled"):
        client.read_snapshot(cancelled=lambda: len(transport.requests) >= 2)
    assert client.current is previous
    assert len(transport.requests) == 2


@pytest.mark.parametrize(
    "last,match",
    [
        (page([node("/flow")], index=1), "duplicate"),
        (page([node("/flow/missing/task")], index=1), "parent"),
        (page([node("/flow/task")], index=2), "order"),
        (page([node("/flow/task")], index=1, total=4), "count"),
    ],
)
def test_rejects_invalid_continuation(last, match):
    client = QueryClient(
        ReplayTransport(
            page([node("/", "root"), node("/flow", "flow")], complete=False),
            last,
        )
    )
    with pytest.raises(ServerResponseError, match=match):
        client.read_snapshot()
    assert client.current is None


def test_query_failure_does_not_publish_partial_view():
    transport = ReplayTransport(
        page([node("/", "root"), node("/flow", "flow")], complete=False),
        FailureReply(
            kind="error",
            schema_version=1,
            code="snapshot_expired",
            message="expired",
            reset_scope="/",
        ),
    )
    client = QueryClient(transport)
    with pytest.raises(QueryResponseError) as exc:
        client.read_snapshot()
    assert exc.value.reset_scope == "/"
    assert client.current is None


def test_expired_detail_marks_current_view_stale_for_rebuild():
    summary = page([node("/", "root"), node("/flow", "flow"), node("/flow/task")])
    expired = FailureReply(
        kind="error", schema_version=1, code="snapshot_expired", message="lease ended"
    )
    client = QueryClient(ReplayTransport(summary, expired, summary))
    old = client.read_snapshot()
    with pytest.raises(QueryResponseError, match="snapshot_expired"):
        client.read_detail(old, "/flow/task", ["runtime"])
    assert client.current.is_stale("/flow/task")
    assert client.read_snapshot().get("/flow/task") is not None
    assert not client.current.is_stale("/flow/task")


def test_client_budget_rejects_replacement_without_losing_old_view(monkeypatch):
    row = page([node("/", "root"), node("/flow", "flow")], total=2)
    client = QueryClient(ReplayTransport(row, row))
    previous = client.read_snapshot()
    monkeypatch.setattr(
        query_module, "MAX_CLIENT_VIEW_BYTES", previous.estimated_bytes * 2 - 1
    )
    with pytest.raises(QueryResponseError, match="memory budget"):
        client.read_snapshot()
    assert client.current is previous


def test_invalid_detail_selection_and_blob_pointer_are_rejected():
    client = QueryClient(
        ReplayTransport(page([node("/", "root"), node("/flow", "flow")], total=2))
    )
    snapshot = client.read_snapshot()
    with pytest.raises(QueryResponseError, match="not in the query view"):
        client.read_detail(snapshot, "/missing", ["parameters"])
    with pytest.raises(QueryResponseError, match="invalid detail groups"):
        client.read_detail(snapshot, "/flow", ["parameters", "parameters"])
    for pointer in (
        "/groups/parameters/user/-1/value",
        "/groups/parameters/user/9/value",
        "/groups/parameters/user/0/missing",
    ):
        with pytest.raises(ServerResponseError, match="blob pointer"):
            client._pointer_parent({"parameters": {"user": [{"value": None}]}}, pointer)


def test_detail_resolves_blob_and_checks_attempt():
    payload = '"' + "x" * 300000 + '"'
    data = payload.encode()
    digest = hashlib.sha256(data).hexdigest()
    summary = page(
        [node("/", "root"), node("/flow", "flow"), node("/flow/task", attempt_id="a1")]
    )
    detail = DetailReply(
        kind="detail",
        schema_version=1,
        session_id="session",
        query_id="query",
        path="/flow/task",
        flow_generation="generation",
        attempt_id="a1",
        sample_mode="live",
        as_of=NOW,
        requested_groups=["parameters"],
        groups=DetailGroups.model_validate(
            {"parameters": {"user": [{"name": "WIDE", "value": None}]}}
        ),
        blob_refs=[
            DetailBlobRef(
                pointer="/groups/parameters/user/0/value",
                blob_id="blob",
                total_bytes=len(data),
                sha256=digest,
            )
        ],
    )
    chunks = [
        ChunkReply(
            kind="chunk",
            schema_version=1,
            blob_id="blob",
            offset=offset,
            total_bytes=len(data),
            sha256=digest,
            data_base64=base64.b64encode(data[offset : offset + 65536]).decode(),
            complete=offset + 65536 >= len(data),
        )
        for offset in range(0, len(data), 65536)
    ]
    transport = ReplayTransport(summary, detail, *chunks)
    client = QueryClient(transport)
    snapshot = client.read_snapshot()
    resolved = client.read_detail(snapshot, "/flow/task", ["parameters"])
    assert resolved.groups.parameters.user[0].value == "x" * 300000
    assert resolved.blob_refs == []
    assert transport.requests[1].attempt_id == "a1"


def test_blob_digest_failure_never_returns_detail():
    summary = page([node("/", "root"), node("/flow", "flow"), node("/flow/task")])
    detail = DetailReply(
        kind="detail",
        schema_version=1,
        session_id="session",
        query_id="query",
        path="/flow/task",
        flow_generation="generation",
        attempt_id=None,
        sample_mode="live",
        as_of=NOW,
        requested_groups=["parameters"],
        groups=DetailGroups.model_validate(
            {"parameters": {"user": [{"name": "X", "value": None}]}}
        ),
        blob_refs=[
            DetailBlobRef(
                pointer="/groups/parameters/user/0/value",
                blob_id="blob",
                total_bytes=3,
                sha256="0" * 64,
            )
        ],
    )
    chunk = ChunkReply(
        kind="chunk",
        schema_version=1,
        blob_id="blob",
        offset=0,
        total_bytes=3,
        sha256="0" * 64,
        data_base64=base64.b64encode(b'"x"').decode(),
        complete=True,
    )
    client = QueryClient(ReplayTransport(summary, detail, chunk))
    snapshot = client.read_snapshot()
    with pytest.raises(ServerResponseError, match="digest"):
        client.read_detail(snapshot, "/flow/task", ["parameters"])


def test_summary_show_requires_no_detail_rpc():
    transport = ReplayTransport(
        page([node("/", "root"), node("/flow", "flow"), node("/flow/task")])
    )
    snapshot = QueryClient(transport).read_snapshot()
    output = io.StringIO()
    print_query_show(snapshot, lambda *_: pytest.fail("unexpected detail"), output)
    assert "task [queued]" in output.getvalue()
    assert len(transport.requests) == 1


def test_unknown_submission_is_visible_in_summary_show():
    task = SummaryNode(
        path="/flow/task",
        node_kind="task",
        type_id="takler.shell_script_task",
        status="submitted",
        suspended=False,
        attempt_id="attempt-1",
        submission_unknown=True,
    )
    transport = ReplayTransport(page([node("/", "root"), node("/flow", "flow"), task]))
    snapshot = QueryClient(transport).read_snapshot()
    output = io.StringIO()
    print_query_show(snapshot, lambda *_: pytest.fail("unexpected detail"), output)
    assert "task [submitted; submission unknown]" in output.getvalue()


def test_detailed_show_does_not_print_partial_output(capsys):
    transport = ReplayTransport(
        page([node("/", "root"), node("/flow", "flow"), node("/flow/task")]),
        FailureReply(
            kind="error",
            schema_version=1,
            code="stale_identity",
            message="changed",
            reset_scope="/flow",
        ),
    )
    client = TaklerServiceClient(transport=transport)
    with pytest.raises(QueryResponseError, match="stale_identity"):
        client.run_query_show(show_parameter=True)
    assert capsys.readouterr().out == ""


def test_detailed_show_renders_requested_groups_and_redaction():
    transport = ReplayTransport(
        page([node("/flow", "flow"), node("/flow/task")], total=2)
    )
    snapshot = QueryClient(transport).read_snapshot(scope_path="/flow")
    calls = []

    def detail_reader(view, path, groups):
        calls.append((view, path, groups))
        return DetailReply(
            kind="detail",
            schema_version=1,
            session_id="session",
            query_id="query",
            path=path,
            flow_generation="generation",
            attempt_id=None,
            sample_mode="live",
            as_of=NOW,
            requested_groups=groups,
            groups=DetailGroups.model_validate(
                {
                    "parameters": {
                        "user": [
                            {"name": "VISIBLE", "value": "hello"},
                            {"name": "SECRET", "redacted": True},
                        ]
                    },
                    "definition": {
                        "trigger": "a == complete",
                        "times": [{"time": "10:00"}],
                        "limits": [{"name": "slots", "limit": 2}],
                        "meters": [
                            {"name": "progress", "min_value": 0, "max_value": 10}
                        ],
                    },
                    "runtime": {
                        "limits": [{"name": "slots", "value": 1}],
                        "events": [{"name": "ready", "value": True}],
                        "meters": [{"name": "progress", "value": 7}],
                    },
                }
            ),
        )

    output = io.StringIO()
    print_query_show(
        snapshot,
        detail_reader,
        output,
        show_parameter=True,
        show_trigger=True,
        show_limit=True,
        show_event=True,
        show_meter=True,
    )
    result = output.getvalue()
    assert "details are sampled live" in result
    assert "param VISIBLE 'hello'" in result
    assert "param SECRET '<redacted>'" in result
    assert "trigger a == complete" in result
    assert "time 10:00" in result
    assert "limit slots [1/2]" in result
    assert "event ready [set]" in result
    assert "meter progress 0 10 [7]" in result
    assert len(calls) == 2
    assert calls[0][2] == ["parameters", "definition", "runtime"]
