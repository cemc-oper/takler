"""A complete delta is the only boundary at which a client watermark moves."""

import pytest
from typer.testing import CliRunner

import takler.client.query_client as query_module
from takler.client import cli
from takler.client.query_client import QueryClient
from takler.client.service_client import TaklerServiceClient
from takler.exceptions import QueryResponseError, ServerResponseError
from takler.protocol.query_v1 import (
    CapabilitiesReply,
    InvalidateScope,
    PageReply,
    RemoveSubtree,
    ResetReply,
    SinceReply,
    SummaryNode,
    UpsertSummary,
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

    def open(self):
        pass

    def close(self):
        pass


def node(path, status="queued", attempt_id=None):
    return SummaryNode(
        path=path,
        node_kind="bunch" if path == "/" else "flow" if path == "/flow" else "task",
        type_id="task",
        status=status,
        suspended=False,
        attempt_id=attempt_id,
    )


def page():
    return PageReply(
        kind="page",
        schema_version=1,
        session_id="session",
        query_id="query",
        snapshot_id="snapshot",
        base_revision=5,
        as_of=NOW,
        expires_at=LATER,
        page_index=0,
        complete=True,
        next_cursor=None,
        root_name="root",
        total_nodes=3,
        flow_generations={"/flow": "generation"},
        nodes=[node("/"), node("/flow"), node("/flow/task")],
    )


def capability(since=True):
    return CapabilitiesReply(
        kind="capabilities",
        schema_version=1,
        snapshot=True,
        detail=True,
        since=since,
        max_page_nodes=2048,
        max_decoded_page_bytes=262144,
        max_chunk_bytes=65536,
    )


def delta(*ops, index=0, complete=True, target=6, batch="batch", cursor="next"):
    return SinceReply(
        kind="since",
        schema_version=1,
        session_id="session",
        query_id="query",
        batch_id=batch,
        from_revision=5,
        target_revision=target,
        as_of=NOW,
        expires_at=LATER,
        page_index=index,
        complete=complete,
        next_cursor=None if complete else cursor,
        unchanged=target == 5,
        operations=list(ops),
    )


def upsert(path="/flow/task", status="active"):
    return UpsertSummary(
        op="upsert_summary",
        path=path,
        flow_generation=None if path == "/" else "generation",
        node=node(path, status),
    )


def test_sparse_update_shares_base_and_moves_watermark_only_after_last_page():
    transport = ReplayTransport(
        page(),
        capability(),
        delta(upsert("/flow", "active"), complete=False),
        delta(upsert(), index=1),
    )
    client = QueryClient(transport)
    old = client.read_snapshot()
    result = client.read_since(old)
    assert old.revision == 5 and old.get("/flow/task").status == "queued"
    assert result.revision == 6 and result.get("/flow/task").status == "active"
    assert result.nodes.base is old.nodes
    assert transport.requests[-1].cursor == "next"
    assert len(result.ordered_paths) == 3


def test_identical_replayed_page_is_ignored_before_continuation():
    first = delta(upsert("/flow", "active"), complete=False)
    client = QueryClient(
        ReplayTransport(page(), capability(), first, first, delta(upsert(), index=1))
    )
    assert (
        client.read_since(client.read_snapshot()).get("/flow/task").status == "active"
    )


@pytest.mark.parametrize(
    "last,match",
    [
        (delta(upsert("/flow"), index=1), "duplicate"),
        (delta(upsert(), index=2), "order"),
        (delta(upsert(), index=1, target=7), "identity"),
        (delta(upsert(), index=1, batch="other"), "identity"),
        (delta(upsert("/flow", "complete"), complete=False), "replay"),
    ],
)
def test_invalid_batch_keeps_old_watermark_and_marks_stale(last, match):
    transport = ReplayTransport(
        page(), capability(), delta(upsert("/flow"), complete=False), last
    )
    client = QueryClient(transport)
    old = client.read_snapshot()
    with pytest.raises(ServerResponseError, match=match):
        client.read_since(old)
    assert old.revision == 5
    assert client.current.revision == 5
    assert client.current.is_stale("/flow/task")


def test_unchanged_keeps_same_view_and_does_not_copy_nodes():
    client = QueryClient(ReplayTransport(page(), capability(), delta(target=5)))
    old = client.read_snapshot()
    assert client.read_since(old) is old


def test_upsert_replaces_attempt_identity_in_full_summary():
    operation = UpsertSummary(
        op="upsert_summary",
        path="/flow/task",
        flow_generation="generation",
        node=node("/flow/task", "active", "new-attempt"),
    )
    client = QueryClient(ReplayTransport(page(), capability(), delta(operation)))
    old = client.read_snapshot()
    current = client.read_since(old)
    assert old.get("/flow/task").attempt_id is None
    assert current.get("/flow/task").attempt_id == "new-attempt"


def test_reset_marks_stale_and_reports_scope():
    reset = ResetReply(
        kind="reset",
        schema_version=1,
        session_id="session",
        query_id="query",
        since_revision=5,
        reset_scope="/",
        reason="history_expired",
    )
    client = QueryClient(ReplayTransport(page(), capability(), reset))
    old = client.read_snapshot()
    with pytest.raises(QueryResponseError) as exc:
        client.read_since(old)
    assert exc.value.code == "reset_required"
    assert exc.value.reset_scope == "/"
    assert client.current.revision == 5 and client.current.is_stale()


def test_local_invalidation_marks_only_subtree_and_retains_other_nodes():
    operation = InvalidateScope(
        op="invalidate_scope", path="/flow/task", reason="structure_changed"
    )
    client = QueryClient(
        ReplayTransport(page(), capability(), delta(operation, upsert("/flow")))
    )
    old = client.read_snapshot()
    result = client.read_since(old)
    assert result.revision == 6
    assert result.stale_scopes == ("/flow/task",)
    assert not result.is_stale("/flow")
    with pytest.raises(QueryResponseError, match="stale_identity"):
        client.read_detail(result, "/flow/task", ["parameters"])


def test_remove_subtree_drops_node_and_rejects_old_generation():
    remove = RemoveSubtree(
        op="remove_subtree", path="/flow/task", flow_generation="generation"
    )
    client = QueryClient(ReplayTransport(page(), capability(), delta(remove)))
    result = client.read_since(client.read_snapshot())
    assert result.revision == 6
    assert result.get("/flow/task") is None
    assert result.ordered_paths == ("/", "/flow")


def test_capability_downgrade_marks_snapshot_stale():
    client = QueryClient(ReplayTransport(page(), capability(False)))
    old = client.read_snapshot()
    with pytest.raises(QueryResponseError, match="unsupported_capability"):
        client.read_since(old)
    assert client.current.revision == old.revision
    assert client.current.is_stale()


def test_budget_rejects_delta_without_publishing(monkeypatch):
    client = QueryClient(ReplayTransport(page(), capability(), delta(upsert())))
    old = client.read_snapshot()
    monkeypatch.setattr(query_module, "MAX_CLIENT_VIEW_BYTES", old.estimated_bytes + 1)
    with pytest.raises(QueryResponseError, match="memory budget"):
        client.read_since(old)
    assert client.current.revision == 5
    assert client.current.is_stale()


def test_high_level_sync_rebuilds_after_local_invalidation():
    invalidation = InvalidateScope(
        op="invalidate_scope", path="/flow", reason="structure_changed"
    )
    transport = ReplayTransport(page(), capability(), delta(invalidation), page())
    client = TaklerServiceClient(transport=transport)
    result = client.query_sync(polls=1)
    assert result.revision == 5
    assert result.stale_scopes == ()
    assert [request.kind for request in transport.requests] == [
        "page_request",
        "capabilities_request",
        "since_request",
        "page_request",
    ]


def test_sync_cli_exposes_explicit_polling(monkeypatch):
    view = QueryClient(ReplayTransport(page())).read_snapshot()
    options = []

    class FakeClient:
        def query_sync(self, **kwargs):
            options.append(kwargs)
            return view

    monkeypatch.setattr(
        cli, "_run_client_command", lambda _host, _port, command: command(FakeClient())
    )
    result = CliRunner().invoke(cli.app, ["sync", "--scope", "/flow", "--polls", "2"])
    assert result.exit_code == 0
    assert "# revision=5" in result.output
    assert "task [queued]" in result.output
    assert options == [
        {
            "scope_path": "/flow",
            "flow_name": None,
            "depth": None,
            "polls": 2,
            "interval": 1.0,
        }
    ]
