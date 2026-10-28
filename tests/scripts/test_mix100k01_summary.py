"""Summary artifacts through the public client and worker; no product service."""

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import struct
import sys
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import mix100k01 as mix
from takler.client.service_client import TaklerServiceClient
from takler.exceptions import QueryResponseError, ServerResponseError
from takler.protocol.query_v1 import (
    MAX_PAGE_BYTES,
    PageReply,
    SummaryNode,
    decode_query_document,
)
from takler.server.protocol.query_adapter import query_to_pb2
from takler.server.protocol.takler_pb2 import QueryDocumentPayload

WIRE = mix.QueryWire


def pages(snapshot="snapshot", revision=7):
    def node(path, kind):
        return SummaryNode(
            path=path,
            node_kind=kind,
            type_id="takler." + kind,
            status="queued",
            suspended=False,
        )

    common = dict(
        kind="page",
        schema_version=1,
        session_id="session",
        query_id="query",
        snapshot_id=snapshot,
        base_revision=revision,
        as_of="2026-10-18T00:00:00Z",
        expires_at="2026-10-18T01:00:00Z",
        total_nodes=3,
        root_name="树",
        flow_generations={"/flow": "generation"},
    )
    return [
        PageReply(
            **common,
            page_index=0,
            complete=False,
            next_cursor="next",
            nodes=[node("/", "bunch"), node("/flow", "flow")],
        ),
        PageReply(
            **common,
            page_index=1,
            complete=True,
            next_cursor=None,
            nodes=[node("/flow/task", "task")],
        ),
    ]


def wire(directory, log, role="summary", responses=None):
    result = WIRE.__new__(WIRE)
    result.directory, result.role, result.log = directory, role, log
    result.summary_archive, result.summary_serial, result.last_summary_digest = (
        None,
        0,
        None,
    )
    result.requests = []
    scripted = iter(responses) if responses is not None else None

    def read_page(payload, **kwargs):
        request = decode_query_document(payload.json)
        result.requests.append(dict(cursor=request.cursor, timeout=kwargs["timeout"]))
        reply = (
            next(scripted)
            if scripted is not None
            else pages()[int(request.cursor is not None)]
        )
        if isinstance(reply, Exception):
            raise reply
        return reply if isinstance(reply, QueryDocumentPayload) else query_to_pb2(reply)

    transport = SimpleNamespace(
        query_stub=SimpleNamespace(ReadPage=read_page),
        _build_metadata=lambda _: [],
        close=lambda: None,
    )
    result.client = TaklerServiceClient(transport=transport)
    transport.query = result.query
    result.begin(role + "-0", time.monotonic_ns() + 60 * mix.NS)
    return result


def complete(log, result, view):
    log(
        "completed",
        request_id=result.request_id,
        channel=result.role,
        nodes=len(view.ordered_paths),
        version=view.revision,
        snapshot_id=view.snapshot_id,
        summary_digest=result.last_summary_digest,
    )


def test_public_multi_page_snapshot_persists_exact_bytes_identity_and_digest(tmp_path):
    # Whitespace and UTF-8 inside a JSON page remain byte-for-byte reproducible.
    source = [
        json.dumps(p.model_dump(mode="json"), ensure_ascii=False, indent=2).encode()
        for p in pages()
    ]
    log = mix.Ledger(tmp_path / "query-requests.jsonl")
    result = wire(
        tmp_path, log, responses=[QueryDocumentPayload(json=p) for p in source]
    )
    view = result.snapshot("scheduled", 3)
    complete(log, result, view)
    log.close()
    evidence = result.last_summary_digest
    raw = (tmp_path / evidence["file"]).read_bytes()
    assert raw == b"".join(struct.pack("<I", len(p)) + p for p in source)
    assert evidence["sha256"] == hashlib.sha256(raw).hexdigest()
    assert (evidence["nodes"], evidence["version"], evidence["pages"]) == (3, 7, 2)
    assert not (tmp_path / (evidence["file"] + ".partial")).exists()
    assert [r["cursor"] for r in result.requests] == [None, "next"]
    assert mix.verify_summary_archives(tmp_path) == dict(
        verified=True, complete=1, failed_partial=0
    )


@pytest.mark.parametrize(
    "fault", ["content", "truncate", "length", "extra", "missing", "version"]
)
def test_archive_corruption_cannot_verify(tmp_path, fault):
    log = mix.Ledger(tmp_path / "query-requests.jsonl")
    result = wire(tmp_path, log)
    result.snapshot("scheduled", 3)
    log.close()
    evidence = dict(result.last_summary_digest)
    path = tmp_path / evidence["file"]
    raw = path.read_bytes()
    if fault == "content":
        path.write_bytes(raw.replace(b"queued", b"active", 1))
    elif fault == "truncate":
        path.write_bytes(raw[:-1])
    elif fault == "length":
        path.write_bytes(struct.pack("<I", MAX_PAGE_BYTES + 1))
    elif fault == "extra":
        path.write_bytes(raw + b"x")
    elif fault == "missing":
        path.unlink()  # This test owns the new temporary artifact.
    else:
        evidence["version"] += 1
    with pytest.raises((mix.SummaryEvidenceError, ValueError)):
        mix.verify_summary_archive(tmp_path, evidence)


def test_completed_summary_without_digest_is_rejected(tmp_path):
    log = mix.Ledger(tmp_path / "query-requests.jsonl")
    log(
        "completed",
        request_id="summary-0",
        channel="summary",
        nodes=3,
        version=7,
        snapshot_id="snapshot",
    )
    log.close()
    with pytest.raises(mix.SummaryEvidenceError, match="digest missing"):
        mix.verify_summary_archives(tmp_path)


def test_completion_cannot_borrow_another_request_digest(tmp_path):
    log = mix.Ledger(tmp_path / "query-requests.jsonl")
    result = wire(tmp_path, log)
    view = result.snapshot("scheduled", 3)
    result.begin("summary-1", time.monotonic_ns() + mix.NS)
    log(
        "completed",
        request_id="summary-1",
        channel="summary",
        nodes=len(view.ordered_paths),
        version=7,
        snapshot_id="snapshot",
        summary_digest=next(
            r["evidence"]
            for r in mix.rows(tmp_path / "query-requests.jsonl")
            if r["kind"] == "summary_digest"
        ),
    )
    log.close()
    with pytest.raises(mix.SummaryEvidenceError, match="digest missing"):
        mix.verify_summary_archives(tmp_path)


@pytest.mark.parametrize("fault", ["timeout", "mixed_version"])
def test_incomplete_public_read_keeps_partial_without_success_digest(tmp_path, fault):
    first, second = pages()
    response = (
        TimeoutError("controlled RPC timeout")
        if fault == "timeout"
        else second.model_copy(update=dict(base_revision=8))
    )
    log = mix.Ledger(tmp_path / "query-requests.jsonl")
    result = wire(tmp_path, log, responses=[first, response])
    with pytest.raises((TimeoutError, ServerResponseError)):
        result.snapshot("baseline", 3)
    assert result.client.query_client.current is None
    log("failed", request_id=result.request_id, channel=result.role)
    log.close()
    assert not list(tmp_path.glob("*.pages"))
    assert len(list(tmp_path.glob("*.pages.partial"))) == 1
    assert all(
        r["kind"] != "summary_digest"
        for r in mix.rows(tmp_path / "query-requests.jsonl")
    )
    assert mix.verify_summary_archives(tmp_path)["failed_partial"] == 1


def test_unarchived_summary_rejected_before_actual_stub(tmp_path):
    log = mix.Ledger(tmp_path / "query-requests.jsonl")
    result = wire(tmp_path, log)
    with pytest.raises(mix.SummaryEvidenceError, match="bypassed"):
        result.client.run_query_snapshot(scope_path="/")
    log.close()
    assert not result.requests
    assert not list(mix.rows(tmp_path / "query-requests.jsonl"))


def test_archive_io_failure_is_critical_and_not_an_rpc_failure(tmp_path, monkeypatch):
    log = mix.Ledger(tmp_path / "query-requests.jsonl")
    result = wire(tmp_path, log)

    def fail(*_):
        raise OSError("controlled archive write failure")

    monkeypatch.setattr(mix.SummaryArchive, "append", fail)
    with pytest.raises(mix.SummaryEvidenceError):
        result.snapshot("scheduled", 3)
    log.close()
    events = list(mix.rows(tmp_path / "query-requests.jsonl"))
    assert sum(r["kind"] == "rpc_return" for r in events) == 1
    assert not any(
        r["kind"] in {"rpc_error", "summary_digest", "completed"} for r in events
    )
    assert "summary evidence" in mix.read(tmp_path / "integrity-failure.json")["error"]
    assert result.summary_archive is None


def test_short_archive_write_is_rejected(tmp_path):
    with mix.SummaryArchive(
        tmp_path / "owned.pages", time.monotonic_ns() + mix.NS
    ) as archive:
        file = archive.file
        archive.file = SimpleNamespace(
            write=lambda raw: file.write(raw[:-1]), close=file.close
        )
        with pytest.raises(mix.SummaryEvidenceError, match="short"):
            archive.append(query_to_pb2(pages()[0]).json)
    assert not (tmp_path / "owned.pages").exists()


def test_archive_shares_absolute_rpc_deadline(tmp_path, monkeypatch):
    now = [0]
    monkeypatch.setattr(mix.time, "monotonic_ns", lambda: now[0])
    log = mix.Ledger(tmp_path / "query-requests.jsonl")
    result = wire(tmp_path, log)
    result.begin("summary-0", 20)
    read = result.client.transport.query_stub.ReadPage

    def timed(*args, **kwargs):
        response = read(*args, **kwargs)
        now[0] += 10
        return response

    result.client.transport.query_stub.ReadPage = timed
    with pytest.raises(TimeoutError):
        result.snapshot("scheduled", 3)
    log.close()
    assert [r["timeout"] for r in result.requests] == [20 / mix.NS, 10 / mix.NS]
    assert not list(tmp_path.glob("*.pages"))
    assert result.last_summary_digest is None


def test_worker_wires_scheduled_baseline_and_both_reset_paths(tmp_path, monkeypatch):
    config = dict(duration=10, nodes=3)
    window = dict(start_ns=1000, end_ns=10 * mix.NS, drain_deadline_ns=60 * mix.NS)
    mix.save(tmp_path / "config.json", config)
    mix.save(
        tmp_path / "credentials.json", {f"/flow/task{i:04}": {} for i in range(250)}
    )
    monkeypatch.setattr(mix, "start_window", lambda *args: (config, window))
    monkeypatch.setattr(mix, "wait_until", lambda *args: None)
    monkeypatch.setattr(mix.time, "monotonic_ns", lambda: 1000)

    def factory(directory, role, log):
        result = wire(directory, log, role)
        calls = [0]

        def since(view):
            calls[0] += 1
            if calls[0] == 1:
                raise QueryResponseError("reset_required", "controlled reset")
            if calls[0] == 2:
                return replace(view, stale_scopes=("/flow",))
            return view

        result.client.run_query_since = since
        result.client.run_query_detail = lambda view, path, groups: SimpleNamespace(
            path=path
        )
        return result

    monkeypatch.setattr(mix, "QueryWire", factory)
    mix.queries(
        tmp_path
    )  # Actual three-channel worker; public summary assembly and persistence.
    events = list(mix.rows(tmp_path / "query-requests.jsonl"))
    digests = [r for r in events if r["kind"] == "summary_digest"]
    assert sorted((r["channel"], r["reason"]) for r in digests) == [
        ("detail", "baseline"),
        ("since", "baseline"),
        ("since", "reset_required"),
        ("since", "stale_scopes"),
        ("summary", "scheduled"),
    ]
    assert len({r["evidence"]["file"] for r in digests}) == 5
    assert not any(r["kind"] == "failed" for r in events)
    assert mix.verify_summary_archives(tmp_path) == dict(
        verified=True, complete=5, failed_partial=0
    )
    assert mix.read(tmp_path / "queries-final.json")["released"]
    assert not (tmp_path / "integrity-failure.json").exists()


def test_expired_before_open_does_not_create_unbacked_archive_start(tmp_path):
    log = mix.Ledger(tmp_path / "query-requests.jsonl")
    result = wire(tmp_path, log)
    result.begin("summary-0", 1)
    with pytest.raises(TimeoutError):
        result.snapshot("scheduled", 3)
    log.close()
    assert not result.requests
    assert not list(tmp_path.glob("*.pages*"))
    assert not list(mix.rows(tmp_path / "query-requests.jsonl"))


def test_storage_timeout_is_an_evidence_error(tmp_path):
    with mix.SummaryArchive(
        tmp_path / "owned.pages", time.monotonic_ns() + mix.NS
    ) as archive:
        file = archive.file

        def timeout(_):
            raise TimeoutError("controlled filesystem timeout")

        archive.file = SimpleNamespace(write=timeout, close=file.close)
        with pytest.raises(mix.SummaryEvidenceError, match="write failed"):
            archive.append(query_to_pb2(pages()[0]).json)


def test_analyzer_rejects_d_diagnostic_without_required_summary_digest(tmp_path):
    from mix100k01_analyze import summarize

    mix.save(
        tmp_path / "config.json",
        dict(condition="D", tasks=100000, duration=20, slots=1),
    )
    mix.save(
        tmp_path / "result.json",
        dict(
            failure=None, stop_batch=False, lifecycle_seconds=1, qualified_smoke=False
        ),
    )
    mix.save(tmp_path / "window.json", dict(start_ns=0, end_ns=20 * mix.NS))
    with (tmp_path / "events.jsonl").open("w") as target:
        for i in [1, 2]:
            target.write(
                json.dumps(
                    dict(
                        event=dict(
                            kind="checkpoint_outcome", values=[i, 1, 1, 0], at_ns=i
                        )
                    )
                )
                + "\n"
            )
    with (tmp_path / "writer-requests.jsonl").open("w") as target:
        target.write(
            json.dumps(dict(kind="sent", slot=0, sent_ns=0, target_ns=0)) + "\n"
        )
        target.write(
            json.dumps(
                dict(kind="returned", slot=0, returned_ns=1, target_ns=0, flag=0)
            )
            + "\n"
        )
    with (tmp_path / "query-requests.jsonl").open("w") as target:
        for channel, count in [("summary", 2), ("since", 20), ("detail", 20)]:
            for i in range(count):
                target.write(
                    json.dumps(
                        dict(
                            kind="completed",
                            channel=channel,
                            request_id=f"{channel}-{i}",
                            ended_ns=i,
                            target_ns=i,
                            nodes=100101,
                            bytes=1,
                        )
                    )
                    + "\n"
                )
    for role in ["full1", "full2"]:
        (tmp_path / (role + "-requests.jsonl")).write_text(
            json.dumps(
                dict(
                    kind="completed",
                    channel=role,
                    request_id=role + "-0",
                    ended_ns=1,
                    target_ns=0,
                    count=100101,
                    bytes=1,
                )
            )
            + "\n"
        )
    result = summarize(tmp_path)
    assert not result["summary_digests"]["verified"]
    assert not result["valid_for_pair"]
    assert not result["usable_D"]
