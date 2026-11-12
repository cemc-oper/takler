"""Canonical fixed-row reuse retains complete page and compatibility checks."""

from dataclasses import FrozenInstanceError, replace
import json

import pytest

from takler.core import Bunch, Flow
from takler.protocol.query_v1 import decode_query_document
from takler.protocol.wire import WireError
from takler.serialization import version as versions
from takler.server import query_service as module
from takler.server.scheduler import Scheduler
from tests.query_fixtures import page_request


def service(tasks=3):
    bunch = Bunch('root 值"],"nodes":[]')
    flow = bunch.add_flow(Flow("f"))
    for i in range(tasks):
        flow.add_task(f"t{i}")
    scheduler = Scheduler(bunch)
    return scheduler, scheduler.query_service, flow


def changed(root, path, **updates):
    shard, offset = root.index[path]
    rows = list(root.nodes[shard])
    rows[offset] = replace(rows[offset], **updates)
    shards = list(root.nodes)
    shards[shard] = tuple(rows)
    return replace(root, nodes=tuple(shards))


def capture(query, root, **request):
    return query._capture(
        page_request(**request), "owner", "query", "snapshot", 1, 32 * 1024**2, root
    )


def normalized(result):
    scope, pages, size = result
    assert size == sum(map(len, pages))
    documents = []
    for raw in pages:
        page = decode_query_document(raw, expected="page")
        assert page.kind == "page"
        doc = json.loads(raw)
        doc.pop("expires_at")  # Each capture has its own observation time.
        documents.append(doc)
    return scope, documents


@pytest.mark.parametrize(
    "replacement", ["copy", "changed", "other_proof", "no_summary"]
)
def test_stale_or_missing_proof_keeps_legacy_validation(replacement):
    scheduler, query, _ = service()
    root = scheduler.version_store.pin()
    row = root.node("/f/t0")
    updates = {}
    expected = json.loads(row.summary)
    if replacement == "copy":
        updates["summary"] = memoryview(row.summary).tobytes()
        assert updates["summary"] is not row.summary
    elif replacement == "changed":
        expected["status"] = "complete"
        updates["summary"] = json.dumps(expected).encode()
    elif replacement == "other_proof":
        updates["summary_encoding"] = root.node("/f/t1").summary_encoding
    else:
        updates["summary"] = None
    try:
        docs = normalized(capture(query, changed(root, row.path, **updates)))[1]
        assert (
            next(n for p in docs for n in p["nodes"] if n["path"] == row.path)
            == expected
        )
    finally:
        query.close()


def test_plain_duplicate_keys_are_normalized_before_page_publication():
    scheduler, query, _ = service()
    root = scheduler.version_store.pin()
    raw = root.node("/f/t0").summary
    raw = raw[:-1] + b',"status":"complete"}'
    try:
        docs = normalized(capture(query, changed(root, "/f/t0", summary=raw)))[1]
        node = next(n for p in docs for n in p["nodes"] if n["path"] == "/f/t0")
        assert node["status"] == "complete"
    finally:
        query.close()


@pytest.mark.parametrize("raw", [b"{", b'{"path":"/f/t0"}', b'{"unknown":true}'])
def test_invalid_plain_rows_still_fail_capture(raw):
    scheduler, query, _ = service()
    try:
        root = changed(scheduler.version_store.pin(), "/f/t0", summary=raw)
        with pytest.raises(ValueError):
            capture(query, root)
        assert not query._snapshots
    finally:
        query.close()


def test_summary_proof_is_bound_immutable_and_reused_for_meter_only_updates():
    scheduler, query, flow = service()
    task = flow.children[0]
    task.add_meter("progress", 0, 100)
    try:
        before = scheduler.version_store.pin()
        proof = before.node(task.node_path).summary_encoding
        assert proof.data is before.node(task.node_path).summary
        assert proof.path == task.node_path
        with pytest.raises(FrozenInstanceError):
            proof.data = b"{}"
        with pytest.raises(TypeError):
            replace(proof, data=b"{}")
        task.set_meter("progress", 1)
        after = scheduler.version_store.pin()
        assert after.node(task.node_path).summary_encoding is proof
        assert after.node(task.node_path).runtime != before.node(task.node_path).runtime
        assert after.retained_bytes == before.retained_bytes
    finally:
        query.close()


def test_invalid_extension_summary_does_not_break_fixed_root_publication(monkeypatch):
    project = versions.project_show

    def custom(*args, **kwargs):
        value = project(*args, **kwargs)
        if value["node_kind"] == "task":
            value["type_id"] = ""
        return value

    monkeypatch.setattr(versions, "project_show", custom)
    scheduler, query, _ = service()
    try:
        root = scheduler.version_store.pin()
        assert root.node("/f/t0").summary_encoding is None
        assert root.node("/f/t0").public is not None
        with pytest.raises(ValueError):
            capture(query, root)
    finally:
        query.close()


def test_complete_page_validation_and_existing_budgets_remain_enforced():
    scheduler, query, _ = service()
    root = scheduler.version_store.pin()
    try:
        with pytest.raises(ValueError):
            query._capture(page_request(), "owner", "query", "snapshot", 0, 2**25, root)
        with pytest.raises(MemoryError):
            query._capture(page_request(), "owner", "query", "snapshot", 1, 10, root)
        public = json.loads(root.node("/").public)
        public["name"] = "x" * module.MAX_PAGE_BYTES
        oversized = changed(root, "/", public=json.dumps(public).encode())
        with pytest.raises(WireError):
            capture(query, oversized)
        duplicate = changed(root, "/f", children=("/f/t0", "/f/t0"))
        with pytest.raises(ValueError):
            capture(query, duplicate)
        assert not query._snapshots
    finally:
        query.close()
