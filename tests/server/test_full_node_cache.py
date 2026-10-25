"""Immutable compact-row reuse, fixed reads, context and retention boundaries."""

import asyncio
from collections import OrderedDict
from dataclasses import replace
import gc
import hashlib
from types import SimpleNamespace
import threading

import pytest

from takler.client.full_snapshot import FullSnapshotClient
from takler.protocol.full_snapshot_codec import pack_node, unpack_node
from takler.protocol.query_v1 import FullSnapshotRequest, decode_query_document
from takler.server import full_snapshot as module
from takler.core import Bunch, Flow
from takler.server.scheduler import Scheduler
from takler.tasks.shell import ShellScriptTask
from tests.server.test_full_snapshot import _LocalTransport, _service


class WireTransport(_LocalTransport):
    def query(self, request):
        if isinstance(request, FullSnapshotRequest):
            raw = asyncio.run(self.service.full_snapshots.page_encoded(request))
            return self.mutate(decode_query_document(raw))
        return super().query(request)


@pytest.mark.parametrize(
    "groups",
    [
        ["summary"],
        ["summary", "definition"],
        ["summary", "runtime"],
        ["summary", "parameters"],
        ["summary", "artifacts"],
        ["summary", "runtime", "definition", "artifacts", "parameters", "service"],
    ],
)
def test_warm_rows_match_legacy_projection_for_selected_groups(groups):
    scheduler, flow = _service(tasks=6)
    task = flow.children[0]
    task.add_parameter("UNICODE", "值α")
    task.add_parameter("JSON", '],"nodes":[["/forged"]],"service":{"x":"值"}')
    task.add_parameter("TAKLER_SECRET", "never-cache-secret")
    task.add_event("ready")
    task.add_meter("progress", 0, 100)
    task.set_meter("progress", 17)
    version = scheduler.version_store.pin()
    expected = {
        row.path: unpack_node(
            pack_node(module._full_node(version, row.path, tuple(groups)), groups),
            groups,
        )
        for row in version.iter_nodes()
    }
    client = FullSnapshotClient(WireTransport(scheduler))
    try:
        cold = client.read_full_snapshot(field_groups=groups)
        warm = client.read_full_snapshot(field_groups=groups)
        assert cold.nodes_by_path == warm.nodes_by_path == expected
        cache = scheduler.query_service.full_snapshots._node_cache
        assert cache.info()["entries"] > 0
        assert all(b"never-cache-secret" not in raw for raw in cache.entries.values())
        assert all(
            type(key) is bytes
            and type(raw) is bytes
            and not gc.is_tracked(key)
            and not gc.is_tracked(raw)
            for key, raw in cache.entries.items()
        )
    finally:
        scheduler.query_service.full_snapshots.close()


def test_warm_read_avoids_source_decodes_and_compact_reconstruction(monkeypatch):
    scheduler, _ = _service(tasks=1000)
    version = scheduler.version_store.pin()
    source_bytes = [
        part
        for row in version.iter_nodes()
        for part in (row.public, row.definition, row.runtime)
    ]
    source_ids = {id(part) for part in source_bytes}
    cached_ids = set()
    counts = dict(source_loads=0, cached_loads=0, projection=0, pack=0)
    loads, full, pack = module.json.loads, module._full_node, module.pack_node

    def decoded(value, *args, **kwargs):
        if id(value) in source_ids:
            counts["source_loads"] += 1
        if id(value) in cached_ids:
            counts["cached_loads"] += 1
        return loads(value, *args, **kwargs)

    def projected(*args):
        counts["projection"] += 1
        return full(*args)

    def packed(*args):
        counts["pack"] += 1
        return pack(*args)

    monkeypatch.setattr(module.json, "loads", decoded)
    monkeypatch.setattr(module, "_full_node", projected)
    monkeypatch.setattr(module, "pack_node", packed)
    client = FullSnapshotClient(WireTransport(scheduler))
    try:
        cold = client.read_full_snapshot()
        assert counts["source_loads"] >= 3 * 1002
        assert counts["projection"] >= 1002
        cached_ids.update(
            id(raw)
            for raw in scheduler.query_service.full_snapshots._node_cache.entries.values()
        )
        counts.update(source_loads=0, cached_loads=0, projection=0, pack=0)
        warm = client.read_full_snapshot()
        assert warm.nodes_by_path == cold.nodes_by_path
        assert counts["projection"] == counts["pack"] == 2
        assert counts["source_loads"] < 15
        assert counts["cached_loads"] == 0
    finally:
        scheduler.query_service.full_snapshots.close()


def test_cache_key_covers_content_metadata_owner_and_group_order():
    scheduler, _ = _service()
    row = scheduler.version_store.pin().node("/f/t0000")
    lease = SimpleNamespace(owner="one", groups=("summary", "runtime", "definition"))
    key = module._node_cache_key(lease, row)
    assert key is not None
    assert type(key) is bytes and len(key) == 32
    prefixed = SimpleNamespace(
        owner=lease.owner,
        groups=lease.groups,
        node_cache_prefix=hashlib.sha256(
            module._canonical([lease.owner, lease.groups])
        ).digest(),
    )
    assert module._node_cache_key(prefixed, row) == key
    for field, value in (
        ("public", row.public + b" "),
        ("definition", row.definition + b" "),
        ("runtime", row.runtime + b" "),
        ("path", "/other"),
        ("parent", "/other"),
        ("children", ("/f/child",)),
    ):
        assert module._node_cache_key(lease, replace(row, **{field: value})) != key
    assert module._node_cache_key(lease, replace(row, identity=row.identity + 1)) == key
    assert (
        module._node_cache_key(SimpleNamespace(owner="two", groups=lease.groups), row)
        != key
    )
    assert (
        module._node_cache_key(
            SimpleNamespace(owner="one", groups=tuple(reversed(lease.groups))), row
        )
        != key
    )
    a = replace(row, public=b"a", definition=b"bc", runtime=b"d")
    b = replace(row, public=b"ab", definition=b"c", runtime=b"d")
    assert module._node_cache_key(lease, a) != module._node_cache_key(lease, b)
    assert module._node_cache_key(
        lease, replace(row, parent=None)
    ) != module._node_cache_key(lease, replace(row, parent=""))
    assert module._node_cache_key(
        lease, replace(row, path="/ab", parent="c")
    ) != module._node_cache_key(lease, replace(row, path="/a", parent="bc"))
    assert not gc.is_tracked(key)
    assert all(key is not part for part in (row.public, row.definition, row.runtime))
    scheduler.query_service.full_snapshots.close()


def test_old_and_new_versions_do_not_share_changed_rows():
    scheduler, flow = _service(tasks=1000)
    task = flow.children[-1]
    task.add_parameter("COLOR", "old")
    task.add_meter("progress", 0, 100)
    transport = WireTransport(scheduler)
    client = FullSnapshotClient(transport)
    first = client.read_full_snapshot()
    changed = False

    def mutate(page):
        nonlocal changed
        if page.kind == "full_snapshot_page" and not changed:
            changed = True
            task.add_parameter("COLOR", "new")
            task.set_meter("progress", 17)
            task.add_event("new-definition")
            flow.add_task("later")
        return page

    transport.mutate = mutate
    try:
        old = client.read_full_snapshot()
        assert old.nodes_by_path == first.nodes_by_path
        fresh = client.read_full_snapshot()
        row = fresh.nodes_by_path[task.node_path]
        assert row["groups"]["parameters"]["user"][0]["value"] == "new"
        assert any(
            item["name"] == "progress" and item["value"] == 17
            for item in row["groups"]["runtime"]["meters"]
        )
        assert row["groups"]["definition"]["events"][0]["name"] == "new-definition"
        assert "/f/later" in fresh.nodes_by_path["/f"]["child_paths"]
        assert fresh.completion.state_version > old.completion.state_version
    finally:
        scheduler.query_service.full_snapshots.close()


def test_shell_and_blob_rows_keep_fixed_context_fallback(tmp_path):
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    flow.add_parameter("TAKLER_HOME", str(tmp_path / "old"))
    flow.add_task(ShellScriptTask("shell"))
    task = flow.add_task("blob")
    task.add_parameter("PAYLOAD", "α" * 100_000)
    scheduler = Scheduler(bunch)
    scheduler.query_service._owner = lambda: "cache-test"
    client = FullSnapshotClient(WireTransport(scheduler))
    try:
        old = client.read_full_snapshot()
        flow.add_parameter("TAKLER_HOME", str(tmp_path / "new"))
        fresh = client.read_full_snapshot()
        assert (
            old.nodes_by_path["/f/shell"]["groups"]["artifacts"]["job_ref"]["path"]
            != fresh.nodes_by_path["/f/shell"]["groups"]["artifacts"]["job_ref"]["path"]
        )
        assert (
            fresh.nodes_by_path["/f/blob"]["groups"]["parameters"]["user"][0]["value"]
            == "α" * 100_000
        )
        version = scheduler.version_store.pin()
        lease = SimpleNamespace(
            owner="cache-test",
            groups=tuple(
                FullSnapshotRequest(
                    kind="full_snapshot_request", schema_version=1
                ).field_groups
            ),
        )
        # The shell has an eligible source key but must not populate the cache;
        # the oversized blob is rejected even before key construction.
        for path in ("/f/shell", "/f/blob"):
            key = module._node_cache_key(lease, version.node(path))
            assert key not in scheduler.query_service.full_snapshots._node_cache.entries
    finally:
        scheduler.query_service.full_snapshots.close()


def test_cache_lru_budget_eviction_and_permanent_close(monkeypatch):
    monkeypatch.setattr(module, "MAX_NODE_CACHE_ENTRIES", 2)
    cache = module._NodeCache()
    cache.resize(4096)
    keys = [bytes([i]) * 32 for i in range(3)]
    cache.put(keys[0], b"[]")
    cache.put(keys[1], b"[]")
    assert cache.get(keys[0]) == b"[]"
    cache.put(keys[2], b"[]")
    assert cache.get(keys[1]) is None and cache.get(keys[0]) == b"[]"
    assert cache.info()["bytes"] <= 4096
    cache.resize(0)
    assert cache.info()["entries"] == cache.info()["bytes"] == 0
    cache.close()
    cache.resize(4096)
    cache.put(keys[0], b"[]")
    assert cache.info()["closed"] and not cache.entries


def test_byte_cache_matches_lru_and_accounting_through_churn(monkeypatch):
    monkeypatch.setattr(module, "MAX_NODE_CACHE_ENTRIES", 7)
    cache, reference = module._NodeCache(), OrderedDict()
    cache.resize(4096)
    for index in range(200):
        key = hashlib.sha256(str(index % 11).encode()).digest()
        raw = b"x" * (index % 23 + 1)
        if index % 3 == 0:
            expected = reference.get(key)
            assert cache.get(key) == expected
            if expected is not None:
                reference.move_to_end(key)
        elif key not in reference:
            while reference and (
                len(reference) >= 7
                or sum(cache._size(k, v) for k, v in reference.items())
                + cache._size(key, raw)
                > cache.limit
            ):
                reference.popitem(last=False)
            reference[key] = raw
            cache.put(key, raw)
        else:
            cache.put(key, raw)
        assert list(cache.entries.items()) == list(reference.items())
        assert cache.bytes == sum(cache._size(k, v) for k, v in reference.items())
        assert cache.bytes <= cache.limit
    cache.resize(1024)
    assert cache.bytes <= 1024
    assert cache.bytes == sum(cache._size(k, v) for k, v in cache.entries.items())
    cache.close()
    assert cache.bytes == 0


def test_optional_cache_yields_budget_before_lease_admission(monkeypatch):
    scheduler, _ = _service(tasks=1000)
    service = scheduler.query_service.full_snapshots
    FullSnapshotClient(WireTransport(scheduler)).read_full_snapshot()
    assert service._node_cache.info()["bytes"] > 4096
    reserve = scheduler.version_store.pin().retained_bytes + 4 * 1024 * 1024
    monkeypatch.setattr(module, "MAX_RETAINED_BYTES", 2 * reserve + 4096)

    async def run():
        request = FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
        try:
            for _ in range(2):
                reply = decode_query_document(await service.page_encoded(request))
                assert reply.kind == "full_snapshot_page"
            assert service._node_cache.info()["bytes"] <= 4096
            assert (
                service._node_cache.info()["bytes"]
                + sum(row.reserved_bytes for row in service._leases.values())
                <= module.MAX_RETAINED_BYTES
            )
        finally:
            service.close()
        assert service._node_cache.info()["bytes"] == 0

    asyncio.run(run())


def test_close_during_worker_prevents_late_cache_retention(monkeypatch):
    scheduler, _ = _service(tasks=1000)
    service = scheduler.query_service.full_snapshots
    entered, proceed = threading.Event(), threading.Event()
    original = module._full_node

    def paused(version, path, groups):
        if path == "/f":
            entered.set()
            assert proceed.wait(5)
        return original(version, path, groups)

    monkeypatch.setattr(module, "_full_node", paused)

    async def run():
        task = asyncio.create_task(
            service.page_encoded(
                FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
            )
        )
        assert await asyncio.to_thread(entered.wait, 5)
        service.close()
        proceed.set()
        reply = decode_query_document(await task)
        assert reply.code == "snapshot_expired"
        assert (
            service._node_cache.info()["bytes"] == 0
            and service._node_cache.info()["closed"]
        )
        assert not service._leases

    asyncio.run(run())
