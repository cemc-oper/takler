"""MIX100K-01 targeted regressions; never starts a Takler listener."""

import asyncio
from dataclasses import replace
import hashlib
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import mix100k01 as mix


def test_slot_admission_and_no_catchup():
    assert mix.slot_reason(100, 100, 10, False, 0) is None
    assert mix.slot_reason(110, 100, 10, False, 0) == "expired"
    assert mix.slot_reason(101, 100, 10, True, 1) == "task_busy"
    assert mix.slot_reason(101, 100, 10, False, 64) == "write_cap"
    assert mix.slot_reason(101, 100, 10, False, 63) is None


def test_actual_send_boundary_rejects_before_sent():
    events, calls = [], []

    def log(kind, **values):
        events.append((kind, values))

    def rpc(*a, **k):
        calls.append((a, k))

    assert not mix.meter_call(rpc, None, [], 0, 1, time.monotonic_ns() + mix.NS, log, 0)
    assert [e[0] for e in events] == ["not_sent"]
    assert not calls
    events.clear()
    with pytest.raises(TimeoutError):
        mix.meter_call(rpc, None, [], 0, time.monotonic_ns() + mix.NS, 1, log, 0)
    assert not events and not calls


def test_actual_stub_call_and_wait_timestamps():
    events = []

    def rpc(payload, **kwargs):
        assert events[-1][0] == "sent"
        assert 0 < kwargs["timeout"] <= 10
        return SimpleNamespace(flag=0)

    now = time.monotonic_ns()
    assert mix.meter_call(
        rpc,
        "encoded",
        [],
        now,
        now + mix.NS,
        now + mix.NS,
        lambda k, **v: events.append((k, v)),
        0,
    )
    assert [e[0] for e in events] == ["sent", "returned"]


def test_ledger_bounded_record_and_no_silent_oversize(tmp_path):
    log = mix.Ledger(tmp_path / "events.jsonl")
    log("small")
    with pytest.raises(ValueError, match="16 KiB"):
        log("large", text="x" * 16384)
    log.close()
    assert len(list(mix.rows(tmp_path / "events.jsonl"))) == 1


def test_full_view_gzip_retains_every_node_and_semantic_digest(tmp_path):
    from takler.client.full_snapshot import FullSnapshotComplete

    node = {"path": "/f/t", "value": "值α" * 10_000}
    raw = mix.canonical(node) + b"\n"
    complete = FullSnapshotComplete(
        session_id="s",
        full_snapshot_id="f",
        state_version=1,
        as_of="2026-10-25T00:00:00Z",
        scope_path="/",
        total_nodes=1,
        total_bytes=len(raw),
        sha256="a" * 64,
        ancestor_context=[],
        service=None,
    )
    wire = mix.QueryWire.__new__(mix.QueryWire)
    wire.deadline_ns = time.monotonic_ns() + 10 * mix.NS
    wire.cancelled, wire.bytes, wire.rpcs = True, 42, 2
    wire.client = SimpleNamespace(
        run_iter_full_snapshot=lambda **_: iter([node, complete])
    )
    for name in ("view.jsonl", "view.jsonl.gz"):
        path = tmp_path / name
        result = wire.full(path, 1)
        assert list(mix.rows(path)) == [node]
        assert result["semantic_sha256"] == hashlib.sha256(raw).hexdigest()
        assert result["view_bytes"] == len(raw)
        assert result["archive_bytes"] == path.stat().st_size
        assert result["count"] == 1 and result["cancelled"]
        assert result["bytes"] == 42 and result["rpcs"] == 2
        with pytest.raises(FileExistsError):
            wire.full(path, 1)
    compressed = tmp_path / "view.jsonl.gz"
    assert compressed.stat().st_size < len(raw) // 10
    compressed.write_bytes(compressed.read_bytes()[:-8])
    with pytest.raises(EOFError):
        list(mix.rows(compressed))
    wire.client = SimpleNamespace(
        run_iter_full_snapshot=lambda **_: iter(
            [node, replace(complete, total_nodes=2)]
        )
    )
    with pytest.raises(ValueError, match="node count"):
        wire.full(tmp_path / "bad.jsonl.gz", 1)


def test_pool_distribution_100k_and_formula():
    # Same deterministic selection rule; avoid building 100k online objects here.
    flows = {
        str(i): SimpleNamespace(
            node_path=f"/flow{i:03d}",
            children=[
                SimpleNamespace(node_path=f"/flow{i:03d}/task{j:04d}") for j in range(3)
            ],
        )
        for i in range(100)
    }
    paths = mix.pool_paths(SimpleNamespace(flows=flows))
    counts = {f"flow{i:03d}": 0 for i in range(100)}
    for path in paths:
        counts[path.split("/")[1]] += 1
    assert len(set(paths)) == 250
    assert sorted(counts.values()) == [2] * 50 + [3] * 50
    assert max(i // 250 + 1 for i in range(18000)) == 72


@pytest.fixture
def prepared():
    from takler.server.scheduler import Scheduler

    bunch, paths = mix.fixture(1000)
    scheduler = Scheduler(bunch)
    loop = asyncio.Runner()
    events = []
    credentials = loop.run(
        mix.prepare(
            scheduler,
            paths,
            lambda k, **v: events.append((k, v)),
            time.monotonic_ns() + 20 * mix.NS,
        )
    )
    yield scheduler, paths, credentials, loop, events
    loop.close()


def test_normal_handler_identity_and_wire_meter(prepared):
    from takler.protocol.commands import Command
    from takler.server.protocol.adapter import request_to_pb2, request_from_pb2
    from takler.server.handlers import CommandHandlers
    from takler.server.auth import (
        CallCredentials,
        set_call_credentials,
        reset_call_credentials,
    )

    scheduler, paths, credentials, loop, events = prepared
    assert len(events) == 500 and all(r[1]["flag"] == 0 for r in events)
    assert all(
        t.find_meter("progress").value == 0
        for t in (scheduler.bunch.find_node(p) for p in paths)
    )
    path = paths[0]
    payload = dict(
        node_path=path,
        source_task_path=path,
        attempt_id=credentials[path]["attempt"],
        meter_name="progress",
        meter_value="72",
    )
    request = request_from_pb2(Command.METER, request_to_pb2(Command.METER, payload))

    async def apply():
        token = set_call_credentials(
            CallCredentials(job_password=credentials[path]["password"])
        )
        try:
            return await CommandHandlers(scheduler).handle(Command.METER, request)
        finally:
            reset_call_credentials(token)

    assert loop.run(apply()).flag == 0
    assert scheduler.bunch.find_node(path).find_meter("progress").value == 72
    assert len(mix.pool_state(scheduler.bunch, paths)) == 250


@pytest.mark.parametrize("scoped", [False, True])
def test_complete_public_stream_link_and_cancel(prepared, tmp_path, scoped):
    from takler.client.full_snapshot import FullSnapshotClient, FullSnapshotComplete
    from takler.protocol.query_v1 import FailureReply

    scheduler, paths, _, loop, _ = prepared
    scope = paths[0] if scoped else "/"
    expected = mix.root_link(scheduler.version_store.pin(), paths, scope)
    cancellation = []

    class Transport:
        def query(self, request):
            if request.kind == "capabilities_request":
                return scheduler.query_service.capabilities()
            if request.kind == "full_snapshot_request":
                reply = loop.run(scheduler.query_service.full_snapshots.page(request))
                assert not isinstance(reply, FailureReply), reply
                return reply
            if request.kind == "cancel_full_snapshot_request":
                reply = loop.run(scheduler.query_service.full_snapshots.cancel(request))
                cancellation.append(reply.cancelled)
                return reply
            raise AssertionError(request.kind)

    import hashlib

    digest, count, complete = hashlib.sha256(), 0, None
    for item in FullSnapshotClient(Transport()).iter_full_snapshot(scope_path=scope):
        if isinstance(item, FullSnapshotComplete):
            complete = item
        else:
            count += 1
            digest.update(mix.canonical(item) + b"\n")
    actual = dict(
        state_version=complete.state_version,
        count=count,
        semantic_sha256=digest.hexdigest(),
        cancelled=cancellation == [True],
    )
    mix.verify_stream_link(actual, expected)
    with pytest.raises(ValueError, match="version/count/digest"):
        mix.verify_stream_link(
            dict(actual, state_version=actual["state_version"] + 1), expected
        )
    with pytest.raises(ValueError, match="version/count/digest"):
        mix.verify_stream_link(dict(actual, semantic_sha256="wrong"), expected)
    assert not scheduler.query_service.full_snapshots._leases


def test_query_wire_deadline_before_boundary(tmp_path):
    events = []
    wire = mix.QueryWire.__new__(mix.QueryWire)
    wire.deadline_ns = 1
    wire.role, wire.request_id = "summary", "s0"
    wire.log = lambda kind, **v: events.append((kind, v))
    wire.client = SimpleNamespace(
        transport=SimpleNamespace(
            query_stub=SimpleNamespace(GetCapabilities=lambda *a, **k: None),
            _build_metadata=lambda kind: [],
        )
    )
    from takler.protocol.query_v1 import CapabilitiesRequest

    with pytest.raises(TimeoutError):
        wire.query(CapabilitiesRequest(kind="capabilities_request", schema_version=1))
    assert not events


def test_checkpoint_restore_own_fixed_version(prepared, tmp_path):
    scheduler, paths, _, _, _ = prepared
    root = scheduler.version_store.pin()
    target = tmp_path / "state.check"
    with target.open("w") as stream:
        root.checkpoint_stream(stream.write)
    result = mix.checkpoint_equivalence(target, root)
    assert result["equivalent"] and result["nodes"] == 1002


def test_registry_watchdog_cleanup_hard_bounded(tmp_path):
    # Outer supervisor enforces a hard 20s including helper cleanup, below 30s.
    helper = """
import sys,time
from pathlib import Path
from r3_14_perf04_safety import Registry,Watchdog
r=Registry(Path(sys.argv[1])/'registry.json')
w=Watchdog(r,time.monotonic_ns()+12_000_000_000)
w.arm()
r.launch([sys.executable,'-c','import time;time.sleep(60)'],'controlled')
s=r.cleanup(time.monotonic_ns()+3_000_000_000)
assert s['live_registered']==[],s
assert s['service_launches']==0,s
"""
    completed = subprocess.run(
        [sys.executable, "-c", helper, str(tmp_path)],
        env=dict(__import__("os").environ, PYTHONPATH=str(Path(mix.__file__).parent)),
        timeout=20,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_missing_query_terminal_is_critical(tmp_path):
    mix.save(tmp_path / "config.json", dict(duration=60))
    log = mix.Ledger(tmp_path / "query-requests.jsonl")
    log("planned", request_id="summary-0")
    log.close()
    with pytest.raises(ValueError, match="terminal missing"):
        mix.verify_query_ledgers(tmp_path)


def test_summary_quantile_keeps_slow_samples():
    from mix100k01_analyze import distribution

    assert distribution([1, 2, 3, 100])["p95"] == 100
    assert distribution([])["n"] == 0


def test_product_profile_uses_fixed_workload_and_new_caps(tmp_path, monkeypatch):
    import r3_14_isolated_probe

    monkeypatch.setattr(r3_14_isolated_probe, "diagnostic_copy", lambda *args: None)
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "uv.lock").write_text("locked test fixture")
    batch = tmp_path / "batch"
    batch.mkdir()
    mix.freeze(batch, repo, product=True)
    config = mix.read(batch / "config.json")
    assert (config["tasks"], config["nodes"], config["pool"]) == (100000, 100101, 250)
    assert (config["duration"], config["rate"], config["slots"]) == (300, 100, 30000)
    assert config["caps"]["setup_s"] == 600
    assert config["caps"]["run_s"] == 1200
    assert config["order"] == ["smoke", "D", "D"]
    assert config["scheduler_interval_s"] == 10
    assert config["boundary_scope"] == "pool_task"
    assert config["full_readers"] == 2 and config["checkpoint_interval_s"] == 120
    assert config["smoke_periodic_tail"] is True
    assert config["meter_max"] == 120
    assert config["stop_limits"]["run_evidence_max"] == 8 * mix.GIB
    assert config["stop_limits"]["rss_sum_max"] == 8 * mix.GIB
    assert (config["smoke"]["duration"], config["smoke"]["slots"]) == (150, 1500)


def test_product_budget_requires_smoke_and_retains_prior_failures(tmp_path):
    mix.save(
        tmp_path / "config.json",
        {"profile": "product-20261025", "order": ["smoke", "D", "D"]},
    )
    mix.save(tmp_path / "budget.json", {"runs": []})
    with pytest.raises(ValueError, match="order"):
        mix.budget_open(tmp_path, "D", 100)
    assert mix.budget_open(tmp_path, "smoke", 100) == 1
    budget = mix.read(tmp_path / "budget.json")
    budget["runs"][0].update(end_ns=200, qualified_smoke=True, stop_batch=False)
    mix.save(tmp_path / "budget.json", budget)
    assert mix.budget_open(tmp_path, "D", 300) == 2
    budget = mix.read(tmp_path / "budget.json")
    budget["runs"][1].update(end_ns=400, stop_batch=True)
    mix.save(tmp_path / "budget.json", budget)
    with pytest.raises(ValueError, match="prior product"):
        mix.budget_open(tmp_path, "D", 500)


def test_wait_until_target_crossing_is_not_deadline_failure(monkeypatch):
    clock = iter([99, 101])
    sleeps = []
    monkeypatch.setattr(mix.time, "monotonic_ns", lambda: next(clock))
    monkeypatch.setattr(mix.time, "sleep", sleeps.append)
    mix.wait_until(100, 200)
    assert sleeps == [1 / mix.NS]


def test_wait_until_does_not_sleep_after_earlier_deadline(monkeypatch):
    monkeypatch.setattr(mix.time, "monotonic_ns", lambda: 100)
    monkeypatch.setattr(mix.time, "sleep", lambda _: pytest.fail("expired wait slept"))
    with pytest.raises(TimeoutError, match="deadline"):
        mix.wait_until(200, 100)


def test_periodic_tail_requires_completed_non_exit_archive(tmp_path):
    path = tmp_path / "server-ledger.jsonl"
    path.write_bytes(b'{"kind":"checkpoint_archive","exit_save":false}')
    assert not mix.periodic_archive_seen(tmp_path)
    path.write_text('{"kind":"checkpoint_archive","exit_save":true}\n')
    assert not mix.periodic_archive_seen(tmp_path)
    path.write_text('{"kind":"checkpoint_archive","exit_save":false}\n')
    assert mix.periodic_archive_seen(tmp_path)


def test_two_minute_smoke_accepts_one_periodic_tail_but_not_exit_save(tmp_path):
    mix.save(tmp_path / "config.json", {"slots": 1, "smoke_periodic_tail": True})
    mix.save(tmp_path / "window.json", {"start_ns": 100, "end_ns": 200})
    mix.save(tmp_path / "meter-verification.json", {"sent": 1})

    def ledger(name, records):
        (tmp_path / name).write_text(
            "".join(mix.canonical(record).decode() + "\n" for record in records)
        )

    ledger("writer-requests.jsonl", [{"kind": "returned", "flag": 0}])
    for role, channel in (("query", "summary"), ("full1", "full1"), ("full2", "full2")):
        ledger(role + "-requests.jsonl", [{"kind": "completed", "channel": channel}])
    ledger("server-ledger.jsonl", [{"kind": "checkpoint_link", "exit_save": False}])
    periodic = {
        "kind": "service_event",
        "event": {"kind": "checkpoint_outcome", "values": [1, 1, 1, 0], "at_ns": 300},
    }
    exit_save = {
        "kind": "service_event",
        "event": {"kind": "checkpoint_outcome", "values": [2, 1, 1, 1], "at_ns": 310},
    }
    ledger("events.jsonl", [periodic, exit_save])
    assert mix.qualify_smoke(tmp_path)
    assert (
        mix.read(tmp_path / "smoke-qualification.json")["periodic_scope"]
        == "whole_smoke_lifecycle"
    )
    mix.save(tmp_path / "config.json", {"slots": 2, "smoke_periodic_tail": True})
    assert mix.qualify_smoke(tmp_path)
    assert mix.read(tmp_path / "smoke-qualification.json")["target_complete"] is False
    ledger("events.jsonl", [exit_save])
    assert not mix.qualify_smoke(tmp_path)


def test_extended_window_meter_pool_remains_legal():
    bunch, paths = mix.fixture(1000, meter_max=120)
    assert max(i // 250 + 1 for i in range(30000)) == 120
    assert all(
        bunch.find_node(path).find_meter("progress").max_value == 120 for path in paths
    )
