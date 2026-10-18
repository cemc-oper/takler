"""MIX100K-01 targeted regressions; never starts a Takler listener."""

import asyncio
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


def test_complete_public_stream_link_and_cancel(prepared, tmp_path):
    from takler.client.full_snapshot import FullSnapshotClient, FullSnapshotComplete
    from takler.protocol.query_v1 import FailureReply

    scheduler, paths, _, loop, _ = prepared
    expected = mix.root_link(scheduler.version_store.pin(), paths)
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
    for item in FullSnapshotClient(Transport()).iter_full_snapshot(scope_path="/"):
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
