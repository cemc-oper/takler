"""Virtual time and substituted transport tests for actual PERF-04 entry points."""

from collections import Counter
import heapq
import itertools
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from r3_14_perf04 import NS, candidates
from r3_14_perf04_runtime import Engine, Limits
from r3_14_perf04_roles import execute
from r3_14_perf04_ledger import Journal, audit_producer


class FakeDispatcher:
    simulation = True

    def __init__(self, delay=0, dispatch_delay=0):
        self.delay, self.dispatch_delay = delay, dispatch_delay
        self.heap = []
        self.seq = itertools.count()
        self.calls = Counter()
        self.peak = 0

    def submit(self, job):
        sent = job["dispatch_ns"] + self.dispatch_delay
        if sent >= job["expires_ns"]:
            event = dict(kind="not_sent", id=job["id"], at_ns=sent, reason="expired")
            heapq.heappush(self.heap, (sent, next(self.seq), event))
            return True
        self.calls[(job["role"], job["operation"])] += 1
        result = dict(flag=0)
        token = job["reservation"]
        if token:
            result["proof"] = dict(
                effective=True,
                path=job["path"],
                stage=token["stage"],
                attempt=token["attempt"],
            )
            if token["stage"] == 11:
                result["identity"] = dict(
                    attempt="new-" + str(job["id"]),
                    password="private-current",
                    generation=job["credentials"]["generation"],
                )
        elif job["channel"] in ("structure", "batch"):
            result["proof"] = dict(effective=True)
        for at, event in [
            (sent, dict(kind="sent", id=job["id"], at_ns=sent)),
            (
                sent + self.delay,
                dict(
                    kind="completed",
                    id=job["id"],
                    at_ns=sent + self.delay,
                    outcome="success",
                    result=result,
                ),
            ),
        ]:
            heapq.heappush(self.heap, (at, next(self.seq), event))
        self.peak = max(self.peak, len(self.heap))
        return True

    def poll(self, now):
        events = []
        while self.heap and self.heap[0][0] <= now:
            events.append(heapq.heappop(self.heap)[2])
        return events


def make_engine(variant="V", delay=0, dispatch_delay=0, emit=lambda row: None):
    size = 1000 if variant in ("S", "V") else 10000
    paths, _ = candidates(size, variant in ("D", "L", "R"))
    ids = {
        p: dict(
            attempt="attempt-" + str(i), password="secret-" + str(i), generation="fixed"
        )
        for i, p in enumerate(paths)
    }
    dispatcher = FakeDispatcher(delay, dispatch_delay)
    return Engine(
        variant,
        100 * NS,
        paths,
        ids,
        dispatcher,
        emit,
        templates={
            "load-A": b"a",
            "load-B": b"b",
            "replace-A": b"a",
            "replace-B": b"b",
        },
    )


def drive(engine, pauses=()):
    now = engine.grid.start_ns
    pause_index = 0
    while engine.grid.cursor < engine.grid.total:
        if (
            pause_index < len(pauses)
            and now >= engine.grid.start_ns + pauses[pause_index][0]
        ):
            now = engine.grid.start_ns + pauses[pause_index][1]
            pause_index += 1
        engine.tick(now)
        future = [engine.grid.at(engine.grid.cursor)]
        if engine.dispatcher.heap:
            future.append(engine.dispatcher.heap[0][0])
        now = max(now, min(future))
    engine.tick(engine.grid.end_ns)
    return engine.finish()


@pytest.mark.parametrize("delay", [0, 2 * NS])
def test_real_engine_grid_and_two_distinct_rotation_commands(delay):
    engine = make_engine(delay=delay)
    result = drive(engine)
    child = next(
        r["counts"]
        for r in result["ledger"]["dimensions"]
        if r["dimension"] == "role" and r["key"] == "W"
    )
    assert child["offered"] == 27000
    assert child["offered"] == child.get("sent", 0) + child.get("not_sent", 0) + sum(
        r["role"] == "W" and r["status"] == "dispatched"
        for r in result["ledger"]["pending"]
    )
    assert (
        result["limits_peak"]["global"] <= 256 and result["limits_peak"]["pool"] <= 250
    )
    assert (
        result["limits_peak"].get("rotation", 0) <= 16 and result["r_pipelines"] <= 16
    )
    assert (
        engine.dispatcher.calls[("R", "requeue")] > 0
        and engine.dispatcher.calls[("R", "run")] > 0
    )
    if delay:
        assert child["not_sent"] > 0
    else:
        assert child["sent"] == 27000


def test_late_wake_and_executor_queue_expiry_are_not_counted_sent():
    engine = make_engine()
    result = drive(engine, [(59 * NS, 71 * NS)])
    assert result["ledger"]["counts"]["expired"] >= 10200
    engine = make_engine(dispatch_delay=11_000_000)
    engine.tick(engine.grid.start_ns)
    assert engine.ledger.counts["sent"] == 0
    engine.tick(engine.grid.start_ns + 11_000_000)
    assert engine.ledger.counts["expired"] >= 1
    assert engine.ledger.counts["sent"] == 3  # Q's three independent 1s slots only.


def test_unknown_then_late_duplicate_success_cannot_change_model():
    engine = make_engine()
    engine.tick(engine.grid.start_ns)
    rid, (_, token) = next((i, v) for i, v in engine.requests.items() if v[1])
    engine.accept(dict(kind="sent", id=rid, at_ns=engine.grid.start_ns))
    engine.accept(
        dict(
            kind="completed", id=rid, at_ns=engine.grid.start_ns + NS, outcome="unknown"
        )
    )
    before = vars(engine.pool.tasks[token.task]).copy()
    engine.accept(
        dict(
            kind="completed",
            id=rid,
            at_ns=engine.grid.start_ns + 2 * NS,
            outcome="success",
        )
    )
    assert vars(engine.pool.tasks[token.task]) == before and engine.stopped


def test_success_without_effect_proof_is_failure_not_child_progress():
    engine = make_engine()
    engine.tick(engine.grid.start_ns)
    rid, (_, token) = next((i, v) for i, v in engine.requests.items() if v[1])
    engine.accept(dict(kind="sent", id=rid, at_ns=engine.grid.start_ns))
    engine.accept(
        dict(
            kind="completed",
            id=rid,
            at_ns=engine.grid.start_ns + 1,
            outcome="success",
            result=dict(flag=0),
        )
    )
    assert (
        engine.stopped == "request invalid_effect"
        and engine.pool.tasks[token.task].unknown
    )
    assert engine.last_child_ns == engine.grid.start_ns


def test_global_role_shared_pool_and_single_task_limits_cannot_borrow():
    limits = Limits()
    for i in range(250):
        assert limits.acquire(i, "child", f"/task{i}") is None
    assert limits.acquire(999, "rotation", "/other") == "pool_busy"
    channels = ["batch", "structure", "since", "detail", "status", "verify"]
    for i, c in enumerate(channels, 250):
        assert limits.acquire(i, c) is None
    assert len(limits.active) == 256
    assert limits.acquire(1000, "verify") == "global_busy"
    for i in range(256):
        limits.release(i)
    assert limits.acquire(1, "child", "/same") is None
    assert limits.acquire(2, "rotation", "/same") == "task_busy"
    limits = Limits()
    for i in range(16):
        assert limits.acquire(i, "rotation", f"/task{i}") is None
    assert limits.acquire(17, "rotation", "/next") == "role_busy"
    assert limits.acquire(20, "since") is None
    assert limits.acquire(21, "since") == "role_busy"


@pytest.mark.parametrize("variant", ["H", "D", "L", "R"])
def test_c_and_q_slots_keep_phases_and_independent_structure_rounds(variant):
    engine = make_engine(variant)
    result = drive(engine)
    assert engine.dispatcher.calls[("C", "force")] == 17
    assert engine.dispatcher.calls[("C", "load")] == (2 if variant == "L" else 0)
    assert engine.dispatcher.calls[("C", "replace")] == (2 if variant == "R" else 0)
    assert engine.dispatcher.calls[("Q", "since")] == 180
    assert engine.dispatcher.calls[("Q", "detail")] == 180
    assert engine.dispatcher.calls[("Q", "status")] == 180
    assert result["limits_peak"]["structure"] <= 1 if variant in ("L", "R") else True


def test_transport_boundary_rechecks_expiry_and_emits_three_waits():
    clock = [0]
    events = []

    class Transport:
        calls = 0

        def invoke(self, job, boundary, timeout):
            clock[0] = job["expires_ns"]
            boundary()
            self.calls += 1

    transport = Transport()
    job = dict(id=1, expires_ns=10, channel="child")
    execute(job, transport, events.append, lambda: clock[0])
    assert transport.calls == 0 and events[0]["kind"] == "not_sent"
    from r3_14_perf04_ledger import Ledger

    rows = []
    ledger = Ledger(rows.append)
    row = dict(
        id=2,
        role="W",
        operation="meter",
        stage=2,
        flow="f",
        second=0,
        scheduled_ns=100,
        dispatch_ns=105,
        expires_ns=200,
    )
    ledger.offered(row)
    ledger.dispatch(row)
    ledger.sent(2, 110)
    ledger.completed(2, 160, "success", True, rpc_returned_ns=160)
    completion = rows[-1]
    assert [
        completion[k] for k in ["send_wait_ns", "rpc_wait_ns", "total_wait_ns"]
    ] == [10, 50, 60]
    assert ledger.snapshot()["counts_closed"]


def test_transport_no_retry_timeout_and_pending_are_preserved():
    events = []

    class Lost:
        calls = 0

        def invoke(self, job, boundary, timeout):
            self.calls += 1
            assert timeout == 10
            boundary()
            raise TimeoutError("response lost")

    lost = Lost()
    execute(
        dict(id=1, expires_ns=100, channel="child"), lost, events.append, lambda: 10
    )
    assert lost.calls == 1 and [e["kind"] for e in events] == ["sent", "completed"]
    assert events[-1]["outcome"] == "timeout"
    engine = make_engine(delay=2 * NS)
    engine.tick(engine.grid.start_ns)
    engine.tick(engine.grid.start_ns + 1)
    assert engine.finish()["ledger"]["pending"]


def test_journal_final_sequence_loss_and_capacity_are_audited(tmp_path):
    import time

    path = tmp_path / "records.jsonl"
    journal = Journal(path)
    for i in range(20):
        journal.emit(dict(kind="test", id=i))
    final = journal.close(time.monotonic_ns() + 2 * NS)
    assert audit_producer(path, final)["tail"] == "known"
    assert (
        final["memory_bytes"] < 8 * 1024**2
        and final["pending_batch_max_bytes"] <= 1024**2
    )
    assert audit_producer(path, None)["tail"] == "unknown"
    final["produced"] += 1
    assert audit_producer(path, final)["problems"]
    journal = Journal(tmp_path / "oversized")
    with pytest.raises(RuntimeError, match="critical journal loss"):
        journal.emit(dict(text="x" * 20000))
    final = journal.close(time.monotonic_ns() + 2 * NS)
    assert (
        final["dropped"] == 1
        and audit_producer(tmp_path / "oversized", final)["problems"]
    )


def test_grpc_actual_stub_boundary_and_private_run_handoff_without_network():
    from types import SimpleNamespace
    from takler.protocol.commands import ServiceResponse
    from takler.server.protocol import adapter
    from r3_14_perf04_roles import GrpcSingleAttempt

    calls = []
    events = []

    def rpc(request, **kwargs):
        assert events[-1] == "sent"
        calls.append(kwargs)
        return adapter.response_to_pb2(ServiceResponse())

    query_stub = SimpleNamespace(
        **{
            n: rpc
            for n in (
                "GetCapabilities",
                "ReadPage",
                "ReadSince",
                "ReadDetail",
                "ReadDetailChunk",
                "ReadFullSnapshot",
                "ReadFullSnapshotChunk",
                "CancelFullSnapshot",
            )
        }
    )
    transport = SimpleNamespace(
        retry_window=0,
        single_timeout=10,
        query_stub=query_stub,
        stub=SimpleNamespace(RunCommandInit=rpc),
        _call=lambda *a: None,
        query=lambda *a: None,
    )
    private = dict(
        proof=dict(effective=True, path="/f/t", stage=0, attempt="a"), identity=None
    )
    worker = GrpcSingleAttempt(
        lambda role: SimpleNamespace(transport=transport), lambda job, timeout: private
    )
    job = dict(
        id=1,
        channel="child",
        role="W",
        operation="init",
        payload=dict(node_path="/f/t", attempt_id="a", task_id="r"),
        credentials=dict(password="must-not-log"),
        reservation=dict(stage=0),
        path="/f/t",
    )
    result = worker.invoke(job, lambda: events.append("sent"), 10)
    assert len(calls) == 1 and result["proof"]["effective"]
    assert calls[0]["metadata"] == [("takler-pass", "must-not-log")]
    assert 0 < calls[0]["timeout"] <= 10
    transport.retry_window = 1
    with pytest.raises(ValueError, match="zero retry"):
        worker.invoke(job, lambda: None, 10)


def test_full_fixed_stream_uses_normal_query_engine_and_requires_cancel_ack(tmp_path):
    import asyncio
    import time
    from takler.server.scheduler import Scheduler
    from r3_14_perf04_fixture import make_fixture
    from r3_14_perf04_verify import stream_fixed_view

    bunch, *_ = make_fixture(1000)
    scheduler = Scheduler(bunch)

    class Local:
        single_timeout = 10

        def query(self, request):
            service = scheduler.query_service
            if request.kind == "full_snapshot_request":
                return asyncio.run(service.full_snapshots.page(request))
            if request.kind == "full_snapshot_chunk_request":
                return asyncio.run(service.full_snapshots.chunk(request))
            if request.kind == "cancel_full_snapshot_request":
                return asyncio.run(service.full_snapshots.cancel(request))
            return service.capabilities()

    rows = []
    checks = []
    result = stream_fixed_view(
        Local(), time.monotonic_ns() + 60 * NS, lambda: checks.append(1), rows.append
    )
    assert result["count"] == 1002 and result["cancelled"] and checks
    assert not scheduler.query_service.full_snapshots._leases
    assert (
        result["completion"]["state_version"]
        == scheduler.version_store.pin().state_version
    )
    assert len({row["path"] for row in rows}) == 1002
    with pytest.raises(TimeoutError):
        stream_fixed_view(Local(), 0, lambda: None, lambda row: None)


def test_l_installer_explicitly_disables_summary_D(monkeypatch):
    import r3_14_observation as observation
    import r3_14_perf08a_hooks as hooks
    from r3_14_perf04_verify import install_l_observation

    seen = []

    class Observation:
        def __init__(self, server, emitter, detailed, **kwargs):
            seen.append((detailed, kwargs))

        def install(self):
            return self

    monkeypatch.setattr(
        observation, "Emitter", lambda socket, capacity: ("emitter", capacity)
    )
    monkeypatch.setattr(observation, "ServiceObservation", Observation)
    monkeypatch.setattr(
        hooks,
        "install_checkpoint_boundaries",
        lambda obs: seen.append("checkpoint-only"),
    )
    install_l_observation(object(), "none")
    assert seen == [(False, dict(lag_mode="record")), "checkpoint-only"]


def test_engine_requires_explicit_safety_for_non_simulated_dispatcher():
    paths, _ = candidates(1000, False)
    with pytest.raises(ValueError, match="explicit PERF-04 guards"):
        Engine("S", 0, paths, {}, object(), lambda row: None)


def test_wrong_identity_handoff_does_not_consume_the_current_proof():
    from r3_14_perf04_verify import WriteVerifier, canonical

    verifier = object.__new__(WriteVerifier)
    value = dict(proof=dict(stage=1, attempt="current"), identity=None)
    verifier.proofs = {"/f/t": value}
    verifier.proof_bytes = len(canonical(value))
    with pytest.raises(ValueError):
        verifier.handoff(dict(path="/f/t", reservation=dict(stage=0, attempt="old")))
    assert verifier.proofs["/f/t"] == value
    assert (
        verifier.handoff(
            dict(path="/f/t", reservation=dict(stage=1, attempt="current"))
        )
        == value
    )
    assert verifier.proof_bytes == 0


@pytest.mark.parametrize("fault", [None, "setup", "verification"])
def test_session_wires_all_phases_and_never_starts_another_variant(fault):
    from types import SimpleNamespace
    from r3_14_perf04_session import Session

    clock = [0]
    events = []
    checks = Counter()

    class Guard:
        measurement_start_ns = None

        def check(self, **kwargs):
            checks["guard"] += 1

    class Registry:
        def cleanup(self, deadline):
            checks["cleanup"] += 1
            assert deadline <= 320 * NS
            return dict(live_registered=[])

        def summary(self):
            return dict(events=events)

    watchdog = SimpleNamespace(arm=lambda: events.append("watchdog"))
    session = Session(
        "S",
        Registry(),
        watchdog,
        Guard(),
        lambda row: None,
        clock=lambda: clock[0],
        sleep=lambda seconds: clock.__setitem__(0, clock[0] + int(seconds * NS)),
    )

    def setup(deadline, check):
        assert events == ["watchdog"]
        if fault == "setup":
            clock[0] = 61 * NS
            check()
        return "prepared"

    def verify(stage, deadline, check):
        events.append(stage)
        if stage == "after" and fault == "verification":
            clock[0] = deadline
            check()
        check()

    def factory(start, prepared):
        engine = make_engine("S")
        from r3_14_perf04 import Grid

        engine.grid = Grid(start, 60, 10, False)
        engine.ledger.start_ns = start
        return engine

    result = session.run(
        setup,
        factory,
        verify,
        lambda deadline, check, expected_stop: (expected_stop(), check()),
    )
    assert checks["cleanup"] == 1 and checks["guard"] > 0
    if fault:
        assert "TimeoutError" in result["exit_reason"]
    else:
        assert result["exit_reason"] == "completed"
        assert (
            result["workload"]["ledger"]["counts_closed"]
            if "counts_closed" in result["workload"]["ledger"]
            else False
        )
        assert (
            next(
                r["counts"]["offered"]
                for r in result["workload"]["ledger"]["dimensions"]
                if r["dimension"] == "role" and r["key"] == "W"
            )
            == 600
        )
        assert events == ["watchdog", "before", "after"]


def test_deadline_unknown_cannot_be_rewritten_by_a_late_success():
    engine = make_engine()
    engine.tick(engine.grid.start_ns)
    rid, (_, token) = next((i, v) for i, v in engine.requests.items() if v[1])
    # Consume only the transport boundary; drop the response.
    engine.dispatcher.heap = []
    engine.accept(dict(kind="sent", id=rid, at_ns=engine.grid.start_ns))
    engine.tick(engine.grid.start_ns + 10 * NS)
    assert engine.stopped and engine.pool.tasks[token.task].unknown
    before = engine.ledger.snapshot()
    engine.accept(
        dict(
            kind="completed",
            id=rid,
            at_ns=engine.grid.start_ns + 11 * NS,
            outcome="success",
        )
    )
    assert engine.ledger.snapshot() == before


def test_new_journal_full_buffer_fails_without_waiting_for_disk(tmp_path):
    import threading
    import time

    release = threading.Event()
    entered = threading.Event()

    class Stream:
        def __init__(self):
            self.target = (tmp_path / "blocked-journal").open("wb", buffering=0)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.target.close()
            return False

        def write(self, data):
            entered.set()
            assert release.wait(2)
            self.target.write(data)

        def fileno(self):
            return self.target.fileno()

    class Path:
        def open(self, *args, **kwargs):
            return Stream()

    journal = Journal(Path())
    try:
        for i in range(journal.capacity):
            journal.emit(dict(kind="bounded", id=i))
        assert entered.wait(1)
        started = time.monotonic()
        with pytest.raises(RuntimeError, match="critical journal loss"):
            journal.emit(dict(kind="overflow"))
        assert time.monotonic() - started < 0.5
    finally:
        release.set()
        final = journal.close(time.monotonic_ns() + 2 * NS)
    assert final["dropped"] == 1 and final["produced"] == 257


def test_role_event_overflow_is_terminal_before_any_business_request(monkeypatch):
    import queue
    import threading
    import r3_14_perf04_roles as roles

    class Output:
        def put_nowait(self, event):
            raise queue.Full

    class Factory:
        def __call__(self, role):
            return object()

    monkeypatch.setattr(roles.os, "setsid", lambda: None)
    stop = threading.Event()
    roles.role_worker("Q", queue.Queue(1), Output(), Factory(), stop)
    assert stop.is_set()
