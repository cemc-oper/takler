"""S closure regressions; no Takler listener or external job is started."""

import json
from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))


def test_complete_record_uses_bounded_fragments():
    from r3_14_perf04_verify import record_fragments, RecordAssembler

    record = dict(
        path="/flow000", child_paths=[f"/flow000/task{i:04d}" for i in range(1000)]
    )
    assert len(json.dumps(record).encode()) > 16384
    rows = list(record_fragments(7, 0, record))
    assert len(rows) > 1 and max(len(json.dumps(r).encode()) for r in rows) < 16384
    receiver = RecordAssembler()
    actual = None
    for row in rows:
        value = receiver.accept(row)
        if value is not None:
            actual = value
    assert actual == record
    receiver.finish(1)


def test_engine_passes_absolute_rpc_deadline():
    from test_r3_14_perf04_runtime import make_engine

    engine = make_engine("S")
    jobs = []
    engine.dispatcher.submit = lambda job: jobs.append(job) or True
    engine.phase_deadline_ns = engine.grid.end_ns + 30_000_000_000
    engine.tick(engine.grid.start_ns)
    assert jobs and all("rpc_deadline_ns" in j for j in jobs)
    assert all(j["rpc_deadline_ns"] <= engine.phase_deadline_ns for j in jobs)


def test_absolute_budget_spans_rpc_and_proof_and_meter_encoding():
    from types import SimpleNamespace
    from takler.protocol.commands import ServiceResponse
    from takler.server.protocol import adapter
    from r3_14_perf04_roles import GrpcSingleAttempt, execute
    from r3_14_perf04 import NS

    now = [100 * NS]
    rows = []

    def rpc(request, **kwargs):
        assert request.meter_value == "25" and kwargs["timeout"] <= 1
        now[0] += 600_000_000
        return adapter.response_to_pb2(ServiceResponse())

    def handoff(job, left):
        assert left <= 0.4 and job["rpc_deadline_ns"] == 101 * NS
        now[0] += 500_000_000
        return dict(proof=dict(effective=True))

    names = (
        "GetCapabilities",
        "ReadPage",
        "ReadSince",
        "ReadDetail",
        "ReadDetailChunk",
        "ReadFullSnapshot",
        "ReadFullSnapshotChunk",
        "CancelFullSnapshot",
    )
    transport = SimpleNamespace(
        retry_window=0,
        single_timeout=10,
        stub=SimpleNamespace(RunCommandMeter=rpc),
        query_stub=SimpleNamespace(**{n: rpc for n in names}),
        _call=lambda *a: None,
        query=lambda *a: None,
    )
    worker = GrpcSingleAttempt(
        lambda r: SimpleNamespace(transport=transport),
        handoff,
        rows.append,
        clock=lambda: now[0],
    )
    job = dict(
        id=8,
        role="W",
        channel="child",
        operation="meter",
        expires_ns=101 * NS,
        rpc_deadline_ns=101 * NS,
        payload=dict(
            node_path="/f/t",
            attempt_id="a",
            meter_name="progress",
            meter_value=25,
            source_task_path="/f/t",
        ),
        credentials=dict(password="private"),
    )
    execute(job, worker, rows.append, lambda: now[0])
    assert [r["kind"] for r in rows] == ["sent", "rpc", "rpc_result", "completed"]
    assert rows[-1]["outcome"] == "timeout"
    assert (
        rows[1]["outcome"] == "returned" and rows[1]["at_ns"] == 100 * NS + 600_000_000
    )
    assert not any(r["kind"] == "proof_completed" for r in rows)


def test_checkpoint_archive_failure_closes_current_and_queued_fds(tmp_path):
    import os
    import time
    import pytest
    from r3_14_perf04_verify import CheckpointAudit

    source = tmp_path / "source"
    source.write_bytes(b"actual checkpoint bytes")
    # Existing archive target causes O_EXCL failure. No fake os.close counters.
    (tmp_path / "checkpoint-0001.check").write_bytes(b"keep original")
    audit = CheckpointAudit(None, tmp_path, lambda row: None)
    fds = [os.open(source, os.O_RDONLY) for _ in range(3)]
    with audit.fd_lock:
        for seq, fd in enumerate(fds, 1):
            audit.jobs.put_nowait((fd, seq, False))
    audit.thread.join(2)
    assert not audit.thread.is_alive() and audit.failure
    for fd in fds:
        with pytest.raises(OSError):
            os.fstat(fd)
    with pytest.raises(RuntimeError):
        audit.close(time.monotonic_ns() + 1_000_000_000)
    assert audit.release()["retained_main_backup"] == 0
    assert (tmp_path / "checkpoint-0001.check").read_bytes() == b"keep original"


def test_fragment_duplicate_missing_and_unknown_ownership_fail_closed():
    import pytest
    from r3_14_perf04_verify import record_fragments, RecordAssembler
    from r3_14_perf04_s import s_joint

    parts = list(record_fragments(1, 0, dict(large="x" * 20000)))
    receiver = RecordAssembler()
    receiver.accept(parts[0])
    with pytest.raises(ValueError):
        receiver.accept(parts[0])
    with pytest.raises(ValueError):
        receiver.finish(1)
    assert not s_joint(dict(exit_reason="completed"))["tool_conjunction_pass"]
    assert "necessary ownership unknown" in s_joint({})["problems"]


def run_closure_case(folder, fault=None):
    """Each auxiliary regression gets 27s plus <=2s owned emergency cleanup."""
    import os
    import signal
    import subprocess
    import time

    folder.mkdir()
    args = [
        sys.executable,
        str(Path(__file__).resolve().parents[2] / "scripts/r3_14_perf04_s.py"),
        "--memory-only",
        "--output",
        str(folder / "case"),
    ]
    if fault:
        args += ["--fault", fault]
    started = time.monotonic()
    started_ns = time.monotonic_ns()
    with (folder / "stdout.log").open("w") as stream:
        proc = subprocess.Popen(
            args, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True
        )
        proc_start_ticks = (
            Path(f"/proc/{proc.pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
        )
        timed_out = False
        try:
            code = proc.wait(timeout=27)
        except subprocess.TimeoutExpired:
            timed_out = True
            code = None
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                code = proc.wait(timeout=1)
                registry = folder / "case/processes.json"
                if registry.exists():
                    for e in json.loads(registry.read_text())["entries"]:
                        stat = Path(f"/proc/{e['pid']}/stat")
                        if (
                            stat.exists()
                            and stat.read_text().rsplit(")", 1)[1].split()[19]
                            == e["proc_start_ticks"]
                        ):
                            try:
                                os.killpg(e["pid"], signal.SIGKILL)
                            except ProcessLookupError:
                                pass
        elapsed = time.monotonic() - started
    (folder / "parent-result.json").write_text(
        json.dumps(
            dict(
                pid=proc.pid,
                proc_start_ticks=proc_start_ticks,
                started_ns=started_ns,
                ended_ns=time.monotonic_ns(),
                exit_code=code,
                timed_out=timed_out,
                wall_seconds=elapsed,
                args=args,
            )
        )
    )
    assert elapsed < 30 and code == 0 and not timed_out, (
        folder / "stdout.log"
    ).read_text()[-5000:]
    return json.loads((folder / "case/result.json").read_text())


@pytest.mark.parametrize("fault", [None, "reordered"])
def test_s_same_entry_normal_and_recomputable_evidence_under_30s(tmp_path, fault):
    result = run_closure_case(tmp_path / "normal", fault)
    assert result["exit_reason"] == "completed"
    child = next(
        r["counts"]
        for r in result["workload"]["ledger"]["dimensions"]
        if r["dimension"] == "role" and r["key"] == "W"
    )
    assert child["offered"] == child["sent"] == child["effective"] == 600
    assert result["joint"]["tool_conjunction_pass"], result["joint"]
    assert (
        result["registry"]["service_launches"] == 0
        and not result["registry"]["live_registered"]
    )
    events = [
        json.loads(line) for line in (tmp_path / "normal/case/events.jsonl").open()
    ]
    assert (
        max(
            len(json.dumps(e).encode()) for e in events if e.get("kind") == "role_event"
        )
        < 16384
    )
    assert any(
        e.get("kind") == "role_event"
        and e["event"]["kind"] == "verification_fragment"
        and e["event"]["offset"] > 0
        for e in events
    )
    assert not any(e.get("channel") in ("batch", "structure") for e in events)
    assert {
        e.get("operation")
        for e in events
        if e.get("role") == "R" and e.get("kind") == "completed"
    } == {"requeue", "run"}


@pytest.mark.parametrize(
    "fault",
    [
        "expired",
        "busy",
        "child_stall",
        "health_loss",
        "collector_exit",
        "identity_race",
        "missing_final",
        "missing_handshake",
        "fragment_loss",
        "event_loss",
        "stage_deadline",
        "save_failure",
        "cleanup_unknown",
        "ownership_unknown",
        "unknown_late",
        "aggregate_stall",
    ],
)
def test_s_same_entry_faults_never_pass_or_invent_service_final(tmp_path, fault):
    result = run_closure_case(tmp_path / fault, fault)
    assert not result["joint"]["tool_conjunction_pass"]
    assert not result["joint"]["service_S_pass"]
    assert not result["registry"]["live_registered"]
    assert result["registry"]["service_launches"] == 0
    triggers = {
        "expired": "10s without effective child",
        "busy": "10s without effective child",
        "child_stall": "10s without effective child",
        "health_loss": "tool health unavailable",
        "collector_exit": "collector_exit",
        "identity_race": "request unknown",
        "missing_handshake": "role handshake missing",
        "fragment_loss": "full verification business failure",
        "event_loss": "critical role sequence loss",
        "stage_deadline": "setup stage deadline",
        "save_failure": "periodic save failed",
        "unknown_late": "request timeout",
        "aggregate_stall": "60s without completed request",
    }
    if fault in triggers:
        assert triggers[fault] in result["exit_reason"], result
    else:
        assert result["exit_reason"] == "completed", result
    if fault == "cleanup_unknown":
        assert result["cleanup_tail"] == "unknown"
    if fault == "ownership_unknown":
        assert not result["ownership"]["known"]
    if fault == "missing_final":
        assert "Q producer final unknown" in result["joint"]["problems"]
    if fault in ("identity_race", "unknown_late"):
        events = [
            json.loads(line) for line in (tmp_path / fault / "case/events.jsonl").open()
        ]
        if fault == "identity_race":
            assert any(
                r.get("kind") == "role_event"
                and r["event"].get("business_code") == "stale_identity"
                for r in events
            )
        else:
            assert any(
                r.get("kind") == "fault_late_callback_checked"
                and r["unknown_preserved"]
                for r in events
            )


def role_archive_case(folder):
    import time
    from test_r3_14_perf04_safety import dummy_factory
    from r3_14_perf04_roles import ProcessRoles
    from r3_14_perf04_safety import Registry, Watchdog, Lifecycle
    from r3_14_perf04_ledger import Journal, audit_producer

    journal = Journal(folder / "events.jsonl")
    registry = Registry(folder / "registry.json")
    start = time.monotonic_ns()
    watchdog = Watchdog(registry, start + 18_000_000_000)
    roles = ProcessRoles(dummy_factory, registry, watchdog, event_sink=journal.emit)
    lifecycle = Lifecycle("S", start)
    try:
        clocks = {
            stage: roles.clock_handshake(stage, lifecycle, lambda: None)
            for stage in ("start", "exit")
        }
        roles.request_stop()
        limit = time.monotonic() + 3
        while len(roles.finals) < 4 and time.monotonic() < limit:
            roles.poll()
            registry.reap()
            time.sleep(0.005)
        assert not roles.audit()["problems"]
    finally:
        watchdog.disarm(time.monotonic_ns() + 500_000_000)
        registry.cleanup(time.monotonic_ns() + 1_000_000_000)
        roles.close_queues()
        final = journal.close(time.monotonic_ns() + 1_000_000_000)
    data = [json.loads(line) for line in (folder / "events.jsonl").open()]
    assert audit_producer(folder / "events.jsonl", final)["tail"] == "known"
    for role in "WRCQ":
        events = [
            r["event"]
            for r in data
            if r["kind"] == "role_event" and r["event"]["role"] == role
        ]
        assert [e["sequence"] for e in events] == [1, 2, 3, 4]
        assert [e["kind"] for e in events] == [
            "role_ready",
            "clock",
            "clock",
            "role_final",
        ]
    (folder / "result.json").write_text(
        json.dumps(dict(registry=registry.summary(), clocks=clocks, journal=final))
    )


def test_actual_process_role_archive_contains_ready_both_clocks_and_final(tmp_path):
    import subprocess

    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--role-archive",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr


if __name__ == "__main__":
    assert sys.argv[1] == "--role-archive"
    role_archive_case(Path(sys.argv[2]))


def test_private_handoff_uses_one_deadline_across_all_socket_waits(monkeypatch):
    import socket
    from r3_14_perf04_verify import HandoffClient

    now = [0]
    waits = []
    chunks = iter([b'{"proof":', b'{"effective":true}}\n'])

    class SlowSocket:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def settimeout(self, value):
            waits.append(value)

        def connect(self, path):
            now[0] += 50_000_000

        def sendall(self, raw):
            now[0] += 50_000_000

        def recv(self, size):
            now[0] += 500_000_000
            return next(chunks)

    monkeypatch.setattr(socket, "socket", lambda *a: SlowSocket())
    client = HandoffClient("/unused", clock=lambda: now[0])
    with pytest.raises(TimeoutError, match="absolute deadline"):
        client(dict(path="/f/t", rpc_deadline_ns=1_000_000_000), 10)
    assert waits == sorted(waits, reverse=True) and waits[0] == 1 and waits[-1] <= 0.4


def test_l_receipts_accept_reentrant_order_but_reject_loss_duplicates_and_overflow():
    from r3_14_perf04_ledger import SequenceWindow

    window = SequenceWindow(4)
    for sequence in (3, 1, 2):
        window.accept(sequence)
    assert window.final(3)["known"]
    with pytest.raises(RuntimeError, match="duplicate"):
        window.accept(2)
    window = SequenceWindow(4)
    for sequence in (3, 1):
        window.accept(sequence)
    assert not window.final(3)["known"]
    with pytest.raises(RuntimeError, match="window"):
        window.accept(6)


def test_collector_preserves_every_l_record_in_bounded_journal_batches():
    from types import SimpleNamespace
    from r3_14_observation import RECORD, decode_packet
    from r3_14_perf04_s import SClosure
    from r3_14_perf04_ledger import SequenceWindow

    raw = b"".join(
        RECORD.pack(i, 1, 1, 1, 1, 0, 0, *([0] * 8), b"x" * 128) for i in range(1, 129)
    )
    rows = []
    owner = SimpleNamespace(
        now=1,
        observation_window=SequenceWindow(),
        observation_received=0,
        socket=SimpleNamespace(recv=lambda size: raw),
        record=rows.append,
        health=SimpleNamespace(accept_legacy=lambda e: None),
    )
    assert SClosure.drain_observation(owner)
    assert len(rows) < 16
    assert all(len(json.dumps(row).encode()) < 16384 for row in rows)
    assert [event for row in rows for event in row["events"]] == list(
        decode_packet(raw)
    )
    assert owner.observation_window.final(128)["known"]


def test_streamed_verification_must_link_to_the_same_fixed_root():
    from r3_14_perf04_s import verify_stream_link

    view = dict(state_version=8, nodes={"/": {}})
    reply = dict(count=1, completion=dict(state_version=8, total_nodes=1))
    verify_stream_link(reply, view)
    reply["completion"]["state_version"] = 9
    with pytest.raises(ValueError, match="fixed root"):
        verify_stream_link(reply, view)
