"""PERF-04A state-channel regressions; never start a Takler service."""

import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS))
probe = importlib.import_module("r3_14_isolated_probe")
analyzer = importlib.import_module("r3_14_perf08a_analyze")
hooks = importlib.import_module("r3_14_perf08a_hooks")
health = importlib.import_module("r3_14_health")
worker = importlib.import_module("r3_14_isolated_worker")

SECOND = 1_000_000_000


@pytest.fixture
def simulated_run(tmp_path, monkeypatch):
    """Exercise the actual orchestrator with fake processes, clocks and events."""
    clock = SimpleNamespace(
        now=100.0, measurement=None, drain_start=None, drain_seconds=0
    )
    processes = []

    def no_process(*args, **kwargs):
        pytest.fail("a real subprocess is forbidden in health regressions")

    monkeypatch.setattr(probe.subprocess, "Popen", no_process)

    class Process:
        def __init__(self, role, args, env, run_dir):
            self.role, self.args, self.run_dir = role, args, run_dir
            self.pid = 1000 + len(processes)
            self.returncode = None
            self.proc = self
            processes.append(self)

        def receive(self, timeout):
            return dict(ready=True)

        def send(self, value):
            if "start_ns" in value:
                clock.measurement = value["start_ns"]
            if value.get("command") == "stop":
                if self.role == "server" and clock.drain_seconds:
                    clock.drain_start = clock.now
                else:
                    self.returncode = 0

        def clock(self, timeout=2, check=None):
            if check is not None:
                check()
            return dict(uncertainty_ns=0, offset_midpoint_ns=0)

        def poll(self):
            return self.returncode

        def wait(self, timeout):
            if self.role == "server" and clock.drain_start is not None:
                if clock.now - clock.drain_start < clock.drain_seconds:
                    clock.now += timeout
                    raise subprocess.TimeoutExpired(self.args, timeout)
            self.returncode = 0

        def cleanup(self, timeout):
            return dict(exit_code=self.returncode, simulated=True)

    def sleep(seconds):
        # One simulated second per loop; no real waiting.
        clock.now += 1

    monkeypatch.setattr(probe, "Process", Process)
    monkeypatch.setattr(
        probe,
        "time",
        SimpleNamespace(
            monotonic=lambda: clock.now,
            monotonic_ns=lambda: round(clock.now * SECOND),
            sleep=sleep,
        ),
    )
    monkeypatch.setattr(probe, "git", lambda *a: "simulated-source\n")
    monkeypatch.setattr(hooks, "arm_hard_deadline", lambda *a: None)
    monkeypatch.setattr(
        analyzer, "analyze", lambda p: dict(evidence_status="simulated, not a run")
    )
    source = tmp_path / "source"
    source.mkdir()
    for name, value in {
        "source-meta.json": '{"takler_head":"simulated-source"}',
        "source-index.json": "{}",
        "tooling.patch": "",
        "gc.patch": "",
    }.items():
        (source / name).write_text(value)

    def run(
        publish,
        *,
        measurement=60,
        withdrawal=120,
        whole=400,
        resource=None,
        drain_seconds=0,
    ):
        clock.drain_seconds = drain_seconds
        base = dict(
            task_id="PERF-08A",
            performance_runs=dict(
                startup_max_seconds=100,
                ready_baseline_no_requests_seconds=0,
                measurement_seconds=measurement,
                withdrawal_no_new_requests_seconds=withdrawal,
                whole_run_max_seconds=whole,
                drain_exit_max_seconds=max(10, drain_seconds + 1),
            ),
        )

        def safety(config, swap_start):
            if processes:
                run_dir = Path(config["runtime"]["run_dir"])
                publish(
                    run_dir / "collector-health.json",
                    dict(
                        run_id=run_dir.name,
                        collector_pid=processes[0].pid,
                        updated_ns=round(clock.now * SECOND),
                        last_progress_ns=round(clock.now * SECOND),
                        failure=None,
                    ),
                    clock,
                    processes,
                )
            return resource(clock) if resource else None

        monkeypatch.setattr(probe, "safety", safety)
        run_dir, _ = probe.run_one(SCRIPTS.parent, tmp_path, source, base, "D", "B", 1)
        assert not list(run_dir.glob("*-final.json"))
        return run_dir, json.loads((run_dir / "manifest.json").read_text()), clock

    return run


@pytest.mark.parametrize("broken", ["", '{"last_progress_ns":'])
@pytest.mark.parametrize("onset", [10, 70])
def test_bad_read_in_measurement_or_withdrawal_is_tool_failure(
    simulated_run, broken, onset
):
    def publish(path, state, clock, processes):
        elapsed = clock.now - (clock.measurement or round(clock.now * SECOND)) / SECOND
        path.write_text(broken if elapsed >= onset else json.dumps(state))

    _, manifest, clock = simulated_run(publish)
    assert "tool health unavailable" in manifest["exit_reason"]
    assert "60s without" not in manifest["exit_reason"]
    assert onset + 5 <= clock.now - clock.measurement / SECOND < onset + 7


def test_no_initial_health_is_bounded_tool_failure(simulated_run):
    _, manifest, clock = simulated_run(lambda *a: None)
    assert "tool health unavailable" in manifest["exit_reason"]
    assert clock.now - 100 <= 5


@pytest.fixture
def channel(tmp_path):
    clock = SimpleNamespace(now=100 * SECOND)
    path = tmp_path / "health.json"
    guard = health.HealthGuard(
        path, "current", 123, 90 * SECOND, clock=lambda: clock.now
    )
    guard.measurement_start_ns = clock.now

    def publish(**changes):
        state = dict(
            run_id="current",
            collector_pid=123,
            updated_ns=clock.now,
            last_progress_ns=0,
            failure=None,
        )
        state.update(changes)
        path.write_text(json.dumps(state))

    return clock, path, guard, publish


@pytest.mark.parametrize("broken", ["", "{", "null", "[]", "\udcff"])
def test_read_errors_preserve_last_valid_progress_and_recover(channel, broken):
    clock, path, guard, publish = channel
    publish(last_progress_ns=99 * SECOND)
    guard.check()
    last = guard.last_valid_state.copy()
    clock.now += SECOND
    path.write_bytes(broken.encode("utf-8", errors="surrogateescape"))
    guard.check()
    assert guard.last_progress_ns == 99 * SECOND
    assert guard.last_valid_state == last
    assert guard.trigger is None
    clock.now += SECOND
    guard.check()
    assert list(guard.errors.values()) == [
        dict(first_ns=101 * SECOND, last_ns=102 * SECOND)
    ]
    publish(last_progress_ns=98 * SECOND)
    guard.check()
    assert guard.unavailable_since_ns is None
    assert guard.last_progress_ns == 99 * SECOND
    assert guard.last_valid_state["last_progress_ns"] == 98 * SECOND


@pytest.mark.parametrize(
    "changes,category",
    [
        ({"run_id": "old"}, "wrong_run"),
        ({"collector_pid": 456}, "wrong_collector"),
        ({"updated_ns": 101 * SECOND}, "future_state"),
        ({"updated_ns": 89 * SECOND}, "invalid_state"),
        ({"updated_ns": 100.0}, "invalid_state"),
        ({"last_progress_ns": True}, "invalid_state"),
        ({"last_progress_ns": 101 * SECOND}, "invalid_state"),
        ({"failure": {}}, "invalid_state"),
        ({"failure": {"kind": []}}, "invalid_state"),
    ],
)
def test_untrusted_identity_or_timestamp_cannot_confirm_no_progress(
    channel, changes, category
):
    clock, _, guard, publish = channel
    guard.measurement_start_ns = 0
    publish(**changes)
    guard.check()
    assert category in guard.errors
    assert guard.last_valid_state is None and guard.last_progress_ns is None
    clock.now += 5 * SECOND
    with pytest.raises(RuntimeError, match="tool health unavailable"):
        # Keep the same untrusted document; no new evidence is introduced.
        guard.check()


def test_different_read_errors_share_one_unavailable_deadline(channel, monkeypatch):
    clock, path, guard, _ = channel
    guard.check()  # missing
    clock.now += SECOND
    path.write_text("{")
    guard.check()
    clock.now += SECOND
    path.write_bytes(b"x" * (health.STATE_LIMIT_BYTES + 1))
    guard.check()
    clock.now += SECOND
    monkeypatch.setattr(Path, "open", lambda *a, **kw: (_ for _ in ()).throw(OSError()))
    guard.check()
    clock.now += 2 * SECOND
    with pytest.raises(RuntimeError, match="tool health unavailable: read_error"):
        guard.check()
    assert set(guard.errors) == {"missing", "invalid_json", "oversize", "read_error"}
    assert guard.trigger["at_ns"] == 105 * SECOND
    assert guard.diagnostics()["last_valid_state"] is None


def test_no_initial_state_recovery_at_five_seconds_cannot_extend_budget(channel):
    clock, _, guard, publish = channel
    guard.check()
    clock.now += 5 * SECOND
    publish(last_progress_ns=clock.now)
    with pytest.raises(
        RuntimeError, match="tool health unavailable: recovery after deadline"
    ):
        guard.check()


def test_stopped_heartbeat_expires_five_seconds_after_last_update(channel):
    clock, _, guard, publish = channel
    publish(last_progress_ns=99 * SECOND)
    guard.check()
    clock.now += 5 * SECOND - 1
    guard.check()
    assert guard.trigger is None
    clock.now += 1
    with pytest.raises(RuntimeError, match="tool health unavailable: stale_state"):
        guard.check()
    assert guard.last_progress_ns == 99 * SECOND
    assert guard.unavailable_since_ns == 100 * SECOND


def test_out_of_order_heartbeat_does_not_replace_last_valid_state(channel):
    clock, _, guard, publish = channel
    publish(last_progress_ns=99 * SECOND)
    guard.check()
    clock.now += SECOND
    publish(updated_ns=99 * SECOND)
    guard.check()
    assert "out_of_order_state" in guard.errors
    assert guard.last_valid_state["updated_ns"] == 100 * SECOND
    assert guard.last_progress_ns == 99 * SECOND


def test_transient_read_failure_does_not_refresh_real_progress_timeout(channel):
    clock, path, guard, publish = channel
    publish()
    guard.check()
    for second in range(1, 60):
        clock.now = (100 + second) * SECOND
        publish()
        guard.check()
    clock.now = 160 * SECOND
    path.write_text("{")
    guard.check()  # Unavailable evidence cannot assert product failure.
    assert guard.trigger is None and guard.last_progress_ns == 0
    clock.now += SECOND
    publish()
    with pytest.raises(RuntimeError, match="60s without completed request"):
        guard.check()  # Recovery did not invent progress or restart its timer.


def test_actual_orchestrator_does_not_keep_rereading_stopped_heartbeat(simulated_run):
    def publish(path, state, clock, processes):
        if not path.exists():
            path.write_text(json.dumps(state))

    _, manifest, clock = simulated_run(publish)
    assert "tool health unavailable: stale_state" in manifest["exit_reason"]
    assert clock.now == 105


def test_continuously_fresh_idle_health_stops_at_exact_sixty_seconds(channel):
    clock, _, guard, publish = channel
    for second in range(60):
        clock.now = (100 + second) * SECOND
        publish()
        guard.check()
    clock.now = 100 * SECOND + 59_999_000_000
    publish()
    guard.check()
    clock.now = 160 * SECOND
    publish()
    with pytest.raises(RuntimeError, match="60s without completed request"):
        guard.check()
    assert guard.trigger["at_ns"] == 160 * SECOND


@pytest.mark.parametrize("measurement", [30, 120])
def test_measurement_and_withdrawal_use_same_real_no_progress_guard(
    simulated_run, measurement
):
    def publish(path, state, clock, processes):
        state["last_progress_ns"] = 0
        path.write_text(json.dumps(state))

    run_dir, manifest, clock = simulated_run(publish, measurement=measurement)
    assert "60s without completed request" in manifest["exit_reason"]
    assert 60 <= clock.now - clock.measurement / SECOND < 61
    diagnostics = json.loads(
        (run_dir / "collector-health-diagnostics.json").read_text()
    )
    assert diagnostics["trigger"]["at_ns"] == round(clock.now * SECOND)
    assert diagnostics["errors"] == {}


@pytest.mark.parametrize("stage", ["startup", "measurement", "withdrawal"])
def test_collector_exit_is_immediate_tool_failure_even_with_fresh_health(
    simulated_run, stage
):
    def publish(path, state, clock, processes):
        path.write_text(json.dumps(state))
        elapsed = clock.now - (clock.measurement or round(clock.now * SECOND)) / SECOND
        if stage == "startup" or elapsed >= (10 if stage == "measurement" else 70):
            processes[0].returncode = 0

    run_dir, manifest, _ = simulated_run(publish)
    assert "tool health unavailable: collector_exit" in manifest["exit_reason"]
    diagnostics = json.loads(
        (run_dir / "collector-health-diagnostics.json").read_text()
    )
    assert "collector_exit" in diagnostics["errors"]


def sample_event(kind, at_ns, values):
    return dict(kind=kind, at_ns=at_ns, values=values, identity="sample")


@pytest.mark.parametrize(
    "kind,values,progress",
    [
        ("request", [1, 2, 0, 0], True),
        ("request_sent", [1, 2, 0, 0], False),
        ("checkpoint_publish", [1, 1, 1, 0], True),
        ("checkpoint_publish", [1, 1, 1, 1], False),
        ("checkpoint_publish", [1, 1, 0, 0], False),
        ("checkpoint_outcome", [1, 1, 1, 0], False),
        ("checkpoint_exit_total", [1, 0, 0, 0], False),
    ],
)
def test_only_completed_requests_and_real_periodic_publication_advance_progress(
    channel, kind, values, progress
):
    clock, path, guard, _ = channel
    state = dict(last_progress_ns=0, failure=None)
    for second in range(120):
        clock.now = (100 + second) * SECOND
        health.update_health(state, sample_event(kind, clock.now, values))
        health.publish_health(path, state, "current", 123, clock.now)
        if second == 60 and not progress:
            with pytest.raises(RuntimeError, match="60s without completed request"):
                guard.check()
            break
        guard.check()
    if progress:
        assert guard.trigger is None
        assert guard.last_progress_ns == clock.now


def test_collector_progress_is_monotonic_across_reordered_events():
    state = dict(last_progress_ns=0, failure=None)
    for at_ns in [200, 300, 150, 299]:
        health.update_health(state, sample_event("request", at_ns, [1, 2, 0, 0]))
    assert state["last_progress_ns"] == 300


@pytest.mark.parametrize(
    "kind,values",
    [
        ("request", [1, 2, 1, 0]),
        ("checkpoint_outcome", [1, 0, 0, 0]),
        ("watchdog", [0] * 4),
    ],
)
def test_explicit_failure_precedes_no_progress_and_unavailable_deadline(
    channel, kind, values
):
    clock, path, guard, _ = channel
    guard.measurement_start_ns = 0  # The product timeout is also eligible.
    guard.check()  # Begin an unavailable period.
    clock.now += 5 * SECOND
    state = dict(last_progress_ns=0, failure=None)
    health.update_health(state, sample_event(kind, clock.now, values))
    health.publish_health(path, state, "current", 123, clock.now)
    with pytest.raises(RuntimeError, match="collector detected failure"):
        guard.check()
    assert kind in guard.trigger["reason"]


@pytest.mark.parametrize("stop", ["resource", "hard_limit"])
def test_resource_and_whole_run_limit_precede_broken_health(simulated_run, stop):
    def publish(path, state, clock, processes):
        path.write_text("{")

    run_dir, manifest, _ = simulated_run(
        publish,
        whole=5,
        resource=(
            lambda clock: "simulated resource limit" if clock.now >= 105 else None
        )
        if stop == "resource"
        else None,
    )
    expected = (
        "simulated resource limit" if stop == "resource" else "whole run hard limit"
    )
    assert expected in manifest["exit_reason"]
    diagnostics = json.loads(
        (run_dir / "collector-health-diagnostics.json").read_text()
    )
    assert expected in diagnostics["trigger"]["reason"]


def test_bounded_error_records_do_not_accumulate_payloads(channel):
    clock, path, guard, _ = channel
    # Many intermittent errors; records are per fixed category, never per poll.
    for _ in range(1000):
        path.write_text("{")
        guard.check()
        clock.now += 1
    assert len(guard.errors) == 1
    assert len(json.dumps(guard.diagnostics())) < 1024


@pytest.mark.parametrize("failure", [False, True])
def test_whole_limit_precedes_simultaneously_eligible_product_stop(
    simulated_run, failure
):
    def publish(path, state, clock, processes):
        state["last_progress_ns"] = 0
        if failure and clock.now >= 161:
            state["failure"] = dict(
                kind="request", at_ns=state["updated_ns"], identity="sample"
            )
        path.write_text(json.dumps(state))

    _, manifest, _ = simulated_run(publish, whole=61)
    assert "whole run hard limit" in manifest["exit_reason"]


@pytest.mark.parametrize("broken", [False, True])
def test_shutdown_wait_keeps_availability_guard_without_extending_product_guard(
    simulated_run, broken
):
    def publish(path, state, clock, processes):
        if clock.drain_start is not None:
            state["last_progress_ns"] = round(clock.drain_start * SECOND)
        path.write_text(
            "{" if broken and clock.drain_start is not None else json.dumps(state)
        )

    _, manifest, clock = simulated_run(
        publish, measurement=10, withdrawal=0, drain_seconds=70
    )
    if broken:
        assert "tool health unavailable" in manifest["exit_reason"]
        assert 5 <= clock.now - clock.drain_start < 5.3
    else:
        assert manifest["exit_reason"] == "completed"
        assert clock.now - clock.drain_start >= 70


def test_control_handshake_checks_health_while_waiting(channel, monkeypatch):
    clock, path, guard, _ = channel
    path.write_text("{")
    process = object.__new__(probe.Process)
    process.role = "simulated"
    process.proc = SimpleNamespace(stdout=object())
    process.send = lambda value: None
    monkeypatch.setattr(
        probe,
        "time",
        SimpleNamespace(
            monotonic=lambda: clock.now / SECOND, monotonic_ns=lambda: clock.now
        ),
    )

    def select(*args):
        clock.now += round(args[3] * SECOND)
        return [], [], []

    monkeypatch.setattr(probe.select, "select", select)
    with pytest.raises(RuntimeError, match="tool health unavailable"):
        process.clock(timeout=10, check=guard.check)
    assert 105 * SECOND <= clock.now < 105_200_000_000


def test_control_handshake_keeps_its_own_timeout_with_fresh_health(
    channel, monkeypatch
):
    clock, _, guard, publish = channel
    process = object.__new__(probe.Process)
    process.role = "simulated"
    process.proc = SimpleNamespace(stdout=object())
    process.send = lambda value: None
    monkeypatch.setattr(
        probe,
        "time",
        SimpleNamespace(
            monotonic=lambda: clock.now / SECOND, monotonic_ns=lambda: clock.now
        ),
    )

    def check():
        publish(last_progress_ns=clock.now)
        guard.check()

    def select(*args):
        clock.now += round(args[3] * SECOND)
        return [], [], []

    monkeypatch.setattr(probe.select, "select", select)
    with pytest.raises(TimeoutError, match="control timed out"):
        process.clock(timeout=2, check=check)
    assert clock.now == 102 * SECOND
    assert guard.trigger is None


def atomic_publication_case(folder):
    """Child-process case: its parent enforces a 20s wall deadline."""
    path = folder / "health.json"
    state = dict(last_progress_ns=1, failure=None)
    health.publish_health(path, state, "current", 123, 2)
    old = path.read_bytes()
    partial, release = threading.Event(), threading.Event()
    errors = []

    def dump(value, stream):
        raw = json.dumps(value)
        stream.write(raw[:10])
        stream.flush()
        partial.set()
        assert release.wait(2)
        stream.write(raw[10:])

    original = health.json.dump
    health.json.dump = dump

    def writer():
        try:
            health.publish_health(path, state, "current", 123, 3)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=writer, daemon=True)
    try:
        thread.start()
        assert partial.wait(2)
        for _ in range(100):
            assert path.read_bytes() == old
            assert json.loads(path.read_bytes())["updated_ns"] == 2
        release.set()
        thread.join(2)
        assert not thread.is_alive() and not errors, errors
        assert json.loads(path.read_bytes())["updated_ns"] == 3
        assert list(folder.iterdir()) == [path]
    finally:
        release.set()
        thread.join(2)
        health.json.dump = original


def test_concurrent_atomic_publication_has_twenty_second_wall_limit(tmp_path):
    result = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--atomic-case", str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_failed_atomic_replace_preserves_file_and_removes_only_own_temporary(
    channel, monkeypatch
):
    _, path, _, publish = channel
    publish()
    before = path.read_bytes()
    other = path.parent / ".unrelated"
    other.write_text("keep")
    monkeypatch.setattr(
        health.os,
        "replace",
        lambda *a: (_ for _ in ()).throw(OSError("replace failed")),
    )
    with pytest.raises(OSError, match="replace failed"):
        health.publish_health(
            path, dict(last_progress_ns=0, failure=None), "current", 123, 1
        )
    assert path.read_bytes() == before and other.read_text() == "keep"
    assert set(path.parent.iterdir()) == {path, other}


def test_collector_wires_event_progress_and_atomic_identity_to_health(
    tmp_path, monkeypatch
):
    (tmp_path / "config.json").write_text("{}")
    events = [
        sample_event("request", 99 * SECOND, [1, 2, 0, 0]),
        sample_event("request", 98 * SECOND, [2, 2, 0, 0]),
        sample_event("checkpoint_publish", 100 * SECOND, [1, 1, 1, 1]),
    ]
    for event in events:
        event["pid"] = 1

    class Socket:
        packet = True

        def setsockopt(self, *a):
            pass

        def getsockopt(self, *a):
            return 65536

        def bind(self, *a):
            pass

        def setblocking(self, *a):
            pass

        def recv(self, *a):
            if self.packet:
                self.packet = False
                return b"simulated events"
            raise BlockingIOError

        def close(self):
            pass

    sock = Socket()
    monkeypatch.setattr(worker.socket, "socket", lambda *a: sock)
    stdin = SimpleNamespace(fileno=lambda: 0)
    monkeypatch.setattr(worker, "sys", SimpleNamespace(stdin=stdin))
    calls = iter([([sock], [], []), ([stdin], [], [])])
    monkeypatch.setattr(worker.select, "select", lambda *a: next(calls))
    monkeypatch.setattr(worker, "read_commands", lambda *a: [dict(command="stop")])
    monkeypatch.setattr(worker, "decode_packet", lambda *a: events)
    monkeypatch.setattr(worker, "proc_sample", lambda *a: dict(rss_bytes=0))
    monkeypatch.setattr(
        worker,
        "time",
        SimpleNamespace(monotonic=lambda: 100, monotonic_ns=lambda: 100 * SECOND),
    )
    worker.collector(
        SimpleNamespace(run_dir=str(tmp_path), socket="unused", perf08a=True)
    )
    state = json.loads((tmp_path / "collector-health.json").read_text())
    assert state["last_progress_ns"] == 99 * SECOND
    assert state["updated_ns"] == 100 * SECOND
    assert state["run_id"] == tmp_path.name and state["collector_pid"] == os.getpid()
    assert not list(tmp_path.glob(".collector-health.json.*"))


if __name__ == "__main__":
    assert sys.argv[1] == "--atomic-case"
    atomic_publication_case(Path(sys.argv[2]))
