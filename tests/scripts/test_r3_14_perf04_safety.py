"""PERF-04 identity/guard wiring and bounded owned helper lifecycle regressions."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from r3_14_perf04 import NS
from r3_14_perf04_safety import (
    Guard,
    Perf04Health,
    Lifecycle,
    BatchBudget,
    Registry,
    Watchdog,
    resource_reason,
    GIB,
)


def safe_resources():
    return dict(
        mem_available=8 * GIB,
        swap_used=0,
        disk_free=30 * GIB,
        tool_rss=10**6,
        evidence_bytes=100,
    )


def guarded(tmp_path):
    clock = SimpleNamespace(now=100 * NS)
    path = tmp_path / "health.json"
    health = Perf04Health(path, "PERF-04-test", 123)
    guard = Guard(
        path,
        "PERF-04-test",
        123,
        clock.now,
        lambda: None,
        safe_resources,
        clock=lambda: clock.now,
    )
    guard.measurement_start_ns = clock.now
    return clock, health, guard


@pytest.mark.parametrize(
    "fault", ["missing", "truncated", "wrong_run", "stopped", "collector_exit"]
)
def test_explicit_perf04_guard_tool_loss_never_becomes_product_stall(tmp_path, fault):
    clock, health, guard = guarded(tmp_path)
    if fault == "collector_exit":
        health.publish(clock.now)
        guard.collector_poll = lambda: 0
        with pytest.raises(RuntimeError, match="collector_exit"):
            guard.check()
        return
    if fault == "stopped":
        health.publish(clock.now)
        guard.check()
        clock.now += 5 * NS
    elif fault == "truncated":
        health.path.write_text("{")
    elif fault == "wrong_run":
        health.publish(clock.now)
        row = json.loads(health.path.read_text())
        row["run_id"] = "PERF-08A-old"
        health.path.write_text(json.dumps(row))
    if fault != "stopped":
        guard.check()
        clock.now += 5 * NS
    with pytest.raises(RuntimeError, match="tool health unavailable"):
        guard.check()
    assert guard.health.trigger and "60s" not in guard.health.trigger["reason"]


def test_q_and_checkpoint_progress_cannot_hide_child_starvation(tmp_path):
    clock, health, guard = guarded(tmp_path)
    start = clock.now
    for second in range(10):
        clock.now = start + second * NS
        health.accept(
            dict(kind="completed", id=1, at_ns=clock.now, outcome="success", role="Q")
        )
        health.accept(
            dict(
                kind="checkpoint_publish",
                at_ns=clock.now,
                success=True,
                published=True,
                exit_save=False,
            )
        )
        health.publish(clock.now)
        guard.check(last_child_ns=start, offered=100, measuring=True)
    clock.now = start + 10 * NS
    health.publish(clock.now)
    with pytest.raises(RuntimeError, match="10s without effective child"):
        guard.check(last_child_ns=start, offered=100, measuring=True)


def test_aggregate_sixty_second_boundary_and_exit_save_semantics(tmp_path):
    clock, health, guard = guarded(tmp_path)
    start = clock.now
    for delta in [59_999_000_000, 60 * NS]:
        clock.now = start + delta
        health.accept(
            dict(
                kind="checkpoint_publish",
                at_ns=clock.now,
                success=True,
                published=True,
                exit_save=True,
            )
        )
        health.publish(clock.now)
        if delta < 60 * NS:
            guard.check(measuring=False)
        else:
            with pytest.raises(RuntimeError, match="60s without"):
                guard.check(measuring=False)
    assert health.state["last_progress_ns"] == 0


@pytest.mark.parametrize(
    "key,value",
    [
        ("mem_available", 4 * GIB - 1),
        ("swap_used", GIB + 1),
        ("disk_free", 20 * GIB - 1),
        ("tool_rss", 512 * 1024**2 + 1),
        ("evidence_bytes", GIB + 1),
        ("fixed_version_bytes", 768 * 1024**2 + 1),
        ("snapshot_extra_bytes", 1536 * 1024**2 + 1),
        ("stream_client_extra_bytes", 128 * 1024**2 + 1),
    ],
)
def test_each_exact_resource_stop_line(key, value, tmp_path):
    clock, health, guard = guarded(tmp_path)
    sample = safe_resources()
    sample[key] = value
    assert resource_reason(sample, 0)
    guard.resources = lambda: sample
    with pytest.raises(RuntimeError):
        guard.check()


def test_resource_boundary_equal_values_do_not_use_greater_or_equal():
    sample = dict(
        mem_available=4 * GIB,
        swap_used=GIB,
        disk_free=20 * GIB,
        tool_rss=512 * 1024**2,
        evidence_bytes=GIB,
    )
    assert resource_reason(sample, 0) is None


@pytest.mark.parametrize("variant,whole", [("S", 320), ("V", 440)])
def test_stage_waits_whole_limits_and_cleanup_reserve(variant, whole):
    clock = [0]
    lifecycle = Lifecycle(variant, 0, clock=lambda: clock[0])
    checks = []

    def guard():
        checks.append(clock[0])

    def sleep(seconds):
        clock[0] += int(seconds * NS)

    with pytest.raises(TimeoutError, match="setup stage"):
        lifecycle.wait(lambda: False, guard, sleep)
    assert checks and clock[0] == 60 * NS
    lifecycle = Lifecycle(variant, 0, clock=lambda: clock[0])
    clock[0] = whole * NS - 11 * NS
    lifecycle.phase_end_ns = whole * NS - 10 * NS
    lifecycle.enter("verification")
    assert lifecycle.phase_end_ns == (whole - 10) * NS and lifecycle.timeout() == 1
    clock[0] = (whole - 10) * NS
    with pytest.raises(TimeoutError):
        lifecycle.check()
    clock[0] = whole * NS
    with pytest.raises(TimeoutError, match="whole lifecycle"):
        lifecycle.check()


def test_guard_is_called_in_waits_not_only_measurement_loop(tmp_path):
    clock, health, guard = guarded(tmp_path)
    lifecycle = Lifecycle("S", clock.now, clock=lambda: clock.now)

    def sleep(seconds):
        clock.now += int(seconds * NS)

    with pytest.raises(RuntimeError, match="tool health unavailable"):
        lifecycle.wait(lambda: False, guard.check, sleep)
    assert clock.now <= 105_050_000_000


def test_batch_budget_attempt_counts_complete_reservation_and_breach_stop():
    budget = BatchBudget()
    budget.started("S", 0)
    budget.ended(320 * NS)
    with pytest.raises(RuntimeError, match="count"):
        budget.reserve("S")
    for i, v in enumerate("VHDL", 1):
        budget.started(v, i * 500 * NS)
        budget.ended((i * 500 + 440) * NS)
    assert budget.reserve("R") == 440
    budget.started("R", 2500 * NS)
    with pytest.raises(RuntimeError, match="budget breach"):
        budget.ended(2941 * NS)
    with pytest.raises(RuntimeError, match="already stopped"):
        budget.reserve("anything")
    budget = BatchBudget()
    budget.rows = [dict(variant="old", started_ns=0, ended_ns=2200 * NS)]
    with pytest.raises(RuntimeError, match="insufficient"):
        budget.reserve("V")


class DummyTransport:
    def invoke(self, job, boundary, timeout):
        assert timeout == 10
        boundary()
        return dict(flag=0)


def dummy_factory(role):
    return DummyTransport()


def roles_case(folder):
    from r3_14_perf04_roles import ProcessRoles

    registry = Registry(folder / "registry.json")
    start = time.monotonic_ns()
    watchdog = Watchdog(registry, start + 22 * NS)
    roles = ProcessRoles(dummy_factory, registry, watchdog)
    received = []
    lifecycle = Lifecycle("S", start)
    start_clocks = roles.clock_handshake("start", lifecycle, lambda: None)
    assert set(start_clocks) == set("WRCQ")
    for i, (role, channel) in enumerate(
        [("W", "child"), ("R", "rotation"), ("C", "batch"), ("Q", "status")]
    ):
        assert roles.submit(
            dict(
                id=i,
                role=role,
                channel=channel,
                operation="test",
                expires_ns=start + 10 * NS,
            )
        )
    deadline = time.monotonic() + 5
    while (
        sum(e["kind"] == "completed" for e in received) < 4
        and time.monotonic() < deadline
    ):
        received.extend(roles.poll())
        time.sleep(0.01)
    assert len([e for e in received if e["kind"] == "sent"]) == 4
    assert len([e for e in received if e["kind"] == "completed"]) == 4
    exit_clocks = roles.clock_handshake("exit", lifecycle, lambda: None)
    assert set(exit_clocks) == set("WRCQ")
    roles.request_stop()
    deadline = time.monotonic() + 5
    while len(roles.finals) < 4 and time.monotonic() < deadline:
        roles.poll()
        registry.reap()
        time.sleep(0.01)
    assert len(roles.finals) == 4
    for final in roles.finals.values():
        assert final["produced"] == final["emitted"] == 5 and final["dropped"] == 0
    assert not roles.audit()["problems"]
    watchdog.disarm(time.monotonic_ns() + NS)
    registry.reap()
    summary = registry.summary()
    if summary["live_registered"]:
        summary = registry.cleanup(time.monotonic_ns() + 2 * NS)
    assert not summary["live_registered"] and summary["service_launches"] == 0
    assert summary["entries"][0]["role"] == "watchdog"
    assert len(summary["entries"]) == 5
    roles.close_queues()
    (folder / "roles-result.json").write_text(
        json.dumps(dict(registry=summary, events=received, finals=roles.finals))
    )


def test_four_real_helper_processes_use_role_entry_and_registered_cleanup_under_25s(
    tmp_path,
):
    result = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--roles-case", str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=25,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    data = json.loads((tmp_path / "roles-result.json").read_text())
    assert {e["role"] for e in data["registry"]["entries"]} == {
        "W",
        "R",
        "C",
        "Q",
        "watchdog",
    }


def watchdog_case(folder):
    registry = Registry(folder / "watch-registry.json")
    watchdog = Watchdog(registry, time.monotonic_ns() + 12 * NS)
    watchdog.arm()
    helper = subprocess.Popen(
        [sys.executable, "-c", "import time;time.sleep(30)"], start_new_session=True
    )
    registry.register(helper, "blocked-helper")
    # The parent performs NO guard/cleanup while blocked. Independent watchdog acts.
    helper.wait(timeout=6)
    assert helper.returncode < 0 and watchdog.marker.exists()
    watchdog.disarm(time.monotonic_ns() + NS)
    registry.reap()
    assert not registry.summary()["live_registered"]
    (folder / "watchdog-result.json").write_text(json.dumps(registry.summary()))


def test_independent_watchdog_acts_while_parent_blocks_under_20s(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--watchdog-case",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert (tmp_path / "watchdog-result.json").exists()


if __name__ == "__main__":
    {"--roles-case": roles_case, "--watchdog-case": watchdog_case}[sys.argv[1]](
        Path(sys.argv[2])
    )


def test_private_writer_handoff_is_bounded_and_never_public(tmp_path):
    import asyncio
    from r3_14_perf04_verify import HandoffServer, HandoffClient

    class Verifier:
        def handoff(self, job, timeout=10):
            assert job["path"] == "/f/t"
            return dict(
                proof=dict(effective=True),
                identity=dict(attempt="a", password="private"),
            )

    async def exercise():
        tmp_path.chmod(0o700)
        path = tmp_path / "private.sock"
        server = await HandoffServer(path, Verifier()).start()
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(HandoffClient(path), dict(path="/f/t")), 2
            )
            assert result["identity"]["password"] == "private"
            assert path.stat().st_mode & 0o077 == 0
        finally:
            await server.close()
        assert not path.exists() and not server.connections

    asyncio.run(exercise())


def test_explicit_perf04_collector_uses_04a_publisher(tmp_path, monkeypatch):
    import r3_14_isolated_worker as worker

    folder = tmp_path / "PERF-04-collector"
    folder.mkdir()
    (folder / "config.json").write_text("{}")
    events = [
        dict(pid=1, kind="request", values=[1, 7, 0], at_ns=100 * NS, identity="child"),
        dict(
            pid=1,
            kind="checkpoint_publish",
            values=[1, 1, 1, 1],
            at_ns=101 * NS,
            identity="exit",
        ),
    ]

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

        def close(self):
            pass

        def recv(self, *a):
            if self.packet:
                self.packet = False
                return b"events"
            raise BlockingIOError

    sock = Socket()
    stdin = SimpleNamespace(fileno=lambda: 0)
    calls = iter([([sock], [], []), ([stdin], [], [])])
    monkeypatch.setattr(worker.socket, "socket", lambda *a: sock)
    monkeypatch.setattr(worker, "sys", SimpleNamespace(stdin=stdin))
    monkeypatch.setattr(worker.select, "select", lambda *a: next(calls))
    monkeypatch.setattr(worker, "read_commands", lambda *a: [dict(command="stop")])
    monkeypatch.setattr(worker, "decode_packet", lambda *a: events)
    monkeypatch.setattr(worker, "proc_sample", lambda *a: dict(rss_bytes=0))
    monkeypatch.setattr(
        worker,
        "time",
        SimpleNamespace(monotonic=lambda: 101, monotonic_ns=lambda: 101 * NS),
    )
    worker.collector(
        SimpleNamespace(run_dir=str(folder), socket="none", perf08a=False, perf04=True)
    )
    state = json.loads((folder / "collector-health.json").read_text())
    assert (
        state["run_id"] == "PERF-04-collector" and state["last_progress_ns"] == 100 * NS
    )
    assert state["collector_pid"] == os.getpid()


def test_registered_cleanup_does_not_touch_unrelated_helper(tmp_path):
    registry = Registry(tmp_path / "cleanup.json")
    owned = subprocess.Popen(
        [sys.executable, "-c", "import time;time.sleep(20)"], start_new_session=True
    )
    unrelated = subprocess.Popen(
        [sys.executable, "-c", "import time;time.sleep(20)"], start_new_session=True
    )
    registry.register(owned, "owned-test")
    try:
        result = registry.cleanup(time.monotonic_ns() + NS)
        assert not result["live_registered"] and unrelated.poll() is None
        assert len([e for e in result["events"] if e["kind"] == "exit"]) == 1
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=1)


def test_resource_sampling_retains_100ms_and_1s_cadences(monkeypatch, tmp_path):
    import r3_14_perf04_safety as safety

    clock = [0]
    calls = []
    rows = []

    def sample(registry, directory, pid, pss):
        calls.append(pss)
        return safe_resources()

    monkeypatch.setattr(safety, "resource_sample", sample)
    sampler = safety.ResourceSampler(object(), tmp_path, 1, clock=lambda: clock[0])
    for tick in range(11):
        clock[0] = tick * 100_000_000
        sampler.tick(rows.append)
        sampler.tick(rows.append)
    assert len(calls) == 11 and calls == [True] + [False] * 9 + [True]


def test_registered_group_cleanup_survives_leader_exit(tmp_path):
    from r3_14_perf04_safety import group_members

    registry = Registry(tmp_path / "orphan.json")
    code = 'import subprocess,sys; child=subprocess.Popen([sys.executable,"-c","import time;time.sleep(20)"]); print(child.pid,flush=True)'
    leader = subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    registry.register(leader, "helper-group")
    child = int(leader.stdout.readline())
    leader.wait(timeout=2)
    registry.reap()
    assert child in group_members(leader.pid) and registry.summary()["live_registered"]
    summary = registry.cleanup(time.monotonic_ns() + 2 * NS)
    assert not summary["live_registered"] and not group_members(leader.pid)
