"""G0 T5/T6: owned real S/C/W entries and bounded synthetic input failures."""

import hashlib
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_snapshot_control as control


def job_for(
    tmp_path, *, program="import time; time.sleep(0.25)", mode="normal", fault=None
):
    # These deadlines only bound controlled helper processes. The actual G0
    # budget and its original t0 are inherited, never restarted by a test.
    budget_path = Path(os.environ["RUN_SNAPSHOT_G0_BUDGET"])
    now = time.monotonic_ns()
    return dict(
        stage="G0",
        start_ns=now,
        operational_ns=now + 2 * control.NS,
        hard_ns=now + 14 * control.NS,
        budget=str(budget_path),
        batch_directory=str(tmp_path),
        sources={
            control.SCRIPT: hashlib.sha256(
                Path(control.SCRIPT).read_bytes()
            ).hexdigest()
        },
        command=[sys.executable, "-c", program],
        coordinator_mode=mode,
        fault=fault,
        pss=False,
        synthetic_helper=True,
    )


def assert_known(result):
    assert result["tail_known"] and result["startup"]["tail_known"], result
    assert not result["hard_breached"]
    assert result["end_ns"] <= result["hard_ns"]
    for item in result["entries"]:
        assert item["ended_ns"] and item["reaped"]
        assert not control.members(item)


def test_real_entry_gate_identity_order_and_exit(tmp_path):
    result = control.run_owned(tmp_path, job_for(tmp_path))
    assert result["success"], result
    assert_known(result)
    events = [control.read(tmp_path / "job.json")]
    assert events[0]["pss"] is False
    rows = [
        __import__("json").loads(line)
        for line in (tmp_path / "supervisor.jsonl").read_text().splitlines()
    ]
    worker_register = next(
        r for r in rows if r["kind"] == "registered" and r["entry"]["role"] == "worker"
    )
    worker_release = next(
        r for r in rows if r["kind"] == "released" and r["role"] == "worker"
    )
    ready = next(r for r in rows if r["kind"] == "watchdog_ready")
    gate = __import__("json").loads(
        (tmp_path / "worker-gate.jsonl").read_text().splitlines()[0]
    )
    assert (
        ready["at_ns"]
        < worker_register["at_ns"]
        <= worker_release["at_ns"]
        <= gate["at_ns"]
    )
    with pytest.raises(RuntimeError, match="already_attempted"):
        control.run_owned(tmp_path, job_for(tmp_path))


def test_sampling_uses_actual_supervisor_birth_after_prelaunch_audit(tmp_path):
    job = job_for(tmp_path)
    job["start_ns"] -= control.NS
    job["operational_ns"] = job["start_ns"] + 2 * control.NS
    job["hard_ns"] = job["start_ns"] + 14 * control.NS
    result = control.run_owned(tmp_path, job)
    assert result["success"], result
    assert_known(result)


@pytest.mark.parametrize("mode", ["blocked", "early_exit"])
def test_coordinator_loss_or_block_does_not_disable_watchdog(tmp_path, mode):
    job = job_for(tmp_path, mode=mode)
    if mode == "blocked":
        job["operational_ns"] = job["start_ns"] + 700_000_000
        job["hard_ns"] = job["start_ns"] + 12 * control.NS
    result = control.run_owned(tmp_path, job)
    assert not result["success"]
    assert_known(result)
    if mode == "blocked":
        assert result["trigger"]["reason"] == "operational_deadline"
        assert (
            job["operational_ns"]
            <= result["trigger"]["at_ns"]
            < job["hard_ns"] - 10 * control.NS
        )
        watch = control.read(tmp_path / "watchdog-final.json")
        assert (
            watch["tail_known"]
            and watch["cleanup_deadline_ns"] <= job["hard_ns"] - 10 * control.NS
        )
    else:
        assert "coordinator_early_exit" in result["error"]


def test_worker_blocked_ignoring_term_is_killed_and_reaped(tmp_path):
    job = job_for(
        tmp_path,
        program="import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(20)",
    )
    job["operational_ns"] = job["start_ns"] + 700_000_000
    job["hard_ns"] = job["start_ns"] + 12 * control.NS
    result = control.run_owned(tmp_path, job)
    assert not result["success"]
    assert_known(result)
    worker = next(e for e in result["entries"] if e["role"] == "worker")
    assert worker["exit_code"] == -9


def test_dead_leader_live_child_group_is_cleaned(tmp_path):
    child_pid = tmp_path / "child.pid"
    program = (
        "import subprocess,sys,os,pathlib; p=subprocess.Popen([sys.executable,'-c',"
        "'import time;time.sleep(20)']); pathlib.Path("
        + repr(str(child_pid))
        + ").write_text(str(p.pid)); os._exit(0)"
    )
    result = control.run_owned(tmp_path, job_for(tmp_path, program=program))
    assert not result["success"] and "descendant_tail" in result["error"]
    assert_known(result)
    pid = int(child_pid.read_text())
    assert not Path(f"/proc/{pid}").exists()
    assert "adopted_reaped" in (tmp_path / "supervisor.jsonl").read_text()


def test_registration_failure_never_releases_business(tmp_path):
    marker = tmp_path / "business.txt"
    result = control.run_owned(
        tmp_path,
        job_for(
            tmp_path,
            fault="registration",
            program="import pathlib;pathlib.Path("
            + repr(str(marker))
            + ").write_text('bad')",
        ),
    )
    assert not result["success"] and "registration" in result["error"]
    assert_known(result)
    assert not marker.exists() and not (tmp_path / "worker-gate.jsonl").exists()


@pytest.mark.parametrize("failure", ["source", "expired", "unclosed"])
def test_admission_rejects_before_any_child(tmp_path, failure):
    job = job_for(tmp_path)
    previous = None
    if failure == "source":
        job["sources"][control.SCRIPT] = "0" * 64
    elif failure == "expired":
        job["operational_ns"] = time.monotonic_ns() - 1
    else:
        previous = dict(closed=False, started_ns=1)
        control.save(tmp_path / "attempt.json", previous)
    result = control.run_owned(tmp_path, job)
    assert not result["success"] and not result["entries"]
    assert not (tmp_path / "watchdog-ready.json").exists()
    if previous:
        assert control.read(tmp_path / "attempt.json") == previous


def test_critical_one_ns_boundary_rejected_at_real_admission(tmp_path, monkeypatch):
    job = job_for(tmp_path)
    control.save(tmp_path / "job.json", job)
    monkeypatch.setattr(control.time, "monotonic_ns", lambda: job["operational_ns"])
    result = control.supervise(tmp_path)
    assert not result["success"] and "budget_admission" in result["error"]
    assert not result["entries"] and result["tail_known"]
    assert not (tmp_path / "watchdog-ready.json").exists()
    assert not (tmp_path / "attempt.json").exists()
    monkeypatch.setattr(control.time, "monotonic_ns", lambda: job["operational_ns"] - 1)
    control.admit(tmp_path, job)
    assert (tmp_path / "attempt.json").exists()


def test_unrelated_owned_by_test_process_is_not_signalled(tmp_path):
    unrelated = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(10)"])
    try:
        result = control.run_owned(tmp_path, job_for(tmp_path, mode="early_exit"))
        assert_known(result)
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=2)


@pytest.mark.parametrize(
    "fault", ["unavailable", "identity", "negative", "gap", "resource"]
)
def test_actual_sampling_failure_stops_entry_and_keeps_tail(tmp_path, fault):
    result = control.run_owned(
        tmp_path, job_for(tmp_path, fault=fault, program="import time;time.sleep(2)")
    )
    assert not result["success"] and result["error"]
    assert_known(result)


def sample_input():
    item = dict(pid=123, role="worker", ticks=7, started_ns=1_000_000_000)
    value = dict(
        at_ns=1_100_000_000,
        mem_available=8 * control.GIB,
        disk_free=40 * control.GIB,
        swap_used=0,
        evidence_bytes=0,
        batch_bytes=0,
        processes=[dict(item, cpu=0.1, rss=1024)],
    )
    return item, value


@pytest.mark.parametrize(
    "fault",
    [
        "missing",
        "unavailable",
        "identity",
        "negative",
        "gap",
        "first_gap",
        "exit_gap",
        "memory",
        "disk",
        "swap",
        "evidence",
        "batch",
        "tool_rss",
        "rss_sum",
    ],
)
def test_actual_sampler_guard_rejects_required_data_and_stop_lines(fault):
    item, value = sample_input()
    guard = control.Samples(0)
    guard.accept(value, [item])
    value["at_ns"] += 100_000_000
    if fault == "missing":
        value["processes"] = []
    elif fault == "unavailable":
        value["processes"][0]["unavailable"] = True
    elif fault == "identity":
        value["processes"][0]["ticks"] += 1
    elif fault == "negative":
        value["processes"][0]["cpu"] = -1
    elif fault == "gap":
        value["at_ns"] += 250_000_001
    elif fault == "first_gap":
        guard = control.Samples(0)
        value["at_ns"] = item["started_ns"] + 250_000_001
    elif fault == "exit_gap":
        with pytest.raises(RuntimeError, match="exit_sample_gap"):
            guard.exited(item, value["at_ns"] + 250_000_001)
        return
    elif fault in {"memory", "disk", "swap", "evidence", "batch"}:
        key, bad = {
            "memory": ("mem_available", 0),
            "disk": ("disk_free", 0),
            "swap": ("swap_used", control.GIB + 1),
            "evidence": ("evidence_bytes", 256 * 1024**2 + 1),
            "batch": ("batch_bytes", 2 * control.GIB + 1),
        }[fault]
        value[key] = bad
    elif fault == "tool_rss":
        item["role"] = value["processes"][0]["role"] = "S"
        value["processes"][0]["rss"] = 513 * 1024**2
    elif fault == "rss_sum":
        value["processes"][0]["rss"] = 8 * control.GIB + 1
        guard.accept(value, [item])
        value["at_ns"] += 100_000_000
    with pytest.raises(RuntimeError):
        guard.accept(value, [item])


def test_pss_is_not_read_and_cannot_erase_cpu_rss(monkeypatch):
    original = Path.read_text

    def read_text(path, *args, **kwargs):
        assert "smaps_rollup" not in str(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_text)
    item = control.identity(os.getpid()) | dict(role="S")
    value = control.sample_process(item)
    assert "cpu" in value and "rss" in value and not value.get("unavailable")


def test_real_supervised_small_diagnostic_cli(tmp_path):
    config = tmp_path / "small.json"
    control.save(config, dict(stage="G0", tasks=250))
    directory = tmp_path / "owned"
    diagnostic = Path(control.SCRIPT).with_name("run_snapshot_diagnostic.py")
    job = job_for(directory)
    job["command"] = [
        sys.executable,
        str(diagnostic),
        str(tmp_path / "diagnostic"),
        str(config),
    ]
    job["sources"].update(
        {
            str(config): hashlib.sha256(config.read_bytes()).hexdigest(),
            str(diagnostic): hashlib.sha256(diagnostic.read_bytes()).hexdigest(),
        }
    )
    job["operational_ns"] = job["start_ns"] + 5 * control.NS
    job["hard_ns"] = job["start_ns"] + 18 * control.NS
    result = control.run_owned(directory, job)
    assert result["success"], result
    assert_known(result)
    diagnostic_result = control.read(tmp_path / "diagnostic/result.json")
    assert diagnostic_result["validation_complete"] and diagnostic_result["restored"]
    assert len(diagnostic_result["samples"]) == 6
    assert diagnostic_result["fixture"]["tasks"] == 250
    assert not diagnostic_result["candidate_selection_allowed"]


def test_owned_descendant_rss_cpu_are_in_actual_samples(tmp_path):
    program = (
        "import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c',"
        "'import time;time.sleep(0.45)']); p.wait(); time.sleep(0.1)"
    )
    result = control.run_owned(tmp_path, job_for(tmp_path, program=program))
    assert result["success"], result
    assert_known(result)
    assert result["observed_descendants"]
    rows = [
        __import__("json").loads(line)["sample"]
        for line in (tmp_path / "resources.jsonl").read_text().splitlines()
    ]
    child_samples = [
        process
        for row in rows
        for process in row["processes"]
        if process["role"] == "worker_child"
    ]
    assert child_samples and all("cpu" in row and "rss" in row for row in child_samples)
    assert all(
        item["ended_ns"] and not control.same_process(item)
        for item in result["observed_descendants"]
    )
