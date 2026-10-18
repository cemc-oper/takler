"""PERF-08A bounded schedule. Two shorts then six full, no retry/qualification."""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
import time

import r3_14_isolated_probe as probe
from r3_14_clock_probe import ClockProbe, clock_info
from r3_14_isolated_analyze import lines
from r3_14_perf08a_analyze import paired_results

REPO = Path(__file__).resolve().parents[1]
ROOT = REPO.parents[1] / "doc/2026-10-13-r3-14-design-performance/evidence"
PREFIX = "PERF-08A"


def read(path):
    return json.loads(Path(path).read_text())


def detail_sample(pid, max_threads=128, proc_root=Path("/proc")):
    tasks, errors = [], []
    entries = list((proc_root / str(pid) / "task").glob("[0-9]*"))
    if len(entries) > max_threads:
        raise RuntimeError("external thread-detail capacity exceeded")
    attempts = 0
    for task in entries:
        try:
            before = time.monotonic_ns()
            attempts += 1
            stat = (task / "stat").read_text().rpartition(")")[2].split()
            attempts += 1
            status = dict(
                line.split(":", 1)
                for line in (task / "status").read_text().splitlines()
            )
            attempts += 1
            sched = list(map(int, (task / "schedstat").read_text().split()))
            attempts += 1
            name = (task / "comm").read_text().strip()
            tasks.append(
                dict(
                    tid=int(task.name),
                    name=name,
                    before_ns=before,
                    after_ns=time.monotonic_ns(),
                    user_seconds=int(stat[11]) / os.sysconf("SC_CLK_TCK"),
                    system_seconds=int(stat[12]) / os.sysconf("SC_CLK_TCK"),
                    runtime_ns=sched[0],
                    runqueue_ns=sched[1],
                    timeslices=sched[2],
                    voluntary=int(status["voluntary_ctxt_switches"]),
                    involuntary=int(status["nonvoluntary_ctxt_switches"]),
                )
            )
        except (OSError, ValueError, KeyError, IndexError) as exc:
            errors.append(dict(tid=int(task.name), error=repr(exc)))
    return dict(
        at_ns=time.monotonic_ns(),
        pid=pid,
        threads=tasks,
        errors=errors,
        enumerated_tids=[int(p.name) for p in entries],
        file_read_attempts=attempts,
    )


class Sampler:
    def __init__(self, settings):
        self.settings = settings
        self.clocks = ClockProbe(settings["clock_record_capacity"])
        self.pid = self.path = None
        self.stop = threading.Event()
        self.ticks = []
        self.detail_calls = self.detail_errors = self.empty = self.detail_peak_bytes = 0

    def tick(self):
        if len(self.ticks) >= self.settings["sample_tick_capacity"]:
            raise RuntimeError("external tick capacity exceeded")
        before, cpu = time.monotonic_ns(), time.thread_time_ns()
        packet = self.clocks.tick(len(self.ticks))
        if self.pid is not None and self.path is not None:
            sample = detail_sample(
                self.pid, self.settings["detail_threads_per_tick_max"]
            )
            # Stream one bounded sample; there is no lifetime list of thread dictionaries.
            raw = json.dumps(sample) + "\n"
            self.detail_peak_bytes = max(self.detail_peak_bytes, len(raw.encode()))
            if len(raw.encode()) > 1024 * 1024:
                raise RuntimeError("thread detail packet exceeds 1 MiB")
            with (self.path / "thread-resources.jsonl").open("a") as stream:
                stream.write(raw)
            self.detail_calls += 1
            self.detail_errors += len(sample["errors"])
            self.empty += not sample["enumerated_tids"]
        packet.update(
            tick_before_ns=before,
            tick_after_ns=time.monotonic_ns(),
            sampling_thread_cpu_ns=time.thread_time_ns() - cpu,
        )
        self.ticks.append(packet)
        self.clocks.check()

    def run(self):
        try:
            while not self.stop.is_set():
                self.tick()
                self.stop.wait(1)
        except BaseException as exc:
            self.clocks.failure = f"external sampler: {type(exc).__name__}: {exc}"

    def metadata(self):
        # Generous per-record bound checked structurally in preflight; details are streamed.
        retained_bound = (
            self.clocks.capacity * 4096
            + self.settings["sample_tick_capacity"] * 2048
            + 2 * 1024 * 1024
        )
        return dict(
            detail_calls=self.detail_calls,
            detail_failed_reads=self.detail_errors,
            empty_enumerations=self.empty,
            detail_peak_json_bytes=self.detail_peak_bytes,
            retained_evidence_bound_bytes=retained_bound,
            thread_details_retained="one tick, <=128 threads; streamed",
            sample_tick_capacity=self.settings["sample_tick_capacity"],
            clock_capacity=self.clocks.capacity,
        )


def audit(path, summary, sampler, short):
    problems = list(summary["evidence_problems"])
    try:
        sampler.clocks.check()
    except RuntimeError as exc:
        problems.append(str(exc))
    if summary["exit_reason"] != "completed":
        problems.append(summary["exit_reason"])
    if not summary["default_gc_continuity"] or not summary["gc_counts_complete"]:
        problems.append("default GC or observation continuity failed")
    for role in ("server", "S", "F1", "F2"):
        p = summary["producers"].get(role, {})
        if (
            p.get("tail") != "known"
            or p.get("produced") != p.get("emitted")
            or p.get("emitted") != p.get("received")
            or any(
                p.get(k, 1)
                for k in (
                    "dropped",
                    "collector_loss",
                    "sequence_missing",
                    "abandoned_ring_records",
                )
            )
        ):
            problems.append(f"{role} loss/accounting failure")
        if (
            p.get("capacity") != 8192
            or not p.get("sender_stopped")
            or p.get("observation_storage_bound_bytes", 10**9) > 8388608
        ):
            problems.append(f"{role} fixed observation bound failed")
    previous = None
    for event in lines(path / "events.jsonl"):
        if event["kind"] != "loop_lag":
            continue
        target, actual = event["start_ns"], event["end_ns"]
        lag, skip = event["values"][:2]
        if (
            lag != max(0, actual - target)
            or skip != lag // 5_000_000
            or (previous and target != previous[0] + (previous[1] + 1) * 5_000_000)
        ):
            problems.append("invalid fixed 5ms grid")
            break
        previous = target, skip
    if previous is None:
        problems.append("no lag records")
    if short and not all(summary["short_gate"].values()):
        problems.append("short correctness minimum not met")
    manifest = read(path / "manifest.json")
    for n, h in manifest["evidence_files"].items():
        if probe.digest(path / n) != h:
            problems.append(f"evidence checksum mismatch {n}")
    source = Path(manifest["source_directory"])
    for n, h in read(source / "source-index.json").items():
        if probe.digest(source / n) != h:
            problems.append(f"source checksum mismatch {n}")
    if (
        sampler.metadata()["retained_evidence_bound_bytes"]
        > sampler.settings["external_retained_evidence_max_bytes"]
    ):
        problems.append("external evidence memory bound exceeded")
    records = sampler.clocks.records
    if [r["seq"] for r in records] != list(range(len(records))) or any(
        "error" in r or r["width_ns"] < 0 for r in records
    ):
        problems.append("clock record integrity failed")
    if any(b["before_ns"] < a["after_ns"] for a, b in zip(records, records[1:])):
        problems.append("monotonic envelope reversed")
    return dict(
        passed=not problems,
        problems=problems,
        short_gate=summary["short_gate"] if short else None,
        analysis_sufficiency=summary["analysis_sufficiency"],
        sampler=sampler.metadata(),
    )


def execute_one(base, source, rows, variant, pair, short):
    sampler = Sampler(base["observation"])
    original_launch = probe.Process.__init__

    def launch(process, role, args, env, run_dir):
        sampler.path = run_dir
        original_launch(process, role, args, env, run_dir)
        if role == "server":
            sampler.pid = process.pid
            original_cleanup = process.cleanup

            def cleanup(timeout=5):
                result = original_cleanup(timeout)
                sampler.stop.set()
                thread.join(timeout=2)
                return result

            process.cleanup = cleanup

    stage = "short" if short else "full"
    row = dict(stage=stage, variant=variant, pair=pair, status="started", path=None)
    rows.append(row)
    probe.save(ROOT / f"{PREFIX}-index.json", rows)
    config = json.loads(json.dumps(base))
    cfg = base["schedule"]
    config["performance_runs"] = dict(
        startup_max_seconds=cfg["startup_seconds"],
        ready_baseline_no_requests_seconds=cfg["baseline_seconds"],
        measurement_seconds=cfg["short_seconds"] if short else cfg["full_seconds"],
        withdrawal_no_new_requests_seconds=cfg["withdrawal_seconds"],
        drain_exit_max_seconds=cfg["cleanup_seconds"],
        whole_run_max_seconds=cfg["short_hard_seconds"]
        if short
        else cfg["full_hard_seconds"],
    )
    config["run_selection"] = dict(stage=stage, pair=pair, variant=variant)
    probe.Process.__init__ = launch
    thread = threading.Thread(
        target=sampler.run, name="perf08a-external-details", daemon=True
    )
    thread.start()
    path, summary, error = None, None, None
    try:
        path, summary = probe.run_one(
            REPO,
            ROOT,
            source,
            config,
            "D",
            variant,
            len(rows),
            smoke=short,
            clock_diagnostics=sampler.clocks,
        )
    except BaseException as exc:
        error = exc
    finally:
        sampler.stop.set()
        thread.join(timeout=2)
        probe.Process.__init__ = original_launch
        if thread.is_alive():
            sampler.clocks.failure = "sampler bounded join failed"
        path = path or sampler.path
        row.update(
            path=str(path) if path else None, error=repr(error) if error else None
        )
        if path:
            probe.save(
                path / "clock-readings.json",
                dict(
                    clock_info=clock_info(),
                    capacity=sampler.clocks.capacity,
                    records=sampler.clocks.records,
                    ticks=sampler.ticks,
                    failure=sampler.clocks.failure,
                ),
            )
            probe.save(path / "thread-detail-status.json", sampler.metadata())
            row["extra_evidence_sha256"] = {
                n: probe.digest(path / n)
                for n in (
                    "clock-readings.json",
                    "thread-detail-status.json",
                    "thread-resources.jsonl",
                )
                if (path / n).exists()
            }
        if error:
            row["status"] = "failed"
        elif summary:
            row["audit"] = audit(path, summary, sampler, short)
            row["status"] = "audited" if row["audit"]["passed"] else "failed"
            row["exit_reason"] = summary["exit_reason"]
        probe.save(ROOT / f"{PREFIX}-index.json", rows)
        print(json.dumps(row), flush=True)
    if error:
        raise error
    if row["status"] != "audited":
        raise RuntimeError(f"{stage} {variant} correctness/safety gate failed")


def execute_schedule(base, source, rows):
    for variant in base["schedule"]["short"]:
        execute_one(base, source, rows, variant, None, True)
    for pair, order in enumerate(base["schedule"]["full"], 1):
        for variant in order:
            execute_one(base, source, rows, variant, pair, False)


def main():
    config_path = ROOT / f"{PREFIX}-config.json"
    base = read(config_path)
    assert not (ROOT / f"{PREFIX}-index.json").exists(), "no rerun permitted"
    frozen = read(ROOT / f"{PREFIX}-preflight.json")
    assert frozen["passed"]
    for path, h in frozen["frozen_sha256"].items():
        assert probe.digest(path) == h, path
    assert (
        probe.git(REPO, "rev-parse", "HEAD").strip() == base["source_heads"]["takler"]
    )
    assert (
        probe.git(REPO.parent / "takler-client", "rev-parse", "HEAD").strip()
        == base["source_heads"]["client"]
    )
    assert not probe.git(
        REPO.parent / "takler-client", "status", "--porcelain=v1", "-uall"
    )
    original = read(ROOT / "PERF-03-config.json")
    assert base["qualification_budgets"] == original["qualification_budgets"]
    source = ROOT / (
        "diagnostic-source-PERF-08A-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    )
    files = probe.diagnostic_copy(REPO, source)
    prior = read(ROOT / "PERF-03-native-reference-execution.json")
    assert probe.digest(source / "gc.patch") == prior["gc_patch_sha256"]
    product = {n: h for n, h in files.items() if n.startswith("src/")}
    old_product = {
        n: h
        for n, h in read(Path(prior["source"]) / "source-index.json").items()
        if n.startswith("src/")
    }
    assert product == old_product, "ordinary product source changed"
    probe.save(
        ROOT / f"{PREFIX}-execution.json",
        dict(
            source=str(source),
            source_index_sha256=probe.digest(source / "source-index.json"),
            gc_patch_sha256=probe.digest(source / "gc.patch"),
            frozen_files=frozen["frozen_sha256"],
            source_files=len(files),
            started_utc=datetime.now(timezone.utc).isoformat(),
            native_reference_loaded=False,
            prior_249_index_sha256=probe.digest(
                ROOT / "PERF-03-native-reference-all-runs-index.json"
            ),
        ),
    )
    rows = []
    try:
        execute_schedule(base, source, rows)
    except BaseException as exc:
        probe.save(
            ROOT / f"{PREFIX}-stop.json",
            dict(
                error=repr(exc),
                attempts=len(rows),
                no_retry=True,
                finished_utc=datetime.now(timezone.utc).isoformat(),
            ),
        )
        raise
    finally:
        probe.save(ROOT / f"{PREFIX}-result.json", paired_results(rows))
    probe.save(
        ROOT / f"{PREFIX}-completion.json",
        dict(
            attempts=len(rows),
            short=2,
            full=6,
            no_retry=True,
            qualification_runs=0,
            finished_utc=datetime.now(timezone.utc).isoformat(),
        ),
    )


if __name__ == "__main__":
    main()
