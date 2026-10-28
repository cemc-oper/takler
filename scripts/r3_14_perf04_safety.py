"""PERF-04 explicit health wiring, stage deadlines and registered owned cleanup."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from r3_14_health import HealthGuard, publish_health
from r3_14_perf04 import NS

GIB = 1024**3


def resource_reason(sample, swap_start):
    if sample["mem_available"] < 4 * GIB:
        return "MemAvailable below 4 GiB"
    if sample["swap_used"] - swap_start > GIB:
        return "swap growth above 1 GiB"
    if sample["disk_free"] < 20 * GIB:
        return "evidence disk below 20 GiB"
    if sample["tool_rss"] > 512 * 1024**2:
        return "tool RSS above 512 MiB"
    if sample["evidence_bytes"] > GIB:
        return "evidence above 1 GiB"
    if sample.get("fixed_version_bytes", 0) > 768 * 1024**2:
        return "fixed-version ownership above 768 MiB"
    if sample.get("snapshot_extra_bytes", 0) > 1536 * 1024**2:
        return "snapshot ownership above 1.5 GiB"
    if sample.get("stream_client_extra_bytes", 0) > 128 * 1024**2:
        return "stream client above 128 MiB"
    return None


class Guard:
    task_id = "PERF-04"

    def __init__(
        self,
        path,
        run_id,
        collector_pid,
        start_ns,
        collector_poll,
        resources,
        swap_start=0,
        clock=time.monotonic_ns,
    ):
        if not run_id.startswith("PERF-04-"):
            raise ValueError("PERF-04 identity required")
        self.clock, self.collector_poll, self.resources = (
            clock,
            collector_poll,
            resources,
        )
        self.health = HealthGuard(path, run_id, collector_pid, start_ns, clock=clock)
        self.swap_start = swap_start
        self.measurement_start_ns = None
        self.failure = None
        self.expected_collector_stop = False
        self.deadline_ns = None

    def check(self, *, now_ns=None, last_child_ns=None, offered=0, measuring=False):
        now = self.clock() if now_ns is None else now_ns
        if self.deadline_ns is not None and now >= self.deadline_ns:
            raise TimeoutError("phase/whole deadline")
        if reason := resource_reason(self.resources(), self.swap_start):
            raise RuntimeError(reason)
        if self.failure:
            raise RuntimeError(self.failure)
        if self.expected_collector_stop:
            return
        self.health.measurement_start_ns = self.measurement_start_ns
        self.health.check(
            collector_exit=self.collector_poll(),
            check_progress=self.measurement_start_ns is not None,
        )
        if (
            measuring
            and offered
            and self.measurement_start_ns is not None
            and now
            - max(self.measurement_start_ns, last_child_ns or self.measurement_start_ns)
            >= 10 * NS
        ):
            raise RuntimeError("10s without effective child completion")


class Perf04Health:
    """Collector-side PERF-04 event adapter using 04A atomic publication."""

    def __init__(self, path, run_id, pid):
        if not run_id.startswith("PERF-04-"):
            raise ValueError("PERF-04 identity required")
        self.path, self.run_id, self.pid = path, run_id, pid
        self.state = dict(last_progress_ns=0, failure=None)

    def accept(self, event):
        kind = event["kind"]
        success = (
            kind == "completed"
            and event.get("outcome") == "success"
            or kind == "checkpoint_publish"
            and event.get("success") is True
            and event.get("published") is True
            and event.get("exit_save") is False
        )
        if success:
            self.state["last_progress_ns"] = max(
                self.state["last_progress_ns"], event["at_ns"]
            )
        if (
            kind == "completed"
            and event.get("outcome") != "success"
            or kind == "checkpoint_outcome"
            and not event["success"]
        ):
            self.state["failure"] = dict(
                kind="request" if kind == "completed" else kind,
                at_ns=event["at_ns"],
                identity=str(event.get("id", "")),
            )

    def accept_legacy(self, event):
        from r3_14_health import update_health

        update_health(self.state, event)

    def publish(self, now_ns):
        publish_health(self.path, self.state, self.run_id, self.pid, now_ns)


class Lifecycle:
    """Phase allowances are caps, never extra time beyond the whole-run reserve."""

    def __init__(self, variant, start_ns, clock=time.monotonic_ns):
        self.clock, self.start_ns = clock, start_ns
        self.hard_ns = start_ns + (320 if variant == "S" else 440) * NS
        self.operational_ns = self.hard_ns - 10 * NS
        self.phase = "setup"
        self.phase_end_ns = min(start_ns + 60 * NS, self.operational_ns)
        self.events = [
            dict(
                kind="phase",
                phase=self.phase,
                at_ns=start_ns,
                deadline_ns=self.phase_end_ns,
            )
        ]
        self.variant = variant

    def enter(self, phase):
        allowances = dict(
            baseline=10,
            measurement=60 if self.variant == "S" else 180,
            withdrawal=30,
            verification=150,
            cleanup=10,
        )
        now = self.clock()
        if now >= self.hard_ns:
            raise TimeoutError("whole lifecycle deadline")
        if phase != "cleanup" and now > self.phase_end_ns:
            raise TimeoutError(self.phase + " stage deadline")
        self.phase = phase
        self.phase_end_ns = min(
            now + allowances[phase] * NS,
            self.hard_ns if phase == "cleanup" else self.operational_ns,
        )
        self.events.append(
            dict(kind="phase", phase=phase, at_ns=now, deadline_ns=self.phase_end_ns)
        )

    def check(self):
        now = self.clock()
        if now >= self.hard_ns:
            raise TimeoutError("whole lifecycle deadline")
        if now >= self.phase_end_ns:
            raise TimeoutError(self.phase + " stage deadline")

    def timeout(self, maximum=10):
        self.check()
        return min(maximum, (self.phase_end_ns - self.clock()) / NS)

    def wait(self, predicate, guard, sleep=time.sleep):
        while not predicate():
            self.check()
            guard()
            sleep(min(0.05, self.timeout(0.05)))
        self.check()
        guard()


class BatchBudget:
    def __init__(self):
        self.rows = []

    def reserve(self, variant):
        maximum = 320 if variant == "S" else 440
        if any(r.get("budget_breach") for r in self.rows):
            raise RuntimeError("batch already stopped by budget breach")
        if any(r["variant"] == variant for r in self.rows) or len(self.rows) >= 6:
            raise RuntimeError("lifecycle count exhausted")
        if any(r.get("ended_ns") is None for r in self.rows):
            raise RuntimeError("previous lifecycle still open")
        consumed = sum((r["ended_ns"] - r["started_ns"]) / NS for r in self.rows)
        if 2520 - consumed < maximum:
            raise RuntimeError("insufficient complete lifecycle budget")
        return maximum

    def started(self, variant, at_ns):
        self.reserve(variant)
        self.rows.append(dict(variant=variant, started_ns=at_ns, ended_ns=None))

    def ended(self, at_ns):
        row = self.rows[-1]
        row["ended_ns"] = at_ns
        row["budget_breach"] = (
            at_ns - row["started_ns"] > (320 if row["variant"] == "S" else 440) * NS
        )
        if row["budget_breach"]:
            raise RuntimeError("actual lifecycle budget breach; batch stopped")


def save_atomic(path, value):
    temporary = path.with_suffix(".own.tmp")
    temporary.write_text(json.dumps(value))
    temporary.replace(path)


def group_members(pid):
    members = []
    for path in Path("/proc").glob("[0-9]*"):
        try:
            stat = (path / "stat").read_text().rpartition(")")[2].split()
        except (FileNotFoundError, ProcessLookupError):
            continue
        if stat[0] != "Z" and int(stat[2]) == pid and int(stat[3]) == pid:
            members.append(int(path.name))
    if len(members) > 64:
        raise RuntimeError("owned group process capacity exceeded")
    return members


class Registry:
    """Only processes actually started through this registry may be cleaned up."""

    def __init__(self, path, clock=time.monotonic_ns):
        self.path, self.clock = Path(path), clock
        self.entries = []
        self.processes = {}
        self.events = []
        self.persist()

    def persist(self):
        save_atomic(self.path, dict(entries=self.entries, events=self.events))

    def register(self, proc, role, category="tool", started_ns=None):
        pid = proc.pid
        if len(self.entries) >= 64:
            raise RuntimeError("registered process capacity exceeded")
        if pid in self.processes:
            raise ValueError("process already registered")
        at = self.clock() if started_ns is None else started_ns
        stat = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
        self.entries.append(
            dict(
                pid=pid,
                role=role,
                category=category,
                registration_only=started_ns is None,
                started_ns=at,
                proc_start_ticks=stat[19],
                ended_ns=None,
                exit_code=None,
            )
        )
        self.processes[pid] = proc
        self.events.append(
            dict(kind="start", pid=pid, role=role, category=category, at_ns=at)
        )
        self.persist()

    def start(self, proc, role):
        # Record the interval before start as well as the resulting real PID.
        before = self.clock()
        proc.start()
        self.register(proc, role, started_ns=before)
        self.events.append(
            dict(
                kind="launch_boundary",
                pid=proc.pid,
                before_ns=before,
                after_ns=self.clock(),
            )
        )
        self.persist()

    def launch(self, args, role, category="tool", **kwargs):
        before = self.clock()
        proc = subprocess.Popen(args, start_new_session=True, **kwargs)
        self.register(proc, role, category, started_ns=before)
        return proc

    @staticmethod
    def poll(proc):
        return proc.poll() if hasattr(proc, "poll") else proc.exitcode

    def reap(self):
        for entry in self.entries:
            proc = self.processes[entry["pid"]]
            if not hasattr(proc, "poll"):
                proc.join(timeout=0)
            code = self.poll(proc)
            entry["group_live"] = group_members(entry["pid"])
            if (
                code is not None
                and not entry["group_live"]
                and entry["ended_ns"] is None
            ):
                entry.update(ended_ns=self.clock(), exit_code=code)
                self.events.append(
                    dict(
                        kind="exit",
                        pid=entry["pid"],
                        at_ns=entry["ended_ns"],
                        exit_code=code,
                    )
                )
        self.persist()

    def signal(self, entry, sig):
        pid = entry["pid"]
        try:
            stat = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
            if stat[19] != entry["proc_start_ticks"]:
                raise RuntimeError("registered PID identity changed")
            if os.getpgid(pid) == pid:
                os.killpg(pid, sig)
            else:
                os.kill(pid, sig)
            self.events.append(
                dict(kind="signal", pid=pid, signal=int(sig), at_ns=self.clock())
            )
        except (ProcessLookupError, FileNotFoundError):
            if group_members(pid):
                os.killpg(pid, sig)
                self.events.append(
                    dict(
                        kind="group_signal",
                        pid=pid,
                        signal=int(sig),
                        at_ns=self.clock(),
                    )
                )

    def cleanup(self, deadline_ns):
        self.reap()
        for entry in self.entries:
            if entry["ended_ns"] is None and entry["role"] != "watchdog":
                self.signal(entry, signal.SIGTERM)
        kill_at = max(self.clock(), deadline_ns - NS)
        while self.clock() < kill_at and any(
            e["ended_ns"] is None for e in self.entries
        ):
            self.reap()
            if all(
                e["ended_ns"] is not None or e["role"] == "watchdog"
                for e in self.entries
            ):
                for e in self.entries:
                    if e["ended_ns"] is None:
                        self.signal(e, signal.SIGTERM)
            time.sleep(min(0.02, max(0, (kill_at - self.clock()) / NS)))
        for entry in self.entries:
            if entry["ended_ns"] is None:
                self.signal(entry, signal.SIGKILL)
        while self.clock() < deadline_ns and any(
            e["ended_ns"] is None for e in self.entries
        ):
            self.reap()
            time.sleep(0.01)
        self.reap()
        return self.summary()

    def summary(self):
        service = [e for e in self.entries if e["category"] == "service"]
        return dict(
            entries=self.entries,
            events=self.events,
            service_launches=len(service),
            timeline_complete=all(not e["registration_only"] for e in self.entries),
            service_process_seconds=sum(
                (e["ended_ns"] - e["started_ns"]) / NS
                for e in service
                if e["ended_ns"] is not None
            ),
            service_tail="unknown"
            if any(e["ended_ns"] is None for e in service)
            else "known",
            live_registered=[e["pid"] for e in self.entries if e["ended_ns"] is None],
            owned_lifecycle_seconds=(
                max((e["ended_ns"] or self.clock()) for e in self.entries)
                - min(e["started_ns"] for e in self.entries)
            )
            / NS
            if self.entries
            else 0,
        )


class Watchdog:
    def __init__(self, registry, hard_ns):
        self.registry, self.hard_ns = registry, hard_ns
        self.proc = None
        self.cancel = registry.path.with_suffix(".watchdog-cancel")
        self.marker = registry.path.with_suffix(".deadline.json")

    def arm(self):
        if self.proc is not None:
            return
        before = self.registry.clock()
        self.proc = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--watchdog",
                str(self.registry.path),
                str(self.hard_ns),
                str(self.cancel),
                str(self.marker),
            ],
            start_new_session=True,
        )
        self.registry.register(self.proc, "watchdog", started_ns=before)

    def disarm(self, deadline_ns):
        self.cancel.touch()
        while self.proc.poll() is None and time.monotonic_ns() < deadline_ns:
            time.sleep(0.01)
        self.registry.reap()


def watchdog_main(path, hard, cancel, marker):
    """Independent process remains live when the parent is blocked in I/O."""
    fired = False
    while not cancel.exists():
        now = time.monotonic_ns()
        if now >= hard - 10 * NS:
            if not fired:
                save_atomic(
                    marker,
                    dict(reason="cleanup reserve reached", at_ns=now, hard_ns=hard),
                )
                fired = True
            try:
                entries = json.loads(path.read_text())["entries"]
            except (OSError, ValueError):
                entries = []
            for entry in entries:
                if entry["pid"] == os.getpid() or entry["ended_ns"] is not None:
                    continue
                try:
                    stat = (
                        Path(f"/proc/{entry['pid']}/stat")
                        .read_text()
                        .rpartition(")")[2]
                        .split()
                    )
                    if stat[19] != entry["proc_start_ticks"]:
                        continue
                    sig = signal.SIGKILL if now >= hard else signal.SIGTERM
                    if os.getpgid(entry["pid"]) == entry["pid"]:
                        os.killpg(entry["pid"], sig)
                    else:
                        os.kill(entry["pid"], sig)
                except (ProcessLookupError, FileNotFoundError):
                    if group_members(entry["pid"]):
                        try:
                            os.killpg(
                                entry["pid"],
                                signal.SIGKILL if now >= hard else signal.SIGTERM,
                            )
                        except ProcessLookupError:
                            pass
            if now >= hard:
                save_atomic(
                    marker,
                    dict(
                        reason="whole hard deadline",
                        at_ns=now,
                        hard_ns=hard,
                        budget_breach=now > hard,
                    ),
                )
                return
        time.sleep(0.02)


if __name__ == "__main__":
    assert sys.argv[1] == "--watchdog"
    watchdog_main(
        Path(sys.argv[2]), int(sys.argv[3]), Path(sys.argv[4]), Path(sys.argv[5])
    )


def resource_sample(registry, evidence_dir, orchestrator_pid=None, pss=False):
    """100ms caller cadence, retained process list only; RSS is conservative sum."""
    import shutil
    from r3_14_isolated_worker import proc_sample, descendants

    memory = {
        line.split(":")[0]: int(line.split()[1]) * 1024
        for line in Path("/proc/meminfo").read_text().splitlines()
        if len(line.split()) == 3
    }
    processes = {e["pid"]: e["role"] for e in registry.entries if e["ended_ns"] is None}
    if orchestrator_pid:
        processes[orchestrator_pid] = "orchestrator"
    for entry in registry.entries:
        if entry["role"] == "server" and entry["ended_ns"] is None:
            for pid in descendants(entry["pid"]):
                processes[pid] = "checkpoint_worker"
    samples = [
        dict(role=role, **proc_sample(pid, pss)) for pid, role in processes.items()
    ]
    tool_rss = sum(
        p.get("rss_bytes", 0)
        for p in samples
        if p["role"] not in ("server", "checkpoint_worker")
    )
    return dict(
        at_ns=time.monotonic_ns(),
        mem_available=memory["MemAvailable"],
        swap_used=memory["SwapTotal"] - memory["SwapFree"],
        disk_free=shutil.disk_usage(evidence_dir).free,
        tool_rss=tool_rss,
        evidence_bytes=sum(
            p.stat().st_size for p in evidence_dir.rglob("*") if p.is_file()
        ),
        processes=samples,
    )


class ResourceSampler:
    def __init__(
        self, registry, evidence_dir, orchestrator_pid, clock=time.monotonic_ns
    ):
        self.registry, self.directory, self.pid, self.clock = (
            registry,
            evidence_dir,
            orchestrator_pid,
            clock,
        )
        self.next_ns = 0
        self.last_pss_ns = None
        self.latest = None

    def tick(self, emit):
        now = self.clock()
        if now < self.next_ns:
            return self.latest
        pss = self.last_pss_ns is None or now - self.last_pss_ns >= NS
        self.latest = resource_sample(self.registry, self.directory, self.pid, pss)
        emit(self.latest)
        self.next_ns = now + 100_000_000
        if pss:
            self.last_pss_ns = now
        return self.latest
