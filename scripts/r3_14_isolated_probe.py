"""Sequential PERF-03 runs, exact diagnostic source copies and owned cleanup."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import difflib
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from r3_14_isolated_worker import save


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True)


def diagnostic_copy(repo, destination):
    destination.mkdir(parents=True)
    save(
        destination / "source-meta.json",
        dict(
            takler_head=git(repo, "rev-parse", "HEAD").strip(),
            takler_git_status=git(repo, "status", "--porcelain=v1", "-uall"),
            captured_utc=datetime.now(timezone.utc).isoformat(),
        ),
    )
    shutil.copytree(
        repo / "src", destination / "src", ignore=shutil.ignore_patterns("__pycache__")
    )
    shutil.copytree(
        repo / "scripts",
        destination / "scripts",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    path = destination / "src/takler/server/full_snapshot.py"
    before = path.read_text()
    after = before
    for old, new in [
        (
            "        if lease is not None and lease.gc_tuned:\n            lease.gc_tuned = False\n            _release_large_snapshot_gc()\n",
            "",
        ),
        (
            "                if len(paths) >= GC_TUNE_MIN_PATHS:\n                    lease.gc_tuned = _acquire_large_snapshot_gc()\n",
            "",
        ),
        (
            "                if page.complete and lease.gc_tuned:\n                    lease.gc_tuned = False\n                    _release_large_snapshot_gc()\n",
            "",
        ),
        (
            "        # Large page streams create many short-lived container objects. A small\n        # collection per page avoids rare whole-generation pauses on the event\n        # loop while preserving normal GC for the rest of the process.\n        if gc.isenabled():\n            gc.collect(0)\n",
            "",
        ),
    ]:
        if after.count(old) != 1:
            raise ValueError("GC diagnostic patch source changed")
        after = after.replace(old, new, 1)
    path.write_text(after)
    patch = "".join(
        difflib.unified_diff(
            before.splitlines(True),
            after.splitlines(True),
            fromfile="a/src/takler/server/full_snapshot.py",
            tofile="b/src/takler/server/full_snapshot.py",
        )
    )
    (destination / "gc.patch").write_text(patch)
    source_patch = git(repo, "diff", "--binary", "HEAD")
    for name in git(repo, "ls-files", "--others", "--exclude-standard").splitlines():
        path = repo / name
        if path.is_file() and path.suffix == ".py":
            source_patch += "".join(
                difflib.unified_diff(
                    [],
                    path.read_text().splitlines(True),
                    fromfile="/dev/null",
                    tofile=f"b/{name}",
                )
            )
    (destination / "tooling.patch").write_text(source_patch)
    files = {
        str(p.relative_to(destination)): digest(p)
        for p in sorted(destination.rglob("*.py"))
    }
    save(destination / "source-index.json", files)
    return files


def safety(config, swap_start):
    memory = {
        line.split(":")[0]: int(line.split()[1]) * 1024
        for line in Path("/proc/meminfo").read_text().splitlines()
        if len(line.split()) == 3
    }
    rule = config["stop_rules"]
    swap = memory["SwapTotal"] - memory["SwapFree"]
    if memory["MemAvailable"] < rule["mem_available_min_bytes"]:
        return "MemAvailable below stop line"
    if swap - swap_start > rule["swap_growth_max_bytes"]:
        return "Swap growth above stop line"
    if (
        shutil.disk_usage(Path(config["runtime"]["run_dir"])).free
        < rule["workspace_free_min_bytes"]
    ):
        return "Workspace disk below stop line"
    return None


class Process:
    def __init__(self, role, args, env, run_dir):
        self.role = role
        self.log = (run_dir / f"{role}.log").open("w")
        self.proc = subprocess.Popen(
            args,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        self.pid = self.proc.pid
        self.args = args

    def send(self, value):
        self.proc.stdin.write(json.dumps(value) + "\n")
        self.proc.stdin.flush()

    def receive(self, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ready, _, _ = select.select(
                [self.proc.stdout], [], [], max(0, deadline - time.monotonic())
            )
            if not ready:
                break
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError(f"{self.role} exited: {self.proc.poll()}")
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                self.log.write(line)
        raise TimeoutError(f"{self.role} control timed out")

    def clock(self, timeout=2):
        nonce = uuid.uuid4().hex
        before = time.monotonic_ns()
        self.send(dict(command="clock", nonce=nonce))
        reply = self.receive(timeout)
        after = time.monotonic_ns()
        if reply.get("nonce") != nonce:
            raise ValueError("clock handshake mismatch")
        return dict(
            pid=self.pid,
            before_ns=before,
            peer_ns=reply["monotonic_ns"],
            after_ns=after,
            uncertainty_ns=(after - before) // 2,
            offset_midpoint_ns=reply["monotonic_ns"] - (before + after) // 2,
            gc=reply.get("gc"),
            lag=reply.get("lag"),
        )

    def cleanup(self, timeout=5):
        # Include session members after leader death (e.g. a checkpoint worker
        # orphaned by the deliberate service SIGKILL).
        children = []
        for entry in Path("/proc").iterdir():
            if entry.name.isdigit():
                try:
                    fields = (entry / "stat").read_text().rpartition(")")[2].split()
                    if (
                        int(fields[3]) == self.pid
                        and int(entry.name) != self.pid
                        and fields[0] != "Z"
                    ):
                        children.append(int(entry.name))
                except (OSError, ValueError):
                    pass
        forced = []
        if self.proc.poll() is None:
            os.killpg(self.pid, signal.SIGTERM)
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(self.pid, signal.SIGKILL)
                forced.append(self.pid)
                self.proc.wait(timeout=2)
        for pid in children:
            try:
                # Child PID reuse is guarded by group identity, established
                # when this process was launched with start_new_session.
                if os.getpgid(pid) == self.pid:
                    os.kill(pid, signal.SIGKILL)
                    forced.append(pid)
            except ProcessLookupError:
                pass
        if children:
            time.sleep(0.1)
        self.log.close()
        remaining = []
        for pid in children:
            try:
                fields = (
                    Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
                )
                if fields[0] != "Z" and int(fields[3]) == self.pid:
                    remaining.append(pid)
            except OSError:
                pass
        return dict(
            pid=self.pid,
            exit_code=self.proc.returncode,
            forced=forced,
            remaining_owned_descendants=remaining,
        )


def run_one(
    repo,
    root,
    source,
    base,
    mode,
    workload,
    index,
    smoke=False,
    fault=None,
    profile_service=False,
    lag_ablation=None,
    clock_diagnostics=None,
):
    if lag_ablation is not None and (mode != "L" or workload != "idle_no_clients"):
        raise ValueError("Lag ablation requires an idle L diagnostic")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_id = f"PERF-03-{stamp}-{workload}-{mode}-r{index}"
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True)
    config = json.loads(json.dumps(base))
    timings = config["performance_runs"]
    if smoke:
        timings.update(
            ready_baseline_no_requests_seconds=2,
            measurement_seconds=12,
            withdrawal_no_new_requests_seconds=2,
            drain_exit_max_seconds=30,
            whole_run_max_seconds=106,
        )
    if fault:
        timings.update(
            ready_baseline_no_requests_seconds=0,
            measurement_seconds=60,
            withdrawal_no_new_requests_seconds=30,
            drain_exit_max_seconds=160,
            whole_run_max_seconds=310,
        )
    config["runtime"] = dict(
        run_id=run_id,
        run_dir=str(run_dir),
        mode=mode,
        workload=workload,
        smoke=smoke,
        fault=fault,
        profile_service=profile_service,
        config_frozen_before_start=True,
        lag_ablation=lag_ablation,
    )
    config["execution_status"] = "running"
    save(run_dir / "config.json", config)
    shutil.copyfile(source / "tooling.patch", run_dir / "source.patch")
    shutil.copyfile(source / "gc.patch", run_dir / "gc.patch")
    manifest = dict(
        schema_version="r3-perf-manifest-v2",
        run_id=run_id,
        task_id="PERF-03",
        decision_id="G0-2026-10-13",
        start_utc=(
            datetime.now(timezone.utc).isoformat()
            if clock_diagnostics is None
            else clock_diagnostics.read(
                "start_utc", lambda: datetime.now(timezone.utc).isoformat()
            )
        ),
        gc_variant="default_no_intervention",
        takler_head=json.loads((source / "source-meta.json").read_text())[
            "takler_head"
        ],
        source_meta=json.loads((source / "source-meta.json").read_text()),
        current_repository_head=git(repo, "rev-parse", "HEAD").strip(),
        takler_client_head=git(
            repo.parent / "takler-client", "rev-parse", "HEAD"
        ).strip(),
        python=sys.version,
        python_executable=sys.executable,
        source_directory=str(source),
        source_index_sha256=digest(source / "source-index.json"),
        lock_sha256=digest(repo / "uv.lock"),
        installed_dependencies={
            name: importlib.metadata.version(name)
            for name in [
                "takler",
                "grpcio",
                "protobuf",
                "pydantic",
                "pytest",
                "hypothesis",
            ]
        },
        go_binary="not used; Python-only diagnostic",
        config_sha256=digest(run_dir / "config.json"),
        tooling_patch_sha256=digest(run_dir / "source.patch"),
        gc_patch_sha256=digest(run_dir / "gc.patch"),
        environment=dict(
            kernel=os.uname().release,
            cpus=len(os.sched_getaffinity(0)),
            filesystem_path=str(run_dir),
        ),
        processes={},
        clock_start={},
        clock_exit={},
        cleanup={},
        exit_reason="in_progress",
    )
    save(run_dir / "manifest.json", manifest)
    processes = []
    started = (
        time.monotonic()
        if clock_diagnostics is None
        else clock_diagnostics.read("started", time.monotonic)
    )
    memory = Path("/proc/meminfo").read_text().splitlines()
    mem = {
        s.split(":")[0]: int(s.split()[1]) * 1024 for s in memory if len(s.split()) == 3
    }
    swap_start = mem["SwapTotal"] - mem["SwapFree"]
    cgroup = Path("/proc/self/cgroup").read_text().strip()
    group_path = Path("/sys/fs/cgroup") / cgroup.rpartition(":")[2].lstrip("/")
    limits = {}
    for name in ["cpu.max", "memory.max", "memory.swap.max"]:
        limits[name] = (
            (group_path / name).read_text().strip()
            if (group_path / name).exists()
            else None
        )
    manifest["environment"].update(
        cpu_model=next(
            line.partition(":")[2].strip()
            for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        ),
        cpu_affinity=sorted(os.sched_getaffinity(0)),
        cgroup=cgroup,
        cgroup_limits=limits,
        memory_at_start=mem,
        swap_used_at_start=swap_start,
        disk_free_at_start=shutil.disk_usage(run_dir).free,
        load_average=os.getloadavg(),
    )
    source_env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("TAKLER_")
        and k.lower() not in {"http_proxy", "https_proxy", "all_proxy", "grpc_proxy"}
    }
    source_env.update(
        PYTHONPATH=f"{source / 'src'}:{source / 'scripts'}", TAKLER_LOG_LEVEL="ERROR"
    )
    worker = source / "scripts/r3_14_isolated_worker.py"
    try:
        with tempfile.TemporaryDirectory(prefix="perf03-") as temporary:
            temp = Path(temporary)
            secret, whitelist = temp / "secret", temp / "whitelist"
            secret.write_text(uuid.uuid4().hex + "\n")
            whitelist.write_text("perf-F\nperf-S\nperf-Q\n")
            secret.chmod(0o600)
            whitelist.chmod(0o600)
            sock = str(temp / "events.sock")
            with socket.socket() as port_socket:
                port_socket.bind(("127.0.0.1", 0))
                port = port_socket.getsockname()[1]
            common = ["--run-dir", str(run_dir), "--socket", sock]

            def launch(role, kind, extra):
                env = dict(source_env, LOGNAME=f"perf-{role}", USER=f"perf-{role}")
                prefix = [sys.executable]
                if kind == "service" and profile_service:
                    prefix += [
                        "-m",
                        "cProfile",
                        "-o",
                        str(run_dir / "service-profile.pstats"),
                    ]
                process = Process(
                    role,
                    [*prefix, str(worker), kind, *common, *extra],
                    env,
                    run_dir,
                )
                processes.append(process)
                ready = process.receive(max(0.1, 60 - (time.monotonic() - started)))
                if not ready.get("ready"):
                    raise ValueError("worker not ready")
                manifest["processes"][role] = dict(
                    pid=process.pid, ready=ready, command=process.args
                )
                return process

            collector = launch("collector", "collector", [])
            capacity = "256" if fault else "8192"
            extra = [
                "--mode",
                mode,
                "--port",
                str(port),
                "--secret",
                str(secret),
                "--whitelist",
                str(whitelist),
                "--capacity",
                capacity,
            ]
            if fault == "backpressure":
                extra.append("--synthetic")
            if lag_ablation is not None:
                extra.extend(["--lag-ablation", lag_ablation])
            server = launch("server", "service", extra)
            clients = []
            if workload == "T3":
                for role in ["F", "S", "Q"]:
                    clients.append(
                        launch(
                            role,
                            "client",
                            [
                                "--role",
                                role,
                                "--port",
                                str(port),
                                "--secret",
                                str(secret),
                            ],
                        )
                    )
            collector.send(
                dict(
                    command="track",
                    pids={p.pid: p.role for p in processes}
                    | {os.getpid(): "orchestrator"},
                )
            )
            for process in processes:
                manifest["clock_start"][process.role] = process.clock()
            baseline_end = (
                time.monotonic() + timings["ready_baseline_no_requests_seconds"]
            )
            while time.monotonic() < baseline_end:
                if clock_diagnostics is not None:
                    clock_diagnostics.check()
                if reason := safety(config, swap_start):
                    raise RuntimeError(reason)
                time.sleep(0.05)
            start_ns = time.monotonic_ns() + 100_000_000
            end_ns = start_ns + int(timings["measurement_seconds"] * 1e9)
            manifest["measurement_start_ns"], manifest["measurement_end_ns"] = (
                start_ns,
                end_ns,
            )
            for process in clients:
                process.send(dict(start_ns=start_ns, end_ns=end_ns))
            if fault == "backpressure":
                collector.send(
                    dict(
                        command="pause",
                        start=start_ns / 1e9 + 10,
                        end=start_ns / 1e9 + 40,
                    )
                )
            save(run_dir / "manifest.json", manifest)
            injected = False
            midpoint_checked = False
            withdrawal_end = (
                end_ns / 1e9 + timings["withdrawal_no_new_requests_seconds"]
            )
            while time.monotonic() < withdrawal_end:
                if clock_diagnostics is not None:
                    clock_diagnostics.check()
                if reason := safety(config, swap_start):
                    raise RuntimeError(reason)
                if time.monotonic() - started >= timings["whole_run_max_seconds"]:
                    raise TimeoutError("whole run hard limit")
                elapsed = time.monotonic() - start_ns / 1e9
                if (
                    not midpoint_checked
                    and elapsed >= timings["measurement_seconds"] / 2
                ):
                    if server.proc.poll() is None:
                        manifest["measurement_gc_handshake"] = server.clock()
                    midpoint_checked = True
                if (
                    fault in {"client_failure", "collector_exit", "service_termination"}
                    and elapsed >= 10
                    and not injected
                ):
                    target = (
                        clients[0]
                        if fault == "client_failure"
                        else collector
                        if fault == "collector_exit"
                        else server
                    )
                    os.killpg(target.pid, signal.SIGKILL)
                    manifest["fault_injection"] = dict(
                        role=target.role,
                        pid=target.pid,
                        at_ns=time.monotonic_ns(),
                        signal="SIGKILL",
                    )
                    injected = True
                for process in processes:
                    expected_dead = (
                        fault == "collector_exit"
                        and process is collector
                        or fault == "service_termination"
                        and process is server
                        or fault == "client_failure"
                        and clients
                        and process is clients[0]
                    )
                    watchdog_exit = (
                        fault == "service_termination"
                        and process.role in {"F", "S", "Q"}
                        and process.proc.poll() == 3
                    )
                    if (
                        process.proc.poll() is not None
                        and not expected_dead
                        and not watchdog_exit
                    ):
                        raise RuntimeError(
                            f"Unexpected {process.role} exit {process.proc.returncode}"
                        )
                time.sleep(0.05)
            # Clients are now idle, but retain their exit-handshake control loop.
            for process in clients:
                if process.proc.poll() is None:
                    manifest["clock_exit"][process.role] = process.clock()
                    process.proc.wait(timeout=5)
            if server.proc.poll() is None:
                manifest["clock_exit"]["server"] = server.clock()
                server.send(dict(command="stop"))
                remaining = timings["whole_run_max_seconds"] - (
                    time.monotonic() - started
                )
                server.proc.wait(
                    timeout=max(0.1, min(timings["drain_exit_max_seconds"], remaining))
                )
            if collector.proc.poll() is None:
                manifest["clock_exit"]["collector"] = collector.clock()
                collector.send(dict(command="stop"))
                collector.proc.wait(timeout=5)
            manifest["exit_reason"] = (
                "completed_fault_scenario" if fault else "completed"
            )
    except Exception as exc:
        manifest["exit_reason"] = f"{type(exc).__name__}: {exc}"
    finally:
        for process in reversed(processes):
            manifest["cleanup"][process.role] = process.cleanup(timeout=2)
        manifest["end_utc"] = (
            datetime.now(timezone.utc).isoformat()
            if clock_diagnostics is None
            else clock_diagnostics.read(
                "end_utc", lambda: datetime.now(timezone.utc).isoformat()
            )
        )
        manifest["elapsed_seconds"] = (
            time.monotonic()
            if clock_diagnostics is None
            else clock_diagnostics.read("ended", time.monotonic)
        ) - started
        manifest["evidence_files"] = {
            p.name: digest(p)
            for p in sorted(run_dir.iterdir())
            if p.is_file() and p.name != "manifest.json"
        }
        save(run_dir / "manifest.json", manifest)
    from r3_14_isolated_analyze import analyze

    summary = analyze(run_dir)
    save(run_dir / "summary.json", summary)
    print(
        json.dumps(
            dict(
                run_id=run_id,
                exit_reason=manifest["exit_reason"],
                summary=summary["evidence_status"],
            )
        ),
        flush=True,
    )
    return run_dir, summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--profile-service", action="store_true")
    parser.add_argument(
        "--fault",
        choices=[
            "backpressure",
            "client_failure",
            "collector_exit",
            "service_termination",
        ],
    )
    parser.add_argument("--mode", choices=["O", "L", "D"])
    parser.add_argument("--workload", choices=["idle_no_clients", "T3"])
    parser.add_argument("--lag-ablation", choices=["off", "wake_only", "record"])
    options = parser.parse_args()
    if options.profile_service and not options.smoke:
        raise ValueError("Service profiling requires a separately labelled smoke run")
    base = json.loads(options.config.read_text())
    if options.lag_ablation and (
        options.mode != "L"
        or options.workload != "idle_no_clients"
        or not base.get("repair_steps", {}).get("lag_ablation")
    ):
        raise ValueError("Pre-register lag ablation and select idle L explicitly")
    if base["status"] != "user_adopted":
        raise ValueError("An adopted config is required")
    repo = Path(__file__).resolve().parents[1]
    root = options.config.resolve().parent
    source = (
        root
        / f"diagnostic-source-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    )
    diagnostic_copy(repo, source)
    groups = (
        [options.workload]
        if options.workload
        else base["performance_runs"]["workload_groups"]
    )
    orders = (
        [[options.mode]]
        if options.mode
        else [["O", "L", "D"]]
        if options.smoke
        else base["performance_runs"]["round_orders_per_group"]
    )
    count = 0
    for group in groups:
        for order in orders:
            for mode in order:
                count += 1
                path, summary = run_one(
                    repo,
                    root,
                    source,
                    base,
                    mode,
                    group,
                    count,
                    options.smoke,
                    options.fault,
                    options.profile_service,
                    options.lag_ablation,
                )
                if (summary["semantic_errors"] and not options.fault) or summary[
                    "exit_reason"
                ] not in {
                    "completed",
                    "completed_fault_scenario",
                }:
                    print(f"Batch stopped; preserve evidence at {path}", flush=True)
                    return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
