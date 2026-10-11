"""Record real periodic checkpoint timing and event-loop lag for an R3 tree.

Run from repo/takler. This probe is diagnostic; it does not replace the
R3-14 cross-client mixed-load acceptance matrix.
"""

from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gc
import json
import os
from pathlib import Path
import runpy
import select
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from uuid import uuid4

from query_a_matrix_100k import child_environment, make_tree
from takler.core import NodeStatus
from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server


def rss_bytes(pid: int) -> int:
    try:
        with open(f"/proc/{pid}/statm", encoding="ascii") as source:
            return int(source.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, ProcessLookupError):
        return 0


def cpu_seconds(pid: int) -> float:
    try:
        with open(f"/proc/{pid}/stat", encoding="ascii") as source:
            fields = source.read().rpartition(")")[2].split()
            return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except (FileNotFoundError, ProcessLookupError):
        return 0.0


async def observe(manager, duration: float, lag: list[dict], resources: list[dict]):
    interval = 0.005
    target = time.perf_counter() + interval
    end = time.perf_counter() + duration
    next_resource = time.perf_counter()
    while time.perf_counter() < end:
        await asyncio.sleep(max(0, target - time.perf_counter()))
        now = time.perf_counter()
        item = {
            "at_ns": time.perf_counter_ns(),
            "lag_ms": max(0, now - target) * 1000,
            "missed_intervals": max(0, int((now - target) / interval)),
        }
        if item["lag_ms"] > 200:
            frames = sys._current_frames()
            item["threads"] = {
                thread.name: [
                    f"{frame.name}:{frame.lineno}"
                    for frame in traceback.extract_stack(frames[thread.ident])[-6:]
                ]
                for thread in threading.enumerate()
                if thread.ident in frames
            }
        lag.append(item)
        target = now + interval
        if now >= next_resource:
            pool = manager._background_pool
            worker_pids = list(pool._processes) if pool is not None else []
            scheduler = manager.query_scheduler
            full_service = scheduler.query_service.full_snapshots
            current_version = scheduler.version_store.current
            resources.append(
                {
                    "at_ns": time.perf_counter_ns(),
                    "wall_ns": time.time_ns(),
                    "server_rss_bytes": rss_bytes(os.getpid()),
                    "server_cpu_seconds": time.process_time(),
                    "gc_threshold": gc.get_threshold(),
                    "worker_rss_bytes": sum(rss_bytes(pid) for pid in worker_pids),
                    "worker_cpu_seconds": sum(cpu_seconds(pid) for pid in worker_pids),
                    "background_writes": len(manager._background_writes),
                    "reserved_bytes": manager._background_reserved,
                    "last_checkpoint_at": manager.last_checkpoint_at,
                    "full_snapshot_leases": len(full_service._leases),
                    "summary_snapshot_leases": len(scheduler.query_service._snapshots),
                    "version_retained_bytes": current_version.retained_bytes
                    if current_version is not None
                    else 0,
                }
            )
            next_resource = now + 0.1


def run(
    size: int,
    wide: bool,
    interval: float,
    duration: float,
    components: set[str],
    full_readers: int,
    sync_transport: str = "http",
    child_transport: str = "grpc",
    full_transport: str = "http",
    full_language: str = "go",
) -> dict:
    fixture_started_utc = datetime.now(timezone.utc).isoformat()
    fixtures = runpy.run_path(str(Path.cwd() / "tests/integration/conftest.py"))
    bunch, _ = make_tree(size, wide)
    events: list[dict] = []
    lag: list[dict] = []
    resources: list[dict] = []
    requests: list[dict] = []
    page_events: list[dict] = []
    chunk_events: list[dict] = []
    gc_started: dict[tuple[int, int], int] = {}
    gc_totals: dict[int, dict[str, int]] = {}

    def observe_gc(phase, info):
        key = (threading.get_ident(), info["generation"])
        now_ns = time.perf_counter_ns()
        if phase == "start":
            gc_started[key] = now_ns
        else:
            start_ns = gc_started.pop(key, None)
            if start_ns is not None:
                elapsed_ns = now_ns - start_ns
                totals = gc_totals.get(info["generation"])
                if totals is None:
                    totals = {"count": 0, "duration_ns": 0}
                    gc_totals[info["generation"]] = totals
                totals["count"] += 1
                totals["duration_ns"] += elapsed_ns
                # Retaining every sub-millisecond collection creates hundreds
                # of thousands of probe objects during long runs and skews GC.
                if elapsed_ns >= 1_000_000:
                    events.append(
                        {
                            "phase": "gc",
                            "generation": info["generation"],
                            "start_ns": start_ns,
                            "end_ns": now_ns,
                            "collected": info["collected"],
                        }
                    )

    gc.callbacks.append(observe_gc)
    with tempfile.TemporaryDirectory(prefix="r3-14-periodic-") as temporary:
        checkpoint = Path(temporary) / "state.check"
        grpc_port, http_port = fixtures["free_port"](), fixtures["free_port"]()
        config = ConnectConfig(
            server=Server(
                address=Address(
                    hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)
                ),
                http=HttpSettings(host="127.0.0.1", port=str(http_port)),
            )
        )
        runner = fixtures["ServerRunner"](
            checkpoint_file=checkpoint,
            checkpoint_interval=3600.0,
            interval_main_loop=10.0,
            port=grpc_port,
            connect_config=config,
        )
        runner.start()
        try:
            for flow in bunch.flows.values():
                flow.begun = False
                runner.server.bunch.add_flow(flow)
            if components:
                flow = runner.server.bunch.add_flow("probe")
                flow.begun = False
                task = flow.add_task("child")
                task.add_event("ready")
                task.add_meter("progress", 0, 10)
                task.attempt_id = str(uuid4())
                task.try_no = 1
                task.job_password = "r3-14-local-only"
                task.state.node_status = NodeStatus.submitted

            async def initialize():
                return runner.server.scheduler.version_store.pin().state_version

            state_version = asyncio.run_coroutine_threadsafe(
                initialize(), runner._loop
            ).result(timeout=60)
            manager = runner.server.checkpoint_manager
            original_write = manager.write_checkpoint_async
            original_pin = manager._pin_fixed_for_write
            original_publish = manager._publish_prepared

            def pin():
                at_ns = time.perf_counter_ns()
                value = original_pin()
                events.append(
                    {
                        "phase": "capture",
                        "start_ns": at_ns,
                        "end_ns": time.perf_counter_ns(),
                        "sequence": value[0],
                        "success": value[1] is not None,
                    }
                )
                return value

            def publish(sequence, prepared):
                at_ns = time.perf_counter_ns()
                value = original_publish(sequence, prepared)
                events.append(
                    {
                        "phase": "publish",
                        "start_ns": at_ns,
                        "end_ns": time.perf_counter_ns(),
                        "sequence": sequence,
                        "success": value[0],
                    }
                )
                return value

            async def write():
                at_ns = time.perf_counter_ns()
                result = await original_write()
                events.append(
                    {
                        "phase": "periodic_total",
                        "start_ns": at_ns,
                        "end_ns": time.perf_counter_ns(),
                        "success": result,
                    }
                )
                return result

            manager._pin_fixed_for_write = pin
            manager._publish_prepared = publish
            manager.write_checkpoint_async = write
            full_service = runner.server.scheduler.query_service.full_snapshots
            original_page = full_service.page
            original_chunk = full_service.chunk

            async def counted_page(request):
                started = time.perf_counter_ns()
                result = await original_page(request)
                page_events.append(
                    {
                        "start_ns": started,
                        "end_ns": time.perf_counter_ns(),
                        "page_index": getattr(result, "page_index", None),
                        "snapshot_id": getattr(result, "full_snapshot_id", None),
                        "error_code": getattr(result, "code", None),
                    }
                )
                return result

            async def counted_chunk(request):
                started = time.perf_counter_ns()
                result = await original_chunk(request)
                chunk_events.append(
                    {
                        "start_ns": started,
                        "end_ns": time.perf_counter_ns(),
                        "error_code": getattr(result, "code", None),
                        "snapshot_id": getattr(result, "full_snapshot_id", None),
                    }
                )
                return result

            full_service.page = counted_page
            full_service.chunk = counted_chunk

            async def start_periodic():
                old_task = manager._snapshot_task
                if old_task is not None:
                    old_task.cancel()
                    try:
                        await old_task
                    except asyncio.CancelledError:
                        pass
                manager._snapshot_task = None
                manager.interval = interval
                await manager.start()

            asyncio.run_coroutine_threadsafe(start_periodic(), runner._loop).result(
                timeout=10
            )
            sample_start_ns = time.perf_counter_ns()
            observer = asyncio.run_coroutine_threadsafe(
                observe(manager, duration, lag, resources), runner._loop
            )
            if components:
                cutoff = time.perf_counter() + duration

                def record(kind, action):
                    start_ns = time.perf_counter_ns()
                    error = None
                    result = None
                    try:
                        result = action()
                    except Exception as exc:  # noqa: BLE001 - preserve load failures
                        error = f"{type(exc).__name__}: {exc}"
                    item = {
                        "kind": kind,
                        "start_ns": start_ns,
                        "end_ns": time.perf_counter_ns(),
                        "error": error,
                    }
                    if result is not None:
                        item.update(result)
                    requests.append(item)
                    return error is None

                def sync_reader():
                    query_port = http_port if sync_transport == "http" else grpc_port
                    command = [
                        sys.executable,
                        str(Path.cwd() / "scripts/r3_14_query_worker.py"),
                        "--transport",
                        sync_transport,
                        "--port",
                        str(query_port),
                    ]
                    worker = subprocess.Popen(
                        command,
                        env=child_environment(sync_transport),
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        bufsize=1,
                    )
                    assert worker.stdin is not None and worker.stdout is not None

                    def receive():
                        if not select.select([worker.stdout], [], [], 180)[0]:
                            raise TimeoutError("independent query worker did not reply")
                        line = worker.stdout.readline()
                        if not line:
                            raise RuntimeError("independent query worker exited")
                        return json.loads(line)

                    ready = receive()
                    if not ready.get("ready"):
                        raise RuntimeError("independent query worker did not start")

                    def send(name):
                        started = time.perf_counter_ns()
                        worker.stdin.write(name + "\n")
                        worker.stdin.flush()
                        result = receive()
                        requests.append(
                            {
                                "kind": f"{name}_python_{sync_transport}"
                                if name != "summary"
                                else f"summary_initial_python_{sync_transport}",
                                "start_ns": started,
                                "end_ns": time.perf_counter_ns(),
                                "error": result.get("error"),
                                "worker": result,
                            }
                        )

                    try:
                        next_initial = 0.0
                        next_control = 0.0
                        while time.perf_counter() < cutoff:
                            if time.perf_counter() >= next_initial:
                                send("summary")
                                next_initial = time.perf_counter() + 10
                            else:
                                send("since")
                                send("detail")
                            if time.perf_counter() >= next_control:
                                send("ping")
                                send("status")
                                next_control = time.perf_counter() + 1
                            time.sleep(0.01)
                    finally:
                        if worker.poll() is None:
                            worker.stdin.write("quit\n")
                            worker.stdin.flush()
                        worker.wait(timeout=10)

                def child_writer():
                    child_port = http_port if child_transport == "http" else grpc_port
                    command = [
                        sys.executable,
                        str(Path.cwd() / "scripts/r3_14_child_worker.py"),
                        "--transport",
                        child_transport,
                        "--port",
                        str(child_port),
                        "--attempt",
                        task.attempt_id,
                    ]
                    worker = subprocess.Popen(
                        command,
                        env=child_environment(child_transport),
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        bufsize=1,
                    )
                    assert worker.stdin is not None and worker.stdout is not None

                    def receive():
                        if not select.select([worker.stdout], [], [], 180)[0]:
                            raise TimeoutError("independent child worker did not reply")
                        line = worker.stdout.readline()
                        if not line:
                            raise RuntimeError("independent child worker exited")
                        return json.loads(line)

                    ready = receive()
                    if not ready.get("ready"):
                        raise RuntimeError("independent child worker did not start")

                    def send(operation, kind):
                        started = time.perf_counter_ns()
                        worker.stdin.write(operation + "\n")
                        worker.stdin.flush()
                        result = receive()
                        requests.append(
                            {
                                "kind": kind,
                                "start_ns": started,
                                "end_ns": time.perf_counter_ns(),
                                "error": result.get("error"),
                                "worker": result,
                            }
                        )

                    try:
                        send("init", f"child_init_python_{child_transport}")
                        index = 0
                        while time.perf_counter() < cutoff:
                            send(
                                f"meter:{index % 11}",
                                f"child_meter_python_{child_transport}",
                            )
                            if index % 10 == 0:
                                send("event", f"child_event_python_{child_transport}")
                            index += 1
                            time.sleep(0.01)
                        send("complete", f"child_complete_python_{child_transport}")
                    finally:
                        if worker.poll() is None:
                            worker.stdin.write("quit\n")
                            worker.stdin.flush()
                        worker.wait(timeout=10)

                def full_reader():
                    if full_language == "python":
                        full_reader_python()
                        return
                    go_binary = Path.cwd().parent / "takler-client/bin/takler_client"
                    if not go_binary.is_file():
                        raise RuntimeError("build the Go client before mixed probe")
                    full_port = http_port if full_transport == "http" else grpc_port
                    command = [
                        str(go_binary),
                        "snapshot",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(full_port),
                        "--stream",
                    ]

                    def read():
                        pages_before = len(page_events)
                        chunks_before = len(chunk_events)
                        with tempfile.TemporaryFile(mode="w+b") as output:
                            with tempfile.TemporaryFile(mode="w+b") as error_output:
                                process = subprocess.Popen(
                                    command,
                                    env=child_environment(full_transport),
                                    stdout=output,
                                    stderr=error_output,
                                )
                                baseline_rss = rss_bytes(process.pid)
                                peak_rss = baseline_rss
                                peak_cpu = cpu_seconds(process.pid)
                                first_ns = None
                                deadline = time.monotonic() + 180
                                while process.poll() is None:
                                    if time.monotonic() >= deadline:
                                        process.kill()
                                        process.wait()
                                        raise TimeoutError(
                                            "full snapshot CLI timed out"
                                        )
                                    peak_rss = max(peak_rss, rss_bytes(process.pid))
                                    peak_cpu = max(peak_cpu, cpu_seconds(process.pid))
                                    if (
                                        first_ns is None
                                        and os.fstat(output.fileno()).st_size
                                    ):
                                        first_ns = time.perf_counter_ns()
                                    time.sleep(0.01)
                                peak_rss = max(peak_rss, rss_bytes(process.pid))
                                if process.returncode:
                                    error_output.seek(0)
                                    raise RuntimeError(
                                        error_output.read()[-1000:].decode(
                                            errors="replace"
                                        )
                                    )
                            output_bytes = os.fstat(output.fileno()).st_size
                            output.seek(max(0, output_bytes - 4096))
                            last_line = output.read().splitlines()[-1]
                            item = json.loads(last_line)
                            completion = item.get("complete")
                            expected_nodes = size + (size + 999) // 1000 + 3
                            if (
                                completion is None
                                or completion["total_nodes"] != expected_nodes
                            ):
                                raise RuntimeError("incomplete full snapshot")
                            snapshot_id = completion.get("full_snapshot_id")
                            pages = (
                                sum(
                                    event["snapshot_id"] == snapshot_id
                                    for event in page_events
                                )
                                if snapshot_id
                                else len(page_events) - pages_before
                            )
                            chunks = (
                                sum(
                                    event["snapshot_id"] == snapshot_id
                                    for event in chunk_events
                                )
                                if snapshot_id
                                else len(chunk_events) - chunks_before
                            )
                            return {
                                "nodes": expected_nodes,
                                "bytes": completion["total_bytes"],
                                "stdout_bytes": output_bytes,
                                "first_batch_ns": first_ns,
                                "rss_before_bytes": baseline_rss,
                                "rss_peak_bytes": peak_rss,
                                "cpu_seconds": peak_cpu,
                                "snapshot_id": snapshot_id,
                                "page_calls": pages,
                                "chunk_calls": chunks,
                            }

                    while time.perf_counter() < cutoff:
                        record(f"full_stream_go_{full_transport}", read)

                def full_reader_python():
                    full_port = http_port if full_transport == "http" else grpc_port
                    worker = subprocess.Popen(
                        [
                            sys.executable,
                            str(Path.cwd() / "scripts/r3_14_query_worker.py"),
                            "--transport",
                            full_transport,
                            "--port",
                            str(full_port),
                        ],
                        env=child_environment(full_transport),
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        bufsize=1,
                    )
                    assert worker.stdin is not None and worker.stdout is not None

                    def receive():
                        if not select.select([worker.stdout], [], [], 180)[0]:
                            raise TimeoutError("independent full reader did not reply")
                        line = worker.stdout.readline()
                        if not line:
                            raise RuntimeError("independent full reader exited")
                        return json.loads(line)

                    try:
                        if not receive().get("ready"):
                            raise RuntimeError("independent full reader did not start")

                        def read():
                            worker.stdin.write("full_stream\n")
                            worker.stdin.flush()
                            result = receive()
                            if result.get("error"):
                                raise RuntimeError(result["error"])
                            expected_nodes = size + (size + 999) // 1000 + 3
                            if result.get("nodes") != expected_nodes:
                                raise RuntimeError("incomplete full snapshot")
                            snapshot_id = result.get("snapshot_id")
                            return {
                                "nodes": expected_nodes,
                                "bytes": result["bytes"],
                                "first_batch_ns": result["first_ns"],
                                "rss_before_bytes": result["rss_before_bytes"],
                                "rss_peak_bytes": result["rss_peak_bytes"],
                                "cpu_seconds": result["cpu_seconds"],
                                "snapshot_id": snapshot_id,
                                "page_calls": sum(
                                    event["snapshot_id"] == snapshot_id
                                    for event in page_events
                                ),
                                "chunk_calls": sum(
                                    event["snapshot_id"] == snapshot_id
                                    for event in chunk_events
                                ),
                            }

                        while time.perf_counter() < cutoff:
                            record(f"full_stream_python_{full_transport}", read)
                    finally:
                        if worker.poll() is None:
                            worker.stdin.write("quit\n")
                            worker.stdin.flush()
                        worker.wait(timeout=10)

                with ThreadPoolExecutor(max_workers=2 + full_readers) as executor:
                    jobs = [
                        executor.submit(action)
                        for name, action in (
                            ("sync", sync_reader),
                            ("child", child_writer),
                        )
                        if name in components
                    ]
                    if "full" in components:
                        jobs.extend(
                            executor.submit(full_reader) for _ in range(full_readers)
                        )
                    for job in jobs:
                        job.result(timeout=duration + 240)
            observer.result(timeout=duration + 240)
            manager.write_checkpoint_async = original_write
            manager._pin_fixed_for_write = original_pin
            manager._publish_prepared = original_publish
            full_service.page = original_page
            full_service.chunk = original_chunk
            checkpoint_bytes = checkpoint.stat().st_size if checkpoint.exists() else 0
            state = {
                "checkpoint_bytes_before_stop": checkpoint_bytes,
                "last_checkpoint_at": manager.last_checkpoint_at,
                "failure_reason": manager.checkpoint_failure_reason,
                "state_version": state_version,
            }
        finally:
            try:
                runner.stop(timeout=120)
            finally:
                gc.callbacks.remove(observe_gc)
    return {
        "fixture_started_utc": fixture_started_utc,
        "sample_start_ns": sample_start_ns,
        "python": sys.version.split()[0],
        "size": size,
        "wide": wide,
        "interval_s": interval,
        "duration_s": duration,
        "components": sorted(components),
        "full_readers": full_readers,
        "sync_transport": sync_transport,
        "child_transport": child_transport,
        "full_transport": full_transport,
        "full_language": full_language,
        "state": state,
        "events": events,
        "gc_totals": gc_totals,
        "requests": requests,
        "page_events": page_events,
        "chunk_events": chunk_events,
        "lag": lag,
        "resources": resources,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=100_000)
    parser.add_argument("--wide", action="store_true")
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--duration", type=float, default=35.0)
    parser.add_argument("--mixed", action="store_true")
    parser.add_argument("--components", choices=("sync", "child", "full"), nargs="+")
    parser.add_argument("--full-readers", type=int, default=1)
    parser.add_argument("--sync-transport", choices=("http", "grpc"), default="http")
    parser.add_argument("--child-transport", choices=("http", "grpc"), default="grpc")
    parser.add_argument("--full-transport", choices=("http", "grpc"), default="http")
    parser.add_argument("--full-language", choices=("go", "python"), default="go")
    parser.add_argument("--output", type=Path, required=True)
    options = parser.parse_args()
    if options.full_readers < 1:
        parser.error("--full-readers must be positive")
    components = set(
        options.components or (("sync", "child", "full") if options.mixed else ())
    )
    result = run(
        options.size,
        options.wide,
        options.interval,
        options.duration,
        components,
        options.full_readers,
        options.sync_transport,
        options.child_transport,
        options.full_transport,
        options.full_language,
    )
    options.output.parent.mkdir(parents=True, exist_ok=True)
    options.output.write_text(json.dumps(result, separators=(",", ":")) + "\n")
    totals = [event for event in result["events"] if event["phase"] == "periodic_total"]
    print(
        json.dumps(
            {
                "periodic_count": len(totals),
                "successful": sum(event["success"] for event in totals),
                "duration_s": [
                    round((event["end_ns"] - event["start_ns"]) / 1e9, 3)
                    for event in totals
                ],
                "max_lag_ms": max(
                    (item["lag_ms"] for item in result["lag"]), default=0
                ),
                "checkpoint_bytes": result["state"]["checkpoint_bytes_before_stop"],
                "request_count": len(result["requests"]),
                "request_failures": sum(
                    item["error"] is not None for item in result["requests"]
                ),
            }
        )
    )


if __name__ == "__main__":
    main()
