"""Local R3-07 fixed-root handoff, checkpoint, and loop-lag probe."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import tempfile
import time
from pathlib import Path

from query_a_matrix_100k import make_tree
from takler.server.checkpoint import CheckpointManager
from takler.server.scheduler import Scheduler


def _rss(pid: int) -> int:
    try:
        with open(f"/proc/{pid}/status", encoding="ascii") as source:
            for line in source:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except (FileNotFoundError, ProcessLookupError):
        pass
    return 0


async def sample(size: int, wide: bool) -> dict:
    bunch, _ = make_tree(size, wide)
    scheduler = Scheduler(bunch)
    version = scheduler.version_store.pin()
    with tempfile.TemporaryDirectory(prefix="r3-07-") as temp:
        manager = CheckpointManager(bunch, checkpoint_file=Path(temp) / "state.check")
        manager.query_scheduler = scheduler
        scheduler.checkpoint_manager = manager
        base_rss = _rss(os.getpid())
        lag = []
        peak_parent = base_rss
        peak_worker = 0
        done = False

        async def observe():
            nonlocal peak_parent, peak_worker
            deadline = time.perf_counter() + 0.005
            while not done:
                await asyncio.sleep(max(0, deadline - time.perf_counter()))
                now = time.perf_counter()
                lag.append(max(0, now - deadline))
                deadline = now + 0.005
                peak_parent = max(peak_parent, _rss(os.getpid()))
                if manager._background_pool is not None:
                    for pid in manager._background_pool._processes:
                        peak_worker = max(peak_worker, _rss(pid))

        watch = asyncio.create_task(observe())
        await asyncio.sleep(0.02)
        start = time.perf_counter()
        success = await manager.write_checkpoint_async()
        elapsed = time.perf_counter() - start
        done = True
        await watch
        await asyncio.sleep(0)
        payload_size = manager.checkpoint_file.stat().st_size if success else 0
        if manager._background_pool is not None:
            await asyncio.to_thread(manager._background_pool.shutdown, wait=True)
        return {
            "tasks": size,
            "wide": wide,
            "success": success,
            "elapsed_s": round(elapsed, 3),
            "version_retained_bytes": version.retained_bytes,
            "payload_bytes": payload_size,
            "max_loop_lag_ms": round(max(lag) * 1000, 2),
            "p95_loop_lag_ms": round(sorted(lag)[int(len(lag) * 0.95)] * 1000, 2),
            "parent_rss_delta_bytes": peak_parent - base_rss,
            "worker_peak_rss_bytes": peak_worker,
            "child_rusage_peak_bytes": resource.getrusage(
                resource.RUSAGE_CHILDREN
            ).ru_maxrss
            * 1024,
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", type=int, nargs="+", default=[1000, 10000])
    parser.add_argument("--wide", action="store_true")
    parser.add_argument("--output")
    options = parser.parse_args()
    results = [asyncio.run(sample(size, options.wide)) for size in options.sizes]
    raw = json.dumps(results, indent=2) + "\n"
    if options.output:
        Path(options.output).write_text(raw)
    print(raw)


if __name__ == "__main__":
    main()
