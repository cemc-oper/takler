"""Local fixed full-detail snapshot probe; final mixed-load gate is R3-14."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

from query_a_matrix_100k import make_tree
from takler.protocol.query_v1 import FullSnapshotRequest
from takler.server.scheduler import Scheduler


def rss_bytes():
    with open("/proc/self/status", encoding="ascii") as source:
        for line in source:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("VmRSS unavailable")


async def sample(size: int, wide: bool) -> dict:
    bunch, _ = make_tree(size, wide)
    scheduler = Scheduler(bunch)
    base = rss_bytes()
    lag = []
    peak = base
    done = False

    async def observe():
        nonlocal peak
        deadline = time.perf_counter() + 0.005
        while not done:
            await asyncio.sleep(max(0, deadline - time.perf_counter()))
            now = time.perf_counter()
            lag.append(max(0, now - deadline))
            deadline = now + 0.005
            peak = max(peak, rss_bytes())

    watch = asyncio.create_task(observe())
    await asyncio.sleep(0.02)
    request = FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
    start = time.perf_counter()
    last_progress = start
    max_no_progress = 0.0
    pages = 0
    first_s = None
    result = None
    while True:
        result = await scheduler.query_service.full_snapshots.page(request)
        now = time.perf_counter()
        max_no_progress = max(max_no_progress, now - last_progress)
        last_progress = now
        pages += 1
        if first_s is None:
            first_s = time.perf_counter() - start
        if result.kind == "error" or result.complete:
            break
        request = request.model_copy(
            update={
                "full_snapshot_id": result.full_snapshot_id,
                "cursor": result.next_cursor,
            }
        )
    elapsed = time.perf_counter() - start
    done = True
    await watch
    return {
        "tasks": size,
        "wide": wide,
        "success": result.kind == "full_snapshot_page" and result.complete,
        "error_code": result.code if result.kind == "error" else None,
        "elapsed_s": round(elapsed, 3),
        "max_no_progress_s": round(max_no_progress, 3),
        "rejected_requests": int(result.kind == "error"),
        "retries": 0,
        "first_page_s": round(first_s, 3),
        "pages": pages,
        "total_nodes": getattr(result, "total_nodes", None),
        "total_bytes": getattr(result, "total_bytes", None),
        "version_retained_bytes": scheduler.version_store.pin().retained_bytes,
        "max_loop_lag_ms": round(max(lag) * 1000, 2),
        "p95_loop_lag_ms": round(sorted(lag)[int(len(lag) * 0.95)] * 1000, 2),
        "rss_delta_bytes": peak - base,
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
