"""Local Python high-level full snapshot probe; mixed socket matrix is R3-14."""

from __future__ import annotations

import argparse
import asyncio
import json
import threading
import time
from pathlib import Path

from query_a_matrix_100k import make_tree
from takler.client.full_snapshot import FullSnapshotClient, FullSnapshotComplete
from takler.exceptions import QueryResponseError
from takler.server.scheduler import Scheduler


def rss_bytes():
    with open("/proc/self/status", encoding="ascii") as source:
        for line in source:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("VmRSS unavailable")


class LocalTransport:
    def __init__(self, scheduler):
        self.service = scheduler.query_service
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()

    def query(self, request):
        if request.kind == "capabilities_request":
            return self.service.capabilities()
        method = {
            "full_snapshot_request": self.service.full_snapshots.page,
            "full_snapshot_chunk_request": self.service.full_snapshots.chunk,
            "cancel_full_snapshot_request": self.service.full_snapshots.cancel,
        }[request.kind]
        return asyncio.run_coroutine_threadsafe(method(request), self.loop).result(
            timeout=120
        )

    def close(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)
        self.loop.close()


def sample(size: int, wide: bool, mode: str) -> dict:
    bunch, _ = make_tree(size, wide)
    scheduler = Scheduler(bunch)
    transport = LocalTransport(scheduler)
    client = FullSnapshotClient(transport)
    baseline = rss_bytes()
    peak = [baseline]
    done = threading.Event()

    def observe():
        while not done.wait(0.05):
            peak[0] = max(peak[0], rss_bytes())

    watcher = threading.Thread(target=observe, daemon=True)
    watcher.start()
    started = time.perf_counter()
    result = {"tasks": size, "wide": wide, "mode": mode}
    try:
        if mode == "assembly":
            full = client.read_full_snapshot()
            result["nodes"] = len(full.nodes_by_path)
            result["complete"] = full.completion.total_nodes == result["nodes"]
            result["total_bytes"] = full.completion.total_bytes
        else:
            count = 0
            completion = None
            for item in client.iter_full_snapshot():
                if isinstance(item, FullSnapshotComplete):
                    completion = item
                else:
                    count += 1
            result["nodes"] = count
            result["complete"] = (
                completion is not None and completion.total_nodes == count
            )
            result["total_bytes"] = completion.total_bytes if completion else None
    except QueryResponseError as exc:
        result["error_code"] = exc.code
        result["error_message"] = exc.message
    finally:
        result["elapsed_s"] = round(time.perf_counter() - started, 3)
        done.set()
        watcher.join(timeout=5)
        result["rss_delta_bytes"] = peak[0] - baseline
        transport.close()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=100000)
    parser.add_argument("--wide", action="store_true")
    parser.add_argument("--mode", choices=("assembly", "stream"), default="assembly")
    parser.add_argument("--output")
    options = parser.parse_args()
    result = sample(options.size, options.wide, options.mode)
    raw = json.dumps(result, indent=2) + "\n"
    if options.output:
        Path(options.output).write_text(raw)
    print(raw)


if __name__ == "__main__":
    main()
