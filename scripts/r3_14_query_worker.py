"""Persistent independent Python query client for R3-14 timing probes."""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time

from takler.client.full_snapshot import FullSnapshotComplete
from takler.client.service_client import TaklerServiceClient
from takler.exceptions import QueryResponseError
from takler.protocol.commands import Command


def rss_bytes() -> int:
    with open("/proc/self/statm", encoding="ascii") as source:
        return int(source.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=("grpc", "http"), required=True)
    parser.add_argument("--port", type=int, required=True)
    options = parser.parse_args()
    client = TaklerServiceClient(
        "127.0.0.1",
        options.port,
        transport_name=options.transport,
        single_timeout=60,
        retry_window=60,
    )
    client.start()
    view = None
    print(json.dumps({"ready": True, "pid": os.getpid()}), flush=True)
    try:
        for line in sys.stdin:
            command = line.strip()
            if command == "quit":
                break
            start_ns = time.perf_counter_ns()
            cpu_start_s = time.process_time()
            before_rss = rss_bytes()
            peak_rss = [before_rss]
            finished = threading.Event()

            def sample_rss():
                while not finished.wait(0.01):
                    peak_rss[0] = max(peak_rss[0], rss_bytes())

            sampler = threading.Thread(target=sample_rss, daemon=True)
            sampler.start()
            result: dict = {"command": command, "start_ns": start_ns, "failures": []}
            try:
                if command == "summary":
                    for attempt in range(3):
                        attempt_start = time.perf_counter_ns()
                        try:
                            view = client.run_query_snapshot()
                            result["nodes"] = len(view.ordered_paths)
                            break
                        except QueryResponseError as exc:
                            result["failures"].append(
                                {
                                    "code": exc.code,
                                    "start_ns": attempt_start,
                                    "end_ns": time.perf_counter_ns(),
                                }
                            )
                            if exc.code != "resource_exhausted" or attempt == 2:
                                raise
                            time.sleep(0.2)
                elif command == "since":
                    if view is None:
                        raise RuntimeError("summary view is absent")
                    try:
                        view = client.run_query_since(view)
                    except QueryResponseError as exc:
                        result["failures"].append(
                            {
                                "code": exc.code,
                                "start_ns": start_ns,
                                "end_ns": time.perf_counter_ns(),
                            }
                        )
                        if exc.code != "reset_required":
                            raise
                        view = client.run_query_snapshot()
                        result["rebuilt"] = True
                    if view.stale_scopes:
                        view = client.run_query_snapshot()
                        result["rebuilt"] = True
                    result["changed"] = len(view.changed_paths)
                elif command == "detail":
                    if view is None:
                        raise RuntimeError("summary view is absent")
                    detail = client.run_query_detail(view, "/probe/child", ["runtime"])
                    result["path"] = detail.path
                elif command == "ping":
                    result["received"] = client.run_request_ping() is not None
                elif command == "status":
                    result["flag"] = client.transport.call(
                        Command.SERVER_STATUS, {}
                    ).flag
                elif command == "full_stream":
                    nodes = 0
                    first_ns = None
                    completion = None
                    for item in client.run_iter_full_snapshot():
                        if first_ns is None:
                            first_ns = time.perf_counter_ns()
                        if isinstance(item, FullSnapshotComplete):
                            completion = item
                        else:
                            nodes += 1
                    if completion is None or completion.total_nodes != nodes:
                        raise RuntimeError("incomplete full snapshot")
                    result.update(
                        nodes=nodes,
                        bytes=completion.total_bytes,
                        first_ns=first_ns,
                        sha256=completion.sha256,
                    )
                else:
                    raise ValueError("unknown probe command")
            except Exception as exc:  # noqa: BLE001 - raw load failures are evidence
                result["error"] = f"{type(exc).__name__}: {exc}"
            finally:
                finished.set()
                sampler.join(timeout=1)
                result["end_ns"] = time.perf_counter_ns()
                result["cpu_seconds"] = time.process_time() - cpu_start_s
                result["rss_before_bytes"] = before_rss
                result["rss_peak_bytes"] = max(peak_rss[0], rss_bytes())
                print(json.dumps(result, separators=(",", ":")), flush=True)
    finally:
        client.close_channel()


if __name__ == "__main__":
    main()
