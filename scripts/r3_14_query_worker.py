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
    parser.add_argument("--phase-timings", action="store_true")
    options = parser.parse_args()
    client = TaklerServiceClient(
        "127.0.0.1",
        options.port,
        transport_name=options.transport,
        single_timeout=60,
        retry_window=60,
    )
    client.start()
    active_phase = [None]
    if options.phase_timings:
        if options.transport == "grpc":
            import takler.client.grpc_transport as transport_module

            decode_name = "query_from_pb2"
        else:
            import takler.client.http_transport as transport_module

            decode_name = "decode_query_document"
        original_decode = getattr(transport_module, decode_name)
        original_query = client.transport.query

        def timed_decode(raw, *args, **kwargs):
            phase = active_phase[0]
            if phase is None:
                return original_decode(raw, *args, **kwargs)
            started = time.perf_counter_ns()
            reply = original_decode(raw, *args, **kwargs)
            phase["decode_events"].append(
                {
                    "duration_ns": time.perf_counter_ns() - started,
                    "bytes": len(raw.json) if options.transport == "grpc" else len(raw),
                }
            )
            return reply

        def timed_query(document):
            phase = active_phase[0]
            if phase is None or document.kind != "page_request":
                return original_query(document)
            before_decode = len(phase["decode_events"])
            started = time.perf_counter_ns()
            reply = original_query(document)
            ended = time.perf_counter_ns()
            decoded = phase["decode_events"][before_decode:]
            phase["pages"].append(
                {
                    "start_ns": started,
                    "end_ns": ended,
                    "decode_ns": sum(item["duration_ns"] for item in decoded),
                    "bytes": sum(item["bytes"] for item in decoded),
                    "snapshot_id": getattr(reply, "snapshot_id", None),
                    "page_index": getattr(reply, "page_index", None),
                    "error_code": getattr(reply, "code", None),
                }
            )
            return reply

        setattr(transport_module, decode_name, timed_decode)
        client.transport.query = timed_query
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
                    phase = {"pages": [], "decode_events": []}
                    if options.phase_timings:
                        active_phase[0] = phase
                    summary_started = time.perf_counter_ns()
                    try:
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
                    finally:
                        if options.phase_timings:
                            phase["client_total_ns"] = (
                                time.perf_counter_ns() - summary_started
                            )
                            phase["transport_ns"] = sum(
                                item["end_ns"] - item["start_ns"]
                                for item in phase["pages"]
                            )
                            phase["decode_ns"] = sum(
                                item["duration_ns"]
                                for item in phase.pop("decode_events")
                            )
                            phase["client_other_ns"] = (
                                phase["client_total_ns"] - phase["transport_ns"]
                            )
                            result["summary_phases"] = phase
                            active_phase[0] = None
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
                        snapshot_id=completion.full_snapshot_id,
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
