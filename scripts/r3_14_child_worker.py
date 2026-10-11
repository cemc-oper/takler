"""Independent persistent child-command client for R3-14 timing probes."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

from takler.client.service_client import TaklerServiceClient
from takler.protocol.commands import Command


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=("grpc", "http"), required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--attempt", required=True)
    options = parser.parse_args()
    client = TaklerServiceClient(
        "127.0.0.1",
        options.port,
        transport_name=options.transport,
        single_timeout=60,
        retry_window=60,
    )
    client.start()
    path = "/probe/child"
    common = {"node_path": path, "attempt_id": options.attempt}
    print(json.dumps({"ready": True, "pid": os.getpid()}), flush=True)
    try:
        for line in sys.stdin:
            operation = line.strip()
            if operation == "quit":
                break
            started = time.perf_counter_ns()
            cpu_start_s = time.process_time()
            result = {"operation": operation, "start_ns": started}
            try:
                if operation == "init":
                    code = Command.INIT
                    payload = {**common, "task_id": "local-probe"}
                elif operation == "event":
                    code = Command.EVENT
                    payload = {
                        **common,
                        "event_name": "ready",
                        "source_task_path": path,
                    }
                elif operation.startswith("meter:"):
                    code = Command.METER
                    payload = {
                        **common,
                        "meter_name": "progress",
                        "meter_value": operation.split(":", 1)[1],
                        "source_task_path": path,
                    }
                elif operation == "complete":
                    code = Command.COMPLETE
                    payload = common
                else:
                    raise ValueError("unknown child probe command")
                reply = client.transport.call(code, payload)
                if reply.flag:
                    raise RuntimeError(f"child reply flag {reply.flag}")
                result["flag"] = reply.flag
            except Exception as exc:  # noqa: BLE001 - raw load failures are evidence
                result["error"] = f"{type(exc).__name__}: {exc}"
            finally:
                result["end_ns"] = time.perf_counter_ns()
                result["cpu_seconds"] = time.process_time() - cpu_start_s
                print(json.dumps(result, separators=(",", ":")), flush=True)
    finally:
        client.close_channel()


if __name__ == "__main__":
    main()
