"""Join independent-client and server phase timings for R3-14 summaries."""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path

from r3_14_analyze import distribution


def analyze(raw: dict) -> dict:
    by_snapshot: dict[str, dict[str, object]] = defaultdict(
        lambda: {"capture": None, "pages": [], "wire": [], "rpc": []}
    )
    for event in raw.get("phase_events", []):
        snapshot_id = event.get("snapshot_id")
        if not snapshot_id:
            continue
        group = by_snapshot[snapshot_id]
        if event["phase"] == "summary_capture":
            group["capture"] = event
        elif event["phase"] == "summary_page":
            group["pages"].append(event)
        elif event["phase"] == "summary_wire_encode":
            group["wire"].append(event)
        elif event["phase"] == "summary_grpc_rpc":
            group["rpc"].append(event)

    windows = [
        (event["start_ns"], event["end_ns"])
        for event in raw["events"]
        if event["phase"] == "periodic_total"
    ]
    joined = []
    for request in raw["requests"]:
        if not request["kind"].startswith("summary_initial_python_"):
            continue
        phase = request.get("worker", {}).get("summary_phases")
        if not phase or not phase["pages"]:
            continue
        snapshot_id = phase["pages"][0]["snapshot_id"]
        group = by_snapshot.get(snapshot_id, {})
        capture = group.get("capture")
        server_pages = group.get("pages", [])
        wire = group.get("wire", [])
        rpc = group.get("rpc", [])
        page_by_index = {event["page_index"]: event for event in server_pages}
        wire_by_index = {event["page_index"]: event for event in wire}
        rpc_by_index = {event["page_index"]: event for event in rpc}
        matched = [
            (
                client_page,
                page_by_index[client_page["page_index"]],
                wire_by_index[client_page["page_index"]],
            )
            for client_page in phase["pages"]
            if client_page["page_index"] in page_by_index
            and client_page["page_index"] in wire_by_index
        ]
        server_page_ns = sum(
            event["end_ns"] - event["start_ns"] for event in server_pages
        )
        server_wire_ns = sum(event["end_ns"] - event["start_ns"] for event in wire)
        decode_ns = phase["decode_ns"]
        item = {
            "start_ns": request["start_ns"],
            "end_ns": request["end_ns"],
            "snapshot_id": snapshot_id,
            "overlap": any(
                request["start_ns"] <= end and request["end_ns"] >= start
                for start, end in windows
            ),
            "page_calls": len(phase["pages"]),
            "server_page_calls": len(server_pages),
            "server_wire_calls": len(wire),
            "server_rpc_calls": len(rpc),
            "client_bytes": sum(event["bytes"] for event in phase["pages"]),
            "server_bytes": sum(event["bytes"] for event in wire),
            "total_ms": (request["end_ns"] - request["start_ns"]) / 1e6,
            "client_cpu_ms": request["worker"]["cpu_seconds"] * 1000,
            "client_transport_ms": phase["transport_ns"] / 1e6,
            "client_decode_ms": decode_ns / 1e6,
            "client_other_ms": phase["client_other_ns"] / 1e6,
            "server_page_ms": server_page_ns / 1e6,
            "server_page_decode_ms": sum(event["decode_ns"] for event in server_pages)
            / 1e6,
            "server_wire_encode_ms": server_wire_ns / 1e6,
            "request_to_server_ms": sum(
                server_page["start_ns"] - client_page["start_ns"]
                for client_page, server_page, _ in matched
            )
            / 1e6,
            "server_to_wire_ms": sum(
                wire_page["start_ns"] - server_page["end_ns"]
                for _, server_page, wire_page in matched
            )
            / 1e6,
            "wire_to_client_ms": sum(
                client_page["end_ns"] - wire_page["end_ns"] - client_page["decode_ns"]
                for client_page, _, wire_page in matched
            )
            / 1e6,
            "transport_residual_ms": (
                phase["transport_ns"] - server_page_ns - server_wire_ns - decode_ns
            )
            / 1e6,
            "error": request.get("error"),
        }
        if capture is not None:
            item.update(
                server_capture_ms=(capture["end_ns"] - capture["start_ns"]) / 1e6,
                server_capture_encode_ms=capture["encode_ns"] / 1e6,
                server_capture_pages=capture["pages"],
                server_capture_bytes=capture["encoded_bytes"],
            )
            if "thread_cpu_ns" in capture:
                item["server_capture_cpu_ms"] = capture["thread_cpu_ns"] / 1e6
        if rpc and all(
            client_page["page_index"] in rpc_by_index for client_page, _, _ in matched
        ):
            item.update(
                client_to_rpc_ms=sum(
                    rpc_by_index[client_page["page_index"]]["start_ns"]
                    - client_page["start_ns"]
                    for client_page, _, _ in matched
                )
                / 1e6,
                rpc_to_page_ms=sum(
                    server_page["start_ns"]
                    - rpc_by_index[client_page["page_index"]]["start_ns"]
                    for client_page, server_page, _ in matched
                )
                / 1e6,
                server_request_decode_ms=sum(
                    event["request_decode_ns"] for event in rpc
                )
                / 1e6,
            )
        joined.append(item)

    fields = (
        "total_ms",
        "client_cpu_ms",
        "client_transport_ms",
        "client_decode_ms",
        "client_other_ms",
        "server_capture_ms",
        "server_capture_cpu_ms",
        "server_capture_encode_ms",
        "server_page_ms",
        "server_page_decode_ms",
        "server_wire_encode_ms",
        "request_to_server_ms",
        "client_to_rpc_ms",
        "rpc_to_page_ms",
        "server_request_decode_ms",
        "server_to_wire_ms",
        "wire_to_client_ms",
        "transport_residual_ms",
        "page_calls",
        "client_bytes",
    )
    groups = {"all": joined, "overlap": [item for item in joined if item["overlap"]]}
    return {
        "size": raw["size"],
        "duration_s": raw["duration_s"],
        "sync_transport": raw["sync_transport"],
        "full_language": raw["full_language"],
        "full_transport": raw["full_transport"],
        "full_readers": raw["full_readers"],
        "components": raw["components"],
        "groups": {
            label: {
                "count": len(items),
                "missing_capture": sum(
                    "server_capture_ms" not in item for item in items
                ),
                "page_count_mismatch": sum(
                    item["page_calls"] != item["server_page_calls"]
                    or item["page_calls"] != item["server_wire_calls"]
                    or (
                        item["server_rpc_calls"] > 0
                        and item["page_calls"] != item["server_rpc_calls"]
                    )
                    for item in items
                ),
                "fields": {
                    field: distribution(
                        [item[field] for item in items if field in item]
                    )
                    for field in fields
                },
            }
            for label, items in groups.items()
        },
        "slowest": sorted(joined, key=lambda item: item["total_ms"])[-10:],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path)
    options = parser.parse_args()
    result = analyze(json.loads(options.source.read_text()))
    output = json.dumps(result, indent=2) + "\n"
    if options.output is None:
        print(output, end="")
    else:
        options.output.write_text(output)


if __name__ == "__main__":
    main()
