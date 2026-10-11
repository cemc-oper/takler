"""Summarize raw R3-14 periodic probe events without dropping failed calls."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
import json
from pathlib import Path


def quantile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def distribution(values: list[float]) -> dict:
    return {
        "count": len(values),
        "p50": quantile(values, 0.5),
        "p95": quantile(values, 0.95),
        "p99": quantile(values, 0.99),
        "max": max(values) if values else None,
    }


def summarize(raw: dict) -> dict:
    checkpoints = [
        event for event in raw["events"] if event["phase"] == "periodic_total"
    ]
    captures = [event for event in raw["events"] if event["phase"] == "capture"]
    publications = [
        event
        for event in raw["events"]
        if event["phase"] == "publish" and event["success"]
    ]
    publication_times = sorted(event["end_ns"] for event in publications)
    windows = [(event["start_ns"], event["end_ns"]) for event in checkpoints]
    by_kind: dict[str, dict[str, list]] = defaultdict(
        lambda: {"all": [], "overlap": []}
    )
    for item in raw["requests"]:
        kind = item["kind"]
        by_kind[kind]["all"].append(item)
        if any(
            item["start_ns"] <= end and item["end_ns"] >= start
            for start, end in windows
        ):
            by_kind[kind]["overlap"].append(item)

    requests = {}
    for kind, groups in sorted(by_kind.items()):
        requests[kind] = {}
        for label, items in groups.items():
            durations = [(item["end_ns"] - item["start_ns"]) / 1e6 for item in items]
            requests[kind][label] = {
                "latency_ms": distribution(durations),
                "failures": [
                    {"start_ns": item["start_ns"], "error": item["error"]}
                    for item in items
                    if item.get("error")
                ],
                "retry_attempts": sum(
                    len(item.get("worker", {}).get("failures", [])) for item in items
                ),
            }
            if kind.startswith("full_stream"):
                requests[kind][label].update(
                    first_batch_ms=distribution(
                        [
                            (item["first_batch_ns"] - item["start_ns"]) / 1e6
                            for item in items
                            if item.get("first_batch_ns") is not None
                        ]
                    ),
                    pages=distribution(
                        [item["page_calls"] for item in items if "page_calls" in item]
                    ),
                    chunks=distribution(
                        [item["chunk_calls"] for item in items if "chunk_calls" in item]
                    ),
                    encoded_bytes=distribution(
                        [item["bytes"] for item in items if "bytes" in item]
                    ),
                    cli_stdout_bytes=distribution(
                        [
                            item["stdout_bytes"]
                            for item in items
                            if "stdout_bytes" in item
                        ]
                    ),
                    client_rss_delta_bytes=distribution(
                        [
                            item["rss_peak_bytes"] - item["rss_before_bytes"]
                            for item in items
                            if "rss_peak_bytes" in item
                        ]
                    ),
                )

    lag = raw["lag"]
    lag_overlapping = [
        item
        for item in lag
        if any(start <= item["at_ns"] <= end for start, end in windows)
    ]
    resources = raw["resources"]
    checkpoint_age = []
    for item in resources:
        saved = item.get("last_checkpoint_at")
        if saved and "wall_ns" in item:
            saved_ns = int(datetime.fromisoformat(saved).timestamp() * 1e9)
            checkpoint_age.append(max(0, (item["wall_ns"] - saved_ns) / 1e9))
    return {
        "fixture_started_utc": raw.get("fixture_started_utc"),
        "size": raw["size"],
        "wide": raw["wide"],
        "interval_s": raw["interval_s"],
        "duration_s": raw["duration_s"],
        "checkpoint": {
            "count": len(checkpoints),
            "successes": sum(item["success"] for item in checkpoints),
            "duration_ms": distribution(
                [(item["end_ns"] - item["start_ns"]) / 1e6 for item in checkpoints]
            ),
            "capture_ms": distribution(
                [(item["end_ns"] - item["start_ns"]) / 1e6 for item in captures]
            ),
            "publish_ms": distribution(
                [(item["end_ns"] - item["start_ns"]) / 1e6 for item in publications]
            ),
            "publication_interval_s": distribution(
                [
                    (later - earlier) / 1e9
                    for earlier, later in zip(publication_times, publication_times[1:])
                ]
            ),
        },
        "loop_lag": {
            label: {
                "latency_ms": distribution([item["lag_ms"] for item in items]),
                "missed_intervals": sum(
                    item.get("missed_intervals", 0) for item in items
                ),
            }
            for label, items in (("all", lag), ("overlap", lag_overlapping))
        },
        "gc_totals": raw.get("gc_totals", {}),
        "rss_peak_bytes": {
            "server_and_probe": max(
                (item["server_rss_bytes"] for item in resources), default=0
            ),
            "checkpoint_worker": max(
                (item["worker_rss_bytes"] for item in resources), default=0
            ),
        },
        "checkpoint_age_s": distribution(checkpoint_age),
        "queue_peaks": {
            "background_writes": max(
                (item["background_writes"] for item in resources), default=0
            ),
            "reserved_bytes": max(
                (item["reserved_bytes"] for item in resources), default=0
            ),
            "full_snapshot_leases": max(
                (item.get("full_snapshot_leases", 0) for item in resources), default=0
            ),
            "summary_snapshot_leases": max(
                (item.get("summary_snapshot_leases", 0) for item in resources),
                default=0,
            ),
        },
        "requests": requests,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("raw", type=Path)
    parser.add_argument("--output", type=Path)
    options = parser.parse_args()
    result = summarize(json.loads(options.raw.read_text()))
    encoded = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if options.output:
        options.output.write_text(encoded)
    print(encoded)


if __name__ == "__main__":
    main()
