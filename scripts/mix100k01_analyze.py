"""Offline, reproducible MIX100K-01 summaries and predeclared repeat selection."""

import argparse
from collections import Counter, defaultdict
import math
from pathlib import Path

from mix100k01 import NS, read, rows, save, verify_summary_archives


def distribution(values):
    values = sorted(values)
    if not values:
        return dict(n=0, p50=None, p95=None, p99=None, max=None)
    return dict(
        n=len(values),
        **{
            name: values[
                min(len(values) - 1, max(0, math.ceil(len(values) * fraction) - 1))
            ]
            for name, fraction in [
                ("p50", 0.5),
                ("p95", 0.95),
                ("p99", 0.99),
                ("max", 1),
            ]
        },
    )


def summarize(directory):
    result = read(directory / "result.json")
    config = read(directory / "config.json")
    window = (
        read(directory / "window.json")
        if (directory / "window.json").exists()
        else None
    )
    counts = defaultdict(Counter)
    rpc_counts = Counter()
    rpc_first = {}
    child_sent = {}
    waits = defaultdict(lambda: defaultdict(list))
    overlapped = defaultdict(list)
    reasons = defaultdict(Counter)
    intervals = []
    checkpoint = dict(
        starts=0,
        success=0,
        published=0,
        failed=0,
        exit_saves=[],
        publish_times_ns=[],
        measurement_published=0,
    )
    checkpoint_total = []
    gc_starts, gc_duration = {}, []
    gc_states = set()
    lag = []
    events_file = directory / "events.jsonl"
    if events_file.exists():
        for row in rows(events_file):
            event = row["event"]
            kind, values = event["kind"], event["values"]
            if kind == "checkpoint_total":
                intervals.append((event["start_ns"], event["end_ns"]))
                checkpoint["starts"] += 1
                checkpoint_total.append((event["end_ns"] - event["start_ns"]) / 1e6)
            elif kind == "checkpoint_outcome":
                if values[3]:
                    continue
                checkpoint["success"] += int(bool(values[1]))
                checkpoint["published"] += int(bool(values[1] and values[2]))
                checkpoint["failed"] += int(not values[1])
                if values[1] and values[2]:
                    checkpoint["publish_times_ns"].append(event["at_ns"])
                    checkpoint["measurement_published"] += int(
                        bool(
                            window
                            and window["start_ns"] <= event["at_ns"] < window["end_ns"]
                        )
                    )
            elif kind == "checkpoint_exit_total":
                checkpoint["exit_saves"].append(
                    dict(
                        success=bool(values[0]),
                        milliseconds=(event["end_ns"] - event["start_ns"]) / 1e6,
                    )
                )
            elif kind == "loop_lag":
                lag.append(values[0] / 1e6)
            elif kind == "gc_start":
                gc_starts[values[0]] = event["at_ns"]
            elif kind == "gc_stop" and values[0] in gc_starts:
                gc_duration.append((event["at_ns"] - gc_starts.pop(values[0])) / 1e6)
            elif kind == "gc_state":
                gc_states.add(tuple(values[:4]))
    for name in ("writer", "query", "full1", "full2"):
        path = directory / f"{name}-requests.jsonl"
        if not path.exists():
            continue
        for row in rows(path):
            channel = row.get("channel", "child")
            kind = row["kind"]
            counts[channel][kind] += 1
            if kind == "not_sent":
                reasons[channel][row["reason"]] += 1
            if kind in {"rpc_sent", "sent"}:
                rpc_counts[channel] += 1
            rid = row.get("request_id", row.get("slot"))
            if kind == "rpc_sent":
                rpc_first.setdefault(rid, row["started_ns"])
            if kind == "sent":
                child_sent[rid] = row["sent_ns"]
            if kind == "returned":
                counts[channel][
                    "business_success" if row["flag"] == 0 else "business_failure"
                ] += 1
            if kind in {"completed", "returned"}:
                end = row.get("ended_ns", row.get("returned_ns"))
                sent = child_sent.get(rid) if channel == "child" else rpc_first.get(rid)
                if sent is not None:
                    metrics = dict(
                        schedule_ms=(sent - row["target_ns"]) / 1e6,
                        rpc_to_return_ms=(end - sent) / 1e6,
                        end_to_end_ms=(end - row["target_ns"]) / 1e6,
                    )
                    for metric, value in metrics.items():
                        waits[channel][metric].append(value)
                    if any(sent < b and end > a for a, b in intervals):
                        overlapped[channel].append(metrics["end_to_end_ms"])
            if kind == "completed":
                counts[channel]["wire_bytes"] += row.get("bytes", 0)
                counts[channel]["nodes"] += row.get("nodes", row.get("count", 0))
    processes = {}
    rss_sum_peak = 0
    sample_count = 0
    evidence_peak = 0
    resource_file = directory / "resources.jsonl"
    if resource_file.exists():
        for row in rows(resource_file):
            sample_count += 1
            rss_sum_peak = max(rss_sum_peak, row["rss_sum"])
            evidence_peak = max(evidence_peak, row["evidence_bytes"])
            for p in row["processes"]:
                if p.get("unavailable"):
                    continue
                entry = processes.setdefault(
                    str(p["pid"]),
                    dict(
                        role=p["role"],
                        first_ns=row["at_ns"],
                        last_ns=row["at_ns"],
                        first_rss=p["rss_bytes"],
                        last_rss=p["rss_bytes"],
                        peak_rss=0,
                        peak_pss=None,
                        cpu_first=p["cpu_seconds"],
                        cpu_last=p["cpu_seconds"],
                    ),
                )
                entry.update(
                    last_ns=row["at_ns"],
                    last_rss=p["rss_bytes"],
                    peak_rss=max(entry["peak_rss"], p["rss_bytes"]),
                    cpu_last=p["cpu_seconds"],
                )
                if "pss_bytes" in p:
                    entry["peak_pss"] = max(entry["peak_pss"] or 0, p["pss_bytes"])
    for entry in processes.values():
        entry["cpu_seconds_delta"] = entry["cpu_last"] - entry["cpu_first"]
        entry["rss_change_bytes"] = entry["last_rss"] - entry["first_rss"]
    points = checkpoint["publish_times_ns"]
    checkpoint["publish_interval_s"] = distribution(
        [(b - a) / NS for a, b in zip(points, points[1:])]
    )
    checkpoint["duration_ms"] = distribution(checkpoint_total)
    final = (
        read(directory / "server-final.json")
        if (directory / "server-final.json").exists()
        else None
    )
    try:
        summary_digests = verify_summary_archives(directory)
    except (ValueError, OSError, KeyError) as exc:
        summary_digests = dict(verified=False, error=f"{type(exc).__name__}: {exc}")
    summary = dict(
        condition=config["condition"],
        tasks=config["tasks"],
        result={
            k: result[k]
            for k in ["failure", "stop_batch", "lifecycle_seconds", "qualified_smoke"]
        },
        window=window,
        counts={k: dict(v) for k, v in counts.items()},
        actual_rpcs=dict(rpc_counts),
        not_sent_reasons={k: dict(v) for k, v in reasons.items()},
        waits_ms={
            k: {m: distribution(v) for m, v in metrics.items()}
            for k, metrics in waits.items()
        },
        checkpoint_overlap_end_to_end_ms={
            k: distribution(v) for k, v in overlapped.items()
        },
        checkpoint=checkpoint,
        resources=dict(
            samples=sample_count,
            rss_sum_peak=rss_sum_peak,
            evidence_peak=evidence_peak,
            processes=processes,
        ),
        gc=dict(
            states=[list(x) for x in gc_states],
            durations_ms=distribution(gc_duration),
            unfinished_generations=list(gc_starts),
        ),
        loop_lag_ms=distribution(lag),
        server_final=final,
        summary_digests=summary_digests,
    )
    child = counts["child"]
    all_queries = all(
        counts[c]["completed"] == config["duration"] // (10 if c == "summary" else 1)
        for c in ("summary", "since", "detail")
    )
    valid = (
        not result["failure"]
        and child["business_success"] == config["slots"]
        and child["sent"] == config["slots"]
        and not child["write_unknown"]
        and all_queries
        and summary_digests["verified"]
    )
    usable_d = (
        config["condition"] == "D"
        and summary_digests["verified"]
        and checkpoint["measurement_published"] >= 2
        and counts["summary"]["completed"] >= 2
        and all(counts[c]["completed"] >= 1 for c in ("full1", "full2"))
    )
    summary.update(
        valid_for_pair=valid,
        usable_D=usable_d,
        actual_write_rate=child["sent"] / config["duration"],
        effective_write_rate=child["business_success"] / config["duration"],
    )
    save(directory / "summary.json", summary)
    return summary


def select_repeats(batch):
    summaries = {}
    for condition, number in [("D", 2), ("B", 3), ("C", 4), ("A", 5)]:
        directory = batch / f"{number:02d}-{condition}"
        if not (directory / "result.json").exists():
            raise ValueError("four screening runs not present")
        summaries[condition] = summarize(directory)
    eligible = []
    for other in ("B", "C"):
        d, x = summaries["D"], summaries[other]
        if d["valid_for_pair"] and x["valid_for_pair"] and d["usable_D"]:
            delta = (
                d["waits_ms"]["child"]["end_to_end_ms"]["p95"]
                - x["waits_ms"]["child"]["end_to_end_ms"]["p95"]
            )
            if delta > 0:
                eligible.append((delta, other))
    # B precedes C for equal positive differences. This file is written once.
    eligible.sort(key=lambda pair: (-pair[0], pair[1]))
    selection = dict(
        eligible=eligible,
        rule="complete write target and ordinary query slots; positive child end-to-end p95 difference; ties D-B; reverse screening order",
        order=[eligible[0][1], "D"] if eligible else [],
        pair=["D", eligible[0][1]] if eligible else None,
        summaries={
            k: dict(valid_for_pair=v["valid_for_pair"], usable_D=v["usable_D"])
            for k, v in summaries.items()
        },
    )
    target = batch / "repeat-selection.json"
    if target.exists():
        raise ValueError("repeat selection already frozen")
    save(target, selection)
    return selection


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["summarize", "select-repeats"])
    parser.add_argument("path", type=Path)
    options = parser.parse_args()
    value = (
        summarize(options.path)
        if options.action == "summarize"
        else select_repeats(options.path)
    )
    print(__import__("json").dumps(value, indent=2))


if __name__ == "__main__":
    main()
