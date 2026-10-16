"""Recompute PERF-03 evidence and O/L/D qualification from raw run files."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import statistics

from r3_14_observation import CHANNELS


def distribution(values):
    if not values:
        return dict(count=0, p50=None, p95=None, p99=None, max=None)
    values = sorted(values)
    return dict(
        count=len(values),
        **{f"p{q}": values[math.ceil(len(values) * q / 100) - 1] for q in [50, 95, 99]},
        max=values[-1],
    )


def lines(path):
    if path.exists():
        with path.open() as source:
            for line in source:
                yield json.loads(line)


def analyze(run_dir):
    run_dir = Path(run_dir)
    manifest = json.loads((run_dir / "manifest.json").read_text())
    config = json.loads((run_dir / "config.json").read_text())
    start = manifest.get("measurement_start_ns", 0)
    end = manifest.get("measurement_end_ns", 0)
    event_counts = Counter()
    by_pid = Counter()
    sequences = defaultdict(set)
    requests = defaultdict(list)
    requests_overlap = defaultdict(list)
    sent_wait = defaultdict(list)
    sending_wait = defaultdict(list)
    first_batch = []
    rpc_counts = Counter()
    rpc_failures = Counter()
    rpc_by_request = Counter()
    legacy_rpc_calls = Counter()
    cancel_counts = Counter()
    failures = defaultdict(list)
    counters = defaultdict(Counter)
    windows = []
    lags = []
    lag_overlap = []
    missed = 0
    gc_starts = {}
    gc_counts = Counter()
    gc_stop_records = Counter()
    gc_pause = defaultdict(list)
    gc_states = set()
    semantic = []
    # Checkpoint windows first, then classify intervals. Both passes stream disk.
    for item in lines(run_dir / "events.jsonl"):
        if (
            item["kind"] == "checkpoint_total"
            and item["start_ns"] < end
            and item["end_ns"] > start
        ):
            windows.append((item["start_ns"], item["end_ns"], bool(item["values"][0])))

    def overlap(a, b):
        return any(a < last and b > first for first, last, _ in windows)

    for item in lines(run_dir / "events.jsonl"):
        kind, values = item["kind"], item["values"]
        event_counts[kind] += 1
        by_pid[item["pid"]] += 1
        sequences[item["pid"]].add(item["sequence"])
        if kind == "request":
            _, channel, outcome, sent, nodes, byte_count, first, late = values
            name = CHANNELS[channel]
            counters[name]["offered"] += 1
            counters[name]["late"] += late
            if outcome == 2:
                counters[name]["not_sent"] += 1
            else:
                counters[name]["sent"] += 1
                if outcome == 0:
                    counters[name]["completed"] += 1
                    duration = (item["end_ns"] - item["start_ns"]) / 1e6
                    requests[name].append(duration)
                    sent_wait[name].append((item["end_ns"] - sent) / 1e6)
                    sending_wait[name].append((sent - item["start_ns"]) / 1e6)
                    if channel == 1 and first:
                        first_batch.append((first - item["start_ns"]) / 1e6)
                    if overlap(item["start_ns"], item["end_ns"]):
                        requests_overlap[name].append(duration)
                else:
                    counters[name]["failed"] += 1
                    failures[name].append(
                        dict(
                            identity=item["identity"],
                            waiting_ms=(item["end_ns"] - item["start_ns"]) / 1e6,
                        )
                    )
                    if item["identity"] in {
                        "IntegrityError",
                        "ServerResponseError",
                        "permission_denied",
                        "invalid_request",
                    }:
                        semantic.append(
                            dict(
                                channel=name,
                                identity=item["identity"],
                                at_ns=item["at_ns"],
                            )
                        )
        elif kind == "rpc_attempt":
            if item["identity"].endswith("_request"):
                legacy_rpc_calls[item["identity"]] += 1
                continue
            rid, channel, attempt, outcome = values[:4]
            rpc_counts[CHANNELS[channel]] += 1
            rpc_failures[CHANNELS[channel]] += outcome
            rpc_by_request[(item["pid"], rid)] += 1
        elif kind == "cancel" and item["schema_version"] == "r3-perf-events-v2.1":
            cancel_counts["attempts"] += 1
            cancel_counts[
                "confirmed" if values[2] == 0 else "failed_or_unconfirmed"
            ] += 1
        elif kind == "watchdog":
            semantic.append(
                dict(channel="watchdog", identity=item["identity"], at_ns=item["at_ns"])
            )
        elif kind == "loop_lag" and item["start_ns"] < end and item["end_ns"] > start:
            lags.append(values[0] / 1e6)
            missed += values[1]
            if overlap(item["start_ns"], item["end_ns"]):
                lag_overlap.append(values[0] / 1e6)
        elif kind == "gc_start":
            gc_starts[(item["tid"], values[0])] = item["at_ns"]
        elif kind == "gc_stop":
            if start <= item["at_ns"] <= end:
                gc_stop_records[values[0]] += 1
            key = item["tid"], values[0]
            previous = gc_starts.pop(key, None)
            if previous is not None and previous < end and item["at_ns"] > start:
                gc_counts[values[0]] += 1
                gc_pause[values[0]].append((item["at_ns"] - previous) / 1e6)
        elif kind == "gc_state":
            gc_states.add(tuple(values[:4]))
    producers = {}
    evidence_problems = []
    for role, data in manifest["processes"].items():
        if role not in {"server", "F", "S", "Q"}:
            continue
        path = run_dir / f"{role}-final.json"
        if not path.exists():
            producers[role] = dict(tail="unknown", received=by_pid[data["pid"]])
            evidence_problems.append(f"{role} final counters absent; tail unknown")
            continue
        final = json.loads(path.read_text())
        accounting = final.get("observation")
        if accounting is None:
            producers[role] = dict(observation="off")
            continue
        received = by_pid[data["pid"]]
        producers[role] = dict(
            **accounting,
            received=received,
            tail="known",
            collector_loss=accounting["emitted"] - received,
        )
        if accounting["dropped"] or received != accounting["emitted"]:
            evidence_problems.append(f"{role} observation loss")
        seq = sequences[data["pid"]]
        producers[role]["sequence_missing"] = accounting["produced"] - len(seq)
        if not accounting["sender_stopped"]:
            evidence_problems.append(f"{role} sender still running")
    resources = defaultdict(list)
    for sample in lines(run_dir / "resources.jsonl"):
        for process in sample["processes"]:
            if not process.get("unavailable"):
                resources[process["role"]].append((sample["at_ns"], process))
    resource_summary = {}
    for role, samples in resources.items():
        measured = [(at, p) for at, p in samples if start <= at <= end]
        # Worker roles may contain multiple PIDs. CPU is computed per PID.
        cpu_by_pid = defaultdict(list)
        for at, process in measured:
            cpu_by_pid[process["pid"]].append(process["cpu_seconds"])
        resource_summary[role] = dict(
            rss_peak_bytes=max(p["rss_bytes"] for _, p in samples),
            measurement_cpu_seconds=sum(
                v[-1] - v[0] for v in cpu_by_pid.values() if len(v) >= 2
            ),
            sample_count=len(samples),
            pss_peak_bytes=max((p.get("pss_bytes", 0) for _, p in samples), default=0),
        )
    if manifest["exit_reason"] != "completed":
        evidence_problems.append(manifest["exit_reason"])
    if semantic:
        evidence_problems.append("semantic or watchdog failures")
    if any(c.get("failed", 0) for c in counters.values()):
        evidence_problems.append("logical request failures")
    if cancel_counts.get("failed_or_unconfirmed", 0):
        evidence_problems.append("full snapshot cancellation unconfirmed")
    if any(s["remaining_owned_descendants"] for s in manifest["cleanup"].values()):
        evidence_problems.append("owned workers still running after cleanup")
    ready = manifest["processes"].get("server", {}).get("ready", {})
    fresh = ready.get("fresh_gc", {})
    ready_gc = ready.get("ready_gc", {})
    final_gc = (
        json.loads((run_dir / "server-final.json").read_text()).get("gc")
        if (run_dir / "server-final.json").exists()
        else None
    )
    gc_same = bool(
        fresh
        and fresh == ready_gc
        and fresh == final_gc
        and (
            manifest.get("measurement_gc_handshake", {}).get("gc") == fresh
            or bool(gc_states)
        )
        and all(
            state[0] == 1 and list(state[1:]) == fresh["threshold"]
            for state in gc_states
        )
    )
    if not gc_same:
        evidence_problems.append("default GC continuity unproven")
    return dict(
        schema_version="r3-perf-summary-v2",
        quantiles="nearest_rank",
        run_id=manifest["run_id"],
        mode=config["runtime"]["mode"],
        workload=config["runtime"]["workload"],
        smoke=config["runtime"]["smoke"],
        lag_ablation=config["runtime"].get("lag_ablation"),
        fault=config["runtime"]["fault"],
        exit_reason=manifest["exit_reason"],
        evidence_status="usable" if not evidence_problems else "incomplete_or_failed",
        evidence_problems=evidence_problems,
        semantic_errors=semantic,
        events=dict(event_counts),
        producers=producers,
        default_gc_continuity=gc_same,
        gc_observed_states=sorted(gc_states),
        requests={
            name: dict(
                counters=dict(counters[name]),
                total_wait_ms=distribution(requests[name]),
                sent_to_completed_ms=distribution(sent_wait[name]),
                scheduled_to_sent_ms=distribution(sending_wait[name]),
                checkpoint_overlap_wait_ms=distribution(requests_overlap[name]),
                failures=failures[name],
            )
            for name in counters
        },
        checkpoint=dict(
            count=len(windows),
            successes=sum(w[2] for w in windows),
            total_ms=distribution([(b - a) / 1e6 for a, b, _ in windows]),
        ),
        first_full_batch_ms=distribution(first_batch),
        rpc_attempts=dict(rpc_counts),
        rpc_attempt_failures=dict(rpc_failures),
        legacy_logical_rpc_calls=dict(legacy_rpc_calls),
        cancellations=dict(cancel_counts),
        rpc_per_logical_request=distribution(list(rpc_by_request.values())),
        loop_lag_ms=distribution(lags),
        loop_lag_overlap_ms=distribution(lag_overlap),
        missed_ticks=missed,
        gc={
            str(gen): dict(
                count=gc_counts[gen],
                pause_ms=distribution(gc_pause[gen]),
                total_pause_ms=sum(gc_pause[gen]),
            )
            for gen in gc_counts
        },
        gc_stop_records=dict(gc_stop_records),
        gc_counts_complete=not any(
            "observation loss" in p or "tail unknown" in p for p in evidence_problems
        ),
        unmatched_gc_starts_at_exit=len(gc_starts),
        resources=resource_summary,
        cleanup=manifest["cleanup"],
    )


def compare(paths):
    summaries = [analyze(path) for path in paths]
    manifests = {
        json.loads((Path(path) / "manifest.json").read_text())["run_id"]: json.loads(
            (Path(path) / "manifest.json").read_text()
        )
        for path in paths
    }
    result = {}
    for workload in ["idle_no_clients", "T3"]:
        groups = {
            mode: [
                s
                for s in summaries
                if s["workload"] == workload
                and s["mode"] == mode
                and not s["smoke"]
                and not s["fault"]
                and s.get("lag_ablation") is None
            ]
            for mode in ["O", "L", "D"]
        }
        output = {}
        for mode in ["L", "D"]:
            if any(len(groups[m]) != 3 for m in ["O", mode]):
                output[mode] = dict(
                    qualification="not_proven",
                    reason="Need exactly three matched O and variant runs",
                )
                continue
            pairs = list(zip(groups["O"], groups[mode]))
            cpu_differences = [
                v["resources"]["server"]["measurement_cpu_seconds"]
                - o["resources"]["server"]["measurement_cpu_seconds"]
                for o, v in pairs
            ]
            base_cpu = statistics.median(
                s["resources"]["server"]["measurement_cpu_seconds"] for s in groups["O"]
            )
            cpu_limit = max(0.05 * base_cpu, 0.5)
            cpu_increment = statistics.median(cpu_differences)
            cpu_variation = max(cpu_differences) - min(cpu_differences)
            latency = {}
            comparable = True
            for channel in groups["O"][0]["requests"]:
                deltas = []
                limits = []
                for o, v in pairs:
                    a, b = o["requests"][channel], v["requests"].get(channel)
                    if (
                        b is None
                        or a["total_wait_ms"]["p95"] is None
                        or b["total_wait_ms"]["p95"] is None
                    ):
                        comparable = False
                        continue
                    for metric in ["offered", "sent", "completed"]:
                        count = a["counters"].get(metric, 0)
                        if abs(b["counters"].get(metric, 0) - count) > max(
                            1, count * 0.05
                        ):
                            comparable = False
                    deltas.append(b["total_wait_ms"]["p95"] - a["total_wait_ms"]["p95"])
                    limits.append(max(a["total_wait_ms"]["p95"] * 0.05, 1))
                latency[channel] = dict(
                    increment_ms=statistics.median(deltas) if deltas else None,
                    budget_ms=statistics.median(limits) if limits else None,
                    per_round_increment_ms=deltas,
                    pair_variation_ms=max(deltas) - min(deltas) if deltas else None,
                )
            loss_free = all(
                s["evidence_status"] == "usable" for s in groups["O"] + groups[mode]
            )
            memory_bounded = all(
                s["producers"]["server"].get("observation_storage_bound_bytes", 2**63)
                <= 8 * 1024 * 1024
                for s in groups[mode]
            )
            source_invariant = (
                len(
                    {
                        (
                            manifests[s["run_id"]]["source_index_sha256"],
                            manifests[s["run_id"]]["gc_patch_sha256"],
                        )
                        for s in groups["O"] + groups[mode]
                    }
                )
                == 1
            )
            passed = (
                loss_free
                and comparable
                and memory_bounded
                and source_invariant
                and cpu_variation <= cpu_limit
                and cpu_increment <= cpu_limit
                and all(
                    v["increment_ms"] is not None
                    and v["increment_ms"] <= v["budget_ms"]
                    and v["pair_variation_ms"] <= v["budget_ms"]
                    for v in latency.values()
                )
            )
            output[mode] = dict(
                qualification="qualified_at_1k_only" if passed else "not_proven",
                loss_free=loss_free,
                observation_storage_within_budget=memory_bounded,
                identical_diagnostic_source=source_invariant,
                comparable=comparable,
                cpu_pair_variation_seconds=cpu_variation,
                cpu_increment_seconds=cpu_increment,
                cpu_budget_seconds=cpu_limit,
                per_round_cpu_increment_seconds=cpu_differences,
                latency=latency,
                run_ids=[s["run_id"] for s in groups[mode]],
            )
        result[workload] = output
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dirs", nargs="+", type=Path)
    parser.add_argument("--compare", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            compare(args.run_dirs)
            if args.compare
            else [analyze(p) for p in args.run_dirs],
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
