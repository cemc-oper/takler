"""Recompute PERF-08A logical/capture links; secondary ambiguity stays unknown."""

from collections import defaultdict
import json
from pathlib import Path
import statistics
import sys

from r3_14_isolated_analyze import analyze as base_analyze, distribution, lines


def matched_summaries(events, manifest, capacity=32768):
    """Identity joins only: (client PID, logical ID, attempt ID), then snapshot."""
    if len(events) > capacity:
        raise ValueError("association record capacity exceeded")
    captures, capture_links = defaultdict(list), defaultdict(list)
    attempts, boundaries = defaultdict(list), defaultdict(list)
    identities, replies = defaultdict(list), defaultdict(list)
    requests, sent = {}, {}
    problems = []
    for e in events:
        v, k = e["values"], e["kind"]
        if k == "summary_capture":
            captures[e["identity"]].append(e)
        elif k == "summary_capture_link":
            capture_links[e["identity"]].append(e)
        elif k == "rpc_attempt" and v[1] == 2:
            attempts[(e["pid"], v[0], v[2])].append(e)
        elif k == "summary_rpc_boundary":
            boundaries[tuple(v[:3])].append(e)
        elif k == "summary_attempt_reply":
            replies[(e["pid"], v[0], v[2])].append(e)
        elif k == "summary_identity":
            identities[(e["pid"], v[0])].append(e)
        elif k == "request" and v[1] == 2 and v[2] != 2:
            key = (e["pid"], v[0])
            if key in requests:
                problems.append(f"duplicate logical request {key}")
            requests[key] = e
        elif k == "request_sent" and v[1] == 2:
            sent[(e["pid"], v[0])] = e
    sid_to_logical = {}
    for sid, cs in captures.items():
        links = capture_links[sid]
        if len(cs) != 1 or len(links) != 1:
            problems.append(f"capture/link non-unique {sid}")
            continue
        link = links[0]["values"]
        key = tuple(link[:2])
        if key not in sent or len(attempts[tuple(link[:3])]) != 1:
            problems.append(f"capture has no unique logical/attempt {sid}")
        sid_to_logical[sid] = key
    for sid in capture_links:
        if sid not in captures:
            problems.append(f"capture link without completed capture {sid}")
    alignment = [
        c["uncertainty_ns"]
        for section in ("clock_start", "clock_exit")
        for role, c in manifest.get(section, {}).items()
        if role in ("server", "S")
    ]
    uncertainty = (
        sum(sorted(alignment, reverse=True)[:2]) if len(alignment) >= 4 else None
    )
    secondary_usable = uncertainty is not None and uncertainty <= 1_000_000
    result = []
    for key, req in sorted(requests.items(), key=lambda kv: kv[1]["start_ns"]):
        v = req["values"]
        row = dict(
            pid=key[0],
            logical_id=key[1],
            scheduled_ns=req["start_ns"],
            sent_ns=v[3],
            ended_ns=req["end_ns"],
            outcome=v[2],
            nodes=v[4],
            E_ns=req["end_ns"] - v[3],
            scheduled_to_complete_ns=req["end_ns"] - req["start_ns"],
            scheduled_to_sent_ns=v[3] - req["start_ns"],
            crosses_measurement_end=req["end_ns"]
            > manifest.get("measurement_end_ns", 0),
            snapshots=[],
            attempt_details=[],
            secondary_alignment_uncertainty_ns=uncertainty,
        )
        ids = identities[key]
        final_sid = json.loads(ids[0]["identity"])[1] if len(ids) == 1 else None
        if v[2] == 0 and (final_sid is None or sid_to_logical.get(final_sid) != key):
            problems.append(f"completed logical lacks final capture {key}")
        owned = [sid for sid, owner in sid_to_logical.items() if owner == key]
        cs = [captures[sid][0] for sid in owned]
        for sid in owned:
            c = captures[sid][0]
            if (
                c["end_ns"] < c["start_ns"]
                or c["values"][0] > c["end_ns"] - c["start_ns"]
            ):
                problems.append(f"invalid capture wall/CPU {sid}")
            row["snapshots"].append(
                dict(
                    snapshot_id=sid,
                    final=sid == final_sid,
                    start_ns=c["start_ns"],
                    end_ns=c["end_ns"],
                    W_ns=c["end_ns"] - c["start_ns"],
                    C_ns=c["values"][0],
                    encode_ns=c["values"][1],
                    bytes=c["values"][4],
                    pages=c["values"][5],
                )
            )
        final_capture = captures.get(final_sid, [])
        row["capture_work_W_ns"] = sum(c["end_ns"] - c["start_ns"] for c in cs)
        row["capture_work_C_ns"] = sum(c["values"][0] for c in cs)
        row["W_ns"] = (
            final_capture[0]["end_ns"] - final_capture[0]["start_ns"]
            if len(final_capture) == 1
            else 0
        )
        row["C_ns"] = final_capture[0]["values"][0] if len(final_capture) == 1 else 0
        row["W_minus_C_ns"] = row["W_ns"] - row["C_ns"]
        row["capture_work_overlap"] = any(
            a["start_ns"] < b["end_ns"] and b["start_ns"] < a["end_ns"]
            for i, a in enumerate(cs)
            for b in cs[i + 1 :]
        )
        gaps = []
        for attempt_key, records in attempts.items():
            if attempt_key[:2] != key:
                continue
            b = boundaries[attempt_key]
            reply = replies[attempt_key]
            item = dict(
                attempt_id=attempt_key[2],
                client_records=records,
                server_records=b,
                replies=reply,
                entrance_before_ns=None,
                secondary="unknown",
            )
            if len(records) == len(b) == 1 and secondary_usable:
                gap = b[0]["start_ns"] - records[0]["start_ns"]
                # Report measured interval, never correct CPU or invent an exact transport stage.
                if gap >= -uncertainty:
                    item.update(entrance_before_ns=gap, secondary="matched")
                    gaps.append(gap)
            row["attempt_details"].append(item)
        row["entrance_before_sum_ns"] = (
            sum(gaps) if len(gaps) == len(row["attempt_details"]) and gaps else None
        )
        row["associated"] = bool(
            v[2] == 0 and final_sid and sid_to_logical.get(final_sid) == key
        )
        result.append(row)
    # A process may die with a sent request but without its terminal record.
    incomplete = [
        dict(pid=k[0], logical_id=k[1], sent=e, outcome="unfinished")
        for k, e in sent.items()
        if k not in requests
    ]
    if incomplete:
        problems.append("sent logical requests lack terminal records")
    return dict(
        requests=result,
        unfinished=incomplete,
        problems=problems,
        secondary_alignment_uncertainty_ns=uncertainty,
        association_records=len(events),
    )


def intersect_duration(intervals_a, intervals_b, start, end):
    return sum(
        max(0, min(a1, b1, end) - max(a0, b0, start))
        for a0, a1 in intervals_a
        for b0, b1 in intervals_b
    )


def analyze(run_dir):
    path = Path(run_dir)
    result = base_analyze(path)
    manifest = json.loads((path / "manifest.json").read_text())
    config = json.loads((path / "config.json").read_text())
    start, end = (
        manifest.get("measurement_start_ns", 0),
        manifest.get("measurement_end_ns", 0),
    )
    selected, full, publishes, totals, outcomes, final_saves, states = (
        [],
        defaultdict(list),
        [],
        [],
        [],
        [],
        [],
    )
    for e in lines(path / "events.jsonl"):
        k, v = e["kind"], e["values"]
        if k in {
            "summary_capture",
            "summary_capture_link",
            "summary_rpc_boundary",
            "summary_attempt_reply",
            "summary_identity",
        } or (k in {"request", "request_sent", "rpc_attempt"} and v[1] == 2):
            selected.append(e)
            if len(selected) > 32768:
                raise ValueError("association record capacity exceeded")
        if k == "request" and v[1] == 1:
            full[e["role"]].append(e)
        elif k == "checkpoint_publish":
            publishes.append(e)
        elif k == "checkpoint_total":
            totals.append(e)
        elif k == "checkpoint_outcome":
            outcomes.append(e)
        elif k == "checkpoint_exit_total":
            final_saves.append(e)
        elif k == "service_state":
            states.append(e)
    linked = matched_summaries(selected, manifest)
    result.update(correlation=linked, event_schema_extension="perf08a-v1")
    result["evidence_problems"].extend(linked["problems"])
    completed = [
        r for r in linked["requests"] if r["associated"] and start <= r["sent_ns"] < end
    ]
    for r in linked["requests"]:
        r["checkpoint_overlap"] = any(
            r["sent_ns"] < e["end_ns"] and r["ended_ns"] > e["start_ns"] for e in totals
        )
    result["summary_stages_ns"] = {
        key: distribution([r[key] for r in completed])
        for key in ("E_ns", "W_ns", "C_ns", "W_minus_C_ns")
    }
    entrance = [r["entrance_before_sum_ns"] for r in completed]
    result["summary_stages_ns"]["entrance_before_sum_ns"] = distribution(
        [v for v in entrance if v is not None]
    )
    result["summary_stages_ns"]["entrance_all_known"] = bool(entrance) and all(
        v is not None for v in entrance
    )
    overlapping = [r for r in completed if r["checkpoint_overlap"]]
    result["checkpoint_overlap_summary"] = dict(
        count=len(overlapping),
        distribution_allowed=len(overlapping) >= 5,
        logical_ids=[r["logical_id"] for r in overlapping],
        E_ns=distribution([r["E_ns"] for r in overlapping])
        if len(overlapping) >= 5
        else None,
    )
    periodic = [
        e
        for e in publishes
        if e["values"][1:4] == [1, 1, 0] and start <= e["end_ns"] <= end
    ]
    result["publication"] = dict(
        periodic_measured=len(periodic),
        all_publish_events=publishes,
        outcomes=outcomes,
        exit_save_events=final_saves,
        periodic_total_events=totals,
        published_false=sum(not e["values"][2] for e in publishes),
        actual_periodic_intervals_ns=[
            b["end_ns"] - a["end_ns"] for a, b in zip(periodic, periodic[1:])
        ],
    )
    if any(not e["values"][1] for e in outcomes) or any(
        not e["values"][0] for e in totals + final_saves
    ):
        result["evidence_problems"].append("checkpoint failure")
    intervals = {
        role: [(e["values"][3], e["end_ns"]) for e in events if e["values"][2] != 2]
        for role, events in full.items()
    }
    fraction = (
        intersect_duration(intervals.get("F1", []), intervals.get("F2", []), start, end)
        / (end - start)
        if end > start
        else 0
    )
    full_counts = {
        role: sum(
            e["values"][2] == 0 and start <= e["values"][3] < end
            for e in full.get(role, [])
        )
        for role in ("F1", "F2")
    }
    result["full_readers"] = dict(
        completed=full_counts,
        both_inflight_fraction=fraction,
        intervals=intervals,
        concurrency_meaning="requests simultaneously in flight; not simultaneous renderer execution",
    )
    if any(
        e["values"][0] > (0 if config["runtime"]["workload"] == "A" else 2)
        for e in states
    ):
        result["evidence_problems"].append(
            "full lease concurrency exceeds configured bound"
        )
    for role, ivs in intervals.items():
        if any(b[0] < a[1] for a, b in zip(ivs, ivs[1:])):
            result["evidence_problems"].append(f"{role} has overlapping logical reads")
    server_final = path / "server-final.json"
    final = json.loads(server_final.read_text()) if server_final.exists() else {}
    result["release"] = dict(
        full_leases=final.get("full_leases"),
        background_writes=final.get("background_writes"),
        background_last_reference=final.get("background_last_reference", "unknown"),
        last_withdrawal_state=states[-1] if states else None,
    )
    if final.get("full_leases") != 0 or final.get("background_writes") != 0:
        result["evidence_problems"].append("lease/background cleanup unconfirmed")
    for role in ("F1", "F2", "S"):
        f = path / f"{role}-final.json"
        if not f.exists() or not json.loads(f.read_text()).get("threads_released"):
            result["evidence_problems"].append(f"{role} tail/cleanup unknown")
    result["short_gate"] = dict(
        summary=len(completed) >= 2,
        periodic=len(periodic) >= 1,
        full=config["runtime"]["workload"] == "A"
        or all(v >= 1 for v in full_counts.values()),
    )
    result["analysis_sufficiency"] = dict(
        summary=len(completed) >= 10,
        periodic=len(periodic) >= 3,
        full=config["runtime"]["workload"] == "A"
        or all(v >= 3 for v in full_counts.values()),
        concurrent=config["runtime"]["workload"] == "A" or fraction >= 0.5,
    )
    result["segments_30s"] = []
    for a in range(0, int((end - start) / 1e9), 30):
        lo, hi = start + a * 10**9, min(end, start + (a + 30) * 10**9)
        rs = [r for r in completed if lo <= r["sent_ns"] < hi]
        resource = defaultdict(list)
        for sample in lines(path / "resources.jsonl"):
            if lo <= sample["at_ns"] < hi:
                for p in sample["processes"]:
                    if not p.get("unavailable"):
                        resource[(p["role"], p["pid"])].append(p)
        result["segments_30s"].append(
            dict(
                start_seconds=a,
                summary_count=len(rs),
                stages_ns={
                    k: distribution([r[k] for r in rs])
                    for k in ("E_ns", "W_ns", "C_ns", "W_minus_C_ns")
                },
                resources=[
                    dict(
                        role=role,
                        pid=pid,
                        cpu_seconds=ps[-1]["cpu_seconds"] - ps[0]["cpu_seconds"],
                        rss_peak_bytes=max(p["rss_bytes"] for p in ps),
                    )
                    for (role, pid), ps in resource.items()
                ],
            )
        )
    if any(
        p.get("exit_code") != 0 or p.get("forced") for p in result["cleanup"].values()
    ):
        result["evidence_problems"].append("abnormal/forced cleanup")
    result["evidence_status"] = (
        "usable" if not result["evidence_problems"] else "incomplete_or_failed"
    )
    return result


def paired_results(rows):
    pairs = []
    for n in (1, 2, 3):
        rs = [r for r in rows if r["stage"] == "full" and r["pair"] == n]
        if len(rs) != 2:
            continue
        variants = {
            r["variant"]: json.loads((Path(r["path"]) / "summary.json").read_text())
            for r in rs
        }
        a, b = variants["A"], variants["B"]
        deltas = {}
        for k in ("E_ns", "W_ns", "C_ns", "W_minus_C_ns", "entrance_before_sum_ns"):
            av, bv = a["summary_stages_ns"][k]["p50"], b["summary_stages_ns"][k]["p50"]
            deltas[k] = None if av is None or bv is None else bv - av
        pairs.append(
            dict(
                pair=n,
                order=[r["variant"] for r in rs],
                delta_median_ns=deltas,
                A=a["summary_stages_ns"],
                B=b["summary_stages_ns"],
                sufficient=all(a["analysis_sufficiency"].values())
                and all(b["analysis_sufficiency"].values()),
            )
        )
    verdict = "insufficient evidence"
    screening = {}
    if len(pairs) == 3 and all(p["sufficient"] for p in pairs):
        d = [p["delta_median_ns"]["E_ns"] for p in pairs]
        median = statistics.median(d)
        threshold = max(
            300_000_000, statistics.median(p["A"]["E_ns"]["p50"] for p in pairs) * 0.1
        )
        screening = dict(
            delta_E_all_positive=all(v > 0 for v in d),
            median_delta_E_ns=median,
            delta_E_range_ns=max(d) - min(d),
            threshold_ns=threshold,
        )
        if all(v > 0 for v in d) and median >= threshold and median > max(d) - min(d):
            w = [p["delta_median_ns"]["W_ns"] for p in pairs]
            if all(v > 0 for v in w) and statistics.median(w) >= median * 0.5:
                if all(
                    p["delta_median_ns"]["W_minus_C_ns"] > p["delta_median_ns"]["C_ns"]
                    for p in pairs
                ):
                    verdict = "prioritize capture wall minus thread CPU difference"
                elif all(
                    p["delta_median_ns"]["C_ns"] > p["delta_median_ns"]["W_minus_C_ns"]
                    for p in pairs
                ):
                    verdict = "prioritize capture thread CPU"
                else:
                    verdict = "mixed or unknown"
            else:
                entrance = [
                    p["delta_median_ns"]["entrance_before_sum_ns"] for p in pairs
                ]
                verdict = "mixed or unknown"
                if (
                    all(v is not None and v > 0 for v in entrance)
                    and statistics.median(entrance) >= median * 0.5
                ):
                    verdict = "prioritize observable pre-handler interval"
        else:
            verdict = "no stable diagnostic lead formed; does not establish absence of contention"
    return dict(
        pairs=pairs,
        screening=screening,
        verdict=verdict,
        causal_or_significance_claim=False,
    )


if __name__ == "__main__":
    target = Path(sys.argv[1])
    value = (
        paired_results(json.loads(target.read_text()))
        if target.is_file()
        else analyze(target)
    )
    print(json.dumps(value, indent=2))
