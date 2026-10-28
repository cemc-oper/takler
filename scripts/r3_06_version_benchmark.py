"""Measure R3-06 version construction, COW changes, and retained roots.

Run from repo/takler with ``uv run python scripts/r3_06_version_benchmark.py``.
This is a local microbenchmark, not the R3-14 concurrent service test.
"""

from __future__ import annotations

import argparse
import gc
import json
import resource
import statistics
import time

from takler.core import Flow, NodeStatus
from takler.protocol.commands import LoadCommand, ReplaceCommand
from takler.serialization.definition import export_definition
from takler.server.scheduler import Scheduler

from query_a_matrix_100k import make_tree


def rss_bytes():
    with open("/proc/self/status", encoding="ascii") as stream:
        for line in stream:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("VmRSS unavailable")


def sample(size, wide, *, structure=False):
    bunch, tasks = make_tree(size, wide)
    gc.collect()
    before = rss_bytes()
    started = time.perf_counter()
    scheduler = Scheduler(bunch)
    build_s = time.perf_counter() - started
    if not scheduler.version_store.ready:
        raise RuntimeError(scheduler.version_store.error)
    base = scheduler.version_store.pin()
    after_build = rss_bytes()
    individual = []
    for index in range(21):
        task = tasks[index]
        started = time.perf_counter()
        task.set_node_status_only(NodeStatus.queued)
        current = scheduler.version_store.pin()
        individual.append(time.perf_counter() - started)
    shared = sum(a is b for a, b in zip(base.nodes, current.nodes))
    scheduler.query_changes.begin()
    started = time.perf_counter()
    for task in tasks[21:121]:
        task.set_node_status_only(NodeStatus.queued)
    scheduler.query_changes.end()
    scheduler.version_store.flush()
    batch_s = time.perf_counter() - started
    batch = scheduler.version_store.pin()
    result = {
        "tasks": size,
        "wide": wide,
        "build_s": round(build_s, 6),
        "single_update_us": {
            "p50": round(statistics.median(individual) * 1e6, 2),
            "p95": round(sorted(individual)[19] * 1e6, 2),
            "max": round(max(individual) * 1e6, 2),
        },
        "batch_100_s": round(batch_s, 6),
        "base_retained_bytes": base.retained_bytes,
        "batch_retained_bytes": batch.retained_bytes,
        "shared_shards_after_21": shared,
        "total_shards": len(base.nodes),
        "rss_before": before,
        "rss_after_build": after_build,
        "peak_rss": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
    }
    if structure:
        loaded = Flow("loaded")
        replacement = Flow("flow000")
        for index in range(1000):
            name = f"task{index:04d}"
            loaded.add_task(name)
            replacement.add_task(name)
        load_bytes = export_definition(loaded).model_dump_json().encode()
        replacement_bytes = export_definition(replacement).model_dump_json().encode()
        started = time.perf_counter()
        scheduler.run_command_load(LoadCommand(flow_bytes=load_bytes))
        after_load = scheduler.version_store.pin()
        result["load_1000_and_pin_s"] = round(time.perf_counter() - started, 6)
        started = time.perf_counter()
        scheduler.run_command_replace(
            ReplaceCommand(target_path="/flow000", flow_bytes=replacement_bytes)
        )
        after_replace = scheduler.version_store.pin()
        result["replace_1000_and_pin_s"] = round(time.perf_counter() - started, 6)
        result["retained_versions"] = [
            base.retained_bytes,
            after_load.retained_bytes,
            after_replace.retained_bytes,
        ]
        result["peak_rss"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", type=int, nargs="+", default=[1000, 10000, 100000])
    parser.add_argument("--wide", action="store_true")
    parser.add_argument("--structure", action="store_true")
    parser.add_argument("--output")
    options = parser.parse_args()
    results = [
        sample(size, options.wide, structure=options.structure)
        for size in options.sizes
    ]
    raw = json.dumps(results, indent=2) + "\n"
    if options.output:
        with open(options.output, "w", encoding="utf-8") as stream:
            stream.write(raw)
    print(raw)


if __name__ == "__main__":
    main()
