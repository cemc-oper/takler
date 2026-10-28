"""Compare the old full-tree artifact scan with the retained-path index.

Run one scenario per process so RSS peaks and cleanup do not bleed between
sizes. This uses synthetic terminal tasks and does not create job files.
"""

import argparse
import json
import statistics
import time
from pathlib import Path

import psutil

from takler.core import Bunch, Flow, Task
from takler.serialization.builder import walk
from takler.serialization.artifact_path_index import ArtifactPathIndex


def build(size: int, references: int) -> Bunch:
    bunch = Bunch()
    fields = ("job_path", "jobout_path", "orvix_info_path")[:references]
    for flow_no in range((size + 999) // 1000):
        flow = Flow(f"f{flow_no:03d}")
        for task_no in range(min(1000, size - flow_no * 1000)):
            task = Task(f"t{task_no:04d}")
            for kind, field in enumerate(fields):
                setattr(
                    task,
                    field,
                    f"/tmp/r3-09-artifacts/{flow_no:03d}/{task_no:04d}.{kind}",
                )
            flow.add_task(task)
        bunch.add_flow(flow)
    return bunch


def legacy_scan(bunch: Bunch, candidate: Path) -> bool:
    for node in walk(bunch):
        if not isinstance(node, Task):
            continue
        for value in (node.job_path, node.jobout_path, node.orvix_info_path):
            if value is not None and Path(value).resolve() == candidate:
                return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--references", type=int, choices=(0, 1, 3), required=True)
    args = parser.parse_args()
    process = psutil.Process()
    started = time.perf_counter()
    bunch = build(args.size, args.references)
    build_seconds = time.perf_counter() - started
    rss_before_index = process.memory_info().rss
    candidate = Path("/tmp/r3-09-artifacts/no-collision").resolve()

    started = time.perf_counter()
    old_result = legacy_scan(bunch, candidate)
    legacy_seconds = time.perf_counter() - started

    started = time.perf_counter()
    index = ArtifactPathIndex(bunch)
    index_build_seconds = time.perf_counter() - started
    index_rss_bytes = process.memory_info().rss - rss_before_index
    owner = next(node for node in walk(bunch) if isinstance(node, Task))
    samples = []
    for _ in range(21):
        started = time.perf_counter()
        new_result = index.conflicts(owner, {candidate})
        samples.append(time.perf_counter() - started)
    assert old_result == new_result is False
    print(
        json.dumps(
            {
                "tasks": args.size,
                "references_per_task": args.references,
                "build_seconds": build_seconds,
                "legacy_scan_seconds": legacy_seconds,
                "index_build_seconds": index_build_seconds,
                "index_extra_rss_bytes": index_rss_bytes,
                "index_entries": index.entry_count,
                "warm_lookup_p50_seconds": statistics.median(samples),
                "warm_lookup_max_seconds": max(samples),
                "process_rss_bytes": process.memory_info().rss,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
