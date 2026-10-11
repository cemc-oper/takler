"""Run one bounded scheduler pass on the R3 fixed-width tree shape.

Examples: uv run python scripts/r3_10_execution_benchmark.py --tasks 10000 --mode scan
          uv run python scripts/r3_10_execution_benchmark.py --tasks 1000 --mode burst
"""

import argparse
import asyncio
import json
import resource
import time

from takler.core import Bunch, Flow, NodeStatus, Task
from takler.logging import configure
from takler.server.scheduler import Scheduler


def percentile(values, percentile_value):
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * percentile_value))]


async def run_benchmark(task_count: int, mode: str):
    starts = []

    class BenchTask(Task):
        def do_run(self):
            starts.append((self.get_flow().name, time.perf_counter()))
            return True

    bunch = Bunch()
    flow_count = (task_count + 999) // 1000
    built_at = time.perf_counter()
    for flow_index in range(flow_count):
        flow = bunch.add_flow(Flow(f"flow{flow_index:04d}"))
        for task_index in range(min(1000, task_count - flow_index * 1000)):
            task = flow.add_task(BenchTask(f"task{task_index:04d}"))
            if mode == "scan":
                task.set_node_status_only(NodeStatus.submitted)
        flow.begin()
        if mode == "scan":
            for task in flow.children:
                task.set_node_status_only(NodeStatus.submitted)
    build_seconds = time.perf_counter() - built_at

    scheduler = Scheduler(bunch, interval_main_loop=60)
    completed_flows = 0
    original = scheduler._flow_work

    def counting_work(name, flow, now):
        nonlocal completed_flows
        yield from original(name, flow, now)
        completed_flows += 1
        if completed_flows == flow_count:
            scheduler.should_stop = True

    scheduler._flow_work = counting_work
    lag_ms = []
    probe_start = time.perf_counter()

    async def probe():
        due = time.perf_counter() + 0.005
        while not scheduler.should_stop:
            await asyncio.sleep(max(0, due - time.perf_counter()))
            now = time.perf_counter()
            lag_ms.append(max(0, now - due) * 1000)
            due += 0.005

    probe_task = asyncio.create_task(probe())
    await scheduler.main_loop()
    elapsed = time.perf_counter() - probe_start
    await probe_task
    first_other = None
    longest_wait = 0.0
    if starts:
        first_other = next(
            (index for index, (name, _) in enumerate(starts) if name != "flow0000"),
            None,
        )
        longest_wait = (starts[-1][1] - probe_start) * 1000
    return {
        "tasks": task_count,
        "mode": mode,
        "flow_count": flow_count,
        "build_seconds": round(build_seconds, 3),
        "pass_seconds": round(elapsed, 3),
        "throughput_tasks_per_second": round(task_count / elapsed, 1),
        "first_other_flow_position": first_other,
        "longest_runnable_wait_ms": round(longest_wait, 1),
        "loop_lag_p99_ms": round(percentile(lag_ms, 0.99), 1),
        "loop_lag_max_ms": round(max(lag_ms, default=0), 1),
        "loop_lag_samples": len(lag_ms),
        "rss_max_mib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024),
        "start_count": len(starts),
    }


if __name__ == "__main__":
    configure(level="CRITICAL", console=False)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tasks", type=int, choices=(1000, 10000, 100000), required=True
    )
    parser.add_argument("--mode", choices=("scan", "burst"), required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run_benchmark(args.tasks, args.mode)), sort_keys=True))
