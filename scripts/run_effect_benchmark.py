"""Time 250 normal RUN/INIT pairs outside handlers, without changing task hooks."""

import argparse
import asyncio
import gc
import json
from pathlib import Path
import resource
import time

from mix100k01 import fixture, pool_state
from takler.protocol.commands import Command, RunCommand, InitCommand
from takler.server.auth import (
    CallCredentials,
    set_call_credentials,
    reset_call_credentials,
)
from takler.server.handlers import CommandHandlers
from takler.server.scheduler import Scheduler


async def measure(tasks):
    from takler.logging import configure

    configure(level="WARNING")
    built = time.perf_counter()
    bunch, paths = fixture(tasks)
    scheduler = Scheduler(bunch)
    scheduler.version_store.rebuild(state_version=scheduler.query_changes.revision)
    handler = CommandHandlers(scheduler)
    rows = []
    started = time.perf_counter()
    cpu = time.process_time()
    for path in paths:
        task = bunch.find_node(path)
        for command in (Command.RUN, Command.INIT):
            request = (
                RunCommand(node_paths=[path])
                if command is Command.RUN
                else InitCommand(
                    node_path=path,
                    task_id="local-" + task.attempt_id,
                    attempt_id=task.attempt_id,
                )
            )
            token = set_call_credentials(
                CallCredentials(job_password=task.job_password)
            )
            before = time.perf_counter_ns()
            try:
                reply = await handler.handle(command, request)
            finally:
                elapsed = time.perf_counter_ns() - before
                reset_call_credentials(token)
            rows.append(
                dict(
                    command=command.value,
                    path=path,
                    elapsed_ns=elapsed,
                    flag=reply.flag,
                )
            )
            assert reply.flag == 0
            assert all(r.effect == "applied" for r in getattr(reply, "results", []))
    elapsed = time.perf_counter() - started
    used_cpu = time.process_time() - cpu
    state = pool_state(bunch, paths)
    return dict(
        log_level="WARNING",
        tasks=tasks,
        nodes=tasks + len(bunch.flows) + 1,
        fixture_rebuild_s=started - built,
        pairs_s=elapsed,
        cpu_s=used_cpu,
        max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold()),
        rows=rows,
        pool=state,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, choices=[10000, 100000], required=True)
    parser.add_argument("--output", type=Path, required=True)
    options = parser.parse_args()
    options.output.write_text(
        json.dumps(asyncio.run(measure(options.tasks)), indent=2) + "\n"
    )
