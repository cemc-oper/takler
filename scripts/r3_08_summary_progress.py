"""Single-reader 100k summary capture/reconnect/delta probe for R3-08."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

from query_a_matrix_100k import make_tree
from takler.protocol.query_v1 import PageRequest, SinceRequest
from takler.server.scheduler import Scheduler


async def _snapshot(service, request):
    started = time.perf_counter()
    last_progress = started
    max_no_progress = 0.0
    reply = await service.page(request)
    now = time.perf_counter()
    max_no_progress = max(max_no_progress, now - last_progress)
    last_progress = now
    if reply.kind != "page":
        raise RuntimeError(f"summary capture failed: {reply}")
    first = reply
    count = 1
    nodes = len(reply.nodes)
    while reply.next_cursor:
        reply = await service.page(
            request.model_copy(update={"cursor": reply.next_cursor})
        )
        now = time.perf_counter()
        max_no_progress = max(max_no_progress, now - last_progress)
        last_progress = now
        if reply.kind != "page":
            raise RuntimeError(f"summary continuation failed: {reply}")
        count += 1
        nodes += len(reply.nodes)
    return first, {
        "elapsed_s": round(time.perf_counter() - started, 3),
        "pages": count,
        "nodes": nodes,
        "max_no_progress_s": round(max_no_progress, 3),
        "rejected_requests": 0,
        "retries": 0,
    }


async def sample(size: int):
    bunch, tasks = make_tree(size, False)
    service = Scheduler(bunch).query_service
    request = PageRequest(
        kind="page_request", schema_version=1, field_groups=["summary"]
    )
    first, initial = await _snapshot(service, request)
    tasks[0].suspend()
    delta_started = time.perf_counter()
    delta = await service.since(
        SinceRequest(
            kind="since_request",
            schema_version=1,
            scope_path="/",
            field_groups=["summary"],
            session_id=first.session_id,
            query_id=first.query_id,
            since_revision=first.base_revision,
        )
    )
    if delta.kind != "since" or delta.unchanged:
        raise RuntimeError(f"nonempty delta failed: {delta}")
    delta_elapsed = time.perf_counter() - delta_started
    _, reconnect = await _snapshot(service, request)
    return {
        "tasks": size,
        "initial": initial,
        "nonempty_delta_s": round(delta_elapsed, 3),
        "delta_operations": len(delta.operations),
        "reconnect": reconnect,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=100000)
    parser.add_argument("--output")
    options = parser.parse_args()
    result = asyncio.run(sample(options.size))
    raw = json.dumps(result, indent=2) + "\n"
    if options.output:
        Path(options.output).write_text(raw)
    print(raw)


if __name__ == "__main__":
    main()
