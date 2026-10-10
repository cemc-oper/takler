"""R3-05 read-only capture prototypes and reproducible local cost samples.

Run from repo/takler with ``uv run python scripts/r3_05_capture_prototype.py``.
This is an experiment, not a checkpoint or query implementation. No version
is published to a live server. Timings include only the named local stages.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import gc
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import platform
import resource
import statistics
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from pydantic import Field

from takler.core import Bunch, Flow, NodeStatus, Task
from takler.protocol.commands import LoadCommand, ReplaceCommand
from takler.schema.definition import DefinitionModel
from takler.serialization import checkpoint as checkpoint_module
from takler.serialization.checkpoint import capture_checkpoint, restore_checkpoint
from takler.serialization.query_projection import (
    iter_query_summaries,
    project_query_detail,
)
from takler.serialization.definition import export_definition
from takler.serialization.registry import NodeRegistration, builtin_registry
from takler.server.scheduler import Scheduler

from query_a_matrix_100k import make_tree


SHARD_NODES = 256
PAGE_BYTES = 230_000
MAX_PAGE_NODES = 2048
DETAIL_GROUPS = ("definition", "parameters", "runtime", "artifacts")


def seconds(start):
    return round(time.perf_counter() - start, 6)


def rss_bytes():
    with open("/proc/self/status", encoding="ascii") as stream:
        for line in stream:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("VmRSS unavailable")


def peak_rss_bytes():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


def checksum(payload):
    return hashlib.sha256(payload).hexdigest()


def accept_bytes(payload):
    return checksum(payload)


@dataclass(frozen=True)
class FrozenVersion:
    """One immutable root, with separately frozen runtime and secret bytes."""

    definition: bytes
    runtime_shards: tuple[tuple[tuple[str, bytes], ...], ...]
    secrets: bytes
    paths: Mapping[str, tuple[int, int]]

    @classmethod
    def from_document(cls, document):
        rows = tuple(
            (path, row.model_dump_json(exclude_unset=True).encode())
            for path, row in document.runtime.nodes.items()
        )
        shards = tuple(
            rows[index : index + SHARD_NODES]
            for index in range(0, len(rows), SHARD_NODES)
        )
        paths = {
            path: (shard_index, offset)
            for shard_index, shard in enumerate(shards)
            for offset, (path, _) in enumerate(shard)
        }
        return cls(
            json.dumps(document.definition, separators=(",", ":")).encode(),
            shards,
            document.secrets.model_dump_json(exclude_unset=True).encode(),
            MappingProxyType(paths),
        )

    def update_runtime(self, changes: dict[str, bytes]):
        changed_shards: dict[int, list[tuple[str, bytes]]] = {}
        for path, record in changes.items():
            shard_index, offset = self.paths[path]
            shard = changed_shards.setdefault(
                shard_index, list(self.runtime_shards[shard_index])
            )
            shard[offset] = (path, record)
        shards = list(self.runtime_shards)
        for index, shard in changed_shards.items():
            shards[index] = tuple(shard)
        return FrozenVersion(self.definition, tuple(shards), self.secrets, self.paths)

    def record(self, path):
        shard, offset = self.paths[path]
        return self.runtime_shards[shard][offset][1]

    def retained_bytes(self):
        return (
            len(self.definition)
            + len(self.secrets)
            + sum(
                len(path) + len(value)
                for shard in self.runtime_shards
                for path, value in shard
            )
        )


def instrument_checkpoint(bunch):
    stages = {"definition": 0.0, "node_capture": 0.0, "validation": 0.0}
    originals = {
        "export_definition": checkpoint_module.export_definition,
        "_capture_node": checkpoint_module._capture_node,
        "_validate_checkpoint_view": checkpoint_module._validate_checkpoint_view,
    }

    def timed(name, fn):
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                stages[name] += time.perf_counter() - start

        return wrapper

    try:
        for name, original in originals.items():
            stage = {
                "export_definition": "definition",
                "_capture_node": "node_capture",
                "_validate_checkpoint_view": "validation",
            }[name]
            setattr(checkpoint_module, name, timed(stage, original))
        start = time.perf_counter()
        document = capture_checkpoint(bunch)
        stages["capture_total"] = seconds(start)
    finally:
        for name, original in originals.items():
            setattr(checkpoint_module, name, original)
    return document, {key: round(value, 6) for key, value in stages.items()}


async def loop_delay_while(function, *, threaded=False):
    delays = []
    ready = asyncio.Event()

    async def ticker():
        target = asyncio.get_running_loop().time() + 0.005
        ready.set()
        for _ in range(30):
            await asyncio.sleep(max(0, target - asyncio.get_running_loop().time()))
            delays.append(max(0, asyncio.get_running_loop().time() - target))
            target += 0.005

    task = asyncio.create_task(ticker())
    await ready.wait()
    await asyncio.sleep(0)
    if threaded:
        await asyncio.to_thread(function)
    else:
        function()
    await task
    return round(max(delays), 6)


def publish_bytes(payload):
    with tempfile.TemporaryDirectory() as directory:
        target = Path(directory) / "checkpoint.json"
        temporary = target.with_suffix(".tmp")
        start = time.perf_counter()
        with temporary.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        handle = os.open(directory, os.O_DIRECTORY)
        try:
            os.fsync(handle)
        finally:
            os.close(handle)
        elapsed = seconds(start)
        assert target.stat().st_size == len(payload)
    return elapsed


def checkpoint_case(size, wide=False):
    start = time.perf_counter()
    bunch, tasks = make_tree(size, wide)
    construction = seconds(start)
    gc.collect()
    rss_before = rss_bytes()
    document, stages = instrument_checkpoint(bunch)
    rss_after_capture = rss_bytes()
    start = time.perf_counter()
    payload = document.model_dump_json(exclude_unset=True).encode()
    encode = seconds(start)
    loop_lag_sync = asyncio.run(
        loop_delay_while(lambda: document.model_dump_json(exclude_unset=True))
    )
    loop_lag_thread = asyncio.run(
        loop_delay_while(
            lambda: document.model_dump_json(exclude_unset=True), threaded=True
        )
    )
    start = time.perf_counter()
    version = FrozenVersion.from_document(document)
    freeze = seconds(start)
    initial_retained = version.retained_bytes()
    rss_after_freeze = rss_bytes()
    selected = tasks[0]
    path = selected.node_path
    old = version.record(path)
    selected.state.node_status = NodeStatus.queued
    registry = builtin_registry()
    samples = []
    for _ in range(21):
        start = time.perf_counter()
        record = (
            checkpoint_module._capture_node(selected, registry)
            .model_dump_json(exclude_unset=True)
            .encode()
        )
        newer = version.update_runtime({path: record})
        samples.append(time.perf_counter() - start)
    assert version.record(path) == old and newer.record(path) != old
    start = time.perf_counter()
    changes = {}
    for task in tasks[: min(100, len(tasks))]:
        task.state.node_status = NodeStatus.queued
        changes[task.node_path] = (
            checkpoint_module._capture_node(task, registry)
            .model_dump_json(exclude_unset=True)
            .encode()
        )
    batch = version.update_runtime(changes)
    batch_seconds = seconds(start)
    assert version.record(path) == old and batch.record(path) != old
    start = time.perf_counter()
    thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    thread.submit(accept_bytes, b"").result()
    thread_handoff = seconds(start)
    start = time.perf_counter()
    thread_digest = thread.submit(accept_bytes, payload).result()
    thread_transfer = seconds(start)
    thread.shutdown()
    assert thread_digest == checksum(payload)
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=1, mp_context=multiprocessing.get_context("fork")
    ) as process:
        process.submit(accept_bytes, b"").result()
        start = time.perf_counter()
        process_digest = process.submit(accept_bytes, payload).result()
        process_transfer = seconds(start)
    assert process_digest == checksum(payload)
    publish = publish_bytes(payload)
    first_page = payload[:262_144]
    start = time.perf_counter()
    for _ in range(100):
        memoryview(first_page).tobytes()
    page_return = round((time.perf_counter() - start) / 100, 8)
    if size <= 10_000:
        start = time.perf_counter()
        restored = restore_checkpoint(document)
        restore = seconds(start)
        assert restored.find_node(path) is not None
    else:
        restore = None
    scheduler = Scheduler(bunch)
    load_candidate = Flow("loaded")
    for index in range(1000):
        load_candidate.add_task(f"task{index:04d}")
    load_bytes = export_definition(load_candidate).model_dump_json().encode()
    start = time.perf_counter()
    scheduler.run_command_load(LoadCommand(flow_bytes=load_bytes))
    load = seconds(start)
    assert bunch.find_flow("loaded") is not None
    replace_candidate = Flow("flow000")
    for index in range(1000):
        replace_candidate.add_task(f"task{index:04d}")
    replace_bytes = export_definition(replace_candidate).model_dump_json().encode()
    start = time.perf_counter()
    scheduler.run_command_replace(
        ReplaceCommand(target_path="/flow000", flow_bytes=replace_bytes)
    )
    replace = seconds(start)
    assert bunch.find_node("/flow000/task0999") is not None
    return {
        "nodes": size + (size + 999) // 1000 + 1,
        "wide_definition": wide,
        "tree_build_s": construction,
        "checkpoint_stages_s": stages,
        "json_encode_s": encode,
        "loop_lag_sync_encode_s": loop_lag_sync,
        "loop_lag_thread_encode_s": loop_lag_thread,
        "json_bytes": len(payload),
        "freeze_s": freeze,
        "frozen_payload_bytes": initial_retained,
        "single_runtime_update_us": {
            "p50": round(statistics.median(samples) * 1e6, 2),
            "p95": round(sorted(samples)[19] * 1e6, 2),
            "max": round(max(samples) * 1e6, 2),
        },
        "batch_100_update_s": batch_seconds,
        "thread_start_and_empty_s": thread_handoff,
        "thread_payload_s": thread_transfer,
        "process_payload_s": process_transfer,
        "atomic_publish_s": publish,
        "page_return_s": page_return,
        "restore_s": restore,
        "load_1000_s": load,
        "replace_1000_s": replace,
        "rss_before_capture": rss_before,
        "rss_after_capture": rss_after_capture,
        "rss_after_freeze": rss_after_freeze,
        "peak_rss": peak_rss_bytes(),
    }


def detail_case(size, wide, *, decode=False):
    start = time.perf_counter()
    bunch, _ = make_tree(size, wide)
    construction = seconds(start)
    start = time.perf_counter()
    summaries = tuple(
        (row.path, row.model_dump_json(exclude_unset=True).encode())
        for row, _ in iter_query_summaries(bunch)
    )
    summary_freeze = seconds(start)
    start = time.perf_counter()
    details = tuple(
        project_query_detail(
            bunch,
            path,
            (*DETAIL_GROUPS, "service") if path == "/" else DETAIL_GROUPS,
            redacted_parameters=("SECRET",),
        )
        .model_dump_json(exclude_unset=True)
        .encode()
        for path, _ in summaries
    )
    detail_freeze = seconds(start)
    assert len(summaries) == size + (size + 999) // 1000 + 1
    assert len(details) == len(summaries)
    assert (b'"PAYLOAD"' in details[2]) == wide
    frozen_bytes = sum(len(raw) for _, raw in summaries) + sum(map(len, details))
    start = time.perf_counter()
    page_sizes = []
    page_size = 1024
    nodes = 0
    digest = hashlib.sha256()
    for (_, summary), detail in zip(summaries, details, strict=True):
        digest.update(summary)
        digest.update(detail)
        row_size = len(summary) + len(detail) + 64
        if page_size + row_size > PAGE_BYTES or nodes >= MAX_PAGE_NODES:
            page_sizes.append(page_size)
            page_size, nodes = 1024, 0
        page_size += row_size
        nodes += 1
    page_sizes.append(page_size)
    streamed = seconds(start)
    assert max(page_sizes) <= PAGE_BYTES
    decoded = None
    if decode:
        before_decode = rss_bytes()
        start = time.perf_counter()
        assembled = {
            path: {"summary": json.loads(summary), "groups": json.loads(detail)}
            for (path, summary), detail in zip(summaries, details, strict=True)
        }
        assert len(assembled) == len(summaries)
        decoded = {
            "seconds": seconds(start),
            "incremental_rss_bytes": rss_bytes() - before_decode,
        }
        del assembled
        gc.collect()
    return {
        "tasks": size,
        "wide": wide,
        "payload_chars_per_task": 2048 if wide else 0,
        "tree_build_s": construction,
        "summary_freeze_s": summary_freeze,
        "detail_freeze_s": detail_freeze,
        "stream_consume_s": streamed,
        "frozen_bytes": frozen_bytes,
        "page_count": len(page_sizes),
        "max_page_bytes": max(page_sizes),
        "digest": digest.hexdigest(),
        "rss_after_freeze": rss_bytes(),
        "peak_rss": peak_rss_bytes(),
        "assembled": decoded,
    }


def optimistic_case(size, writer_interval=0.002, trials=20):
    values = [0] * size
    revision = [0]
    stop = threading.Event()

    def writer():
        index = 0
        while not stop.wait(writer_interval):
            revision[0] += 1
            values[index % size] += 1
            revision[0] += 1
            index += 1

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    attempt_counts = []
    successes = 0
    start = time.perf_counter()
    try:
        for _ in range(trials):
            for attempt in range(1, 4):
                before = revision[0]
                chunks = []
                for index in range(0, size, SHARD_NODES):
                    chunks.append(tuple(values[index : index + SHARD_NODES]))
                    if index % (8 * SHARD_NODES) == 0:
                        time.sleep(0)
                if before % 2 == 0 and before == revision[0]:
                    successes += 1
                    assert sum(map(len, chunks)) == size
                    break
            attempt_counts.append(attempt)
    finally:
        stop.set()
        thread.join()
    return {
        "nodes": size,
        "writer_interval_s": writer_interval,
        "trials": trials,
        "successes": successes,
        "attempt_counts": attempt_counts,
        "elapsed_s": seconds(start),
    }


def semantic_case():
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task(Task("t"))
    flow.begin()
    assert task.run()
    task.mark_submission_unknown("external reply lost")
    document = capture_checkpoint(bunch)
    version = FrozenVersion.from_document(document)
    record = json.loads(version.record("/f/t"))
    assert record["submission_unknown_reason"] == "external reply lost"
    assert record["attempt_id"] == task.attempt_id
    assert b"external reply lost" not in version.secrets
    assert task.job_password.encode() in version.secrets
    task.submission_unknown_reason = "later mutation"
    assert (
        json.loads(version.record("/f/t"))["submission_unknown_reason"]
        == "external reply lost"
    )

    class PluginTask(Task):
        def __init__(self, name, command):
            super().__init__(name)
            self.command = command
            self.payload = {"nested": [1, 2]}

    class PluginDefinition(DefinitionModel):
        command: str

    class PluginRuntime(DefinitionModel):
        payload: dict[str, list[int]] = Field(default_factory=dict)

    registry = builtin_registry()
    registry.register(
        NodeRegistration(
            type_id="example.r3_05",
            python_type=PluginTask,
            kind="task",
            definition_schema=PluginDefinition,
            export_definition=lambda node: {"command": node.command},
            construct=lambda name, data: PluginTask(name, data.command),
            runtime_schema=PluginRuntime,
            export_runtime=lambda node: {"payload": node.payload},
            restore_runtime=lambda node, data: setattr(node, "payload", data.payload),
            query=lambda node: {"command": node.command},
        )
    )
    plugin_bunch = Bunch()
    plugin = plugin_bunch.add_flow(Flow("p")).add_task(PluginTask("t", "echo"))
    start = time.perf_counter()
    plugin_doc = capture_checkpoint(plugin_bunch, registry=registry)
    plugin_capture = seconds(start)
    start = time.perf_counter()
    plugin_version = FrozenVersion.from_document(plugin_doc)
    plugin_freeze = seconds(start)
    plugin.payload["nested"].append(3)
    assert json.loads(plugin_version.record("/p/t"))["runtime_data"]["payload"] == {
        "nested": [1, 2]
    }
    plugin_samples = []
    for _ in range(21):
        start = time.perf_counter()
        record = (
            checkpoint_module._capture_node(plugin, registry)
            .model_dump_json(exclude_unset=True)
            .encode()
        )
        changed = plugin_version.update_runtime({"/p/t": record})
        plugin_samples.append(time.perf_counter() - start)
    assert changed.record("/p/t") != plugin_version.record("/p/t")
    restored = restore_checkpoint(plugin_doc, registry=registry)
    assert restored.find_node("/p/t").payload == {"nested": [1, 2]}
    return {
        "unknown_marker": True,
        "secret_separation": True,
        "nested_plugin_copy": True,
        "plugin_capture_s": plugin_capture,
        "plugin_freeze_s": plugin_freeze,
        "plugin_record_bytes": len(plugin_version.record("/p/t")),
        "plugin_version_bytes": plugin_version.retained_bytes(),
        "plugin_update_us": {
            "p50": round(statistics.median(plugin_samples) * 1e6, 2),
            "p95": round(sorted(plugin_samples)[19] * 1e6, 2),
            "max": round(max(plugin_samples) * 1e6, 2),
        },
    }


def machine_info():
    return {
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "cpu_count": os.cpu_count(),
        "memory_total_bytes": os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"),
        "cwd": str(Path.cwd()),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument(
        "--only", choices=("all", "checkpoint", "detail", "optimistic"), default="all"
    )
    parser.add_argument("--sizes", type=int, nargs="+")
    parser.add_argument(
        "--detail-kind", choices=("both", "standard", "wide"), default="both"
    )
    parser.add_argument("--decode", action="store_true")
    parser.add_argument("--checkpoint-wide", action="store_true")
    options = parser.parse_args()
    results = {"machine": machine_info(), "semantics": semantic_case()}
    sizes = options.sizes or ((1000,) if options.quick else (1000, 10_000, 100_000))
    if options.only in ("all", "checkpoint"):
        results["checkpoint"] = [
            checkpoint_case(size, wide=options.checkpoint_wide) for size in sizes
        ]
    if options.only in ("all", "optimistic"):
        results["optimistic"] = [optimistic_case(size) for size in sizes]
    if options.only in ("all", "detail"):
        kinds = {
            "both": (False, True),
            "standard": (False,),
            "wide": (True,),
        }[options.detail_kind]
        results["detail"] = [
            detail_case(size, wide, decode=options.decode)
            for size in sizes
            for wide in kinds
        ]
    raw = json.dumps(results, indent=2) + "\n"
    if options.output:
        options.output.write_text(raw, encoding="utf-8")
    print(raw)


if __name__ == "__main__":
    main()
