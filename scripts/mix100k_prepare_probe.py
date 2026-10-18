"""One bounded, in-process diagnostic of the MIX100K legal pool preparation.

No transport, scheduler loop, checkpoint task, or mixed-load run is started.
Only process-local timing wrappers change; the product checkout stays intact.
"""

import argparse
import asyncio
from contextlib import ExitStack, contextmanager
import gc
import hashlib
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

from mix100k01 import (
    GIB,
    NS,
    Ledger,
    fixture,
    pool_state,
    prepare,
    read,
    remaining,
    save,
)
from takler.server.handlers import CommandHandlers


def store_state(store):
    root = store.current
    return dict(
        pending_paths=len(store._pending),
        needs_rebuild=store._needs_rebuild,
        ready=store.ready,
        error=store.error,
        state_version=None if root is None else root.state_version,
        nodes=0 if root is None else len(root.index),
    )


@contextmanager
def timed_handlers(scheduler, log):
    """Wrap the real shared prepare() entry; persist only after each command.

    _run includes its awaited INIT coroutine. Flush intervals are separate;
    a nested flush is subtracted from the inclusive body to avoid double count.
    Each command has at most eight intervals; no per-node observation is added.
    """
    original_handle, original_run = CommandHandlers.handle, CommandHandlers._run
    store = scheduler.version_store
    original_flush = store.flush
    had_override = "flush" in vars(store)
    active = None
    serial = 0

    async def body(handler, *args, **kwargs):
        if active is None or handler.scheduler is not scheduler:
            raise ValueError("unattributed handler body")
        start, cpu = time.monotonic_ns(), time.process_time_ns()
        try:
            value = original_run(handler, *args, **kwargs)
            return await value if inspect.isawaitable(value) else value
        finally:
            if len(active["body"]) >= 8:
                raise ValueError("body interval capacity")
            active["body"].append(
                dict(
                    start_ns=start,
                    end_ns=time.monotonic_ns(),
                    cpu_ns=time.process_time_ns() - cpu,
                )
            )

    def flush():
        if active is None:
            raise ValueError("unattributed version flush")
        before = store_state(store)
        start, cpu = time.monotonic_ns(), time.process_time_ns()
        try:
            return original_flush()
        finally:
            end, used_cpu = time.monotonic_ns(), time.process_time_ns() - cpu
            if len(active["flush"]) >= 8:
                raise ValueError("flush interval capacity")
            active["flush"].append(
                dict(
                    start_ns=start,
                    end_ns=end,
                    cpu_ns=used_cpu,
                    before=before,
                    after=store_state(store),
                )
            )

    async def handle(handler, command, request, *args, **kwargs):
        nonlocal active, serial
        if active is not None or handler.scheduler is not scheduler:
            raise ValueError("concurrent/unrelated diagnostic command")
        serial += 1
        path = getattr(request, "node_path", None)
        if path is None:
            path = list(request.node_paths)[0]
        log("command_started", command_id=serial, command=command.value, path=path)
        active = dict(body=[], flush=[])
        start, cpu = time.monotonic_ns(), time.process_time_ns()
        response, error = None, None
        try:
            response = await original_handle(handler, command, request, *args, **kwargs)
            return response
        except BaseException as exc:
            error = type(exc).__name__
            raise
        finally:
            end, cpu_ns = time.monotonic_ns(), time.process_time_ns() - cpu
            spans, active = active, None
            body_ns = sum(s["end_ns"] - s["start_ns"] for s in spans["body"])
            flush_ns = sum(s["end_ns"] - s["start_ns"] for s in spans["flush"])
            overlap = sum(
                max(
                    0, min(b["end_ns"], f["end_ns"]) - max(b["start_ns"], f["start_ns"])
                )
                for b in spans["body"]
                for f in spans["flush"]
            )
            log(
                "command_completed",
                command_id=serial,
                command=command.value,
                path=path,
                start_ns=start,
                end_ns=end,
                total_ns=end - start,
                cpu_ns=cpu_ns,
                body_inclusive_ns=body_ns,
                body_exclusive_ns=body_ns - overlap,
                flush_ns=flush_ns,
                overlap_ns=overlap,
                remainder_ns=end - start - body_ns - flush_ns + overlap,
                body=spans["body"],
                flush=spans["flush"],
                flag=None if response is None else response.flag,
                error=error,
            )

    CommandHandlers.handle, CommandHandlers._run, store.flush = handle, body, flush
    try:
        yield
    finally:
        CommandHandlers.handle, CommandHandlers._run = original_handle, original_run
        if had_override:
            store.flush = original_flush
        else:
            del store.flush


def verify_pool(scheduler, paths, credentials, expected_nodes):
    states = pool_state(scheduler.bunch, paths)
    root = scheduler.version_store.pin()
    if len(root.index) != expected_nodes or scheduler.version_store._pending:
        raise ValueError("fixed version incomplete")
    digest = hashlib.sha256()
    for row in states:
        path = row["path"]
        task = scheduler.bunch.find_node(path)
        runtime = json.loads(root.node(path).runtime)
        shard, offset = root.index[path]
        secret = json.loads(root.secrets[shard][offset])
        identity = credentials[path]
        if (
            row["meter"] != 0
            or runtime["meter_values"]["progress"] != 0
            or runtime["attempt_id"] != identity["attempt"]
            or secret["attempt_id"] != identity["attempt"]
            or secret["password"] != identity["password"]
            or task.job_password != identity["password"]
            or row["task_id"] != "mix100k-" + identity["attempt"]
            or runtime["task_id"] != row["task_id"]
            or runtime["state"]["status"] != task.state.node_status.value
        ):
            raise ValueError("prepared live/fixed identity or meter mismatch")
        digest.update(root.node(path).runtime)
    return dict(
        verified=True,
        pool=len(states),
        nodes=len(root.index),
        state_version=root.state_version,
        runtime_sha256=digest.hexdigest(),
    )


async def worker(directory):
    from takler.server import TaklerServer
    from takler.server.connect_config import (
        Address,
        ConnectConfig,
        SecuritySettings,
        Server,
    )

    config = read(directory / "config.json")
    deadline = read(directory / "lifecycle.json")["operational_deadline_ns"]
    log = Ledger(directory / "commands.jsonl")
    initial_gc = dict(enabled=gc.isenabled(), threshold=gc.get_threshold())
    started = time.monotonic_ns()
    result = dict(success=False, error=None, stages=[])

    def stage(name, before):
        result["stages"].append(
            dict(name=name, start_ns=before, end_ns=time.monotonic_ns())
        )
        log("stage", **result["stages"][-1])
        remaining(deadline)

    try:
        connect = ConnectConfig(
            server=Server(
                address=Address(hostname="127.0.0.1", ip="127.0.0.1", port="0")
            ),
            security=SecuritySettings(auth_mode="enabled"),
        )
        before = time.monotonic_ns()
        server = TaklerServer(
            host="127.0.0.1",
            port=0,
            connect_config=connect,
            checkpoint_file=directory / "unused.check",
            checkpoint_interval=10,
        )
        stage("server_object", before)
        before = time.monotonic_ns()
        bunch, paths = fixture(config["tasks"])
        stage("fixture", before)
        before = time.monotonic_ns()
        for flow in bunch.flows.values():
            server.bunch.add_flow(flow)
        # Same restore/rebuild sequence as server.start(), before the original prepare().
        # No start() call: no sockets, background checkpoint, or scheduler loop.
        assert not server.checkpoint_manager.restore()
        server.scheduler.last_checkpoint_at = (
            server.checkpoint_manager.last_checkpoint_at
        )
        server.scheduler.restore_summary = server.checkpoint_manager.restore_summary
        server.scheduler.version_store.rebuild(
            state_version=server.scheduler.query_changes.revision
        )
        if not server.scheduler.version_store.ready:
            raise ValueError("initial version unavailable")
        stage("install_restore_rebuild", before)
        save(
            directory / "initial-state.json",
            dict(
                gc=initial_gc,
                store=store_state(server.scheduler.version_store),
                paths=paths,
                source=__import__("takler").__file__,
                grpc_started=server.grpc_transport.grpc_server is not None,
                checkpoint_task=server.checkpoint_manager._snapshot_task is not None,
            ),
        )
        before = time.monotonic_ns()
        with ExitStack() as timing:
            timing.enter_context(timed_handlers(server.scheduler, log))
            if config.get("run_chain_timing"):
                from mix100k_run_timing import timed_run_chain

                timing.enter_context(timed_run_chain(server.scheduler, log))
            credentials = await prepare(server.scheduler, paths, log, deadline)
        stage("prepare", before)
        before = time.monotonic_ns()
        result["pool"] = verify_pool(
            server.scheduler, paths, credentials, config["nodes"]
        )
        stage("verify", before)
        result["success"] = True
    except BaseException as exc:
        result.update(
            error=f"{type(exc).__name__}: {exc}", traceback=traceback.format_exc()
        )
    finally:
        result.update(
            start_ns=started,
            end_ns=time.monotonic_ns(),
            initial_gc=initial_gc,
            final_gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold()),
            network_service_starts=0,
        )
        save(directory / "worker-result.json", result)
        log.close()
    return result


def run(directory):
    from r3_14_perf04_safety import Registry, Watchdog, resource_sample

    config = read(directory / "config.json")
    frozen = directory / "diagnostic"
    for name, digest in read(frozen / "source-index.json").items():
        if hashlib.sha256((frozen / name).read_bytes()).hexdigest() != digest:
            raise ValueError("frozen source changed: " + name)
    if (directory / "lifecycle.json").exists():
        raise ValueError("diagnostic already attempted; no rerun")
    if (
        hashlib.sha256((directory / "config.json").read_bytes()).hexdigest()
        != (directory / "config.sha256").read_text().strip()
    ):
        raise ValueError("frozen diagnostic config changed")
    budget = read(directory / "budget.json")
    start = int(os.environ.get("MIX100K_PREP_START_NS", time.monotonic_ns()))
    if start / NS - budget["start_monotonic"] > 900:
        raise TimeoutError("preparation budget exhausted")
    operational, hard = start + 870 * NS, start + 900 * NS
    save(
        directory / "lifecycle.json",
        dict(
            start_ns=start, operational_deadline_ns=operational, hard_deadline_ns=hard
        ),
    )
    registry = Registry(directory / "registry.json")
    resources = Ledger(directory / "resources.jsonl")
    output = (directory / "worker.log").open("xb", buffering=0)
    process, error = None, None
    swap_start, over, last_pss = None, 0, 0

    def sample():
        nonlocal swap_start, over, last_pss
        now = time.monotonic_ns()
        value = resource_sample(registry, directory, os.getpid(), now - last_pss >= NS)
        if now - last_pss >= NS:
            last_pss = now
        resources("sample", sample=value)
        swap_start = value["swap_used"] if swap_start is None else swap_start
        rss = sum(p.get("rss_bytes", 0) for p in value["processes"])
        over = over + 1 if rss > 8 * GIB else 0
        if (
            value["mem_available"] < 4 * GIB
            or value["disk_free"] < 20 * GIB
            or value["swap_used"] - swap_start > GIB
            or over >= 2
            or value["evidence_bytes"] > config["evidence_max_bytes"]
            or any(
                p.get("rss_bytes", 0) > 512 * 1024**2
                for p in value["processes"]
                if p["role"] == "orchestrator"
            )
        ):
            raise RuntimeError("diagnostic resource stop line")

    try:
        sample()  # Before any owned child process, including watchdog.
        Watchdog(registry, hard).arm()
        env = {k: v for k, v in os.environ.items() if not k.startswith("TAKLER_")}
        env.update(
            PYTHONPATH=str(frozen / "src") + os.pathsep + str(frozen / "scripts"),
            PYTHONUNBUFFERED="1",
            TAKLER_LOG_LEVEL="WARNING",
        )
        process = registry.launch(
            [
                sys.executable,
                str(frozen / "scripts/mix100k_prepare_probe.py"),
                "worker",
                str(directory),
            ],
            "prepare_worker",
            stdout=output,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=directory,
        )
        while process.poll() is None:
            if time.monotonic_ns() >= operational:
                raise TimeoutError("diagnostic operational deadline")
            sample()
            time.sleep(min(0.1, max(0, (operational - time.monotonic_ns()) / NS)))
        if process.returncode or not read(directory / "worker-result.json")["success"]:
            raise RuntimeError("worker failed; see retained worker-result/log")
    except BaseException as exc:
        error = f"{type(exc).__name__}: {exc}"
    finally:
        cleanup = registry.cleanup(min(time.monotonic_ns() + 20 * NS, hard))
        output.close()
        resources.close()
        result = dict(
            start_ns=start,
            end_ns=time.monotonic_ns(),
            error=error,
            cleanup=cleanup,
            worker_exit_code=None if process is None else process.returncode,
        )
        save(directory / "result.json", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["run", "worker"])
    parser.add_argument("directory", type=Path)
    options = parser.parse_args()
    if options.action == "worker":
        asyncio.run(worker(options.directory.resolve()))
    else:
        run(options.directory.resolve())
