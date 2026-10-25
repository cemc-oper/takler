"""Bounded MIX100K diagnostic batches, no product optimization.

Run freeze, no-service tests, then run-one smoke/D/B/C/A from the frozen copy.
No old runner CLI is invoked. All paths are batch-local; raw failures survive.
"""

from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import gc
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import traceback

NS = 1_000_000_000
GIB = 1024**3


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def save(path, value):
    temporary = Path(str(path) + ".own.tmp")
    temporary.write_bytes(canonical(value) + b"\n")
    temporary.replace(path)


def read(path):
    return json.loads(Path(path).read_bytes())


class Ledger:
    """Synchronous, bounded single-record writes; no unbounded event queue."""

    def __init__(self, path):
        self.file = Path(path).open("xb", buffering=0)
        self.lock = threading.Lock()
        self.sequence = 0

    def __call__(self, kind, **values):
        with self.lock:
            self.sequence += 1
            row = dict(
                sequence=self.sequence,
                pid=os.getpid(),
                at_ns=time.monotonic_ns(),
                kind=kind,
                **values,
            )
            raw = canonical(row) + b"\n"
            if len(raw) > 16384:
                raise ValueError("ledger record exceeds 16 KiB")
            if self.file.write(raw) != len(raw):
                raise OSError("short evidence write")

    def close(self):
        self.file.close()


def remaining(deadline_ns, maximum=10):
    left = (deadline_ns - time.monotonic_ns()) / NS
    if left <= 0:
        raise TimeoutError("absolute deadline exhausted before stub")
    return min(maximum, left)


def wait_until(target_ns, deadline_ns):
    while True:
        now = time.monotonic_ns()
        if now >= target_ns:
            return
        if now >= deadline_ns:
            raise TimeoutError("absolute deadline exhausted while waiting")
        # Reaching a scheduled target between two clock reads is normal, not
        # deadline exhaustion. RPC admission still checks its own live budget.
        time.sleep(min(0.02, (target_ns - now) / NS, (deadline_ns - now) / NS))


def pool_paths(bunch):
    """Round-robin evenly across sorted flows, then retain stable path order."""
    flows = sorted(bunch.flows.values(), key=lambda f: f.node_path)
    selected = [
        sorted(flows[i % len(flows)].children, key=lambda t: t.node_path)[
            i // len(flows)
        ].node_path
        for i in range(250)
    ]
    return sorted(selected)


def fixture(size, meter_max=100):
    from query_a_matrix_100k import make_tree

    bunch, _ = make_tree(size, False)
    paths = pool_paths(bunch)
    for path in paths:
        bunch.find_node(path).add_meter("progress", 0, meter_max)
    return bunch, paths


def pool_state(bunch, paths):
    result = []
    for path in paths:
        task = bunch.find_node(path)
        result.append(
            dict(
                path=path,
                status=task.state.node_status.name,
                attempt=task.attempt_id,
                task_id=task.task_id,
                try_no=task.try_no,
                generation=task.definition_generation,
                meter=task.find_meter("progress").value,
                unknown=task.submission_unknown_reason,
            )
        )
        if (
            task.state.node_status.name != "active"
            or not task.attempt_id
            or not task.job_password
            or task.try_no != 1
            or task.submission_unknown_reason
        ):
            raise ValueError("pool active identity mismatch")
    selected = set(paths)
    for flow in bunch.flows.values():
        if flow.state.node_status != flow.computed_status(immediate=True):
            raise ValueError("parent aggregation mismatch")
        for task in flow.children:
            if task.in_limit_manager.in_limit_list or task.limits:
                raise ValueError("standard fixture unexpectedly has limits")
            if (
                task.node_path not in selected
                and task.state.node_status.name != "complete"
            ):
                raise ValueError("nonpool task changed")
    return result


async def prepare(scheduler, paths, log, deadline_ns):
    from takler.protocol.commands import Command, REQUEST_TYPE_BY_COMMAND
    from takler.server.handlers import CommandHandlers
    from takler.server.auth import (
        CallCredentials,
        set_call_credentials,
        reset_call_credentials,
    )

    handler = CommandHandlers(scheduler)
    credentials = {}
    for path in paths:
        for command, payload in [
            (Command.RUN, dict(node_paths=[path], force=False)),
            (Command.INIT, None),
        ]:
            remaining(deadline_ns)
            task = scheduler.bunch.find_node(path)
            if payload is None:
                payload = dict(
                    node_path=path,
                    task_id="mix100k-" + task.attempt_id,
                    attempt_id=task.attempt_id,
                )
            token = set_call_credentials(
                CallCredentials(job_password=task.job_password)
            )
            try:
                reply = await handler.handle(
                    command, REQUEST_TYPE_BY_COMMAND[command](**payload)
                )
            finally:
                reset_call_credentials(token)
            log("setup_handler", operation=command.value, path=path, flag=reply.flag)
            if reply.flag or any(
                r.flag or r.effect != "applied" for r in getattr(reply, "results", [])
            ):
                raise ValueError("normal setup handler failed")
        credentials[path] = dict(attempt=task.attempt_id, password=task.job_password)
    pool_state(scheduler.bunch, paths)
    return credentials


def root_link(version, paths, scope="/"):
    """Boundary-only projection, linked to the exact streamed fixed root."""
    from takler.server.full_snapshot import _full_node
    from takler.protocol.query_v1 import FULL_DEFAULT_GROUPS
    from takler.protocol.full_snapshot_codec import pack_node, unpack_node

    digest = hashlib.sha256()
    linked_paths = version.index if scope == "/" else [scope]
    for path in linked_paths:
        row = unpack_node(
            pack_node(
                _full_node(version, path, FULL_DEFAULT_GROUPS), FULL_DEFAULT_GROUPS
            ),
            FULL_DEFAULT_GROUPS,
        )
        digest.update(canonical(row) + b"\n")
    return dict(
        state_version=version.state_version,
        count=len(linked_paths),
        semantic_sha256=digest.hexdigest(),
    )


def verify_stream_link(actual, expected):
    if any(
        actual[key] != expected[key]
        for key in ("state_version", "count", "semantic_sha256")
    ):
        raise ValueError("complete stream/fixed root version/count/digest mismatch")
    if not actual["cancelled"]:
        raise ValueError("full stream cancellation not acknowledged")


def checkpoint_equivalence(path, version):
    """Independent main/backup restoration against their own retained roots."""
    from takler.serialization.checkpoint import restore_checkpoint
    from takler.serialization.version import VersionStore

    document = read(path)
    restored = restore_checkpoint(document)
    if document["source_address"] != json.loads(version.source_address):
        raise ValueError("checkpoint address mismatch")
    restored.server_config = type(restored.server_config).from_dict(
        json.loads(version.source_address)
    )
    status = json.loads(version.node("/").public)["server_status"]
    actual = VersionStore(restored, status_provider=lambda: status).pin()
    if set(actual.index) != set(version.index) or actual.secrets != version.secrets:
        raise ValueError("checkpoint paths/secrets mismatch")
    for node in version.iter_nodes():
        other = actual.node(node.path)
        if (node.definition, node.runtime, node.public) != (
            other.definition,
            other.runtime,
            other.public,
        ):
            raise ValueError("checkpoint restored fixed root mismatch: " + node.path)
    return dict(
        file=path.name,
        saved_at=document["saved_at"],
        state_version=version.state_version,
        nodes=len(actual.index),
        sha256=hashlib.file_digest(path.open("rb"), "sha256").hexdigest(),
        equivalent=True,
    )


async def service(directory):
    from takler.server import TaklerServer
    from takler.server.connect_config import (
        Address,
        ConnectConfig,
        SecuritySettings,
        Server,
    )
    from r3_14_observation import Emitter, ServiceObservation
    from r3_14_perf08a_hooks import install_checkpoint_boundaries
    from r3_14_perf04_verify import CheckpointAudit

    config = read(directory / "config.json")
    log = Ledger(directory / "server-ledger.jsonl")
    fresh = dict(enabled=gc.isenabled(), threshold=gc.get_threshold())
    connect = ConnectConfig(
        server=Server(
            address=Address(
                hostname="127.0.0.1", ip="127.0.0.1", port=str(config["port"])
            )
        ),
        security=SecuritySettings(
            auth_mode="enabled",
            operator_secret_file=str(directory / "secret"),
            operator_whitelist_file=str(directory / "whitelist"),
        ),
    )
    server = TaklerServer(
        host="127.0.0.1",
        port=config["port"],
        connect_config=connect,
        checkpoint_file=directory / "state.check",
        checkpoint_interval=config.get("checkpoint_interval_s", 10),
    )
    original_failure_category = server.checkpoint_manager._failure_category

    def failure_detail(exc):
        log("checkpoint_failure_detail", error=f"{type(exc).__name__}: {exc}"[:2000])
        return original_failure_category(exc)

    server.checkpoint_manager._failure_category = failure_detail
    server.scheduler.interval_main_loop = config.get("scheduler_interval_s", 0.05)
    bunch, paths = fixture(config["tasks"], config.get("meter_max", 100))
    for flow in bunch.flows.values():
        server.bunch.add_flow(flow)
    await server.start()
    if not config["checkpoint"]:
        periodic = server.checkpoint_manager._snapshot_task
        periodic.cancel()
        await asyncio.gather(periodic, return_exceptions=True)
        server.checkpoint_manager._snapshot_task = None
    credentials = await prepare(
        server.scheduler, paths, log, config["setup_deadline_ns"]
    )
    save(directory / "credentials.json", credentials)
    os.chmod(directory / "credentials.json", 0o600)
    emitter = Emitter(config["socket"], capacity=8192)
    observation = ServiceObservation(server, emitter).install()
    install_checkpoint_boundaries(observation)
    audit = CheckpointAudit(
        server.checkpoint_manager, directory, lambda row: log(**row)
    ).install()
    run_task = asyncio.create_task(server.run())
    ready = dict(
        fresh_gc=fresh,
        ready_gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold()),
        source=__import__("takler").__file__,
        paths=paths,
        tasks=config["tasks"],
        flows=len(bunch.flows),
        nodes=config["nodes"],
        loop=type(asyncio.get_running_loop()).__qualname__,
    )
    save(directory / "server-ready.json", ready)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    seen = set()
    full_service = server.scheduler.query_service.full_snapshots
    # Observe the common page state machine, including an encoded wire entry.
    page_method = "_page" if hasattr(full_service, "_page") else "page"
    original_page = getattr(full_service, page_method)
    boundary_root = None
    boundary_phase = None
    boundary_scope = "/"

    async def linked_page(request):
        nonlocal boundary_root, boundary_phase, boundary_scope
        wire_reply = reply = await original_page(request)
        if isinstance(reply, bytes):
            # Decode only pre/post boundary proofs, never steady full pages.
            if boundary_root is None and (
                request.full_snapshot_id is not None
                or all(
                    phase in seen or not (directory / f"{phase}-request.json").exists()
                    for phase in ("pre", "post")
                )
            ):
                return wire_reply
            from takler.protocol.query_v1 import decode_query_document

            reply = decode_query_document(reply, expected="full_snapshot_page")
        if request.full_snapshot_id is None and hasattr(reply, "full_snapshot_id"):
            for phase in ("pre", "post"):
                if phase not in seen and (directory / f"{phase}-request.json").exists():
                    boundary_root = full_service._leases[reply.full_snapshot_id].version
                    boundary_phase = phase
                    boundary_scope = request.scope_path
                    seen.add(phase)
                    break
        if getattr(reply, "complete", False) and boundary_root is not None:
            phase, version = boundary_phase, boundary_root
            boundary_root = boundary_phase = None
            deadline = read(directory / f"{phase}-request.json")["deadline_ns"]
            remaining(deadline)
            link = await asyncio.to_thread(root_link, version, paths, boundary_scope)
            save(directory / f"{phase}-root.json", link)
            states = pool_state(server.bunch, paths)
            with (directory / f"{phase}-pool.jsonl").open("x") as target:
                for row in states:
                    target.write(json.dumps(row) + "\n")
        return wire_reply

    setattr(full_service, page_method, linked_page)
    try:
        while not stop.is_set():
            audit.check()
            if server.checkpoint_manager.checkpoint_failure_reason:
                save(
                    directory / "integrity-failure.json",
                    dict(
                        role="checkpoint",
                        error=server.checkpoint_manager.checkpoint_failure_reason,
                    ),
                )
                raise RuntimeError("checkpoint failure; diagnostic stopped")
            if (directory / "stop-server.json").exists():
                break
            await asyncio.sleep(0.02)
    finally:
        await server.stop()
        await run_task
        final_deadline = (
            read(directory / "stop-server.json")["deadline_ns"]
            if (directory / "stop-server.json").exists()
            else config["operational_deadline_ns"]
        )
        audit.close(final_deadline)
        recovered = []
        for path, version in [
            (server.checkpoint_manager.checkpoint_file, audit.main),
            (server.checkpoint_manager.backup_file, audit.backup),
        ]:
            remaining(final_deadline)
            if version is None:
                if (
                    path.exists()
                    or path == server.checkpoint_manager.checkpoint_file
                    or config["checkpoint"]
                ):
                    raise ValueError("checkpoint fixed-root link missing")
                recovered.append(
                    dict(
                        file=path.name,
                        applicable=False,
                        reason="periodic disabled; no backup formed",
                    )
                )
            else:
                recovered.append(
                    await asyncio.to_thread(checkpoint_equivalence, path, version)
                )
        await observation.close()
        final = dict(
            gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold()),
            observation=emitter.close(),
            recovered=recovered,
            leases=len(server.scheduler.query_service.full_snapshots._leases),
            checkpoint_background=len(server.checkpoint_manager._background_writes),
            exit_save_ok=server.shutdown_checkpoint_ok,
            released=audit.release(),
        )
        save(directory / "server-final.json", final)
        log.close()


class SummaryEvidenceError(ValueError):
    """A required summary artifact could not be retained or verified."""


class SummaryArchive:
    """One bounded wire page at a time; incomplete reads remain .partial files."""

    def __init__(self, path, deadline_ns):
        self.path, self.deadline_ns = path, deadline_ns
        self.partial = Path(str(path) + ".partial")
        if path.exists():
            raise SummaryEvidenceError("summary artifact already exists")
        remaining(deadline_ns)
        try:
            self.file = self.partial.open("xb", buffering=0)
        except OSError as exc:
            raise SummaryEvidenceError(f"summary artifact open failed: {exc}") from exc
        self.digest = hashlib.sha256()
        self.pages = self.bytes = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        try:
            self.file.close()
        except OSError as exc:
            raise SummaryEvidenceError(f"summary artifact close failed: {exc}") from exc

    def append(self, raw):
        from takler.protocol.query_v1 import MAX_PAGE_BYTES

        remaining(self.deadline_ns)
        if not 0 < len(raw) <= MAX_PAGE_BYTES:
            raise SummaryEvidenceError("summary page exceeds archive bound")
        record = struct.pack("<I", len(raw)) + raw
        try:
            written = self.file.write(record)
        except OSError as exc:
            raise SummaryEvidenceError(f"summary artifact write failed: {exc}") from exc
        if written != len(record):
            raise SummaryEvidenceError("short summary artifact write")
        self.digest.update(record)
        self.pages += 1
        self.bytes += len(record)
        remaining(self.deadline_ns)

    def finish(self, view, expected_nodes):
        if (
            not self.pages
            or len(view.ordered_paths) != expected_nodes
            or view.revision is None
        ):
            raise SummaryEvidenceError("summary node count/fixed revision missing")
        remaining(self.deadline_ns)
        try:
            self.file.close()
        except OSError as exc:
            raise SummaryEvidenceError(f"summary artifact close failed: {exc}") from exc
        remaining(self.deadline_ns)
        try:
            self.partial.rename(self.path)
        except OSError as exc:
            raise SummaryEvidenceError(
                f"summary artifact publication failed: {exc}"
            ) from exc
        return dict(
            format="takler-summary-pages-v1",
            file=self.path.name,
            sha256=self.digest.hexdigest(),
            archive_bytes=self.bytes,
            pages=self.pages,
            nodes=expected_nodes,
            version=view.revision,
            session_id=view.session_id,
            query_id=view.query_id,
            snapshot_id=view.snapshot_id,
            as_of=view.as_of,
            expires_at=view.expires_at,
            root_name=view.root_name,
            scope_path=view.scope_path,
            depth=view.depth,
        )


class QueryWire:
    """Public client assembly, one actual stub attempt with a shared deadline."""

    def __init__(self, directory, role, log):
        from takler.client.service_client import TaklerServiceClient

        self.directory, self.role, self.log = directory, role, log
        self.client = TaklerServiceClient(
            "127.0.0.1",
            read(directory / "config.json")["port"],
            single_timeout=10,
            retry_window=0,
            secret_file=str(directory / "secret"),
            transport_name="grpc",
        )
        self.client.start()
        self.deadline_ns = 0
        self.request_id = "setup"
        self.cancelled = False
        self.bytes = 0
        self.rpcs = 0
        self.summary_archive = None
        self.summary_serial = 0
        self.last_summary_digest = None
        self.client.transport.query = self.query

    def query(self, document):
        from takler.server.protocol.query_adapter import query_to_pb2, query_from_pb2
        from takler.client.retry import CommandKind

        if document.kind == "page_request" and self.summary_archive is None:
            raise SummaryEvidenceError("summary request bypassed required archive")
        methods = dict(
            capabilities_request="GetCapabilities",
            page_request="ReadPage",
            detail_request="ReadDetail",
            chunk_request="ReadDetailChunk",
            since_request="ReadSince",
            full_snapshot_request="ReadFullSnapshot",
            full_snapshot_chunk_request="ReadFullSnapshotChunk",
            cancel_full_snapshot_request="CancelFullSnapshot",
        )
        transport = self.client.transport
        payload = query_to_pb2(document)
        metadata = transport._build_metadata(CommandKind.QUERY)
        rpc = getattr(transport.query_stub, methods[document.kind])
        timeout = remaining(self.deadline_ns)
        started = time.monotonic_ns()
        self.log(
            "rpc_sent",
            request_id=self.request_id,
            channel=self.role,
            operation=document.kind,
            started_ns=started,
            deadline_ns=self.deadline_ns,
        )
        self.rpcs += 1
        try:
            response = rpc(payload, timeout=timeout, metadata=metadata)
            self.bytes += len(response.json)
            reply = query_from_pb2(response)
            if document.kind == "cancel_full_snapshot_request":
                self.cancelled = getattr(reply, "cancelled", False) is True
            self.log(
                "rpc_return",
                request_id=self.request_id,
                channel=self.role,
                operation=document.kind,
                started_ns=started,
                bytes=len(response.json),
                version=getattr(reply, "state_version", None),
                snapshot_id=getattr(reply, "snapshot_id", None),
                code=getattr(reply, "code", None),
            )
        except Exception as exc:
            self.log(
                "rpc_error",
                request_id=self.request_id,
                channel=self.role,
                operation=document.kind,
                started_ns=started,
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        if document.kind == "page_request":
            self.summary_archive.append(bytes(response.json))
        return reply

    def begin(self, request_id, deadline_ns):
        self.request_id, self.deadline_ns = request_id, deadline_ns
        self.cancelled, self.bytes, self.rpcs = False, 0, 0
        self.last_summary_digest = None

    def snapshot(self, reason, expected_nodes):
        self.summary_serial += 1
        filename = f"{self.request_id}-summary-{self.summary_serial:04d}.pages"
        context = dict(request_id=self.request_id, channel=self.role, reason=reason)
        self.last_summary_digest = None
        try:
            with SummaryArchive(self.directory / filename, self.deadline_ns) as archive:
                self.log("summary_archive_started", file=filename, **context)
                self.summary_archive = archive
                view = self.client.run_query_snapshot(scope_path="/")
                evidence = archive.finish(view, expected_nodes)
            self.log("summary_digest", evidence=evidence, **context)
            self.last_summary_digest = evidence
            return view
        except TimeoutError:
            raise  # Transport/absolute-budget failure leaves a partial, not a false integrity alarm.
        except (OSError, SummaryEvidenceError) as exc:
            save(
                self.directory / "integrity-failure.json",
                dict(role=self.role, error=f"summary evidence: {exc}"),
            )
            raise SummaryEvidenceError(str(exc)) from exc
        finally:
            self.summary_archive = None

    def full(self, output, nodes, scope="/"):
        from takler.client.full_snapshot import FullSnapshotComplete

        digest, count, completion = hashlib.sha256(), 0, None
        with output.open("xb", buffering=65536) as target:
            for item in self.client.run_iter_full_snapshot(scope_path=scope):
                remaining(self.deadline_ns)
                if isinstance(item, FullSnapshotComplete):
                    completion = item
                else:
                    raw = canonical(item) + b"\n"
                    target.write(raw)
                    digest.update(raw)
                    count += 1
        if completion is None or count != nodes or completion.total_nodes != count:
            raise ValueError("complete stream node count mismatch")
        if not self.cancelled:
            raise ValueError("cancel/release unconfirmed")
        return dict(
            state_version=completion.state_version,
            count=count,
            semantic_sha256=digest.hexdigest(),
            cancelled=self.cancelled,
            completion=asdict(completion),
            bytes=self.bytes,
            rpcs=self.rpcs,
        )

    def close(self):
        self.client.close_channel()


def start_window(directory, role):
    save(
        directory / f"{role}-ready.json",
        dict(
            pid=os.getpid(),
            gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold()),
        ),
    )
    config = read(directory / "config.json")
    while not (directory / "window.json").exists():
        remaining(config["setup_deadline_ns"] + 10 * NS)
        time.sleep(0.01)
    window = read(directory / "window.json")
    wait_until(window["start_ns"], config["operational_deadline_ns"])
    return config, window


def slot_reason(now_ns, target_ns, period_ns, busy, inflight):
    if now_ns >= target_ns + period_ns:
        return "expired"
    if busy:
        return "task_busy"
    if inflight >= 64:
        return "write_cap"
    return None


def meter_call(
    rpc,
    payload,
    metadata,
    target_ns,
    slot_end_ns,
    deadline_ns,
    log,
    slot,
    clock=time.monotonic_ns,
):
    """All admission/deadline checks precede the actual stub boundary."""
    if clock() >= slot_end_ns:
        log("not_sent", slot=slot, target_ns=target_ns, reason="expired_at_stub")
        return False
    timeout = remaining(deadline_ns)
    sent = clock()
    log("sent", slot=slot, target_ns=target_ns, sent_ns=sent, deadline_ns=deadline_ns)
    response = rpc(payload, timeout=timeout, metadata=metadata)
    log(
        "returned",
        slot=slot,
        target_ns=target_ns,
        sent_ns=sent,
        returned_ns=clock(),
        flag=response.flag,
    )
    return response.flag == 0


def writer(directory):
    import grpc
    from takler.protocol.commands import Command
    from takler.server.protocol.adapter import request_to_pb2
    from takler.server.protocol import takler_pb2_grpc

    config = read(directory / "config.json")
    credentials = read(directory / "credentials.json")
    paths = sorted(credentials)
    log = Ledger(directory / "writer-requests.jsonl")
    channel = grpc.insecure_channel(f"127.0.0.1:{config['port']}")
    stub = takler_pb2_grpc.TaklerServerStub(channel)
    _, window = start_window(directory, "writer")
    busy, lock = set(), threading.Lock()
    state = dict(
        success=0,
        failure=0,
        unknown=0,
        last_success_ns=window["start_ns"],
        last_completion_ns=window["start_ns"],
    )
    period = NS // config["rate"]
    stop = threading.Event()

    def call(slot, target, path):
        attempted = False
        try:
            identity = credentials[path]
            payload = request_to_pb2(
                Command.METER,
                dict(
                    node_path=path,
                    source_task_path=path,
                    attempt_id=identity["attempt"],
                    meter_name="progress",
                    meter_value=str(slot // 250 + 1),
                ),
            )
            metadata = [("takler-pass", identity["password"])]

            def actual(*args, **kwargs):
                nonlocal attempted
                attempted = True
                return stub.RunCommandMeter(*args, **kwargs)

            success = meter_call(
                actual,
                payload,
                metadata,
                target,
                target + period,
                min(time.monotonic_ns() + 10 * NS, window["drain_deadline_ns"]),
                log,
                slot,
            )
            if attempted:
                with lock:
                    state["success" if success else "failure"] += 1
                    state["last_completion_ns"] = time.monotonic_ns()
                    if success:
                        state["last_success_ns"] = time.monotonic_ns()
        except Exception as exc:
            log(
                "write_unknown" if attempted else "not_sent",
                slot=slot,
                target_ns=target,
                reason=f"{type(exc).__name__}: {exc}",
            )
            with lock:
                state["unknown"] += int(attempted)
            stop.set()
            save(
                directory / "writer-failure.json",
                dict(
                    reason="write response unknown"
                    if attempted
                    else "writer tooling failure",
                    attempted=attempted,
                    slot=slot,
                ),
            )
        finally:
            with lock:
                busy.remove(path)

    pool = ThreadPoolExecutor(max_workers=64)
    for slot in range(config["slots"]):
        target = window["start_ns"] + slot * period
        wait_until(target, window["drain_deadline_ns"])
        path = paths[slot % 250]
        with lock:
            reason = (
                "stopped"
                if stop.is_set() or (directory / "withdraw.json").exists()
                else slot_reason(
                    time.monotonic_ns(), target, period, path in busy, len(busy)
                )
            )
            if reason is None:
                busy.add(path)
        log("planned", slot=slot, target_ns=target, path=path, value=slot // 250 + 1)
        if reason:
            log("not_sent", slot=slot, target_ns=target, reason=reason)
        else:
            pool.submit(call, slot, target, path)
        if slot % max(1, config["rate"] // 10) == 0:
            with lock:
                save(
                    directory / "writer-health.json",
                    dict(at_ns=time.monotonic_ns(), **state),
                )
        if stop.is_set() or (directory / "withdraw.json").exists():
            for rest in range(slot + 1, config["slots"]):
                log(
                    "planned",
                    slot=rest,
                    target_ns=window["start_ns"] + rest * period,
                    path=paths[rest % 250],
                    value=rest // 250 + 1,
                )
                log(
                    "not_sent",
                    slot=rest,
                    target_ns=window["start_ns"] + rest * period,
                    reason="withdrawn",
                )
            break
    pool.shutdown(wait=True)
    channel.close()
    save(
        directory / "writer-final.json",
        dict(
            **state,
            released=True,
            gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold()),
        ),
    )
    log.close()


def queries(directory):
    from takler.exceptions import QueryResponseError

    config = read(directory / "config.json")
    log = Ledger(directory / "query-requests.jsonl")
    wires = {
        name: QueryWire(directory, name, log) for name in ("summary", "since", "detail")
    }
    _, window = start_window(directory, "queries")
    paths = sorted(read(directory / "credentials.json"))

    def work(name):
        wire = wires[name]
        view = None
        period = (10 if name == "summary" else 1) * NS
        for slot in range(config["duration"] // (10 if name == "summary" else 1)):
            target = window["start_ns"] + slot * period
            wait_until(target, window["drain_deadline_ns"])
            rid = f"{name}-{slot}"
            log("planned", request_id=rid, channel=name, target_ns=target)
            if (directory / "withdraw.json").exists():
                log(
                    "not_sent",
                    request_id=rid,
                    channel=name,
                    target_ns=target,
                    reason="withdrawn",
                )
                continue
            if time.monotonic_ns() >= target + period:
                log(
                    "not_sent",
                    request_id=rid,
                    channel=name,
                    target_ns=target,
                    reason="busy_or_expired",
                )
                continue
            started = time.monotonic_ns()
            wire.begin(
                rid,
                min(
                    started + (60 if name == "summary" else 10) * NS,
                    window["drain_deadline_ns"],
                ),
            )
            try:
                if name == "summary":
                    view = wire.snapshot("scheduled", config["nodes"])
                else:
                    if view is None:
                        log("baseline_subrequest", request_id=rid, channel=name)
                        view = wire.snapshot("baseline", config["nodes"])
                    if name == "since":
                        try:
                            view = wire.client.run_query_since(view)
                        except QueryResponseError as exc:
                            if exc.code != "reset_required":
                                raise
                            log(
                                "protocol_reset",
                                request_id=rid,
                                channel=name,
                                code=exc.code,
                            )
                            view = wire.snapshot("reset_required", config["nodes"])
                        if view.stale_scopes:
                            log(
                                "protocol_reset",
                                request_id=rid,
                                channel=name,
                                code="stale_scopes",
                            )
                            view = wire.snapshot("stale_scopes", config["nodes"])
                    else:
                        reply = wire.client.run_query_detail(
                            view, paths[slot % 250], ["runtime"]
                        )
                        if reply.path != paths[slot % 250]:
                            raise ValueError("detail identity mismatch")
                if len(view.ordered_paths) != config["nodes"]:
                    raise ValueError("summary/since complete node count mismatch")
                log(
                    "completed",
                    request_id=rid,
                    channel=name,
                    target_ns=target,
                    started_ns=started,
                    ended_ns=time.monotonic_ns(),
                    nodes=len(view.ordered_paths),
                    snapshot_id=view.snapshot_id,
                    version=view.revision,
                    bytes=wire.bytes,
                    rpcs=wire.rpcs,
                    **(
                        dict(summary_digest=wire.last_summary_digest)
                        if name == "summary"
                        else {}
                    ),
                )
            except Exception as exc:
                log(
                    "failed",
                    request_id=rid,
                    channel=name,
                    target_ns=target,
                    started_ns=started,
                    ended_ns=time.monotonic_ns(),
                    error=f"{type(exc).__name__}: {exc}",
                )
                if isinstance(exc, SummaryEvidenceError) or type(exc).__name__ in {
                    "ValueError",
                    "ServerResponseError",
                }:
                    save(
                        directory / "integrity-failure.json",
                        dict(role=name, error=str(exc)),
                    )
        wire.close()

    threads = [threading.Thread(target=work, args=(name,)) for name in wires]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(remaining(window["drain_deadline_ns"], 60 + config["duration"]))
    if any(t.is_alive() for t in threads):
        raise TimeoutError("query thread release unknown")
    save(
        directory / "queries-final.json",
        dict(
            released=True, gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold())
        ),
    )
    log.close()


def full_reader(directory, role):
    config = read(directory / "config.json")
    log = Ledger(directory / f"{role}-requests.jsonl")
    wire = QueryWire(directory, role, log)
    _, window = start_window(directory, role)
    slot = 0
    while (
        time.monotonic_ns() < window["end_ns"]
        and not (directory / "withdraw.json").exists()
    ):
        if not config["full"]:
            time.sleep(0.02)
            continue
        rid = f"{role}-{slot}"
        started = time.monotonic_ns()
        log("planned", channel=role, request_id=rid, target_ns=started)
        wire.begin(rid, min(started + 60 * NS, window["drain_deadline_ns"]))
        try:
            result = wire.full(directory / f"{rid}-view.jsonl", config["nodes"])
            log(
                "completed",
                channel=role,
                request_id=rid,
                target_ns=started,
                started_ns=started,
                ended_ns=time.monotonic_ns(),
                **result,
            )
        except Exception as exc:
            log(
                "failed",
                channel=role,
                request_id=rid,
                target_ns=started,
                started_ns=started,
                ended_ns=time.monotonic_ns(),
                error=f"{type(exc).__name__}: {exc}",
            )
            if type(exc).__name__ in {"ValueError", "ServerResponseError"}:
                save(
                    directory / "integrity-failure.json",
                    dict(role=role, error=str(exc)),
                )
        slot += 1
    wire.close()
    save(
        directory / f"{role}-final.json",
        dict(
            released=True,
            requests=slot,
            gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold()),
        ),
    )
    log.close()


class ReceiptError(ValueError):
    """A classified receipt failure, distinct from a still-open sequence gap."""

    def __init__(self, category, message):
        super().__init__(message)
        self.category = category


class ObservationReceipt:
    """One registered service producer; <=8192 pending sequences and one frame."""

    def __init__(self, directory, log):
        from r3_14_perf04_ledger import SequenceWindow
        from r3_14_observation import RECORD, MAX_PACKET_RECORDS

        self.directory, self.log = directory, log
        self.window = SequenceWindow(8192)
        self.max_packet_bytes = RECORD.size * MAX_PACKET_RECORDS
        self.pid = None
        self.highest = self.reordered = 0
        self.last_packet, self.last_flags = b"", 0

    def producer(self):
        if self.pid is None:
            try:
                entries = read(self.directory / "registry.json")["entries"]
                servers = [e["pid"] for e in entries if e["role"] == "server"]
            except (OSError, ValueError, KeyError, TypeError) as exc:
                raise ReceiptError(
                    "producer_identity", "producer registry unavailable"
                ) from exc
            if len(servers) != 1:
                raise ReceiptError(
                    "producer_identity", "one registered server required"
                )
            self.pid = servers[0]
        return self.pid

    def state(self):
        return dict(
            pid=self.pid,
            capacity=self.window.capacity,
            contiguous=self.window.next - 1,
            received=self.window.received,
            pending_count=len(self.window.waiting),
            first_unreceived=self.window.next,
            highest_received=self.highest,
            reordered=self.reordered,
        )

    def failure(self, exc, *, frame_role, record_index=None, sequence=None):
        """Keep the first failure only; never call a retained prefix a full frame."""
        packet = self.last_packet
        raw_path = self.directory / "collector-anomaly.bin"
        metadata = dict(
            category=getattr(exc, "category", "receipt_io_or_decode"),
            error=f"{type(exc).__name__}: {exc}",
            at_ns=time.monotonic_ns(),
            frame_role=frame_role,
            record_index=record_index,
            offending_sequence=sequence,
            retained_bytes=len(packet),
            receive_limit_bytes=self.max_packet_bytes + 1,
            datagram_truncated=bool(self.last_flags & socket.MSG_TRUNC),
            frame_present=bool(packet) or frame_role == "offending_frame",
            sha256=hashlib.sha256(packet).hexdigest(),
            receipt=self.state(),
        )
        try:
            with raw_path.open("xb", buffering=0) as target:
                if target.write(packet) != len(packet):
                    raise OSError("short anomaly frame write")
            save(self.directory / "collector-anomaly.json", metadata)
        except OSError as retention:
            raise ReceiptError(
                "anomaly_retention_failed",
                f"could not retain {metadata['category']}: {exc}; {retention}",
            ) from retention

    def accept(self, packet, flags=0):
        from r3_14_observation import decode_packet

        # recvmsg is bounded too; never retain an unbounded caller-supplied frame.
        self.last_packet = packet[: self.max_packet_bytes + 1]
        self.last_flags = flags | (
            socket.MSG_TRUNC if len(packet) > len(self.last_packet) else 0
        )
        index = sequence = None
        try:
            if (
                self.last_flags & socket.MSG_TRUNC
                or len(packet) > self.max_packet_bytes
            ):
                raise ReceiptError("malformed_frame", "oversized/truncated event frame")
            records = iter(decode_packet(packet))
            index = 0
            while True:
                sequence = None
                try:
                    row = next(records)
                except StopIteration:
                    break
                except (KeyError, ValueError) as exc:
                    raise ReceiptError("malformed_frame", str(exc)) from exc
                sequence = row["sequence"]
                if row["pid"] != self.producer():
                    raise ReceiptError(
                        "producer_identity", "unregistered event producer"
                    )
                if sequence == 0:
                    raise ReceiptError("invalid_sequence", "sequence must start at one")
                expected = self.window.next
                try:
                    self.window.accept(sequence)
                except RuntimeError as exc:
                    category = (
                        "duplicate" if "duplicate" in str(exc) else "out_of_window"
                    )
                    raise ReceiptError(category, str(exc)) from exc
                self.highest = max(self.highest, sequence)
                self.reordered += int(sequence != expected)
                self.log("service_event", event=row)
                index += 1
        except Exception as exc:
            self.failure(
                exc, frame_role="offending_frame", record_index=index, sequence=sequence
            )
            raise

    def finish(self):
        """A gap becomes proven missing only against a stopped sender's totals."""
        try:
            pid = self.producer()
            try:
                final = read(self.directory / "server-final.json")["observation"]
            except (OSError, ValueError, KeyError, TypeError) as exc:
                raise ReceiptError(
                    "unknown_final", "producer final unavailable"
                ) from exc
            if not isinstance(final, dict):
                raise ReceiptError("invalid_final", "invalid producer final")
            fields = ("produced", "emitted", "dropped", "abandoned_ring_records")
            if any(type(final.get(k)) is not int or final[k] < 0 for k in fields):
                raise ReceiptError("invalid_final", "invalid producer accounting")
            produced, emitted, dropped = (final[k] for k in fields[:3])
            if (
                produced != emitted + dropped
                or self.highest > produced
                or self.window.received > emitted
            ):
                raise ReceiptError("invalid_final", "producer/receipt counts disagree")
            if (
                final.get("sender_stopped") is not True
                or final["abandoned_ring_records"]
            ):
                raise ReceiptError("unknown_final", "sender tail is not closed")
            if dropped or self.window.received != produced or self.window.waiting:
                raise ReceiptError(
                    "missing",
                    f"missing {produced - self.window.received} records; "
                    f"first unreceived {self.window.next}; producer dropped {dropped}",
                )
            return dict(
                sequences={str(pid): self.window.next - 1},
                receipts={str(pid): self.state()},
                sequence_accounting_verified=True,
                released=True,
            )
        except Exception as exc:
            self.failure(exc, frame_role="last_received_frame")
            raise


def collector(directory):
    import select
    import shutil
    from r3_14_isolated_worker import proc_sample, descendants
    from r3_14_observation import RECORD, MAX_PACKET_RECORDS
    import r3_14_perf08a_hooks  # noqa: F401 - checkpoint event schema

    config = read(directory / "config.json")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
    sock.bind(config["socket"])
    sock.setblocking(False)
    log = Ledger(directory / "events.jsonl")
    resource_log = Ledger(directory / "resources.jsonl")
    receipt = ObservationReceipt(directory, log)
    next_sample = last_health = last_scan = 0
    evidence_bytes = batch_bytes = 0
    over = 0
    swap_start = None
    save(directory / "collector-ready.json", dict(pid=os.getpid()))
    try:
        while not (directory / "stop-collector.json").exists():
            if select.select([sock], [], [], 0.005)[0]:
                for _ in range(8):
                    try:
                        packet, _, flags, _ = sock.recvmsg(
                            RECORD.size * MAX_PACKET_RECORDS + 1
                        )
                    except BlockingIOError:
                        break
                    receipt.accept(packet, flags)
            now = time.monotonic_ns()
            if now >= next_sample:
                registry = read(directory / "registry.json")
                pids = {
                    e["pid"]: e["role"]
                    for e in registry["entries"]
                    if e["ended_ns"] is None
                }
                pids[config["coordinator_pid"]] = "coordinator"
                pids[config["supervisor_pid"]] = "coordinator_supervisor"
                for pid, role in list(pids.items()):
                    if role == "server":
                        for child in descendants(pid):
                            pids[child] = "checkpoint_worker"
                take_pss = now - last_scan >= NS
                processes = [
                    dict(role=role, **proc_sample(pid, take_pss))
                    for pid, role in pids.items()
                ]
                memory = {
                    line.split(":")[0]: int(line.split()[1]) * 1024
                    for line in Path("/proc/meminfo").read_text().splitlines()
                    if len(line.split()) == 3
                }
                swap = memory["SwapTotal"] - memory["SwapFree"]
                if swap_start is None:
                    swap_start = swap
                if take_pss:
                    evidence_bytes = sum(
                        p.stat().st_size for p in directory.rglob("*") if p.is_file()
                    )
                    batch_bytes = sum(
                        p.stat().st_size
                        for p in directory.parent.rglob("*")
                        if p.is_file()
                    )
                    last_scan = now
                rss = sum(p.get("rss_bytes", 0) for p in processes)
                over = over + 1 if rss > 8 * GIB else 0
                resource_log(
                    "sample",
                    processes=processes,
                    rss_sum=rss,
                    mem_available=memory["MemAvailable"],
                    swap_used=swap,
                    disk_free=shutil.disk_usage(directory).free,
                    evidence_bytes=evidence_bytes,
                    batch_evidence_bytes=batch_bytes,
                )
                reasons = []
                if memory["MemAvailable"] < 4 * GIB:
                    reasons.append("MemAvailable below 4 GiB")
                if swap - swap_start > GIB:
                    reasons.append("swap growth above 1 GiB")
                if shutil.disk_usage(directory).free < 20 * GIB:
                    reasons.append("disk free below 20 GiB")
                if over >= 2:
                    reasons.append("owned RSS above 8 GiB at two points")
                if any(
                    p.get("rss_bytes", 0) > 512 * 1024**2
                    for p in processes
                    if p["role"]
                    in {"collector", "coordinator", "coordinator_supervisor"}
                ):
                    reasons.append("collector/coordinator RSS above 512 MiB")
                limits = config.get("stop_limits", {})
                if evidence_bytes > limits.get(
                    "run_evidence_max", 4 * GIB
                ) or batch_bytes > limits.get("batch_evidence_max", 12 * GIB):
                    reasons.append("evidence capacity exceeded")
                if reasons:
                    raise RuntimeError("; ".join(reasons))
                next_sample = now + 100_000_000
            if now - last_health >= 100_000_000:
                save(
                    directory / "collector-health.json",
                    dict(
                        at_ns=now,
                        pid=os.getpid(),
                        sequences={}
                        if receipt.pid is None
                        else {str(receipt.pid): receipt.window.next - 1},
                        receipt=receipt.state(),
                    ),
                )
                last_health = now
        # Service emitter is closed before this marker. Drain every queued packet.
        while select.select([sock], [], [], 0)[0]:
            packet, _, flags, _ = sock.recvmsg(RECORD.size * MAX_PACKET_RECORDS + 1)
            receipt.accept(packet, flags)
        save(directory / "collector-final.json", receipt.finish())
    except BaseException as exc:
        save(
            directory / "collector-failure.json",
            dict(
                error=f"{type(exc).__name__}: {exc}",
                category=getattr(exc, "category", "collector_failure"),
                at_ns=time.monotonic_ns(),
            ),
        )
        raise
    finally:
        log.close()
        resource_log.close()
        sock.close()


def finish_collector(directory, process, server_pid, produced, deadline_ns):
    """Shared coordinator exit gate: drain first, then require verified receipts."""
    remaining(deadline_ns)
    save(directory / "stop-collector.json", dict(at_ns=time.monotonic_ns()))
    while process.poll() is None:
        remaining(deadline_ns)
        time.sleep(0.02)
    if (directory / "collector-failure.json").exists():
        failure = read(directory / "collector-failure.json")
        raise ReceiptError(
            failure.get("category", "collector_failure"),
            "collector final failed: " + failure["error"],
        )
    if process.returncode != 0:
        raise ReceiptError("unknown_final", "collector exited without a trusted final")
    try:
        observed = read(directory / "collector-final.json")
    except (OSError, ValueError) as exc:
        raise ReceiptError("unknown_final", "collector final unavailable") from exc
    if (
        observed.get("sequence_accounting_verified") is not True
        or observed.get("sequences", {}).get(str(server_pid)) != produced
    ):
        raise ReceiptError("invalid_final", "collector final count/proof mismatch")
    return observed


def freeze(batch, repo, batch_id="MIX100K-01", *, product=False):
    from r3_14_isolated_probe import diagnostic_copy

    if (batch / "config.json").exists():
        raise ValueError("batch already frozen")
    config = dict(
        batch=batch_id,
        seed=20261018,
        tasks=100000,
        flows=100,
        nodes=100101,
        file_references=0,
        pool=250,
        duration=180,
        rate=100,
        slots=18000,
        period_ns=10000000,
        writer_inflight=64,
        per_task_inflight=1,
        actual_rpc_cap=256,
        actual_rpc_structural_max=69,  # 64 writer + 3 ordinary + 2 full; boundary reads outside measurement
        summary_period_s=10,
        since_period_s=1,
        detail_period_s=1,
        scope="/",
        full_readers=2,
        checkpoint_interval_s=10,
        rpc_timeout_s=10,
        ordinary_deadline_s=10,
        full_summary_deadline_s=60,
        retry_window=0,
        gc="default diagnostic copy; exact gc.patch",
        order=["smoke", "D", "B", "C", "A"],
        smoke=dict(tasks=1000, nodes=1002, duration=60, rate=10, slots=600),
        caps=dict(
            run_s=480,
            operational_s=470,
            setup_s=180,
            baseline_s=10,
            withdrawal_s=60,
            post_exit_s=40,
            cleanup_s=10,
        ),
        stop_limits=dict(
            mem_available_min=4 * GIB,
            swap_growth_max=GIB,
            disk_free_min=20 * GIB,
            rss_sum_max=8 * GIB,
            coordinator_collector_rss_max=512 * 1024**2,
            run_evidence_max=4 * GIB,
            batch_evidence_max=12 * GIB,
        ),
        pair_eligibility="all planned writes sent/business-success; no unknown; all ordinary slots completed; usable D; positive child end-to-end p95 delta; ties D-B",
        observation_capacity=8192,
        resource_period_ms=100,
        pss_period_ms=1000,
        interpretations=dict(
            seed="fixture deterministic; no randomness",
            limit_state="standard tree has no limits; verified empty",
            detail_baseline="independent / summary first; stays within detail logical 10 s",
            grid="slot may dispatch until following grid tick; expired slots skipped",
        ),
    )
    if product:
        config.update(
            profile="product-20261025",
            order=["smoke", "D", "D"],
            scheduler_interval_s=10,
            boundary_scope="pool_task",
            checkpoint_interval_s=120,
            duration=300,
            slots=30000,
            meter_max=120,
            smoke_periodic_tail=True,
        )
        config["smoke"].update(duration=150, slots=1500)
        # The longer user-selected window retains proportionally more full
        # read evidence. Memory/disk-free protection lines stay unchanged.
        config["stop_limits"].update(
            run_evidence_max=8 * GIB, batch_evidence_max=24 * GIB
        )
        config["caps"].update(
            run_s=1200, operational_s=1190, setup_s=600, post_exit_s=300
        )
    save(batch / "config.json", config)
    (batch / "config.sha256").write_text(
        hashlib.sha256((batch / "config.json").read_bytes()).hexdigest() + "\n"
    )
    diagnostic_copy(repo, batch / "diagnostic")
    save(
        batch / "environment.json",
        dict(
            python=sys.version,
            executable=sys.executable,
            executable_realpath=str(Path(sys.executable).resolve()),
            platform=platform.platform(),
            uname=list(platform.uname()),
            cpu_count=os.cpu_count(),
            cpuinfo=Path("/proc/cpuinfo").read_text(),
            meminfo=Path("/proc/meminfo").read_text(),
            boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            dependencies={
                d.metadata["Name"]: d.version
                for d in importlib.metadata.distributions()
            },
        ),
    )
    (batch / "uv.lock").write_bytes((repo / "uv.lock").read_bytes())
    print(str(batch / "diagnostic/scripts/mix100k01.py"))


def budget_open(batch, condition, start_ns):
    budget = read(batch / "budget.json")
    history = budget.get("runs", [])
    if len(history) >= 7 or any(r.get("end_ns") is None for r in history):
        raise RuntimeError("service count/tail prohibits launch")
    config = read(batch / "config.json")
    if config.get("profile") == "product-20261025":
        if condition != config["order"][len(history)]:
            raise ValueError("product diagnostic order violated")
        if history and (
            not history[0].get("qualified_smoke")
            or any(r.get("stop_batch") for r in history)
        ):
            raise ValueError("prior product diagnostic failed")
        history.append(dict(condition=condition, start_ns=start_ns, end_ns=None))
        budget["runs"] = history
        save(batch / "budget.json", budget)
        return len(history)
    order = ["smoke", "D", "B", "C", "A"]
    if len(history) < 5:
        if condition != order[len(history)]:
            raise ValueError("predeclared service order violated")
        if history and not history[0].get("qualified_smoke"):
            raise ValueError("smoke did not pass; batch stopped")
        if any(r.get("stop_batch") for r in history):
            raise ValueError("prior batch stop")
    else:
        selection = read(batch / "repeat-selection.json")
        if condition != selection["order"][len(history) - 5]:
            raise ValueError("repeat order not preselected")
    service_seconds = sum((r["end_ns"] - r["start_ns"]) / NS for r in history)
    started = budget["start_monotonic"] * NS
    elapsed = (start_ns - started) / NS
    preparation = budget.get("preparation_seconds", elapsed if not history else 0)
    if (
        preparation > 7200
        or elapsed + 480 + 1800 > 14400
        or service_seconds + 480 > 3600
    ):
        raise RuntimeError("independent budget cannot reserve lifecycle plus delivery")
    if not history:
        budget["preparation_seconds"] = preparation
        budget["preparation_end_ns"] = start_ns
    elif start_ns - history[0]["start_ns"] - service_seconds * NS > 1800 * NS:
        raise RuntimeError("analysis reserve insufficient")
    history.append(
        dict(
            condition=condition,
            start_ns=start_ns,
            utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            end_ns=None,
        )
    )
    budget["runs"] = history
    save(batch / "budget.json", budget)
    return len(history)


def run_one(batch, condition):
    from r3_14_perf04_safety import Registry, Watchdog

    base = read(batch / "config.json")
    if (
        hashlib.sha256((batch / "config.json").read_bytes()).hexdigest()
        != (batch / "config.sha256").read_text().strip()
    ):
        raise ValueError("frozen config changed")
    for name, digest in read(batch / "diagnostic/source-index.json").items():
        if (
            hashlib.sha256((batch / "diagnostic" / name).read_bytes()).hexdigest()
            != digest
        ):
            raise ValueError("frozen source changed: " + name)
    # Check host protection before any owned experiment process.
    import shutil

    memory = {
        line.split(":")[0]: int(line.split()[1]) * 1024
        for line in Path("/proc/meminfo").read_text().splitlines()
        if len(line.split()) == 3
    }
    if memory["MemAvailable"] < 4 * GIB or shutil.disk_usage(batch).free < 20 * GIB:
        raise RuntimeError("prelaunch resource stop")
    start_ns = int(os.environ.get("MIX100K_START_NS", time.monotonic_ns()))
    number = budget_open(batch, condition, start_ns)
    directory = batch / f"{number:02d}-{condition}"
    directory.mkdir()
    os.chmod(directory, 0o700)
    config = dict(base)
    if condition == "smoke":
        config.update(base["smoke"])
        config["flows"] = 1
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        config["port"] = reservation.getsockname()[1]
    config.update(
        checkpoint=condition in {"smoke", "D", "B"},
        full=condition in {"smoke", "D", "C"},
        condition=condition,
        coordinator_pid=os.getpid(),
        supervisor_pid=int(os.environ.get("MIX100K_SUPERVISOR_PID", os.getppid())),
        setup_deadline_ns=start_ns + base["caps"]["setup_s"] * NS,
        operational_deadline_ns=start_ns + base["caps"]["operational_s"] * NS,
        socket=f"/tmp/mix100k01-{os.getpid()}-{number}.sock",
    )
    save(directory / "config.json", config)
    (directory / "secret").write_text(os.urandom(32).hex() + "\n")
    os.chmod(directory / "secret", 0o600)
    import getpass

    (directory / "whitelist").write_text(getpass.getuser() + "\n")
    log = Ledger(directory / "coordinator.jsonl")
    registry = Registry(directory / "registry.json")
    watchdog = Watchdog(registry, start_ns + base["caps"]["run_s"] * NS)
    watchdog.arm()  # Must precede the first business/collector process.
    processes = {}
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("TAKLER_")
        and k.lower() not in {"http_proxy", "https_proxy", "all_proxy", "grpc_proxy"}
    }
    env.update(
        PYTHONPATH=str(batch / "diagnostic/src")
        + os.pathsep
        + str(batch / "diagnostic/scripts"),
        PYTHONUNBUFFERED="1",
        TAKLER_LOG_LEVEL="WARNING",
        NO_PROXY="127.0.0.1,localhost",
        no_proxy="127.0.0.1,localhost",
    )
    script = batch / "diagnostic/scripts/mix100k01.py"
    result = dict(
        condition=condition, failure=None, stop_batch=False, qualified_smoke=False
    )
    phase_deadline = config["setup_deadline_ns"]
    collector_started = None

    def launch(role):
        output = (directory / f"{role}.log").open("xb", buffering=0)
        proc = registry.launch(
            [sys.executable, str(script), role, "--directory", str(directory)],
            role,
            category="service" if role == "server" else "tool",
            stdout=output,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=directory,
        )
        output.close()
        processes[role] = proc
        log("process_started", role=role, child_pid=proc.pid)

    def check():
        remaining(min(phase_deadline, config["operational_deadline_ns"]))
        now = time.monotonic_ns()
        if "collector" in processes:
            if processes["collector"].poll() is not None:
                raise RuntimeError("collector exited")
            health_file = directory / "collector-health.json"
            last = (
                read(health_file)["at_ns"]
                if health_file.exists()
                else collector_started
            )
            if now - last >= 5 * NS:
                raise RuntimeError("collector health unavailable for 5 s")
        for failure in (
            "collector-failure.json",
            "integrity-failure.json",
            "writer-failure.json",
        ):
            if (directory / failure).exists():
                raise RuntimeError(failure + ": " + str(read(directory / failure)))
        for role, proc in processes.items():
            if proc.poll() not in (None, 0):
                raise RuntimeError(f"{role} unexpected exit {proc.returncode}")

    def await_file(name):
        while not (directory / name).exists():
            check()
            time.sleep(0.02)
        check()
        return read(directory / name)

    def boundary(phase):
        save(directory / f"{phase}-request.json", dict(deadline_ns=phase_deadline))
        wire = QueryWire(directory, phase, log)
        wire.begin(phase, min(phase_deadline, time.monotonic_ns() + 60 * NS))
        executor = ThreadPoolExecutor(max_workers=1)
        scope = (
            read(directory / "server-ready.json")["paths"][0]
            if config.get("boundary_scope") == "pool_task"
            else "/"
        )
        future = executor.submit(
            wire.full,
            directory / f"{phase}-view.jsonl",
            1 if scope != "/" else config["nodes"],
            scope,
        )
        try:
            while not future.done():
                check()
                time.sleep(0.02)
            actual = future.result()
        finally:
            wire.close()
            executor.shutdown(wait=True, cancel_futures=True)
        root = await_file(f"{phase}-root.json")
        verify_stream_link(actual, root)  # Real entry wiring, not just a helper test.
        save(directory / f"{phase}-link.json", actual)
        check()

    try:
        collector_started = time.monotonic_ns()
        launch("collector")
        await_file("collector-ready.json")
        launch("server")
        ready = await_file("server-ready.json")
        if ready["fresh_gc"] != ready["ready_gc"] or not ready["ready_gc"]["enabled"]:
            raise RuntimeError("default GC state inconsistent")
        if (
            Path(ready["source"]).resolve()
            != (batch / "diagnostic/src/takler/__init__.py").resolve()
        ):
            raise RuntimeError("source provenance mismatch")
        boundary("pre")
        for role in ("writer", "queries", "full1", "full2"):
            launch(role)
            await_file(f"{role}-ready.json")
        measure_start = time.monotonic_ns() + 10 * NS
        end = measure_start + config["duration"] * NS
        window = dict(
            start_ns=measure_start,
            end_ns=end,
            drain_deadline_ns=min(end + 60 * NS, config["operational_deadline_ns"]),
        )
        save(directory / "window.json", window)
        phase_deadline = min(measure_start, config["operational_deadline_ns"])
        log("phase", phase="baseline", deadline_ns=phase_deadline)
        while time.monotonic_ns() < phase_deadline:
            check()
            time.sleep(min(0.02, max(0, (phase_deadline - time.monotonic_ns()) / NS)))
        phase_deadline = min(end, config["operational_deadline_ns"])
        log("phase", phase="measurement", **window)
        last_child = measure_start
        while time.monotonic_ns() < end:
            check()
            health = directory / "writer-health.json"
            if health.exists():
                last_child = max(last_child, read(health)["last_success_ns"])
            if time.monotonic_ns() - last_child >= 10 * NS:
                result["product_stop"] = "10 s without successful child"
                break
            time.sleep(0.02)
        save(directory / "withdraw.json", dict(at_ns=time.monotonic_ns()))
        phase_deadline = min(
            time.monotonic_ns() + 60 * NS, config["operational_deadline_ns"]
        )
        log("phase", phase="withdrawal", deadline_ns=phase_deadline)
        while any(
            processes[r].poll() is None for r in ("writer", "queries", "full1", "full2")
        ):
            check()
            time.sleep(0.02)
        for role in ("writer", "queries", "full1", "full2"):
            await_file(f"{role}-final.json")
        if condition == "smoke" and config.get("smoke_periodic_tail"):
            phase_deadline = min(
                time.monotonic_ns() + (config["checkpoint_interval_s"] + 10) * NS,
                config["operational_deadline_ns"],
            )
            log(
                "phase", phase="stopped_write_periodic_tail", deadline_ns=phase_deadline
            )
            while not periodic_archive_seen(directory):
                check()
                time.sleep(0.1)
        phase_deadline = min(
            time.monotonic_ns() + base["caps"]["post_exit_s"] * NS,
            config["operational_deadline_ns"],
        )
        log("phase", phase="post_and_exit", deadline_ns=phase_deadline)
        boundary("post")
        verify_final_meters(directory)
        verify_query_ledgers(directory, phase_deadline)
        save(directory / "stop-server.json", dict(deadline_ns=phase_deadline))
        final = await_file("server-final.json")
        while processes["server"].poll() is None:
            check()
            time.sleep(0.02)
        # Drain and audit even a sender-reported loss, retaining its final gap.
        finish_collector(
            directory,
            processes["collector"],
            processes["server"].pid,
            final["observation"]["produced"],
            phase_deadline,
        )
        if (
            final["gc"] != ready["fresh_gc"]
            or final["observation"]["dropped"]
            or not final["observation"]["sender_stopped"]
            or not final["exit_save_ok"]
            or final["leases"]
            or final["checkpoint_background"]
        ):
            raise RuntimeError("critical GC/observation/exit final failed")
        result["qualified_smoke"] = (
            qualify_smoke(directory) if condition == "smoke" else False
        )
        if condition == "smoke" and not result["qualified_smoke"]:
            raise RuntimeError("smoke qualification unmet; no rerun")
    except BaseException as exc:
        result["failure"] = f"{type(exc).__name__}: {exc}"
        result["traceback"] = traceback.format_exc()
        result["stop_batch"] = True
        log("run_failure", error=result["failure"])
        save(
            directory / "withdraw.json",
            dict(at_ns=time.monotonic_ns(), reason=result["failure"]),
        )
    finally:
        cleanup_start = time.monotonic_ns()
        cleanup_deadline = min(
            cleanup_start + 10 * NS, start_ns + base["caps"]["run_s"] * NS
        )
        log("phase", phase="cleanup", deadline_ns=cleanup_deadline)
        cleanup = registry.cleanup(cleanup_deadline)
        result["cleanup"] = cleanup
        result["stop_batch"] |= (
            bool(cleanup["live_registered"]) or cleanup["service_tail"] == "unknown"
        )
        result["end_ns"] = time.monotonic_ns()
        result["start_ns"] = start_ns
        result["lifecycle_seconds"] = (result["end_ns"] - start_ns) / NS
        result["stop_batch"] |= result["lifecycle_seconds"] > base["caps"]["run_s"]
        save(directory / "result.json", result)
        budget = read(batch / "budget.json")
        budget["runs"][-1].update(
            end_ns=result["end_ns"],
            stop_batch=result["stop_batch"],
            qualified_smoke=result["qualified_smoke"],
            failure=result["failure"],
        )
        save(batch / "budget.json", budget)
        log.close()
    print(
        json.dumps(
            {k: v for k, v in result.items() if k not in {"cleanup", "traceback"}},
            indent=2,
        )
    )
    return result


def rows(path):
    with path.open() as source:
        for line in source:
            yield json.loads(line)


def verify_final_meters(directory):
    credentials = read(directory / "credentials.json")
    values = {p: 0 for p in credentials}
    sent, returned, unknown = set(), set(), set()
    planned = {}
    for row in rows(directory / "writer-requests.jsonl"):
        slot = row.get("slot")
        if row["kind"] == "planned":
            if slot in planned:
                raise ValueError("duplicate planned slot")
            planned[slot] = (row["path"], row["value"])
        elif row["kind"] == "sent":
            if slot in sent:
                raise ValueError("duplicate sent slot")
            sent.add(slot)
        elif row["kind"] == "returned":
            returned.add(slot)
            if row["flag"] == 0:
                path, value = planned[slot]
                values[path] = max(values[path], value)
        elif row["kind"] == "write_unknown":
            unknown.add(slot)
    if unknown or sent != returned:
        raise ValueError("unknown write tail")
    if len(planned) != read(directory / "config.json")["slots"]:
        raise ValueError("planned ledger incomplete")
    for row in rows(directory / "post-pool.jsonl"):
        if (
            row["attempt"] != credentials[row["path"]]["attempt"]
            or row["meter"] != values[row["path"]]
            or row["status"] != "active"
        ):
            raise ValueError("final meter/identity mismatch")
    save(
        directory / "meter-verification.json",
        dict(
            planned=len(planned),
            sent=len(sent),
            returned=len(returned),
            unknown=0,
            equivalent=True,
        ),
    )


def verify_summary_archive(directory, evidence, deadline_ns=None):
    """Recompute exact framed bytes, page identity/order and complete node count."""
    from takler.protocol.query_v1 import MAX_PAGE_BYTES, decode_query_document

    name = evidence["file"]
    if (
        Path(name).name != name
        or not name.endswith(".pages")
        or evidence.get("format") != "takler-summary-pages-v1"
    ):
        raise SummaryEvidenceError("invalid summary artifact reference")
    digest, pages, nodes, size = hashlib.sha256(), 0, 0, 0
    complete = False
    fields = (
        "session_id",
        "query_id",
        "snapshot_id",
        "as_of",
        "expires_at",
        "root_name",
    )
    try:
        with (directory / name).open("rb") as source:
            while prefix := source.read(4):
                if deadline_ns is not None:
                    remaining(deadline_ns)
                if complete or len(prefix) != 4:
                    raise SummaryEvidenceError("extra/truncated summary frame")
                length = struct.unpack("<I", prefix)[0]
                if not 0 < length <= MAX_PAGE_BYTES:
                    raise SummaryEvidenceError("summary frame length exceeds bound")
                raw = source.read(length)
                if len(raw) != length:
                    raise SummaryEvidenceError("truncated summary page")
                page = decode_query_document(raw, expected="page")
                if (
                    page.page_index != pages
                    or page.base_revision != evidence["version"]
                    or any(getattr(page, k) != evidence[k] for k in fields)
                ):
                    raise SummaryEvidenceError(
                        "summary fixed identity/page order mismatch"
                    )
                if pages == 0 and (
                    not page.nodes or page.nodes[0].path != evidence["scope_path"]
                ):
                    raise SummaryEvidenceError("summary scope root mismatch")
                nodes += len(page.nodes)
                if (
                    page.total_nodes is not None
                    and page.total_nodes != evidence["nodes"]
                ):
                    raise SummaryEvidenceError("summary expected node count mismatch")
                complete = page.complete
                digest.update(prefix)
                digest.update(raw)
                size += 4 + length
                pages += 1
    except OSError as exc:
        raise SummaryEvidenceError(f"summary artifact unavailable: {name}") from exc
    if (
        not complete
        or pages != evidence["pages"]
        or nodes != evidence["nodes"]
        or size != evidence["archive_bytes"]
        or digest.hexdigest() != evidence["sha256"]
    ):
        raise SummaryEvidenceError("summary digest/length/completeness mismatch")
    if deadline_ns is not None:
        remaining(deadline_ns)


def verify_summary_archives(directory, deadline_ns=None):
    started, verified, terminals = {}, {}, {}
    for row in rows(directory / "query-requests.jsonl"):
        kind, rid = row["kind"], row.get("request_id")
        if kind == "summary_archive_started":
            if row["file"] in started:
                raise SummaryEvidenceError("duplicate summary archive start")
            started[row["file"]] = (rid, row["channel"], row["reason"])
        elif kind == "summary_digest":
            evidence = row["evidence"]
            name = evidence["file"]
            if name in verified or started.get(name) != (
                rid,
                row["channel"],
                row["reason"],
            ):
                raise SummaryEvidenceError("unmatched/duplicate summary digest")
            verify_summary_archive(directory, evidence, deadline_ns)
            verified[name] = evidence
        elif kind in {"completed", "failed", "not_sent"}:
            terminals[rid] = kind
            if kind == "completed" and row["channel"] == "summary":
                reference = row.get("summary_digest")
                if (
                    not reference
                    or verified.get(reference["file"]) != reference
                    or started[reference["file"]][:2] != (rid, "summary")
                    or any(
                        row[k] != reference[k]
                        for k in ("nodes", "version", "snapshot_id")
                    )
                ):
                    raise SummaryEvidenceError(
                        "completed summary digest missing/mismatched"
                    )
    partial = 0
    for name, (rid, _, _) in started.items():
        if name not in verified:
            if (
                terminals.get(rid) != "failed"
                or not (directory / (name + ".partial")).is_file()
            ):
                raise SummaryEvidenceError("summary archive has no trusted terminal")
            partial += 1
    return dict(verified=True, complete=len(verified), failed_partial=partial)


def verify_query_ledgers(directory, deadline_ns=None):
    config = read(directory / "config.json")
    for role in ("query", "full1", "full2"):
        plans, terminals = set(), set()
        for row in rows(directory / f"{role}-requests.jsonl"):
            rid = row.get("request_id")
            if row["kind"] == "planned":
                if rid in plans:
                    raise ValueError("duplicate query plan")
                plans.add(rid)
            elif row["kind"] in {"completed", "failed", "not_sent"}:
                if rid in terminals:
                    raise ValueError("duplicate query terminal")
                terminals.add(rid)
        if plans != terminals:
            raise ValueError("necessary query terminal missing")
        if (
            role == "query"
            and len(plans) != config["duration"] * 2 + config["duration"] // 10
        ):
            raise ValueError("ordinary query plan incomplete")
    verify_summary_archives(directory, deadline_ns)


def periodic_archive_seen(directory):
    """A completed archive proves publication; ignore a concurrent partial line."""
    path = directory / "server-ledger.jsonl"
    if not path.exists():
        return False
    with path.open("rb") as source:
        for raw in source:
            if raw.endswith(b"\n"):
                row = json.loads(raw)
                if row["kind"] == "checkpoint_archive" and not row["exit_save"]:
                    return True
    return False


def qualify_smoke(directory):
    config = read(directory / "config.json")
    counts = {name: 0 for name in ("summary", "full1", "full2")}
    success = sum(
        r["kind"] == "returned" and r["flag"] == 0
        for r in rows(directory / "writer-requests.jsonl")
    )
    for name in ("query", "full1", "full2"):
        for row in rows(directory / f"{name}-requests.jsonl"):
            if row["kind"] == "completed" and row["channel"] in counts:
                counts[row["channel"]] += 1
    window = read(directory / "window.json")
    publishes = sum(
        r["kind"] == "checkpoint_link" and not r["exit_save"]
        for r in rows(directory / "server-ledger.jsonl")
    )
    outcomes = sum(
        r["kind"] == "service_event"
        and r["event"]["kind"] == "checkpoint_outcome"
        and r["event"]["values"][1:4] == [1, 1, 0]
        and (
            config.get("smoke_periodic_tail")
            or window["start_ns"] <= r["event"]["at_ns"] <= window["end_ns"]
        )
        for r in rows(directory / "events.jsonl")
    )
    verified = read(directory / "meter-verification.json")
    result = dict(
        success=success,
        sent=verified["sent"],
        planned=config["slots"],
        completed=counts,
        periodic_links=publishes,
        target_complete=success == config["slots"],
        measurement_periodic_success=outcomes,
        periodic_scope="whole_smoke_lifecycle"
        if config.get("smoke_periodic_tail")
        else "measurement",
    )
    passed = (
        success == verified["sent"]
        and (
            success > 0
            if config.get("smoke_periodic_tail")
            else success == config["slots"]
        )
        and counts["summary"] >= 1
        and min(counts["full1"], counts["full2"]) >= 1
        and outcomes >= (1 if config.get("smoke_periodic_tail") else 2)
    )
    save(directory / "smoke-qualification.json", dict(passed=passed, **result))
    return passed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=[
            "freeze",
            "run-one",
            "server",
            "collector",
            "writer",
            "queries",
            "full1",
            "full2",
        ],
    )
    parser.add_argument("--batch", type=Path)
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--batch-id", default="MIX100K-01")
    parser.add_argument(
        "--product",
        action="store_true",
        help="Use the 2026-10-25 single-product diagnostic profile",
    )
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--condition", choices=["smoke", "D", "B", "C", "A"])
    options = parser.parse_args()
    if options.action == "freeze":
        freeze(
            options.batch.resolve(),
            options.repo.resolve(),
            options.batch_id,
            product=options.product,
        )
    elif options.action == "run-one":
        run_one(options.batch.resolve(), options.condition)
    elif options.action == "server":
        asyncio.run(service(options.directory.resolve()))
    elif options.action in {"full1", "full2"}:
        full_reader(options.directory.resolve(), options.action)
    else:
        globals()[options.action](options.directory.resolve())


if __name__ == "__main__":
    main()
