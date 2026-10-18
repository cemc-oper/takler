"""PERF-04 bounded write proofs and fixed-root/checkpoint boundary verification."""

from collections import Counter
import hashlib
import json

from r3_14_perf04 import STEPS
from r3_14_perf04_fixture import assert_stage, check_limits, task_state


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def fixed_view(version):
    """Complete public fields from ONE immutable root, only at run boundaries."""
    records = {path: version.public_node(path) for path in version.index}
    # Public detail intentionally omits holder paths. Read them from the SAME
    # immutable root; the normal full snapshot exposes this runtime field too.
    for path, record in records.items():
        record["limit_usage"] = json.loads(version.node(path).runtime).get(
            "limit_usage", {}
        )
    return dict(
        state_version=version.state_version,
        paths=list(version.index),
        nodes=records,
        sha256=hashlib.sha256(canonical(records)).hexdigest(),
    )


def verify_view(view, expected_paths=None, pool=None, sentinel=None):
    records = view["nodes"]
    if view["sha256"] != hashlib.sha256(canonical(records)).hexdigest():
        raise ValueError("fixed view digest mismatch")
    if set(view["paths"]) != set(records) or len(view["paths"]) != len(
        set(view["paths"])
    ):
        raise ValueError("fixed view paths mismatch")
    if expected_paths is not None and set(records) != set(expected_paths):
        raise ValueError("fixed view expected paths mismatch")
    for path, record in records.items():
        if record["path"] != path or record["state_version"] != view["state_version"]:
            raise ValueError("mixed fixed version")
    # C1 trees are flat root -> flow -> tasks. Check stored parent aggregation.
    priority = [6, 5, 4, 3, 2]
    for path, record in records.items():
        if record["node_kind"] != "flow":
            continue
        counts = Counter(records[p]["state"]["status"] for p in record["child_paths"])
        expected = next((s for s in priority if counts[s]), 1)
        if record["state"]["status"] != expected:
            raise ValueError("parent aggregate mismatch")
    if pool is not None:
        from r3_14_perf04 import prefix

        for task in pool.tasks:
            if task.unknown:
                raise ValueError("unknown write cannot validate as effective")
            actual = records[task.path]
            expected = (
                prefix(task.stage)
                if task.stage < 11
                else dict(status="queued", events=[False] * 4, meter=0)
            )
            statuses = {
                1: "unknown",
                2: "complete",
                3: "queued",
                4: "submitted",
                5: "active",
                6: "aborted",
            }
            if statuses[actual["state"]["status"]] != expected["status"]:
                raise ValueError("fixed pool status mismatch")
            attempt = None if task.stage == 11 else task.attempt
            if actual.get("attempt_id") != attempt:
                raise ValueError("fixed pool attempt mismatch")
            if [e["value"] for e in actual["events"]] != expected["events"] or actual[
                "meters"
            ][0]["value"] != expected["meter"]:
                raise ValueError("fixed pool event/meter mismatch")
    for path, record in records.items():
        if record["node_kind"] != "flow":
            continue
        bound = {}
        for child in record["child_paths"]:
            task = records[child]
            for binding in task["in_limit_manager"]["in_limit_list"]:
                if (
                    binding["node_path"] != path
                    or binding["limit_name"] != "capacity"
                    or binding["tokens"] != 1
                ):
                    raise ValueError("fixed limit binding mismatch")
                if task["state"]["status"] in (4, 5):
                    bound[child] = binding["tokens"]
        limit = record["limits"][0]
        usage = record["limit_usage"][limit["name"]]
        if (
            set(usage["node_paths"]) != set(bound)
            or usage["value"] != sum(bound.values())
            or limit["value"] != usage["value"]
        ):
            raise ValueError("fixed limit occupancy mismatch")
    if sentinel is not None:
        path, expected = sentinel
        for key in (
            "attempt_id",
            "submission_unknown_reason",
            "state",
            "try_no",
            "task_id",
            "definition_generation",
        ):
            if records[path].get(key) != expected.get(key):
                raise ValueError("fixed unknown sentinel mismatch")
    return True


def record_fragments(request_id, ordinal, record):
    """Preserve complete records across the unchanged 16 KiB event channel."""
    import base64

    raw = canonical(record)
    if len(raw) > 1024**2:
        raise ValueError("S verification record exceeds 1 MiB")
    digest = hashlib.sha256(raw).hexdigest()
    for offset in range(0, len(raw), 6144):
        yield dict(
            kind="verification_fragment",
            id=request_id,
            record_ordinal=ordinal,
            offset=offset,
            total_bytes=len(raw),
            sha256=digest,
            data_base64=base64.b64encode(raw[offset : offset + 6144]).decode(),
        )


class RecordAssembler:
    """One bounded record at a time; the caller streams completed nodes to disk."""

    def __init__(self):
        self.ordinal = 0
        self.data = bytearray()
        self.identity = None

    def accept(self, event):
        import base64

        identity = (
            event["id"],
            event["record_ordinal"],
            event["total_bytes"],
            event["sha256"],
        )
        if not 0 < identity[2] <= 1024**2 or identity[1] != self.ordinal:
            raise ValueError("verification ordinal/size mismatch")
        if self.identity is None:
            self.identity = identity
        if identity != self.identity or event["offset"] != len(self.data):
            raise ValueError("verification fragment identity/offset mismatch")
        part = base64.b64decode(event["data_base64"], validate=True)
        if not part or len(part) > 6144 or len(self.data) + len(part) > identity[2]:
            raise ValueError("verification fragment length mismatch")
        self.data.extend(part)
        if len(self.data) != identity[2]:
            return None
        if hashlib.sha256(self.data).hexdigest() != identity[3]:
            raise ValueError("verification fragment digest mismatch")
        record = json.loads(self.data)
        self.ordinal += 1
        self.data.clear()
        self.identity = None
        return record

    def finish(self, count):
        if self.data or self.identity is not None or self.ordinal != count:
            raise ValueError("verification fragments incomplete")


def checkpoint_equivalence(path, expected_version):
    """Validate main OR backup independently without starting a restore service."""
    from takler.serialization.checkpoint import restore_checkpoint
    from takler.serialization.version import VersionStore

    document = json.loads(path.read_bytes())
    restored = restore_checkpoint(document)
    if document["source_address"] != json.loads(expected_version.source_address):
        raise ValueError("checkpoint source context mismatch")
    # Like CheckpointManager, restore into the same deployment context.
    restored.server_config = type(restored.server_config).from_dict(
        json.loads(expected_version.source_address)
    )
    status = json.loads(expected_version.node("/").public)["server_status"]
    store = VersionStore(restored, status_provider=lambda: status)
    actual = store.pin()

    # Versions/as_of are provenance, not semantic runtime differences.
    def semantic(root):
        return {
            p: (root.node(p).definition, root.node(p).runtime, root.node(p).public)
            for p in root.index
        }

    if semantic(actual) != semantic(expected_version):
        raise ValueError("checkpoint restored fixed-root mismatch")
    if actual.secrets != expected_version.secrets:
        raise ValueError("checkpoint restored identity mismatch")
    check_limits(restored)
    return dict(
        file=path.name,
        saved_at=document["saved_at"],
        nodes=len(actual.index),
        state_version=expected_version.state_version,
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        equivalent=True,
    )


class WriteVerifier:
    """Install on the normal handler. No per-child full-tree projection.

    _run executes inside the product writer lock. dispatch returns after its
    finally/flush; no await occurs between that return and the proof capture.
    Handoffs are returned to private IPC only; public journal rows omit passwords.
    """

    def __init__(self, handler, paths, sentinel, record):
        self.handler, self.scheduler, self.record = handler, handler.scheduler, record
        self.paths = {p: i for i, p in enumerate(paths)}
        self.stages = {p: i // 25 for i, p in enumerate(paths)}
        self.sentinel = sentinel
        self.sentinel_state = task_state(self.scheduler.bunch.find_node(sentinel))
        self.sentinel_password = self.scheduler.bunch.find_node(sentinel).job_password
        self.status_counts = {
            f.node_path: Counter(t.state.node_status.value for t in f.children)
            for f in self.scheduler.bunch.flows.values()
        }
        self.limits = {
            p: set(v["node_paths"])
            for p, v in check_limits(self.scheduler.bunch).items()
        }
        self.proofs = {}
        self.proof_bytes = 0
        self.attempts = {p: self.scheduler.bunch.find_node(p).attempt_id for p in paths}
        self.original = handler.dispatch

    def install(self):
        original = self.original

        async def dispatch(command, parse_request, **kwargs):
            holder = {}

            def parse():
                request = parse_request()
                path = getattr(request, "node_path", None)
                if path is None and len(getattr(request, "node_paths", [])) == 1:
                    path = request.node_paths[0]
                holder.update(request=request, path=path)
                if command.value in ("force", "load", "replace"):
                    holder["control_before"] = self.scheduler.version_store.current
                if path in self.paths:
                    holder["before"] = task_state(self.scheduler.bunch.find_node(path))
                    holder["version"] = (
                        self.scheduler.version_store.current.state_version
                    )
                return request

            reply = await original(command, parse, **kwargs)
            path = holder.get("path")
            if path in self.paths:
                if reply.flag:
                    raise ValueError("normal write rejected")
                self._verify(command.value, holder, reply)
            elif "control_before" in holder:
                op = command.value
                request = holder["request"]
                if op == "force":
                    target = "/flow008"
                    expected = len(holder["control_before"].index)
                elif op == "replace":
                    target = "/flow009"
                    expected = 10011
                else:
                    data = json.loads(request.flow_bytes)
                    target = "/" + data["root"]["name"]
                    if target not in ("/flow010", "/flow011"):
                        raise ValueError("unexpected LOAD target")
                    expected = len(holder["control_before"].index) + 1001
                proof = verify_structure(
                    self.scheduler, op, reply, holder["control_before"], expected
                )
                self._store_proof(target, dict(proof=proof))
                self.record(
                    dict(kind="structure_proof", operation=op, path=target, **proof)
                )
            return reply

        self.handler.dispatch = dispatch
        return self

    def _verify(self, operation, holder, reply):
        path, before, request = holder["path"], holder["before"], holder["request"]
        task = self.scheduler.bunch.find_node(path)
        stage = self.stages[path]
        if stage < 10:
            expected, value = STEPS[stage]
            if operation != expected or request.attempt_id != before["attempt"]:
                raise ValueError("stage or attempt mismatch")
            if operation == "event" and request.event_name != value:
                raise ValueError("event stage mismatch")
            if operation == "meter" and request.meter_value != value:
                raise ValueError("meter stage mismatch")
            assert_stage(task, stage + 1)
        elif stage == 10:
            if (
                operation != "requeue"
                or task.attempt_id is not None
                or task.try_no != 0
            ):
                raise ValueError("requeue identity mismatch")
            if (
                task.state.node_status.name != "queued"
                or any(e.value for e in task.events)
                or task.find_meter("progress").value
            ):
                raise ValueError("requeue reset mismatch")
        else:
            if (
                operation != "run"
                or request.force
                or not task.attempt_id
                or task.attempt_id == before["attempt"]
            ):
                raise ValueError("RUN did not establish new attempt")
            assert_stage(task, 0)
        if any(r.flag or r.effect != "applied" for r in getattr(reply, "results", [])):
            raise ValueError("ineffective control write")
        after = task_state(task)
        root = self.scheduler.version_store.pin()
        if root.state_version <= holder["version"] or after == before:
            raise ValueError("successful no-op is not effective throughput")
        flow = task.parent
        counts = self.status_counts[flow.node_path]
        counts[
            {
                "unknown": 1,
                "complete": 2,
                "queued": 3,
                "submitted": 4,
                "active": 5,
                "aborted": 6,
            }[before["status"]]
        ] -= 1
        counts[task.state.node_status.value] += 1
        aggregate = next((s for s in [6, 5, 4, 3, 2] if counts[s]), 1)
        frozen_parent = json.loads(root.node(flow.node_path).public)
        if frozen_parent["state"]["status"] != aggregate:
            raise ValueError("parent aggregation mismatch")
        limit = flow.find_limit("capacity")
        expected_paths = self.limits[flow.node_path]
        if task.in_limit_manager.in_limit_list:
            if after["status"] in ("submitted", "active"):
                expected_paths.add(path)
            else:
                expected_paths.discard(path)
        if limit.node_paths != expected_paths or limit.value != len(expected_paths):
            raise ValueError("write limit mismatch")
        if (
            self.scheduler.bunch.find_node(self.sentinel).job_password
            != self.sentinel_password
        ):
            raise ValueError("unknown sentinel credential changed")
        if (
            task_state(self.scheduler.bunch.find_node(self.sentinel))
            != self.sentinel_state
        ):
            raise ValueError("unknown sentinel changed")
        proof = dict(
            effective=True,
            path=path,
            stage=stage,
            attempt=self.attempts[path],
            state_version=root.state_version,
        )
        self._store_proof(
            path,
            dict(proof=proof, identity=self.identity(path) if stage == 11 else None),
        )
        if stage == 11:
            self.attempts[path] = task.attempt_id
        self.stages[path] = (stage + 1) % 12
        self.record(
            dict(
                kind="write_proof",
                operation=operation,
                stage=stage,
                path=path,
                before=before,
                after=after,
                state_version=root.state_version,
                parent_status=aggregate,
                limit_value=limit.value,
            )
        )

    def _store_proof(self, path, value):
        encoded = len(canonical(value))
        if encoded > 65536 or self.proof_bytes + encoded > 1024**2:
            raise ValueError("proof byte capacity exceeded")
        if path in self.proofs or len(self.proofs) >= 252:
            raise ValueError("unconsumed proof or proof capacity exceeded")
        self.proofs[path] = value
        self.proof_bytes += encoded

    def handoff(self, job, timeout=10):
        # Invoke through a private writer-loop IPC adapter in a future live run.
        value = self.proofs[job["path"]]
        token = job.get("reservation")
        if token and any(value["proof"][k] != token[k] for k in ("stage", "attempt")):
            raise ValueError("handoff reservation mismatch")
        self.proofs.pop(job["path"])
        self.proof_bytes -= len(canonical(value))
        return value

    def identity(self, path):
        # Call only on the owning writer loop after a confirmed RUN.
        task = self.scheduler.bunch.find_node(path)
        return dict(
            attempt=task.attempt_id,
            password=task.job_password,
            generation=task.definition_generation,
        )

    def close(self):
        self.handler.dispatch = self.original
        pending = dict(
            unconsumed_paths=list(self.proofs), retained_bytes=self.proof_bytes
        )
        self.proofs.clear()
        self.proof_bytes = 0
        return pending


def verify_structure(
    scheduler, operation, reply, before, expected_nodes, expected_generation=None
):
    """C-channel postconditions from one fixed root after the normal transaction."""
    from r3_14_perf04_fixture import require_load_state

    root = scheduler.version_store.pin()
    if (
        reply.flag
        or root.state_version <= before.state_version
        or len(root.index) != expected_nodes
    ):
        raise ValueError("structural reply/version/size mismatch")
    if operation == "load":
        added = set(root.index) - set(before.index)
        flows = [p for p in added if p.count("/") == 1]
        if len(added) != 1001 or len(flows) != 1:
            raise ValueError("LOAD shape mismatch")
        require_load_state(scheduler.bunch.find_node(flows[0]))
    elif operation == "replace":
        if set(root.index) != set(before.index) or expected_nodes != 10011:
            raise ValueError("REPLACE must retain independent 10k scope")
        flow = scheduler.bunch.find_flow("flow009")
        if (
            not flow.begun
            or not flow.state.suspended
            or any(t.state.node_status.name != "queued" for t in flow.children)
        ):
            raise ValueError("REPLACE initialization mismatch")
        if (
            expected_generation
            and flow.user_parameters["PERF04_GENERATION"].value != expected_generation
        ):
            raise ValueError("REPLACE generation mismatch")
    elif operation == "force":
        if len(reply.results) != 100 or any(
            r.flag or r.effect != "applied" for r in reply.results
        ):
            raise ValueError("batch items incomplete or ineffective")
    else:
        raise ValueError("unknown structural operation")
    check_limits(scheduler.bunch)
    return dict(
        effective=True,
        state_version=root.state_version,
        nodes=len(root.index),
        results=[r.model_dump(mode="json") for r in getattr(reply, "results", [])],
    )


class HandoffServer:
    """Private bounded identity/proof IPC on the service's owning writer loop."""

    def __init__(self, path, verifier, remaining=lambda: 10):
        self.path, self.verifier, self.remaining = path, verifier, remaining
        self.connections = set()
        self.server = None

    async def start(self):
        import asyncio
        import os
        import stat

        if stat.S_IMODE(self.path.parent.stat().st_mode) & 0o077:
            raise ValueError("handoff directory must be private")
        self.server = await asyncio.start_unix_server(
            self._serve, str(self.path), limit=1024
        )
        os.chmod(self.path, 0o600)
        return self

    async def _serve(self, reader, writer):
        import asyncio
        import time

        task = asyncio.current_task()
        if len(self.connections) >= 256:
            writer.close()
            return
        self.connections.add(task)
        try:
            left = min(10, self.remaining())
            if left <= 0:
                raise TimeoutError("handoff phase deadline")
            raw = await asyncio.wait_for(reader.readline(), left)
            if len(raw) > 1024:
                raise ValueError("handoff request bound")
            request = json.loads(raw)
            deadline = min(
                time.monotonic_ns() + int(max(0, self.remaining()) * 1e9),
                request.get("rpc_deadline_ns", 2**63 - 1),
            )
            if time.monotonic_ns() >= deadline:
                raise TimeoutError("handoff server absolute deadline")
            value = self.verifier.handoff(request)
            response = canonical(value) + b"\n"
            if len(response) > 65536:
                raise ValueError("handoff response bound")
            writer.write(response)
            await asyncio.wait_for(
                writer.drain(),
                min(
                    left,
                    max(0, self.remaining()),
                    max(0, (deadline - time.monotonic_ns()) / 1e9),
                ),
            )
        finally:
            self.connections.discard(task)
            writer.close()

    async def close(self):
        import asyncio

        self.server.close()
        await self.server.wait_closed()
        for task in list(self.connections):
            task.cancel()
        await asyncio.gather(*self.connections, return_exceptions=True)
        self.path.unlink(missing_ok=True)


class HandoffClient:
    def __init__(self, path, clock=None, check=lambda: None):
        import time

        self.path, self.clock, self.check = path, clock or time.monotonic_ns, check

    def __call__(self, job, timeout=10):
        import socket

        request = dict(
            path=job["path"],
            reservation=job.get("reservation"),
            rpc_deadline_ns=job.get("rpc_deadline_ns", 2**63 - 1),
        )
        raw = canonical(request) + b"\n"
        if len(raw) > 1024:
            raise ValueError("handoff request bound")
        deadline = min(
            job.get("rpc_deadline_ns", 2**63 - 1),
            self.clock() + int(min(10, timeout) * 1e9),
        )

        def remaining():
            self.check()
            left = (deadline - self.clock()) / 1e9
            if left <= 0:
                raise TimeoutError("handoff absolute deadline")
            return left

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(remaining())
            sock.connect(str(self.path))
            sock.settimeout(remaining())
            sock.sendall(raw)
            data = bytearray()
            while b"\n" not in data:
                sock.settimeout(remaining())
                chunk = sock.recv(min(4096, 65537 - len(data)))
                if not chunk:
                    raise ValueError("incomplete handoff response")
                data.extend(chunk)
                if len(data) > 65536:
                    raise ValueError("handoff response bound")
        remaining()
        return json.loads(data)


def install_l_observation(server, socket):
    """Explicit PERF-04 L installation; no summary capture/link/D instrumentation."""
    from r3_14_observation import Emitter, ServiceObservation
    from r3_14_perf08a_hooks import install_checkpoint_boundaries

    emitter = Emitter(socket, 8192)
    observation = ServiceObservation(
        server, emitter, False, lag_mode="record"
    ).install()
    install_checkpoint_boundaries(
        observation
    )  # Shared publication/exit semantics only.
    return observation, emitter


def stream_fixed_view(transport, deadline_ns, check, emit, clock=None):
    """Q pre/post verification: one full lease, complete validation, cancel ACK."""
    import time
    from dataclasses import asdict
    from takler.client.full_snapshot import FullSnapshotClient, FullSnapshotComplete

    clock = clock or time.monotonic_ns
    original = transport.query
    cancelled = False
    complete = None
    count = 0
    semantic_digest = hashlib.sha256()

    def query(request):
        nonlocal cancelled
        check()
        left = (deadline_ns - clock()) / 1e9
        if left <= 0:
            raise TimeoutError("full verification deadline")
        previous = transport.single_timeout
        transport.single_timeout = min(10, left)
        try:
            reply = original(request)
            if request.kind == "cancel_full_snapshot_request":
                cancelled = getattr(reply, "cancelled", False) is True
            return reply
        finally:
            transport.single_timeout = previous

    transport.query = query
    try:
        for record in FullSnapshotClient(transport).iter_full_snapshot(scope_path="/"):
            check()
            if clock() >= deadline_ns:
                raise TimeoutError("full verification deadline")
            if isinstance(record, FullSnapshotComplete):
                complete = record
            else:
                semantic_digest.update(canonical(record) + b"\n")
                emit(record)
                count += 1
    finally:
        transport.query = original
    if complete is None or not cancelled or count != complete.total_nodes:
        raise ValueError("full verification incomplete or cancel unconfirmed")
    return dict(
        completion=asdict(complete),
        cancelled=cancelled,
        count=count,
        semantic_sha256=semantic_digest.hexdigest(),
    )


class CheckpointAudit:
    """Bounded sequence/root links and asynchronous archive of actual publications.

    Retain <=3 pending fixed roots plus current main/backup roots. Intermediate
    files are preserved, but only retained main/backup roots claim equivalence.
    """

    def __init__(self, manager, directory, record):
        import queue
        import threading

        self.manager, self.directory, self.record = manager, directory, record
        self.pending = {}
        self.main = self.backup = None
        self.jobs = queue.Queue(3)
        self.fd_lock = threading.Lock()
        self.stop = threading.Event()
        self.failure = None
        self.originals = []
        self.thread = threading.Thread(target=self._archive, daemon=True)
        self.thread.start()

    def install(self):
        from r3_14_perf08a_hooks import EXIT_SAVE

        manager = self.manager
        pin = manager._pin_fixed_for_write
        publish = manager._publish_prepared

        def captured():
            result = pin()
            sequence, version, saved_at = result
            if version is not None:
                if len(self.pending) >= 3:
                    self.failure = "checkpoint pin capacity exceeded"
                else:
                    self.pending[sequence] = (version, saved_at)
                self.record(
                    dict(
                        kind="checkpoint_link",
                        checkpoint_sequence=sequence,
                        state_version=version.state_version,
                        saved_at=saved_at,
                        exit_save=EXIT_SAVE.get(),
                    )
                )
            return result

        def published(sequence, prepared):
            import os
            import queue

            result = publish(sequence, prepared)
            source = self.pending.pop(sequence, None)
            with self.fd_lock:
                if self.failure:
                    return result
                if result[0] and result[1] and source is not None:
                    self.backup, self.main = self.main, source[0]
                    fd = os.open(manager.checkpoint_file, os.O_RDONLY)
                    try:
                        self.jobs.put_nowait((fd, sequence, EXIT_SAVE.get()))
                    except queue.Full:
                        os.close(fd)
                        self.failure = "checkpoint archive capacity exceeded"
                elif not result[0]:
                    self.failure = "periodic/exit checkpoint failure"
            return result

        for name, replacement in [
            ("_pin_fixed_for_write", captured),
            ("_publish_prepared", published),
        ]:
            self.originals.append((name, getattr(manager, name)))
            setattr(manager, name, replacement)
        return self

    def _archive(self):
        import os
        import queue

        try:
            while not self.stop.is_set() or not self.jobs.empty():
                try:
                    fd, sequence, exit_save = self.jobs.get(timeout=0.02)
                except queue.Empty:
                    continue
                digest = hashlib.sha256()
                size = 0
                path = self.directory / f"checkpoint-{sequence:04d}.check"
                with os.fdopen(fd, "rb") as source:
                    target_fd = os.open(
                        path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
                    )
                    with os.fdopen(target_fd, "wb") as target:
                        while chunk := source.read(1024**2):
                            size += len(chunk)
                            if size > 1024**3:
                                raise ValueError("checkpoint evidence exceeds 1 GiB")
                            target.write(chunk)
                            digest.update(chunk)
                        target.flush()
                        os.fsync(target.fileno())
                self.record(
                    dict(
                        kind="checkpoint_archive",
                        checkpoint_sequence=sequence,
                        exit_save=exit_save,
                        bytes=size,
                        sha256=digest.hexdigest(),
                        file=path.name,
                    )
                )
        except BaseException as exc:
            self.failure = type(exc).__name__
            self.stop.set()
        finally:
            with self.fd_lock:
                while True:
                    try:
                        fd, _, _ = self.jobs.get_nowait()
                    except queue.Empty:
                        break
                    os.close(fd)

    def check(self):
        if self.failure:
            raise RuntimeError(self.failure)

    def close(self, deadline_ns):
        import time

        for name, original in reversed(self.originals):
            setattr(self.manager, name, original)
        self.stop.set()
        self.thread.join(max(0, min(2, (deadline_ns - time.monotonic_ns()) / 1e9)))
        if self.thread.is_alive():
            raise TimeoutError("checkpoint archive final unknown")
        self.check()

    def release(self):
        if self.thread.is_alive():
            raise RuntimeError("archive references still active")
        pending = list(self.pending)
        self.pending.clear()
        self.main = self.backup = None
        self.originals.clear()
        return dict(pending_sequences=pending, retained_main_backup=0)

    def verify_final(self, check):
        results = []
        for path, version in [
            (self.manager.checkpoint_file, self.main),
            (self.manager.backup_file, self.backup),
        ]:
            check()
            if version is None:
                raise ValueError("checkpoint root link unknown")
            results.append(checkpoint_equivalence(path, version))
        return results
