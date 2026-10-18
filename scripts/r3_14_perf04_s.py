"""Concrete S closure, exercised with normal handlers and substituted wire stubs.

The CLI exposes ONLY memory mode. It cannot start a Takler listener. The clock,
wire and role channel substitutions are explicit in every result; this is not S
service evidence. No batch/structure or wider batch runner is assembled here.
"""

import argparse
import asyncio
from collections import Counter, deque
import gc
import hashlib
import json
import os
from pathlib import Path
import socket
import tempfile
import time
from types import SimpleNamespace

from r3_14_perf04 import NS, Pool
from r3_14_perf04_fixture import make_fixture, prepare_pool, check_limits
from r3_14_perf04_ledger import Journal, audit_producer, audit_s_window, SequenceWindow
from r3_14_perf04_roles import GrpcSingleAttempt, execute, public_event
from r3_14_perf04_runtime import Engine
from r3_14_perf04_safety import Registry, Watchdog, Perf04Health, Guard, ResourceSampler
from r3_14_perf04_session import Session
from r3_14_perf04_verify import (
    CheckpointAudit,
    WriteVerifier,
    RecordAssembler,
    fixed_view,
    verify_view,
    canonical,
    install_l_observation,
)


class MemoryRoles:
    """Bounded zero-delay role-channel model, NOT four OS role processes.

    Each actual GrpcSingleAttempt executes only in poll, through normal protobuf
    codecs and handlers. Fragments are consumed immediately at the receiver;
    ordinary completion messages remain bounded and may be reordered by tests.
    """

    simulation = True

    def __init__(self, owner):
        self.owner = owner
        self.jobs = deque()
        self.events = deque()
        self.delayed_callbacks = []
        self.sequence = Counter()
        self.received_sequence = Counter()
        self.produced = Counter()
        self.finals = {}
        self.handshakes = {}
        self.metadata = []
        self.workers = {
            r: GrpcSingleAttempt(
                owner.client,
                owner.handoff,
                lambda e, r=r: self.emit(r, e),
                clock=owner.clock,
                check=owner.role_check,
            )
            for r in "WRCQ"
        }
        self.q_workers = {"verify": self.workers["Q"]}
        self.q_workers.update(
            {
                channel: GrpcSingleAttempt(
                    owner.client,
                    owner.handoff,
                    lambda e: self.emit("Q", e),
                    clock=owner.clock,
                    check=owner.role_check,
                )
                for channel in ("since", "detail", "status")
            }
        )
        for role in self.workers:
            self.emit(
                role,
                dict(
                    kind="role_ready",
                    at_ns=owner.clock(),
                    fresh_gc=None,
                    ready_gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold()),
                    execution="in_process_channel_model",
                    pid=os.getpid(),
                ),
            )

    def emit(self, role, event):
        self.sequence[role] += 1
        row = dict(event, role=role, sequence=self.sequence[role])
        if len(canonical(public_event(row))) > 16384:
            raise RuntimeError("role event exceeds unchanged 16 KiB bound")
        if (
            self.owner.fault == "missing_final"
            and role == "Q"
            and event["kind"] == "role_final"
        ):
            return
        if self.owner.fault == "event_loss" and role == "W" and row["sequence"] == 3:
            return
        if row["sequence"] != self.received_sequence[role] + 1:
            raise RuntimeError("critical role sequence loss")
        self.received_sequence[role] = row["sequence"]
        self.owner.record(dict(kind="role_event", event=public_event(row)))
        if event["kind"] == "role_final":
            if self.owner.fault != "missing_final" or role != "Q":
                self.finals[role] = row
            return
        self.produced[role] += 1
        if event["kind"] in ("clock", "role_ready"):
            if len(self.metadata) >= 32:
                raise RuntimeError("role metadata overflow")
            self.metadata.append(row)
        elif event["kind"] == "verification_fragment":
            self.owner.fragment(row)
        else:
            if len(self.events) >= 256:
                raise RuntimeError("role event overflow")
            self.events.append(row)

    def submit(self, job):
        if (
            len(self.jobs) >= 256
            or sum(j["role"] == job["role"] for j in self.jobs)
            >= {"W": 250, "R": 16, "C": 2, "Q": 4}[job["role"]]
        ):
            return False
        self.jobs.append(job)
        return True

    def poll(self, now=None):
        while self.jobs:
            job = self.jobs.popleft()
            if self.owner.fault == "child_stall" and job["channel"] == "child":
                # Sent and retained pending; successful Q cannot repair this.
                self.emit(
                    "W", dict(kind="sent", id=job["id"], at_ns=self.owner.clock())
                )
                continue
            if self.owner.fault == "expired" and job["channel"] == "child":
                self.emit(
                    "W",
                    dict(
                        kind="not_sent",
                        id=job["id"],
                        at_ns=self.owner.clock(),
                        reason="expired",
                    ),
                )
                continue

            def received(event):
                if self.owner.fault == "reordered" and event["kind"] == "completed":
                    self.delayed_callbacks.append((job["role"], event))
                else:
                    self.emit(job["role"], event)

            execute(
                job,
                self.q_workers[job["channel"]]
                if job["role"] == "Q"
                else self.workers[job["role"]],
                received,
                self.owner.clock,
            )
        for role, event in reversed(self.delayed_callbacks):
            self.emit(role, event)
        self.delayed_callbacks.clear()
        result = list(self.events)
        self.events.clear()
        for e in result:
            if e["kind"] == "completed":
                self.owner.health.accept(e)
        return result

    def handshake(self, stage, deadline, check):
        check()
        if self.owner.clock() >= deadline or self.jobs or self.events:
            raise RuntimeError("handshake phase/pending mismatch")
        before = self.owner.clock()
        nonce = stage + "-S"
        for role in self.workers:
            if (
                self.owner.fault == "missing_handshake"
                and stage == "exit"
                and role == "C"
            ):
                continue
            self.emit(role, dict(kind="clock", nonce=nonce, at_ns=self.owner.clock()))
        replies = {e["role"]: e for e in self.metadata if e.get("nonce") == nonce}
        self.handshakes[stage] = dict(
            before_ns=before, after_ns=self.owner.clock(), replies=replies
        )
        self.owner.record(
            dict(kind="role_handshake", stage=stage, **self.handshakes[stage])
        )
        if set(replies) != set("WRCQ"):
            raise RuntimeError("role handshake missing")

    def close(self):
        if self.jobs or self.events:
            raise RuntimeError("role pending at close")
        for worker in self.q_workers.values():
            worker.close()
        for role, worker in self.workers.items():
            worker.close()
            self.emit(
                role,
                dict(
                    kind="role_final",
                    at_ns=self.owner.clock(),
                    produced=self.produced[role],
                    emitted=self.produced[role],
                    dropped=0,
                ),
            )
        return dict(finals=self.finals, handshakes=self.handshakes)


class MemoryWire:
    """Normal client codecs -> substituted stubs -> normal in-memory handlers."""

    def __init__(self, owner):
        from takler.client.grpc_transport import GrpcTransport
        from takler.server.protocol import adapter
        from takler.server.protocol.query_adapter import query_from_pb2, query_to_pb2

        self.transport = GrpcTransport(single_timeout=10, retry_window=0)
        self.transport.stub = SimpleNamespace()
        for code, name in adapter.GRPC_METHOD_BY_COMMAND.items():

            def rpc(raw, _code=code, **kwargs):
                owner.role_check()
                request = adapter.request_from_pb2(_code, raw)
                if _code.value in ("init", "event", "meter", "complete"):
                    task = owner.scheduler.bunch.find_node(request.node_path)
                    if (
                        dict(kwargs.get("metadata", ())).get("takler-pass")
                        != task.job_password
                    ):
                        raise ValueError("simulated child password mismatch")
                reply = owner.loop.run_until_complete(
                    owner.handler.handle(_code, request)
                )
                return adapter.response_to_pb2(reply)

            setattr(self.transport.stub, name, rpc)
        methods = dict(
            GetCapabilities="capabilities",
            ReadPage="page",
            ReadSince="since",
            ReadDetail="detail",
            ReadDetailChunk="chunk",
            ReadFullSnapshot="full_page",
            ReadFullSnapshotChunk="full_chunk",
            CancelFullSnapshot="full_cancel",
        )
        self.transport.query_stub = SimpleNamespace()
        for name, method in methods.items():

            def query(raw, _method=method, **kwargs):
                owner.role_check()
                request = query_from_pb2(raw)
                service = owner.scheduler.query_service
                if _method == "capabilities":
                    reply = service.capabilities()
                else:
                    target = {
                        "full_page": service.full_snapshots.page,
                        "full_chunk": service.full_snapshots.chunk,
                        "full_cancel": service.full_snapshots.cancel,
                    }.get(_method)
                    target = target or getattr(service, _method)
                    if (
                        owner.fault == "identity_race"
                        and _method == "detail"
                        and owner.session.engine is not None
                    ):
                        # Controlled fault: finish this attempt normally, then
                        # commit R before the delayed Q request reads live state.
                        # Extra writes are fault-injection records, never W slots.
                        from takler.protocol.commands import (
                            Command,
                            REQUEST_TYPE_BY_COMMAND,
                        )
                        from r3_14_perf04_fixture import child_payload

                        path = request.path
                        stage = owner.verifier.stages[path]
                        for step in range(stage, 12):
                            task = owner.bunch.find_node(path)
                            if step < 10:
                                operation, payload = child_payload(task, step)
                            else:
                                operation = "requeue" if step == 10 else "run"
                                payload = dict(node_paths=[path])
                                if step == 11:
                                    payload["force"] = False
                            code = Command(operation)
                            reply_fault = owner.loop.run_until_complete(
                                owner.handler.handle(
                                    code, REQUEST_TYPE_BY_COMMAND[code](**payload)
                                )
                            )
                            if reply_fault.flag:
                                raise ValueError("fault setup rejected")
                            owner.verifier.handoff(dict(path=path))
                            owner.record(
                                dict(
                                    kind="fault_injection",
                                    operation=operation,
                                    path=path,
                                )
                            )
                    reply = owner.loop.run_until_complete(target(request))
                return query_to_pb2(reply)

            setattr(self.transport.query_stub, name, query)

    def close_channel(self):
        self.transport.close()


class SClosure:
    """Concrete, non-network S assembly. No unimplemented setup callbacks."""

    def __init__(self, directory, fault=None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=False)
        self.fault = fault
        self.started_wall = time.monotonic_ns()
        self.now = self.started_wall
        self.loop = asyncio.new_event_loop()
        self.journal = Journal(self.directory / "events.jsonl")
        self.registry = Registry(self.directory / "processes.json")
        self.watchdog = Watchdog(self.registry, self.started_wall + 320 * NS)
        self.watchdog.arm()
        self.health = Perf04Health(
            self.directory / "health.json", "PERF-04-S-memory", os.getpid()
        )
        self.sampler = ResourceSampler(
            self.registry, self.directory, os.getpid(), clock=self.clock
        )
        self.sample = None
        self.guard = Guard(
            self.directory / "health.json",
            "PERF-04-S-memory",
            os.getpid(),
            self.now,
            self.collector_poll,
            self.resources,
            clock=self.clock,
        )
        self.session = Session(
            "S",
            self.registry,
            self.watchdog,
            self.guard,
            self.record,
            clock=self.clock,
            sleep=self.advance,
            step_seconds=0.005,
        )
        self.roles = self.audit = self.verifier = self.observation = self.emitter = None
        self.receiver = None
        self.stream = None
        self.verified = {}
        self.saved = []
        self.periodic_at = None
        self.collector_closed = False
        self.next_health = 0
        self.next_check = None
        self.ownership = dict(
            known=False,
            method="whole-process writable-data/stack reservation upper bound; no RSS attribution",
            max_process_rss=0,
            max_writable_bytes=0,
            service_ownership_known=False,
            max_fixed_bytes=0,
        )
        self.publish_health()
        self.guard.swap_start = self.resources()["swap_used"]

    def clock(self):
        return self.now

    def record(self, row):
        self.journal.emit(row)

    def collector_poll(self):
        return 1 if self.fault == "collector_exit" and self.session.engine else None

    def publish_health(self):
        if self.fault != "health_loss" or not self.session.engine:
            self.health.publish(self.now)
        self.next_health = self.now + 100_000_000

    def resources(self):
        if self.journal.failure:
            raise RuntimeError("critical journal writer failure")
        if self.sample is None or self.now >= self.sampler.next_ns:
            self.sample = self.sampler.tick(
                lambda s: self.record(
                    dict(kind="resource", sample=s, logical_ns=self.now)
                )
            )
        return self.sample

    def ownership_sample(self):
        from r3_14_isolated_worker import proc_sample

        sample = proc_sample(os.getpid())
        rss = sample.get("rss_bytes")
        roots = [self.scheduler.version_store.current]
        if self.audit:
            roots += [self.audit.main, self.audit.backup]
            roots += [v[0] for v in self.audit.pending.values()]
        roots += [
            lease.version
            for lease in self.scheduler.query_service.full_snapshots._leases.values()
        ]
        fixed = sum(
            v.retained_bytes
            for v in {id(v): v for v in roots if v is not None}.values()
        )
        self.ownership["max_fixed_bytes"] = max(
            self.ownership["max_fixed_bytes"], fixed
        )
        status = {
            line.split(":")[0]: int(line.split()[1]) * 1024
            for line in Path("/proc/self/status").read_text().splitlines()
            if line.startswith(("VmData:", "VmStk:"))
        }
        bound = None if set(status) != {"VmData", "VmStk"} else sum(status.values())
        if rss is not None:
            self.ownership["max_process_rss"] = max(
                self.ownership["max_process_rss"], rss
            )
        if bound is None:
            self.ownership["unavailable"] = True
        else:
            self.ownership["max_writable_bytes"] = max(
                self.ownership["max_writable_bytes"], bound
            )
        # Only this co-located, non-network fixture is covered. The entire
        # writable heap/stack reservation bounds all its Python client buffers
        # including untouched pages; RSS or an RSS difference cannot do that.
        # If this conservative bound is too large, ownership remains UNKNOWN.
        self.ownership["known"] = (
            not self.ownership.get("unavailable")
            and self.ownership["max_writable_bytes"] <= 128 * 1024**2
        )
        if self.fault == "ownership_unknown":
            self.ownership["known"] = False
        self.record(dict(kind="ownership_bound", logical_ns=self.now, **self.ownership))
        if fixed > 768 * 1024**2:
            raise RuntimeError("fixed-version ownership above 768 MiB")
        if rss is not None and rss > 1536 * 1024**2:
            raise RuntimeError(
                "snapshot upper bound inconclusive and process RSS unsafe"
            )

    def role_check(self):
        self.session.check()
        if self.active_deadline is not None and self.now >= self.active_deadline:
            raise TimeoutError("role absolute phase deadline")

    def handoff(self, job, timeout):
        self.role_check()
        if self.now >= job["rpc_deadline_ns"]:
            raise TimeoutError("private handoff absolute deadline")
        value = self.verifier.handoff(job, timeout)
        if self.fault == "unknown_late" and not hasattr(self, "late"):
            self.late = (job, value)
            raise TimeoutError("injected lost private proof response")
        return value

    def client(self, role):
        return MemoryWire(self)

    def drain_observation(self):
        from r3_14_observation import decode_packet

        try:
            raw = self.socket.recv(65536)
        except BlockingIOError:
            return False
        batch = []
        for event in decode_packet(raw):
            self.observation_window.accept(event["sequence"])
            self.observation_received += 1
            candidate = dict(
                kind="L_packet", events=batch + [event], received_logical_ns=self.now
            )
            if len(canonical(candidate)) > 12000:
                self.record(
                    dict(kind="L_packet", events=batch, received_logical_ns=self.now)
                )
                batch = []
            batch.append(event)
            self.health.accept_legacy(dict(event, at_ns=self.now))
        if batch:
            self.record(
                dict(kind="L_packet", events=batch, received_logical_ns=self.now)
            )
        # This is the collector, not a product producer. Process one bounded
        # packet then yield; never spin through an arbitrary IPC backlog while
        # starving the unchanged nonblocking 256-slot Journal writer.
        time.sleep(0)
        return True

    def drain_final_observation(self):
        deadline = min(self.started_wall + 310 * NS, time.monotonic_ns() + NS)
        for _ in range(8192):
            if time.monotonic_ns() >= deadline:
                raise TimeoutError("L final drain deadline")
            if not self.drain_observation():
                return
        raise RuntimeError("L final drain packet bound")

    def advance(self, seconds):
        self.now += int(seconds * NS)
        self.loop.run_until_complete(asyncio.sleep(0))
        self.drain_observation()
        if self.now >= self.next_health:
            self.publish_health()
        if self.periodic_at is not None and self.now >= self.periodic_at:
            self.save_periodic()
            self.periodic_at = self.now + 10 * NS
        if self.audit:
            self.audit.check()
        if (
            self.fault == "aggregate_stall"
            and self.session.engine
            and not getattr(self, "aggregate_injected", False)
        ):
            self.aggregate_injected = True
            self.now = self.session.engine.grid.start_ns + 60 * NS
            self.health.publish(self.now)  # Fresh trusted state, no progress refresh.

    def setup(self, deadline, check):
        from takler.server.scheduler import Scheduler
        from takler.server.handlers import CommandHandlers
        from takler.server.checkpoint import CheckpointManager

        self.active_deadline = deadline
        self.bunch, self.paths, self.sentinel, _ = make_fixture(1000)
        self.scheduler = Scheduler(self.bunch)
        self.identities = self.loop.run_until_complete(
            prepare_pool(
                self.scheduler,
                self.paths,
                self.sentinel,
                record=self.record,
                check=check,
            )
        )
        if self.fault == "stage_deadline":
            self.now = deadline
            check()
        self.handler = CommandHandlers(self.scheduler)
        self.verifier = WriteVerifier(
            self.handler, self.paths, self.sentinel, self.record
        ).install()
        self.manager = CheckpointManager(
            self.bunch, checkpoint_file=self.directory / "state.check", interval=10
        )
        self.manager.query_scheduler = self.scheduler
        # Capture normal synchronous writer before the L wrapper labels exit-save.
        self.periodic_write = self.manager.write_checkpoint
        self.socket_dir = tempfile.TemporaryDirectory(prefix="p04s-")
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.socket.bind(self.socket_dir.name + "/l")
        self.socket.setblocking(False)
        self.observation_received = 0
        self.observation_window = SequenceWindow()

        async def install():
            return install_l_observation(
                SimpleNamespace(
                    scheduler=self.scheduler, checkpoint_manager=self.manager
                ),
                self.socket_dir.name + "/l",
            )

        self.observation, self.emitter = self.loop.run_until_complete(install())
        archives = self.directory / "checkpoints"
        archives.mkdir()
        self.audit = CheckpointAudit(self.manager, archives, self.record).install()
        self.roles = MemoryRoles(self)
        self.roles.handshake("start", deadline, check)
        initial = fixed_view(self.scheduler.version_store.pin())
        self.sentinel_record = initial["nodes"][self.sentinel]
        self.ownership_sample()
        self.periodic_at = self.now + 10 * NS
        self.save_periodic()
        check()
        return self.identities

    def save_periodic(self):
        self.role_check()
        prepared_path = self.manager._prepared_path
        if self.fault == "save_failure" and self.session.engine:
            # A missing directory is normally created by the checkpoint writer.
            # A regular file in its place forces a real preparation failure.
            blocked = self.directory / "blocked-parent"
            if not blocked.exists():
                with blocked.open("xb") as stream:
                    stream.write(b"controlled checkpoint failure fixture")
            self.manager._prepared_path = lambda seq: blocked / f"{seq}.prepare"
        try:
            success = self.periodic_write()
        finally:
            self.manager._prepared_path = prepared_path
        if not success:
            raise RuntimeError("periodic save failed")
        self.saved.append(dict(kind="periodic", at_ns=self.now))
        self.record(dict(kind="periodic_saved", at_ns=self.now))
        self.health.accept(
            dict(
                kind="checkpoint_publish",
                at_ns=self.now,
                success=True,
                published=True,
                exit_save=False,
            )
        )

    def fragment(self, event):
        self.role_check()
        if self.receiver is None:
            raise RuntimeError("verification fragment outside verification")
        if self.fault == "fragment_loss" and event["record_ordinal"] == 1:
            return
        record = self.receiver.accept(event)
        if record is not None:
            self.ownership_sample()
            expected = self.expected_nodes[record["path"]]
            runtime = record["groups"].get("runtime", {})
            from takler.core import NodeStatus

            if (
                record["child_paths"] != expected["child_paths"]
                or record["parent_path"] != expected["parent_path"]
                or record["summary"]["status"]
                != NodeStatus(expected["state"]["status"]).name
                or record["summary"].get("attempt_id") != expected.get("attempt_id")
                or runtime.get("limit_usage", {}) != expected["limit_usage"]
            ):
                raise ValueError("transferred fixed view/model mismatch")
            for key in (
                "try_no",
                "task_id",
                "submission_unknown_reason",
                "events",
                "meters",
            ):
                if runtime.get(key) != expected.get(key):
                    raise ValueError("transferred runtime mismatch: " + key)
            self.stream.write(canonical(record) + b"\n")
            self.stream_digest.update(canonical(record) + b"\n")
        self.drain_observation()

    def verify(self, stage, deadline, check):
        check()
        self.active_deadline = min(deadline, self.now + 60 * NS)
        self.receiver = RecordAssembler()
        self.stream_digest = hashlib.sha256()
        engine = self.session.engine
        model = engine.pool if engine else Pool(self.paths, self.identities)
        # Boundary full root comparison is separate from the streamed public view.
        view = fixed_view(self.scheduler.version_store.pin())
        fixed_path = self.directory / (stage + "-fixed.json")
        fixed_path.write_bytes(canonical(view))
        verify_view(view, pool=model, sentinel=(self.sentinel, self.sentinel_record))
        check_limits(self.bunch)
        self.expected_nodes = view["nodes"]
        if len(view["nodes"]) != 1002:
            raise ValueError("S size mismatch")
        with (self.directory / (stage + "-full.jsonl")).open("wb") as stream:
            self.stream = stream
            job = dict(
                id=-1 if stage == "before" else -2,
                role="Q",
                channel="verify",
                operation="full_verify",
                expires_ns=self.active_deadline,
                rpc_deadline_ns=self.active_deadline,
                payload={},
            )
            assert self.roles.submit(job)
            events = self.roles.poll()
            done = [e for e in events if e["kind"] == "completed"]
            if len(done) != 1 or done[0]["outcome"] != "success":
                raise ValueError("full verification business failure")
            result = done[0]["result"]["verification"]
            self.receiver.finish(result["count"])
            if (
                result["count"] != 1002
                or result["semantic_sha256"] != self.stream_digest.hexdigest()
            ):
                raise ValueError("full streamed count/digest mismatch")
            if self.scheduler.query_service.full_snapshots._leases:
                raise ValueError("full verification lease retained")
            self.verified[stage] = dict(
                **result, fixed_sha256=view["sha256"], state_correct=True
            )
            self.record(
                dict(kind="fixed_verification", stage=stage, **self.verified[stage])
            )
        self.stream = None
        self.receiver = None
        self.expected_nodes = None
        self.active_deadline = None
        check()

    def make_engine(self, start, identities):
        self.active_deadline = None
        engine = Engine(
            "S", start, self.paths, identities, self.roles, self.record, self.guard
        )
        if self.fault == "busy":
            for task in engine.pool.tasks:
                task.busy = True
            self.record(
                dict(kind="fault_injection", reason="held model pool reservations")
            )
        engine.phase_deadline_ns = min(
            start + 90 * NS, self.session.lifecycle.operational_ns
        )
        return engine

    def shutdown(self, deadline, check, expect_collector_stop):
        self.periodic_at = None
        self.active_deadline = deadline
        self.roles.handshake("exit", deadline, check)
        self.role_final = self.roles.close()
        if not self.manager.write_checkpoint():
            raise RuntimeError("exit save failed")
        self.saved.append(dict(kind="exit", at_ns=self.now))
        self.audit.close(
            min(time.monotonic_ns() + 2 * NS, self.started_wall + 310 * NS)
        )
        self.final_checkpoints = self.audit.verify_final(check)
        self.audit_release = self.audit.release()
        self.proof_release = self.verifier.close()
        self.loop.run_until_complete(self.observation.close())
        self.observation = None
        self.emitter_final = self.emitter.close()
        self.drain_final_observation()
        expect_collector_stop()
        self.collector_closed = True
        self.socket.close()
        self.socket_dir.cleanup()
        self.watchdog.disarm(time.monotonic_ns() + NS)
        self.registry.reap()
        self.active_deadline = None
        check()

    def run(self):
        self.active_deadline = None
        cleanup_errors = []
        cleanup_deadline = None
        result = None

        def cleanup(name, action):
            if cleanup_deadline is not None and time.monotonic_ns() >= cleanup_deadline:
                cleanup_errors.append(dict(step=name, error_type="cleanup_deadline"))
                return None
            try:
                return action()
            except Exception as exc:
                item = dict(step=name, error_type=type(exc).__name__)
                cleanup_errors.append(item)
                try:
                    self.record(dict(kind="cleanup_unknown", **item))
                except Exception:
                    pass  # Journal's actual failure is retained in its final.
                return None

        try:
            result = self.session.run(
                self.setup, self.make_engine, self.verify, self.shutdown
            )
        except Exception as exc:
            result = dict(
                task_id="PERF-04",
                exit_reason="orchestration failure: " + type(exc).__name__,
                workload=None
                if self.session.engine is None
                else self.session.engine.finish(),
                registry=self.registry.summary(),
            )
        finally:
            cleanup_deadline = min(
                self.started_wall + 320 * NS, time.monotonic_ns() + 10 * NS
            )
            if hasattr(self, "late") and self.session.engine is not None:

                def check_late():
                    job, value = self.late
                    before = [dict(vars(t)) for t in self.session.engine.pool.tasks]
                    self.session.engine.accept(
                        dict(
                            kind="completed",
                            id=job["id"],
                            at_ns=self.now + NS,
                            outcome="success",
                            result=dict(flag=0, **value),
                        )
                    )
                    if before != [
                        dict(vars(t)) for t in self.session.engine.pool.tasks
                    ]:
                        raise AssertionError("late success rewrote unknown state")
                    self.record(
                        dict(
                            kind="fault_late_callback_checked",
                            id=job["id"],
                            at_ns=self.now + NS,
                            unknown_preserved=True,
                        )
                    )

                cleanup("late_callback_check", check_late)
            if self.observation:
                cleanup(
                    "L_hooks",
                    lambda: self.loop.run_until_complete(
                        asyncio.wait_for(self.observation.close(), 1)
                    ),
                )
            if self.emitter and not getattr(self, "emitter_final", None):
                self.emitter_final = cleanup("L_sender", self.emitter.close)
                cleanup("L_drain", self.drain_final_observation)
            if self.audit:
                cleanup(
                    "checkpoint_archive",
                    lambda: self.audit.close(
                        min(cleanup_deadline, time.monotonic_ns() + NS)
                    ),
                )
                if not self.audit.thread.is_alive():
                    self.audit_release = cleanup("checkpoint_roots", self.audit.release)
            if self.roles:
                for channel, worker in self.roles.q_workers.items():
                    cleanup("Q_" + channel, worker.close)
                for role, worker in self.roles.workers.items():
                    cleanup(role + "_client", worker.close)
            if hasattr(self, "socket") and not self.collector_closed:
                cleanup("collector_socket", self.socket.close)
                cleanup("collector_directory", self.socket_dir.cleanup)
            if self.verifier and self.handler.dispatch != self.verifier.original:
                self.proof_release = cleanup("private_proofs", self.verifier.close)
            cleanup(
                "default_executor",
                lambda: self.loop.run_until_complete(
                    asyncio.wait_for(
                        self.loop.shutdown_default_executor(timeout=1), 1.5
                    )
                ),
            )
            cleanup("writer_loop", self.loop.close)
            cleanup(
                "owned_processes",
                lambda: self.registry.cleanup(
                    min(cleanup_deadline, time.monotonic_ns() + NS)
                ),
            )
            final = self.journal.close(
                min(cleanup_deadline, time.monotonic_ns() + 2 * NS)
            )
        result["fault_plan"] = self.fault
        result["cleanup_errors"] = cleanup_errors
        result["cleanup_tail"] = (
            "unknown"
            if self.fault == "cleanup_unknown" or cleanup_errors
            else (
                "known" if not self.registry.summary()["live_registered"] else "unknown"
            )
        )
        if self.fault == "cleanup_unknown":
            # Physical helpers were still reclaimed. The simulated missing ack
            # is retained as unknown, rather than fabricating an alive PID.
            result["cleanup_note"] = "injected missing cleanup acknowledgement"
        result["evidence_kind"] = (
            "service_free_virtual_clock_normal_handlers_substituted_stubs"
        )
        result["wall_seconds"] = (time.monotonic_ns() - self.started_wall) / NS
        result["journal_final"] = final
        if self.session.engine is not None:
            result["raw_window_audit"] = audit_s_window(
                self.directory / "events.jsonl", self.session.engine.grid.start_ns
            )
        result["role_audit"] = audit_role_archive(self.directory / "events.jsonl")
        result["journal_audit"] = audit_producer(self.directory / "events.jsonl", final)
        result["ownership"] = self.ownership
        result["verified"] = self.verified
        result["role_final"] = getattr(self, "role_final", None)
        result["saved"] = self.saved
        result["final_checkpoints"] = getattr(self, "final_checkpoints", None)
        result["audit_release"] = getattr(self, "audit_release", None)
        result["proof_release"] = getattr(self, "proof_release", None)
        result["L_final"] = getattr(self, "emitter_final", None)
        result["L_received"] = getattr(self, "observation_received", None)
        result["L_receipts"] = (
            None
            if not self.emitter
            else self.observation_window.final(
                (self.emitter_final or {}).get("produced", -1)
            )
        )
        result["joint"] = s_joint(result)
        (self.directory / "result.json").write_text(json.dumps(result, indent=2))
        return result


def verify_stream_link(result, view):
    if (
        result["completion"]["state_version"] != view["state_version"]
        or result["completion"]["total_nodes"] != len(view["nodes"])
        or result["count"] != len(view["nodes"])
    ):
        raise ValueError("full stream/fixed root version or size mismatch")


def audit_role_archive(path):
    sequences = Counter()
    finals = {}
    clocks = {"start-S": {}, "exit-S": {}}
    problems = []
    with path.open() as stream:
        for line in stream:
            row = json.loads(line)
            if row.get("kind") != "role_event":
                continue
            event = row["event"]
            role = event["role"]
            if event["sequence"] != sequences[role] + 1:
                problems.append(role + " role sequence loss")
            sequences[role] = event["sequence"]
            if event["kind"] == "clock":
                clocks.setdefault(event["nonce"], {})[role] = event
            if event["kind"] == "role_final":
                finals[role] = event
                if (
                    event["produced"] != event["sequence"] - 1
                    or event["produced"] != event["emitted"]
                    or event["dropped"]
                ):
                    problems.append(role + " final counts mismatch")
    if set(finals) != set("WRCQ"):
        problems.append("role final missing")
    if any(set(clocks[stage]) != set("WRCQ") for stage in ("start-S", "exit-S")):
        problems.append("raw handshake missing")
    return dict(
        problems=problems, sequences=dict(sequences), clocks=clocks, finals=finals
    )


def s_joint(result):
    """Evidence conjunction; Session.completed alone never grants a pass."""
    problems = list(
        result.get("role_audit", {}).get("problems", ["raw role audit missing"])
    )
    problems += result.get("raw_window_audit", {}).get(
        "problems", ["raw window audit missing"]
    )
    work = result.get("workload") or {}
    ledger = work.get("ledger", {})
    child = next(
        (
            r["counts"]
            for r in ledger.get("dimensions", [])
            if r["dimension"] == "role" and r["key"] == "W"
        ),
        {},
    )
    if (
        any(
            child.get(k, 0) != 600
            for k in ("offered", "sent", "completed", "effective")
        )
        or child.get("not_sent", 0)
        or child.get("unknown", 0)
    ):
        problems.append("S 600 effective slots not closed")
    if (
        result.get("exit_reason") != "completed"
        or work.get("stopped")
        or ledger.get("pending")
        or not ledger.get("counts_closed")
    ):
        problems.append("stop/pending/counts unknown")
    if result.get("journal_audit", {}).get("tail") != "known":
        problems.append("journal final unknown")
    for stage in ("before", "after"):
        if not result.get("verified", {}).get(stage, {}).get("state_correct"):
            problems.append(stage + " state unknown")
    if not result.get("ownership", {}).get("known"):
        problems.append("necessary ownership unknown")
    roles = result.get("role_final") or {}
    for stage in ("start", "exit"):
        if set(roles.get("handshakes", {}).get(stage, {}).get("replies", {})) != set(
            "WRCQ"
        ):
            problems.append(stage + " handshake unknown")
    for role in "WRCQ":
        row = roles.get("finals", {}).get(role)
        if row is None or row["produced"] != row["emitted"] or row["dropped"]:
            problems.append(role + " producer final unknown")
    if len(result.get("final_checkpoints") or []) != 2 or not any(
        r["kind"] == "periodic" for r in result.get("saved", [])
    ):
        problems.append("save evidence unknown")
    if result.get("cleanup_tail") != "known":
        problems.append("cleanup acknowledgement unknown")
    if result.get("registry", {}).get("live_registered") or not result.get(
        "registry", {}
    ).get("timeline_complete"):
        problems.append("process tail unknown")
    if not result.get("audit_release") or result["audit_release"]["pending_sequences"]:
        problems.append("checkpoint reference tail unknown")
    if not result.get("proof_release") or result["proof_release"]["unconsumed_paths"]:
        problems.append("proof tail unknown")
    final = result.get("L_final") or {}
    if (
        not final
        or not (result.get("L_receipts") or {}).get("known")
        or final.get("produced") != result.get("L_received")
        or final.get("dropped")
        or not final.get("sender_stopped")
    ):
        problems.append("L final unknown")
    return dict(
        tool_conjunction_pass=not problems, problems=problems, service_S_pass=False
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--memory-only", action="store_true", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--fault",
        choices=[
            "expired",
            "child_stall",
            "health_loss",
            "collector_exit",
            "identity_race",
            "missing_final",
            "missing_handshake",
            "fragment_loss",
            "save_failure",
            "busy",
            "event_loss",
            "stage_deadline",
            "cleanup_unknown",
            "ownership_unknown",
            "unknown_late",
            "aggregate_stall",
            "reordered",
        ],
    )
    args = parser.parse_args()
    result = SClosure(args.output, args.fault).run()
    print(
        json.dumps(
            dict(
                joint=result["joint"],
                wall_seconds=result["wall_seconds"],
                exit_reason=result["exit_reason"],
            )
        )
    )


if __name__ == "__main__":
    main()
