"""Persistent W/R/C/Q process entry points and single-attempt transport boundary."""

from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import threading
import json
import os
import queue
import time


ROLE_CAPS = dict(W=250, R=16, C=2, Q=4)


def execute(job, transport, emit, clock=time.monotonic_ns):
    """Execute only after a worker is available. No queue time is called sent."""
    sent = False

    def boundary():
        nonlocal sent
        now = clock()
        if sent:
            raise ValueError("transport attempted retry")
        if now >= job["expires_ns"]:
            raise TimeoutError("unsent slot expired")
        sent = True
        emit(dict(kind="sent", id=job["id"], at_ns=now))

    try:
        if clock() >= job["expires_ns"]:
            emit(dict(kind="not_sent", id=job["id"], reason="expired", at_ns=clock()))
            return
        result = transport.invoke(
            job, boundary, 60 if job["channel"] == "verify" else 10
        )
        if not sent:
            raise ValueError("transport returned without sending")
        outcome = "success" if result.get("flag", 0) == 0 else "error"
        emit(
            dict(
                kind="completed",
                id=job["id"],
                at_ns=clock(),
                outcome=outcome,
                result=result,
                channel=job["channel"],
            )
        )
    except Exception as exc:
        if not sent and isinstance(exc, TimeoutError):
            emit(dict(kind="not_sent", id=job["id"], reason="expired", at_ns=clock()))
        else:
            # exception text may contain credentials: only the class is emitted.
            emit(
                dict(
                    kind="completed" if sent else "transport_failure",
                    id=job["id"],
                    at_ns=clock(),
                    outcome="timeout" if isinstance(exc, TimeoutError) else "unknown",
                    error_type=type(exc).__name__,
                    channel=job["channel"],
                )
            )


def role_worker(role, incoming, outgoing, factory, stop, observation_socket=None):
    os.setsid()
    os.environ.update(LOGNAME="perf-" + role, USER="perf-" + role)
    import gc

    fresh_gc = dict(enabled=gc.isenabled(), threshold=gc.get_threshold())
    transport = factory(role)
    observation = None
    if observation_socket:
        from r3_14_observation import Emitter

        observation = Emitter(observation_socket, 8192)
    futures = set()
    produced = emitted = dropped = 0
    lock = threading.Lock()

    def emit(event):
        nonlocal produced, emitted, dropped
        if observation is not None and event["kind"] == "completed":
            channel = {
                "child": 7,
                "rotation": 8,
                "batch": 9,
                "structure": 10,
                "since": 3,
                "detail": 4,
                "status": 6,
            }.get(event.get("channel"), 0)
            observation.emit(
                20, a=event["id"], b=channel, c=int(event["outcome"] != "success")
            )
        with lock:
            produced += 1
            event = dict(event, role=role, producer_pid=os.getpid(), sequence=produced)
            try:
                if len(json.dumps(event)) > 16384:
                    raise queue.Full
                outgoing.put_nowait(event)
                emitted += 1
            except queue.Full:
                dropped += 1
                stop.set()

    emit(
        dict(
            kind="role_ready",
            pid=os.getpid(),
            fresh_gc=fresh_gc,
            ready_gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold()),
        )
    )
    if hasattr(transport, "rpc_sink"):
        transport.rpc_sink = emit
    executors = {}
    try:
        while not stop.is_set():
            futures = {f for f in futures if not f.done()}
            if len(futures) >= ROLE_CAPS[role]:
                time.sleep(0.001)
                continue
            try:
                job = incoming.get(timeout=0.02)
            except queue.Empty:
                continue
            if job is None:
                break
            if job.get("command") == "clock":
                emit(
                    dict(
                        kind="clock",
                        nonce=job["nonce"],
                        pid=os.getpid(),
                        at_ns=time.monotonic_ns(),
                        role=role,
                    )
                )
                continue
            key = job["channel"] if role in ("C", "Q") else role
            if key not in executors:
                executors[key] = ThreadPoolExecutor(
                    max_workers=1 if role in ("C", "Q") else ROLE_CAPS[role],
                    thread_name_prefix="perf04-" + key,
                )
            futures.add(executors[key].submit(execute, job, transport, emit))
    finally:
        for executor in executors.values():
            executor.shutdown(wait=True)
    close = getattr(transport, "close", None)
    if close:
        close()
    emit(
        dict(
            kind="role_final",
            role=role,
            pid=os.getpid(),
            produced=produced,
            emitted=emitted,
            dropped=dropped,
            capacity=256,
            max_record_bytes=16384,
            observation=None if observation is None else observation.close(),
        )
    )


class ProcessRoles:
    """Exactly four business roles. Registry/watchdog supplied by the caller.

    The factory is injectable: tests use non-network transports. This object
    does not create a Takler service or acquire service execution authorization.
    """

    def __init__(
        self, factory, registry, watchdog, context=None, observation_socket=None
    ):
        watchdog.arm()  # Before semaphores, queues, or any other helper process.
        self.context = context or multiprocessing.get_context("fork")
        self.stop = self.context.Event()
        self.incoming = {r: self.context.Queue(ROLE_CAPS[r]) for r in ROLE_CAPS}
        self.outgoing = self.context.Queue(256)
        self.processes, self.finals = {}, {}
        self.metadata = []
        self.received = {r: 0 for r in ROLE_CAPS}
        self.sequence = {r: 0 for r in ROLE_CAPS}
        for role in ROLE_CAPS:
            proc = self.context.Process(
                target=role_worker,
                args=(
                    role,
                    self.incoming[role],
                    self.outgoing,
                    factory,
                    self.stop,
                    observation_socket,
                ),
                name="PERF-04-" + role,
            )
            registry.start(proc, role)
            self.processes[role] = proc

    def submit(self, job):
        try:
            self.incoming[job["role"]].put_nowait(job)
            return True
        except queue.Full:
            return False

    def poll(self, now=None):
        result = []
        for _ in range(512):
            try:
                event = self.outgoing.get_nowait()
            except queue.Empty:
                break
            role = event["role"]
            if event["sequence"] != self.sequence[role] + 1:
                raise RuntimeError("critical role sequence loss")
            self.sequence[role] = event["sequence"]
            if event["kind"] == "role_final":
                self.finals[event["role"]] = event
            else:
                self.received[role] += 1
                if event["kind"] in ("role_ready", "clock"):
                    if len(self.metadata) >= 32:
                        raise RuntimeError("role metadata capacity exceeded")
                    self.metadata.append(event)
                else:
                    result.append(event)
        if self.stop.is_set():
            raise RuntimeError("role event channel loss")
        return result

    def clock_handshake(self, stage, lifecycle, guard):
        import uuid

        nonce = uuid.uuid4().hex
        before = time.monotonic_ns()
        for role, channel in self.incoming.items():
            channel.put_nowait(dict(command="clock", nonce=nonce))

        def replies():
            data = self.poll()
            if data:
                raise RuntimeError("business requests active during clock handshake")
            return [
                e for e in self.metadata if e["kind"] == "clock" and e["nonce"] == nonce
            ]

        lifecycle.wait(lambda: len(replies()) == 4, guard)
        after = time.monotonic_ns()
        return {
            e["role"]: dict(
                stage=stage,
                pid=e["pid"],
                before_ns=before,
                remote_ns=e["at_ns"],
                after_ns=after,
                uncertainty_ns=after - before,
                secondary_usable=after - before <= 1_000_000,
            )
            for e in replies()
        }

    def audit(self):
        problems = []
        for role in ROLE_CAPS:
            final = self.finals.get(role)
            if final is None:
                problems.append(role + " final unknown")
            elif (
                final["produced"] != final["emitted"]
                or final["emitted"] != self.received[role]
                or final["dropped"]
            ):
                problems.append(role + " counts do not close")
            if final is not None and final.get("observation") is not None:
                obs = final["observation"]
                if (
                    obs["produced"] != obs["emitted"]
                    or obs["dropped"]
                    or not obs["sender_stopped"]
                ):
                    problems.append(role + " observation loss")
        return dict(problems=problems, received=self.received, finals=self.finals)

    def request_stop(self):
        for channel in self.incoming.values():
            try:
                channel.put_nowait(None)
            except queue.Full:
                self.stop.set()

    def close_queues(self):
        for channel in [*self.incoming.values(), self.outgoing]:
            channel.cancel_join_thread()
            channel.close()


class GrpcSingleAttempt:
    """Thread-local normal transport with retries=0 and private proof handoff.

    A live caller supplies an endpoint and writer-side proof/identity IPC.
    This class never launches a service and is tested with substituted stubs.
    """

    def __init__(self, client_factory, proof_handoff, rpc_sink=lambda row: None):
        import threading

        self.local = threading.local()
        self.client_factory, self.proof_handoff = client_factory, proof_handoff
        self.rpc_sink = rpc_sink
        self.clients = []
        self.clients_lock = threading.Lock()

    def invoke(self, job, boundary, timeout):
        from takler.protocol.commands import Command
        from takler.server.protocol import adapter
        from takler.client.query_client import QueryClient

        if not hasattr(self.local, "client"):
            self.local.client = self.client_factory(job["role"])
            with self.clients_lock:
                self.clients.append(self.local.client)
            self.local.query = QueryClient(self.local.client.transport)
            self.local.view = None
        client = self.local.client
        transport = client.transport
        if transport.retry_window != 0 or transport.single_timeout > 10:
            raise ValueError("PERF-04 requires zero retry and <=10s RPC timeout")
        # Wrap the actual gRPC callable, after encoding and credential construction.
        original_call = transport._call
        original_query = transport.query
        marked = False
        deadline_ns = None
        ordinal = 0

        def mark():
            nonlocal marked, deadline_ns
            if not marked:
                boundary()
                marked = True
                deadline_ns = min(
                    time.monotonic_ns() + int(timeout * 1e9),
                    job.get("rpc_deadline_ns", 2**63 - 1),
                )

        def actual_rpc(method, rpc, *args, **kwargs):
            nonlocal ordinal
            mark()
            started = time.monotonic_ns()
            left = (deadline_ns - started) / 1e9
            if left <= 0:
                raise TimeoutError("logical request deadline")
            kwargs["timeout"] = min(timeout, left, kwargs.get("timeout", timeout))
            ordinal += 1
            outcome = "unknown"
            try:
                response = rpc(*args, **kwargs)
                outcome = "success" if getattr(response, "flag", 0) == 0 else "error"
                return response
            finally:
                self.rpc_sink(
                    dict(
                        kind="rpc",
                        id=job["id"],
                        ordinal=ordinal,
                        method=method,
                        started_ns=started,
                        at_ns=time.monotonic_ns(),
                        outcome=outcome,
                    )
                )

        def call(operation, rpc, request, kind):
            def actual(*args, **kwargs):
                return actual_rpc(operation, rpc, *args, **kwargs)

            return original_call(operation, actual, request, kind)

        # Query's direct callable is a stub method; use a per-thread transport.
        old_methods = {}
        for name in (
            "GetCapabilities",
            "ReadPage",
            "ReadSince",
            "ReadDetail",
            "ReadDetailChunk",
            "ReadFullSnapshot",
            "ReadFullSnapshotChunk",
            "CancelFullSnapshot",
        ):
            rpc = getattr(transport.query_stub, name)
            old_methods[name] = rpc

            def wrapped(*args, _rpc=rpc, _name=name, **kwargs):
                return actual_rpc(_name, _rpc, *args, **kwargs)

            setattr(transport.query_stub, name, wrapped)
        transport._call = call
        try:
            op = job["operation"]
            if job["channel"] == "verify":
                from r3_14_perf04_verify import stream_fixed_view

                deadline = time.monotonic_ns() + int(min(60, timeout) * 1e9)
                result = stream_fixed_view(
                    transport,
                    deadline,
                    lambda: None,
                    lambda record: self.rpc_sink(
                        dict(kind="verification_node", id=job["id"], record=record)
                    ),
                )
                return dict(flag=0, verification=result)
            if job["channel"] in ("since", "detail"):
                query = self.local.query
                if self.local.view is None:
                    self.local.view = query.read_snapshot()
                if op == "since":
                    from takler.exceptions import QueryResponseError

                    try:
                        self.local.view = query.read_since(self.local.view)
                    except QueryResponseError as exc:
                        if exc.code != "reset_required":
                            raise
                        # Public delta reset handshake, not a transport retry.
                        self.rpc_sink(
                            dict(
                                kind="query_reset",
                                id=job["id"],
                                at_ns=time.monotonic_ns(),
                            )
                        )
                        self.local.view = query.read_snapshot()
                    return dict(flag=0)
                from takler.protocol.query_v1 import DetailRequest

                view = self.local.view
                expected = job["expected_identity"]
                request = DetailRequest(
                    kind="detail_request",
                    schema_version=1,
                    session_id=view.session_id,
                    query_id=view.query_id,
                    path=job["payload"]["path"],
                    flow_generation=expected["generation"],
                    attempt_id=expected["attempt"],
                    field_groups=["runtime"],
                )
                reply = transport.query(request)
                query._failure(reply)
                if (
                    reply.path != request.path
                    or reply.attempt_id != request.attempt_id
                    or reply.flow_generation != request.flow_generation
                ):
                    raise ValueError("detail identity mismatch")
                return dict(flag=0)
            code = Command.SERVER_STATUS if op == "status" else Command(op)
            # Child passwords are request-local, never environment mutation.
            if job["channel"] == "child":
                payload = adapter.request_to_pb2(code, job["payload"])
                rpc = getattr(transport.stub, adapter.GRPC_METHOD_BY_COMMAND[code])
                metadata = [("takler-pass", job["credentials"]["password"])]
                response = actual_rpc(
                    op, rpc, payload, timeout=timeout, metadata=metadata
                )
                reply = adapter.response_from_pb2(code, response)
            else:
                reply = transport.call(code, job["payload"])
            result = reply.model_dump(mode="json")
            if (
                job["channel"] in ("child", "rotation", "batch", "structure")
                and not reply.flag
            ):
                # Normal RPC reply does not expose passwords. Only this private
                # bounded writer handoff can release a task into a new attempt.
                result.update(
                    self.proof_handoff(
                        job,
                        min(timeout, max(0, (deadline_ns - time.monotonic_ns()) / 1e9)),
                    )
                )
            return result
        finally:
            transport._call = original_call
            transport.query = original_query
            for name, rpc in old_methods.items():
                setattr(transport.query_stub, name, rpc)

    def close(self):
        for client in self.clients:
            client.close_channel()
        self.clients.clear()
