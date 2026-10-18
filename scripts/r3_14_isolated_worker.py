"""PERF-03 service, collector and persistent clients in separate processes."""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
from pathlib import Path
import select
import signal
import socket
import sys
import threading
import time

from r3_14_health import publish_health, update_health
from r3_14_observation import (
    CHANNELS,
    MAX_PACKET_RECORDS,
    Emitter,
    RECORD,
    ServiceObservation,
    decode_packet,
)


class IntegrityError(ValueError):
    """A proven fixture/protocol invariant violation, distinct from RPC errors."""


def semantic_failure(exc):
    return type(exc).__name__ in {"IntegrityError", "ServerResponseError"} or getattr(
        exc, "code", None
    ) in {"permission_denied", "invalid_request"}


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def read_commands(fd, buffer):
    chunk = os.read(fd, 65536)
    if not chunk:
        return None
    buffer.extend(chunk)
    if len(buffer) > 65536:
        raise ValueError("control buffer exceeded limit")
    result = []
    while b"\n" in buffer:
        line, _, rest = buffer.partition(b"\n")
        buffer[:] = rest
        result.append(json.loads(line))
    return result


def control():
    return json.loads(sys.stdin.readline())


def handshake():
    request = control()
    print(
        json.dumps(
            dict(
                nonce=request["nonce"],
                monotonic_ns=time.monotonic_ns(),
                pid=os.getpid(),
            )
        ),
        flush=True,
    )


async def service(options):
    from query_a_matrix_100k import make_tree
    from takler.server import TaklerServer
    from takler.server.connect_config import (
        Address,
        ConnectConfig,
        SecuritySettings,
        Server,
    )

    fresh_gc = dict(enabled=gc.isenabled(), threshold=gc.get_threshold())
    config = ConnectConfig(
        server=Server(
            address=Address(
                hostname="127.0.0.1", ip="127.0.0.1", port=str(options.port)
            )
        ),
        security=SecuritySettings(
            auth_mode="enabled",
            operator_secret_file=options.secret,
            operator_whitelist_file=options.whitelist,
        ),
    )
    server = TaklerServer(
        host="127.0.0.1",
        port=options.port,
        connect_config=config,
        checkpoint_file=Path(options.run_dir) / "state.check",
        checkpoint_interval=10,
    )
    server.scheduler.interval_main_loop = 0.05
    bunch, _ = make_tree(options.tasks, False)
    for flow in bunch.flows.values():
        flow.begun = False
        server.bunch.add_flow(flow)
    emitter = observation = None
    if options.mode != "O":
        emitter = Emitter(options.socket, options.capacity)
        if options.perf08a:
            from r3_14_perf08a_hooks import install_capture_link

            install_capture_link(server, emitter)
        observation = ServiceObservation(
            server,
            emitter,
            options.mode == "D",
            lag_mode=options.lag_ablation or "record",
        ).install()
        if options.perf08a:
            from r3_14_perf08a_hooks import install_checkpoint_boundaries

            install_checkpoint_boundaries(observation)
    await server.start()
    run_task = asyncio.create_task(server.run())
    print(
        json.dumps(
            dict(
                ready=True,
                pid=os.getpid(),
                fresh_gc=fresh_gc,
                ready_gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold()),
                tree_tasks=options.tasks,
                tree_flows=len(bunch.flows),
                source=__import__("takler").__file__,
                event_loop_type=type(asyncio.get_running_loop()).__qualname__,
                lag_timer_backend=(
                    None
                    if observation is None or observation.lag_mode == "off"
                    else "owned_timer_reuse"
                    if observation.reuse_lag_timer
                    else "public_call_at"
                ),
                lag=None if observation is None else observation.lag_state(),
            )
        ),
        flush=True,
    )
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()

    input_buffer = bytearray()

    def input_ready():
        messages = read_commands(sys.stdin.fileno(), input_buffer)
        if messages is None:
            stopped.set()
            return
        for request in messages:
            if request.get("command") == "clock":
                print(
                    json.dumps(
                        dict(
                            nonce=request["nonce"],
                            monotonic_ns=time.monotonic_ns(),
                            pid=os.getpid(),
                            gc=dict(
                                enabled=gc.isenabled(), threshold=gc.get_threshold()
                            ),
                            lag=None
                            if observation is None
                            else observation.lag_state(),
                        )
                    ),
                    flush=True,
                )
            elif request.get("command") == "stop":
                stopped.set()

    loop.add_reader(sys.stdin, input_ready)
    for sig in [signal.SIGTERM, signal.SIGINT]:
        loop.add_signal_handler(sig, stopped.set)
    synthetic = None
    if options.synthetic:

        async def produce():
            while True:
                for _ in range(20):
                    emitter.emit(9)
                await asyncio.sleep(0.02)

        synthetic = asyncio.create_task(produce())
    await stopped.wait()
    loop.remove_reader(sys.stdin)
    if synthetic:
        synthetic.cancel()
        await asyncio.gather(synthetic, return_exceptions=True)
    await server.stop()
    await run_task
    if observation:
        await observation.close()
    save(
        Path(options.run_dir) / "server-final.json",
        dict(
            pid=os.getpid(),
            observation=None if emitter is None else emitter.close(),
            lag=None if observation is None else observation.lag_state(),
            gc=dict(enabled=gc.isenabled(), threshold=gc.get_threshold()),
            full_leases=len(server.scheduler.query_service.full_snapshots._leases),
            background_writes=len(server.checkpoint_manager._background_writes),
            background_last_reference="unknown",
            summary_snapshots=len(server.scheduler.query_service._snapshots),
            checkpoint_failure=server.checkpoint_manager.checkpoint_failure_reason,
            checkpoint_last_at=server.checkpoint_manager.last_checkpoint_at,
        ),
    )


def proc_sample(pid, pss=False):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
        rss = int(Path(f"/proc/{pid}/statm").read_text().split()[1]) * os.sysconf(
            "SC_PAGE_SIZE"
        )
        result = dict(
            pid=pid,
            ppid=int(stat[1]),
            rss_bytes=rss,
            cpu_seconds=(int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
            cpu_user_seconds=int(stat[11]) / os.sysconf("SC_CLK_TCK"),
            cpu_system_seconds=int(stat[12]) / os.sysconf("SC_CLK_TCK"),
        )
        if pss:
            rollup = Path(f"/proc/{pid}/smaps_rollup").read_text()
            result["pss_bytes"] = (
                int(
                    next(
                        line.split()[1]
                        for line in rollup.splitlines()
                        if line.startswith("Pss:")
                    )
                )
                * 1024
            )
        return result
    except (OSError, StopIteration, ValueError):
        return dict(pid=pid, unavailable=True)


def descendants(pid):
    result = []
    todo = [pid]
    while todo and len(result) < 64:
        parent = todo.pop()
        try:
            children = [
                int(n)
                for n in Path(f"/proc/{parent}/task/{parent}/children")
                .read_text()
                .split()
            ]
        except OSError:
            continue
        result.extend(children)
        todo.extend(children)
    return result


def collector(options):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
    sock.bind(options.socket)
    sock.setblocking(False)
    print(
        json.dumps(
            dict(
                ready=True,
                pid=os.getpid(),
                receive_buffer_bytes=sock.getsockopt(
                    socket.SOL_SOCKET, socket.SO_RCVBUF
                ),
            )
        ),
        flush=True,
    )
    tracked = {}
    limits_state = {}
    limits = json.loads((Path(options.run_dir) / "config.json").read_text()).get(
        "resource_limits"
    )
    input_buffer = bytearray()
    event_count = 0
    pending = bytearray()
    batch_peak = 0
    stopped = False
    last_sample = last_pss = last_flush = 0
    pause_start = pause_end = 0
    health = dict(last_progress_ns=0, failure=None)
    last_health = 0
    with (
        (Path(options.run_dir) / "events.jsonl").open("ab", buffering=0) as events,
        (Path(options.run_dir) / "resources.jsonl").open(
            "ab", buffering=0
        ) as resources,
    ):
        while True:
            now = time.monotonic()
            readable, _, _ = select.select(
                [sys.stdin, sock]
                if not pause_start <= now < pause_end
                else [sys.stdin],
                [],
                [],
                0.01,
            )
            if sys.stdin in readable:
                messages = read_commands(sys.stdin.fileno(), input_buffer)
                if messages is None:
                    stopped = True
                else:
                    for message in messages:
                        command = message["command"]
                        if command == "track":
                            tracked.update(
                                {
                                    int(pid): role
                                    for pid, role in message["pids"].items()
                                }
                            )
                        elif command == "clock":
                            print(
                                json.dumps(
                                    dict(
                                        nonce=message["nonce"],
                                        monotonic_ns=time.monotonic_ns(),
                                        pid=os.getpid(),
                                    )
                                ),
                                flush=True,
                            )
                        elif command == "pause":
                            pause_start, pause_end = message["start"], message["end"]
                        elif command == "stop":
                            stopped = True
            if sock in readable:
                # Limit one batch per iteration so resource sampling/control progresses.
                for _ in range(8):
                    try:
                        raw = sock.recv(RECORD.size * MAX_PACKET_RECORDS + 1)
                    except BlockingIOError:
                        break
                    for event in decode_packet(raw):
                        event["run_id"] = Path(options.run_dir).name
                        event["role"] = tracked.get(event["pid"], "unknown")
                        line = json.dumps(event, separators=(",", ":")).encode() + b"\n"
                        if len(pending) + len(line) > 1048576:
                            events.write(pending)
                            pending.clear()
                        pending.extend(line)
                        batch_peak = max(batch_peak, len(pending))
                        event_count += 1
                        if options.perf08a:
                            update_health(health, event)
            if now - last_sample >= 0.1:
                take_pss = now - last_pss >= 1
                pids = dict(tracked)
                pids[os.getpid()] = "collector"
                for pid, role in list(tracked.items()):
                    if role == "server":
                        pids.update(
                            {child: "checkpoint_worker" for child in descendants(pid)}
                        )
                sample = dict(
                    at_ns=time.monotonic_ns(),
                    processes=[
                        dict(role=role, **proc_sample(pid, take_pss))
                        for pid, role in pids.items()
                    ],
                )
                resources.write(
                    json.dumps(sample, separators=(",", ":")).encode() + b"\n"
                )
                if options.perf08a and limits:
                    from r3_14_perf08a_hooks import resource_violation

                    violation = resource_violation(sample, limits, limits_state)
                    if violation:
                        save(
                            Path(options.run_dir) / "collector-safety.json",
                            dict(reason=violation, sample=sample),
                        )
                        raise RuntimeError(violation)
                last_sample = now
                if take_pss:
                    last_pss = now
            if options.perf08a and now - last_health >= 1:
                publish_health(
                    Path(options.run_dir) / "collector-health.json",
                    health,
                    Path(options.run_dir).name,
                    os.getpid(),
                    time.monotonic_ns(),
                )
                last_health = now
            if pending and (now - last_flush >= 0.1 or stopped):
                events.write(pending)
                pending.clear()
                last_flush = now
            if stopped and sock not in readable:
                break
        os.fsync(events.fileno())
        os.fsync(resources.fileno())
    save(
        Path(options.run_dir) / "collector-final.json",
        dict(
            pid=os.getpid(), received=event_count, pending_batch_peak_bytes=batch_peak
        ),
    )
    sock.close()


def client(options):
    from takler.client.full_snapshot import FullSnapshotComplete
    from takler.client.service_client import TaklerServiceClient
    from takler.exceptions import QueryResponseError
    from takler.protocol.commands import Command

    emitter = Emitter(options.socket)
    clients = {}
    active = {}
    cancellations = {}
    lock = threading.Lock()
    view = [None]
    # Exactly three persistent processes; Q channels use separate connections
    # and threads within their process to avoid serializing unrelated reads.
    channels = (
        [1]
        if options.role in {"F", "F1", "F2"}
        else [2]
        if options.role == "S"
        else [3, 4, 5, 6]
    )
    for channel in channels:
        cl = TaklerServiceClient(
            "127.0.0.1",
            options.port,
            transport_name="grpc",
            secret_file=options.secret,
            single_timeout=10,
            retry_window=60,
        )
        cl.start()
        clients[channel] = cl
        attempts = __import__("itertools").count(1)
        for stub in [cl.transport.query_stub, cl.transport.stub]:
            for rpc_name, rpc in vars(stub).items():
                if not callable(rpc):
                    continue

                def timed_rpc(
                    *args,
                    _rpc=rpc,
                    _name=rpc_name,
                    _channel=channel,
                    _attempts=attempts,
                    **kwargs,
                ):
                    attempt_id = next(_attempts)
                    started = time.monotonic_ns()
                    rid = active.get(_channel, (0, 0))[1]
                    outcome = 0
                    response_bytes = 0
                    request_bytes = args[0].ByteSize()
                    try:
                        if options.perf08a and _name == "ReadPage":
                            from r3_14_perf08a_hooks import attempt_metadata

                            kwargs = attempt_metadata(
                                kwargs, os.getpid(), rid, attempt_id
                            )
                        reply = _rpc(*args, **kwargs)
                        if options.perf08a and _name == "ReadPage":
                            from r3_14_perf08a_hooks import encode_reply

                            encode_reply(emitter, reply, rid, _channel, attempt_id)
                        response_bytes = reply.ByteSize()
                        return reply
                    except Exception:
                        outcome = 1
                        raise
                    finally:
                        ended = time.monotonic_ns()
                        emitter.emit(
                            22,
                            started,
                            ended,
                            a=rid,
                            b=_channel,
                            c=attempt_id,
                            d=outcome,
                            e=request_bytes,
                            f=response_bytes,
                            identity=_name,
                        )

                setattr(stub, rpc_name, timed_rpc)
        original_query = cl.transport.query

        def query(document, _original=original_query, _channel=channel):
            if document.kind != "cancel_full_snapshot_request":
                return _original(document)
            started = time.monotonic_ns()
            confirmed = False
            error = ""
            try:
                reply = _original(document)
                confirmed = bool(getattr(reply, "cancelled", False))
                error = getattr(reply, "code", "")
                return reply
            except Exception as exc:
                error = type(exc).__name__
                raise
            finally:
                rid = active.get(_channel, (0, 0))[1]
                if options.perf08a:
                    cancellations[rid] = confirmed
                emitter.emit(
                    25,
                    started,
                    time.monotonic_ns(),
                    a=rid,
                    b=_channel,
                    c=int(not confirmed),
                    identity=error,
                )

        cl.transport.query = query
    print(json.dumps(dict(ready=True, pid=os.getpid(), channels=channels)), flush=True)
    handshake()
    schedule = control()
    start, end = schedule["start_ns"], schedule["end_ns"]
    stop = threading.Event()
    ids = __import__("itertools").count(1)

    def request(channel, scheduled):
        request_id = next(ids)
        started = time.monotonic_ns()
        if channel == 4 and view[0] is None:
            emitter.emit(
                20,
                scheduled,
                started,
                a=request_id,
                b=channel,
                c=2,
                identity="view_not_ready",
            )
            return
        with lock:
            active[channel] = (started, request_id)
        emitter.emit(21, scheduled, started, a=request_id, b=channel)
        outcome = nodes = byte_count = first = 0
        identity = ""
        fatal = False
        cl = clients[channel]
        try:
            if channel == 1:
                complete = None
                for item in cl.run_iter_full_snapshot():
                    if first == 0:
                        first = time.monotonic_ns()
                    if isinstance(item, FullSnapshotComplete):
                        complete = item
                    else:
                        nodes += 1
                if (
                    complete is None
                    or complete.total_nodes != nodes
                    or nodes != options.tasks + (options.tasks + 999) // 1000 + 1
                ):
                    raise IntegrityError("full snapshot integrity/node count failure")
                if options.perf08a and not cancellations.pop(request_id, False):
                    raise IntegrityError("full cancellation not confirmed")
                byte_count = complete.total_bytes
                identity = complete.sha256
                emitter.emit(
                    27,
                    started,
                    time.monotonic_ns(),
                    a=request_id,
                    b=channel,
                    identity=json.dumps(
                        [
                            complete.session_id,
                            complete.full_snapshot_id,
                            complete.state_version,
                        ],
                        separators=(",", ":"),
                    ),
                )
            elif channel == 2:
                result = cl.run_query_snapshot()
                nodes = len(result.ordered_paths)
                emitter.emit(
                    28,
                    started,
                    time.monotonic_ns(),
                    a=request_id,
                    b=channel,
                    identity=json.dumps(
                        [result.session_id, result.snapshot_id, result.revision],
                        separators=(",", ":"),
                    ),
                )
                if nodes != options.tasks + (options.tasks + 999) // 1000 + 1:
                    raise IntegrityError("summary node count failure")
            elif channel == 3:
                if view[0] is None:
                    boot = time.monotonic_ns()
                    view[0] = cl.run_query_snapshot()
                    emitter.emit(23, boot, time.monotonic_ns(), a=request_id, b=channel)
                try:
                    view[0] = cl.run_query_since(view[0])
                except QueryResponseError as exc:
                    if exc.code != "reset_required":
                        raise
                    boot = time.monotonic_ns()
                    view[0] = cl.run_query_snapshot()
                    emitter.emit(
                        23, boot, time.monotonic_ns(), a=request_id, b=channel, c=1
                    )
            elif channel == 4:
                if view[0] is None:
                    outcome = 2
                    identity = "view_not_ready"
                else:
                    cl.run_query_detail(view[0], "/flow000/task0000", ["runtime"])
            elif channel == 5:
                if cl.run_request_ping() is None:
                    raise IntegrityError("ping absent")
            else:
                if cl.transport.call(Command.SERVER_STATUS, {}).flag:
                    raise IntegrityError("status failed")
        except Exception as exc:
            outcome = 1
            identity = getattr(exc, "code", None) or type(exc).__name__
            fatal = semantic_failure(exc) or options.perf08a
            print(
                f"{CHANNELS[channel]} failure: {type(exc).__name__}",
                file=sys.stderr,
                flush=True,
            )
        finally:
            ended = time.monotonic_ns()
            emitter.emit(
                20,
                scheduled,
                ended,
                a=request_id,
                b=channel,
                c=outcome,
                d=started,
                e=nodes,
                f=byte_count,
                g=first,
                h=int(started - scheduled >= 10_000_000),
                identity=identity,
            )
            with lock:
                active.pop(channel, None)
            if fatal:
                stop.set()
                save(
                    Path(options.run_dir) / f"{options.role}-semantic-error.json",
                    dict(
                        pid=os.getpid(),
                        channel=CHANNELS[channel],
                        request_id=request_id,
                        error=identity,
                        at_ns=ended,
                    ),
                )
                emitter.close()
                os._exit(4)

    def channel_loop(channel):
        period = {
            2: options.summary_period_ns,
            3: 100_000_000,
            4: 200_000_000,
            5: 1_000_000_000,
            6: 1_000_000_000,
        }.get(channel)
        target = start
        while target < end and not stop.is_set():
            stop.wait(max(0, target - time.monotonic_ns()) / 1e9)
            if stop.is_set() or time.monotonic_ns() >= end:
                if period is not None:
                    while target < end:
                        emitter.emit(
                            20,
                            target,
                            time.monotonic_ns(),
                            b=channel,
                            c=2,
                            identity="missed_at_stop",
                        )
                        target += period
                break
            request(channel, target)
            if period is None:
                target = time.monotonic_ns()
                continue
            target += period
            now = time.monotonic_ns()
            from r3_14_perf08a_hooks import missed_slots

            for missed in missed_slots(target, now, end, period):
                emitter.emit(20, missed, now, b=channel, c=2, identity="busy_or_missed")
                target = missed + period
        emitter.emit(26, a=channel)

    if options.perf08a and options.idle:
        channels = []
    threads = [
        threading.Thread(
            target=channel_loop, args=(ch,), name=f"perf-{CHANNELS[ch]}", daemon=True
        )
        for ch in channels
    ]
    for thread in threads:
        thread.start()
    timed_out = False
    while any(thread.is_alive() for thread in threads):
        now = time.monotonic_ns()
        with lock:
            overdue = [
                (ch, item)
                for ch, item in active.items()
                if now - item[0] >= 60_000_000_000
            ]
        if overdue:
            timed_out = True
            stop.set()
            for channel, (sent, rid) in overdue:
                emitter.emit(24, sent, now, a=rid, b=channel, c=3)
                clients[channel].close_channel()
            break
        readable, _, _ = select.select([sys.stdin], [], [], 0.01)
        if sys.stdin in readable:
            line = sys.stdin.readline()
            if not line:
                stop.set()
                break
            msg = json.loads(line)
            if msg["command"] == "clock":
                print(
                    json.dumps(
                        dict(
                            nonce=msg["nonce"],
                            monotonic_ns=time.monotonic_ns(),
                            pid=os.getpid(),
                        )
                    ),
                    flush=True,
                )
    for thread in threads:
        thread.join(timeout=1 if timed_out else 2)
    if any(thread.is_alive() for thread in threads):
        emitter.emit(24, identity="worker_not_released")
        save(
            Path(options.run_dir) / f"{options.role}-final.json",
            dict(
                pid=os.getpid(),
                observation=emitter.close(),
                threads_released=False,
                watchdog=True,
            ),
        )
        # Hard close the process, including blocked RPC/retry threads; bounded
        # lifetime is observable and cannot be mistaken for successful cleanup.
        os._exit(3)
    for cl in clients.values():
        cl.close_channel()
    view[0] = None
    clients.clear()
    cl = stub = rpc = timed_rpc = original_query = query = None
    emitter.emit(26, identity="client_view_and_channels_cleared")
    # Participate in the exit clock handshake without sending new RPCs.
    handshake()
    save(
        Path(options.run_dir) / f"{options.role}-final.json",
        dict(
            pid=os.getpid(),
            observation=emitter.close(),
            threads_released=True,
            watchdog=timed_out,
        ),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=["service", "collector", "client"])
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--port", type=int)
    parser.add_argument("--secret")
    parser.add_argument("--whitelist")
    parser.add_argument("--mode", choices=["O", "L", "D"], default="L")
    parser.add_argument("--role", choices=["F", "F1", "F2", "S", "Q"])
    parser.add_argument("--perf08a", action="store_true")
    parser.add_argument("--tasks", type=int, default=1000)
    parser.add_argument("--summary-period-ns", type=int, default=1_000_000_000)
    parser.add_argument("--idle", action="store_true")
    parser.add_argument("--capacity", type=int, default=8192)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--lag-ablation", choices=["off", "wake_only", "record"])
    options = parser.parse_args()
    if options.perf08a:
        # Register the diagnostic kinds in every PERF-08A producer/collector.
        import r3_14_perf08a_hooks  # noqa: F401
    if options.tasks != 1000 and not options.perf08a:
        parser.error("tree size override is PERF-08A-only")
    if options.lag_ablation and (options.kind != "service" or options.mode != "L"):
        parser.error("lag ablation is a service-only L diagnostic")
    if options.kind == "service":
        asyncio.run(service(options))
    else:
        globals()[options.kind](options)


if __name__ == "__main__":
    main()
