"""Bounded PERF-03 records. Producers never wait for the collector or disk.

A fixed pool of records is shared through CPython's reentrant SimpleQueue.
Sequence allocation uses atomic next(count), including GC reentry; producers
encode no JSON. The sole sender owns IPC and keeps at most one pending record.
"""

from __future__ import annotations

import asyncio
import contextvars
import gc
import heapq
import itertools
import os
import queue
import socket
import struct
import sys
import threading
import time


RECORD = struct.Struct("<15Q128s8x")
SCHEMA = "r3-perf-events-v2.1"
KINDS = {
    1: "loop_lag",
    2: "gc_start",
    3: "gc_stop",
    4: "gc_state",
    5: "checkpoint_total",
    6: "checkpoint_pin",
    7: "checkpoint_publish",
    8: "service_state",
    9: "synthetic",
    10: "summary_capture",
    11: "summary_page",
    12: "summary_grpc_rpc",
    13: "summary_wire_encode",
    20: "request",
    21: "request_sent",
    22: "rpc_attempt",
    23: "bootstrap",
    24: "watchdog",
    25: "cancel",
    26: "client_release",
    27: "full_identity",
    28: "summary_identity",
}
MAX_PACKET_RECORDS = 128
CHANNELS = {
    1: "full_stream",
    2: "summary",
    3: "since",
    4: "detail",
    5: "ping",
    6: "status",
}


def next_grid(target: int, actual: int, interval: int = 5_000_000):
    lag = max(0, actual - target)
    missed = lag // interval
    return target + (missed + 1) * interval, lag, missed


def decode_record(raw: bytes) -> dict:
    seq, pid, tid, kind, at, start, end, *values = RECORD.unpack(raw)
    identity = values.pop().split(b"\0", 1)[0].decode("utf-8", errors="replace")
    return dict(
        schema_version=SCHEMA,
        producer_id=str(pid),
        sequence=seq,
        pid=pid,
        tid=tid,
        kind=KINDS[kind],
        at_ns=at,
        start_ns=start,
        end_ns=end,
        values=values,
        identity=identity,
    )


def decode_packet(raw):
    if not raw or len(raw) % RECORD.size or len(raw) > RECORD.size * MAX_PACKET_RECORDS:
        raise ValueError("invalid bounded packet")
    for offset in range(0, len(raw), RECORD.size):
        yield decode_record(raw[offset : offset + RECORD.size])


class Emitter:
    def __init__(self, destination: str, capacity: int = 8192, *, start_sender=True):
        if not 1 <= capacity <= 8192:
            raise ValueError("capacity outside the recorded service budget")
        self.capacity = capacity
        self.storage = tuple(bytearray(RECORD.size) for _ in range(capacity))
        self.available = queue.SimpleQueue()
        self.ready = queue.SimpleQueue()
        for record in self.storage:
            self.available.put(record)
        self.sequence = itertools.count(1)
        self.peak = 0
        self.emitted = 0
        self.rejections = {
            name: itertools.count(1)
            for name in (
                "closed",
                "contention",
                "overflow",
                "oversize",
                "ipc",
                "drain_timeout",
            )
        }
        self.drain_deadline = float("inf")
        self.done = threading.Event()
        self.pid = os.getpid()
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.socket.setblocking(False)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
        self.destination = destination
        self.sender = threading.Thread(
            target=self._send, name="perf-event-sender", daemon=True
        )
        if start_sender:
            self.sender.start()

    def emit(
        self, kind, start=0, end=0, a=0, b=0, c=0, d=0, e=0, f=0, g=0, h=0, identity=""
    ):
        seq = next(self.sequence)
        if self.done.is_set():
            next(self.rejections["closed"])
            return False
        try:
            record = self.available.get_nowait()
        except queue.Empty:
            next(self.rejections["overflow"])
            return False
        self.peak = max(self.peak, self.capacity - self.available.qsize())
        try:
            label = identity.encode("utf-8") if identity else b""
            if len(label) > 128:
                next(self.rejections["oversize"])
                self.available.put(record)
                return False
            RECORD.pack_into(
                record,
                0,
                seq,
                self.pid,
                threading.get_ident(),
                kind,
                time.monotonic_ns(),
                start,
                end,
                a,
                b,
                c,
                d,
                e,
                f,
                g,
                h,
                label,
            )
            self.ready.put(record)
            return True
        except BaseException:
            self.available.put(record)
            raise

    def _send(self):
        while not self.done.is_set() or not self.ready.empty():
            self.done.wait(0.1)
            remaining = self.capacity
            while remaining and not self.ready.empty():
                if self.done.is_set() and time.monotonic() >= self.drain_deadline:
                    return
                batch = []
                packet = bytearray()
                for _ in range(min(MAX_PACKET_RECORDS, remaining)):
                    try:
                        record = self.ready.get_nowait()
                    except queue.Empty:
                        break
                    batch.append(record)
                    packet.extend(record)
                remaining -= len(batch)
                while True:
                    try:
                        self.socket.sendto(packet, self.destination)
                        self.emitted += len(batch)
                        break
                    except BlockingIOError:
                        if (
                            self.done.is_set()
                            and time.monotonic() >= self.drain_deadline
                        ):
                            for _ in batch:
                                next(self.rejections["drain_timeout"])
                            break
                        time.sleep(0.001)
                    except OSError:
                        for _ in batch:
                            next(self.rejections["ipc"])
                        break
                for record in batch:
                    self.available.put(record)

    def memory_bound(self):
        # Enumerate only fixed, owned records, outside measurement after hooks
        # are removed. Reserve 512 KiB for both queue reference arrays, counters,
        # control objects and sender state. Allocation tests check the plateau.
        return (
            sys.getsizeof(self.storage)
            + sum(sys.getsizeof(r) for r in self.storage)
            + sys.getsizeof(self)
            + sys.getsizeof(self.__dict__)
            + 524288
        )

    def close(self):
        self.drain_deadline = time.monotonic() + 1.5
        self.done.set()
        if self.sender.ident is None:
            self.sender.start()
        self.sender.join(timeout=2)
        produced = next(self.sequence) - 1
        result = dict(
            produced=produced,
            emitted=self.emitted,
            dropped=produced - self.emitted,
            buffer_peak=self.peak,
            capacity=self.capacity,
            record_bytes=RECORD.size,
            packet_max_records=MAX_PACKET_RECORDS,
            packet_max_bytes=MAX_PACKET_RECORDS * RECORD.size,
            sender_flush_interval_ms=100,
            observation_storage_bound_bytes=self.memory_bound(),
            socket_send_buffer_bytes=self.socket.getsockopt(
                socket.SOL_SOCKET, socket.SO_SNDBUF
            ),
            sender_stopped=not self.sender.is_alive(),
            drop_reasons={
                name: next(counter) - 1 for name, counter in self.rejections.items()
            },
            abandoned_ring_records=self.ready.qsize(),
        )
        if not self.sender.is_alive():
            self.socket.close()
        return result


class PhaseSink:
    """Compatibility sink for existing summary hooks, without event retention."""

    def __init__(self, emitter):
        self.emitter = emitter

    def append(self, item):
        kinds = {
            "summary_capture": 10,
            "summary_page": 11,
            "summary_grpc_rpc": 12,
            "summary_wire_encode": 13,
        }
        self.emitter.emit(
            kinds[item["phase"]],
            item["start_ns"],
            item["end_ns"],
            a=item.get("thread_cpu_ns", 0),
            b=item.get("encode_ns", 0),
            c=item.get("decode_ns", 0),
            d=item.get("request_decode_ns", 0),
            e=item.get("encoded_bytes", item.get("bytes", 0)),
            f=item.get("pages", 0),
            g=item.get("page_index") or 0,
            h=int(bool(item.get("error_code"))),
            identity=item.get("snapshot_id") or "",
        )


class ServiceObservation:
    def __init__(self, server, emitter, detailed=False, *, lag_mode="record"):
        if lag_mode not in {"off", "wake_only", "record"}:
            raise ValueError("unknown lag diagnostic mode")
        self.server, self.emitter, self.detailed = server, emitter, detailed
        self.lag_mode = lag_mode
        self.lag_ticks = self.lag_missed = 0
        self.lag_handle = None
        self.originals = []
        self.tasks = []
        self.phase = None
        self.gc_callback = self._gc

    def _gc(self, phase, info):
        self.emitter.emit(
            2 if phase == "start" else 3,
            a=info["generation"],
            b=info.get("collected", 0),
            c=info.get("uncollectable", 0),
        )

    def _lag_tick(self):
        actual = time.monotonic_ns()
        following, lag, missed = next_grid(self.target, actual)
        self.lag_ticks += 1
        self.lag_missed += missed
        if self.lag_mode == "record":
            self.emitter.emit(1, self.target, actual, a=lag, b=missed)
        self.target = following
        if self.reuse_lag_timer:
            # CPython's selector loop pops a TimerHandle before invoking it.
            # Reinsert only our popped handle, on its owning loop thread;
            # preserve the fixed grid and avoid allocating 200 handles/second.
            self.lag_handle._when = self.target / 1e9
            heapq.heappush(self.loop._scheduled, self.lag_handle)
            self.lag_handle._scheduled = True
        else:
            self.lag_handle = self.loop.call_at(
                self.target / 1e9, self.lag_callback, context=self.lag_context
            )

    def lag_state(self):
        return dict(mode=self.lag_mode, ticks=self.lag_ticks, missed=self.lag_missed)

    async def _state(self):
        while True:
            scheduler = self.server.scheduler
            query = scheduler.query_service
            manager = self.server.checkpoint_manager
            self.emitter.emit(
                8,
                a=len(query.full_snapshots._leases),
                b=len(query._snapshots),
                c=len(manager._background_writes),
                d=manager._background_reserved,
            )
            self.emitter.emit(
                4,
                a=int(gc.isenabled()),
                b=gc.get_threshold()[0],
                c=gc.get_threshold()[1],
                d=gc.get_threshold()[2],
            )
            await asyncio.sleep(0.1)

    def install(self):
        manager = self.server.checkpoint_manager
        for name, kind in [("_pin_fixed_for_write", 6), ("_publish_prepared", 7)]:
            original = getattr(manager, name)

            def wrap(*args, _original=original, _kind=kind):
                started = time.monotonic_ns()
                result = _original(*args)
                self.emitter.emit(
                    _kind,
                    started,
                    time.monotonic_ns(),
                    a=result[0] if _kind == 6 else args[0],
                    b=int(result[1] is not None) if _kind == 6 else int(result[0]),
                )
                return result

            self.originals.append((manager, name, original))
            setattr(manager, name, wrap)
        original = manager.write_checkpoint_async

        async def write():
            started = time.monotonic_ns()
            success = False
            try:
                success = await original()
                return success
            finally:
                self.emitter.emit(5, started, time.monotonic_ns(), a=int(success))

        self.originals.append((manager, "write_checkpoint_async", original))
        manager.write_checkpoint_async = write
        gc.callbacks.append(self.gc_callback)
        self.loop = asyncio.get_running_loop()
        # Other loop implementations retain the public call_at path. This
        # tooling optimization is qualified only on the exact recorded loop.
        self.reuse_lag_timer = type(self.loop) is asyncio.SelectorEventLoop
        self.target = time.monotonic_ns() + 5_000_000
        self.lag_callback = self._lag_tick
        self.lag_context = contextvars.copy_context()
        if self.lag_mode != "off":
            self.lag_handle = self.loop.call_at(
                self.target / 1e9, self.lag_callback, context=self.lag_context
            )
        self.tasks = [asyncio.create_task(self._state())]
        if self.detailed:
            from r3_14_phase_probe import SummaryPhaseProbe

            self.phase = SummaryPhaseProbe(
                self.server.scheduler.query_service,
                self.server.grpc_transport,
                sink=PhaseSink(self.emitter),
            ).install()
        return self

    async def close(self):
        if self.lag_handle is not None:
            self.lag_handle.cancel()
        if self.gc_callback in gc.callbacks:
            gc.callbacks.remove(self.gc_callback)
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.phase is not None:
            self.phase.close()
        for obj, name, original in reversed(self.originals):
            setattr(obj, name, original)
