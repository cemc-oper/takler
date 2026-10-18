"""PERF-04 bounded append-only producer evidence and recomputable counters."""

from collections import Counter, defaultdict
import itertools
import json
import queue
import threading
import time

SCHEMA = "r3-perf04-tools-v2"


class Journal:
    """256 fixed 16KiB slots (<8MiB), one <=16KiB pending write, no producer wait."""

    capacity, slot_bytes = 256, 16384

    def __init__(self, path):
        self.path = path
        self.sequence = itertools.count(1)
        self.storage = [bytearray(self.slot_bytes) for _ in range(self.capacity)]
        self.free, self.ready = queue.SimpleQueue(), queue.SimpleQueue()
        for slot in self.storage:
            self.free.put(slot)
        self.done = threading.Event()
        self.emit_lock = threading.RLock()
        self.loss = Counter()
        self.emitted = self.peak = 0
        self.failure = None
        self.thread = threading.Thread(target=self._write, daemon=True)
        self.thread.start()

    def emit(self, row):
        with self.emit_lock:
            return self._emit(row)

    def _emit(self, row):
        seq = next(self.sequence)
        raw = (
            json.dumps(
                dict(row, schema_version=SCHEMA, sequence=seq), separators=(",", ":")
            ).encode()
            + b"\n"
        )
        try:
            if self.done.is_set() or len(raw) > self.slot_bytes:
                raise ValueError("closed or oversized")
            slot = self.free.get_nowait()
        except (queue.Empty, ValueError):
            self.loss["critical"] += 1
            raise RuntimeError("critical journal loss") from None
        slot[: len(raw)] = raw
        self.ready.put((slot, len(raw)))
        self.peak = max(self.peak, self.capacity - self.free.qsize())

    def _write(self):
        try:
            with self.path.open("wb", buffering=0) as stream:
                while not self.done.is_set() or not self.ready.empty():
                    try:
                        slot, count = self.ready.get(timeout=0.02)
                    except queue.Empty:
                        continue
                    stream.write(memoryview(slot)[:count])
                    self.emitted += 1
                    self.free.put(slot)
        except BaseException as exc:
            self.failure = type(exc).__name__

    def close(self, deadline_ns):
        self.done.set()
        self.thread.join(max(0, min(2, (deadline_ns - time.monotonic_ns()) / 1e9)))
        produced = next(self.sequence) - 1
        return dict(
            produced=produced,
            emitted=self.emitted,
            dropped=sum(self.loss.values()),
            high_water=self.peak,
            capacity=self.capacity,
            sender_stopped=not self.thread.is_alive(),
            writer_failure=self.failure,
            pending=self.ready.qsize(),
            memory_bytes=self.capacity * self.slot_bytes,
            pending_batch_max_bytes=self.slot_bytes,
        )


def audit_producer(path, final):
    if final is None:
        return dict(tail="unknown", problems=["missing final"])
    expected, received = 1, 0
    problems = []
    with path.open() as stream:
        for line in stream:
            row = json.loads(line)
            if row["sequence"] != expected:
                problems.append("sequence gap or duplicate")
            expected = row["sequence"] + 1
            received += 1
    if (
        received != final["emitted"]
        or final["produced"] != received
        or final["dropped"]
        or final["pending"]
        or not final["sender_stopped"]
        or final.get("writer_failure")
    ):
        problems.append("producer counts do not close")
    return dict(
        tail="known" if not problems else "unknown",
        received=received,
        problems=problems,
    )


class Ledger:
    """Current requests <=256, histories streamed. Dimension keys are bounded."""

    def __init__(self, emit, start_ns=0):
        self.emit = emit
        self.start_ns = start_ns
        self.counts = Counter()
        self.dimensions = defaultdict(Counter)
        self.active = {}

    def bump(self, event, row, count=1):
        self.counts[event] += count
        for dimension in ("second", "operation", "stage", "flow", "role"):
            if dimension in row:
                self.dimensions[(dimension, str(row[dimension]))][event] += count

    def offered(self, row):
        self.bump("offered", row)
        self.emit(dict(row, kind="offered"))

    def expired_range(self, grid, lo, hi):
        # Split only into bounded second/stage buckets; never retain slot objects.
        cursor = lo
        while cursor < hi:
            second = (grid.at(cursor) - grid.start_ns) // 1_000_000_000
            end = cursor + 1
            while (
                end < hi and (grid.at(end) - grid.start_ns) // 1_000_000_000 == second
            ):
                end += 1
            for stage in range(10):
                count = (end - 1 - stage) // 10 - (cursor - 1 - stage) // 10
                if not count:
                    continue
                from r3_14_perf04 import STEPS

                row = dict(
                    second=second,
                    stage=stage,
                    operation=STEPS[stage][0],
                    flow="unassigned",
                    role="W",
                )
                self.bump("offered", row, count)
                self.bump("not_sent", row, count)
                self.bump("expired", row, count)
            self.emit(
                dict(
                    kind="expired_range",
                    first=cursor,
                    stop=end,
                    second=second,
                    count=end - cursor,
                    role="W",
                )
            )
            cursor = end

    def dispatch(self, row):
        if len(self.active) >= 256 or row["id"] in self.active:
            raise RuntimeError("request buffer exceeded")
        self.active[row["id"]] = dict(row, status="dispatched")
        self.emit(dict(row, kind="dispatch"))

    def not_sent(self, row, reason):
        self.active.pop(row["id"], None)
        self.bump("not_sent", row)
        self.bump(reason, row)
        self.emit(dict(row, kind="not_sent", reason=reason))

    def sent(self, rid, now):
        row = self.active[rid]
        if row["status"] != "dispatched" or now >= row["expires_ns"]:
            raise RuntimeError("duplicate or expired transport send")
        row.update(status="sent", sent_ns=now)
        timed = dict(row, second=(now - self.start_ns) // 1_000_000_000)
        self.bump("sent", timed)
        if now - row["scheduled_ns"] >= 10_000_000:
            self.bump("late", timed)
        self.emit(
            dict(
                row,
                kind="sent",
                send_wait_ns=now - row["scheduled_ns"],
                dispatch_wait_ns=now - row["dispatch_ns"],
            )
        )

    def completed(self, rid, now, outcome, effective=False):
        row = self.active.pop(rid)
        if row["status"] != "sent" or now < row["sent_ns"]:
            raise RuntimeError("completion without transport send")
        timed = dict(row, second=(now - self.start_ns) // 1_000_000_000)
        self.bump("completed", timed)
        self.bump(outcome, timed)
        if effective:
            self.bump("effective", timed)
        self.emit(
            dict(
                row,
                kind="completed",
                completed_ns=now,
                outcome=outcome,
                effective=effective,
                send_wait_ns=row["sent_ns"] - row["scheduled_ns"],
                rpc_wait_ns=now - row["sent_ns"],
                total_wait_ns=now - row["scheduled_ns"],
            )
        )

    def snapshot(self):
        sent_pending = sum(r["status"] == "sent" for r in self.active.values())
        dispatched = len(self.active) - sent_pending
        closed = (
            self.counts["offered"]
            == self.counts["sent"] + self.counts["not_sent"] + dispatched
            and self.counts["sent"] == self.counts["completed"] + sent_pending
        )
        return dict(
            counts=dict(self.counts),
            dimensions=[
                dict(dimension=d, key=k, counts=dict(v))
                for (d, k), v in sorted(self.dimensions.items())
            ],
            pending=list(self.active.values()),
            sent_pending=sent_pending,
            dispatch_pending=dispatched,
            counts_closed=closed and dispatched == 0,
            provisional_counts_closed=closed,
        )
