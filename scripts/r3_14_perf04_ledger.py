"""PERF-04 bounded append-only producer evidence and recomputable counters."""

from collections import Counter, defaultdict
import itertools
import json
import queue
import threading
import time

SCHEMA = "r3-perf04-s-tools-v3"


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

    def completed(
        self,
        rid,
        now,
        outcome,
        effective=False,
        *,
        rpc_returned_ns=None,
        proof_completed_ns=None,
    ):
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
                rpc_wait_ns=None
                if rpc_returned_ns is None
                else rpc_returned_ns - row["sent_ns"],
                rpc_returned_ns=rpc_returned_ns,
                proof_completed_ns=proof_completed_ns,
                proof_wait_ns=None
                if rpc_returned_ns is None or proof_completed_ns is None
                else proof_completed_ns - rpc_returned_ns,
                logical_wait_ns=now - row["sent_ns"],
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


def audit_s_window(path, start_ns):
    """Recompute S admission from raw W rows, RPC results and writer proofs."""
    from r3_14_perf04 import STEPS

    rows = {k: [] for k in ("offered", "sent", "completed", "not_sent")}
    facts = defaultdict(Counter)
    unexpected = []
    with path.open() as stream:
        for line in stream:
            row = json.loads(line)
            kind = row.get("kind")
            if row.get("role") == "W" and kind in rows:
                rows[kind].append(row)
            if kind == "expired_range":
                unexpected.append("expired grid range")
            if kind == "role_event":
                event = row["event"]
                if event["role"] == "W" and event["kind"] in (
                    "rpc",
                    "rpc_result",
                    "proof_completed",
                ):
                    facts[event["id"]][event["kind"]] += 1
                    if event["kind"] == "rpc" and event["outcome"] != "returned":
                        unexpected.append("transport not returned")
                    if event["kind"] == "rpc_result" and event["flag"] != 0:
                        unexpected.append("business error")
    problems = list(dict.fromkeys(unexpected))
    offered = rows["offered"]
    if len(offered) != 600 or any(
        r["scheduled_ns"] != start_ns + i * 100_000_000
        or r["stage"] != i % 10
        or r["operation"] != STEPS[i % 10][0]
        for i, r in enumerate(offered)
    ):
        problems.append("raw 600-slot grid mismatch")
    ids = {r["id"] for r in offered}
    if len(ids) != 600 or rows["not_sent"]:
        problems.append("missing/duplicate/unsent W slots")
    seconds = Counter()
    for row in rows["sent"]:
        seconds[(row["sent_ns"] - start_ns) // 1_000_000_000] += 1
    if seconds != Counter({i: 10 for i in range(60)}):
        problems.append("raw per-second sent mismatch")
    for kind in ("sent", "completed"):
        if len(rows[kind]) != 600 or {r["id"] for r in rows[kind]} != ids:
            problems.append("raw " + kind + " identities mismatch")
    if any(
        r.get("outcome") != "success" or not r.get("effective")
        for r in rows["completed"]
    ):
        problems.append("raw effective completion mismatch")
    if any(
        facts[rid] != Counter(rpc=1, rpc_result=1, proof_completed=1) for rid in ids
    ):
        problems.append("raw RPC/business/proof correlation mismatch")
    return dict(
        problems=problems,
        counts={k: len(v) for k, v in rows.items()},
        sent_by_second=dict(sorted(seconds.items())),
        rpc_business_proof_ids=len(facts),
    )


class SequenceWindow:
    """Bounded receipt window for GC-reentrant L producers; no loss is hidden."""

    def __init__(self, capacity=8192):
        self.capacity, self.next, self.received = capacity, 1, 0
        self.waiting = set()

    def accept(self, sequence):
        if sequence < self.next or sequence in self.waiting:
            raise RuntimeError("duplicate L producer sequence")
        if sequence >= self.next + self.capacity:
            raise RuntimeError("L sequence window exceeded")
        self.waiting.add(sequence)
        self.received += 1
        while self.next in self.waiting:
            self.waiting.remove(self.next)
            self.next += 1

    def final(self, produced):
        return dict(
            known=not self.waiting
            and self.received == produced
            and self.next == produced + 1,
            received=self.received,
            next_sequence=self.next,
            unresolved_sequences=sorted(self.waiting),
            capacity=self.capacity,
        )
