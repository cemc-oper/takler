"""Bounded external clock evidence; never imported by service or collector."""

from datetime import datetime, timezone
import threading
import time


def utc_ns(value):
    """Convert ISO UTC without float epoch rounding (datetime has microseconds)."""
    delta = datetime.fromisoformat(value) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        delta.days * 86400 + delta.seconds
    ) * 1_000_000_000 + delta.microseconds * 1000


class ClockProbe:
    def __init__(self, capacity=4096):
        self.capacity = capacity
        self.records = []
        self.failure = None
        self.lock = threading.Lock()
        self.previous_monotonic = None

    def read(self, label, reader):
        # One lock preserves actual reading order across the existing sampler and
        # the orchestrator's four metadata reads; no service lock is involved.
        with self.lock:
            before = time.monotonic_ns()
            record = dict(seq=len(self.records), label=label, before_ns=before)
            try:
                value = reader()
            except Exception as exc:
                record["error"] = f"{type(exc).__name__}: {exc}"
                self.failure = record["error"]
                raise
            finally:
                after = time.monotonic_ns()
                record.update(after_ns=after, width_ns=after - before)
                if len(self.records) < self.capacity:
                    self.records.append(record)
                else:
                    self.failure = "clock evidence capacity exceeded"
                if after < before or (
                    len(self.records) > 1 and before < self.records[-2]["after_ns"]
                ):
                    self.failure = "Python monotonic reading moved backwards"
            record["value"] = value
            if label in {"python_monotonic", "started", "ended"}:
                if (
                    self.previous_monotonic is not None
                    and value < self.previous_monotonic
                ):
                    self.failure = "Python monotonic float moved backwards"
                self.previous_monotonic = value
            if label in {"utc", "start_utc", "end_utc"}:
                record["utc_ns"] = utc_ns(value)
            return value

    def tick(self, tick):
        cpu_before = time.thread_time_ns()
        first = len(self.records)
        for name, reader in (
            ("utc", lambda: datetime.now(timezone.utc).isoformat()),
            ("python_monotonic", time.monotonic),
            ("python_monotonic_ns", time.monotonic_ns),
            ("realtime", lambda: time.clock_gettime_ns(time.CLOCK_REALTIME)),
            ("monotonic", lambda: time.clock_gettime_ns(time.CLOCK_MONOTONIC)),
            ("raw", lambda: time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)),
        ):
            self.read(name, reader)
        return dict(
            tick=tick,
            first_seq=first,
            end_seq=len(self.records),
            thread_cpu_ns=time.thread_time_ns() - cpu_before,
        )

    def check(self):
        if self.failure:
            raise RuntimeError(self.failure)


def clock_info():
    return {
        name: vars(time.get_clock_info(name))
        for name in ("time", "monotonic", "perf_counter", "thread_time")
    }
