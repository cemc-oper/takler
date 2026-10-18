"""Bounded orchestration of injected PERF-04 setup/roles/verification callbacks.

This is a future service integration entry, not a service launcher. The current
scope tests it only with fake clocks, dispatchers and controlled helper workers.
"""

import time

from r3_14_perf04 import NS, RUNS
from r3_14_perf04_safety import Lifecycle


class Session:
    def __init__(
        self,
        variant,
        registry,
        watchdog,
        guard,
        emit,
        clock=time.monotonic_ns,
        sleep=time.sleep,
    ):
        self.variant, self.registry, self.watchdog, self.guard, self.emit = (
            variant,
            registry,
            watchdog,
            guard,
            emit,
        )
        self.clock, self.sleep = clock, sleep
        origin = (
            getattr(
                watchdog, "hard_ns", clock() + (320 if variant == "S" else 440) * NS
            )
            - (320 if variant == "S" else 440) * NS
        )
        self.lifecycle = Lifecycle(variant, origin, clock=clock)
        self.engine = None
        self.outcome = None

    def check(self):
        if self.clock() >= self.lifecycle.operational_ns:
            raise TimeoutError("whole operational deadline; cleanup reserve")
        if self.lifecycle.phase in ("setup", "verification"):
            self.lifecycle.check()
        self.guard.deadline_ns = self.lifecycle.operational_ns
        self.guard.check(
            now_ns=self.clock(),
            last_child_ns=self.engine.last_child_ns if self.engine else None,
            offered=self.engine.grid.cursor if self.engine else 0,
            measuring=self.engine is not None
            and self.engine.grid.start_ns <= self.clock() < self.engine.grid.end_ns,
        )

    def until(self, end_ns, action=lambda: None):
        while self.clock() < end_ns:
            self.check()
            action()
            if self.engine is not None and self.engine.stopped:
                raise RuntimeError(self.engine.stopped)
            self.sleep(min(0.001, max(0, (end_ns - self.clock()) / NS)))
        self.check()

    def phase(self, name, start_ns, end_ns):
        if end_ns > self.lifecycle.operational_ns:
            end_ns = self.lifecycle.operational_ns
        self.lifecycle.phase = name
        self.lifecycle.phase_end_ns = end_ns
        self.emit(
            dict(
                kind="phase",
                phase=name,
                scheduled_start_ns=start_ns,
                observed_ns=self.clock(),
                deadline_ns=end_ns,
            )
        )
        return end_ns

    def expect_collector_stop(self):
        if self.lifecycle.phase != "verification":
            raise ValueError("collector stop outside shutdown")
        self.check()
        self.guard.expected_collector_stop = True

    def run(self, setup, make_engine, verify, shutdown):
        """Each callback receives an absolute deadline and guard; no fresh waits."""
        self.watchdog.arm()
        try:
            self.check()
            prepared = setup(self.lifecycle.phase_end_ns, self.check)
            self.lifecycle.check()
            verify("before", self.lifecycle.phase_end_ns, self.check)
            self.lifecycle.check()
            ready = self.clock()
            baseline_end = self.phase("baseline", ready, ready + 10 * NS)
            self.until(baseline_end)
            start = baseline_end
            end = self.phase(
                "measurement", start, start + RUNS[self.variant]["seconds"] * NS
            )
            self.guard.measurement_start_ns = start
            self.engine = make_engine(start, prepared)
            self.until(end, lambda: self.engine.tick(self.clock()))
            # Close every remaining scheduled slot at the actual wake time.
            self.engine.tick(self.clock())
            withdrawal_end = self.phase("withdrawal", end, end + 30 * NS)
            self.until(withdrawal_end, lambda: self.engine.tick(self.clock()))
            if self.engine.requests:
                raise RuntimeError("pending/unknown after bounded withdrawal")
            self.guard.measurement_start_ns = None
            deadline = self.phase(
                "verification", withdrawal_end, self.clock() + 150 * NS
            )
            verify("after", deadline, self.check)
            shutdown(deadline, self.check, self.expect_collector_stop)
            self.check()
            self.outcome = (
                "completed" if not self.engine.stopped else self.engine.stopped
            )
        except BaseException as exc:
            self.outcome = type(exc).__name__ + ": " + str(exc)
            if self.engine:
                self.engine.stop(self.outcome)
        finally:
            cleanup_deadline = min(self.lifecycle.hard_ns, self.clock() + 10 * NS)
            try:
                cleanup = self.registry.cleanup(cleanup_deadline)
            except Exception as exc:
                cleanup = {
                    "live_registered": self.registry.summary().get(
                        "live_registered", []
                    )
                }
                self.outcome = "cleanup failure: " + type(exc).__name__
            if cleanup.get("live_registered") or self.clock() > self.lifecycle.hard_ns:
                self.outcome = "cleanup incomplete or actual lifecycle budget breach"
            self.emit(dict(kind="exit_reason", reason=self.outcome, at_ns=self.clock()))
        return dict(
            task_id="PERF-04",
            exit_reason=self.outcome,
            workload=None if self.engine is None else self.engine.finish(),
            registry=self.registry.summary(),
        )
