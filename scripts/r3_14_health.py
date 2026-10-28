"""Bounded diagnostic health channel; independent of product progress."""

import json
import os
from pathlib import Path
import tempfile
import time

HEALTH_TIMEOUT_NS = 5_000_000_000
NO_PROGRESS_NS = 60_000_000_000
STATE_LIMIT_BYTES = 16_384


def publish_health(path, health, run_id, collector_pid, updated_ns):
    """Readers see either complete version; no durability claim is needed here."""
    path = Path(path)
    state = dict(
        health, run_id=run_id, collector_pid=collector_pid, updated_ns=updated_ns
    )
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as stream:
            temporary = Path(stream.name)
            json.dump(state, stream)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def update_health(health, event):
    """Keep the existing event meanings, including publication vs exit save."""
    v, kind = event["values"], event["kind"]
    if (
        kind == "request"
        and v[2] == 0
        or kind == "checkpoint_publish"
        and v[1:4] == [1, 1, 0]
    ):
        health["last_progress_ns"] = max(health["last_progress_ns"], event["at_ns"])
    if (
        kind == "request"
        and v[2] == 1
        or kind == "watchdog"
        or kind == "checkpoint_outcome"
        and not v[1]
    ):
        health["failure"] = dict(
            kind=kind, at_ns=event["at_ns"], identity=event["identity"]
        )
    if kind == "service_state":
        health["service_state"] = event


class HealthGuard:
    def __init__(self, path, run_id, collector_pid, run_started_ns, *, clock=None):
        self.path = Path(path)
        self.run_id = run_id
        self.collector_pid = collector_pid
        self.run_started_ns = run_started_ns
        self.clock = clock or time.monotonic_ns
        self.measurement_start_ns = None
        self.last_valid_state = None
        self.last_progress_ns = None
        self.unavailable_since_ns = None
        self.errors = {}
        self.trigger = None

    def _read(self):
        try:
            with self.path.open("rb") as stream:
                raw = stream.read(STATE_LIMIT_BYTES + 1)
        except FileNotFoundError:
            return None, "missing"
        except OSError:
            return None, "read_error"
        if len(raw) > STATE_LIMIT_BYTES:
            return None, "oversize"
        try:
            return json.loads(raw), None
        except (ValueError, UnicodeDecodeError, RecursionError):
            return None, "invalid_json"

    def _validate(self, state, now):
        if not isinstance(state, dict):
            return "invalid_state"
        if state.get("run_id") != self.run_id:
            return "wrong_run"
        if (
            type(state.get("collector_pid")) is not int
            or state["collector_pid"] != self.collector_pid
        ):
            return "wrong_collector"
        updated, progress = state.get("updated_ns"), state.get("last_progress_ns")
        if (
            type(updated) is not int
            or type(progress) is not int
            or updated < self.run_started_ns
            or not 0 <= progress <= updated
            or "failure" not in state
        ):
            return "invalid_state"
        failure = state["failure"]
        if failure is not None and (
            not isinstance(failure, dict)
            or failure.get("kind") not in ("request", "watchdog", "checkpoint_outcome")
            or type(failure.get("at_ns")) is not int
            or not 0 <= failure["at_ns"] <= updated
            or not isinstance(failure.get("identity"), str)
            or len(failure["identity"]) > 128
        ):
            return "invalid_state"
        if updated > now:
            return "future_state"
        if self.last_valid_state and updated < self.last_valid_state["updated_ns"]:
            return "out_of_order_state"
        if now - updated >= HEALTH_TIMEOUT_NS:
            return "stale_state"
        return None

    def record_stop(self, reason, at_ns):
        self.trigger = dict(at_ns=at_ns, reason=reason)

    def _stop(self, reason, now):
        self.record_stop(reason, now)
        raise RuntimeError(reason)

    def check(self, *, collector_exit=None, check_progress=True):
        state, error = self._read()
        # Read first: a concurrent publisher can timestamp after an earlier poll.
        now = self.clock()
        if collector_exit is not None:
            error = "collector_exit"
        elif error is None:
            error = self._validate(state, now)
        if error:
            entry = self.errors.setdefault(error, dict(first_ns=now, last_ns=now))
            entry["last_ns"] = now
            since = state["updated_ns"] if error == "stale_state" else now
            self.unavailable_since_ns = min(
                since,
                self.unavailable_since_ns
                if self.unavailable_since_ns is not None
                else since,
            )
            if (
                collector_exit is not None
                or now - self.unavailable_since_ns >= HEALTH_TIMEOUT_NS
            ):
                self._stop(f"tool health unavailable: {error}", now)
            return
        # Retain only the validated, bounded fields needed to explain a decision.
        self.last_valid_state = {
            k: state[k]
            for k in (
                "run_id",
                "collector_pid",
                "updated_ns",
                "last_progress_ns",
                "failure",
            )
        }
        self.last_progress_ns = max(
            self.last_progress_ns or 0, state["last_progress_ns"]
        )
        if state["failure"] is not None:
            self._stop(f"collector detected failure: {state['failure']}", now)
        # A late recovery cannot erase an already exhausted unavailable budget.
        if (
            self.unavailable_since_ns is not None
            and now - self.unavailable_since_ns >= HEALTH_TIMEOUT_NS
        ):
            self._stop("tool health unavailable: recovery after deadline", now)
        self.unavailable_since_ns = None
        if (
            check_progress
            and self.measurement_start_ns is not None
            and now - max(self.measurement_start_ns, self.last_progress_ns)
            >= NO_PROGRESS_NS
        ):
            self._stop("60s without completed request or periodic publication", now)

    def diagnostics(self):
        return dict(
            schema_version="r3-health-guard-v1",
            errors=self.errors,
            unavailable_since_ns=self.unavailable_since_ns,
            last_valid_state=self.last_valid_state,
            last_progress_ns=self.last_progress_ns,
            measurement_start_ns=self.measurement_start_ns,
            trigger=self.trigger,
        )
