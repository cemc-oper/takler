"""PERF-04 four-role workload coordinator; transport and clock are injected.

No service launcher or automatic S/V/H/D/L/R batch is provided. A caller must
supply initialized identities, bounded role workers, guards and evidence sinks.
"""

from collections import Counter
from dataclasses import asdict
import itertools

from r3_14_perf04 import Grid, NS, RUNS, STEPS, Pool
from r3_14_perf04_ledger import Ledger

CHANNELS = {
    "child": ("W", 250),
    "rotation": ("R", 16),
    "batch": ("C", 1),
    "structure": ("C", 1),
    "since": ("Q", 1),
    "detail": ("Q", 1),
    "status": ("Q", 1),
    "verify": ("Q", 1),
}


class Limits:
    def __init__(self):
        self.active = {}
        self.counts = Counter()
        self.paths = set()
        self.peak = Counter()
        self.rotating = set()

    def reason(self, channel, path=None):
        if len(self.active) >= 256:
            return "global_busy"
        if self.counts[channel] >= CHANNELS[channel][1]:
            return "role_busy"
        if channel in ("child", "rotation") and self.counts["pool"] >= 250:
            return "pool_busy"
        if path is not None and path in self.paths:
            return "task_busy"
        return None

    def acquire(self, rid, channel, path=None):
        if reason := self.reason(channel, path):
            return reason
        if rid in self.active:
            raise ValueError("duplicate request identity")
        self.active[rid] = (channel, path)
        self.counts[channel] += 1
        if channel in ("child", "rotation"):
            self.counts["pool"] += 1
        if path is not None:
            self.paths.add(path)
        for name in (channel, "pool"):
            self.peak[name] = max(self.peak[name], self.counts[name])
        self.peak["global"] = max(self.peak["global"], len(self.active))
        return None

    def release(self, rid):
        channel, path = self.active.pop(rid)
        self.counts[channel] -= 1
        if channel in ("child", "rotation"):
            self.counts["pool"] -= 1
        if path is not None:
            self.paths.remove(path)


class Engine:
    def __init__(
        self,
        variant,
        start_ns,
        paths,
        identities,
        dispatcher,
        emit,
        guard=None,
        templates=None,
    ):
        if guard is None and not getattr(dispatcher, "simulation", False):
            raise ValueError("live role entry requires explicit PERF-04 guards")
        self.variant, self.settings = variant, RUNS[variant]
        self.grid = Grid(
            start_ns,
            self.settings["seconds"],
            self.settings["rate"],
            self.settings["burst"],
        )
        self.pool = Pool(paths, identities)
        self.identities = dict(identities)  # 250 current entries, no attempt history.
        self.dispatcher, self.guard = dispatcher, guard
        self.ledger, self.limits = Ledger(emit, start_ns), Limits()
        self.requests = {}
        self.terminal_unknown = set()
        self.serial = itertools.count(1)
        self.stopped = None
        self.last_child_ns = start_ns
        self.templates = templates or {}
        self.phase_deadline_ns = self.grid.end_ns + 30 * NS
        self.cursors = dict(batch=10, structure=60, since=0, detail=0, status=0)

    def stop(self, reason):
        if self.stopped is None:
            self.stopped = reason
            self.ledger.emit(dict(kind="stop", reason=reason))

    def _row(self, operation, channel, scheduled, expires, now, token=None, path=None):
        return dict(
            id=next(self.serial),
            operation=operation,
            channel=channel,
            role=CHANNELS[channel][0],
            scheduled_ns=scheduled,
            expires_ns=expires,
            dispatch_ns=now,
            second=(scheduled - self.grid.start_ns) // NS,
            stage=token.stage if token else operation,
            path=path,
            flow=path.split("/")[1] if path else "unassigned",
            attempt=token.attempt if token else None,
        )

    def _submit(self, row, payload, token=None):
        self.ledger.offered(row)
        if reason := self.limits.acquire(
            row["id"], row["channel"], row["path"] if token else None
        ):
            if token:
                self.pool.cancel_unsent(token)
            self.ledger.not_sent(row, reason)
            return False
        self.ledger.dispatch(row)
        job = dict(
            row,
            payload=payload,
            reservation=asdict(token) if token else None,
            rpc_deadline_ns=min(
                self.phase_deadline_ns,
                row["dispatch_ns"] + (60 if row["channel"] == "verify" else 10) * NS,
            ),
        )
        if token:
            job["credentials"] = self.identities[row["path"]]
        elif row["channel"] == "detail":
            identity = self.identities[row["path"]]
            job["expected_identity"] = {
                k: identity[k] for k in ("attempt", "generation")
            }
        self.requests[row["id"]] = (row, token)
        if not self.dispatcher.submit(job):
            self.limits.release(row["id"])
            self.requests.pop(row["id"])
            if token:
                self.pool.cancel_unsent(token)
            self.ledger.not_sent(row, "dispatch_busy")
            return False
        return True

    def _pool_job(self, token, scheduled, expires, now):
        task = self.pool.tasks[token.task]
        if token.stage < 10:
            operation, value = STEPS[token.stage]
            payload = dict(node_path=task.path, attempt_id=token.attempt)
            if operation == "init":
                payload["task_id"] = "perf04-" + token.attempt
            elif operation == "event":
                payload.update(event_name=value, source_task_path=task.path)
            elif operation == "meter":
                payload.update(
                    meter_name="progress", meter_value=value, source_task_path=task.path
                )
        else:
            operation = token.operation
            payload = dict(node_paths=[task.path])
            if operation == "run":
                payload["force"] = False
        row = self._row(
            operation,
            "child" if token.stage < 10 else "rotation",
            scheduled,
            expires,
            now,
            token,
            task.path,
        )
        return self._submit(row, payload, token)

    def accept(self, event):
        rid = event["id"]
        if rid in self.terminal_unknown:
            self.ledger.emit(dict(kind="ignored_unknown_callback", id=rid))
            return
        if rid not in self.requests:
            # Old/duplicate messages cannot resolve any later reservation.
            self.ledger.emit(dict(kind="ignored_callback", id=rid))
            return
        row, token = self.requests[rid]
        if event["kind"] in (
            "verification_node",
            "verification_fragment",
            "rpc_result",
            "proof_completed",
        ):
            self.ledger.emit(event)
            return
        if event["kind"] == "query_reset":
            self.ledger.emit(event)
            return
        if event["kind"] == "rpc":
            self.ledger.bump("rpc_completed", row)
            if event["outcome"] not in ("success", "returned"):
                self.ledger.bump("rpc_failed", row)
            self.ledger.emit(
                dict(
                    event,
                    role=row["role"],
                    operation=row["operation"],
                    flow=row["flow"],
                )
            )
            return
        if event["kind"] == "sent":
            self.ledger.sent(rid, event["at_ns"])
            return
        if event["kind"] == "not_sent":
            self.ledger.not_sent(row, event["reason"])
            if token:
                self.pool.cancel_unsent(token)
        elif event["kind"] == "completed":
            result = event.get("result", {})
            outcome = event["outcome"]
            effective = outcome == "success" and (
                row["channel"] not in ("child", "rotation", "batch", "structure")
                or result.get("proof", {}).get("effective") is True
            )
            identity = result.get("identity")
            proof = result.get("proof", {})
            if token and effective:
                if (
                    proof.get("path") != row["path"]
                    or proof.get("stage") != token.stage
                    or proof.get("attempt") != token.attempt
                ):
                    effective = False
                if token.stage == 11 and (
                    not identity
                    or not identity.get("password")
                    or not identity.get("attempt")
                    or identity["attempt"] == token.attempt
                    or identity.get("generation")
                    != self.identities[row["path"]]["generation"]
                ):
                    effective = False
            if not effective and outcome == "success":
                outcome = "invalid_effect"
            self.ledger.completed(
                rid,
                event["at_ns"],
                outcome,
                effective,
                rpc_returned_ns=event.get("rpc_returned_ns"),
                proof_completed_ns=event.get("proof_completed_ns"),
            )
            if token:
                if outcome != "success":
                    self.pool.finish(token, success=False)
                    self.ledger.bump("unknown", row)
                else:
                    self.pool.finish(
                        token,
                        success=True,
                        attempt=identity["attempt"] if token.stage == 11 else None,
                    )
                    if token.stage == 11:
                        self.identities[row["path"]] = identity
                        self.limits.rotating.discard(token.task)
                    if token.stage < 10:
                        self.last_child_ns = max(self.last_child_ns, event["at_ns"])
            # Full reply details are public DTOs only. Never journal handoff credentials.
            self.ledger.emit(
                dict(
                    kind="reply",
                    id=rid,
                    flag=result.get("flag"),
                    results=result.get("results", []),
                )
            )
            if row["channel"] in ("batch", "structure") and outcome in (
                "unknown",
                "timeout",
                "invalid_effect",
            ):
                self.ledger.bump("unknown", row)
            if outcome != "success":
                self.stop("request " + outcome)
        elif event["kind"] == "transport_failure":
            self.ledger.emit(
                dict(
                    kind="unknown_dispatch", id=rid, error_type=event.get("error_type")
                )
            )
            if token:
                self.pool.finish(token, success=False)
            self.stop("transport worker failure before confirmed send")
            # Do not invent sent or not_sent when the transport boundary is unknown.
            return
        else:
            raise ValueError("invalid completion event")
        self.requests.pop(rid)
        self.limits.release(rid)

    def tick(self, now):
        for event in self.dispatcher.poll(now):
            self.accept(event)
        for rid, (row, token) in list(self.requests.items()):
            active = self.ledger.active[rid]
            if (
                rid not in self.terminal_unknown
                and active["status"] == "sent"
                and now - active["sent_ns"] >= 10 * NS
                and row["channel"] != "verify"
            ):
                self.terminal_unknown.add(rid)
                self.ledger.bump("timeout", row)
                self.ledger.bump("unknown", row)
                self.ledger.emit(
                    dict(
                        kind="pending_unknown",
                        id=rid,
                        at_ns=now,
                        reason="RPC deadline without completion",
                    )
                )
                if token:
                    self.pool.finish(token, success=False)
                self.stop("RPC deadline; completion unknown")
        if self.guard:
            self.guard.check(
                now_ns=now,
                last_child_ns=self.last_child_ns,
                offered=self.ledger.counts["offered"],
                measuring=now < self.grid.end_ns,
            )
        if self.stopped:
            return
        expired, index = self.grid.poll(now)
        self.ledger.expired_range(self.grid, *expired)
        if index is not None:
            stage = index % 10
            reason = self.limits.reason("child")
            token = None if reason else self.pool.reserve(stage)
            if token is None:
                row = self._row(
                    STEPS[stage][0],
                    "child",
                    self.grid.at(index),
                    self.grid.at(index + 1),
                    now,
                )
                row["stage"] = stage
                self.ledger.offered(row)
                self.ledger.not_sent(row, reason or "stage_busy")
            else:
                self._pool_job(token, self.grid.at(index), self.grid.at(index + 1), now)
        if now >= self.grid.end_ns:
            return
        # R pipelines are bounded by task, across BOTH distinct control RPCs.
        for i, task in enumerate(self.pool.tasks):
            if task.stage not in (10, 11) or task.busy or task.unknown:
                continue
            if i not in self.limits.rotating and len(self.limits.rotating) >= 16:
                continue
            self.limits.rotating.add(i)
            self.pool.next[task.stage] = i
            token = self.pool.reserve(task.stage)
            self._pool_job(token, now, min(self.grid.end_ns, now + 10 * NS), now)
        for channel in ("batch", "structure", "since", "detail", "status"):
            if channel == "batch" and self.settings["tasks"] != 10000:
                continue
            if channel == "structure" and self.variant not in ("L", "R"):
                continue
            interval = 10 if channel == "batch" else 60 if channel == "structure" else 1
            target = self.cursors[channel]
            while (
                self.grid.start_ns + target * NS <= now
                and target < self.settings["seconds"]
            ):
                scheduled = self.grid.start_ns + target * NS
                self.cursors[channel] = target + interval
                expires = min(self.grid.end_ns, scheduled + interval * NS)
                operation = channel
                payload = {}
                path = None
                if channel == "batch":
                    operation = "force"
                    payload = dict(
                        paths=[f"/flow008/task{i:04d}:batch_event" for i in range(100)],
                        state="set" if target // 10 % 2 else "clear",
                        recursive=False,
                    )
                    path = "/flow008"
                elif channel == "structure":
                    label = ("load-" if self.variant == "L" else "replace-") + (
                        "A" if target == 60 else "B"
                    )
                    operation = "load" if self.variant == "L" else "replace"
                    payload = dict(flow_bytes=self.templates[label])
                    if operation == "replace":
                        payload["target_path"] = "/flow009"
                    else:
                        payload["flow_type"] = "json"
                    path = (
                        "/flow009"
                        if operation == "replace"
                        else "/flow010"
                        if target == 60
                        else "/flow011"
                    )
                elif channel == "detail":
                    path = self.pool.tasks[target % 250].path
                    payload = dict(path=path)
                row = self._row(operation, channel, scheduled, expires, now, path=path)
                if now >= expires:
                    self.ledger.offered(row)
                    self.ledger.not_sent(row, "expired")
                else:
                    self._submit(row, payload)
                target = self.cursors[channel]

    def submit_verification(self, now, deadline_ns):
        if self.grid.start_ns <= now < self.grid.end_ns:
            raise ValueError("full verification forbidden during measurement")
        row = self._row(
            "full_verify", "verify", now, min(deadline_ns, now + 60 * NS), now
        )
        old = self.phase_deadline_ns
        self.phase_deadline_ns = min(deadline_ns, now + 60 * NS)
        try:
            return self._submit(row, {})
        finally:
            self.phase_deadline_ns = old

    def finish(self):
        return dict(
            task_id="PERF-04",
            stopped=self.stopped,
            ledger=self.ledger.snapshot(),
            limits_peak=dict(self.limits.peak),
            r_pipelines=len(self.limits.rotating),
            pool=[
                dict(
                    path=t.path,
                    stage=t.stage,
                    attempt=t.attempt,
                    unknown=t.unknown,
                    busy=t.busy,
                )
                for t in self.pool.tasks
            ],
        )
