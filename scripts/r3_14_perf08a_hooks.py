"""Only PERF-08A boundaries: diagnostic metadata, publication and resource limits."""

import contextvars
import time

from r3_14_observation import KINDS
from r3_14_phase_probe import _page_identity

KINDS.update(
    {
        29: "summary_capture_link",
        30: "summary_rpc_boundary",
        31: "summary_attempt_reply",
        32: "checkpoint_outcome",
        33: "checkpoint_exit_total",
    }
)
ATTEMPT = contextvars.ContextVar("perf08a_attempt", default=(0, 0, 0))
EXIT_SAVE = contextvars.ContextVar("perf08a_exit_save", default=False)
METADATA_KEY = "x-perf08a-attempt"


def install_capture_link(server, emitter):
    """Install before SummaryPhaseProbe; asyncio.to_thread carries this context."""
    service = server.scheduler.query_service
    capture = service._capture

    def linked(*args):
        pid, rid, attempt = ATTEMPT.get()
        emitter.emit(29, a=pid, b=rid, c=attempt, identity=args[3])
        return capture(*args)

    service._capture = linked
    transport = server.grpc_transport
    original = transport.ReadPage

    async def read_page(request, context):
        key = next(
            (v for k, v in context.invocation_metadata() if k == METADATA_KEY), ""
        )
        parts = tuple(map(int, key.split(":"))) if key else (0, 0, 0)
        if len(parts) != 3:
            raise ValueError("invalid diagnostic attempt key")
        token = ATTEMPT.set(parts)
        started = time.monotonic_ns()
        snapshot, page = None, None
        failed = True
        try:
            reply = await original(request, context)
            snapshot, page = _page_identity(bytes(reply.json))
            failed = False
            return reply
        finally:
            emitter.emit(
                30,
                started,
                time.monotonic_ns(),
                a=parts[0],
                b=parts[1],
                c=parts[2],
                d=page or 0,
                e=int(failed),
                identity=snapshot or "",
            )
            ATTEMPT.reset(token)

    transport.ReadPage = read_page


def install_checkpoint_boundaries(observation):
    manager, emitter = observation.server.checkpoint_manager, observation.emitter
    original_publish = next(
        original
        for obj, name, original in observation.originals
        if obj is manager and name == "_publish_prepared"
    )

    def publish(*args):
        started = time.monotonic_ns()
        result = original_publish(*args)
        emitter.emit(
            7,
            started,
            time.monotonic_ns(),
            a=args[0],
            b=int(result[0]),
            c=int(result[1]),
            d=int(EXIT_SAVE.get()),
        )
        return result

    manager._publish_prepared = publish
    original_outcome = manager._record_outcome

    def outcome(sequence, success, published, saved_at, reason):
        emitter.emit(
            32,
            a=sequence,
            b=int(success),
            c=int(published),
            d=int(EXIT_SAVE.get()),
            identity=reason or "",
        )
        return original_outcome(sequence, success, published, saved_at, reason)

    original_sync = manager.write_checkpoint

    def sync():
        token = EXIT_SAVE.set(True)
        started, success = time.monotonic_ns(), False
        try:
            success = original_sync()
            return success
        finally:
            emitter.emit(33, started, time.monotonic_ns(), a=int(success))
            EXIT_SAVE.reset(token)

    observation.originals.extend(
        [
            (manager, "_record_outcome", original_outcome),
            (manager, "write_checkpoint", original_sync),
        ]
    )
    manager._record_outcome = outcome
    manager.write_checkpoint = sync


def attempt_metadata(kwargs, pid, rid, attempt):
    result = dict(kwargs)
    result["metadata"] = [
        *result.get("metadata", ()),
        (METADATA_KEY, f"{pid}:{rid}:{attempt}"),
    ]
    return result


def encode_reply(emitter, reply, rid, channel, attempt):
    sid, page = _page_identity(bytes(reply.json))
    emitter.emit(31, a=rid, b=channel, c=attempt, d=page or 0, identity=sid or "")


def resource_violation(sample, limits, previous):
    live = [p for p in sample["processes"] if not p.get("unavailable")]
    checks = {
        "owned_rss": sum(p["rss_bytes"] for p in live) > limits["owned_rss_bytes"],
        "collector_rss": any(
            p["role"] == "collector" and p["rss_bytes"] > limits["collector_rss_bytes"]
            for p in live
        ),
    }
    for name, over in checks.items():
        previous[name] = previous.get(name, 0) + 1 if over else 0
        if previous[name] >= limits["consecutive_points"]:
            return name + " exceeded for consecutive resource points"
    return None


def missed_slots(target, now, end, period):
    """Yield skipped fixed slots after a completed request; never catch up."""
    while target < min(now, end):
        yield target
        target += period


def arm_hard_deadline(processes, seconds, run_dir):
    """Independent wall deadline also interrupts a blocked shutdown/handshake."""
    import os
    import signal
    import threading
    from r3_14_isolated_worker import save

    def expire():
        save(
            run_dir / "hard-deadline.json",
            dict(at_ns=time.monotonic_ns(), reason="whole run hard limit"),
        )
        for process in list(processes):
            if process.proc.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    timer = threading.Timer(seconds, expire)
    timer.daemon = True
    timer.start()
    return timer
