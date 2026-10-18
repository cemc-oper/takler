"""Bounded process-local RUN call-tree timings; never replace product work."""

from contextlib import contextmanager
from functools import wraps
import time

from takler.core.bunch import Bunch
from takler.core.task_node import Task
from takler.server import batch
from takler.server.scheduler import Scheduler

MAX_SPANS = 64


@contextmanager
def timed_run_chain(scheduler, log):
    """Record only selected outer calls, without observing each tree node.

    Inclusive intervals retain parent IDs. Exclusive durations are calculated
    offline by subtracting direct children, never by summing nested intervals.
    The product's existing batch snapshots are timed, not newly performed.
    """
    active = None
    serial = 0
    originals = []

    def measure(name, operation, args, kwargs):
        if len(active["spans"]) >= MAX_SPANS:
            active["instrumentation_error"] = "span capacity exceeded"
            raise ValueError("RUN span capacity exceeded")
        span = dict(
            id=len(active["spans"]),
            name=name,
            parent=active["stack"][-1] if active["stack"] else None,
            start_ns=time.monotonic_ns(),
            end_ns=None,
            cpu_ns=None,
            error=None,
        )
        active["spans"].append(span)
        active["stack"].append(span["id"])
        cpu = time.process_time_ns()
        try:
            return operation(*args, **kwargs)
        except BaseException as exc:
            span["error"] = type(exc).__name__
            raise
        finally:
            span["end_ns"] = time.monotonic_ns()
            span["cpu_ns"] = time.process_time_ns() - cpu
            assert active["stack"].pop() == span["id"]

    def wrap(owner, attribute, name):
        operation = getattr(owner, attribute)

        @wraps(operation)
        def observed(*args, **kwargs):
            nonlocal active, serial
            if name == "Scheduler.run_command_run":
                if args[0] is not scheduler:
                    return operation(*args, **kwargs)
                if active is not None:
                    raise ValueError("concurrent RUN diagnostic")
                paths = list(args[1].node_paths)
                if len(paths) != 1:
                    raise ValueError("diagnostic requires one normal RUN target")
                serial += 1
                log("run_chain_started", run_id=serial, path=paths[0])
                active = dict(spans=[], stack=[], task=None, instrumentation_error=None)
                response = None
                try:
                    response = measure(name, operation, args, kwargs)
                    return response
                finally:
                    result, active = active, None
                    log(
                        "run_chain",
                        run_id=serial,
                        path=paths[0],
                        flag=getattr(response, "flag", None),
                        spans=result["spans"],
                        instrumentation_error=result["instrumentation_error"],
                    )
            if active is None:
                return operation(*args, **kwargs)
            if owner is Scheduler and args[0] is not scheduler:
                return operation(*args, **kwargs)
            if owner is Bunch and args[0] is not scheduler.bunch:
                return operation(*args, **kwargs)
            if owner is Task:
                if attribute == "run":
                    active["task"] = args[0]
                if args[0] is not active["task"]:
                    return operation(*args, **kwargs)
            return measure(name, operation, args, kwargs)

        had_attribute = attribute in vars(owner)
        originals.append((owner, attribute, had_attribute, vars(owner).get(attribute)))
        setattr(owner, attribute, observed)

    try:
        for owner, attribute in [
            (Scheduler, "run_command_run"),
            (Scheduler, "_run_node"),
            (Scheduler, "_require_begun"),
            (Bunch, "find_node"),
            (batch, "_snapshot"),
            (Bunch, "to_dict"),
            (batch, "deepcopy"),
            (Task, "run"),
            (Task, "_require_running"),
            (Task, "unresolved_limits_up"),
            (Task, "check_in_limit_up"),
            (Task, "increment_try_no"),
            (Task, "before_run"),
            (Task, "do_run"),
            (Task, "after_run"),
            (Task, "handle_status_change"),
            (Task, "update_limits"),
            (Task, "update_generated_parameters"),
            (Task, "notify_query_change"),
        ]:
            label = "batch" if owner is batch else owner.__name__
            wrap(owner, attribute, label + "." + attribute)
        yield
    finally:
        for owner, attribute, had_attribute, original in reversed(originals):
            if had_attribute:
                setattr(owner, attribute, original)
            else:
                delattr(owner, attribute)
