"""Preservation property tests for the *server-exception-resilience* bugfix.

These tests encode **Property 5: Preservation** (a.k.a. "Property 2" in the
spec ``tasks.md`` task list) from the feature design. They lock in the
*success-path* behavior that must remain **unchanged** by the fix -- i.e. every
input that does **not** trigger ``isBugCondition`` (no unexpected exception) must
keep behaving exactly as it does today (``design.md`` Preservation Requirements,
acceptance criteria 3.1 - 3.4).

Observation-first methodology
-----------------------------
Following the spec's observation-first approach, the behaviors locked in here
were first observed on the **UNFIXED** code:

* An error-free :meth:`Scheduler.main_loop` iteration calls
  ``update_calendar`` for *every* flow and resolves dependencies for *every*
  flow via ``travel_bunch``, then continues on the next interval (Req 3.1).
* A command handler (``RunCommandComplete`` / ``RunCommandInit`` / ...) invoked
  with a valid ``node_path`` pointing to a :class:`Task` performs the operation
  and returns ``ServiceResponse(flag=0, message="")`` (Req 3.2, 3.3).
* The query handlers ``RunRequestShow`` / ``RunRequestPing`` / ``QueryCoroutine``
  return their corresponding success response types ``ShowResponse`` /
  ``PingResponse`` / ``CoroutineResponse`` (Req 3.2).
* :meth:`TaklerServer.stop` cleanly shuts the scheduler and network service down
  without raising (Req 3.4).

Because these are *preservation* checks of existing behavior, the tests **PASS
on the UNFIXED code** (and must continue to pass after the fix lands, proving no
regression).

Property-based testing
----------------------
Per the design's testing strategy, preservation is validated with
property-based tests (the repo's existing ``hypothesis``) so the success-path
invariants are exercised across a wide input domain (random valid command
sequences, valid node paths, legal meter values, and random sets of error-free
flows). Both property tests and focused unit/example tests are provided.

There is no ``pytest-asyncio`` in this project (see ``pyproject.toml``), so the
async handlers / loop are driven with :func:`asyncio.run`, mirroring the
existing ``tests/logging/test_server_logging_integration.py`` and
``tests/server/test_exception_resilience_bug_condition.py`` conventions.

Validates: Requirements 3.1, 3.2, 3.3, 3.4
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
from unittest import mock

from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from takler.core import Bunch, Flow, NodeStatus
from takler.server import TaklerServer
from takler.server.network_service import TaklerService
from takler.server.protocol import takler_pb2
from takler.server.scheduler import Scheduler


# ---------------------------------------------------------------------------
# Strategies / helpers
# ---------------------------------------------------------------------------

# Valid, simple node names: start with a letter, then letters/digits/underscore.
_task_names = st.from_regex(r"[a-z][a-z0-9_]{0,7}", fullmatch=True)


def _unique_names(min_size: int, max_size: int):
    """A strategy producing a list of distinct valid task names."""
    return st.lists(_task_names, min_size=min_size, max_size=max_size, unique=True)


def _make_bunch() -> Bunch:
    return Bunch(host="localhost", port="33999")


def _make_service_with_tasks(task_names) -> TaklerService:
    """Build a hermetic ``TaklerService`` over a bunch ``/flow1/<task_name>``.

    The gRPC server is never started, so no port is bound (mirrors the hermetic
    setup used by the bug-condition and logging integration tests).
    """
    bunch = _make_bunch()
    flow = Flow("flow1")
    for name in task_names:
        flow.add_task(name)
    bunch.add_flow(flow)
    # The flow is begun so control commands (requeue / run / force / free-dep)
    # are valid requests: the ``_require_begun`` guard rejects them on an
    # un-begun flow (Requirement 8.10).
    flow.begin()
    scheduler = Scheduler(bunch=bunch)
    return TaklerService(scheduler=scheduler, host="[::]", port=33999)


def _free_port() -> int:
    """Return a currently-free TCP port for a hermetic server bind."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


# ---------------------------------------------------------------------------
# Stubs for the scheduler-main-loop preservation property
# ---------------------------------------------------------------------------


class RecordingFlow:
    """A minimal error-free flow stub that records how often it is processed.

    The scheduler main loop only calls ``update_calendar`` and
    ``resolve_dependencies`` on each flow, so this stub is sufficient to observe
    that an error-free iteration touches *every* flow (Req 3.1). It mirrors the
    stub used by the bug-condition exploration tests.
    """

    def __init__(self, name: str):
        self.name = name
        # The main loop only processes begun flows (Requirement 8.9), so the
        # stub presents itself as begun.
        self.begun = True
        self.calendar_updates = 0
        self.dependency_resolutions = 0

    def update_calendar(self, time):  # noqa: D401 - simple stub
        self.calendar_updates += 1

    def resolve_dependencies(self) -> bool:
        self.dependency_resolutions += 1
        return False


def _drive_main_loop_for_a_while(scheduler: Scheduler, run_seconds: float = 0.08):
    """Run ``scheduler.main_loop`` briefly then stop it cleanly.

    Returns ``survived`` -- True if the loop task was still running (i.e. it kept
    iterating by interval and did not terminate) when we asked it to stop. The
    task is always torn down before returning.
    """

    async def runner():
        task = asyncio.create_task(scheduler.main_loop())
        await asyncio.sleep(run_seconds)

        survived = not task.done()

        scheduler.should_stop = True
        if task.done():
            task.exception()  # retrieve to avoid "never retrieved" warning
        else:
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except asyncio.TimeoutError:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        return survived

    return asyncio.run(runner())


# ===========================================================================
# Property 1 -- error-free main loop updates calendars and resolves deps for
# ALL flows and continues by interval (Requirement 3.1).
# ===========================================================================


@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(flow_names=_unique_names(min_size=1, max_size=5))
def test_error_free_iteration_processes_all_flows_and_continues(flow_names):
    """An error-free iteration touches every flow and keeps running.

    Preservation -- Requirement 3.1: for any set of error-free flows, a single
    main-loop pass updates the calendar and resolves dependencies for *every*
    flow, and the loop continues iterating by interval (it does not terminate).

    Asserted on the UNFIXED code (and must remain true after the fix).
    """
    bunch = _make_bunch()
    flows = {name: RecordingFlow(name) for name in flow_names}
    bunch.flows.update(flows)

    # Small interval so several iterations happen within ``run_seconds``,
    # demonstrating the loop continues "by interval".
    scheduler = Scheduler(bunch=bunch, interval_main_loop=0.01)

    survived = _drive_main_loop_for_a_while(scheduler, run_seconds=0.08)

    # The loop kept running across intervals (it was not terminated).
    assert survived, "error-free main loop must keep iterating by interval"

    for name, flow in flows.items():
        # Every flow had its calendar updated and dependencies resolved.
        assert flow.calendar_updates >= 1, (
            f"flow {name!r} calendar was never updated in an error-free iteration"
        )
        assert flow.dependency_resolutions >= 1, (
            f"flow {name!r} dependencies were never resolved in an error-free iteration"
        )
        # update_calendar and resolve_dependencies are called the same number of
        # times per iteration, so the counts must stay in lock-step (no flow is
        # partially skipped on the success path).
        assert flow.calendar_updates == flow.dependency_resolutions, (
            f"flow {name!r} was processed inconsistently: "
            f"{flow.calendar_updates} calendar updates vs "
            f"{flow.dependency_resolutions} dependency resolutions"
        )


# ===========================================================================
# Property 2 -- valid command handlers return flag=0 / empty message AND the
# operation actually executes (Requirements 3.2, 3.3).
# ===========================================================================


@settings(max_examples=30, deadline=None)
@given(
    task_name=_task_names,
    task_id=st.text(
        alphabet=st.characters(min_codepoint=48, max_codepoint=122), max_size=8
    ),
)
def test_valid_complete_then_init_execute_and_return_success(task_name, task_id):
    """``RunCommandComplete`` / ``RunCommandInit`` on a valid Task succeed.

    Preservation -- Requirements 3.2, 3.3: a command targeting a valid node path
    pointing to a :class:`Task` performs the requested operation and returns
    ``ServiceResponse(flag=0, message="")``.
    """
    service = _make_service_with_tasks([task_name])
    node_path = f"/flow1/{task_name}"

    # --- RunCommandComplete: operation executes, node becomes complete. ---
    complete_req = mock.MagicMock()
    complete_req.child_options.node_path = node_path
    context = mock.MagicMock()

    complete_resp = asyncio.run(service.RunCommandComplete(complete_req, context))

    assert isinstance(complete_resp, takler_pb2.ServiceResponse)
    assert complete_resp.flag == 0
    assert complete_resp.message == ""
    node = service.scheduler.bunch.find_node(node_path)
    assert node.state.node_status == NodeStatus.complete, (
        "Complete command did not execute the operation"
    )

    # --- RunCommandInit: operation executes, node becomes active with task_id. ---
    init_req = mock.MagicMock()
    init_req.child_options.node_path = node_path
    init_req.task_id = task_id

    init_resp = asyncio.run(service.RunCommandInit(init_req, context))

    assert isinstance(init_resp, takler_pb2.ServiceResponse)
    assert init_resp.flag == 0
    assert init_resp.message == ""
    assert node.state.node_status == NodeStatus.active, (
        "Init command did not execute the operation"
    )
    assert node.task_id == task_id, "Init command did not record the task id"


# A command sequence applied to valid tasks must keep returning flag=0 / "".
# Each command is a no-throw operation against an existing Task node.
_VALID_COMMANDS = st.sampled_from(
    ["complete", "init", "abort", "suspend", "resume", "requeue"]
)


@settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(
    task_names=_unique_names(min_size=1, max_size=3),
    commands=st.lists(
        st.tuples(st.integers(min_value=0, max_value=2), _VALID_COMMANDS),
        min_size=1,
        max_size=8,
    ),
)
def test_valid_command_sequence_always_returns_success(task_names, commands):
    """Any sequence of valid commands returns flag=0 with an empty message.

    Preservation -- Requirements 3.2, 3.3: random valid command sequences over
    valid node paths must each succeed (``flag=0``, empty ``message``) and not
    raise -- the success path is unchanged by the fix.
    """
    service = _make_service_with_tasks(task_names)
    context = mock.MagicMock()

    for task_index, command in commands:
        # Map the generated index onto an existing task (always a valid path).
        name = task_names[task_index % len(task_names)]
        node_path = f"/flow1/{name}"

        request = mock.MagicMock()
        request.child_options.node_path = node_path
        request.node_path = [node_path]
        request.task_id = "rid-1"
        request.reason = "because"

        if command == "complete":
            response = asyncio.run(service.RunCommandComplete(request, context))
        elif command == "init":
            response = asyncio.run(service.RunCommandInit(request, context))
        elif command == "abort":
            response = asyncio.run(service.RunCommandAbort(request, context))
        elif command == "suspend":
            response = asyncio.run(service.RunCommandSuspend(request, context))
        elif command == "resume":
            response = asyncio.run(service.RunCommandResume(request, context))
        elif command == "requeue":
            response = asyncio.run(service.RunCommandRequeue(request, context))
        else:  # pragma: no cover - guarded by the strategy
            raise AssertionError(f"unexpected command {command!r}")

        assert isinstance(response, takler_pb2.ServiceResponse)
        assert response.flag == 0, (
            f"valid {command} on {node_path} returned flag={response.flag!r}"
        )
        assert response.message == "", (
            f"valid {command} on {node_path} returned message={response.message!r}"
        )


@settings(max_examples=40, deadline=None)
@given(data=st.data())
def test_valid_meter_value_executes_and_returns_success(data):
    """``RunCommandMeter`` with a legal value succeeds and updates the meter.

    Preservation -- Requirements 3.2, 3.3: a legal (in-range) meter value is
    applied to the meter and the handler returns ``ServiceResponse(flag=0,
    message="")``.
    """
    task_name = data.draw(_task_names)
    min_value = data.draw(st.integers(min_value=-100, max_value=100))
    max_value = data.draw(st.integers(min_value=min_value, max_value=min_value + 200))
    meter_value = data.draw(st.integers(min_value=min_value, max_value=max_value))

    bunch = _make_bunch()
    flow = Flow("flow1")
    task = flow.add_task(task_name)
    task.add_meter("m", min_value, max_value)
    bunch.add_flow(flow)
    scheduler = Scheduler(bunch=bunch)
    service = TaklerService(scheduler=scheduler, host="[::]", port=33999)

    node_path = f"/flow1/{task_name}"
    request = mock.MagicMock()
    request.child_options.node_path = node_path
    request.meter_name = "m"
    request.meter_value = str(meter_value)
    context = mock.MagicMock()

    response = asyncio.run(service.RunCommandMeter(request, context))

    assert isinstance(response, takler_pb2.ServiceResponse)
    assert response.flag == 0
    assert response.message == ""
    assert task.find_meter("m").value == meter_value, (
        "Meter command did not apply the legal value"
    )


# ===========================================================================
# Property 3 -- query handlers return their success response types (Req 3.2).
# ===========================================================================


@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(
    task_names=_unique_names(min_size=0, max_size=4),
    flags=st.tuples(*[st.booleans()] * 5),
)
def test_query_handlers_return_success_response_types(task_names, flags):
    """``RunRequestShow`` / ``RunRequestPing`` / ``QueryCoroutine`` succeed.

    Preservation -- Requirement 3.2: the query handlers return their existing
    success response types (``ShowResponse`` / ``PingResponse`` /
    ``CoroutineResponse``) for any valid request.
    """
    service = _make_service_with_tasks(task_names)
    context = mock.MagicMock()

    show_parameter, show_trigger, show_limit, show_event, show_meter = flags
    show_req = mock.MagicMock()
    show_req.show_parameter = show_parameter
    show_req.show_trigger = show_trigger
    show_req.show_limit = show_limit
    show_req.show_event = show_event
    show_req.show_meter = show_meter

    show_resp = asyncio.run(service.RunRequestShow(show_req, context))
    assert isinstance(show_resp, takler_pb2.ShowResponse)
    # The output is the JSON serialization of the bunch -- it must parse.
    json.loads(show_resp.output)

    ping_resp = asyncio.run(service.RunRequestPing(mock.MagicMock(), context))
    assert isinstance(ping_resp, takler_pb2.PingResponse)

    coroutine_resp = asyncio.run(service.QueryCoroutine(mock.MagicMock(), context))
    assert isinstance(coroutine_resp, takler_pb2.CoroutineResponse)


# ===========================================================================
# Property 4 / clean shutdown -- TaklerServer.stop() shuts down cleanly without
# raising (Requirement 3.4).
# ===========================================================================


def _run_start_then_stop(server: TaklerServer):
    """Start a real server, run briefly, then stop it; return any exception.

    Returns ``None`` if ``stop()`` completed cleanly, otherwise the exception
    raised during shutdown.
    """

    async def runner():
        await server.start()
        loop = asyncio.get_running_loop()
        run_task = loop.create_task(server.run(), name="takler.server.test")
        # Let the scheduler main loop iterate at least once.
        await asyncio.sleep(0.05)

        error = None
        try:
            await asyncio.wait_for(server.stop(), timeout=5.0)
        except Exception as exc:  # noqa: BLE001 - this is exactly what we test for
            error = exc

        # Tear the run task down so nothing leaks regardless of outcome.
        if not run_task.done():
            try:
                await asyncio.wait_for(run_task, timeout=2.0)
            except (asyncio.TimeoutError, Exception):  # noqa: BLE001
                run_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await run_task
        else:
            run_task.exception()  # retrieve to silence asyncio warnings
        return error

    return asyncio.run(runner())


@settings(
    max_examples=4,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(flow_names=_unique_names(min_size=0, max_size=2))
def test_stop_shuts_down_cleanly_without_raising(flow_names, tmp_path):
    """``TaklerServer.stop()`` cleanly stops scheduler and network service.

    Preservation -- Requirement 3.4: stopping the server must tear down the
    scheduler and the gRPC network service without raising, regardless of how
    many (error-free) flows are loaded.
    """
    # A clean shutdown writes the final snapshot (Requirement 5.9), and the
    # default Checkpoint_File path is ``takler.check`` relative to the current
    # working directory -- point it at ``tmp_path`` so the suite does not drop
    # snapshots into the source tree.
    server = TaklerServer(
        host="localhost",
        port=_free_port(),
        checkpoint_file=tmp_path / "takler.check",
    )
    # Use a tiny interval so the main loop notices ``should_stop`` promptly.
    server.scheduler.interval_main_loop = 0.01

    tasks = []
    for name in flow_names:
        flow = Flow(name)
        task = flow.add_task("task1")
        server.bunch.add_flow(flow)
        # begin starts the calendar so error-free iterations are guaranteed.
        flow.begin()
        assert task.state.node_status == NodeStatus.queued
        tasks.append(task)

    error = _run_start_then_stop(server)

    assert error is None, f"stop() raised during clean shutdown: {error!r}"
    # After a clean shutdown the scheduler's stop flag has been unset again.
    assert server.scheduler.should_stop is False
    # The main loop skips flows which have not begun (Requirement 8.9), so a
    # test whose flows were never begun would still shut down cleanly while
    # doing nothing at all. Assert the expected status progression to prove the
    # loop really processed these flows: a dependency-free queued task is
    # submitted on the first pass.
    for task in tasks:
        assert task.state.node_status == NodeStatus.submitted, (
            f"main loop did not process {task.node_path}: status is "
            f"{task.state.node_status!r}, expected submitted"
        )
        assert task.try_no == 1, (
            f"main loop did not run {task.node_path} exactly once: "
            f"try_no is {task.try_no}"
        )
