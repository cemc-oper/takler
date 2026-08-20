"""Bug-condition exploration tests for the *server-exception-resilience* bugfix.

These tests encode **Property 1: Bug Condition** from the feature design. They
deliberately assert the *expected (post-fix) resilient behavior* described in
``design.md`` (Expected Behavior 2.1, 2.2, 2.3):

* An exception raised while a single flow is processed inside
  :meth:`Scheduler.main_loop` must be caught/logged and **only that flow
  skipped** -- the loop (and therefore the awaited scheduler task and the
  server process) must keep running and continue processing the remaining
  flows on the next interval.
* An exception raised by a scheduler operation invoked from a
  :class:`TaklerService` RPC handler must be caught/logged and converted into a
  ``ServiceResponse`` with a non-zero ``flag`` and a descriptive ``message``
  -- the exception must not escape the handler / abort the RPC.

Bug-exploration semantics (see the spec ``tasks.md`` task 1)
------------------------------------------------------------
On the **UNFIXED** code these expected-behavior assertions FAIL, and that
failure is the *success* outcome for this task -- it confirms the bug exists:

* the scheduler main loop has no per-flow exception boundary, so a single
  throwing flow propagates out of the awaited scheduler task and terminates the
  whole server (Bug Analysis 1.1, 1.2);
* the RPC handlers call scheduler operations without any wrapping, so
  ``ValueError`` / ``RuntimeError`` / ``json.JSONDecodeError`` escape and abort
  the RPC with an opaque transport error instead of a meaningful response
  (Bug Analysis 1.3).

After the fix lands, these *same* tests flip to passing, validating the fix.

Scoped property-based testing
-----------------------------
The defect is deterministic, so each property is narrowed (via ``hypothesis``
``sampled_from`` / small string strategies) to the concrete failing cases for
reproducibility, following the "Scoped PBT" approach in the design's testing
strategy.

There is no ``pytest-asyncio`` in this project (see ``pyproject.toml``), so the
async handlers / loop are driven with :func:`asyncio.run`, mirroring the
existing ``tests/logging/test_server_logging_integration.py`` convention.

Validates: Requirements 1.1, 1.2, 1.3, 1.4
"""

from __future__ import annotations

import asyncio
import contextlib
from unittest import mock

from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from takler.core import Bunch, Flow
from takler.server.network_service import TaklerService
from takler.server.protocol import takler_pb2
from takler.server.scheduler import Scheduler
from takler.server import TaklerServer
from takler.server.connect_config import ExceptionPolicy


# ---------------------------------------------------------------------------
# Test doubles / helpers
# ---------------------------------------------------------------------------


class RecordingFlow:
    """A minimal flow stub that records how often it is processed.

    The scheduler main loop only calls ``update_calendar`` and
    ``resolve_dependencies`` on each flow, so a lightweight stub is sufficient
    to observe whether a *healthy* flow keeps being processed while another
    flow throws.
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


class ThrowingFlow:
    """A flow stub that raises from ``update_calendar`` or ``resolve_dependencies``.

    Which method throws (and the exception type) is parametrized by the
    scoped property test so the concrete failing cases are reproducible.
    """

    def __init__(self, name: str, throw_in: str, exc_type: type):
        self.name = name
        # The main loop only processes begun flows (Requirement 8.9), so the
        # stub presents itself as begun.
        self.begun = True
        self._throw_in = throw_in
        self._exc_type = exc_type

    def update_calendar(self, time):
        if self._throw_in == "update_calendar":
            raise self._exc_type(f"boom in update_calendar for {self.name}")

    def resolve_dependencies(self) -> bool:
        if self._throw_in == "resolve_dependencies":
            raise self._exc_type(f"boom in resolve_dependencies for {self.name}")
        return False


def _make_bunch() -> Bunch:
    return Bunch(host="localhost", port="33999")


def _make_service_with_task() -> TaklerService:
    """Build a TaklerService over a real bunch containing /flow1/task1.

    The gRPC server is never started, so no port is bound (mirrors the hermetic
    setup used by the logging integration tests).
    """
    bunch = _make_bunch()
    flow = Flow("flow1")
    flow.add_task("task1")
    bunch.add_flow(flow)
    scheduler = Scheduler(bunch=bunch)
    return TaklerService(scheduler=scheduler, host="[::]", port=33999)


def _drive_main_loop(scheduler: Scheduler, run_seconds: float = 0.1):
    """Run ``scheduler.main_loop`` briefly; report whether it survived.

    Returns ``(survived, exception)`` where ``survived`` is True if the loop
    task was still running (i.e. it did *not* terminate due to an unhandled
    exception) after ``run_seconds``. The task is always cleaned up before
    returning so no "task exception was never retrieved" warning leaks.
    """

    async def runner():
        task = asyncio.create_task(scheduler.main_loop())
        await asyncio.sleep(run_seconds)

        survived = not task.done()

        # Ask the loop to stop and tear the task down cleanly.
        scheduler.should_stop = True
        if task.done():
            # Retrieve the exception so asyncio does not warn about it.
            exc = task.exception()
        else:
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except asyncio.TimeoutError:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            exc = None
        return survived, exc

    return asyncio.run(runner())


# ---------------------------------------------------------------------------
# Test A -- Scheduler main loop
# ---------------------------------------------------------------------------


@settings(
    max_examples=12,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(
    throw_in=st.sampled_from(["update_calendar", "resolve_dependencies"]),
    exc_type=st.sampled_from([RuntimeError, ValueError, KeyError]),
)
def test_scheduler_main_loop_isolates_throwing_flow(throw_in, exc_type):
    """Test A: a single throwing flow must not terminate the scheduler loop.

    Expected (post-fix, RESILIENT) behavior -- Requirements 1.1, 1.2 / 2.1, 2.2:
    the exception is caught and the offending flow skipped, the loop keeps
    running and the healthy flow continues to be processed.

    On UNFIXED code the exception propagates out of the awaited
    ``main_loop`` task, so ``survived`` is False and this assertion fails --
    confirming the bug (a single bad flow terminates the whole server).
    """
    bunch = _make_bunch()
    # Insert the throwing flow first so that, on unfixed code, the healthy flow
    # downstream of it is starved once the exception escapes.
    throwing = ThrowingFlow("bad", throw_in=throw_in, exc_type=exc_type)
    healthy = RecordingFlow("good")
    bunch.flows["bad"] = throwing
    bunch.flows["good"] = healthy

    scheduler = Scheduler(bunch=bunch, interval_main_loop=0.01)

    survived, exc = _drive_main_loop(scheduler, run_seconds=0.1)

    # Primary expected-behavior assertion: the loop kept running despite the
    # throwing flow. (Fails on unfixed code -- the bug.)
    assert survived, (
        f"scheduler main loop terminated after {throw_in} raised "
        f"{exc_type.__name__}; unhandled exception escaped the awaited task: "
        f"{exc!r}"
    )

    # The healthy flow must still be processed while the bad flow is skipped.
    assert healthy.calendar_updates >= 1, (
        "healthy flow was never processed -- the throwing flow was not isolated"
    )


# ---------------------------------------------------------------------------
# Test B -- RPC node-not-found
# ---------------------------------------------------------------------------


@settings(max_examples=10, deadline=None)
@given(
    node_path=st.sampled_from(
        [
            "/flow1/missing_task",  # flow exists, node does not
            "/missing_flow/task1",  # flow does not exist
            "/flow1/container/deep/task",  # nothing along the path
        ]
    )
)
def test_rpc_complete_unknown_node_returns_error_response(node_path):
    """Test B: ``RunCommandComplete`` on a missing node must return an error.

    Expected (post-fix, RESILIENT) behavior -- Requirements 1.3 / 2.3: the
    ``ValueError`` raised by ``run_command_complete`` (node not found) is
    caught/logged and converted into a ``ServiceResponse`` with a non-zero
    ``flag`` and a descriptive ``message``; the handler does not raise.

    On UNFIXED code the ``ValueError`` escapes the handler (the RPC aborts with
    an opaque error), so this test fails -- confirming the bug.
    """
    service = _make_service_with_task()

    request = mock.MagicMock()
    request.child_options.node_path = node_path
    context = mock.MagicMock()

    response = asyncio.run(service.RunCommandComplete(request, context))

    assert isinstance(response, takler_pb2.ServiceResponse)
    assert response.flag != 0, (
        "expected a non-zero flag error response for an unknown node, "
        f"got flag={response.flag!r}"
    )
    assert response.message, "expected a descriptive error message"


# ---------------------------------------------------------------------------
# Test C -- RPC malformed / unsupported load payload
# ---------------------------------------------------------------------------


@settings(max_examples=15, deadline=None)
@given(
    payload=st.sampled_from(
        [
            ("json", b"not json at all"),  # json.JSONDecodeError
            ("json", b"{ broken : json"),  # json.JSONDecodeError
            ("json", b""),  # json.JSONDecodeError
            ("yaml", b"a: 1"),  # unsupported flow_type -> RuntimeError
            ("xml", b"<flow/>"),  # unsupported flow_type -> RuntimeError
        ]
    )
)
def test_rpc_load_malformed_payload_returns_error_response(payload):
    """Test C: ``RunCommandLoad`` with bad input must return an error response.

    Expected (post-fix, RESILIENT) behavior -- Requirements 1.3 / 2.3: a
    ``json.JSONDecodeError`` (malformed JSON) or ``RuntimeError`` (unsupported
    ``flow_type``) is caught/logged and converted into a ``ServiceResponse``
    with a non-zero ``flag`` and a descriptive ``message``.

    On UNFIXED code the exception escapes the handler, so this test fails --
    confirming the bug.
    """
    flow_type, flow_bytes = payload
    service = _make_service_with_task()

    request = mock.MagicMock()
    request.flow_type = flow_type
    request.flow = flow_bytes
    context = mock.MagicMock()

    response = asyncio.run(service.RunCommandLoad(request, context))

    assert isinstance(response, takler_pb2.ServiceResponse)
    assert response.flag != 0, (
        "expected a non-zero flag error response for a malformed/unsupported "
        f"load payload {flow_type!r}, got flag={response.flag!r}"
    )
    assert response.message, "expected a descriptive error message"


# ---------------------------------------------------------------------------
# Test D -- RPC invalid meter value
# ---------------------------------------------------------------------------


@settings(max_examples=15, deadline=None)
@given(meter_value=st.sampled_from(["abc", "", "1.5", "ten", "  ", "0x10", "NaNish"]))
def test_rpc_meter_invalid_value_returns_error_response(meter_value):
    """Test D: ``RunCommandMeter`` with a non-int value must return an error.

    Expected (post-fix, RESILIENT) behavior -- Requirements 1.3 / 2.3: the
    ``ValueError`` raised by ``int(meter_value)`` inside ``run_command_meter``
    is caught/logged and converted into a ``ServiceResponse`` with a non-zero
    ``flag`` and a descriptive ``message``.

    The node ``/flow1/task1`` exists, so the failure is specifically the integer
    conversion (not a missing node).

    On UNFIXED code the ``ValueError`` escapes the handler, so this test fails
    -- confirming the bug.
    """
    # Guard the strategy: every sampled value must genuinely fail int().
    with contextlib.suppress(ValueError):
        int(meter_value)
        # If it parsed as an int, it is not a valid counterexample; skip it.
        return

    service = _make_service_with_task()

    request = mock.MagicMock()
    request.child_options.node_path = "/flow1/task1"
    request.meter_name = "m"
    request.meter_value = meter_value
    context = mock.MagicMock()

    response = asyncio.run(service.RunCommandMeter(request, context))

    assert isinstance(response, takler_pb2.ServiceResponse)
    assert response.flag != 0, (
        "expected a non-zero flag error response for an invalid meter value "
        f"{meter_value!r}, got flag={response.flag!r}"
    )
    assert response.message, "expected a descriptive error message"


# ===========================================================================
# FAIL_FAST fix-checking assertions (companion to the tests above)
# ===========================================================================
#
# The tests above exercise the default RESILIENT policy: exceptions are caught
# at the boundary, logged, and either isolated (scheduler) or converted into an
# error ``ServiceResponse`` (RPC), and the server keeps running. Task 3.6 also
# asks us to validate **Property 3: Bug Condition (Fail-Fast)** from
# ``design.md``:
#
#   For any scheduler main-loop iteration or RPC command handler, when an
#   exception is raised while the policy is FAIL_FAST, the fixed code SHALL log
#   the exception (with operation + error detail) and then trigger a clean
#   server-process shutdown.
#
# These assertions construct the Scheduler / TaklerService / TaklerServer with
# ``ExceptionPolicy.FAIL_FAST`` and assert that on an exception the failure is
# logged and the fatal-shutdown is triggered (via a mock callback and via the
# server's shared fatal-error event), and that the server goes through its
# unified clean-shutdown path without raising.
#
# Validates: Requirements 2.4, 2.5 (Property 3)


def _run_main_loop_to_completion(scheduler: Scheduler, timeout: float = 2.0):
    """Run ``scheduler.main_loop`` and report whether it terminated on its own.

    Returns ``True`` if the loop task completed within ``timeout`` (i.e. the
    FAIL_FAST break exited the loop). If it is still running after ``timeout``
    the task is cancelled/cleaned up and ``False`` is returned.
    """

    async def runner():
        task = asyncio.create_task(scheduler.main_loop())
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            scheduler.should_stop = True
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            return False

    return asyncio.run(runner())


def test_scheduler_fail_fast_logs_and_triggers_fatal_shutdown():
    """Scheduler FAIL_FAST: a throwing flow logs the error and triggers shutdown.

    Property 3 (scheduler path) -- Requirements 2.4, 2.5: under FAIL_FAST a flow
    that raises during processing must be logged and then the fatal-shutdown
    trigger fired, and the main loop must exit so the server can shut down.
    """
    bunch = _make_bunch()
    throwing = ThrowingFlow("bad", throw_in="update_calendar", exc_type=RuntimeError)
    healthy = RecordingFlow("good")
    bunch.flows["bad"] = throwing
    bunch.flows["good"] = healthy

    fatal_shutdown = mock.MagicMock(name="fatal_shutdown")
    scheduler = Scheduler(
        bunch=bunch,
        interval_main_loop=0.01,
        exception_policy=ExceptionPolicy.FAIL_FAST,
        fatal_shutdown=fatal_shutdown,
    )

    with mock.patch("takler.server.scheduler.logger") as mock_logger:
        terminated = _run_main_loop_to_completion(scheduler)

    # The loop exited on its own (FAIL_FAST broke out of it).
    assert terminated, "FAIL_FAST scheduler loop did not terminate after a flow raised"
    # The fatal-shutdown trigger was invoked exactly once.
    fatal_shutdown.assert_called_once_with()
    # The failure was logged with diagnostic context before shutdown.
    assert mock_logger.error.called, "the flow failure was not logged"
    logged = mock_logger.error.call_args[0][0]
    assert "bad" in logged, "log message is missing the flow name context"


def test_rpc_fail_fast_logs_and_triggers_fatal_shutdown():
    """RPC FAIL_FAST: a handler exception logs the error and triggers shutdown.

    Property 3 (RPC path) -- Requirements 2.4, 2.5: under FAIL_FAST an exception
    raised by a scheduler operation invoked from an RPC handler must be logged
    and then the fatal-shutdown trigger fired. An error response is still
    returned to the client before the server shuts down.
    """
    bunch = _make_bunch()
    flow = Flow("flow1")
    flow.add_task("task1")
    bunch.add_flow(flow)
    scheduler = Scheduler(bunch=bunch)

    fatal_shutdown = mock.MagicMock(name="fatal_shutdown")
    service = TaklerService(
        scheduler=scheduler,
        host="[::]",
        port=33999,
        exception_policy=ExceptionPolicy.FAIL_FAST,
        fatal_shutdown=fatal_shutdown,
    )

    request = mock.MagicMock()
    request.child_options.node_path = (
        "/flow1/missing_task"  # unknown node -> ValueError
    )
    context = mock.MagicMock()

    with mock.patch("takler.server.network_service.logger") as mock_logger:
        response = asyncio.run(service.RunCommandComplete(request, context))

    # The fatal-shutdown trigger was invoked (server will exit cleanly).
    fatal_shutdown.assert_called_once_with()
    # The failure was logged with diagnostic context.
    assert mock_logger.error.called, "the RPC failure was not logged"
    logged = mock_logger.error.call_args[0][0]
    assert "RunCommandComplete" in logged, (
        "log message is missing the RPC operation context"
    )
    # An error response is still returned to the client.
    assert isinstance(response, takler_pb2.ServiceResponse)
    assert response.flag != 0 and response.message


def test_server_fail_fast_signals_clean_exit():
    """Server FAIL_FAST: a scheduler flow exception raises the clean-exit signal.

    Property 3 (server path) -- Requirements 2.4, 2.5: constructing a
    ``TaklerServer`` with FAIL_FAST wires the shared fatal-error event into the
    scheduler. When a flow raises, the scheduler triggers that event, which is
    exactly the signal ``run()`` waits on (alongside the scheduler task) before
    driving its unified, clean shutdown path. We assert the signal is raised
    (so ``run()`` would wake and shut down cleanly) rather than binding a real
    gRPC port here.
    """
    server = TaklerServer(host="localhost", port=33999, exception_policy="FAIL_FAST")
    assert server.exception_policy is ExceptionPolicy.FAIL_FAST
    # The scheduler's fatal-shutdown trigger is wired to the server's own
    # ``_trigger_fatal_shutdown`` (which sets the shared event ``run()`` awaits).
    assert server.scheduler.fatal_shutdown == server._trigger_fatal_shutdown
    assert not server._fatal_error_event.is_set()

    # Inject a throwing flow into the server's own bunch.
    throwing = ThrowingFlow("bad", throw_in="resolve_dependencies", exc_type=ValueError)
    server.bunch.flows["bad"] = throwing

    # Drive the server's scheduler main loop; FAIL_FAST must set the server's
    # shared fatal-error event (the signal ``run()`` waits on to exit) and exit
    # the loop.
    terminated = _run_main_loop_to_completion(server.scheduler)

    assert terminated, "FAIL_FAST scheduler loop did not terminate"
    assert server._fatal_error_event.is_set(), (
        "server fatal-error event was not set -- run() would not wake to shut down"
    )

    # The clean-exit signal is idempotent: re-triggering keeps the event set and
    # does not raise, so the shared shutdown path is reached at most once.
    server._trigger_fatal_shutdown()
    assert server._fatal_error_event.is_set()
