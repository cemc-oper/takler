"""Shared command-handler cases, driven through both handler layers.

This conftest is the M3 task 5 test base: one list of command scenarios,
:class:`HandlerCase`, plus the drivers and the assertion helper, exposed as
fixtures (the suite runs with ``--import-mode=importlib``, so sharing goes
through fixtures rather than module imports). Two test modules consume them --

* ``test_handlers_unit.py`` runs each case directly against
  :class:`~takler.server.handlers.CommandHandlers` (DTO in, DTO out),
* ``test_handlers_grpc_boundary.py`` runs the same case through the gRPC
  adapter (:class:`~takler.server.grpc_transport.GrpcTransport` with a pb2
  request), proving the adapter preserves the handler's semantics, and
* ``test_handlers_http_boundary.py`` runs it through the HTTP transport
  (:class:`~takler.server.http_transport.HttpTransport`'s FastAPI app with an
  envelope body), proving the JSON boundary preserves them too (M3 task 7).

A case carries the request as plain kwargs rather than a built DTO so the
parse -- including its failures, e.g. a non-numeric ``meter_value`` -- happens
inside the handler's exception boundary on every driver, exactly where the
production boundary runs it.
"""

from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest import mock

import pytest

from takler.core import Bunch, Flow, NodeStatus
from takler.protocol.commands import (
    REQUEST_TYPE_BY_COMMAND,
    BeginCommand,
    Command,
)
from takler.protocol.envelope import Envelope
from takler.server.handlers import METHOD_NAME_BY_COMMAND, CommandHandlers
from takler.server.grpc_transport import GrpcTransport
from takler.server.protocol import takler_pb2
from takler.server.scheduler import Scheduler

FLOW1 = "flow1"
TASK1 = "/flow1/container1/task1"
TASK2 = "/flow1/task2"

#: Error_Code values, stated literally on purpose: the cases pin the
#: classification, they do not derive it from the table under test.
CODE_SUCCESS = 0
CODE_NODE_NOT_FOUND = 10
CODE_UNSUPPORTED_VALUE = 13
CODE_FLOW_STATE = 14
CODE_INVALID_REQUEST = 15
CODE_INTERNAL_ERROR = 99


def build_scheduler() -> Scheduler:
    """The scenario bunch: ``flow1`` begun, ``flow2`` left un-begun.

    ``flow1`` carries one container with one task (owning an event and a
    meter) plus a bare task; ``flow2`` exists so the ``begin`` success case
    has a target.
    """
    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    flow = Flow(FLOW1)
    container1 = flow.add_container("container1")
    task1 = container1.add_task("task1")
    task1.add_event("event1")
    task1.add_meter("meter1", 0, 100)
    flow.add_task("task2")
    scheduler.bunch.add_flow(flow)
    flow2 = Flow("flow2")
    flow2.add_task("task1")
    scheduler.bunch.add_flow(flow2)
    scheduler.run_command_begin(BeginCommand(flow_name=FLOW1))
    return scheduler


@dataclass(frozen=True)
class HandlerCase:
    """One command scenario.

    Attributes:
        id: Pytest case id.
        command: The command to dispatch.
        kwargs: The request DTO's fields, as plain values.
        expected_flag: The ``flag`` of a ``ServiceResponse`` command.
        message_part: Substring the error ``message`` must carry.
        output_part: Substring a ``show`` response's ``output`` must carry.
        check: Extra state assertions against the bunch after the run.
    """

    id: str
    command: Command
    kwargs: Dict[str, Any]
    expected_flag: int = CODE_SUCCESS
    message_part: Optional[str] = None
    output_part: Optional[str] = None
    check: Optional[Callable[[Bunch], None]] = None


def _new_flow_bytes() -> bytes:
    flow = Flow("flow3")
    flow.add_task("task1")
    return json.dumps(flow.to_dict()).encode("utf-8")


HANDLER_CASES: List[HandlerCase] = [
    # -- child commands ------------------------------------------------
    HandlerCase(
        "init",
        Command.INIT,
        {"node_path": TASK1, "task_id": "job-42"},
        check=lambda bunch: (
            _assert_status(bunch, TASK1, NodeStatus.active),
            _assert_task_id(bunch, TASK1, "job-42"),
        ),
    ),
    HandlerCase(
        "complete",
        Command.COMPLETE,
        {"node_path": TASK1},
        check=lambda bunch: _assert_status(bunch, TASK1, NodeStatus.complete),
    ),
    HandlerCase(
        "abort",
        Command.ABORT,
        {"node_path": TASK1, "reason": "boom"},
        check=lambda bunch: (
            _assert_status(bunch, TASK1, NodeStatus.aborted),
            _assert_aborted_reason(bunch, TASK1, "boom"),
        ),
    ),
    HandlerCase(
        "event",
        Command.EVENT,
        {"node_path": TASK1, "event_name": "event1"},
        check=lambda bunch: _assert_event(bunch, TASK1, "event1", True),
    ),
    HandlerCase(
        "meter",
        Command.METER,
        {"node_path": TASK1, "meter_name": "meter1", "meter_value": 50},
        check=lambda bunch: _assert_meter(bunch, TASK1, "meter1", 50),
    ),
    HandlerCase(
        "meter-rejects-a-non-numeric-value",
        Command.METER,
        {"node_path": TASK1, "meter_name": "meter1", "meter_value": "abc"},
        # The DTO's ``int`` coercion fails; a ValidationError is a foreign
        # exception, classified as internal_error exactly like the ValueError
        # the scheduler's own ``int()`` used to raise.
        expected_flag=CODE_INTERNAL_ERROR,
        message_part="ValidationError",
        check=lambda bunch: _assert_meter(bunch, TASK1, "meter1", 0),
    ),
    HandlerCase(
        "complete-an-unknown-node",
        Command.COMPLETE,
        {"node_path": "/flow1/no_such"},
        expected_flag=CODE_NODE_NOT_FOUND,
        message_part="NodeNotFoundError",
    ),
    # -- control commands ------------------------------------------------
    HandlerCase(
        "requeue",
        Command.REQUEUE,
        {"node_paths": [f"/{FLOW1}"]},
        check=lambda bunch: _assert_status(bunch, TASK1, NodeStatus.queued),
    ),
    HandlerCase(
        "suspend",
        Command.SUSPEND,
        {"node_paths": [f"/{FLOW1}"]},
        check=lambda bunch: _assert_suspended(bunch, FLOW1, True),
    ),
    HandlerCase(
        "resume",
        Command.RESUME,
        {"node_paths": [TASK1]},
        check=lambda bunch: _assert_status(bunch, TASK1, NodeStatus.queued),
    ),
    HandlerCase(
        "run",
        Command.RUN,
        {"node_paths": [TASK1]},
    ),
    HandlerCase(
        "force-a-node",
        Command.FORCE,
        {"paths": [TASK1], "state": "complete", "recursive": True},
        check=lambda bunch: _assert_status(bunch, TASK1, NodeStatus.complete),
    ),
    HandlerCase(
        "force-an-event",
        Command.FORCE,
        {"paths": [f"{TASK1}:event1"], "state": "set", "recursive": True},
        check=lambda bunch: _assert_event(bunch, TASK1, "event1", True),
    ),
    HandlerCase(
        "free-dep",
        Command.FREE_DEP,
        {"paths": [TASK2], "dep_type": "all"},
    ),
    HandlerCase(
        "load",
        Command.LOAD,
        {"flow_type": "json", "flow_bytes": _new_flow_bytes()},
        check=lambda bunch: _assert_flow_loaded(bunch, "flow3"),
    ),
    HandlerCase(
        "load-rejects-bad-json",
        Command.LOAD,
        {"flow_type": "json", "flow_bytes": b"not a json"},
        expected_flag=CODE_INVALID_REQUEST,
        message_part="InvalidRequestError",
    ),
    HandlerCase(
        "load-rejects-an-unsupported-type",
        Command.LOAD,
        {"flow_type": "yaml", "flow_bytes": b""},
        expected_flag=CODE_UNSUPPORTED_VALUE,
        message_part="UnsupportedValueError",
    ),
    HandlerCase(
        "begin",
        Command.BEGIN,
        {"flow_name": "flow2"},
        check=lambda bunch: _assert_begun(bunch, "flow2", True),
    ),
    HandlerCase(
        "begin-an-unknown-flow",
        Command.BEGIN,
        {"flow_name": "no_such_flow"},
        expected_flag=16,
        message_part="failed=1",
    ),
    HandlerCase(
        "begin-an-already-begun-flow",
        Command.BEGIN,
        {"flow_name": FLOW1},
        expected_flag=16,
        message_part="failed=1",
    ),
    # -- query commands --------------------------------------------------
    HandlerCase(
        "show",
        Command.SHOW,
        {
            "show_trigger": False,
            "show_parameter": False,
            "show_limit": True,
            "show_event": True,
            "show_meter": True,
        },
        output_part=FLOW1,
    ),
    HandlerCase("ping", Command.PING, {}),
    HandlerCase("coroutine", Command.COROUTINE, {}),
]


# state assertions --------------------------------------------------------


def _assert_status(bunch: Bunch, node_path: str, status: NodeStatus) -> None:
    node = bunch.find_node(node_path)
    assert node is not None, node_path
    assert node.state.node_status is status, node_path


def _assert_task_id(bunch: Bunch, node_path: str, task_id: str) -> None:
    assert bunch.find_node(node_path).task_id == task_id


def _assert_aborted_reason(bunch: Bunch, node_path: str, reason: str) -> None:
    assert bunch.find_node(node_path).aborted_reason == reason


def _assert_event(bunch: Bunch, node_path: str, name: str, value: bool) -> None:
    assert bunch.find_node(node_path).find_event(name).value is value


def _assert_meter(bunch: Bunch, node_path: str, name: str, value: int) -> None:
    assert bunch.find_node(node_path).find_meter(name).value == value


def _assert_suspended(bunch: Bunch, flow_name: str, value: bool) -> None:
    assert bunch.find_flow(flow_name).state.suspended is value


def _assert_flow_loaded(bunch: Bunch, flow_name: str) -> None:
    flow = bunch.find_flow(flow_name)
    assert flow is not None
    assert flow.begun is False


def _assert_begun(bunch: Bunch, flow_name: str, value: bool) -> None:
    assert bunch.find_flow(flow_name).begun is value


# drivers -----------------------------------------------------------------


def run_via_handlers(case: HandlerCase) -> Tuple[Any, Bunch]:
    """Run one case against ``CommandHandlers`` directly (DTO in, DTO out).

    The DTO is built inside ``dispatch``'s parse callable, so a validation
    failure is answered by the boundary like a wire-level parse failure is.
    """
    scheduler = build_scheduler()
    handlers = CommandHandlers(scheduler)
    request_type = REQUEST_TYPE_BY_COMMAND[case.command]
    response = asyncio.run(
        handlers.dispatch(case.command, lambda: request_type(**case.kwargs))
    )
    return response, scheduler.bunch


def run_via_grpc(case: HandlerCase) -> Tuple[Any, Bunch]:
    """Run one case through the gRPC boundary: pb2 in, pb2 out."""
    scheduler = build_scheduler()
    service = GrpcTransport(scheduler=scheduler)
    method = getattr(service, METHOD_NAME_BY_COMMAND[case.command])
    request = to_pb2(case.command, case.kwargs)
    response = asyncio.run(method(request, mock.MagicMock()))
    return response, scheduler.bunch


def run_via_http(case: HandlerCase) -> Tuple[Any, Bunch]:
    """Run one case through the HTTP boundary: envelope JSON in, envelope JSON out.

    The app is driven through httpx's ASGI transport, so the whole HTTP stack
    -- routing, envelope validation, the authentication dependency, dispatch
    -- runs without a uvicorn server (uvicorn's own lifecycle is covered in
    ``test_http_transport_unit.py``). Both imports are lazy: fastapi and
    httpx live behind the ``http`` extra, and a gRPC-only checkout must still
    collect this conftest.
    """
    import httpx

    from takler.server.http_transport import API_PREFIX, create_app

    scheduler = build_scheduler()
    app = create_app(CommandHandlers(scheduler))

    async def post():
        payload = dict(case.kwargs)
        if "flow_bytes" in payload:
            # The JSON envelope carries raw bytes base64 encoded, mirroring
            # what ``LoadCommand``'s model config does on the client side.
            payload["flow_bytes"] = base64.b64encode(payload["flow_bytes"]).decode(
                "ascii"
            )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            return await client.post(
                f"{API_PREFIX}/commands/{case.command.value}",
                json={"command": case.command.value, "payload": payload},
            )

    http_response = asyncio.run(post())
    assert http_response.status_code == 200, http_response.text
    envelope = Envelope.model_validate(http_response.json())
    return envelope.parse_response(), scheduler.bunch


def assert_case(case: HandlerCase, response, bunch: Bunch) -> None:
    """Assert a case's expectations against either response encoding.

    Works on the response DTO and on the pb2 message alike: both carry the
    attributes read here (``flag``/``message``, ``output``, ``coroutines``).
    """
    if case.command is Command.SHOW:
        assert case.output_part in response.output
    elif case.command is Command.PING:
        pass  # arriving at all is the answer
    elif case.command is Command.COROUTINE:
        # The dispatch itself is a task of the running loop.
        assert len(response.coroutines) >= 1
    else:
        assert response.flag == case.expected_flag
        if case.message_part is not None:
            assert case.message_part in response.message
    if case.check is not None:
        case.check(bunch)


# pb2 request builders ----------------------------------------------------
#
# The test-owned inverse of ``server/protocol/adapter.py``: cases are written
# as DTO kwargs, and the gRPC driver needs the wire form. Kept deliberately
# independent of the adapter so a wrong conversion cannot cancel out between
# the two directions.


def to_pb2(command: Command, kwargs: Dict[str, Any]):
    """Build the pb2 request message of ``command`` from case kwargs."""
    if command is Command.INIT:
        return takler_pb2.InitCommand(
            child_options=takler_pb2.ChildCommandOptions(node_path=kwargs["node_path"]),
            task_id=kwargs["task_id"],
        )
    if command is Command.COMPLETE:
        return takler_pb2.CompleteCommand(
            child_options=takler_pb2.ChildCommandOptions(node_path=kwargs["node_path"])
        )
    if command is Command.ABORT:
        return takler_pb2.AbortCommand(
            child_options=takler_pb2.ChildCommandOptions(node_path=kwargs["node_path"]),
            reason=kwargs.get("reason", ""),
        )
    if command is Command.EVENT:
        return takler_pb2.EventCommand(
            child_options=takler_pb2.ChildCommandOptions(node_path=kwargs["node_path"]),
            event_name=kwargs["event_name"],
        )
    if command is Command.METER:
        return takler_pb2.MeterCommand(
            child_options=takler_pb2.ChildCommandOptions(node_path=kwargs["node_path"]),
            meter_name=kwargs["meter_name"],
            meter_value=str(kwargs["meter_value"]),
        )
    if command is Command.REQUEUE:
        return takler_pb2.RequeueCommand(node_path=kwargs["node_paths"])
    if command is Command.SUSPEND:
        return takler_pb2.SuspendCommand(node_path=kwargs["node_paths"])
    if command is Command.RESUME:
        return takler_pb2.ResumeCommand(node_path=kwargs["node_paths"])
    if command is Command.RUN:
        return takler_pb2.RunCommand(
            node_path=kwargs["node_paths"], force=kwargs.get("force", False)
        )
    if command is Command.FORCE:
        return takler_pb2.ForceCommand(
            path=kwargs["paths"],
            state=takler_pb2.ForceCommand.ForceState.Value(kwargs["state"]),
            recursive=kwargs["recursive"],
        )
    if command is Command.FREE_DEP:
        return takler_pb2.FreeDepCommand(
            path=kwargs["paths"],
            dep_type=takler_pb2.FreeDepCommand.DepType.Value(kwargs["dep_type"]),
        )
    if command is Command.LOAD:
        return takler_pb2.LoadCommand(
            flow_type=kwargs.get("flow_type", "json"), flow=kwargs["flow_bytes"]
        )
    if command is Command.BEGIN:
        return takler_pb2.BeginCommand(
            flow_name=kwargs.get("flow_name", ""), force=kwargs.get("force", False)
        )
    if command is Command.SHOW:
        return takler_pb2.ShowRequest(
            show_trigger=kwargs["show_trigger"],
            show_parameter=kwargs["show_parameter"],
            show_limit=kwargs["show_limit"],
            show_event=kwargs["show_event"],
            show_meter=kwargs["show_meter"],
        )
    if command is Command.PING:
        return takler_pb2.PingRequest()
    if command is Command.COROUTINE:
        return takler_pb2.CoroutineRequest()
    raise AssertionError(f"no pb2 builder for {command}")


# fixtures ---------------------------------------------------------------


@pytest.fixture(params=HANDLER_CASES, ids=lambda case: case.id)
def handler_case(request) -> HandlerCase:
    """One shared command scenario."""
    return request.param


@pytest.fixture
def run_via_handlers_fixture():
    """The handler-level driver: DTO in, DTO out."""
    return run_via_handlers


@pytest.fixture
def run_via_grpc_fixture():
    """The gRPC-boundary driver: pb2 in, pb2 out."""
    return run_via_grpc


@pytest.fixture
def run_via_http_fixture():
    """The HTTP-boundary driver: envelope JSON in, envelope JSON out."""
    return run_via_http


@pytest.fixture
def assert_handler_case():
    """The shared case assertion."""
    return assert_case


@pytest.fixture
def build_scenario_scheduler():
    """A fresh scenario-scheduler factory."""
    return build_scheduler


@pytest.fixture
def handler_cases() -> List[HandlerCase]:
    """The whole shared case list (for coverage assertions)."""
    return HANDLER_CASES
