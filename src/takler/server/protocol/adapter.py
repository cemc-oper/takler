"""pb2 <-> DTO conversion: the gRPC encoding of the command model.

This module is the gRPC codec of the command model, shared by both peers:

* the server direction -- :func:`request_from_pb2` parses a generated
  ``takler_pb2`` request message into the command's DTO
  (:mod:`takler.protocol.commands`) and :func:`response_to_pb2` encodes the
  response DTO back into the pb2 message the RPC must return, reducing
  ``GrpcTransport`` (the servicer half) to one line per RPC method;
* the client direction -- :func:`request_to_pb2` encodes a command's request
  payload (a plain mapping keyed by the DTO's field names) into its pb2
  request message and :func:`response_from_pb2` parses the pb2 response into
  the response DTO, so the client's ``GrpcTransport`` is the only place there
  that touches generated code.

The two request directions deliberately differ in validation. The server
direction validates: ``request_from_pb2`` builds the DTO, so a non-numeric
``meter_value`` surfaces as a ``pydantic.ValidationError``. The client
direction does not: ``request_to_pb2`` copies the payload verbatim, because
validating locally would diverge the two clients -- a job script's
``meter abc`` must reach the server and be answered with the same
``internal_error`` flag whichever client carried it.

Conversions worth knowing about:

* The proto's ``ChildCommandOptions`` wrapper is flattened: its ``node_path``
  becomes the DTO's own field, and is rebuilt on the way out.
* ``MeterCommand.meter_value`` travels as a string on the wire; handing it to
  the DTO unchanged lets pydantic's ``int`` coercion reproduce the ``int()``
  call the scheduler used to do.
* ``ForceCommand.state`` and ``FreeDepCommand.dep_type`` travel as enum
  numbers; they are translated to and from their proto enum *names* here so
  the DTO layer deals in names (its ``ForceState`` / ``DepType`` enums),
  never in the wire's numbering. ``request_to_pb2`` resolves a name with
  ``Value()``, raising ``ValueError`` for an unknown one -- exactly what the
  client did when it built the message itself.
* Server inbound, every DTO field is set explicitly from the message. proto3
  fills unset scalar fields with zero values, so relying on the DTO defaults
  would turn a client's omitted flag into the CLI default rather than the
  zero value the client actually sent.

Nothing here touches the scheduler or the handlers; this module is the one
place allowed to know both ``takler_pb2`` and
:mod:`takler.protocol.commands`.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Mapping

from takler.protocol import commands
from takler.protocol.commands import Command, ProtocolModel
from takler.server.protocol import takler_pb2

__all__ = [
    "GRPC_METHOD_BY_COMMAND",
    "request_from_pb2",
    "request_to_pb2",
    "response_from_pb2",
    "response_to_pb2",
]


#: Command -> the gRPC method that carries it, that is the stub attribute a
#: client calls and the servicer method a server implements.
#:
#: This is wire knowledge, so the codec owns this copy;
#: ``takler.server.handlers.METHOD_NAME_BY_COMMAND`` holds the same names for
#: the transport-neutral layer (logging, auditing), which cannot import this
#: module. The two tables and the ``takler.proto`` service descriptor are
#: pinned equal by the protocol contract tests.
GRPC_METHOD_BY_COMMAND: Dict[Command, str] = {
    Command.INIT: "RunCommandInit",
    Command.COMPLETE: "RunCommandComplete",
    Command.ABORT: "RunCommandAbort",
    Command.EVENT: "RunCommandEvent",
    Command.METER: "RunCommandMeter",
    Command.REQUEUE: "RunCommandRequeue",
    Command.SUSPEND: "RunCommandSuspend",
    Command.RESUME: "RunCommandResume",
    Command.RUN: "RunCommandRun",
    Command.FORCE: "RunCommandForce",
    Command.FREE_DEP: "RunCommandFreeDep",
    Command.LOAD: "RunCommandLoad",
    Command.BEGIN: "RunCommandBegin",
    Command.SHOW: "RunRequestShow",
    Command.PING: "RunRequestPing",
    Command.COROUTINE: "QueryCoroutine",
}


def _init(message) -> commands.InitCommand:
    return commands.InitCommand(
        node_path=message.child_options.node_path,
        task_id=message.task_id,
    )


def _complete(message) -> commands.CompleteCommand:
    return commands.CompleteCommand(node_path=message.child_options.node_path)


def _abort(message) -> commands.AbortCommand:
    return commands.AbortCommand(
        node_path=message.child_options.node_path,
        reason=message.reason,
    )


def _event(message) -> commands.EventCommand:
    return commands.EventCommand(
        node_path=message.child_options.node_path,
        event_name=message.event_name,
    )


def _meter(message) -> commands.MeterCommand:
    return commands.MeterCommand(
        node_path=message.child_options.node_path,
        meter_name=message.meter_name,
        meter_value=message.meter_value,
    )


def _requeue(message) -> commands.RequeueCommand:
    return commands.RequeueCommand(node_paths=list(message.node_path))


def _suspend(message) -> commands.SuspendCommand:
    return commands.SuspendCommand(node_paths=list(message.node_path))


def _resume(message) -> commands.ResumeCommand:
    return commands.ResumeCommand(node_paths=list(message.node_path))


def _run(message) -> commands.RunCommand:
    return commands.RunCommand(
        node_paths=list(message.node_path),
        force=message.force,
    )


def _force(message) -> commands.ForceCommand:
    return commands.ForceCommand(
        paths=list(message.path),
        state=takler_pb2.ForceCommand.ForceState.Name(message.state),
        recursive=message.recursive,
    )


def _free_dep(message) -> commands.FreeDepCommand:
    return commands.FreeDepCommand(
        paths=list(message.path),
        dep_type=takler_pb2.FreeDepCommand.DepType.Name(message.dep_type),
    )


def _load(message) -> commands.LoadCommand:
    return commands.LoadCommand(
        flow_type=message.flow_type,
        flow_bytes=message.flow,
    )


def _begin(message) -> commands.BeginCommand:
    return commands.BeginCommand(
        flow_name=message.flow_name,
        force=message.force,
    )


def _show(message) -> commands.ShowRequest:
    return commands.ShowRequest(
        show_trigger=message.show_trigger,
        show_parameter=message.show_parameter,
        show_limit=message.show_limit,
        show_event=message.show_event,
        show_meter=message.show_meter,
    )


def _ping(message) -> commands.PingRequest:
    return commands.PingRequest()


def _coroutine(message) -> commands.CoroutineRequest:
    return commands.CoroutineRequest()


#: Command -> converter from its pb2 request message to its request DTO.
_REQUEST_FROM_PB2: Dict[Command, Callable[[object], ProtocolModel]] = {
    Command.INIT: _init,
    Command.COMPLETE: _complete,
    Command.ABORT: _abort,
    Command.EVENT: _event,
    Command.METER: _meter,
    Command.REQUEUE: _requeue,
    Command.SUSPEND: _suspend,
    Command.RESUME: _resume,
    Command.RUN: _run,
    Command.FORCE: _force,
    Command.FREE_DEP: _free_dep,
    Command.LOAD: _load,
    Command.BEGIN: _begin,
    Command.SHOW: _show,
    Command.PING: _ping,
    Command.COROUTINE: _coroutine,
}


def request_from_pb2(command: Command, message) -> ProtocolModel:
    """Convert the pb2 request ``message`` of ``command`` into its DTO.

    Raises:
        pydantic.ValidationError: If the message's fields fail the DTO's
            validation -- a non-numeric ``meter_value`` is the one the wire
            makes possible. Callers run this inside the command boundary, so
            the error is classified and answered like any command failure.
        KeyError: If ``command`` has no converter (unreachable while the table
            covers the sixteen commands; the module's tests pin that).
    """
    return _REQUEST_FROM_PB2[command](message)


def response_to_pb2(response: ProtocolModel):
    """Convert a response DTO into the pb2 message of its RPC."""
    if isinstance(response, commands.ServiceResponse):
        return takler_pb2.ServiceResponse(flag=response.flag, message=response.message)
    if isinstance(response, commands.ShowResponse):
        return takler_pb2.ShowResponse(output=response.output)
    if isinstance(response, commands.PingResponse):
        return takler_pb2.PingResponse()
    if isinstance(response, commands.CoroutineResponse):
        return takler_pb2.CoroutineResponse(
            coroutines=[
                takler_pb2.Coroutine(name=c.name, description=c.description)
                for c in response.coroutines
            ]
        )
    raise TypeError(f"no pb2 encoding for response type {type(response).__name__}")


# ---------------------------------------------------------------------------
# Client direction: payload -> pb2 request, pb2 response -> DTO
# ---------------------------------------------------------------------------
#
# The payload is a plain mapping keyed by the DTO's field names, holding the
# values exactly as the client means to send them -- ``meter_value`` as the
# string the job script passed, ``state`` / ``dep_type`` as the enum *name* the
# operator typed. Nothing is validated here on purpose (see the module
# docstring): a malformed value crosses the wire and the server classifies it,
# so both clients answer it the same way.


def _child_options(payload: Mapping[str, Any]):
    """Rebuild the proto's ``ChildCommandOptions`` wrapper from the payload."""
    return takler_pb2.ChildCommandOptions(node_path=payload["node_path"])


def _init_to_pb2(payload):
    return takler_pb2.InitCommand(
        child_options=_child_options(payload),
        task_id=payload["task_id"],
    )


def _complete_to_pb2(payload):
    return takler_pb2.CompleteCommand(child_options=_child_options(payload))


def _abort_to_pb2(payload):
    return takler_pb2.AbortCommand(
        child_options=_child_options(payload),
        reason=payload["reason"],
    )


def _event_to_pb2(payload):
    return takler_pb2.EventCommand(
        child_options=_child_options(payload),
        event_name=payload["event_name"],
    )


def _meter_to_pb2(payload):
    return takler_pb2.MeterCommand(
        child_options=_child_options(payload),
        meter_name=payload["meter_name"],
        meter_value=payload["meter_value"],
    )


def _requeue_to_pb2(payload):
    return takler_pb2.RequeueCommand(node_path=list(payload["node_paths"]))


def _suspend_to_pb2(payload):
    return takler_pb2.SuspendCommand(node_path=list(payload["node_paths"]))


def _resume_to_pb2(payload):
    return takler_pb2.ResumeCommand(node_path=list(payload["node_paths"]))


def _run_to_pb2(payload):
    return takler_pb2.RunCommand(
        node_path=list(payload["node_paths"]),
        force=payload["force"],
    )


def _force_to_pb2(payload):
    return takler_pb2.ForceCommand(
        state=takler_pb2.ForceCommand.ForceState.Value(payload["state"]),
        recursive=payload["recursive"],
        path=list(payload["paths"]),
    )


def _free_dep_to_pb2(payload):
    return takler_pb2.FreeDepCommand(
        dep_type=takler_pb2.FreeDepCommand.DepType.Value(payload["dep_type"]),
        path=list(payload["paths"]),
    )


def _load_to_pb2(payload):
    return takler_pb2.LoadCommand(
        flow_type=payload["flow_type"],
        flow=payload["flow_bytes"],
    )


def _begin_to_pb2(payload):
    return takler_pb2.BeginCommand(
        flow_name=payload["flow_name"],
        force=payload["force"],
    )


def _show_to_pb2(payload):
    return takler_pb2.ShowRequest(
        show_trigger=payload["show_trigger"],
        show_parameter=payload["show_parameter"],
        show_limit=payload["show_limit"],
        show_event=payload["show_event"],
        show_meter=payload["show_meter"],
    )


def _ping_to_pb2(payload):
    return takler_pb2.PingRequest()


def _coroutine_to_pb2(payload):
    return takler_pb2.CoroutineRequest()


#: Command -> converter from its request payload to its pb2 request message.
_REQUEST_TO_PB2: Dict[Command, Callable[[Mapping[str, Any]], Any]] = {
    Command.INIT: _init_to_pb2,
    Command.COMPLETE: _complete_to_pb2,
    Command.ABORT: _abort_to_pb2,
    Command.EVENT: _event_to_pb2,
    Command.METER: _meter_to_pb2,
    Command.REQUEUE: _requeue_to_pb2,
    Command.SUSPEND: _suspend_to_pb2,
    Command.RESUME: _resume_to_pb2,
    Command.RUN: _run_to_pb2,
    Command.FORCE: _force_to_pb2,
    Command.FREE_DEP: _free_dep_to_pb2,
    Command.LOAD: _load_to_pb2,
    Command.BEGIN: _begin_to_pb2,
    Command.SHOW: _show_to_pb2,
    Command.PING: _ping_to_pb2,
    Command.COROUTINE: _coroutine_to_pb2,
}


def request_to_pb2(command: Command, payload: Mapping[str, Any]):
    """Encode the request ``payload`` of ``command`` into its pb2 message.

    The payload is keyed by the request DTO's field names and is copied
    verbatim -- no validation runs on this side of the wire (see the module
    docstring). An unknown ``state`` / ``dep_type`` name raises the
    ``ValueError`` of the proto enum's ``Value()`` lookup, as it did when the
    client built the message itself.

    Raises:
        KeyError: If ``command`` has no converter, or the payload lacks a
            field the command needs (both are caller bugs, not wire data).
    """
    return _REQUEST_TO_PB2[command](payload)


def response_from_pb2(command: Command, message) -> ProtocolModel:
    """Convert the pb2 response ``message`` of ``command`` into its DTO.

    The command, not the message's class, selects the conversion: the response
    type of a command is fixed by the protocol, and reading the fields
    structurally keeps the conversion usable against any object with the
    right attributes (a test double, say).

    Raises:
        KeyError: If ``command`` is not one of the sixteen.
    """
    response_type = commands.RESPONSE_TYPE_BY_COMMAND[command]
    if response_type is commands.ServiceResponse:
        return commands.ServiceResponse(flag=message.flag, message=message.message)
    if response_type is commands.ShowResponse:
        return commands.ShowResponse(output=message.output)
    if response_type is commands.PingResponse:
        return commands.PingResponse()
    return commands.CoroutineResponse(
        coroutines=[
            commands.Coroutine(name=c.name, description=c.description)
            for c in message.coroutines
        ]
    )
