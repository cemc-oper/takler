"""pb2 <-> DTO conversion: the gRPC encoding of the command model.

This module is the gRPC transport's half of the adapter pattern: it converts
a generated ``takler_pb2`` request message into the command's DTO
(:mod:`takler.protocol.commands`) and the response DTO back into the pb2
message the RPC must return. ``TaklerService`` (the other half) is reduced by
it to one line per RPC method.

Conversions worth knowing about:

* The proto's ``ChildCommandOptions`` wrapper is flattened: its ``node_path``
  becomes the DTO's own field.
* ``MeterCommand.meter_value`` travels as a string on the wire; handing it to
  the DTO unchanged lets pydantic's ``int`` coercion reproduce the ``int()``
  call the scheduler used to do, and a non-numeric string surfaces as a
  ``pydantic.ValidationError`` raised from :func:`request_from_pb2`.
* ``ForceCommand.state`` and ``FreeDepCommand.dep_type`` travel as enum
  numbers; they are translated to their proto enum *names* here so the DTO
  layer deals in names (its ``ForceState`` / ``DepType`` enums), never in the
  wire's numbering.
* Every DTO field is set explicitly from the message. proto3 fills unset
  scalar fields with zero values, so relying on the DTO defaults would turn a
  client's omitted flag into the CLI default rather than the zero value the
  client actually sent.

Nothing here touches the scheduler or the handlers; this module is the one
place in the server allowed to know both ``takler_pb2`` and
:mod:`takler.protocol.commands`.
"""

from __future__ import annotations

from typing import Callable, Dict

from takler.protocol import commands
from takler.protocol.commands import Command, ProtocolModel
from takler.server.protocol import takler_pb2

__all__ = ["request_from_pb2", "response_to_pb2"]


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
