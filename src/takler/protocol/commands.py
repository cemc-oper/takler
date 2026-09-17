"""Transport-neutral model of the sixteen takler commands.

Every command takler speaks -- the five Child_Commands, the eight
Control_Commands and the three Query_Commands -- has its request and response
defined here as pydantic models. The ``Scheduler`` and the command handlers
depend on these DTOs and only on these; a transport adapter converts between
a DTO and its wire encoding at the boundary (the gRPC adapter lives in
``takler.server.protocol``, the HTTP one comes with ``takler[http]``).

Two encodings of this model exist, and this module belongs to neither:

* gRPC: the ``takler.proto`` messages, generated into ``takler_pb2``. The DTO
  classes deliberately carry the same names as their proto messages -- the
  mapping is one to one, so ``takler.protocol.InitCommand`` is the model and
  ``takler_pb2.InitCommand`` is one encoding of it. The proto's
  ``ChildCommandOptions`` wrapper is a gRPC encoding detail and is flattened
  into each child command's ``node_path`` field here.
* HTTP: the JSON envelope of :class:`takler.protocol.Envelope`, whose
  ``payload`` is a request or response DTO serialized with
  ``model_dump(mode="json")``.

Layering rule: nothing in ``takler.protocol`` imports ``grpc``,
``takler_pb2`` or anything under ``takler.server`` / ``takler.client`` --
that is what makes the model usable from both the server and either client
without dragging a transport along.

Validation the handlers used to do by hand lives on the DTOs now:

* ``MeterCommand.meter_value`` travels as a string on the wire (both clients
  send it verbatim) and leaves the DTO as an ``int`` -- the ``int()`` call
  that used to sit in ``Scheduler.run_command_meter`` moved here.
* ``ForceCommand.state`` and ``FreeDepCommand.dep_type`` travel as names and
  leave the DTO as the :class:`ForceState` / :class:`DepType` enums -- the
  name translation both CLIs do on their own side is reproduced here, so a
  transport adapter never deals in raw enum numbers.

A validation failure raises ``pydantic.ValidationError``; classifying it as
an Error_Code is the transport adapter's job, not the DTO's.

Field defaults mirror the command-line surface rather than the proto3 zero
values -- ``ForceCommand.recursive`` defaults to ``True`` because both CLIs
default ``--recursive`` to true, and the three ``show`` flags the Python CLI
turns on by default default to ``True`` here. A request either carries every
field explicitly, which is what the gRPC encoding does, or it omits one and
gets the value an operator typing the CLI without that flag would get.
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, List, Type

from pydantic import BaseModel, ConfigDict

__all__ = [
    "Command",
    "ForceState",
    "DepType",
    "ProtocolModel",
    "ChildCommand",
    "InitCommand",
    "CompleteCommand",
    "AbortCommand",
    "EventCommand",
    "MeterCommand",
    "NodePathsCommand",
    "RequeueCommand",
    "SuspendCommand",
    "ResumeCommand",
    "RunCommand",
    "ForceCommand",
    "FreeDepCommand",
    "LoadCommand",
    "BeginCommand",
    "ShowRequest",
    "PingRequest",
    "CoroutineRequest",
    "ServiceResponse",
    "ShowResponse",
    "PingResponse",
    "Coroutine",
    "CoroutineResponse",
    "REQUEST_TYPE_BY_COMMAND",
    "RESPONSE_TYPE_BY_COMMAND",
]


class ProtocolModel(BaseModel):
    """Base of every DTO in :mod:`takler.protocol`.

    ``extra="forbid"`` makes a misspelled or unknown field a validation error
    instead of a silently dropped one. That is the right default while the
    two ends of every transport ship in lockstep (the M3 protocol is free to
    evolve, but both ends always move together); it would have to be
    revisited the day mixed-version deployments become a supported shape.
    """

    model_config = ConfigDict(extra="forbid")


class Command(str, Enum):
    """The name of one of the sixteen commands, as the envelope carries it.

    The values are the CLI words -- ``init`` .. ``coroutine`` -- because they
    are the one command surface already shared by both clients, and because
    the HTTP transport addresses a command as ``POST /v1/commands/{command}``.
    The gRPC encoding never transmits them: there the method name of the RPC
    plays this role, and the gRPC adapter maps between the two.
    """

    INIT = "init"
    COMPLETE = "complete"
    ABORT = "abort"
    EVENT = "event"
    METER = "meter"
    REQUEUE = "requeue"
    SUSPEND = "suspend"
    RESUME = "resume"
    RUN = "run"
    FORCE = "force"
    FREE_DEP = "free-dep"
    LOAD = "load"
    BEGIN = "begin"
    SHOW = "show"
    PING = "ping"
    COROUTINE = "coroutine"


class ForceState(str, Enum):
    """The states the ``force`` command can impose, by name.

    The names are exactly the ``ForceCommand.ForceState`` enum value names of
    ``takler.proto`` (which are also the words both CLIs accept); the numbers
    of that enum are a gRPC encoding detail this layer never sees. ``CLEAR``
    and ``SET`` apply to an Event target, the rest to a Node target --
    deciding which is the scheduler's business, not the DTO's.
    """

    UNKNOWN = "unknown"
    COMPLETE = "complete"
    QUEUED = "queued"
    SUBMITTED = "submitted"
    ACTIVE = "active"
    ABORTED = "aborted"
    CLEAR = "clear"
    SET = "set"


class DepType(str, Enum):
    """The dependency classes the ``free-dep`` command can clear, by name.

    Same naming rule as :class:`ForceState`: the names are the
    ``FreeDepCommand.DepType`` enum value names of ``takler.proto``.
    """

    ALL = "all"
    TRIGGER = "trigger"
    TIME = "time"


# ---------------------------------------------------------------------------
# Child commands
# ---------------------------------------------------------------------------


class ChildCommand(ProtocolModel):
    """Base of the five Child_Commands: the shared ``node_path`` field.

    This is the flattened form of the proto's ``ChildCommandOptions``
    wrapper; the wrapper exists in the proto for historical reasons and is
    rebuilt by the gRPC adapter.
    """

    node_path: str


class InitCommand(ChildCommand):
    """``init``: a job reports itself started, carrying its Job_Id."""

    task_id: str


class CompleteCommand(ChildCommand):
    """``complete``: a job reports itself finished."""


class AbortCommand(ChildCommand):
    """``abort``: a job reports itself failed, optionally with a reason."""

    reason: str = ""


class EventCommand(ChildCommand):
    """``event``: a job sets one of its node's events."""

    event_name: str


class MeterCommand(ChildCommand):
    """``meter``: a job advances one of its node's meters.

    ``meter_value`` is an ``int`` here even though the wire carries a
    string: pydantic coerces ``"50"`` to ``50`` and rejects ``"abc"`` with a
    ``ValidationError``, which is the ``int()`` conversion that used to live
    in ``Scheduler.run_command_meter`` (a ``ValueError`` there classified as
    ``internal_error``; the classification of a DTO rejection is the
    adapter's decision).
    """

    meter_name: str
    meter_value: int


# ---------------------------------------------------------------------------
# Control commands
# ---------------------------------------------------------------------------


class NodePathsCommand(ProtocolModel):
    """Base of the control commands whose payload is a list of node paths."""

    node_paths: List[str]


class RequeueCommand(NodePathsCommand):
    """``requeue``: reset the nodes so they can run again."""


class SuspendCommand(NodePathsCommand):
    """``suspend``: hold the nodes out of scheduling."""


class ResumeCommand(NodePathsCommand):
    """``resume``: return suspended nodes to scheduling."""


class RunCommand(NodePathsCommand):
    """``run``: force a task into execution, ignoring its dependencies."""

    force: bool = False


class ForceCommand(ProtocolModel):
    """``force``: set a node's status, or set/clear an event.

    The field is named ``paths`` (not ``node_paths``) because a target may
    also be an event path like ``/flow/task:event`` -- the same reason the
    proto field is named ``path``.
    """

    paths: List[str]
    state: ForceState
    # Both CLIs default --recursive to true, so the DTO default follows the
    # command surface rather than the proto3 zero value (see the module
    # docstring).
    recursive: bool = True


class FreeDepCommand(ProtocolModel):
    """``free-dep``: drop a node's dependencies so it can run."""

    paths: List[str]
    dep_type: DepType = DepType.ALL


class LoadCommand(ProtocolModel):
    """``load``: add a flow definition to the bunch.

    ``flow_bytes`` is raw bytes, like the proto field. In the JSON envelope
    it travels base64-encoded (the model config below), which keeps the
    encoding defined for arbitrary bytes instead of only for UTF-8 text.
    ``flow_type`` stays a plain string: the scheduler owns the list of
    supported types and reports an unsupported one as
    ``unsupported_value``, which a DTO-level enum would reclassify.
    """

    model_config = ConfigDict(
        extra="forbid", val_json_bytes="base64", ser_json_bytes="base64"
    )

    flow_type: str = "json"
    flow_bytes: bytes


class BeginCommand(ProtocolModel):
    """``begin``: start a flow's calendar and reset its node tree.

    An empty ``flow_name`` addresses every flow of the bunch -- that is the
    protocol's "all flows" form, shared by both CLIs.
    """

    flow_name: str = ""
    force: bool = False


# ---------------------------------------------------------------------------
# Query commands
# ---------------------------------------------------------------------------


class ShowRequest(ProtocolModel):
    """``show``: serialize the bunch. The flags select the detail sections.

    The defaults are the Python CLI's defaults, per the module docstring's
    rule: an empty ``show`` request asks for what ``takler-client-py show``
    prints without flags.
    """

    show_trigger: bool = False
    show_parameter: bool = False
    show_limit: bool = True
    show_event: bool = True
    show_meter: bool = True


class PingRequest(ProtocolModel):
    """``ping``: check the server is alive. Carries nothing."""


class CoroutineRequest(ProtocolModel):
    """``coroutine``: list the server's asyncio tasks. Carries nothing."""


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------


class ServiceResponse(ProtocolModel):
    """The response of every Child_ and Control_Command.

    ``flag`` is the Error_Code: ``0`` for success, a non-zero classification
    otherwise; ``message`` carries the human-readable detail. The error-code
    table itself moves into this package with task 4.
    """

    flag: int = 0
    message: str = ""


class ShowResponse(ProtocolModel):
    """``show`` response: the serialized bunch in ``output``."""

    output: str = ""


class PingResponse(ProtocolModel):
    """``ping`` response. Carries nothing: arriving at all is the answer."""


class Coroutine(ProtocolModel):
    """One asyncio task of the server, as ``coroutine`` reports it."""

    name: str
    description: str


class CoroutineResponse(ProtocolModel):
    """``coroutine`` response: the server's asyncio tasks."""

    coroutines: List[Coroutine] = []


#: Command -> request DTO type, one entry per command. The envelope's
#: ``parse_request`` looks the payload's type up here, so this map -- not a
#: convention -- is what ties a command name to its model.
REQUEST_TYPE_BY_COMMAND: Dict[Command, Type[ProtocolModel]] = {
    Command.INIT: InitCommand,
    Command.COMPLETE: CompleteCommand,
    Command.ABORT: AbortCommand,
    Command.EVENT: EventCommand,
    Command.METER: MeterCommand,
    Command.REQUEUE: RequeueCommand,
    Command.SUSPEND: SuspendCommand,
    Command.RESUME: ResumeCommand,
    Command.RUN: RunCommand,
    Command.FORCE: ForceCommand,
    Command.FREE_DEP: FreeDepCommand,
    Command.LOAD: LoadCommand,
    Command.BEGIN: BeginCommand,
    Command.SHOW: ShowRequest,
    Command.PING: PingRequest,
    Command.COROUTINE: CoroutineRequest,
}

#: Command -> response DTO type. The thirteen child and control commands all
#: answer :class:`ServiceResponse`; only the three queries have a response
#: type of their own.
RESPONSE_TYPE_BY_COMMAND: Dict[Command, Type[ProtocolModel]] = {
    Command.INIT: ServiceResponse,
    Command.COMPLETE: ServiceResponse,
    Command.ABORT: ServiceResponse,
    Command.EVENT: ServiceResponse,
    Command.METER: ServiceResponse,
    Command.REQUEUE: ServiceResponse,
    Command.SUSPEND: ServiceResponse,
    Command.RESUME: ServiceResponse,
    Command.RUN: ServiceResponse,
    Command.FORCE: ServiceResponse,
    Command.FREE_DEP: ServiceResponse,
    Command.LOAD: ServiceResponse,
    Command.BEGIN: ServiceResponse,
    Command.SHOW: ShowResponse,
    Command.PING: PingResponse,
    Command.COROUTINE: CoroutineResponse,
}
