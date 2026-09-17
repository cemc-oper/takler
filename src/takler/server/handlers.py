"""Transport-neutral command handlers: DTO in, DTO out.

``CommandHandlers`` sits between a transport adapter and the ``Scheduler``.
Its callers hand it a command name and a *parse* callable that produces the
request DTO -- the gRPC adapter parses a pb2 message, the HTTP adapter will
parse an envelope payload -- and it answers with the response DTO. Neither
side of that conversation mentions a wire encoding, which is what lets the
same handlers serve both transports.

Three cross-cutting concerns live here, once, instead of once per transport:

* the exception boundary (:meth:`CommandHandlers._handle_command`): an
  exception escaping a command -- including the ``pydantic.ValidationError``
  of a rejected parse, which happens *inside* the boundary by construction --
  is logged with its context and converted into the error representation of
  the command's response type, honouring the server's Exception_Policy;
* the Error_Code mapping: a command failure carries
  ``error_code_for_exception(exc)`` in ``ServiceResponse.flag``;
* the control-command Audit_Record (Requirement 11.2): exactly one record per
  handled Control_Command, built from the response that is about to be
  returned.

The operation names used for logging and auditing are the gRPC method names
(``RunCommandRequeue`` and friends) even though no gRPC code runs here: they
are the canonical names of the commands, the privilege table
(:data:`~takler.server.auth.PRIVILEGE_BY_METHOD`) is keyed by them, and the
audit trail's ``command`` field is derived from them. Keeping them stable
keeps the audit format identical across transports.

The credentials an audit record names come from the Auth_Interceptor's
call-local publication (:func:`~takler.server.auth.get_call_credentials`);
the peer address, which is transport metadata rather than a credential, is
handed in by the adapter as the ``peer`` argument of
:meth:`CommandHandlers.dispatch`.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Callable, Dict, List, Optional, Sequence, Union

from takler.logging import get_logger
from takler.protocol.commands import (
    Command,
    Coroutine,
    CoroutineResponse,
    PingResponse,
    ProtocolModel,
    ServiceResponse,
    ShowResponse,
)
from takler.protocol.error_code import SUCCESS, error_code_for_exception
from takler.server.audit import (
    EVENT_CONTROL,
    OUTCOME_ERROR,
    OUTCOME_SUCCESS,
    AuditLogger,
    AuditRecord,
    audit_command_name,
    audit_peer,
    audit_timestamp,
)
from takler.server.auth import (
    PRIVILEGE_BY_METHOD,
    SERVICE_METHOD_PREFIX,
    PrivilegeLevel,
    get_call_credentials,
)
from takler.server.connect_config import DEFAULT_EXCEPTION_POLICY, ExceptionPolicy
from takler.server.scheduler import Scheduler

logger = get_logger("server.handlers")


#: Methods that need operator credentials but change nothing, and are therefore
#: not Control_Commands.
#:
#: Requirement 11.2 audits Control_Commands, i.e. the commands that write to the
#: Bunch. ``show`` and ``coroutine`` are Operator level because they expose the
#: whole flow definition, not because they modify anything, and ``ping`` is
#: public. Auditing them would bury the eight records that matter under one
#: record per TUI refresh -- the TUI polls ``show`` continuously.
_READ_ONLY_OPERATOR_METHODS: "frozenset[str]" = frozenset(
    {
        "RunRequestShow",
        "QueryCoroutine",
    }
)


def _control_method_names() -> "frozenset[str]":
    """Return the bare names of the Control_Command RPC methods.

    Derived from :data:`~takler.server.auth.PRIVILEGE_BY_METHOD` rather than
    listed again here: the privilege table is the one place a method's
    classification is decided, and a second list would be a second place for a
    new command to be forgotten. A new Operator level rpc is therefore audited
    from the moment it is classified, and a read-only one has to be named in
    :data:`_READ_ONLY_OPERATOR_METHODS` to opt out.

    Returns:
        The bare method names, for example ``"RunCommandRequeue"``, as they
        appear in :data:`METHOD_NAME_BY_COMMAND`.
    """
    names = set()
    for full_name, level in PRIVILEGE_BY_METHOD.items():
        if level is not PrivilegeLevel.OPERATOR:
            continue
        bare_name = full_name[len(SERVICE_METHOD_PREFIX) :]
        if bare_name in _READ_ONLY_OPERATOR_METHODS:
            continue
        names.add(bare_name)
    return frozenset(names)


#: The RPC methods whose handling produces one Audit_Record (Requirement 11.2).
CONTROL_METHOD_NAMES: "frozenset[str]" = _control_method_names()


#: Command -> canonical operation name. The values are the gRPC method names
#: of ``takler.proto``; see the module docstring for why a transport-neutral
#: layer names its operations after them.
METHOD_NAME_BY_COMMAND: Dict[Command, str] = {
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


def command_error_response(exc: Exception) -> ServiceResponse:
    """Build the error representation of a command handler's response.

    RESILIENT command handlers convert a caught scheduler-operation exception
    into a ``ServiceResponse`` with a non-zero ``flag`` and a descriptive
    ``message`` (Requirement 2.3).

    ``flag`` carries the Error_Code that classifies ``exc``: an
    Exception_Hierarchy type maps to its dedicated non-zero code, a
    ``TaklerError`` without a dedicated code maps to the generic code 1, and
    anything else -- a ``pydantic.ValidationError`` of a rejected request
    among them -- maps to the internal-server-error code (Requirements 3.4,
    3.5). Success responses keep ``flag=0`` (Requirement 3.3), and failures stay
    non-zero for clients that only test ``flag != 0``.
    """
    return ServiceResponse(
        flag=error_code_for_exception(exc),
        message=f"{type(exc).__name__}: {exc}",
    )


#: Command -> the error representation of its response type. The query
#: commands reuse their own response types for errors (``show`` writes the
#: error into ``output``; ``ping`` and ``coroutine`` answer empty); every
#: child and control command defaults to :func:`command_error_response`.
_ERROR_RESPONSE_BY_COMMAND: Dict[Command, Callable[[Exception], ProtocolModel]] = {
    Command.SHOW: lambda exc: ShowResponse(
        output=f"error: {type(exc).__name__}: {exc}"
    ),
    Command.PING: lambda exc: PingResponse(),
    Command.COROUTINE: lambda exc: CoroutineResponse(),
}


#: Command -> the diagnostic summary of its request, used by the boundary's
#: error log. Only key fields are named; a failed parse is reported by
#: :meth:`CommandHandlers.dispatch` before these run.
_REQUEST_INFO_BY_COMMAND: Dict[Command, Callable[[ProtocolModel], str]] = {
    Command.INIT: lambda r: f"node_path={r.node_path}, task_id={r.task_id}",
    Command.COMPLETE: lambda r: f"node_path={r.node_path}",
    Command.ABORT: lambda r: f"node_path={r.node_path}",
    Command.EVENT: lambda r: f"node_path={r.node_path}, event_name={r.event_name}",
    Command.METER: lambda r: (
        f"node_path={r.node_path}, meter={r.meter_name}, value={r.meter_value}"
    ),
    Command.REQUEUE: lambda r: f"node_path={list(r.node_paths)}",
    Command.SUSPEND: lambda r: f"node_path={list(r.node_paths)}",
    Command.RESUME: lambda r: f"node_path={list(r.node_paths)}",
    Command.RUN: lambda r: f"node_path={list(r.node_paths)}, force={r.force}",
    Command.FORCE: lambda r: (
        f"path={list(r.paths)}, state={r.state.value}, recursive={r.recursive}"
    ),
    Command.FREE_DEP: lambda r: f"path={list(r.paths)}, dep_type={r.dep_type.value}",
    Command.BEGIN: lambda r: f"flow_name={r.flow_name}, force={r.force}",
    Command.LOAD: lambda r: f"flow_type={r.flow_type}",
    Command.SHOW: lambda r: "show",
    Command.PING: lambda r: "ping",
    Command.COROUTINE: lambda r: "coroutine",
}


def _audit_target(command: Command, request: ProtocolModel) -> Optional[List[str]]:
    """The node paths or flow names a command acts on, for the Audit_Record.

    Only the Control_Commands produce records (see :data:`CONTROL_METHOD_NAMES`),
    so only they need a target. A ``begin`` acts on one flow, named rather than
    pathed (Requirement 11.9). A ``load`` carries a serialized flow, not a
    name: the flow it defines is only known once the scheduler has
    deserialized it, so there is no target to name before the command runs and
    none to name at all when it fails.
    """
    if command in (Command.REQUEUE, Command.SUSPEND, Command.RESUME, Command.RUN):
        return list(request.node_paths)
    if command in (Command.FORCE, Command.FREE_DEP):
        return list(request.paths)
    if command is Command.BEGIN:
        return [request.flow_name]
    return None


class CommandHandlers:
    """The sixteen commands behind the exception and audit boundary.

    Attributes
    ----------
    scheduler : Scheduler
        The scheduler every command is executed against.
    exception_policy : ExceptionPolicy
        Whether a handler exception is converted into an error response
        (``RESILIENT``) or also triggers the server's clean shutdown
        (``FAIL_FAST``).
    fatal_shutdown : Optional[Callable[[], None]]
        The shared fatal-shutdown trigger fired in ``FAIL_FAST`` mode.
    audit_logger : Optional[AuditLogger]
        Audit_Logger every Control_Command is recorded to (Requirement 11.2).
        ``None`` disables the record, which is what handlers built directly by
        a test get.
    """

    def __init__(
        self,
        scheduler: Scheduler,
        exception_policy: Optional[ExceptionPolicy] = None,
        fatal_shutdown: Optional[Callable[[], None]] = None,
        audit_logger: Optional[AuditLogger] = None,
    ):
        self.scheduler: Scheduler = scheduler
        self.exception_policy: ExceptionPolicy = (
            exception_policy
            if exception_policy is not None
            else DEFAULT_EXCEPTION_POLICY
        )
        self.fatal_shutdown: Optional[Callable[[], None]] = fatal_shutdown
        self.audit_logger: Optional[AuditLogger] = audit_logger

    # Dispatch -----------------------------------------------------

    async def dispatch(
        self,
        command: Command,
        parse_request: Callable[[], ProtocolModel],
        peer: Optional[str] = None,
    ) -> ProtocolModel:
        """Run one command behind the boundary and answer with its DTO.

        Args:
            command: The command to run.
            parse_request: Zero-argument callable producing the request DTO.
                It runs *inside* the exception boundary, so a transport's
                ``pydantic.ValidationError`` is classified and answered like
                any other command failure instead of escaping the handler.
            peer: The caller's network address as the transport knows it, used
                for the Audit_Record when the published credentials do not
                carry one.

        Returns:
            The response DTO of the command -- its success form, or its error
            representation after a caught exception.
        """
        if not isinstance(command, Command):
            command = Command(command)
        method_name = METHOD_NAME_BY_COMMAND[command]
        holder: Dict[str, ProtocolModel] = {}

        def op():
            request = parse_request()
            holder["request"] = request
            return self._run(command, request)

        def request_info() -> str:
            request = holder.get("request")
            if request is None:
                return "request failed validation"
            return _REQUEST_INFO_BY_COMMAND[command](request)

        def audit_target() -> Optional[List[str]]:
            request = holder.get("request")
            if request is None:
                return None
            return _audit_target(command, request)

        return await self._handle_command(
            method_name,
            request_info,
            op,
            error_response=_ERROR_RESPONSE_BY_COMMAND.get(
                command, command_error_response
            ),
            audit_target=audit_target,
            peer=peer,
        )

    async def handle(
        self,
        command: Command,
        request: ProtocolModel,
        peer: Optional[str] = None,
    ) -> ProtocolModel:
        """Dispatch a request DTO that is already parsed.

        This is :meth:`dispatch` with the parse reduced to a constant -- the
        form in-process callers (tests, and any transport whose request
        arrives as a DTO) use.
        """
        return await self.dispatch(command, lambda: request, peer=peer)

    def _run(self, command: Command, request: ProtocolModel):
        """Execute ``request`` against the scheduler and build the response.

        The result may be an awaitable (``init`` is the one asynchronous
        scheduler operation); the boundary awaits it.
        """
        if command is Command.INIT:
            return self._init(request)
        if command is Command.COMPLETE:
            return self._complete(request)
        if command is Command.ABORT:
            return self._abort(request)
        if command is Command.EVENT:
            return self._event(request)
        if command is Command.METER:
            return self._meter(request)
        if command is Command.REQUEUE:
            return self._service(self.scheduler.run_command_requeue, request)
        if command is Command.SUSPEND:
            return self._service(self.scheduler.run_command_suspend, request)
        if command is Command.RESUME:
            return self._service(self.scheduler.run_command_resume, request)
        if command is Command.RUN:
            return self._service(self.scheduler.run_command_run, request)
        if command is Command.FORCE:
            return self._service(self.scheduler.run_command_force, request)
        if command is Command.FREE_DEP:
            return self._service(self.scheduler.run_command_free_dep, request)
        if command is Command.LOAD:
            return self._service(self.scheduler.run_command_load, request)
        if command is Command.BEGIN:
            return self._service(self.scheduler.run_command_begin, request)
        if command is Command.SHOW:
            return ShowResponse(output=self.scheduler.handle_request_show(request))
        if command is Command.PING:
            return PingResponse()
        if command is Command.COROUTINE:
            return self._coroutine()
        raise UnsupportedCommandError(command)

    async def _init(self, request) -> ServiceResponse:
        await self.scheduler.run_command_init(request)
        return ServiceResponse()

    def _complete(self, request) -> ServiceResponse:
        self.scheduler.run_command_complete(request)
        return ServiceResponse()

    def _abort(self, request) -> ServiceResponse:
        self.scheduler.run_command_abort(request)
        return ServiceResponse()

    def _event(self, request) -> ServiceResponse:
        self.scheduler.run_command_event(request)
        return ServiceResponse()

    def _meter(self, request) -> ServiceResponse:
        self.scheduler.run_command_meter(request)
        return ServiceResponse()

    def _service(self, operation, request) -> ServiceResponse:
        operation(request)
        return ServiceResponse()

    def _coroutine(self) -> CoroutineResponse:
        loop = asyncio.get_running_loop()
        return CoroutineResponse(
            coroutines=[
                Coroutine(name=t.get_name(), description=repr(t.get_coro()))
                for t in asyncio.all_tasks(loop=loop)
            ]
        )

    # Exception boundary -------------------------------------------------

    def _trigger_fatal_shutdown(self):
        """Invoke the fatal-shutdown trigger if one was provided.

        In ``FAIL_FAST`` mode the handlers ask the owning ``TaklerServer`` to
        exit through its unified clean-shutdown path. If no trigger was wired
        in (e.g. the handlers are used standalone in tests), this is a no-op.
        """
        if self.fatal_shutdown is not None:
            self.fatal_shutdown()

    async def _handle_command(
        self,
        operation_name: str,
        request_info: Union[str, Callable[[], str]],
        op: Callable,
        error_response: Callable[[Exception], ProtocolModel],
        audit_target: Union[
            None, Sequence[str], Callable[[], Optional[Sequence[str]]]
        ] = None,
        peer: Optional[str] = None,
    ) -> ProtocolModel:
        """Run a handler body behind the command exception boundary.

        On the success path (no exception) ``op`` runs to completion and its
        result -- the command's success response DTO -- is returned unchanged,
        so ``flag=0`` command responses and the query success responses are
        preserved (Requirements 3.2, 3.3).

        On an unexpected exception the failure is always logged with enough
        context to diagnose it -- the operation name, key request fields and
        the exception type/message with a traceback (Requirement 2.7). Then, by
        policy:

        * ``RESILIENT`` (default): return the error representation of the
          command's response type (a non-zero ``flag`` ``ServiceResponse`` for
          command handlers, or the error/empty representation of the query
          response type), keeping the server running (Requirement 2.3).
        * ``FAIL_FAST``: fire the shared fatal-shutdown trigger so the server
          exits through its unified clean-shutdown path; an error response is
          still returned to the client before the server shuts down
          (Requirement 2.5).

        Parameters
        ----------
        operation_name
            Canonical operation name (the gRPC method name), used for
            diagnostic context and the Audit_Record.
        request_info
            Key request fields (e.g. node path), or a callable producing them.
            The callable form exists because the request DTO only exists once
            ``op`` has parsed it; it is consulted only when an error is
            logged, and reports a placeholder for a request that failed
            validation.
        op
            Zero-argument callable parsing the request, running the scheduler
            operation and building the success response. May be a coroutine
            function / return an awaitable.
        error_response
            Callable mapping the caught exception to the error representation
            of this command's response type.
        audit_target
            Node paths or flow names this command acts on, for the ``target``
            field of the Audit_Record (Requirement 11.9), or a callable
            producing them. Only the Control_Commands carry one; see
            :meth:`_audit_control`.
        peer
            The caller's network address as the transport knows it, read for
            the Audit_Record when the credentials published by the
            Auth_Interceptor do not carry one.

        Notes
        -----
        Whichever way the handling ends -- normal response, error response
        under ``RESILIENT``, or error response on the way out under
        ``FAIL_FAST`` -- a Control_Command produces exactly one Audit_Record,
        built from the response that is about to be returned
        (Requirement 11.2).
        """
        try:
            result = op()
            if inspect.isawaitable(result):
                result = await result
            self._audit_control(operation_name, _resolve(audit_target), result, peer)
            return result
        except Exception as exc:  # noqa: BLE001 - boundary is intentional
            info = request_info() if callable(request_info) else request_info
            logger.error(
                f"error handling RPC {operation_name} ({info}): "
                f"{type(exc).__name__}: {exc}",
                exc_info=True,
            )
            if self.exception_policy is ExceptionPolicy.FAIL_FAST:
                logger.critical(
                    f"fail-fast policy active after RPC {operation_name} "
                    f"failure; shutting server down"
                )
                self._trigger_fatal_shutdown()
            response = error_response(exc)
            self._audit_control(operation_name, _resolve(audit_target), response, peer)
            return response

    def _audit_control(
        self,
        operation_name: str,
        audit_target: Optional[Sequence[str]],
        response: ProtocolModel,
        peer: Optional[str],
    ) -> None:
        """Write the ``control`` Audit_Record of one handled command (Req 11.2).

        Only a Control_Command is recorded: ``show``, ``coroutine`` and
        ``ping`` are not (see :data:`CONTROL_METHOD_NAMES`). The membership
        test is what makes it safe for every command to funnel through
        :meth:`_handle_command` while only eight of them produce records.

        ``outcome`` and ``error_code`` come from the response that is about to
        be returned, so they agree with what the client sees by construction:
        a ``flag`` of 0 is ``success`` and anything else is ``error``
        (Requirement 11.7).

        ``user`` and ``peer`` come from the Credential_Metadata the
        Auth_Interceptor published for this call (Requirement 11.8). The peer
        address is not part of the metadata, so it is taken from the
        transport (the ``peer`` argument) when the published credentials do
        not carry it, which is the ordinary case.

        Nothing here can fail the command. :meth:`AuditLogger.record` already
        absorbs every write failure (Requirement 11.15), and the record
        building is wrapped as well: a command must not fail because its audit
        record could not be assembled.

        Args:
            operation_name: The canonical operation name of the command.
            audit_target: Node paths or flow names the command acted on.
            response: The response about to be returned, read for its ``flag``.
            peer: The caller's network address as the transport reported it,
                or ``None``.
        """
        if self.audit_logger is None or operation_name not in CONTROL_METHOD_NAMES:
            return

        try:
            flag = int(getattr(response, "flag", SUCCESS) or SUCCESS)
            credentials = get_call_credentials()
            record_peer = credentials.peer
            if record_peer is None:
                record_peer = peer
            self.audit_logger.record(
                AuditRecord(
                    timestamp=audit_timestamp(),
                    event=EVENT_CONTROL,
                    command=audit_command_name(operation_name),
                    user=credentials.audit_user(),
                    peer=audit_peer(record_peer),
                    target=list(audit_target) if audit_target else [],
                    outcome=OUTCOME_SUCCESS if flag == SUCCESS else OUTCOME_ERROR,
                    error_code=flag,
                )
            )
        except Exception as exc:  # noqa: BLE001 - auditing is never fatal
            logger.warning(
                f"could not build the audit record of {operation_name}: "
                f"{type(exc).__name__}: {exc}"
            )


def _resolve(
    target: Union[None, Sequence[str], Callable[[], Optional[Sequence[str]]]],
) -> Optional[Sequence[str]]:
    """Resolve an ``audit_target`` given either directly or as a callable."""
    if callable(target):
        return target()
    return target


class UnsupportedCommandError(Exception):
    """A command with no registered handler implementation.

    Unreachable while :data:`METHOD_NAME_BY_COMMAND` and
    :meth:`CommandHandlers._run` cover the same sixteen commands; the tests
    of this module pin that correspondence.
    """
