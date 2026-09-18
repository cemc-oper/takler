"""The HTTP transport of the takler server (``takler[http]``).

``HttpTransport`` is the :class:`~takler.server.transport.ServerTransport`
that speaks HTTP: a FastAPI application serving the command endpoint, mounted
on a uvicorn server whose lifecycle (``start`` / ``run`` / ``stop``) the
class drives the same way :class:`~takler.server.grpc_transport.GrpcTransport`
drives the ``grpc.aio`` one. The module lives behind the ``http`` extra --
fastapi and uvicorn are not part of the default install -- so it is only ever
imported when a ``connect.yaml`` asks for the HTTP listener; ``TaklerServer``
does that import lazily and reports a missing extra as a startup error.

The wire contract is the JSON :class:`~takler.protocol.envelope.Envelope`:

* one endpoint, ``POST /v1/commands/{command}``, where ``{command}`` is the
  CLI word of one of the sixteen commands (``init`` .. ``coroutine``); the
  body is the request envelope and the answer is the response envelope, whose
  ``trace_id`` echoes the request's;
* the HTTP status code says nothing about the command's outcome -- a business
  failure is a ``200`` whose response payload carries the Error_Code in
  ``flag``, exactly as on gRPC. The status code is reserved for transport
  level failures: an unroutable command (``422``, FastAPI's parameter
  validation), a body that is not well formed JSON or not a valid envelope
  (``422``; the JSON syntax check is the one step that may precede
  authentication), an envelope whose ``command`` disagrees with the URL
  (``400``), and an authentication refusal (``401`` / ``403``).

Authentication is the same decision the gRPC boundary makes, taken by the
same object: the three credential keys of the cross-language contract arrive
as the HTTP headers ``takler-pass`` / ``takler-secret`` / ``takler-user``
(the header names are the gRPC metadata keys, and HTTP headers are
case-insensitive, so the transport passes them to the
:class:`~takler.server.auth.AuthGate` unchanged), the gate answers an
:class:`~takler.server.auth.AuthOutcome`, and a refusal is recorded by
:meth:`~takler.server.auth.AuthGate.refuse` -- the same WARNING and the same
``denied`` Audit_Record as on gRPC -- before the status code goes out. The
accepted credentials are published into the call's context for exactly the
duration of the dispatch (see :func:`create_app`), so the Zombie_Detector and
the Audit_Logger read them through the same
:func:`~takler.server.auth.get_call_credentials` they read on gRPC.

TLS is configured with a certificate/key pair handed to uvicorn (or,
recommended, terminated at a reverse proxy in front of the plaintext
listener; see ``doc/source/operation/deployment.rst``). The pair is resolved
and validated by ``TaklerServer`` before any listener starts, mirroring the
gRPC side: a half configured or unreadable pair aborts the start-up rather
than degrading to plaintext.
"""

from __future__ import annotations

import socket
from typing import Callable, Dict, Optional

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request

from takler.logging import get_logger
from takler.protocol.commands import Command
from takler.protocol.envelope import Envelope
from takler.server.audit import AuditLogger
from takler.server.auth import (
    SERVICE_METHOD_PREFIX,
    AuthGate,
    CallCredentials,
    RejectionReason,
    reset_call_credentials,
    set_call_credentials,
)
from takler.server.connect_config import ExceptionPolicy
from takler.server.handlers import METHOD_NAME_BY_COMMAND, CommandHandlers
from takler.server.scheduler import Scheduler
from takler.server.transport import ServerTransport

__all__ = [
    "API_PREFIX",
    "HTTP_STATUS_BY_REJECTION",
    "HttpTransport",
    "create_app",
]

logger = get_logger("server.service")


#: URL prefix of every route this transport serves. Versioned so a future
#: envelope format can be served next to this one.
API_PREFIX: str = "/v1"

#: The one route: the command is addressed by its CLI word in the path.
COMMAND_PATH: str = API_PREFIX + "/commands/{command}"


#: HTTP status code each :class:`~takler.server.auth.RejectionReason` is
#: answered with. The split
#: mirrors ``STATUS_CODE_BY_REJECTION`` on the gRPC side: ``401`` is "you
#: presented no credentials", ``403`` is "you presented credentials and they
#: do not grant this" -- which is exactly what ``UNAUTHENTICATED`` and
#: ``PERMISSION_DENIED`` mean there.
HTTP_STATUS_BY_REJECTION: Dict[RejectionReason, int] = {
    RejectionReason.MISSING_CREDENTIAL: 401,
    RejectionReason.INVALID_CREDENTIAL: 403,
    RejectionReason.NOT_IN_WHITELIST: 403,
}


def _peer_of(request: Request) -> Optional[str]:
    """Read the caller's network address off the HTTP request.

    The form is ``host:port``; ``None`` when the server cannot say (an ASGI
    server is not required to report a client). The address is diagnostic --
    it feeds the refusal record and the Audit_Record -- so reading it never
    fails the command.
    """
    client = request.client
    if client is None:
        return None
    return f"{client.host}:{client.port}"


def create_app(
    handlers: CommandHandlers,
    gate: Optional[AuthGate] = None,
) -> FastAPI:
    """Build the FastAPI application of the HTTP transport.

    Kept separate from :class:`HttpTransport` so a test can drive the whole
    HTTP boundary -- routing, envelope validation, authentication, dispatch --
    through an ASGI client without standing up a uvicorn server.

    Args:
        handlers: The transport-neutral command handlers the endpoint
            dispatches to (the exception boundary, the Error_Code mapping and
            the control-command audit all live there).
        gate: The :class:`~takler.server.auth.AuthGate` authenticating every
            request. A server
            running both transports hands both the same instance, so a
            credential is accepted or refused identically on either. ``None``
            builds a default gate (Auth_Mode ``disabled``), which is what a
            standalone test gets.

    Returns:
        The application. It serves exactly one route,
        ``POST /v1/commands/{command}``; FastAPI's own ``/docs`` comes along
        with it and doubles as the transport's self-description.
    """
    if gate is None:
        gate = AuthGate()
    app = FastAPI(title="takler")

    async def authorize(
        request: Request, command: Command
    ) -> Optional[CallCredentials]:
        """Authenticate one request through the shared gate.

        The method name the gate decides on is the canonical operation name
        of the command -- the gRPC method name, fully qualified -- so the
        privilege table, the refusal records and the audit trail neither know
        nor care which transport a call arrived on.

        A refusal is recorded by the gate and then answered with the mapped
        status code; the endpoint never runs, so a refused request cannot
        change any node state. The gate also decides before the *envelope* is
        looked at -- FastAPI resolves dependencies before validating the
        body, so an unauthenticated caller cannot even make the server
        validate an envelope; the only parsing that may precede the decision
        is the JSON syntax check itself.
        """
        method = SERVICE_METHOD_PREFIX + METHOD_NAME_BY_COMMAND[command]
        outcome = gate.authorize(
            method, request.headers.items(), peer=_peer_of(request)
        )
        if outcome.rejection is not None:
            # The credentials are never None on this path: a rejection only
            # exists for a CHILD or OPERATOR level method, whose metadata the
            # gate parsed.
            text = gate.refuse(method, outcome.credentials, outcome.rejection)
            raise HTTPException(
                status_code=HTTP_STATUS_BY_REJECTION[outcome.rejection],
                detail=text,
            )
        return outcome.credentials

    @app.post(COMMAND_PATH, response_model=Envelope)
    async def run_command(
        command: Command,
        envelope: Envelope,
        request: Request,
        credentials: Optional[CallCredentials] = Depends(authorize),
    ) -> Envelope:
        """Run one command and answer with its response envelope.

        The request payload is parsed *inside* the handlers' exception
        boundary, so a payload that fails DTO validation is classified and
        answered like any command failure -- ``200`` with a non-zero ``flag``
        -- exactly as on gRPC, where a malformed meter value is the server's
        ``internal_error`` and not a transport error.
        """
        if envelope.command is not command:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"the envelope command {envelope.command.value!r} does not "
                    f"match the command of the URL {command.value!r}"
                ),
            )

        # Publish the accepted credentials for exactly the duration of the
        # dispatch: the Zombie_Detector and the Audit_Logger read them from
        # the context variable, and the reset guarantees that nothing --
        # neither the next request on this keep-alive connection nor the test
        # driving this app in its own context -- ever observes another call's
        # identity.
        token = set_call_credentials(credentials) if credentials is not None else None
        try:
            response = await handlers.dispatch(
                command, envelope.parse_request, peer=_peer_of(request)
            )
        finally:
            if token is not None:
                reset_call_credentials(token)

        return Envelope.for_response(command, response, trace_id=envelope.trace_id)

    return app


def _bind_socket(host: str, port: int) -> socket.socket:
    """Bind the listen address and return the bound socket.

    The socket is bound here rather than by uvicorn because uvicorn's own
    bind path answers an ``OSError`` -- the port taken, the address
    unassignable -- with ``sys.exit``, killing the server process from inside
    ``start()`` with no chance for the unified clean-shutdown path to run. A
    bind failure here is an ordinary exception out of :meth:`HttpTransport.start`,
    the same way ``add_insecure_port`` failing is on the gRPC side.

    ``SO_REUSEADDR`` matches what asyncio and uvicorn set when they bind
    themselves; the ``listen()`` call itself is left to asyncio's
    ``create_server``, which uvicorn invokes with this socket.
    """
    infos = socket.getaddrinfo(
        host, port, type=socket.SOCK_STREAM, flags=socket.AI_PASSIVE
    )
    family, socktype, proto, _, sockaddr = infos[0]
    sock = socket.socket(family, socktype, proto)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(sockaddr)
    except OSError:
        sock.close()
        raise
    return sock


class HttpTransport(ServerTransport):
    """HTTP 服务端 transport：FastAPI 应用挂在 uvicorn 上，与 gRPC transport 同进程并存。

    Attributes
    ----------
    scheduler : Scheduler
        A link to the scheduler. Service use it to run commands.
    handlers : CommandHandlers
        The transport-neutral command handlers the endpoint dispatches to.
    gate : AuthGate
        The authentication decision layer shared with the gRPC boundary.
    host : str
        Service host
    port : int
        Service port
    app : fastapi.FastAPI
        The application uvicorn serves; also directly drivable by a test
        through an ASGI client.
    tls_cert_file : Optional[str]
        Certificate file the listener is bound with, kept so the start-up
        record can name it. ``None`` means plaintext.
    tls_key_file : Optional[str]
        The private key of the pair, all-or-nothing with the certificate.
    """

    def __init__(
        self,
        scheduler: Scheduler,
        host: str = None,
        port: int = None,
        exception_policy: Optional[ExceptionPolicy] = None,
        fatal_shutdown: Optional[Callable[[], None]] = None,
        gate: Optional[AuthGate] = None,
        audit_logger: Optional[AuditLogger] = None,
        tls_cert_file: Optional[str] = None,
        tls_key_file: Optional[str] = None,
    ):
        self.scheduler: Scheduler = scheduler
        if host is None:
            host = "0.0.0.0"
        if port is None:
            port = 8080
        self.host: str = host
        self.port: int = port
        # Exception-handling policy, fatal-shutdown trigger and audit logger
        # are threaded in from ``TaklerServer`` and handed to the command
        # handlers, exactly as on the gRPC side.
        self.handlers: CommandHandlers = CommandHandlers(
            scheduler,
            exception_policy=exception_policy,
            fatal_shutdown=fatal_shutdown,
            audit_logger=audit_logger,
        )
        # A standalone transport (a test) gets a gate of its own; a server
        # running both transports hands both one shared instance.
        self.gate: AuthGate = (
            gate if gate is not None else AuthGate(audit_logger=audit_logger)
        )
        self.app: FastAPI = create_app(self.handlers, gate=self.gate)
        self.tls_cert_file: Optional[str] = tls_cert_file
        self.tls_key_file: Optional[str] = tls_key_file
        self._server: Optional[uvicorn.Server] = None

    @property
    def listen_address(self) -> str:
        """
        str: HTTP server's listen address
        """
        return f"{self.host}:{self.port}"

    async def start(self):
        """
        Start the HTTP server: bind the listen address and be ready to serve.

        The socket is bound by :func:`_bind_socket` and handed to uvicorn, so
        a bind failure raises out of here -- before the server announces
        itself as started -- instead of exiting the process. The TLS pair, if
        configured, is handed to uvicorn as certificate and key file paths;
        that the pair is readable and matches has already been validated by
        ``TaklerServer`` before any listener started.

        Which posture was bound is stated in the log the same way the gRPC
        side states it: an INFO naming the address and the certificate file
        for TLS, a WARNING naming the address and saying the transport is
        unencrypted otherwise.
        """
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            # Logging stays takler's: uvicorn must neither install its own
            # logging configuration nor duplicate the command records the
            # handler layer writes.
            log_config=None,
            access_log=False,
            ssl_certfile=self.tls_cert_file,
            ssl_keyfile=self.tls_key_file,
        )
        # The preamble of uvicorn's own ``_serve`` up to ``startup``, run by
        # hand: ``serve()`` would additionally install its own SIGINT /
        # SIGTERM handlers, which would shadow the Server_CLI's (a signal
        # must reach ``TaklerServer.stop`` so the final snapshot is written,
        # requirement 5.9). Loading the config and constructing the lifespan
        # here is what ``startup`` assumes done in recent uvicorn releases;
        # older ones redo both harmlessly.
        if not config.loaded:
            config.load()
        self._server = uvicorn.Server(config)
        self._server.lifespan = config.lifespan_class(config)
        sock = _bind_socket(self.host, self.port)
        try:
            await self._server.startup(sockets=[sock])
        except BaseException:
            sock.close()
            raise
        if self.tls_cert_file is None:
            logger.warning(
                f"listening on {self.listen_address} without TLS: the HTTP "
                f"transport is unencrypted, so commands, node paths and "
                f"credentials cross the network in the clear; configure a "
                f"certificate and private key for the HTTP listener, or "
                f"terminate TLS at a reverse proxy in front of it"
            )
        else:
            logger.info(
                f"listening on {self.listen_address} with TLS, certificate "
                f"file {self.tls_cert_file!r}"
            )
        logger.info(f"service started: {self.listen_address}")

    async def run(self):
        """
        Serve until :meth:`stop` is called, then shut uvicorn down.

        ``main_loop`` wakes on the ``should_exit`` flag that :meth:`stop`
        sets (within one tick, a tenth of a second), after which the shutdown
        closes the listener and finishes the in-flight requests.
        """
        if self._server is None:
            raise RuntimeError("HttpTransport.start() must run before run()")
        await self._server.main_loop()
        await self._server.shutdown()

    async def stop(self):
        """
        Ask the HTTP server to stop serving.

        The actual wind-down happens in :meth:`run`, which is where the
        serving task is; a transport that was never started (a start-up that
        failed earlier) is a no-op.
        """
        if self._server is not None:
            self._server.should_exit = True
