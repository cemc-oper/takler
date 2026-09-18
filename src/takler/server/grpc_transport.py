"""The gRPC transport of the takler server.

``GrpcTransport`` is the :class:`~takler.server.transport.ServerTransport`
that speaks gRPC: it owns the ``grpc.aio`` server lifecycle (``start`` /
``run`` / ``stop``) and the listen-address bookkeeping, and -- as the
generated servicer's implementation -- adapts each of the 16 RPCs to the
transport-neutral handler layer: every method converts its pb2 request into
the command's DTO (``takler.server.protocol.adapter``), hands it to
:class:`~takler.server.handlers.CommandHandlers` and converts the response
DTO back. Everything a command needs beyond its encoding -- the exception
boundary, the Error_Code mapping, the control audit -- lives in that handler
layer and is shared with any other transport.

Authentication is not the transport's business either: every RPC passes the
:class:`~takler.server.auth.AuthInterceptor` -- the gRPC adapter of the
transport-neutral :class:`~takler.server.auth.AuthGate` -- which
``TaklerServer`` registers at construction, because ``grpc.aio`` only accepts
interceptors when the server object is created.
"""

from typing import Callable, Optional, Sequence

import grpc

from takler.protocol.commands import Command
from takler.server.protocol import adapter, takler_pb2_grpc
from takler.logging import get_logger
from takler.server.audit import AuditLogger
from takler.server.handlers import CommandHandlers
from takler.server.scheduler import Scheduler
from takler.server.connect_config import ExceptionPolicy
from takler.server.transport import ServerTransport


logger = get_logger("server.service")


def _peer_of(context) -> Optional[str]:
    """Read the caller's network address off a gRPC ``ServicerContext``.

    ``None`` when there is no context or it cannot say; the address is
    diagnostic (it feeds the Audit_Record), so reading it never fails the
    command.
    """
    if context is None:
        return None
    try:
        return context.peer()
    except Exception:  # noqa: BLE001 - the address is diagnostic
        return None


class GrpcTransport(takler_pb2_grpc.TaklerServerServicer, ServerTransport):
    """
    gRPC 服务端，把 16 个 RPC 适配到传输中立的命令 handler。

    Attributes
    ----------
    scheduler : Scheduler
        A link to the scheduler. Service use it to run commands.
    handlers : CommandHandlers
        The transport-neutral command handlers every RPC is dispatched to.
    host : str
        Service host
    port : int
        Service port
    server_credentials : Optional[grpc.ServerCredentials]
        TLS credentials the listen address is bound with. ``None`` means TLS is
        not configured and the port is bound in plaintext (Requirements 1.1,
        1.2).
    interceptors : Sequence[grpc.aio.ServerInterceptor]
        Server interceptors, the Auth_Interceptor among them. They have to be
        known before :meth:`start` because ``grpc.aio`` only accepts them when
        the server object is constructed.
    tls_cert_file : Optional[str]
        Path the ``server_credentials`` were built from, kept only so the
        start-up INFO record can name it (Requirement 1.7).
    """

    def __init__(
        self,
        scheduler: Scheduler,
        host: str = None,
        port: int = None,
        exception_policy: Optional[ExceptionPolicy] = None,
        fatal_shutdown: Optional[Callable[[], None]] = None,
        server_credentials: Optional[grpc.ServerCredentials] = None,
        interceptors: "Optional[Sequence[grpc.aio.ServerInterceptor]]" = None,
        tls_cert_file: Optional[str] = None,
        audit_logger: Optional[AuditLogger] = None,
    ):
        self.scheduler: Scheduler = scheduler
        if host is None:
            host = "[::]"
        if port is None:
            port = 33083
        self.host: str = host
        self.port: int = port
        self.grpc_server: Optional[grpc.aio.Server] = None
        # Exception-handling policy, fatal-shutdown trigger and audit logger
        # are threaded in from ``TaklerServer`` and handed to the command
        # handlers, which own the RPC exception boundary: in RESILIENT mode a
        # caught exception is logged and converted into an error response of
        # the command's response type; in FAIL_FAST mode it is logged and then
        # the shared fatal-shutdown trigger is fired so the server exits
        # through its unified clean-shutdown path.
        self.handlers: CommandHandlers = CommandHandlers(
            scheduler,
            exception_policy=exception_policy,
            fatal_shutdown=fatal_shutdown,
            audit_logger=audit_logger,
        )

        # TLS credentials and interceptors are decided by ``TaklerServer`` and
        # only consumed here, in ``start()``. Both are plain attributes rather
        # than read-only properties because the owning server resolves the TLS
        # pair during its own ``start()`` -- a security misconfiguration must
        # abort the start-up, not the construction of the service (Requirement
        # 1.6 relies on the Server_CLI catching it around the start-up).
        self.server_credentials: Optional[grpc.ServerCredentials] = server_credentials
        self.tls_cert_file: Optional[str] = tls_cert_file
        # ``grpc.aio`` has no way to add an interceptor to an existing server, so
        # the list has to be complete before ``grpc.aio.server()`` is called.
        self.interceptors: "tuple[grpc.aio.ServerInterceptor, ...]" = (
            tuple(interceptors) if interceptors else ()
        )

    @property
    def listen_address(self) -> str:
        """
        str: gRPC server's listen address
        """
        return f"{self.host}:{self.port}"

    async def start(self):
        """
        Start gRPC server.

        The listen address is bound with :attr:`server_credentials` when TLS is
        configured and in plaintext when it is not (Requirements 1.1, 1.2), and
        which of the two happened is stated in the log: an INFO naming the
        address and the certificate file for TLS (Requirement 1.7), a WARNING
        naming the address and saying the transport is unencrypted otherwise
        (Requirement 1.3). The plaintext case is a warning and not merely an
        info because it is the default, so it is the case an operator is most
        likely to be in without having decided to be.

        :attr:`interceptors` are handed to ``grpc.aio.server()`` here because
        that is the only place they can be handed over at all -- unlike the
        synchronous server, ``grpc.aio`` offers no way to register an
        interceptor afterwards.
        """
        self.grpc_server = grpc.aio.server(interceptors=self.interceptors)
        takler_pb2_grpc.add_TaklerServerServicer_to_server(self, self.grpc_server)
        if self.server_credentials is None:
            self.grpc_server.add_insecure_port(self.listen_address)
            logger.warning(
                f"listening on {self.listen_address} without TLS: the transport "
                f"is unencrypted, so commands, node paths and credentials cross "
                f"the network in the clear; configure a server certificate and "
                f"private key to enable TLS"
            )
        else:
            self.grpc_server.add_secure_port(
                self.listen_address, self.server_credentials
            )
            logger.info(
                f"listening on {self.listen_address} with TLS, certificate file "
                f"{self.tls_cert_file!r}"
            )
        await self.grpc_server.start()
        logger.info(f"service started: {self.listen_address}")

    async def run(self):
        """
        Wait until gRPC server is terminated.
        """
        await self.grpc_server.wait_for_termination()

    async def stop(self):
        """
        Stop gRPC server with time limit.
        """
        logger.info("service shutting down..")
        await self.grpc_server.stop(5)
        logger.info("service shutting down..done")

    # RPC methods ----------------------------------------------------
    #
    # Every method is the same one-liner: parse the pb2 request into the
    # command's DTO (inside the handlers' exception boundary, so a validation
    # failure is classified and answered like any command failure), run the
    # command, encode the response DTO back into pb2.

    async def _dispatch(self, command: Command, request, context):
        response = await self.handlers.dispatch(
            command,
            lambda: adapter.request_from_pb2(command, request),
            peer=_peer_of(context),
        )
        return adapter.response_to_pb2(response)

    async def RunCommandInit(self, request, context):
        return await self._dispatch(Command.INIT, request, context)

    async def RunCommandComplete(self, request, context):
        return await self._dispatch(Command.COMPLETE, request, context)

    async def RunCommandAbort(self, request, context):
        return await self._dispatch(Command.ABORT, request, context)

    async def RunCommandEvent(self, request, context):
        return await self._dispatch(Command.EVENT, request, context)

    async def RunCommandMeter(self, request, context):
        return await self._dispatch(Command.METER, request, context)

    async def RunCommandRequeue(self, request, context):
        return await self._dispatch(Command.REQUEUE, request, context)

    async def RunCommandSuspend(self, request, context):
        return await self._dispatch(Command.SUSPEND, request, context)

    async def RunCommandResume(self, request, context):
        return await self._dispatch(Command.RESUME, request, context)

    async def RunCommandRun(self, request, context):
        return await self._dispatch(Command.RUN, request, context)

    async def RunCommandForce(self, request, context):
        return await self._dispatch(Command.FORCE, request, context)

    async def RunCommandFreeDep(self, request, context):
        return await self._dispatch(Command.FREE_DEP, request, context)

    async def RunCommandLoad(self, request, context):
        return await self._dispatch(Command.LOAD, request, context)

    async def RunCommandBegin(self, request, context):
        return await self._dispatch(Command.BEGIN, request, context)

    async def RunRequestShow(self, request, context):
        return await self._dispatch(Command.SHOW, request, context)

    async def RunRequestPing(self, request, context):
        return await self._dispatch(Command.PING, request, context)

    async def QueryCoroutine(self, request, context):
        return await self._dispatch(Command.COROUTINE, request, context)
