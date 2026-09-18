"""The server-side transport mount point.

A *transport* is one way for the server to receive commands: the gRPC
listener (:class:`~takler.server.grpc_transport.GrpcTransport`) today, the
HTTP listener of ``takler[http]`` alongside it in a later milestone. What the
``TaklerServer`` needs of either is the same small
surface, defined here as :class:`ServerTransport`: bind the listen address
(``start``), serve until stopped (``run``) and release the port (``stop``).

A transport owns its wire concerns -- sockets, TLS at the listener, metadata
or header adaptation -- and nothing else: every command it accepts is handed
to the transport-neutral :class:`~takler.server.handlers.CommandHandlers` as
a DTO, and every call passes the transport-neutral
:class:`~takler.server.auth.AuthGate` first. ``listen_address`` is the one
piece of information the rest of the server needs back, for the start-up
record and the job scripts' ``TAKLER_HOST`` / ``TAKLER_PORT``.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["ServerTransport"]


@runtime_checkable
class ServerTransport(Protocol):
    """One listen endpoint of the server.

    Implementations: :class:`takler.server.grpc_transport.GrpcTransport`
    (gRPC, the default), and the HTTP transport of the ``takler[http]``
    extra. The protocol is deliberately asynchronous end to end: the gRPC
    side is built on ``grpc.aio`` and the HTTP side on an ASGI server, so a
    blocking surface would fit neither.
    """

    @property
    def listen_address(self) -> str:
        """The ``host:port`` the transport listens on, for log records."""
        ...

    async def start(self) -> None:
        """Bind the listen address and be ready to serve.

        A misconfiguration that must abort the server's start-up -- an
        unreadable TLS certificate, say -- raises from here, before the
        server announces itself as started.
        """
        ...

    async def run(self) -> None:
        """Serve until :meth:`stop` is called."""
        ...

    async def stop(self) -> None:
        """Stop serving and release the listen address."""
        ...
