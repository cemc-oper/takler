"""The client-side transport interface.

A *transport* is the one way a client reaches the server over a given wire
protocol: gRPC (:class:`~takler.client.grpc_transport.GrpcTransport`, the
default) today, HTTP with the ``takler[http]`` extra in a later milestone.
:class:`~takler.client.service_client.TaklerServiceClient` depends only on
this interface, so the command methods -- and the TUI above them -- never
know which protocol carries a call.

A transport owns everything the wire imposes:

* the connection lifecycle (:meth:`ClientTransport.open` /
  :meth:`ClientTransport.close`),
* the encoding of the request payload and the decoding of the response into
  the DTOs of :mod:`takler.protocol.commands`,
* the Credential_Metadata every call carries (the same three keys on every
  transport -- gRPC metadata keys, HTTP header names),
* the retry loop: per-attempt deadline, backoff and Retry_Window, and the
  classification of a transport failure as retryable, fatal or "the request
  itself is wrong".

What a transport never sees is a *business* failure: a response that arrived
carries its Error_Code in ``flag`` and is returned to the caller unchanged,
whatever its value (requirement 9.7).

The request travels as a plain payload mapping keyed by the request DTO's
field names, not as a validated DTO: validating on the client would diverge
the two clients, since a malformed value (a non-numeric ``meter_value``) must
reach the server and be answered with the same ``flag`` whichever client
carried it. Validation is the server's end of the wire.
"""

from __future__ import annotations

from typing import Any, Mapping, Protocol

from takler.protocol.commands import Command, ProtocolModel

__all__ = ["ClientTransport"]


class ClientTransport(Protocol):
    """One way for the client to reach the server.

    Implementations: :class:`takler.client.grpc_transport.GrpcTransport`
    (gRPC, always available), and the HTTP transport of the ``takler[http]``
    extra. Unlike the server side the interface is synchronous: the client's
    callers -- the CLI, job scripts, the TUI's polling thread -- are
    synchronous, so the async-ness of a wire stack is the transport's own
    business.
    """

    def open(self) -> None:
        """Establish the connection, if the transport keeps one.

        Called before the first :meth:`call` of a session; transports without
        a persistent connection treat it as a no-op. May raise
        ``InvalidRequestError`` for a misconfiguration that must name itself
        before any call is attempted (an unreadable TLS CA file).
        """
        ...

    def close(self) -> None:
        """Release the connection. Always safe to call, even unopened."""
        ...

    def call(
        self,
        command: Command,
        payload: Mapping[str, Any],
    ) -> ProtocolModel:
        """Run one command and return its response DTO.

        Args:
            command: The command to run.
            payload: The request fields, keyed by the request DTO's field
                names, in wire shape (``meter_value`` as the string the
                caller passed, enum fields as their names). Not validated on
                this side of the wire -- see the module docstring.

        Returns:
            The response DTO of the command, including responses whose
            ``flag`` is non-zero: a business failure is not a transport
            failure, so it is never retried and never raised here
            (requirement 9.7).

        Raises:
            InvalidRequestError, NodeNotFoundError, PermissionDeniedError:
                The transport-level refusal kinds where retrying cannot help
                (requirement 9.8).
            ClientConnectionError: The Retry_Window is exhausted
                (requirement 9.5).
            TransportError: Any other transport-level failure.
        """
        ...
