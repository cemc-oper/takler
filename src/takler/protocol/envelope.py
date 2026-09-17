"""The command envelope: one wrapper for every command, on every transport.

An :class:`Envelope` carries a command's request -- or its response -- plus
the metadata that is about the *message* rather than about the command:

* ``version`` is the envelope format version (``"1"`` today). It exists so a
  future format change can be recognized instead of failing obscurely; no
  negotiation happens in this phase.
* ``trace_id`` correlates the log lines a command produces on its way
  through client, transport and server. It is generated per request and
  passed through into the server's log records; nothing here feeds a tracing
  system yet.
* ``target`` is reserved for the far-future proxy routing of the roadmap and
  is never set in this phase.
* ``auth`` carries the three credential values when they travel *inside* the
  message. On gRPC they travel as metadata keys and on HTTP as headers
  instead; the field exists for the proxy shape, where a forwarder needs the
  credentials in the body it relays.

The gRPC transport never serializes an envelope -- the method name and the
message play the roles of ``command`` and ``payload`` there. The envelope is
the HTTP wire form, and the place where ``trace_id`` is generated and read
regardless of transport.

``command`` is typed as :class:`~takler.protocol.commands.Command`, not as a
plain ``str``: every consumer in this phase needs a known command, so an
unknown name fails at envelope validation rather than somewhere downstream.
A forwarder that wanted to relay commands it does not know would relax this
-- deliberately not supported yet.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, Optional

from pydantic import Field

from takler.protocol.commands import (
    REQUEST_TYPE_BY_COMMAND,
    RESPONSE_TYPE_BY_COMMAND,
    Command,
    ProtocolModel,
)

__all__ = [
    "PROTOCOL_VERSION",
    "AuthInfo",
    "Envelope",
]

#: The envelope format version. Bump it when the envelope's shape changes in
#: a way a reader must know about before parsing.
PROTOCOL_VERSION: str = "1"


def _new_trace_id() -> str:
    """Return a fresh Trace_Id: 32 lowercase hex characters, no dashes."""
    return uuid.uuid4().hex


class AuthInfo(ProtocolModel):
    """The three credential values of the cross-language contract.

    The field names mirror the gRPC metadata keys ``takler-pass`` /
    ``takler-secret`` / ``takler-user`` (and the HTTP headers those keys
    become). Every field is optional: which of them a call must carry is a
    property of the command's privilege level, decided by the auth layer,
    not of this model. The caller's network address is deliberately absent:
    it is a property of the connection, read by each transport from its own
    context.
    """

    job_password: Optional[str] = None
    secret: Optional[str] = None
    user: Optional[str] = None


class Envelope(ProtocolModel):
    """One command request or response plus its message metadata."""

    version: str = PROTOCOL_VERSION
    trace_id: str = Field(default_factory=_new_trace_id)
    target: Optional[str] = None
    auth: Optional[AuthInfo] = None
    command: Command
    payload: Dict[str, Any]

    @classmethod
    def for_request(
        cls,
        command: Command,
        request: ProtocolModel,
        *,
        auth: Optional[AuthInfo] = None,
        target: Optional[str] = None,
    ) -> "Envelope":
        """Build the envelope carrying ``request`` for ``command``.

        ``request`` must be the request DTO type registered for ``command``
        in ``REQUEST_TYPE_BY_COMMAND``; a mismatch is a programming error of
        the caller and raises ``TypeError`` rather than producing an envelope
        whose payload fails to parse on the far side.

        The payload is serialized in JSON mode, so a ``bytes`` field already
        sits in the dict in its base64 form.
        """
        request_type = REQUEST_TYPE_BY_COMMAND[command]
        if not isinstance(request, request_type):
            raise TypeError(
                f"command {command.value!r} takes {request_type.__name__}, "
                f"got {type(request).__name__}"
            )
        return cls(
            command=command,
            payload=request.model_dump(mode="json"),
            auth=auth,
            target=target,
        )

    @classmethod
    def for_response(
        cls,
        command: Command,
        response: ProtocolModel,
        *,
        trace_id: str,
    ) -> "Envelope":
        """Build the envelope carrying ``response`` for ``command``.

        ``trace_id`` is required (no default): the response must echo the
        request's Trace_Id, and having to pass it is what makes the echo
        hard to forget. Same type check as :meth:`for_request`, against
        ``RESPONSE_TYPE_BY_COMMAND``.
        """
        response_type = RESPONSE_TYPE_BY_COMMAND[command]
        if not isinstance(response, response_type):
            raise TypeError(
                f"command {command.value!r} answers {response_type.__name__}, "
                f"got {type(response).__name__}"
            )
        return cls(
            command=command,
            payload=response.model_dump(mode="json"),
            trace_id=trace_id,
        )

    def parse_request(self) -> ProtocolModel:
        """Validate ``payload`` as the request DTO of ``command``.

        Raises ``pydantic.ValidationError`` when the payload does not fit the
        registered request type; mapping that failure to an Error_Code is
        the transport adapter's job.
        """
        return REQUEST_TYPE_BY_COMMAND[self.command].model_validate(self.payload)

    def parse_response(self) -> ProtocolModel:
        """Validate ``payload`` as the response DTO of ``command``."""
        return RESPONSE_TYPE_BY_COMMAND[self.command].model_validate(self.payload)
