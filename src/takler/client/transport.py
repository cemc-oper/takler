"""The client-side transport interface.

A *transport* is the one way a client reaches the server over a given wire
protocol: gRPC (:class:`~takler.client.grpc_transport.GrpcTransport`, the
default) or HTTP (:class:`~takler.client.http_transport.HttpTransport`, with
the ``takler[http]`` extra, M3 task 8).
:class:`~takler.client.service_client.TaklerServiceClient` depends only on
this interface, so the command methods -- and the TUI above them -- never
know which protocol carries a call.

A transport owns everything the wire imposes:

* the connection lifecycle (:meth:`ClientTransport.open` /
  :meth:`ClientTransport.close`),
* the encoding of the request payload and the decoding of the response into
  the DTOs of :mod:`takler.protocol.commands`,
* the Credential_Metadata every call carries (the same three keys on every
  transport -- gRPC metadata keys, HTTP header names -- built once by
  :func:`build_credential_pairs`),
* the retry loop: per-attempt deadline, backoff and Retry_Window, and the
  classification of a transport failure as retryable, fatal or "the request
  itself is wrong". Both transports run the same loop
  (:func:`takler.client.retry.run_with_retry`) over their own mapping of
  wire failures to :class:`~takler.client.retry.FailureVerdict`, so the
  retry semantics -- and the exceptions the CLI turns into exit codes --
  cannot drift apart.

What a transport never sees is a *business* failure: a response that arrived
carries its Error_Code in ``flag`` and is returned to the caller unchanged,
whatever its value (requirement 9.7).

The request travels as a plain payload mapping keyed by the request DTO's
field names, not as a validated DTO: validating on the client would diverge
the two clients, since a malformed value (a non-numeric ``meter_value``) must
reach the server and be answered with the same ``flag`` whichever client
carried it. Validation is the server's end of the wire.

Which transport a client uses is decided by :func:`resolve_transport` --
explicit argument > the ``transport`` field of the Connect_Config ``server``
section > the ``TAKLER_TRANSPORT`` environment variable > gRPC -- and the
chosen implementation is built by :func:`build_client_transport`, which is
also where the lazy import of the HTTP stack lives: a gRPC-only install never
touches httpx, and selecting ``http`` without the extra is a clear error
naming ``takler[http]`` rather than an ``ImportError``.
"""

from __future__ import annotations

import os
from typing import Any, Callable, List, Mapping, Optional, Protocol, Tuple, Union

from takler.client.credentials import (
    ENV_JOB_PASSWORD,
    METADATA_KEY_JOB_PASSWORD,
    METADATA_KEY_SECRET,
    METADATA_KEY_USER,
    current_user_name,
    read_first_secret,
)
from takler.client.retry import CommandKind
from takler.exceptions import TaklerError
from takler.logging import get_logger
from takler.protocol.commands import Command, ProtocolModel
from takler.server.connect_config import ConnectConfig

__all__ = [
    "ClientTransport",
    "TRANSPORT_GRPC",
    "TRANSPORT_HTTP",
    "KNOWN_TRANSPORTS",
    "DEFAULT_TRANSPORT",
    "ENV_TRANSPORT",
    "resolve_transport",
    "build_credential_pairs",
    "build_client_transport",
]

logger = get_logger("client")


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


# Transport selection (M3 task 8) ------------------------------------------


#: The gRPC transport: always available, and the default.
TRANSPORT_GRPC: str = "grpc"

#: The HTTP transport, available with the ``takler[http]`` extra.
TRANSPORT_HTTP: str = "http"

#: Every name :func:`resolve_transport` accepts (compared case-insensitively).
KNOWN_TRANSPORTS: Tuple[str, str] = (TRANSPORT_GRPC, TRANSPORT_HTTP)

#: Applied when no source selects a transport: the default install shape.
DEFAULT_TRANSPORT: str = TRANSPORT_GRPC

#: Environment variable selecting the client transport, the third precedence
#: level of :func:`resolve_transport`.
ENV_TRANSPORT: str = "TAKLER_TRANSPORT"


def _is_blank(value: Optional[str]) -> bool:
    """Return ``True`` when a configured value counts as "not provided"."""
    return value is None or value.strip() == ""


def _normalize_transport(value: str, source: str) -> Optional[str]:
    """Return the canonical transport name in ``value``, or ``None``.

    Matching is case-insensitive and ignores surrounding whitespace. An
    unrecognized name is not an error: a single WARNING naming the offending
    value and its source is logged and ``None`` lets the next precedence
    source apply -- the same degrade-and-warn shape the server's
    ``from_str`` parsers use, so a typo can never strand a job script without
    a client.
    """
    normalized = value.strip().lower()
    if normalized in KNOWN_TRANSPORTS:
        return normalized
    logger.warning(
        f"invalid transport name {value!r} from {source}; "
        f"expected one of {list(KNOWN_TRANSPORTS)}; ignoring it."
    )
    return None


def resolve_transport(
    explicit: Optional[str] = None,
    connect_config: Optional[ConnectConfig] = None,
    env: Optional[Mapping[str, str]] = None,
) -> str:
    """Resolve which transport the client uses.

    Applies per-source precedence -- explicit argument > the ``transport``
    field of the Connect_Config ``server`` section > the
    ``TAKLER_TRANSPORT`` environment variable > :data:`DEFAULT_TRANSPORT`.
    This is the address resolution chain's own ordering (the Connect_Config
    outranks the environment there too), deliberately not the
    ``resolve_*`` family's env-over-config ordering: the transport belongs to
    the address the client dials. An absent value at any level (``None``,
    empty or whitespace-only) lets the next source take effect, and an
    unrecognized name degrades to the next source with a WARNING rather than
    raising.

    Args:
        explicit: An explicitly supplied transport name (constructor
            argument). ``None`` means "not provided".
        connect_config: A loaded Connect_Config whose ``server`` section is
            consulted, or ``None`` when no config file is in play.
        env: A mapping of environment variables (defaults to
            :data:`os.environ`). Only ``TAKLER_TRANSPORT`` is consulted.

    Returns:
        One of :data:`TRANSPORT_GRPC` / :data:`TRANSPORT_HTTP`.
    """
    if not _is_blank(explicit):
        resolved = _normalize_transport(explicit, "the explicit argument")
        if resolved is not None:
            return resolved

    if connect_config is not None and not _is_blank(connect_config.server.transport):
        resolved = _normalize_transport(
            connect_config.server.transport, "the Connect_Config server section"
        )
        if resolved is not None:
            return resolved

    if env is None:
        env = os.environ
    env_value = env.get(ENV_TRANSPORT)
    if not _is_blank(env_value):
        resolved = _normalize_transport(
            env_value, f"the {ENV_TRANSPORT} environment variable"
        )
        if resolved is not None:
            return resolved

    return DEFAULT_TRANSPORT


# Credential pairs ----------------------------------------------------------


def build_credential_pairs(
    kind: CommandKind, secret_file: Optional[str]
) -> List[Tuple[str, str]]:
    """Build the credentials one logical call of ``kind`` carries.

    A Child_Command carries ``takler-pass`` taken from ``TAKLER_PASS``
    (requirement 8.2); unset or whitespace-only means the key is left out
    (requirement 8.3). Every other kind is an Operator_Command, which carries
    ``takler-user`` (requirement 8.4) and, when a secret file is configured
    and holds a secret, ``takler-secret`` (requirement 8.5).

    ``CommandKind.CONTROL`` and ``CommandKind.QUERY`` are both Operator, so
    ``ping`` carries credentials it does not need. The server does not check
    them on a ``PUBLIC`` method, and the redundancy saves the client a second
    per-method classification table.

    An absent credential is left out and the call goes ahead, letting the
    server decide whether to refuse it (requirements 8.3, 8.7, 8.8). Failing
    here instead would stop a client from talking to a server running with
    ``Auth_Mode=disabled``, which is the default.

    This is the one builder both transports call -- the pairs are gRPC
    metadata on one transport and HTTP headers on the other, and sharing the
    builder is what keeps the two wire forms of the Cross-Language Contract
    from drifting (M3 task 8). Nothing on this path logs a value
    (requirements 8.9, 12.7): the WARNING for an unusable secret file is
    :func:`~takler.client.credentials.read_first_secret`'s, and it names the
    path and the reason only.

    Args:
        kind: Command classification of the logical call.
        secret_file: The resolved Operator_Secret_File path, or ``None``.

    Returns:
        The pairs to hand to every attempt of this call, possibly empty. The
        order matches the documented key order of the Cross-Language
        Contract.
    """
    if kind is CommandKind.CHILD:
        password = os.environ.get(ENV_JOB_PASSWORD)
        if password is not None and password.strip():
            return [(METADATA_KEY_JOB_PASSWORD, password)]
        return []

    pairs: List[Tuple[str, str]] = []
    # A Child_Command never carries an operator credential, and an
    # Operator_Command never carries the job password: the two credential
    # sets stay disjoint, so a job script's ``TAKLER_PASS`` cannot leak into
    # a control call.
    secret = read_first_secret(secret_file)
    if secret is not None:
        pairs.append((METADATA_KEY_SECRET, secret))

    user_name = current_user_name()
    if user_name is not None:
        pairs.append((METADATA_KEY_USER, user_name))
    return pairs


# Factory -------------------------------------------------------------------


def build_client_transport(
    name: str,
    *,
    host: str,
    port: Union[int, str],
    single_timeout: float,
    retry_window: Optional[float],
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    ca_file: Optional[str],
    server_name: Optional[str],
    secret_file: Optional[str],
) -> ClientTransport:
    """Build the client transport named ``name``.

    Both implementations are imported lazily, here and nowhere else: the gRPC
    one so this module stays the leaf every transport imports, and the HTTP
    one so a gRPC-only install never touches httpx. Selecting ``http``
    without the ``takler[http]`` extra raises a
    :class:`~takler.exceptions.TaklerError` naming the fix, not a bare
    ``ImportError``.

    Args:
        name: One of :data:`TRANSPORT_GRPC` / :data:`TRANSPORT_HTTP`,
            typically from :func:`resolve_transport`. Any other value is a
            programming error of the caller and raises ``ValueError``.
        host, port: Server address.
        single_timeout, retry_window, clock, sleep: The Call_Wrapper knobs,
            forwarded verbatim.
        ca_file, server_name, secret_file: The resolved TLS and credential
            inputs, forwarded verbatim.

    Returns:
        The transport, not yet opened.
    """
    kwargs = dict(
        host=host,
        port=port,
        single_timeout=single_timeout,
        retry_window=retry_window,
        clock=clock,
        sleep=sleep,
        ca_file=ca_file,
        server_name=server_name,
        secret_file=secret_file,
    )
    if name == TRANSPORT_GRPC:
        from takler.client.grpc_transport import GrpcTransport

        return GrpcTransport(**kwargs)
    if name == TRANSPORT_HTTP:
        try:
            from takler.client.http_transport import HttpTransport
        except ImportError as exc:
            raise TaklerError(
                "the HTTP client transport needs the takler[http] extra; "
                "install it with: pip install 'takler[http]'"
            ) from exc
        return HttpTransport(**kwargs)
    raise ValueError(f"unknown transport name {name!r}")
