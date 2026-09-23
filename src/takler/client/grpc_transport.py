"""The gRPC client transport: channel, credentials and the Call_Wrapper.

``GrpcTransport`` is the :class:`~takler.client.transport.ClientTransport`
that speaks gRPC. It owns everything the gRPC wire imposes on a call:

* the channel lifecycle (:meth:`GrpcTransport.open` /
  :meth:`GrpcTransport.close`), encrypted when a CA certificate is configured
  (:func:`build_channel_credentials`, m2 requirements 2.1, 2.2, 2.4, 2.6) --
  neither the caller nor the Call_Wrapper knows which of the two a call runs
  over, so the timeout, retry and Retry_Window semantics are unchanged by TLS
  (requirement 2.8);
* the encoding of the request payload into a pb2 message and the decoding of
  the pb2 response into the response DTO, both through
  :mod:`takler.server.protocol.adapter`, the one module that knows both
  ``takler_pb2`` and the command model;
* the Credential_Metadata, built in the one place both client transports
  share (:func:`takler.client.transport.build_credential_pairs`, reached
  through :meth:`GrpcTransport._build_metadata`) from the command's
  :class:`~takler.client.retry.CommandKind` (m2 requirement 8.1), so no
  command call site carries credential code. Where the credentials come from
  is :mod:`takler.client.credentials`'s business;
* the Call_Wrapper :meth:`GrpcTransport._call` (requirement 9.1): a
  per-attempt deadline so a wedged connection cannot block a job script
  forever (requirement 9.2), backoff retry on transport-level failures until
  the Retry_Window is exhausted (requirements 9.3 - 9.6), and the mapping of
  gRPC status codes to takler exceptions (requirement 9.8). Since M3 task 8
  the loop itself is the transport-neutral
  :func:`~takler.client.retry.run_with_retry`; what stays here is the gRPC
  invocation shape and the status-code classification
  (:func:`classify_grpc_error`).

Business failures are *not* retried: a response carrying a non-zero ``flag``
is handed back to the caller unchanged (requirement 9.7), and the CLI turns
its Error_Code into an exit code.

Requirements: 9.1, 9.2, 9.5, 9.6, 9.7, 9.8, 11.4, 11.5, 11.6,
2.1, 2.2, 2.4, 2.6, 2.8, 8.1, 8.2, 8.3, 8.4, 8.5, 8.7, 8.8, 8.9.
"""

from __future__ import annotations

import ssl
import time
from pathlib import Path
from typing import Any, Callable, List, Mapping, Optional, Tuple, TypeVar, Union

import grpc

from takler.protocol.commands import BATCH_COMMANDS
from takler.protocol.batch import validate_batch_response
from takler.client.retry import (
    COMMAND_KIND_BY_COMMAND,
    DEFAULT_SINGLE_TIMEOUT,
    NON_RETRYABLE_EXCEPTION_BY_STATUS,
    RETRYABLE_STATUS_CODES,
    CommandKind,
    FailureCategory,
    FailureVerdict,
    RetryPolicy,
    resolve_retry_window,
    run_with_retry,
)
from takler.client.transport import ClientTransport, build_credential_pairs
from takler.constant import DEFAULT_HOST, DEFAULT_PORT
from takler.exceptions import InvalidRequestError
from takler.logging import get_logger
from takler.protocol.commands import Command, ProtocolModel
from takler.server.protocol import adapter
from takler.server.protocol.takler_pb2_grpc import TaklerServerStub


logger = get_logger("client")

T = TypeVar("T")

#: gRPC channel option carrying the name the server certificate's host name is
#: verified against, used when that name differs from the host the client
#: connects to (m2 requirement 2.4). On HPC a server certificate is typically
#: issued for the login node's short name while job scripts connect through the
#: long one, so without this override TLS cannot be deployed there at all.
SSL_TARGET_NAME_OVERRIDE_OPTION: str = "grpc.ssl_target_name_override"


def _is_blank(value: Optional[str]) -> bool:
    """Return ``True`` when a configured path counts as "not provided"."""
    return value is None or value.strip() == ""


def _validate_ca_file(path: str) -> None:
    """Check that ``path`` really holds PEM certificate material.

    :func:`grpc.ssl_channel_credentials` takes the root certificates as opaque
    bytes and never looks at them, so a truncated or wrong-format CA file
    builds a credentials object happily and only fails later, at handshake
    time, as ``UNAVAILABLE``. The Call_Wrapper classifies that status as
    retryable, so the real symptom would be the command spending its whole
    Retry_Window before reporting "server unreachable" -- with the actual cause,
    the CA file, named nowhere. Requirement 2.6 asks for the path and the
    reason instead, which means parsing the file before it is used.

    The parser is :meth:`ssl.SSLContext.load_verify_locations` from the
    standard library, i.e. OpenSSL reading the same PEM material gRPC ends up
    reading: no extra dependency, and the same accept/reject boundary. This
    mirrors what :func:`takler.server.tls._validate_pair` does for the server
    side pair.

    Args:
        path: The configured CA certificate file path.

    Raises:
        InvalidRequestError: The content cannot be parsed as certificates. The
            message carries the path and the reason (requirement 2.6).
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    try:
        context.load_verify_locations(cafile=path)
    except (ssl.SSLError, OSError, ValueError) as exc:
        raise InvalidRequestError(
            f"cannot use TLS CA certificate file {path!r} as a root of trust: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


def build_channel_credentials(
    ca_file: Optional[str],
) -> Optional[grpc.ChannelCredentials]:
    """Build the client TLS credentials, or ``None`` when no CA is configured.

    Args:
        ca_file: The resolved CA certificate file path, typically from
            :func:`takler.client.credentials.resolve_ca_file`. ``None`` or
            blank means no CA certificate is configured.

    Returns:
        A :class:`grpc.ChannelCredentials` trusting ``ca_file`` as its root
        (requirement 2.1), or ``None`` when no CA certificate is configured, in
        which case the caller builds an unencrypted channel just as in M1
        (requirement 2.2).

    Raises:
        InvalidRequestError: The CA certificate file does not exist, is not
            readable, is empty, or cannot be parsed as a certificate. The
            message carries the path and the reason (requirement 2.6).

            The type is deliberate. ``SecurityConfigError`` is documented as a
            start-up-only failure of the *server*, so it is not the right one
            here; among the client visible types, ``InvalidRequestError`` is the
            one whose Error_Code (15) maps to exit code 1, "the request was
            wrong". That is the correct signal: a job script that names an
            unusable CA file has a wrong invocation, not an unreachable server,
            and must not be retried by the caller.
    """
    if _is_blank(ca_file):
        return None

    path = ca_file.strip()
    try:
        root_certificates = Path(path).read_bytes()
    except OSError as exc:
        raise InvalidRequestError(
            f"cannot read TLS CA certificate file {path!r}: {type(exc).__name__}: {exc}"
        ) from exc

    if not root_certificates.strip():
        raise InvalidRequestError(f"TLS CA certificate file {path!r} is empty")

    _validate_ca_file(path)

    logger.debug(f"built TLS channel credentials from CA certificate file {path!r}")
    return grpc.ssl_channel_credentials(root_certificates=root_certificates)


def _status_code(exc: grpc.RpcError) -> Optional[grpc.StatusCode]:
    """Return the gRPC status code of ``exc``, or ``None`` when unavailable.

    ``grpc.RpcError`` only guarantees ``code()`` on the ``Call`` flavour of the
    error, so a defensive lookup keeps a malformed error from masking the
    original failure with an ``AttributeError``.
    """
    code_getter = getattr(exc, "code", None)
    if code_getter is None:
        return None
    try:
        return code_getter()
    except Exception:  # pragma: no cover - defensive
        return None


def _status_name(code: Optional[grpc.StatusCode]) -> str:
    """Return a printable name for ``code``, tolerating ``None``."""
    return getattr(code, "name", None) or str(code)


def _status_details(exc: grpc.RpcError) -> str:
    """Return the server supplied details of ``exc``, or ``""``."""
    details_getter = getattr(exc, "details", None)
    if details_getter is None:
        return ""
    try:
        return details_getter() or ""
    except Exception:  # pragma: no cover - defensive
        return ""


def classify_grpc_error(exc: Exception) -> FailureVerdict:
    """Classify one failed gRPC attempt into a transport-neutral verdict.

    The mapping is the gRPC share of the Call_Wrapper's classification
    (requirements 9.3, 9.8): the status codes of
    :data:`~takler.client.retry.NON_RETRYABLE_EXCEPTION_BY_STATUS` answer
    :attr:`~takler.client.retry.FailureCategory.NON_RETRYABLE` with the
    exception the client raises, the codes of
    :data:`~takler.client.retry.RETRYABLE_STATUS_CODES` answer
    :attr:`~takler.client.retry.FailureCategory.RETRYABLE`, and any other
    status is :attr:`~takler.client.retry.FailureCategory.FATAL`.

    An exception that is not a :class:`grpc.RpcError` is not classified but
    re-raised: only a wire failure may enter the retry loop's decision, never
    a programming error of the caller.
    """
    if not isinstance(exc, grpc.RpcError):
        raise exc

    code = _status_code(exc)
    status_name = _status_name(code)
    details = _status_details(exc)
    failure_name = f"gRPC status {status_name}"
    log_field = f"status={status_name}"

    exception_type = NON_RETRYABLE_EXCEPTION_BY_STATUS.get(code)
    if exception_type is not None:
        return FailureVerdict(
            FailureCategory.NON_RETRYABLE,
            failure_name,
            log_field,
            details,
            exception_type,
        )

    if code not in RETRYABLE_STATUS_CODES:
        return FailureVerdict(FailureCategory.FATAL, failure_name, log_field, details)

    return FailureVerdict(FailureCategory.RETRYABLE, failure_name, log_field, details)


class GrpcTransport(ClientTransport):
    """The gRPC implementation of the client transport.

    Attributes:
        host: gRPC server host.
        port: gRPC server port.
        channel: The open ``grpc.Channel``, or ``None`` when closed.
        stub: The ``TaklerServerStub`` bound to :attr:`channel`.
        single_timeout: Per-attempt deadline in seconds, handed to every RPC
            (requirement 9.2).
        retry_window: Retry_Window in seconds. ``None`` means "resolve it from
            ``TAKLER_TIMEOUT`` and the command kind" (requirements 9.9 - 9.11).
        clock: Monotonic time source used to measure the Retry_Window.
        sleep: Blocking sleep used between retries. Together with ``clock``
            this is the injection point that lets tests span a long outage
            instantly.
        ca_file: Resolved CA certificate file the client trusts, or ``None``
            for an unencrypted channel (m2 requirement 2.2).
        server_name: Resolved host name the server certificate is verified
            against (m2 requirement 2.5), or ``None``.
        secret_file: Resolved Operator_Secret_File the client reads its
            ``takler-secret`` from (m2 requirement 8.6), or ``None``.

    The three resolved paths and names stay writable, which is what the tests
    use; they are read at :meth:`create_channel` / metadata-building time, so
    a reassignment takes effect on the next open channel or call.
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: Union[int, str] = DEFAULT_PORT,
        single_timeout: float = DEFAULT_SINGLE_TIMEOUT,
        retry_window: Optional[float] = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        ca_file: Optional[str] = None,
        server_name: Optional[str] = None,
        secret_file: Optional[str] = None,
    ):
        self.host: str = host
        self.port: str = str(port)
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[TaklerServerStub] = None
        self.single_timeout: float = single_timeout
        self.retry_window: Optional[float] = retry_window
        self.clock: Callable[[], float] = clock
        self.sleep: Callable[[float], None] = sleep
        self.ca_file: Optional[str] = ca_file
        self.server_name: Optional[str] = server_name
        self.secret_file: Optional[str] = secret_file

    @property
    def listen_address(self) -> str:
        """
        str: gRPC server's listen address
        """
        return f"{self.host}:{self.port}"

    # Connection lifecycle --------------------------------------------

    def create_channel(self):
        """Open the channel, encrypted when a CA certificate is configured.

        Raises
        ------
        InvalidRequestError
            The configured CA certificate file cannot be read or parsed
            (requirement 2.6). Raised here, before any RPC, so the failure names
            the file instead of showing up as an unreachable server after the
            Retry_Window.
        """
        credentials = build_channel_credentials(self.ca_file)
        if credentials is None:
            # Requirement 2.2: unchanged M1 behaviour when no CA is configured.
            self.channel = grpc.insecure_channel(self.listen_address)
            return

        options: List[Tuple[str, str]] = []
        if not _is_blank(self.server_name):
            options.append((SSL_TARGET_NAME_OVERRIDE_OPTION, self.server_name.strip()))
        # Requirement 2.1.
        self.channel = grpc.secure_channel(
            self.listen_address, credentials, options=options
        )

    def create_stub(self):
        self.stub = TaklerServerStub(self.channel)
        return self.stub

    def open(self) -> None:
        """Open the channel and bind the stub."""
        self.create_channel()
        self.create_stub()

    def close(self) -> None:
        """
        Close the channel and drop the stub, at most once.

        Calling it without an established channel returns silently
        (requirement 11.4), which is what makes the ``try/finally`` of the
        client's command wrappers safe even when the failure happened before
        the channel existed.
        """
        if self.channel is None:
            self.stub = None
            return
        self.channel.close()
        self.channel = None
        self.stub = None

    # The call ---------------------------------------------------------

    def call(
        self,
        command: Command,
        payload: Mapping[str, Any],
    ) -> ProtocolModel:
        """Run one command over the open channel and return its response DTO.

        The payload is encoded into the pb2 request
        (:func:`takler.server.protocol.adapter.request_to_pb2`), the stub
        method named by the codec's method table runs under the Call_Wrapper,
        and the pb2 response is decoded into the response DTO
        (:func:`~takler.server.protocol.adapter.response_from_pb2`).
        """
        kind = COMMAND_KIND_BY_COMMAND[command]
        request = adapter.request_to_pb2(command, payload)
        rpc = getattr(self.stub, adapter.GRPC_METHOD_BY_COMMAND[command])
        response = self._call(command.value, rpc, request, kind)
        decoded = adapter.response_from_pb2(command, response)
        return (
            validate_batch_response(command, payload, decoded)
            if command in BATCH_COMMANDS
            else decoded
        )

    # Credential metadata -----------------------------------------------

    def _build_metadata(self, kind: CommandKind) -> List[Tuple[str, str]]:
        """Build the Credential_Metadata one logical call of ``kind`` carries.

        The pairs come from the shared
        :func:`~takler.client.transport.build_credential_pairs` -- the same
        builder the HTTP transport turns into request headers (m2 requirement
        8.1, shared since M3 task 8). They are built once per logical call and
        handed to every attempt: once per logical call rather than once per
        attempt because a retry is the same call, and re-reading the secret
        file mid-retry would let a rotation land halfway through one command.

        Parameters
        ----------
        kind
            Command classification of the logical call.

        Returns
        -------
        The metadata pairs to hand to every attempt of this call, possibly
        empty. The order matches the documented key order of the
        Cross-Language Contract.
        """
        return build_credential_pairs(kind, self.secret_file)

    # Call wrapper -------------------------------------------------------

    def _retry_policy(self, kind: CommandKind) -> RetryPolicy:
        """Build the policy for one logical call of ``kind``."""
        retry_window = self.retry_window
        if retry_window is None:
            retry_window = resolve_retry_window(kind)
        return RetryPolicy(
            retry_window=retry_window,
            single_timeout=self.single_timeout,
            clock=self.clock,
            sleep=self.sleep,
        )

    def _call(
        self,
        operation_name: str,
        rpc: Callable[..., T],
        request: Any,
        kind: CommandKind,
    ) -> T:
        """Invoke ``rpc`` with timeout, retry and error mapping.

        The retry loop itself is the transport-neutral
        :func:`~takler.client.retry.run_with_retry`; what stays gRPC-specific
        here is the invocation shape (``timeout=`` + ``metadata=`` on every
        attempt) and the classification of :class:`grpc.RpcError` into a
        :class:`~takler.client.retry.FailureVerdict`
        (:func:`classify_grpc_error`).

        Parameters
        ----------
        operation_name
            The command name used in log messages, for example ``complete``.
        rpc
            The stub method to call; invoked as
            ``rpc(request, timeout=single_timeout, metadata=metadata)``.
        request
            The protobuf request message.
        kind
            Command classification selecting the default Retry_Window.

        Returns
        -------
        The response returned by the stub, including responses whose ``flag``
        is non zero: a business failure is not a transport failure, so it is
        never retried and never turned into an exception here (requirement 9.7).

        Raises
        ------
        InvalidRequestError, NodeNotFoundError, PermissionDeniedError
            For status codes where retrying cannot help (requirement 9.8).
        ClientConnectionError
            When the Retry_Window is exhausted (requirement 9.5).
        TransportError
            For any other gRPC status code.
        """
        metadata = self._build_metadata(kind)
        policy = self._retry_policy(kind)
        if operation_name in {command.value for command in BATCH_COMMANDS}:
            policy.retry_window = 0
        return run_with_retry(
            policy,
            operation_name,
            self.listen_address,
            lambda: rpc(request, timeout=policy.single_timeout, metadata=metadata),
            classify_grpc_error,
        )
