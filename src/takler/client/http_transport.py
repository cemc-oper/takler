"""The HTTP client transport of the ``takler[http]`` extra (M3 task 8).

``HttpTransport`` is the :class:`~takler.client.transport.ClientTransport`
that speaks HTTP: it posts the JSON
:class:`~takler.protocol.envelope.Envelope` to the server's
``POST /v1/commands/{command}`` endpoint and decodes the response envelope
into the command's response DTO. The module lives behind the ``http`` extra
-- httpx is not part of the default install -- so it is only ever imported
through :func:`takler.client.transport.build_client_transport`, which turns
a missing extra into a clear error naming ``takler[http]``.

Everything a call shares with the gRPC transport comes from the same one
place, so the two transports cannot drift:

* the credentials travel as the HTTP headers ``takler-pass`` /
  ``takler-secret`` / ``takler-user`` -- the header names are the gRPC
  metadata keys -- built by the shared
  :func:`~takler.client.transport.build_credential_pairs` (m2 requirement
  8.1);
* the retry loop is the transport-neutral
  :func:`~takler.client.retry.run_with_retry`: same per-attempt deadline
  (requirement 9.2), same backoff and Retry_Window (requirements 9.3 - 9.6),
  same exception types, and with them the same CLI exit codes. What is local
  to this module is only the classification of HTTP failures into a
  :class:`~takler.client.retry.FailureVerdict`
  (:func:`classify_http_error`), mirroring the gRPC status-code mapping
  (requirements 9.3, 9.8):
    - the server never uses an HTTP status to report a business outcome, so
      a non-``200`` status is always a transport-level event: ``401`` /
      ``403`` answer ``PermissionDeniedError`` (the HTTP form of
      ``UNAUTHENTICATED`` / ``PERMISSION_DENIED``), ``400`` / ``422`` answer
      ``InvalidRequestError`` (the request cannot be routed or the envelope
      is malformed -- retrying cannot fix either), the transient statuses of
      :data:`RETRYABLE_HTTP_STATUSES` are retried, and any other status is a
      ``TransportError`` without retry;
    - an httpx transport error (refused connection, timeout, reset stream)
      is the HTTP form of ``UNAVAILABLE`` and is retried;
* a business failure is not retried and not raised: a response envelope
  carrying a non-zero ``flag`` is handed back unchanged (requirement 9.7),
  and the CLI turns its Error_Code into an exit code.

What deliberately does not cross over is client-side validation: the payload
is posted verbatim (``meter_value: "abc"`` included), because the server's
end of the wire owns validation and both clients must be answered with the
same ``flag`` for the same malformed request. The one encoding concern this
module owns is the JSON form of ``bytes`` payload fields -- base64, matching
the DTOs' JSON-mode serialization, exactly as ``LoadCommand.flow_bytes``
travels.

TLS mirrors the gRPC channel rules: a configured CA certificate file
switches the base URL to ``https`` and is validated at :meth:`open` with the
same path-and-reason messages (requirement 2.6); without one the transport
is plaintext (requirement 2.2) -- the recommended deployment shape behind a
TLS-terminating reverse proxy. The ``server_name`` override has no httpx
equivalent (the certificate is always verified against the URL host), so a
configured override draws one WARNING at :meth:`open` rather than being
silently ignored.
"""

from __future__ import annotations

import base64
import ssl
import time
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Union

import httpx

from takler.exceptions import ServerResponseError
from takler.protocol.commands import BATCH_COMMANDS
from takler.protocol.batch import validate_batch_response
from takler.client.retry import (
    COMMAND_KIND_BY_COMMAND,
    DEFAULT_SINGLE_TIMEOUT,
    CommandKind,
    FailureCategory,
    FailureVerdict,
    RetryPolicy,
    resolve_retry_window,
    run_with_retry,
)
from takler.client.transport import ClientTransport, build_credential_pairs
from takler.constant import DEFAULT_HOST
from takler.exceptions import (
    InvalidRequestError,
    PermissionDeniedError,
    TransportError,
)
from takler.logging import get_logger
from takler.protocol.commands import Command, ProtocolModel
from takler.protocol.envelope import Envelope

__all__ = [
    "COMMAND_URL_PREFIX",
    "RETRYABLE_HTTP_STATUSES",
    "NON_RETRYABLE_EXCEPTION_BY_HTTP_STATUS",
    "HttpTransport",
    "classify_http_error",
]

logger = get_logger("client")


#: URL prefix of the command endpoint, mirroring ``API_PREFIX`` +
#: ``/commands/`` on the server side (``takler.server.http_transport``). The
#: full URL of a command is this prefix plus the command's CLI word.
COMMAND_URL_PREFIX: str = "/v1/commands/"

#: HTTP status codes that mean "transport level failure, worth retrying"
#: (requirement 9.3): the request timed out or was throttled, or a server or
#: proxy in between reported a transient condition. Business outcomes never
#: arrive as a status code -- they are a ``200`` with the Error_Code in the
#: envelope's ``flag`` -- so every status in this table is genuinely about
#: the path, not about the command.
RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})

#: HTTP status codes that mean "the request itself is wrong, retrying cannot
#: help", mapped to the exception the client raises (requirement 9.8). The
#: mapping mirrors the gRPC one: ``401`` / ``403`` are the HTTP form of
#: ``UNAUTHENTICATED`` / ``PERMISSION_DENIED`` (both map to
#: ``PermissionDeniedError`` there), and ``400`` / ``422`` are the server's
#: "the envelope is malformed or does not match the URL" answers, the HTTP
#: form of ``INVALID_ARGUMENT``.
NON_RETRYABLE_EXCEPTION_BY_HTTP_STATUS = {
    400: InvalidRequestError,
    401: PermissionDeniedError,
    403: PermissionDeniedError,
    422: InvalidRequestError,
}

#: httpx transport errors that mean "the server could not be reached or the
#: connection broke", the HTTP form of ``UNAVAILABLE`` /
#: ``DEADLINE_EXCEEDED`` -- worth spending the Retry_Window on (requirement
#: 9.3). Deliberately not the whole ``httpx.TransportError`` family:
#: ``LocalProtocolError`` and ``UnsupportedProtocol`` are client-side or
#: configuration mistakes a retry cannot fix, so they stay FATAL.
_RETRYABLE_HTTPX_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.ReadError,
    httpx.WriteError,
    httpx.CloseError,
    httpx.RemoteProtocolError,
)

#: How much of an unreadable error body goes into the exception message.
#: Enough to identify what answered (a proxy's error page, say), short
#: enough for a single terminal line.
_ERROR_BODY_SNIPPET_LENGTH: int = 200


class _HttpStatusFailure(Exception):
    """One attempt answered with a non-200 HTTP status.

    An internal carrier between the attempt callable and
    :func:`classify_http_error`: the retry loop only sees exceptions, so a
    status answer is raised as one, carrying the status code and the detail
    text the server (or a proxy) sent along.
    """

    def __init__(self, status_code: int, details: str) -> None:
        super().__init__(f"HTTP status {status_code}: {details}")
        self.status_code: int = status_code
        self.details: str = details


def _response_details(response: httpx.Response) -> str:
    """Extract the detail text of a non-200 response.

    The takler server answers a refusal as FastAPI's
    ``{"detail": "<text>"}``; anything else answering (a reverse proxy's
    error page, a truncated body) is quoted as a snippet. No content ever
    lands here that is not already on its way into an exception message, and
    the server's refusal texts are sanitized by construction -- they name
    the reason, never a credential value.
    """
    try:
        body = response.json()
    except ValueError:
        body = None
    if isinstance(body, dict) and isinstance(body.get("detail"), str):
        return body["detail"]
    return response.text[:_ERROR_BODY_SNIPPET_LENGTH]


def classify_http_error(exc: Exception) -> FailureVerdict:
    """Classify one failed HTTP attempt into a transport-neutral verdict.

    The HTTP share of the Call_Wrapper's classification (requirements 9.3,
    9.8), mirroring the gRPC status-code mapping:

    * a status of :data:`NON_RETRYABLE_EXCEPTION_BY_HTTP_STATUS` answers
      :attr:`~takler.client.retry.FailureCategory.NON_RETRYABLE` with the
      exception the client raises;
    * a status of :data:`RETRYABLE_HTTP_STATUSES` and the connection errors
      of ``_RETRYABLE_HTTPX_ERRORS`` answer
      :attr:`~takler.client.retry.FailureCategory.RETRYABLE`;
    * any other status or httpx transport error is
      :attr:`~takler.client.retry.FailureCategory.FATAL`.

    An exception that is neither a status answer nor an httpx transport
    error is not classified but re-raised: only a wire failure may enter the
    retry loop's decision, never a programming error of the caller (a
    malformed response envelope raises its own ``ValueError`` /
    ``ValidationError`` outside this classification, exactly as a malformed
    pb2 response does on the gRPC side).
    """
    if isinstance(exc, _HttpStatusFailure):
        failure_name = f"HTTP status {exc.status_code}"
        log_field = f"status={exc.status_code}"
        exception_type = NON_RETRYABLE_EXCEPTION_BY_HTTP_STATUS.get(exc.status_code)
        if exception_type is not None:
            return FailureVerdict(
                FailureCategory.NON_RETRYABLE,
                failure_name,
                log_field,
                exc.details,
                exception_type,
            )
        if exc.status_code in RETRYABLE_HTTP_STATUSES:
            return FailureVerdict(
                FailureCategory.RETRYABLE, failure_name, log_field, exc.details
            )
        return FailureVerdict(
            FailureCategory.FATAL, failure_name, log_field, exc.details
        )

    if isinstance(exc, _RETRYABLE_HTTPX_ERRORS):
        error_name = type(exc).__name__
        return FailureVerdict(
            FailureCategory.RETRYABLE,
            f"connection error ({error_name})",
            f"error={error_name}",
            str(exc),
        )

    if isinstance(exc, httpx.TransportError):
        error_name = type(exc).__name__
        return FailureVerdict(
            FailureCategory.FATAL,
            f"connection error ({error_name})",
            f"error={error_name}",
            str(exc),
        )

    raise exc


def _json_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Return ``payload`` in its JSON wire shape.

    The payload is passed through verbatim -- validating it is the server's
    end of the wire (see the module docstring). The one encoding this
    transport owns is the JSON form of ``bytes`` values: base64, matching
    the request DTOs' JSON-mode serialization
    (``LoadCommand.flow_bytes``).
    """
    encoded: Dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, (bytes, bytearray)):
            encoded[key] = base64.b64encode(bytes(value)).decode("ascii")
        else:
            encoded[key] = value
    return encoded


def _build_ssl_context(ca_file: str) -> ssl.SSLContext:
    """Build the TLS client context trusting ``ca_file`` as its root.

    The three failure shapes and their messages mirror the gRPC side's
    :func:`takler.client.grpc_transport.build_channel_credentials`
    (requirement 2.6): an unreadable file, an empty file and unparseable
    content are all reported at :meth:`HttpTransport.open` -- naming the
    path and the reason -- instead of surfacing after the Retry_Window as an
    unreachable server.

    Args:
        ca_file: The resolved CA certificate file path.

    Raises:
        InvalidRequestError: The file cannot be read, is empty, or cannot be
            parsed as certificate material.
    """
    path = ca_file.strip()
    try:
        content = Path(path).read_bytes()
    except OSError as exc:
        raise InvalidRequestError(
            f"cannot read TLS CA certificate file {path!r}: {type(exc).__name__}: {exc}"
        ) from exc

    if not content.strip():
        raise InvalidRequestError(f"TLS CA certificate file {path!r} is empty")

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    try:
        context.load_verify_locations(cafile=path)
    except (ssl.SSLError, OSError, ValueError) as exc:
        raise InvalidRequestError(
            f"cannot use TLS CA certificate file {path!r} as a root of trust: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    return context


class HttpTransport(ClientTransport):
    """The HTTP implementation of the client transport (``takler[http]``).

    Attributes:
        host: Server host.
        port: Server port of the HTTP listener.
        client: The open ``httpx.Client``, or ``None`` when closed.
        single_timeout: Per-attempt deadline in seconds, handed to every
            request (requirement 9.2). httpx splits a deadline into connect /
            read / write / pool timeouts; one value is applied to all four,
            which plays the gRPC per-attempt deadline's role here.
        retry_window: Retry_Window in seconds. ``None`` means "resolve it
            from ``TAKLER_TIMEOUT`` and the command kind" (requirements
            9.9 - 9.11).
        clock: Monotonic time source used to measure the Retry_Window.
        sleep: Blocking sleep used between retries. Together with ``clock``
            this is the injection point that lets tests span a long outage
            instantly.
        ca_file: Resolved CA certificate file the client trusts, or ``None``
            for plaintext HTTP (requirement 2.2). Set, the base URL switches
            to ``https`` (requirement 2.1).
        server_name: Resolved host name override (requirement 2.5). httpx
            always verifies the certificate against the URL host, so a
            configured override cannot be honored; :meth:`open` logs one
            WARNING instead of silently ignoring a security knob.
        secret_file: Resolved Operator_Secret_File the client reads its
            ``takler-secret`` from (requirement 8.6), or ``None``.

    The address and the three resolved paths stay writable, which is what
    the tests use; they are read at :meth:`open` / header-building time, so
    a reassignment takes effect on the next opened client or call.
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: Union[int, str] = 8080,
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
        self.client: Optional[httpx.Client] = None
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
        str: HTTP server's listen address
        """
        return f"{self.host}:{self.port}"

    # Connection lifecycle --------------------------------------------

    def open(self) -> None:
        """Open the httpx client, encrypted when a CA certificate is configured.

        The base URL is fixed here from the current ``host`` / ``port`` /
        ``ca_file``: ``https`` when a CA is configured (requirement 2.1),
        plaintext ``http`` otherwise (requirement 2.2).

        Raises:
            InvalidRequestError: The configured CA certificate file cannot be
                read or parsed (requirement 2.6). Raised here, before any
                request, so the failure names the file instead of showing up
                as an unreachable server after the Retry_Window.
        """
        if self.client is not None:
            return

        verify: Union[bool, ssl.SSLContext] = True
        scheme = "http"
        if self.ca_file is not None and self.ca_file.strip():
            verify = _build_ssl_context(self.ca_file)
            scheme = "https"

        if self.server_name is not None and self.server_name.strip():
            logger.warning(
                f"the HTTP transport cannot honor the certificate host name "
                f"override {self.server_name.strip()!r}: httpx verifies the "
                f"server certificate against the URL host {self.host!r}. "
                f"Use the gRPC transport when the certificate name differs "
                f"from the dialed host."
            )

        logger.debug(f"opening HTTP client for {scheme}://{self.listen_address}")
        self.client = httpx.Client(
            base_url=f"{scheme}://{self.listen_address}", verify=verify
        )

    def create_channel(self) -> None:
        """gRPC-named compatibility shim for :meth:`open`.

        ``TaklerServiceClient.create_channel`` predates the transport
        abstraction and calls this on whichever transport sits behind it;
        HTTP has no channel, so opening the client is the whole answer.
        """
        self.open()

    def create_stub(self) -> None:
        """gRPC-named compatibility shim: HTTP has no stub, returns ``None``."""
        return None

    def close(self) -> None:
        """
        Close the client, at most once.

        Calling it without an open client returns silently (requirement
        11.4), which is what makes the ``try/finally`` of the client's
        command wrappers safe even when the failure happened before the
        client existed.
        """
        if self.client is None:
            return
        self.client.close()
        self.client = None

    # The call ---------------------------------------------------------

    def call(
        self,
        command: Command,
        payload: Mapping[str, Any],
    ) -> ProtocolModel:
        """Run one command over HTTP and return its response DTO.

        The payload is wrapped in a request envelope unvalidated (see the
        module docstring), posted to ``/v1/commands/{command}`` under the
        Call_Wrapper, and the response envelope is decoded into the
        command's response DTO -- including responses whose ``flag`` is
        non-zero, which are handed back unchanged (requirement 9.7).
        """
        if self.client is None:
            raise TransportError(
                f"cannot run {command.value} on server {self.listen_address}: "
                f"the HTTP transport is not open; call open() first"
            )
        kind = COMMAND_KIND_BY_COMMAND[command]
        envelope = Envelope(command=command, payload=_json_payload(payload))
        response = self._call(command.value, envelope, kind)
        if command in BATCH_COMMANDS and (
            response.command != command
            or response.trace_id != envelope.trace_id
            or response.version != envelope.version
        ):
            raise ServerResponseError("batch response does not match request envelope")
        decoded = response.parse_response()
        return (
            validate_batch_response(command, payload, decoded)
            if command in BATCH_COMMANDS
            else decoded
        )

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
        envelope: Envelope,
        kind: CommandKind,
    ) -> Envelope:
        """Post ``envelope`` with timeout, retry and error mapping.

        The retry loop is the transport-neutral
        :func:`~takler.client.retry.run_with_retry`; what stays HTTP-specific
        here is the invocation shape (one ``POST`` with the credential
        headers and the per-attempt deadline) and the classification of the
        failures (:func:`classify_http_error`).

        The headers are built once per logical call and handed to every
        attempt, exactly as the gRPC metadata is (m2 requirement 8.1): a
        retry is the same call, and re-reading the secret file mid-retry
        would let a rotation land halfway through one command.

        Parameters
        ----------
        operation_name
            The command name, used in the URL and in log and exception
            messages, for example ``complete``.
        envelope
            The request envelope; its ``command`` and ``payload`` form the
            posted JSON body.
        kind
            Command classification selecting the credentials and the default
            Retry_Window.

        Returns
        -------
        The response envelope, including envelopes whose ``flag`` is non
        zero: a business failure is not a transport failure, so it is never
        retried and never turned into an exception here (requirement 9.7).

        Raises
        ------
        InvalidRequestError, PermissionDeniedError
            For status codes where retrying cannot help (requirement 9.8).
        ClientConnectionError
            When the Retry_Window is exhausted (requirement 9.5).
        TransportError
            For any other HTTP or connection failure.
        """
        headers = dict(build_credential_pairs(kind, self.secret_file))
        policy = self._retry_policy(kind)
        if operation_name in {command.value for command in BATCH_COMMANDS}:
            policy.retry_window = 0
        body = envelope.model_dump(mode="json")
        url = f"{COMMAND_URL_PREFIX}{operation_name}"

        def attempt() -> Envelope:
            response = self.client.post(
                url, json=body, headers=headers, timeout=policy.single_timeout
            )
            if response.status_code != 200:
                raise _HttpStatusFailure(
                    response.status_code, _response_details(response)
                )
            # A non-envelope answer (a proxy's error page with a 200, a
            # truncated body) raises its ValueError / ValidationError here,
            # outside the retry classification -- exactly as a malformed pb2
            # response raises on the gRPC side.
            return Envelope.model_validate(response.json())

        return run_with_retry(
            policy,
            operation_name,
            self.listen_address,
            attempt,
            classify_http_error,
        )
