"""Unit tests for the client-side ``HttpTransport`` (``takler[http]``, M3 task 8).

The wire is faked with ``httpx.MockTransport``, so the whole HTTP stack of the
transport -- envelope encoding, header building, status classification, the
shared retry loop -- runs without a server. What is pinned here:

* the envelope contract: ``POST /v1/commands/{command}`` with the command's
  CLI word, the payload passed through unvalidated (``meter_value: "abc"``
  crosses the wire as a string), ``bytes`` fields base64 encoded;
* the credential headers: the same three keys the gRPC metadata carries,
  built by the same shared builder;
* the failure classification: the HTTP status tables are literal, the mapped
  exceptions and messages mirror the gRPC ones, and the retry behaviour
  (backoff, Retry_Window, WARNING shape) is the shared loop's;
* the TLS knobs of :meth:`HttpTransport.open`: plaintext by default,
  ``https`` with a CA file, path-and-reason errors for unusable CA files.

Both imports live behind the ``http`` extra, so a gRPC-only checkout skips
this module instead of failing at collection.
"""

from __future__ import annotations

# ruff: noqa: E402 -- the takler imports below intentionally follow the
# importorskip guard, so a checkout without the ``http`` extra skips this
# module instead of failing at collection.

import contextlib
import io
import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

httpx = pytest.importorskip(
    "httpx", reason="the HTTP client transport lives behind the takler[http] extra"
)

import takler.logging
from takler.client.http_transport import (
    NON_RETRYABLE_EXCEPTION_BY_HTTP_STATUS,
    RETRYABLE_HTTP_STATUSES,
    HttpTransport,
)
from takler.exceptions import (
    ClientConnectionError,
    InvalidRequestError,
    PermissionDeniedError,
    TransportError,
)
from takler.protocol.commands import (
    RESPONSE_TYPE_BY_COMMAND,
    CoroutineResponse,
    PingResponse,
    ServiceResponse,
    BatchResponse,
    BATCH_COMMANDS,
    ShowResponse,
    Command,
)
from takler.protocol.envelope import Envelope


#: Command -> canonical payload, mirroring the table in
#: ``test_grpc_transport_unit.py``: the same logical call must reach the
#: server whichever transport carries it.
PAYLOAD_BY_COMMAND: Dict[Command, Dict[str, Any]] = {
    Command.INIT: {"node_path": "/flow1/task1", "task_id": "12345"},
    Command.COMPLETE: {"node_path": "/flow1/task1"},
    Command.ABORT: {"node_path": "/flow1/task1", "reason": "boom"},
    Command.EVENT: {"node_path": "/flow1/task1", "event_name": "event1"},
    Command.METER: {
        "node_path": "/flow1/task1",
        "meter_name": "meter1",
        "meter_value": "50",
    },
    Command.REQUEUE: {"node_paths": ["/flow1"]},
    Command.SUSPEND: {"node_paths": ["/flow1"]},
    Command.RESUME: {"node_paths": ["/flow1"]},
    Command.RUN: {"node_paths": ["/flow1/task1"], "force": True},
    Command.FORCE: {
        "paths": ["/flow1/task1"],
        "state": "queued",
        "recursive": True,
    },
    Command.FREE_DEP: {"paths": ["/flow1/task1"], "dep_type": "all"},
    Command.LOAD: {"flow_type": "json", "flow_bytes": b"{}"},
    Command.BEGIN: {"flow_name": "flow1", "force": False},
    Command.SHOW: {
        "show_trigger": True,
        "show_parameter": True,
        "show_limit": True,
        "show_event": True,
        "show_meter": True,
    },
    Command.PING: {},
    Command.COROUTINE: {},
}


def _response_dto(command: Command):
    """A valid response DTO instance of ``command``."""
    if command is Command.SHOW:
        return ShowResponse(output="{}")
    if command is Command.PING:
        return PingResponse()
    if command is Command.COROUTINE:
        return CoroutineResponse(coroutines=[])
    if command in BATCH_COMMANDS:
        payload = PAYLOAD_BY_COMMAND[command]
        targets = (
            ["/" + payload["flow_name"]]
            if command == Command.BEGIN
            else payload.get("node_paths", payload.get("paths"))
        )
        return BatchResponse(
            flag=0,
            message="",
            results=[
                dict(index=i, target=t, flag=0, message="", effect="applied")
                for i, t in enumerate(targets)
            ],
        )
    return ServiceResponse(flag=0, message="")


def _response_body(command: Command, trace_id: str = "0" * 32, **response_kwargs):
    """The JSON body of a response envelope for ``command``."""
    envelope = Envelope.for_response(command, _response_dto(command), trace_id=trace_id)
    if response_kwargs:
        envelope.payload.update(response_kwargs)
    return envelope.model_dump(mode="json")


class Wire:
    """A ``httpx.MockTransport`` handler factory recording every request.

    ``answers`` is a list of responses (``httpx.Response``) or exceptions to
    raise, replayed in order; the last one repeats when the list runs out.
    """

    def __init__(self, *answers):
        self.answers: List[Any] = list(answers) or [httpx.Response(503)]
        self.requests: List[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        index = min(len(self.requests), len(self.answers)) - 1
        answer = self.answers[index]
        if isinstance(answer, BaseException):
            raise answer
        if answer.status_code == 200:
            body = answer.json()
            body["trace_id"] = json.loads(request.content)["trace_id"]
            return httpx.Response(200, json=body)
        return answer

    def bind(self, transport: HttpTransport) -> None:
        """Stand in for ``open()``: bind a mock-backed client."""
        transport.client = httpx.Client(
            transport=httpx.MockTransport(self.handler),
            base_url=f"http://{transport.listen_address}",
        )

    def last_json(self) -> dict:
        return json.loads(self.requests[-1].content.decode("utf-8"))


def make_transport(
    fake_clock=None, retry_window: float = 0.0, **kwargs
) -> HttpTransport:
    """A transport with no retrying, so one logical call is one attempt."""
    kwargs.setdefault("host", "localhost")
    kwargs.setdefault("port", 33084)
    kwargs.setdefault("retry_window", retry_window)
    if fake_clock is not None:
        kwargs.setdefault("clock", fake_clock)
        kwargs.setdefault("sleep", fake_clock.sleep)
    return HttpTransport(**kwargs)


@pytest.fixture
def captured_console_log():
    """Capture takler's console log output (the logging backend does not
    route records into pytest's ``caplog``). Mirrors the server-side helper
    in ``tests/server/test_http_transport_unit.py``."""
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="WARNING", console=True)
            yield buffer
    finally:
        takler.logging.configure(console=True)


def warning_lines(captured: io.StringIO) -> List[str]:
    return [line for line in captured.getvalue().splitlines() if "WARNING" in line]


# the envelope contract -----------------------------------------------------


@pytest.mark.parametrize("command", sorted(PAYLOAD_BY_COMMAND, key=str))
def test_call_posts_the_request_envelope_and_decodes_the_response(
    command: Command,
) -> None:
    wire = Wire(httpx.Response(200, json=_response_body(command)))
    transport = make_transport()
    wire.bind(transport)

    response = transport.call(command, PAYLOAD_BY_COMMAND[command])

    assert len(wire.requests) == 1
    request = wire.requests[0]
    assert request.method == "POST"
    assert request.url.path == f"/v1/commands/{command.value}"

    body = wire.last_json()
    assert body["command"] == command.value
    assert body["version"] == "1"
    assert len(body["trace_id"]) == 32

    payload = dict(PAYLOAD_BY_COMMAND[command])
    if "flow_bytes" in payload:
        # The JSON envelope carries raw bytes base64 encoded.
        import base64

        payload["flow_bytes"] = base64.b64encode(payload["flow_bytes"]).decode("ascii")
    assert body["payload"] == payload

    assert isinstance(response, RESPONSE_TYPE_BY_COMMAND[command])
    if isinstance(response, ServiceResponse):
        assert response.flag == 0


def test_call_passes_the_payload_through_unvalidated() -> None:
    """A non-numeric ``meter_value`` crosses the wire as the string the
    caller passed: validation is the server's end of the wire, and both
    clients must be answered with the same flag for the same bad value."""
    wire = Wire(httpx.Response(200, json=_response_body(Command.METER, flag=99)))
    transport = make_transport()
    wire.bind(transport)

    response = transport.call(
        Command.METER,
        {
            "node_path": "/flow1/task1",
            "meter_name": "meter1",
            "meter_value": "abc",
        },
    )

    assert wire.last_json()["payload"]["meter_value"] == "abc"
    assert response.flag == 99


def test_business_failure_is_returned_not_raised(fake_clock) -> None:
    """A non-zero flag arrives as a 200 and is handed back unchanged, on the
    first attempt, with nothing slept (requirement 9.7)."""
    wire = Wire(
        httpx.Response(
            200, json=_response_body(Command.COMPLETE, flag=10, message="no such node")
        )
    )
    transport = make_transport(fake_clock, retry_window=60.0)
    wire.bind(transport)

    response = transport.call(Command.COMPLETE, {"node_path": "/flow1/no_such"})

    assert response.flag == 10
    assert response.message == "no such node"
    assert len(wire.requests) == 1
    assert fake_clock.slept == []


def test_malformed_success_body_raises_outside_the_retry_loop(fake_clock) -> None:
    """A 200 whose body is not a response envelope is a broken answer, not a
    transport failure: it raises straight away, without retrying."""
    wire = Wire(httpx.Response(200, content=b"<html>not an envelope</html>"))
    transport = make_transport(fake_clock, retry_window=60.0)
    wire.bind(transport)

    with pytest.raises(ValueError):
        transport.call(Command.PING, {})

    assert len(wire.requests) == 1
    assert fake_clock.slept == []


# credential headers ----------------------------------------------------------


def test_child_command_carries_the_job_password_header(monkeypatch) -> None:
    monkeypatch.setenv("TAKLER_PASS", "job-password")
    wire = Wire(httpx.Response(200, json=_response_body(Command.INIT)))
    transport = make_transport()
    wire.bind(transport)

    transport.call(Command.INIT, PAYLOAD_BY_COMMAND[Command.INIT])

    headers = wire.requests[0].headers
    assert headers["takler-pass"] == "job-password"
    assert "takler-secret" not in headers
    assert "takler-user" not in headers


def test_child_command_without_a_password_carries_no_credential_headers(
    monkeypatch,
) -> None:
    monkeypatch.delenv("TAKLER_PASS", raising=False)
    wire = Wire(httpx.Response(200, json=_response_body(Command.COMPLETE)))
    transport = make_transport()
    wire.bind(transport)

    transport.call(Command.COMPLETE, PAYLOAD_BY_COMMAND[Command.COMPLETE])

    headers = wire.requests[0].headers
    assert "takler-pass" not in headers
    assert "takler-secret" not in headers
    assert "takler-user" not in headers


def test_operator_command_carries_secret_and_user_headers(
    monkeypatch, tmp_path: Path
) -> None:
    secret_file = tmp_path / "operator.secret"
    secret_file.write_text("shared-secret\n")
    monkeypatch.setenv("LOGNAME", "alice")
    monkeypatch.delenv("TAKLER_PASS", raising=False)
    wire = Wire(httpx.Response(200, json=_response_body(Command.REQUEUE)))
    transport = make_transport(secret_file=str(secret_file))
    wire.bind(transport)

    transport.call(Command.REQUEUE, PAYLOAD_BY_COMMAND[Command.REQUEUE])

    headers = wire.requests[0].headers
    assert headers["takler-secret"] == "shared-secret"
    assert headers["takler-user"] == "alice"
    # The job password never leaks into an operator call.
    assert "takler-pass" not in headers


# the status classification ---------------------------------------------------


def test_retryable_http_statuses_are_exactly_the_transient_set() -> None:
    """Literal on purpose: the transient set is a contract, not a derivation."""
    assert RETRYABLE_HTTP_STATUSES == frozenset({408, 429, 500, 502, 503, 504})


def test_non_retryable_status_table_is_exact() -> None:
    """Literal on purpose, mirroring the gRPC table's pin in
    ``tests/client/test_retry_unit.py``."""
    assert NON_RETRYABLE_EXCEPTION_BY_HTTP_STATUS == {
        400: InvalidRequestError,
        401: PermissionDeniedError,
        403: PermissionDeniedError,
        422: InvalidRequestError,
    }
    assert not (RETRYABLE_HTTP_STATUSES & set(NON_RETRYABLE_EXCEPTION_BY_HTTP_STATUS))


@pytest.mark.parametrize("status", [401, 403])
def test_auth_refusal_raises_permission_denied_with_the_server_text(
    status: int,
) -> None:
    detail = "RunCommandRequeue refused: missing credential"
    wire = Wire(httpx.Response(status, json={"detail": detail}))
    transport = make_transport()
    wire.bind(transport)

    with pytest.raises(PermissionDeniedError) as exc_info:
        transport.call(Command.REQUEUE, PAYLOAD_BY_COMMAND[Command.REQUEUE])

    text = str(exc_info.value)
    assert "requeue on server localhost:33084" in text
    assert f"HTTP status {status}" in text
    assert detail in text
    assert len(wire.requests) == 1


@pytest.mark.parametrize("status", [400, 422])
def test_malformed_request_answer_raises_invalid_request(status: int) -> None:
    wire = Wire(httpx.Response(status, json={"detail": "bad envelope"}))
    transport = make_transport()
    wire.bind(transport)

    with pytest.raises(InvalidRequestError) as exc_info:
        transport.call(Command.PING, {})

    assert f"HTTP status {status}" in str(exc_info.value)
    assert len(wire.requests) == 1


@pytest.mark.parametrize("status", [404, 501])
def test_an_unmapped_status_is_a_fatal_transport_error(status: int) -> None:
    wire = Wire(httpx.Response(status, text="odd answer"))
    transport = make_transport()
    wire.bind(transport)

    with pytest.raises(TransportError) as exc_info:
        transport.call(Command.PING, {})

    assert f"HTTP status {status}" in str(exc_info.value)
    assert len(wire.requests) == 1


# retry behaviour (the shared loop) -------------------------------------------


def test_retryable_status_is_retried_until_success(
    fake_clock, captured_console_log
) -> None:
    wire = Wire(
        httpx.Response(503, text="busy"),
        httpx.Response(500, text="boom"),
        httpx.Response(200, json=_response_body(Command.COMPLETE)),
    )
    transport = make_transport(fake_clock, retry_window=60.0)
    wire.bind(transport)

    response = transport.call(Command.COMPLETE, PAYLOAD_BY_COMMAND[Command.COMPLETE])

    assert response.flag == 0
    assert len(wire.requests) == 3
    assert fake_clock.slept == [1.0, 2.0]
    warnings = warning_lines(captured_console_log)
    assert len(warnings) == 2
    for line in warnings:
        assert "localhost:33084" in line
        assert "complete" in line
        assert "elapsed=" in line
    assert "status=503" in warnings[0]
    assert "status=500" in warnings[1]


def test_connection_error_is_retried_until_success(fake_clock) -> None:
    wire = Wire(
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("slow"),
        httpx.Response(200, json=_response_body(Command.PING)),
    )
    transport = make_transport(fake_clock, retry_window=60.0)
    wire.bind(transport)

    transport.call(Command.PING, {})

    assert len(wire.requests) == 3
    assert fake_clock.slept == [1.0, 2.0]


def test_exhausted_window_raises_client_connection_error_for_a_status(
    fake_clock,
) -> None:
    wire = Wire(httpx.Response(503, text="busy"))
    transport = make_transport(fake_clock, retry_window=3.0)
    wire.bind(transport)

    with pytest.raises(ClientConnectionError) as exc_info:
        transport.call(Command.COMPLETE, PAYLOAD_BY_COMMAND[Command.COMPLETE])

    text = str(exc_info.value)
    assert "localhost:33084" in text
    assert f"{len(wire.requests)} attempts" in text
    assert "last HTTP status 503" in text
    # 1 + 2 fills the 3 second window exactly, so the third failure gives up.
    assert fake_clock.slept == [1.0, 2.0]
    assert len(wire.requests) == 3


def test_exhausted_window_raises_client_connection_error_for_a_connection_error(
    fake_clock,
) -> None:
    wire = Wire(httpx.ConnectError("refused"))
    transport = make_transport(fake_clock, retry_window=1.0)
    wire.bind(transport)

    with pytest.raises(ClientConnectionError) as exc_info:
        transport.call(Command.PING, {})

    assert "last connection error (ConnectError)" in str(exc_info.value)


def test_zero_window_makes_a_single_attempt(fake_clock) -> None:
    wire = Wire(httpx.Response(503, text="busy"))
    transport = make_transport(fake_clock, retry_window=0.0)
    wire.bind(transport)

    with pytest.raises(ClientConnectionError):
        transport.call(Command.PING, {})

    assert len(wire.requests) == 1
    assert fake_clock.slept == []


def test_single_timeout_reaches_the_request() -> None:
    wire = Wire(httpx.Response(200, json=_response_body(Command.PING)))
    transport = make_transport(single_timeout=2.5)
    wire.bind(transport)

    transport.call(Command.PING, {})

    timeout = wire.requests[0].extensions["timeout"]
    assert timeout == {
        "connect": 2.5,
        "read": 2.5,
        "write": 2.5,
        "pool": 2.5,
    }


# open / close -----------------------------------------------------------------


def _write_ca_file(path: Path) -> None:
    """Write a self-signed certificate usable as a CA file.

    Generated in the test process with the ``cryptography`` test dependency,
    like the server-side TLS tests: a checked-in certificate expires and then
    fails the suite on a date nobody changed anything on.
    """
    import datetime

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))


def test_open_defaults_to_plaintext_http() -> None:
    transport = make_transport()

    transport.open()
    try:
        assert str(transport.client.base_url) == "http://localhost:33084"
    finally:
        transport.close()


def test_open_with_a_ca_file_switches_to_https(tmp_path: Path) -> None:
    ca_file = tmp_path / "ca.pem"
    _write_ca_file(ca_file)
    transport = make_transport(ca_file=str(ca_file))

    transport.open()
    try:
        assert str(transport.client.base_url) == "https://localhost:33084"
    finally:
        transport.close()


def test_open_with_an_unreadable_ca_file_raises_before_any_request(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "no-such.pem"
    transport = make_transport(ca_file=str(missing))

    with pytest.raises(InvalidRequestError) as exc_info:
        transport.open()

    text = str(exc_info.value)
    assert str(missing) in text
    assert "cannot read" in text
    assert transport.client is None


def test_open_with_an_empty_ca_file_raises(tmp_path: Path) -> None:
    ca_file = tmp_path / "ca.pem"
    ca_file.write_bytes(b"  \n")
    transport = make_transport(ca_file=str(ca_file))

    with pytest.raises(InvalidRequestError, match="is empty"):
        transport.open()


def test_open_with_an_unparseable_ca_file_raises(tmp_path: Path) -> None:
    ca_file = tmp_path / "ca.pem"
    ca_file.write_bytes(b"not a certificate")
    transport = make_transport(ca_file=str(ca_file))

    with pytest.raises(InvalidRequestError) as exc_info:
        transport.open()

    text = str(exc_info.value)
    assert str(ca_file) in text
    assert "as a root of trust" in text


def test_open_with_a_server_name_override_warns(captured_console_log) -> None:
    transport = make_transport(server_name="login01.short")

    transport.open()
    try:
        warnings = warning_lines(captured_console_log)
        assert len(warnings) == 1
        assert "login01.short" in warnings[0]
        assert "localhost" in warnings[0]
    finally:
        transport.close()


def test_open_twice_keeps_the_same_client() -> None:
    transport = make_transport()
    transport.open()
    client = transport.client
    try:
        transport.open()
        assert transport.client is client
    finally:
        transport.close()


def test_close_without_open_is_silent_and_close_is_idempotent() -> None:
    transport = make_transport()
    transport.close()
    transport.open()
    transport.close()
    transport.close()
    assert transport.client is None


def test_call_before_open_raises_a_transport_error() -> None:
    transport = make_transport()

    with pytest.raises(TransportError, match="not open"):
        transport.call(Command.PING, {})
