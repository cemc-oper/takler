"""Unit tests for the HTTP transport: the endpoint, the auth wiring, uvicorn.

``test_handlers_http_boundary.py`` proves the sixteen commands keep their
semantics over HTTP; this file pins what the transport adds around them:

* the envelope contract -- the response echoes the request's ``trace_id``, an
  unroutable command or a malformed envelope is a transport error (``422``),
  a mismatched envelope ``command`` is a ``400``, and a business failure is a
  ``200`` with the Error_Code in ``flag`` (the status code never says whether
  the *command* succeeded);
* the authentication wiring -- the three credential headers drive the shared
  :class:`~takler.server.auth.AuthGate`, a refusal is answered ``401`` /
  ``403`` with the same WARNING and the same ``denied`` Audit_Record as on
  gRPC, and a refused request is rejected before its body is parsed;
* the credential publication -- the accepted credentials are visible to the
  zombie check and the audit records of the dispatch, and they never leak
  into the next request;
* the uvicorn lifecycle -- ``start`` binds (a taken port is an ordinary
  exception, not a process exit), ``run`` serves until ``stop``.

No credential value is printed, put into a test name or into an assertion
message: the secrets are literals of this file and only ever appear inside an
assertion expression or handed to the code under test.
"""

from __future__ import annotations

# ruff: noqa: E402 -- the takler imports below intentionally follow the
# importorskip guards, so a checkout without the ``http`` extra skips this
# module instead of failing at collection.

import asyncio
import contextlib
import io
import json
from pathlib import Path
from typing import Dict, List, Optional

import pytest

pytest.importorskip(
    "fastapi", reason="the HTTP transport lives behind the takler[http] extra"
)
httpx = pytest.importorskip("httpx", reason="the HTTP test driver posts through httpx")

import takler.logging
from takler.core import Bunch, Flow
from takler.core.state import NodeStatus
from takler.server.audit import (
    DENIED_ERROR_CODE,
    EVENT_DENIED,
    OUTCOME_DENIED,
)
from takler.server.auth import (
    AuthGate,
    CredentialStore,
    get_call_credentials,
)
from takler.server.connect_config import AuthMode, ZombiePolicy
from takler.server.handlers import CommandHandlers
from takler.server.http_transport import API_PREFIX, HttpTransport, create_app
from takler.server.scheduler import Scheduler
from takler.server.zombie import ZombieDetector

SECRET = "operator-secret-value"
WRONG_SECRET = "retired-secret-value"
USER = "alice"
OTHER_USER = "bob"
JOB_PASSWORD = "job-password-value"
OTHER_PASSWORD = "stale-password-value"

TASK1 = "/flow1/task1"


# helpers and fixtures --------------------------------------------------------


class RecordingAuditLogger:
    """An Audit_Logger double collecting the records it is handed."""

    def __init__(self) -> None:
        self.records: List = []

    def record(self, record) -> None:
        self.records.append(record)


@pytest.fixture
def store(tmp_path: Path) -> CredentialStore:
    """A store holding one operator secret and one whitelisted user."""
    secret_file = tmp_path / "secret"
    secret_file.write_text(f"{SECRET}\n")
    whitelist_file = tmp_path / "whitelist"
    whitelist_file.write_text(f"{USER}\n")
    return CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)


def build_app(
    scheduler: Optional[Scheduler] = None,
    gate: Optional[AuthGate] = None,
    audit_logger: Optional[RecordingAuditLogger] = None,
):
    """An app on a fresh bunch, with the audit logger shared by gate and handlers."""
    if scheduler is None:
        scheduler = Scheduler(bunch=Bunch(name="bunch"))
    handlers = CommandHandlers(scheduler, audit_logger=audit_logger)
    if gate is None and audit_logger is not None:
        gate = AuthGate(audit_logger=audit_logger)
    return create_app(handlers, gate=gate)


def enabled_app(store: CredentialStore, audit_logger: RecordingAuditLogger):
    """An app whose gate enforces authentication against ``store``."""
    gate = AuthGate(
        auth_mode=AuthMode.ENABLED,
        credential_store=store,
        audit_logger=audit_logger,
    )
    return build_app(gate=gate, audit_logger=audit_logger)


async def _post(
    app,
    command: str,
    body,
    headers: Optional[Dict[str, str]] = None,
) -> "httpx.Response":
    """POST ``body`` to the command endpoint through the ASGI transport."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        return await client.post(
            f"{API_PREFIX}/commands/{command}", content=body, headers=headers
        )


def post_envelope(
    app,
    command: str,
    payload: Optional[dict] = None,
    headers: Optional[Dict[str, str]] = None,
    trace_id: Optional[str] = None,
) -> "httpx.Response":
    """POST a well formed request envelope and answer with the raw response."""
    envelope = {"command": command, "payload": {} if payload is None else payload}
    if trace_id is not None:
        envelope["trace_id"] = trace_id
    return asyncio.run(
        _post(
            app,
            command,
            json.dumps(envelope).encode("utf-8"),
            headers={"content-type": "application/json", **(headers or {})},
        )
    )


def operator_headers(
    secret: Optional[str] = SECRET, user: Optional[str] = USER
) -> Dict[str, str]:
    headers = {}
    if secret is not None:
        headers["takler-secret"] = secret
    if user is not None:
        headers["takler-user"] = user
    return headers


@contextlib.contextmanager
def captured_console_log():
    """Capture takler's console log output (the logging backend does not
    route records into pytest's ``caplog``)."""
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="DEBUG", console=True)
            yield buffer
    finally:
        takler.logging.configure(console=True)


# the envelope contract -------------------------------------------------------


def test_response_echoes_the_trace_id() -> None:
    app = build_app()

    response = post_envelope(app, "ping", trace_id="0" * 32)

    assert response.status_code == 200
    assert response.json()["trace_id"] == "0" * 32


def test_unknown_command_is_a_transport_error() -> None:
    app = build_app()

    response = post_envelope(app, "nope")

    # FastAPI's path parameter validation: the URL names no known command.
    assert response.status_code == 422


def test_a_body_that_is_not_an_envelope_is_a_transport_error() -> None:
    app = build_app()

    for body in (
        b"not json",
        b'{"payload": {}}',  # no command
        b'{"command": "ping", "payload": {}, "bogus": 1}',  # extra field
    ):
        response = asyncio.run(
            _post(
                app,
                "ping",
                body,
                headers={"content-type": "application/json"},
            )
        )
        assert response.status_code == 422, body


def test_envelope_command_must_match_the_url() -> None:
    app = build_app()

    body = json.dumps({"command": "show", "payload": {}}).encode("utf-8")
    response = asyncio.run(
        _post(app, "ping", body, headers={"content-type": "application/json"})
    )

    assert response.status_code == 400
    assert "'show'" in response.json()["detail"]
    assert "'ping'" in response.json()["detail"]


def test_a_business_failure_is_200_with_the_error_code_in_flag() -> None:
    """The HTTP status speaks for the transport, never for the command: a
    command the server refuses at the business level is a ``200`` whose
    response payload carries the Error_Code, exactly as on gRPC."""
    app = build_app()

    response = post_envelope(app, "complete", {"node_path": "/no/such/node"})

    assert response.status_code == 200
    payload = response.json()["payload"]
    assert payload["flag"] == 10  # node_not_found, stated literally
    assert "NodeNotFoundError" in payload["message"]


def test_a_payload_failing_dto_validation_is_a_business_failure() -> None:
    """The payload is parsed inside the handlers' exception boundary, so a
    malformed meter value is the server's ``internal_error`` classification
    (``flag=99``), not a transport ``422`` -- identical to gRPC."""
    app = build_app()

    response = post_envelope(
        app,
        "meter",
        {"node_path": TASK1, "meter_name": "meter1", "meter_value": "abc"},
    )

    assert response.status_code == 200
    payload = response.json()["payload"]
    assert payload["flag"] == 99
    assert "ValidationError" in payload["message"]


# authentication --------------------------------------------------------------


def test_operator_command_without_credentials_is_401(
    store: CredentialStore,
) -> None:
    audit_logger = RecordingAuditLogger()
    app = enabled_app(store, audit_logger)

    with captured_console_log() as log:
        response = post_envelope(app, "requeue", {"node_paths": ["/flow1"]})

    assert response.status_code == 401
    detail = response.json()["detail"]
    assert detail.endswith("refused: missing_credential")
    # The same refusal records as on gRPC: one WARNING, one denied record.
    assert "refused" in log.getvalue()
    assert "missing_credential" in log.getvalue()
    assert len(audit_logger.records) == 1
    record = audit_logger.records[0]
    assert record.event == EVENT_DENIED
    assert record.outcome == OUTCOME_DENIED
    assert record.error_code == DENIED_ERROR_CODE
    assert record.command == "requeue"
    assert record.user == "unknown"


def test_operator_command_with_a_wrong_secret_is_403(store: CredentialStore) -> None:
    audit_logger = RecordingAuditLogger()
    app = enabled_app(store, audit_logger)

    with captured_console_log() as log:
        response = post_envelope(
            app,
            "requeue",
            {"node_paths": ["/flow1"]},
            headers=operator_headers(secret=WRONG_SECRET),
        )

    assert response.status_code == 403
    # No credential value reaches the refusal records.
    assert WRONG_SECRET not in response.json()["detail"]
    assert WRONG_SECRET not in log.getvalue()
    assert WRONG_SECRET not in audit_logger.records[0].user


def test_operator_command_with_an_unlisted_user_is_403(
    store: CredentialStore,
) -> None:
    app = enabled_app(store, RecordingAuditLogger())

    response = post_envelope(
        app,
        "requeue",
        {"node_paths": ["/flow1"]},
        headers=operator_headers(user=OTHER_USER),
    )

    assert response.status_code == 403
    assert response.json()["detail"].endswith("refused: not_in_whitelist")


def test_operator_command_with_valid_credentials_passes(
    store: CredentialStore,
) -> None:
    """Valid credentials let the command through, and the published identity
    reaches the control Audit_Record of the dispatch -- the same record the
    gRPC transport writes (Requirement 11.8)."""
    audit_logger = RecordingAuditLogger()
    app = enabled_app(store, audit_logger)

    response = post_envelope(
        app,
        "requeue",
        {"node_paths": ["/flow1"]},
        headers=operator_headers(),
    )

    assert response.status_code == 200
    assert response.json()["payload"]["flag"] == 10  # /flow1 does not exist here
    assert len(audit_logger.records) == 1
    record = audit_logger.records[0]
    assert record.event == "control"
    assert record.command == "requeue"
    assert record.user == USER


def test_child_command_without_a_job_password_is_401(
    store: CredentialStore,
) -> None:
    app = enabled_app(store, RecordingAuditLogger())

    response = post_envelope(app, "complete", {"node_path": TASK1})

    assert response.status_code == 401
    assert response.json()["detail"].endswith("refused: missing_credential")


def test_child_command_with_any_job_password_passes(store: CredentialStore) -> None:
    """The gate checks presence only; the value is the Zombie_Detector's
    business (Requirements 6.4, 6.13)."""
    app = enabled_app(store, RecordingAuditLogger())

    response = post_envelope(
        app,
        "complete",
        {"node_path": TASK1},
        headers={"takler-pass": JOB_PASSWORD},
    )

    assert response.status_code == 200
    assert response.json()["payload"]["flag"] == 10  # no such node here


def test_ping_is_public_even_when_authentication_is_enabled(
    store: CredentialStore,
) -> None:
    audit_logger = RecordingAuditLogger()
    app = enabled_app(store, audit_logger)

    with captured_console_log() as log:
        response = post_envelope(app, "ping")

    assert response.status_code == 200
    assert "refused" not in log.getvalue()
    assert audit_logger.records == []


def test_a_refusal_happens_before_the_envelope_is_validated(
    store: CredentialStore,
) -> None:
    """An unauthenticated caller may not make the server validate its
    request: the authentication dependency runs before the body is validated,
    so a well formed JSON document that is not a valid envelope is answered
    401, not 422. (The JSON *syntax* check is the one step that precedes
    authentication -- a malformed document is a plain 422 either way.)"""
    app = enabled_app(store, RecordingAuditLogger())

    invalid_envelope = asyncio.run(
        _post(
            app,
            "requeue",
            b'{"payload": {}}',
            headers={"content-type": "application/json"},
        )
    )
    malformed_json = asyncio.run(
        _post(app, "requeue", b"not json", headers={"content-type": "application/json"})
    )

    assert invalid_envelope.status_code == 401
    assert malformed_json.status_code == 422


def test_disabled_mode_publishes_the_parsed_credentials(
    store: CredentialStore,
) -> None:
    """With Auth_Mode ``disabled`` nothing is refused, and a ``takler-user``
    header still reaches the Audit_Record (Requirement 6.3)."""
    audit_logger = RecordingAuditLogger()
    app = build_app(audit_logger=audit_logger)

    response = post_envelope(
        app,
        "requeue",
        {"node_paths": ["/flow1"]},
        headers=operator_headers(secret=None),
    )

    assert response.status_code == 200
    assert audit_logger.records[0].user == USER


# credential publication --------------------------------------------------------


def test_credentials_do_not_leak_between_requests(store: CredentialStore) -> None:
    """Two requests served in the same context (which is how the ASGI test
    transport drives the app) must each see only their own credentials, and
    none at all once they are done: the endpoint publishes for exactly the
    duration of the dispatch."""
    audit_logger = RecordingAuditLogger()
    gate = AuthGate(
        auth_mode=AuthMode.ENABLED,
        credential_store=store,
        audit_logger=audit_logger,
    )
    app = build_app(gate=gate, audit_logger=audit_logger)

    async def two_requests():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            first = await client.post(
                f"{API_PREFIX}/commands/requeue",
                json={"command": "requeue", "payload": {"node_paths": ["/flow1"]}},
                headers=operator_headers(user=USER),
            )
            # USER is whitelisted; OTHER_USER is not, so the second request is
            # answered 403 -- what matters is which user the records name.
            second = await client.post(
                f"{API_PREFIX}/commands/requeue",
                json={"command": "requeue", "payload": {"node_paths": ["/flow1"]}},
                headers=operator_headers(user=OTHER_USER),
            )
            return first, second

    first, second = asyncio.run(two_requests())

    assert first.status_code == 200
    assert second.status_code == 403
    assert [record.user for record in audit_logger.records] == [USER, OTHER_USER]
    credentials = get_call_credentials()
    assert credentials.user is None
    assert credentials.job_password is None


def test_a_zombie_child_command_is_disposed_like_on_grpc(
    store: CredentialStore,
) -> None:
    """The Job_Password travels as the ``takler-pass`` header and reaches the
    Zombie_Detector through the same context publication as on gRPC: a stale
    password is a zombie (policy ``fail`` leaves the task untouched and
    answers an error), the current password runs the command."""
    flow1 = Flow("flow1")
    with flow1:
        flow1.add_task("task1")
    flow1.begin()
    task1 = flow1.find_node(TASK1)
    task1.run()
    task1.init(task_id="job-1")
    task1.job_password = JOB_PASSWORD

    bunch = Bunch(name="bunch")
    bunch.add_flow(flow1)
    scheduler = Scheduler(
        bunch=bunch,
        zombie_detector=ZombieDetector(
            auth_mode=AuthMode.ENABLED, zombie_policy=ZombiePolicy.FAIL
        ),
    )
    gate = AuthGate(auth_mode=AuthMode.ENABLED, credential_store=store)
    app = build_app(scheduler=scheduler, gate=gate)

    stale = post_envelope(
        app,
        "complete",
        {"node_path": TASK1},
        headers={"takler-pass": OTHER_PASSWORD},
    )

    assert stale.status_code == 200
    payload = stale.json()["payload"]
    assert payload["flag"] != 0
    assert "ZombieError" in payload["message"]
    assert task1.state.node_status is NodeStatus.active

    current = post_envelope(
        app,
        "complete",
        {"node_path": TASK1},
        headers={"takler-pass": JOB_PASSWORD},
    )

    assert current.status_code == 200
    assert current.json()["payload"]["flag"] == 0
    assert task1.state.node_status is NodeStatus.complete


# uvicorn lifecycle -------------------------------------------------------------


def _serve_once(transport: HttpTransport) -> None:
    """start -> ping over a real socket -> stop, awaiting the run task."""

    async def main():
        await transport.start()
        run_task = asyncio.create_task(transport.run())
        port = transport._server.servers[0].sockets[0].getsockname()[1]
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
                for _ in range(10):
                    try:
                        response = await client.post(
                            f"{API_PREFIX}/commands/ping",
                            json={"command": "ping", "payload": {}},
                        )
                        break
                    except httpx.ConnectError:
                        await asyncio.sleep(0.05)
                else:
                    raise AssertionError(
                        "the HTTP listener never accepted a connection"
                    )
            assert response.status_code == 200
            assert response.json()["command"] == "ping"
        finally:
            await transport.stop()
            await asyncio.wait_for(run_task, timeout=10)

    asyncio.run(main())


def test_start_run_stop_serves_real_http() -> None:
    """The full uvicorn lifecycle: ``start`` binds, ``run`` serves until
    ``stop``, and a real socket round trip answers ping. Port 0 lets the OS
    pick the port."""
    transport = HttpTransport(
        scheduler=Scheduler(bunch=Bunch(name="bunch")), host="127.0.0.1", port=0
    )

    _serve_once(transport)


def test_start_with_a_taken_port_raises_instead_of_exiting() -> None:
    """uvicorn's own bind path answers a taken port with ``sys.exit``; this
    transport binds the socket itself, so the failure is an ordinary
    ``OSError`` out of ``start()`` and the process -- and its unified
    shutdown path -- survives."""
    first = HttpTransport(
        scheduler=Scheduler(bunch=Bunch(name="bunch")), host="127.0.0.1", port=0
    )

    async def main():
        await first.start()
        port = first._server.servers[0].sockets[0].getsockname()[1]
        second = HttpTransport(
            scheduler=Scheduler(bunch=Bunch(name="bunch")),
            host="127.0.0.1",
            port=port,
        )
        with pytest.raises(OSError):
            await second.start()
        await first.stop()

    asyncio.run(main())


def test_run_before_start_is_an_error_and_stop_a_no_op() -> None:
    transport = HttpTransport(scheduler=Scheduler(bunch=Bunch(name="bunch")))

    asyncio.run(transport.stop())
    with pytest.raises(RuntimeError, match="start"):
        asyncio.run(transport.run())
