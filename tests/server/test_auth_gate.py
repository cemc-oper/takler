"""Unit tests for the transport-neutral Auth_Gate.

``tests/server/test_auth_interceptor.py`` proves the *wiring* -- that a real
server refuses what the gate decides to refuse. This file pins the decision
layer itself, without a gRPC server, an interceptor or even a ``grpc`` call
frame: metadata goes in as plain ``(key, value)`` pairs, the answer comes back
as an :class:`~takler.server.auth.AuthOutcome`. This is the surface the HTTP
transport's middleware will drive (M3 task 7), so every branch of it is
asserted here directly.

What is pinned, one claim per group of tests:

* ``PUBLIC`` methods pass with the metadata never parsed -- garbage metadata
  and broken credential files included (Requirement 6.8);
* Auth_Mode ``disabled`` passes everything and still returns the parsed
  credentials for publication, because the Zombie_Detector and the
  Audit_Logger read them regardless (Requirement 6.3);
* a Child_Command needs ``takler-pass`` present, and only present
  (Requirements 6.4, 6.13);
* an Operator_Command needs a valid ``takler-secret`` plus a whitelisted
  ``takler-user``; a credential file that cannot be read fails closed
  (Requirements 6.5, 6.6, 6.7, 7.7);
* an unregistered method is treated as an Operator_Command -- fail-closed
  (Requirement 6.2);
* :meth:`AuthGate.refuse` writes the WARNING and the ``denied`` Audit_Record,
  returns the ``"<method> refused: <reason>"`` text for the wire, and leaks
  no credential value into any of the three (Requirements 6.10, 6.12, 11.3,
  12.1).

No credential value is printed, put into a test name or into an assertion
message: the secrets are literals of this file and only ever appear inside an
assertion expression or handed to the code under test.

Validates: Requirements 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.10, 6.12, 6.13,
7.7, 11.3, 12.1
"""

from __future__ import annotations

import contextlib
import io
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import pytest

import takler.logging
from takler.server.auth import (
    METADATA_KEY_JOB_PASSWORD,
    METADATA_KEY_SECRET,
    METADATA_KEY_USER,
    SERVICE_METHOD_PREFIX,
    AuthGate,
    CallCredentials,
    CredentialStore,
    RejectionReason,
)
from takler.server.audit import DENIED_ERROR_CODE, EVENT_DENIED, OUTCOME_DENIED
from takler.server.connect_config import AuthMode

SECRET = "operator-secret-value"
WRONG_SECRET = "retired-secret-value"
USER = "alice"
INTRUDER = "intruder"
JOB_PASSWORD = "job-password-value"
PEER = "ipv4:127.0.0.1:54321"

CHILD_METHOD = SERVICE_METHOD_PREFIX + "RunCommandComplete"
OPERATOR_METHOD = SERVICE_METHOD_PREFIX + "RunCommandRequeue"
PUBLIC_METHOD = SERVICE_METHOD_PREFIX + "RunRequestPing"
UNREGISTERED_METHOD = "/takler_protocol.TaklerServer/RunCommandDropDatabase"


def operator_metadata(
    secret: Optional[str] = SECRET, user: Optional[str] = USER
) -> List[Tuple[str, str]]:
    """The metadata of an Operator_Command carrying ``secret`` and ``user``."""
    metadata = []
    if secret is not None:
        metadata.append((METADATA_KEY_SECRET, secret))
    if user is not None:
        metadata.append((METADATA_KEY_USER, user))
    return metadata


@pytest.fixture
def store(tmp_path: Path) -> CredentialStore:
    """A store holding one operator secret and one whitelisted user."""
    secret_file = tmp_path / "secret"
    secret_file.write_text(f"{SECRET}\n")
    whitelist_file = tmp_path / "whitelist"
    whitelist_file.write_text(f"{USER}\n")
    return CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)


@pytest.fixture
def enabled_gate(store: CredentialStore) -> AuthGate:
    return AuthGate(auth_mode=AuthMode.ENABLED, credential_store=store)


# -- PUBLIC ------------------------------------------------------------------


def test_public_method_passes_without_parsing_the_metadata(
    enabled_gate: AuthGate,
) -> None:
    """A PUBLIC method is let through with the metadata never looked at.

    The metadata here is not merely missing but unparseable, and the
    credential files of ``enabled_gate``'s store are replaced with a directory
    -- an unreadable path -- first: both would fail the call if the gate so
    much as read them (Requirement 6.8).
    """
    enabled_gate.credential_store = CredentialStore(
        secret_file=Path("/nonexistent/secret"),
        whitelist_file=Path("/nonexistent/whitelist"),
    )

    outcome = enabled_gate.authorize(PUBLIC_METHOD, metadata=[("not-a-pair",)])

    assert outcome.authorized
    assert outcome.rejection is None
    assert outcome.credentials is None, "a PUBLIC call publishes nothing"


# -- Auth_Mode disabled -------------------------------------------------------


def test_disabled_mode_passes_and_returns_the_parsed_credentials(
    store: CredentialStore,
) -> None:
    """With Auth_Mode ``disabled`` nothing is checked (Requirement 6.3)."""
    gate = AuthGate(auth_mode=AuthMode.DISABLED, credential_store=store)

    outcome = gate.authorize(OPERATOR_METHOD, metadata=None, peer=PEER)

    assert outcome.authorized
    assert outcome.credentials is not None
    assert outcome.credentials.secret is None
    assert outcome.credentials.peer == PEER


def test_disabled_mode_still_parses_the_credentials(store: CredentialStore) -> None:
    """The Zombie_Detector and the Audit_Logger read the credentials even when
    authentication is off, so they are parsed and returned either way."""
    gate = AuthGate(auth_mode=AuthMode.DISABLED, credential_store=store)

    outcome = gate.authorize(
        CHILD_METHOD, metadata=[(METADATA_KEY_JOB_PASSWORD, JOB_PASSWORD)]
    )

    assert outcome.authorized
    assert outcome.credentials is not None
    assert outcome.credentials.job_password == JOB_PASSWORD


# -- CHILD --------------------------------------------------------------------


def test_child_command_without_a_job_password_is_missing_credential(
    enabled_gate: AuthGate,
) -> None:
    outcome = enabled_gate.authorize(CHILD_METHOD, metadata=[])

    assert not outcome.authorized
    assert outcome.rejection is RejectionReason.MISSING_CREDENTIAL


def test_child_command_with_any_job_password_passes(
    enabled_gate: AuthGate,
) -> None:
    """Presence is all the gate checks; the value is the Zombie_Detector's
    business (Requirements 6.4, 6.13)."""
    outcome = enabled_gate.authorize(
        CHILD_METHOD, metadata=[(METADATA_KEY_JOB_PASSWORD, JOB_PASSWORD)]
    )

    assert outcome.authorized
    assert outcome.credentials is not None
    assert outcome.credentials.job_password == JOB_PASSWORD


# -- OPERATOR -----------------------------------------------------------------


@pytest.mark.parametrize(
    "metadata",
    [
        [],
        operator_metadata(secret=None),
        operator_metadata(user=None),
    ],
    ids=["nothing", "no-secret", "no-user"],
)
def test_operator_command_without_both_keys_is_missing_credential(
    enabled_gate: AuthGate, metadata: Sequence[Tuple[str, str]]
) -> None:
    outcome = enabled_gate.authorize(OPERATOR_METHOD, metadata=metadata)

    assert outcome.rejection is RejectionReason.MISSING_CREDENTIAL


def test_operator_command_with_a_wrong_secret_is_invalid_credential(
    enabled_gate: AuthGate,
) -> None:
    outcome = enabled_gate.authorize(
        OPERATOR_METHOD, metadata=operator_metadata(secret=WRONG_SECRET)
    )

    assert outcome.rejection is RejectionReason.INVALID_CREDENTIAL


def test_operator_command_with_a_non_whitelisted_user_is_refused(
    enabled_gate: AuthGate,
) -> None:
    outcome = enabled_gate.authorize(
        OPERATOR_METHOD, metadata=operator_metadata(user=INTRUDER)
    )

    assert outcome.rejection is RejectionReason.NOT_IN_WHITELIST


def test_operator_command_with_valid_credentials_passes(
    enabled_gate: AuthGate,
) -> None:
    outcome = enabled_gate.authorize(OPERATOR_METHOD, metadata=operator_metadata())

    assert outcome.authorized
    assert outcome.credentials is not None
    assert outcome.credentials.secret == SECRET
    assert outcome.credentials.user == USER


def test_operator_command_fails_closed_on_an_unreadable_secret_file(
    tmp_path: Path,
) -> None:
    """A credential file that cannot be read is answered like a wrong secret:
    the server cannot check, and "cannot check" must never mean "let through"
    (Requirement 7.7)."""
    secret_file = tmp_path / "secret"
    secret_file.write_text(f"{SECRET}\n")
    gate = AuthGate(
        auth_mode=AuthMode.ENABLED,
        credential_store=CredentialStore(secret_file=secret_file),
    )
    secret_file.unlink()

    outcome = gate.authorize(OPERATOR_METHOD, metadata=operator_metadata())

    assert outcome.rejection is RejectionReason.INVALID_CREDENTIAL


def test_unregistered_method_fails_closed_as_operator(
    enabled_gate: AuthGate,
) -> None:
    """A method nobody classified demands operator credentials (Req 6.2)."""
    outcome = enabled_gate.authorize(UNREGISTERED_METHOD, metadata=[])

    assert outcome.rejection is RejectionReason.MISSING_CREDENTIAL

    outcome = enabled_gate.authorize(UNREGISTERED_METHOD, metadata=operator_metadata())

    assert outcome.authorized


# -- refuse -------------------------------------------------------------------


class RecordingAuditLogger:
    """An Audit_Logger double collecting the records it is handed."""

    def __init__(self) -> None:
        self.records: List = []

    def record(self, record) -> None:
        self.records.append(record)


def _refuse_capturing_warning(
    gate: AuthGate, method: str, credentials, reason: RejectionReason
) -> Tuple[str, str]:
    """Run ``gate.refuse`` while capturing the console log output."""
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="DEBUG", console=True)
            details = gate.refuse(method, credentials, reason)
    finally:
        takler.logging.configure(console=True)
    return details, buffer.getvalue()


def test_refuse_returns_the_details_and_logs_the_warning(
    store: CredentialStore,
) -> None:
    """One refusal: one WARNING naming method, user, peer and classification,
    and the wire text ``"<method> refused: <reason>"`` (Requirement 6.10)."""
    gate = AuthGate(auth_mode=AuthMode.ENABLED, credential_store=store)
    credentials = CallCredentials(user=USER, peer=PEER)

    details, log = _refuse_capturing_warning(
        gate, OPERATOR_METHOD, credentials, RejectionReason.INVALID_CREDENTIAL
    )

    assert details == f"{OPERATOR_METHOD} refused: invalid_credential"
    assert OPERATOR_METHOD in log
    assert USER in log
    assert PEER in log
    assert "invalid_credential" in log


def test_refuse_writes_the_denied_audit_record(store: CredentialStore) -> None:
    """The ``denied`` Audit_Record: fixed event/outcome, the denial error code,
    an empty target -- the request body is never parsed on this path --
    and the caller's user and peer (Requirement 11.3)."""
    audit_logger = RecordingAuditLogger()
    gate = AuthGate(
        auth_mode=AuthMode.ENABLED,
        credential_store=store,
        audit_logger=audit_logger,
    )

    gate.refuse(
        OPERATOR_METHOD,
        CallCredentials(user=USER, peer=PEER),
        RejectionReason.NOT_IN_WHITELIST,
    )

    assert len(audit_logger.records) == 1
    record = audit_logger.records[0]
    assert record.event == EVENT_DENIED
    assert record.outcome == OUTCOME_DENIED
    assert record.error_code == DENIED_ERROR_CODE
    assert record.target == []
    assert record.user == USER
    assert record.peer == PEER


def test_refuse_echoes_no_credential_value(store: CredentialStore) -> None:
    """Neither the WARNING, nor the wire text, nor the audit record carries a
    credential value -- not even one the caller echoed in an echoed field
    (Requirements 6.12, 12.1)."""
    audit_logger = RecordingAuditLogger()
    gate = AuthGate(
        auth_mode=AuthMode.ENABLED,
        credential_store=store,
        audit_logger=audit_logger,
    )
    # A caller whose ``takler-user`` is the very secret it presented.
    credentials = CallCredentials(secret=SECRET, user=SECRET, peer=PEER)

    details, log = _refuse_capturing_warning(
        gate, OPERATOR_METHOD, credentials, RejectionReason.NOT_IN_WHITELIST
    )

    assert SECRET not in details
    assert SECRET not in log
    assert len(audit_logger.records) == 1
    assert SECRET not in audit_logger.records[0].user


def test_refuse_without_an_audit_logger_still_logs(store: CredentialStore) -> None:
    """No Audit_Logger configured: the WARNING is emitted either way and the
    refusal still answers normally (Requirement 6.10)."""
    gate = AuthGate(auth_mode=AuthMode.ENABLED, credential_store=store)

    details, log = _refuse_capturing_warning(
        gate,
        OPERATOR_METHOD,
        CallCredentials(user=USER, peer=PEER),
        RejectionReason.MISSING_CREDENTIAL,
    )

    assert details.endswith("refused: missing_credential")
    assert "missing_credential" in log
