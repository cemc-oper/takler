"""Unit tests for the rejection record of :class:`AuthInterceptor`.

Task 7.4 of the *m2-security* spec is about what a refusal is allowed to say.
Requirement 6.10 fixes the content of the WARNING -- method name,
``takler-user`` value, caller address, classification -- and Requirements 6.12
and 12.1 fix what neither the WARNING nor the gRPC abort details may contain:
any credential value.

Two things are pinned here. The first is the record itself: one WARNING per
refused RPC, carrying the four required elements, and abort details carrying
the method name plus the classification and nothing else. The second is the
sanitizing of the two caller-controlled fields, which is the part that is easy
to lose in a later edit: the method name of an unregistered rpc and the
``takler-user`` value both arrive from the wire on exactly this path, so a
newline in either forges a log line, an unbounded value writes an unbounded
record, and a caller that names itself after its own secret would otherwise get
that value echoed into the log and back over the wire.

The interceptor is exercised directly with a stand-in
``handler_call_details`` and a stand-in ``ServicerContext`` rather than through
a real gRPC server: this file is about the text of the record, and the
end-to-end status codes belong to ``test_auth_interceptor.py`` (task 7.5).

Log assertions go through a captured console sink rather than ``caplog``, since
the logging backend does not route records into pytest's handler (same approach
as ``test_credential_store_fail_closed.py``).

Validates: Requirements 6.10, 6.12, 12.1
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import io
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

import pytest

import takler.logging
from takler.server.auth import (
    MAX_ECHOED_LENGTH,
    METADATA_KEY_JOB_PASSWORD,
    METADATA_KEY_SECRET,
    METADATA_KEY_USER,
    REDACTED,
    SERVICE_METHOD_PREFIX,
    TRUNCATION_MARKER,
    AuthInterceptor,
    CallCredentials,
    CredentialStore,
    RejectionReason,
    sanitize_echoed_value,
)
from takler.server.connect_config import AuthMode

SECRET = "operator-secret-value"
STALE_SECRET = "retired-secret-value"
USER = "alice"
PEER = "ipv4:127.0.0.1:54321"

CHILD_METHOD = SERVICE_METHOD_PREFIX + "RunCommandComplete"
OPERATOR_METHOD = SERVICE_METHOD_PREFIX + "RunCommandRequeue"


@dataclasses.dataclass
class _HandlerCallDetails:
    """The two attributes :meth:`AuthInterceptor.intercept_service` reads."""

    method: str
    invocation_metadata: Sequence[Tuple[str, str]] = ()


class _Context:
    """A stand-in ``ServicerContext`` that records the abort instead of raising."""

    def __init__(self, peer: Optional[str] = PEER) -> None:
        self._peer = peer
        self.aborted: List[Tuple[Any, str]] = []

    def peer(self) -> Optional[str]:
        return self._peer

    async def abort(self, code: Any, details: str) -> None:
        self.aborted.append((code, details))


async def _never_called(handler_call_details: Any) -> Any:
    """A ``continuation`` that fails the test if a refused call reaches it."""
    raise AssertionError("a refused RPC must not reach the continuation")


@pytest.fixture
def store(tmp_path: Path) -> CredentialStore:
    """A store holding one operator secret and one whitelisted user."""
    secret_file = tmp_path / "secret"
    secret_file.write_text(f"{SECRET}\n")
    whitelist_file = tmp_path / "whitelist"
    whitelist_file.write_text(f"{USER}\n")
    return CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)


def _refuse(
    interceptor: AuthInterceptor,
    method: str,
    metadata: Sequence[Tuple[str, str]],
) -> Tuple[Any, str, str]:
    """Run one RPC that is expected to be refused.

    Returns:
        The status code, the abort details and the captured log output.
    """
    context = _Context()
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="DEBUG", console=True)

            async def run() -> None:
                handler = await interceptor.intercept_service(
                    _never_called,
                    _HandlerCallDetails(method=method, invocation_metadata=metadata),
                )
                assert handler is not None
                await handler.unary_unary(object(), context)

            asyncio.run(run())
    finally:
        takler.logging.configure(console=True)

    assert len(context.aborted) == 1, "a refusal aborts the call exactly once"
    code, details = context.aborted[0]
    return code, details, buffer.getvalue()


def test_rejection_warning_carries_the_four_required_elements(
    store: CredentialStore,
) -> None:
    """The WARNING names the method, the user, the peer and the classification.

    Validates: Requirement 6.10
    """
    interceptor = AuthInterceptor(auth_mode=AuthMode.ENABLED, credential_store=store)

    _, _, log = _refuse(
        interceptor,
        OPERATOR_METHOD,
        [(METADATA_KEY_SECRET, STALE_SECRET), (METADATA_KEY_USER, USER)],
    )

    assert "WARNING" in log
    assert OPERATOR_METHOD in log
    assert USER in log
    assert PEER in log
    assert RejectionReason.INVALID_CREDENTIAL.value in log


def test_rejection_warning_is_emitted_once_per_refused_rpc(
    store: CredentialStore,
) -> None:
    """One refusal writes one record, not one per check that failed."""
    interceptor = AuthInterceptor(auth_mode=AuthMode.ENABLED, credential_store=store)

    _, _, log = _refuse(
        interceptor,
        OPERATOR_METHOD,
        [(METADATA_KEY_SECRET, STALE_SECRET), (METADATA_KEY_USER, USER)],
    )

    assert log.count("refused ") == 1


def test_missing_user_is_recorded_as_unknown(store: CredentialStore) -> None:
    """A caller that identifies itself with nothing is logged as ``unknown``.

    Validates: Requirement 6.10
    """
    interceptor = AuthInterceptor(auth_mode=AuthMode.ENABLED, credential_store=store)

    _, _, log = _refuse(interceptor, OPERATOR_METHOD, [(METADATA_KEY_SECRET, SECRET)])

    assert "user=unknown" in log
    assert RejectionReason.MISSING_CREDENTIAL.value in log


@pytest.mark.parametrize(
    "metadata, expected",
    [
        pytest.param(
            [(METADATA_KEY_SECRET, STALE_SECRET), (METADATA_KEY_USER, USER)],
            (STALE_SECRET,),
            id="wrong-secret",
        ),
        pytest.param(
            [(METADATA_KEY_SECRET, SECRET), (METADATA_KEY_USER, "intruder")],
            (SECRET,),
            id="not-whitelisted",
        ),
    ],
)
def test_neither_log_nor_details_echo_the_presented_secret(
    store: CredentialStore,
    metadata: Sequence[Tuple[str, str]],
    expected: Sequence[str],
) -> None:
    """A presented Operator_Secret reaches neither the log nor the client.

    Validates: Requirements 6.12, 12.1
    """
    interceptor = AuthInterceptor(auth_mode=AuthMode.ENABLED, credential_store=store)

    _, details, log = _refuse(interceptor, OPERATOR_METHOD, metadata)

    for secret in expected:
        assert secret not in details
        assert secret not in log
    # The configured secret must not leak either, however the refusal came
    # about: a rejected caller must not learn what would have been accepted.
    assert SECRET not in details


def test_child_rejection_does_not_echo_the_job_password(
    store: CredentialStore,
) -> None:
    """A Child_Command refusal carries no Job_Password.

    A refused child carries no password by definition -- that is why it is
    refused -- so this covers the blank case, which must not be echoed either.

    Validates: Requirements 6.12, 12.1
    """
    interceptor = AuthInterceptor(auth_mode=AuthMode.ENABLED, credential_store=store)

    _, details, log = _refuse(interceptor, CHILD_METHOD, [])

    assert RejectionReason.MISSING_CREDENTIAL.value in details
    assert METADATA_KEY_JOB_PASSWORD not in details
    assert "refused " in log


def test_details_carry_only_the_method_and_the_classification(
    store: CredentialStore,
) -> None:
    """The abort details say what was refused and how it is classified.

    They deliberately do not say *which* check failed on the server's own files:
    "your secret is right but your user name is not whitelisted" tells a caller
    that the secret it holds is still live.

    Validates: Requirement 6.12
    """
    interceptor = AuthInterceptor(auth_mode=AuthMode.ENABLED, credential_store=store)

    _, details, _ = _refuse(
        interceptor,
        OPERATOR_METHOD,
        [(METADATA_KEY_SECRET, SECRET), (METADATA_KEY_USER, "intruder")],
    )

    assert (
        details
        == f"{OPERATOR_METHOD} refused: {RejectionReason.NOT_IN_WHITELIST.value}"
    )


def test_a_newline_in_the_user_name_cannot_forge_a_log_line(
    store: CredentialStore,
) -> None:
    """A control character in ``takler-user`` is escaped, not written through.

    Validates: Requirements 6.10, 12.1
    """
    interceptor = AuthInterceptor(auth_mode=AuthMode.ENABLED, credential_store=store)
    forged = "alice\nWARNING refused nothing: authorized"

    _, _, log = _refuse(
        interceptor,
        OPERATOR_METHOD,
        [(METADATA_KEY_SECRET, STALE_SECRET), (METADATA_KEY_USER, forged)],
    )

    # The escaped newline is the whole point: the refusal stays one line, so
    # the forged text cannot pass for a record of its own.
    lines = [line for line in log.splitlines() if "refused " in line]
    assert len(lines) == 1
    assert "\\x0a" in lines[0]
    assert "WARNING refused nothing" in lines[0]  # inside the record, escaped
    assert not any(line.startswith("WARNING") for line in log.splitlines())


def test_a_hostile_method_name_is_bounded_in_the_record(
    store: CredentialStore,
) -> None:
    """An unregistered, oversized method name does not write an unbounded record.

    An unregistered method resolves to ``OPERATOR`` and is therefore refused
    rather than dropped, which makes the method name caller-controlled here.

    Validates: Requirements 6.10, 6.12
    """
    interceptor = AuthInterceptor(auth_mode=AuthMode.ENABLED, credential_store=store)
    method = SERVICE_METHOD_PREFIX + "A" * 5000

    _, details, log = _refuse(
        interceptor,
        method,
        [(METADATA_KEY_SECRET, STALE_SECRET), (METADATA_KEY_USER, USER)],
    )

    assert TRUNCATION_MARKER in details
    assert len(details) < MAX_ECHOED_LENGTH + 100
    assert len(log) < 2 * (MAX_ECHOED_LENGTH + 200)


def test_a_user_name_equal_to_the_secret_is_redacted(store: CredentialStore) -> None:
    """A caller cannot get its own secret echoed by sending it as its user name.

    Validates: Requirements 6.12, 12.1
    """
    interceptor = AuthInterceptor(auth_mode=AuthMode.ENABLED, credential_store=store)

    _, details, log = _refuse(
        interceptor,
        OPERATOR_METHOD,
        [(METADATA_KEY_SECRET, SECRET), (METADATA_KEY_USER, SECRET)],
    )

    assert SECRET not in log
    assert SECRET not in details
    assert REDACTED in log


def test_sanitize_leaves_a_normal_value_untouched() -> None:
    """Nothing legitimate is rewritten: the common case is the identity."""
    assert sanitize_echoed_value(OPERATOR_METHOD) == OPERATOR_METHOD
    assert sanitize_echoed_value(USER) == USER
    # Printable non-ASCII stays readable rather than being escaped.
    assert sanitize_echoed_value("用户-李") == "用户-李"
    assert sanitize_echoed_value(None) == "None"


def test_sanitize_keeps_short_credentials_readable() -> None:
    """A credential too short to be worth redacting does not garble the record.

    Blanking a one-character value out of every record would destroy the record
    while protecting a value that is guessable in a handful of attempts.
    """
    credentials = CallCredentials(secret="a", user="alice")

    assert sanitize_echoed_value(OPERATOR_METHOD, credentials) == OPERATOR_METHOD


def test_sanitize_redacts_before_truncating() -> None:
    """A credential parked past the length limit is removed, not cut in half."""
    secret = "x" * 32
    credentials = CallCredentials(secret=secret)

    parked = sanitize_echoed_value("b" * (MAX_ECHOED_LENGTH - 5) + secret, credentials)
    assert secret not in parked

    within = sanitize_echoed_value(f"{secret}-tail", credentials)
    assert within == f"{REDACTED}-tail"
