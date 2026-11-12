"""Unit tests for the run-time fail-closed path of :class:`CredentialStore`.

Task 6.4 of the *m2-security* spec adds
:meth:`takler.server.auth.CredentialStore.authorize_operator`, the run-time
counterpart of ``validate_at_startup``. This file pins the part of it that
Requirement 7.7 is about: when a credential file cannot be read *while the
server is serving*, the store must log an ERROR naming the path and the reason
and answer with the ``invalid_credential`` classification -- never raise, since
an exception escaping the Auth_Interceptor would reach the client as ``UNKNOWN``
instead of ``PERMISSION_DENIED``.

The happy path and the ordering of the classifications are asserted alongside
it, because "refused because unreadable" only means something if the same call
returns ``None`` when the files are in place.

Log assertions go through a captured console sink rather than ``caplog``: the
logging backend does not route records into pytest's handler (same approach as
``test_checkpoint_write_unit.py``).

Validates: Requirements 7.7, 6.11
"""

from __future__ import annotations

import contextlib
import io
from pathlib import Path
from typing import Tuple

import pytest

import takler.logging
from takler.server.auth import CredentialStore, RejectionReason

SECRET = "s3cret-value"
USER = "alice"


def _capturing_stderr(func) -> Tuple[object, str]:
    """Run ``func`` while capturing the console log output."""
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="DEBUG", console=True)
            result = func()
    finally:
        takler.logging.configure(console=True)
    return result, buffer.getvalue()


@pytest.fixture
def store_files(tmp_path: Path) -> Tuple[Path, Path]:
    """A readable secret file and whitelist file holding one value each."""
    secret_file = tmp_path / "secret"
    secret_file.write_text(f"# rotation round 1\n{SECRET}\n")
    whitelist_file = tmp_path / "whitelist"
    whitelist_file.write_text(f"{USER}\n")
    return secret_file, whitelist_file


def test_authorizes_when_both_files_are_readable(store_files) -> None:
    """A matching secret plus a whitelisted user is authorized."""
    secret_file, whitelist_file = store_files
    store = CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)

    assert store.authorize_operator(SECRET, USER) is None


def test_missing_credentials_are_reported_before_the_files_are_read(
    tmp_path: Path,
) -> None:
    """An absent secret or user yields ``missing_credential``.

    The files do not even exist here, which is the point: presence is checked
    first, so a caller carrying nothing is told it carries nothing rather than
    being told about the server's own files.
    """
    store = CredentialStore(
        secret_file=tmp_path / "absent-secret",
        whitelist_file=tmp_path / "absent-whitelist",
    )

    assert store.authorize_operator(None, USER) is RejectionReason.MISSING_CREDENTIAL
    assert store.authorize_operator(SECRET, None) is RejectionReason.MISSING_CREDENTIAL


def test_wrong_secret_and_unlisted_user_are_classified_apart(store_files) -> None:
    """A stale secret is ``invalid_credential``, an unlisted user is not."""
    secret_file, whitelist_file = store_files
    store = CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)

    assert store.authorize_operator("wrong", USER) is RejectionReason.INVALID_CREDENTIAL
    assert (
        store.authorize_operator(SECRET, "mallory") is RejectionReason.NOT_IN_WHITELIST
    )


def test_deleted_secret_file_refuses_with_an_error_log(store_files) -> None:
    """A secret file removed at run time refuses the call and logs an ERROR.

    The previously parsed set must not keep being accepted, and the ERROR has
    to name the path and the reason so the operator can tell a server-side
    breakage from a client presenting a stale secret (Requirement 7.7).
    """
    secret_file, whitelist_file = store_files
    store = CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)
    # Populate the fingerprint cache first, so the refusal cannot be an
    # artifact of the file having never been read.
    assert store.authorize_operator(SECRET, USER) is None

    secret_file.unlink()
    reason, output = _capturing_stderr(lambda: store.authorize_operator(SECRET, USER))

    assert reason is RejectionReason.INVALID_CREDENTIAL
    assert "ERROR" in output
    assert str(secret_file) in output
    assert RejectionReason.INVALID_CREDENTIAL.value in output
    assert SECRET not in output


def test_unreadable_whitelist_file_refuses_with_an_error_log(store_files) -> None:
    """An unreadable whitelist refuses too, rather than accepting anybody."""
    secret_file, whitelist_file = store_files
    store = CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)

    whitelist_file.unlink()
    reason, output = _capturing_stderr(lambda: store.authorize_operator(SECRET, USER))

    assert reason is RejectionReason.INVALID_CREDENTIAL
    assert str(whitelist_file) in output


def test_a_transient_read_failure_heals_on_the_next_call(store_files) -> None:
    """Once the file is readable again the call is authorized once more."""
    secret_file, whitelist_file = store_files
    store = CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)
    content = secret_file.read_text()

    secret_file.unlink()
    assert store.authorize_operator(SECRET, USER) is RejectionReason.INVALID_CREDENTIAL

    secret_file.write_text(content)
    assert store.authorize_operator(SECRET, USER) is None


def test_unconfigured_secret_file_refuses_with_an_error_log() -> None:
    """No secret configured must not mean no secret required."""
    store = CredentialStore()

    reason, output = _capturing_stderr(lambda: store.authorize_operator(SECRET, USER))

    assert reason is RejectionReason.INVALID_CREDENTIAL
    assert "ERROR" in output
    assert "operator_secret_file" in output
