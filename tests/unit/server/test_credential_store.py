"""Unit tests for :class:`takler.server.auth.CredentialStore`.

Task 6.5 of the *m2-security* spec. What is pinned here is the file handling of
the store, which is where the operator-facing behaviour of the shared secret
lives:

* the parsing rule of a credential file, over the five shapes a real file takes
  -- one line, several lines, blank lines, ``#`` comments and lines padded with
  whitespace (Requirements 7.1, 7.2, 16.24);
* no-downtime rotation: with two lines both values verify, and removing one
  makes that value stop verifying (Requirements 7.12, 7.13, 16.23);
* hot reload: an edit takes effect on the next call, with no new store built
  and no restart (Requirement 7.6);
* the whitelist comparison being byte-exact -- case sensitive, no prefix or
  suffix match (Requirement 7.10);
* the four branches of ``validate_at_startup`` (Requirements 7.3, 7.4, 7.5,
  7.11) and the run-time refusal when the file is gone (Requirement 7.7).

Two conventions carried from the sibling tests in this directory. Log
assertions go through a captured console sink rather than ``caplog``, because
the logging backend does not route records into pytest's handler (same as
``test_credential_store_fail_closed.py``). And every secret used here is an
invented literal, asserted *absent* from the captured output wherever output is
captured at all, so no test can turn into a channel that prints credentials.

Hot reload is exercised by rewriting a file and then moving its mtime forward
explicitly. The store keys its cache on ``(st_mtime_ns, st_size)``, and two
writes inside the same test can land on the same timestamp on a filesystem
whose stat granularity is coarser than the write; bumping the mtime makes the
reload deterministic instead of dependent on the machine the suite runs on.

Validates: Requirements 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.10, 7.11, 7.12,
7.13, 16.23, 16.24
"""

from __future__ import annotations

import contextlib
import io
import os
import stat
from pathlib import Path
from typing import Callable, Optional, Tuple, TypeVar

import pytest

import takler.logging
from takler.exceptions import SecurityConfigError
from takler.server.auth import CredentialStore, RejectionReason
from takler.server.connect_config import AuthMode

#: Invented secret values. Nothing here is a real credential, and the tests
#: that capture log output assert these strings do not appear in it.
SECRET_OLD = "old-secret-0000"
SECRET_NEW = "new-secret-1111"

USER = "alice"

T = TypeVar("T")


def _capturing_stderr(func: Callable[[], T]) -> Tuple[T, str]:
    """Run ``func`` while capturing what the console log sink emits."""
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="DEBUG", console=True)
            result = func()
    finally:
        takler.logging.configure(console=True)
    return result, buffer.getvalue()


def _capturing_stderr_raises(
    func: Callable[[], object],
    expected: type,
) -> Tuple[BaseException, str]:
    """Run ``func`` expecting ``expected``, returning it with the log output."""
    raised: Optional[BaseException] = None

    def call() -> None:
        nonlocal raised
        with pytest.raises(expected) as excinfo:
            func()
        raised = excinfo.value

    _, output = _capturing_stderr(call)
    assert raised is not None
    return raised, output


def _rewrite(path: Path, text: str) -> None:
    """Replace the content of ``path`` and force its mtime to move forward.

    See the module docstring: the store re-reads on a fingerprint change, and
    the bump makes that change certain regardless of filesystem timestamp
    granularity.
    """
    previous = os.stat(path).st_mtime_ns if path.exists() else 0
    path.write_text(text)
    later = previous + 1_000_000_000
    os.utime(path, ns=(later, later))


def _secret_file(tmp_path: Path, text: str) -> Path:
    """Write a secret file with owner-only permissions."""
    path = tmp_path / "operator.secret"
    path.write_text(text)
    path.chmod(0o600)
    return path


# ---------------------------------------------------------------------------
# Parsing: the five shapes of a credential file (Requirements 7.1, 7.2, 16.24)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "shape, text, expected",
    [
        ("single line", f"{SECRET_OLD}\n", {SECRET_OLD}),
        (
            "several lines",
            f"{SECRET_OLD}\n{SECRET_NEW}\n",
            {SECRET_OLD, SECRET_NEW},
        ),
        (
            "blank lines",
            f"\n{SECRET_OLD}\n\n   \n\t\n{SECRET_NEW}\n\n",
            {SECRET_OLD, SECRET_NEW},
        ),
        (
            "comment lines",
            f"# round 1\n{SECRET_OLD}\n#{SECRET_NEW}\n   # indented comment\n",
            {SECRET_OLD},
        ),
        (
            "padded lines",
            f"  {SECRET_OLD}  \n\t{SECRET_NEW}\t\n",
            {SECRET_OLD, SECRET_NEW},
        ),
    ],
)
def test_secret_file_parsing_shapes(
    tmp_path: Path, shape: str, text: str, expected: set
) -> None:
    """Every non-blank, non-comment line yields one stripped value.

    The five parametrized shapes are the five cases Requirement 16.24 asks for.
    Note the comment case: a ``#`` immediately followed by a value comments the
    value out, which is how an operator retires a secret without deleting the
    line -- and it means the commented value must not verify.
    """
    store = CredentialStore(secret_file=_secret_file(tmp_path, text))

    content = store.read_secret_set()

    assert content.configured is True
    assert content.ok is True
    assert set(content.values) == expected
    for value in expected:
        assert store.verify_secret(value) is True


def test_whitelist_file_parsing_follows_the_same_rule(tmp_path: Path) -> None:
    """The whitelist file is parsed by the rule the secret file is."""
    whitelist_file = tmp_path / "operator.whitelist"
    whitelist_file.write_text("# operators\n  alice  \n\n#bob\ncarol\n")
    store = CredentialStore(whitelist_file=whitelist_file)

    content = store.read_whitelist()

    assert content.configured is True
    assert set(content.values) == {"alice", "carol"}
    assert store.is_whitelisted("bob") is False


def test_an_all_comment_file_holds_no_value(tmp_path: Path) -> None:
    """A file of only comments and blanks parses to an empty set.

    Which has to mean "verifies nothing" rather than "verifies anything": an
    empty Operator_Secret_Set is the fail-closed state.
    """
    store = CredentialStore(secret_file=_secret_file(tmp_path, "# nothing\n\n   \n"))

    assert set(store.read_secret_set().values) == set()
    assert store.verify_secret(SECRET_OLD) is False
    assert store.verify_secret("") is False


# ---------------------------------------------------------------------------
# Rotation (Requirements 7.12, 7.13, 16.23)
# ---------------------------------------------------------------------------


def test_two_line_secret_file_accepts_both_values(tmp_path: Path) -> None:
    """Both lines of a two-line secret file verify (Requirement 7.12)."""
    store = CredentialStore(
        secret_file=_secret_file(tmp_path, f"{SECRET_OLD}\n{SECRET_NEW}\n")
    )

    assert store.verify_secret(SECRET_OLD) is True
    assert store.verify_secret(SECRET_NEW) is True
    assert store.verify_secret("neither-of-them") is False


def test_removing_a_line_rejects_that_value_without_a_restart(tmp_path: Path) -> None:
    """The removed value stops being accepted on the next call.

    This is the last step of the rotation table in the design: the same store
    object, no restart, and the client left behind on the old secret is refused
    with the ``invalid_credential`` classification (Requirement 7.13).
    """
    secret_file = _secret_file(tmp_path, f"{SECRET_OLD}\n{SECRET_NEW}\n")
    whitelist_file = tmp_path / "operator.whitelist"
    whitelist_file.write_text(f"{USER}\n")
    store = CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)
    assert store.authorize_operator(SECRET_OLD, USER) is None

    _rewrite(secret_file, f"{SECRET_NEW}\n")

    assert store.verify_secret(SECRET_OLD) is False
    assert store.verify_secret(SECRET_NEW) is True
    assert (
        store.authorize_operator(SECRET_OLD, USER) is RejectionReason.INVALID_CREDENTIAL
    )
    assert store.authorize_operator(SECRET_NEW, USER) is None


# ---------------------------------------------------------------------------
# Hot reload (Requirement 7.6)
# ---------------------------------------------------------------------------


def test_secret_file_edits_take_effect_on_the_same_store(tmp_path: Path) -> None:
    """Appending and then replacing a secret needs no new store."""
    secret_file = _secret_file(tmp_path, f"{SECRET_OLD}\n")
    store = CredentialStore(secret_file=secret_file)
    assert store.verify_secret(SECRET_NEW) is False

    _rewrite(secret_file, f"{SECRET_OLD}\n{SECRET_NEW}\n")
    assert store.verify_secret(SECRET_NEW) is True

    _rewrite(secret_file, f"{SECRET_NEW}\n")
    assert store.verify_secret(SECRET_OLD) is False


def test_whitelist_edits_take_effect_on_the_same_store(tmp_path: Path) -> None:
    """Adding an authorized user does not require restarting the server."""
    whitelist_file = tmp_path / "operator.whitelist"
    whitelist_file.write_text(f"{USER}\n")
    store = CredentialStore(whitelist_file=whitelist_file)
    assert store.is_whitelisted("dave") is False

    _rewrite(whitelist_file, f"{USER}\ndave\n")
    assert store.is_whitelisted("dave") is True

    _rewrite(whitelist_file, f"{USER}\n")
    assert store.is_whitelisted("dave") is False


def test_a_file_created_after_the_store_is_picked_up(tmp_path: Path) -> None:
    """A store built before its file exists reads it once it appears.

    Construction never touches the filesystem, so the first read simply fails
    and is not cached; the following one succeeds.
    """
    secret_file = tmp_path / "operator.secret"
    store = CredentialStore(secret_file=secret_file)
    assert store.read_secret_set().ok is False

    secret_file.write_text(f"{SECRET_OLD}\n")

    assert store.verify_secret(SECRET_OLD) is True


# ---------------------------------------------------------------------------
# Whitelist comparison (Requirement 7.10)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "candidate",
    [
        "Alice",  # case folding would accept this
        "ALICE",
        "ali",  # a prefix of the listed name
        "alice2",  # the listed name plus a suffix
        "alicealice",
        " alice",  # untrimmed, unlike the file's own lines
        "alice ",
        "",
    ],
)
def test_whitelist_is_byte_exact(tmp_path: Path, candidate: str) -> None:
    """Only the exact listed name is whitelisted.

    Case folding would hand ``alice``'s authority to ``Alice`` -- POSIX user
    names are case sensitive -- and a prefix match would hand it to ``alice2``.
    The file's own lines are stripped when parsed, but a candidate is not: it
    arrives from the metadata and is compared as it came.
    """
    whitelist_file = tmp_path / "operator.whitelist"
    whitelist_file.write_text(f"{USER}\n")
    store = CredentialStore(whitelist_file=whitelist_file)

    assert store.is_whitelisted(USER) is True
    assert store.is_whitelisted(candidate) is False


def test_no_whitelist_file_accepts_any_user_name(tmp_path: Path) -> None:
    """With no whitelist configured, the secret alone authorizes."""
    store = CredentialStore(secret_file=_secret_file(tmp_path, f"{SECRET_OLD}\n"))

    assert store.is_whitelisted(USER) is True
    assert store.is_whitelisted("mallory") is True


# ---------------------------------------------------------------------------
# Startup validation: the four branches (Requirements 7.3, 7.4, 7.5, 7.11)
# ---------------------------------------------------------------------------


def test_startup_refuses_when_no_secret_file_is_configured() -> None:
    """Enabled with no secret file at all is fatal (Requirement 7.3)."""
    store = CredentialStore()

    error, output = _capturing_stderr_raises(
        lambda: store.validate_at_startup(AuthMode.ENABLED),
        SecurityConfigError,
    )

    assert "operator_secret_file" in str(error)
    assert "ERROR" in output
    assert "operator_secret_file" in output


def test_startup_refuses_when_the_secret_file_is_missing(tmp_path: Path) -> None:
    """A configured but absent secret file is fatal (Requirement 7.4)."""
    secret_file = tmp_path / "absent.secret"
    store = CredentialStore(secret_file=secret_file)

    error, output = _capturing_stderr_raises(
        lambda: store.validate_at_startup(AuthMode.ENABLED),
        SecurityConfigError,
    )

    assert str(secret_file) in str(error)
    assert "ERROR" in output
    assert str(secret_file) in output


def test_startup_refuses_when_the_secret_file_holds_no_value(tmp_path: Path) -> None:
    """A secret file of only blanks and comments is fatal (Requirement 7.4).

    Starting would leave a server that refuses every Operator_Command, which
    reads as a client bug rather than as the misconfiguration it is.
    """
    store = CredentialStore(
        secret_file=_secret_file(tmp_path, "# retired\n#" + SECRET_OLD + "\n\n")
    )

    error, output = _capturing_stderr_raises(
        lambda: store.validate_at_startup(AuthMode.ENABLED),
        SecurityConfigError,
    )

    assert "no operator secret" in str(error)
    assert "ERROR" in output
    assert SECRET_OLD not in output


def test_startup_warns_when_no_whitelist_file_is_configured(tmp_path: Path) -> None:
    """An absent whitelist warns and starts (Requirement 7.5)."""
    store = CredentialStore(secret_file=_secret_file(tmp_path, f"{SECRET_OLD}\n"))

    _, output = _capturing_stderr(lambda: store.validate_at_startup(AuthMode.ENABLED))

    assert "WARNING" in output
    assert "operator_whitelist_file" in output
    assert SECRET_OLD not in output


def test_startup_warns_on_a_world_readable_secret_file(tmp_path: Path) -> None:
    """Mode 0644 warns with the path and the offending bits, and starts on.

    A warning rather than a refusal: the owner may have widened the group bits
    on purpose to share the secret with a second operator account, and a mode
    bit should not strand a server whose authentication is otherwise sound
    (Requirement 7.11).
    """
    secret_file = _secret_file(tmp_path, f"{SECRET_OLD}\n")
    whitelist_file = tmp_path / "operator.whitelist"
    whitelist_file.write_text(f"{USER}\n")
    secret_file.chmod(0o644)
    store = CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)

    _, output = _capturing_stderr(lambda: store.validate_at_startup(AuthMode.ENABLED))

    assert "WARNING" in output
    assert str(secret_file) in output
    assert "0644" in output
    assert SECRET_OLD not in output


def test_startup_is_quiet_for_an_owner_only_secret_file(tmp_path: Path) -> None:
    """Mode 0600 plus a whitelist produces no WARNING at all."""
    secret_file = _secret_file(tmp_path, f"{SECRET_OLD}\n")
    whitelist_file = tmp_path / "operator.whitelist"
    whitelist_file.write_text(f"{USER}\n")
    store = CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)

    _, output = _capturing_stderr(lambda: store.validate_at_startup(AuthMode.ENABLED))

    assert "WARNING" not in output
    assert stat.S_IMODE(os.stat(secret_file).st_mode) == 0o600


def test_startup_validates_nothing_when_authentication_is_disabled(
    tmp_path: Path,
) -> None:
    """An M1 deployment with no credential file keeps starting unchanged."""
    store = CredentialStore()

    _, output = _capturing_stderr(lambda: store.validate_at_startup(AuthMode.DISABLED))

    assert "ERROR" not in output
    assert "WARNING" not in output


# ---------------------------------------------------------------------------
# Run-time deletion (Requirement 7.7)
# ---------------------------------------------------------------------------


def test_secret_file_deleted_at_run_time_refuses_and_logs_an_error(
    tmp_path: Path,
) -> None:
    """A secret file removed while serving refuses with an ERROR, never raises.

    An exception escaping the Auth_Interceptor would reach the client as
    ``UNKNOWN`` rather than as ``PERMISSION_DENIED``, so the store answers with
    a classification. Refusing rather than passing is the only safe reading of
    "the server cannot check right now".
    """
    secret_file = _secret_file(tmp_path, f"{SECRET_OLD}\n")
    whitelist_file = tmp_path / "operator.whitelist"
    whitelist_file.write_text(f"{USER}\n")
    store = CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)
    # Fill the fingerprint cache first, so the refusal cannot be an artifact of
    # the file having never been read successfully.
    assert store.authorize_operator(SECRET_OLD, USER) is None

    secret_file.unlink()
    reason, output = _capturing_stderr(
        lambda: store.authorize_operator(SECRET_OLD, USER)
    )

    assert reason is RejectionReason.INVALID_CREDENTIAL
    assert "ERROR" in output
    assert str(secret_file) in output
    assert RejectionReason.INVALID_CREDENTIAL.value in output
    assert SECRET_OLD not in output


def test_the_cached_secret_set_is_dropped_when_the_file_disappears(
    tmp_path: Path,
) -> None:
    """``verify_secret`` stops accepting the value a deleted file carried."""
    secret_file = _secret_file(tmp_path, f"{SECRET_OLD}\n")
    store = CredentialStore(secret_file=secret_file)
    assert store.verify_secret(SECRET_OLD) is True

    secret_file.unlink()

    assert store.verify_secret(SECRET_OLD) is False
