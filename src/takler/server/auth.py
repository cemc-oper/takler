"""Authentication support for the Takler server.

This module owns the server side of the authentication contract: the
Operator_Secret_Set and the Operator_Whitelist (:class:`CredentialStore`), the
method-name privilege table, the per-call credentials taken from the gRPC
metadata and the Auth_Interceptor that applies them.

Four pieces live here: :class:`CredentialStore` with its file handling, the
method-name privilege table (:class:`PrivilegeLevel`,
:data:`PRIVILEGE_BY_METHOD`, :func:`privilege_for_method`), the per-call
credentials (:class:`CallCredentials` plus the context variable that carries
them from the interceptor to the Zombie_Detector and to the Audit_Logger), and
:class:`AuthInterceptor`, which joins the three into the single check every RPC
passes through.

Everything but the interceptor deliberately depends on nothing outside the
standard library and :mod:`takler.logging` -- not even on :mod:`grpc` -- so the
parsing, the hot-reload behaviour, the privilege lookup and the metadata
parsing can all be tested without standing up a gRPC server. Only
:class:`AuthInterceptor` needs :mod:`grpc`, for the base class, the status codes
and the abort handler it returns.

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9, 6.10, 6.11, 6.12,
6.13, 7.1, 7.2, 7.3,
7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 7.10, 7.11, 7.12, 7.13, 11.3, 11.7, 11.8, 12.1,
12.3.
"""

from __future__ import annotations

import contextvars
import dataclasses
import enum
import hmac
import os
import stat
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import grpc

from takler.exceptions import SecurityConfigError
from takler.logging import get_logger
from takler.server.audit import (
    DENIED_ERROR_CODE,
    EVENT_DENIED,
    OUTCOME_DENIED,
    AuditLogger,
    AuditRecord,
    audit_command_name,
    audit_peer,
    audit_timestamp,
)
from takler.server.connect_config import DEFAULT_AUTH_MODE, AuthMode

__all__ = [
    "AUDIT_UNKNOWN_USER",
    "COMMENT_PREFIX",
    "CREDENTIAL_METADATA_KEYS",
    "MAX_ECHOED_LENGTH",
    "METADATA_KEY_JOB_PASSWORD",
    "METADATA_KEY_SECRET",
    "METADATA_KEY_USER",
    "MIN_REDACTED_LENGTH",
    "PRIVILEGE_BY_METHOD",
    "REDACTED",
    "SERVICE_METHOD_PREFIX",
    "STATUS_CODE_BY_REJECTION",
    "TRUNCATION_MARKER",
    "AuthInterceptor",
    "CallCredentials",
    "CredentialFileContent",
    "CredentialStore",
    "PrivilegeLevel",
    "RejectionReason",
    "compare_secret_values",
    "get_call_credentials",
    "privilege_for_method",
    "reset_call_credentials",
    "sanitize_echoed_value",
    "set_call_credentials",
]

logger = get_logger("server.auth")


#: Line prefix that marks a comment in the Operator_Secret_File and in the
#: Operator_Whitelist_File. A commented line is dropped before the value is
#: taken, so an operator can annotate which secret belongs to which rotation
#: round without that annotation becoming a secret of its own
#: (Requirements 7.1, 7.2).
COMMENT_PREFIX: str = "#"

#: Content fingerprint of a credential file: ``(st_mtime_ns, st_size)``.
#:
#: Nanosecond precision rather than :attr:`os.stat_result.st_mtime`: second
#: precision would miss two edits landing within the same second, which is
#: exactly what happens when a rotation script appends a secret and then
#: removes the old one. The size is compared alongside it because it is free
#: and catches a same-timestamp edit that changes the length.
Fingerprint = Tuple[int, int]


def _is_blank(value: Optional[str]) -> bool:
    """Return ``True`` when a configured path counts as "not provided".

    Empty and whitespace-only strings are treated as absent, so a
    ``connect.yaml`` holding ``operator_secret_file: ""`` reads as "no secret
    file configured" instead of resolving to a nameless path. This mirrors the
    same helper in :mod:`takler.server.connect_config` and
    :mod:`takler.server.checkpoint`.
    """
    return value is None or value.strip() == ""


def parse_credential_lines(text: str) -> FrozenSet[str]:
    """Parse the text of a credential file into its set of values.

    A value is taken from every line that is neither blank nor a comment, with
    the surrounding whitespace removed. Both credential files share this rule:
    the values of the Operator_Secret_File form the Operator_Secret_Set and the
    values of the Operator_Whitelist_File form the Operator_Whitelist
    (Requirements 7.1, 7.2).

    Every value of the secret file is a valid Operator_Secret -- not just the
    first one. That is the whole of the no-downtime rotation mechanism: the
    server accepts a set while a client sends a single value, so a new secret
    can be appended, the clients updated one by one, and the old secret removed
    afterwards (Requirement 7.12).

    Duplicated lines collapse, since the result is a set. Verifying a candidate
    against the same value twice would only make the check slower.

    Args:
        text: The full content of a credential file.

    Returns:
        The set of values the file carries, possibly empty.
    """
    values = set()
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith(COMMENT_PREFIX):
            continue
        values.add(stripped)
    return frozenset(values)


#: Signature of the secret comparison used by
#: :meth:`CredentialStore.verify_secret`: it takes the encoded candidate and one
#: encoded Operator_Secret and answers whether they are equal.
SecretComparison = Callable[[bytes, bytes], bool]


def _encode_secret(value: str) -> bytes:
    """Encode a credential value to the byte form the comparison works on.

    :func:`hmac.compare_digest` accepts two :class:`str` operands only when both
    are ASCII, and refuses to mix :class:`str` with :class:`bytes` at all. Both
    restrictions are reachable here: a secret is whatever an operator put in the
    file, and a candidate is whatever a client sent. Encoding both sides to
    UTF-8 removes the type question entirely and keeps the comparison
    byte-exact, since equal text encodes to equal bytes and unequal text does
    not.

    ``surrogatepass`` is used so that no :class:`str` can make this raise. A
    lone surrogate cannot be encoded to UTF-8 by the strict handler, and it can
    reach here from a metadata value that arrived as text; a
    :exc:`UnicodeEncodeError` escaping the verification would surface as an
    ``UNKNOWN`` status code instead of the intended ``PERMISSION_DENIED``.
    """
    return value.encode("utf-8", errors="surrogatepass")


def compare_secret_values(candidate: bytes, secret: bytes) -> bool:
    """Compare one candidate against one Operator_Secret in constant time.

    This is a one-line wrapper on :func:`hmac.compare_digest`, and it exists for
    two reasons.

    The first is Requirement 7.8: the comparison must not short-circuit on the
    first differing byte, otherwise the response time of a rejected
    Operator_Command reveals how long a prefix the caller guessed right, which
    turns guessing a secret from an exponential search into a linear one.

    The second is that it is the seam Requirement 7.9 is tested through. The
    property test injects a counting stand-in for this function and asserts that
    one :meth:`CredentialStore.verify_secret` call performs exactly
    ``len(Operator_Secret_Set)`` comparisons, whatever the candidate is and
    wherever in the file it matches. That assertion is what keeps a later
    refactor from "optimizing" the loop with a ``break``; without an
    indirection to substitute, the count would not be observable from a test.

    Args:
        candidate: The candidate value, already encoded by
            :func:`_encode_secret`.
        secret: One Operator_Secret, already encoded the same way.

    Returns:
        Whether the two byte strings are equal.
    """
    return hmac.compare_digest(candidate, secret)


@dataclasses.dataclass(frozen=True)
class CredentialFileContent:
    """The outcome of reading one credential file.

    Reading a credential file has three distinguishable outcomes, and every
    caller has to tell them apart, so they are reported as data rather than
    through an exception:

    * not configured (:attr:`configured` is ``False``) -- an absent
      Operator_Whitelist_File means every user name holding a valid secret is
      accepted (Requirement 7.5), while an absent Operator_Secret_File is a
      startup error when authentication is enabled (Requirement 7.3);
    * read failure (:attr:`error` is set) -- the file was configured but could
      not be stat-ed, read or decoded, which must be answered fail-closed
      (Requirement 7.7);
    * success -- :attr:`values` holds the parsed set, possibly empty.

    :attr:`values` is empty whenever the read failed, so a caller that only
    looks at :attr:`values` still fails closed: an empty Operator_Secret_Set
    verifies no candidate at all.

    Attributes:
        configured: Whether a path was configured for this file.
        values: The parsed values, empty when not configured or on failure.
        error: A human readable failure reason, or ``None`` when the read
            succeeded or no path is configured. The text names the failing path
            and the underlying cause, and never contains file content, so it is
            safe to log.
    """

    configured: bool
    values: FrozenSet[str] = frozenset()
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        """Whether the file was read successfully, or is not configured."""
        return self.error is None


class _CredentialFile:
    """One credential file together with its fingerprint keyed cache.

    The file is re-read only when its ``(st_mtime_ns, st_size)`` fingerprint
    differs from the cached one, which is what makes an edit take effect
    without restarting the server (Requirement 7.6): an operator adding an
    authorized user saves the file and the next RPC sees it. Requiring a
    restart to add one user is what gets an authorization mechanism bypassed in
    practice.

    ``stat()`` is an order of magnitude cheaper than reading and parsing, and
    Operator_Commands are human driven anyway, so the cache is about keeping
    the check cheap rather than about a measured bottleneck. Child_Commands
    never touch these files at all.

    Reading never raises: a failure is returned as a
    :class:`CredentialFileContent` carrying :attr:`~CredentialFileContent.error`
    instead. Nor is it logged here -- the caller knows whether it is validating
    at startup (where a failure is fatal) or serving an RPC (where it turns
    into a fail-closed rejection), and logging in both places would duplicate
    every record.

    A failed read is not cached: the fingerprint is cleared so the next call
    retries. A file that is momentarily unreadable -- being replaced by a
    rotation script, or on a network filesystem hiccup -- therefore recovers on
    its own once the read succeeds again.
    """

    def __init__(self, path: Optional[Union[str, Path]], description: str) -> None:
        """Bind the file to a path, without touching the filesystem.

        No read happens here: constructing a :class:`CredentialStore` must not
        fail because of a file that is not there yet, and the startup
        validation is a separate, explicit step.

        Args:
            path: The configured path, or ``None`` / blank when the file is not
                configured.
            description: A short human readable name of this file, used in
                failure reasons, for example ``"operator secret file"``.
        """
        # ``Path("")`` normalizes to ``.``, so a blank path is filtered out on
        # the text form before it becomes a Path.
        raw = None if path is None else os.fspath(path)
        self._path: Optional[Path] = None if _is_blank(raw) else Path(raw)
        self._description = description
        self._fingerprint: Optional[Fingerprint] = None
        self._values: FrozenSet[str] = frozenset()

    @property
    def path(self) -> Optional[Path]:
        """The configured path, or ``None`` when the file is not configured."""
        return self._path

    @property
    def configured(self) -> bool:
        """Whether a path was configured for this file."""
        return self._path is not None

    @property
    def description(self) -> str:
        """A short human readable name of this file, used in messages."""
        return self._description

    def read(self) -> CredentialFileContent:
        """Return the current content of the file, re-reading it when it changed.

        Returns:
            A :class:`CredentialFileContent` describing one of the three
            outcomes: not configured, read failure, or the parsed value set.
        """
        if self._path is None:
            return CredentialFileContent(configured=False)

        try:
            stat_result = os.stat(self._path)
        except OSError as exc:
            return self._failure(exc)

        fingerprint: Fingerprint = (stat_result.st_mtime_ns, stat_result.st_size)
        if fingerprint == self._fingerprint:
            return CredentialFileContent(configured=True, values=self._values)

        try:
            text = self._path.read_text()
        except (OSError, UnicodeDecodeError) as exc:
            return self._failure(exc)

        self._values = parse_credential_lines(text)
        self._fingerprint = fingerprint
        # The count is safe to log, the values are not: a secret value must
        # never reach any log (Requirement 12.1).
        logger.debug(
            f"reloaded {self._description} {self._path}: {len(self._values)} value(s)"
        )
        return CredentialFileContent(configured=True, values=self._values)

    def _failure(self, exc: BaseException) -> CredentialFileContent:
        """Drop the cache and describe a read failure.

        The cached fingerprint is cleared so the next :meth:`read` retries
        rather than serving a stale value set: if the file is gone or its
        permissions changed, the previously parsed secrets must stop being
        accepted.
        """
        self._fingerprint = None
        self._values = frozenset()
        reason = f"{type(exc).__name__}: {exc}"
        return CredentialFileContent(
            configured=True,
            error=f"cannot read {self._description} {self._path}: {reason}",
        )


class RejectionReason(enum.Enum):
    """Why an Operator_Command was refused (Requirement 6.11).

    The three values are the whole vocabulary of a refusal: the classification
    is what reaches the rejection WARNING, the Audit_Record and the gRPC abort
    details, and it is all that reaches them -- no credential value is ever
    echoed back (Requirements 6.10, 6.12, 12.1).

    The split is by *what the caller has to do about it*, not by which check
    failed:

    * :attr:`MISSING_CREDENTIAL`: nothing was presented, which for a client is
      "you are not configured yet" -- a missing ``TAKLER_SECRET_FILE``, an
      un-upgraded client against an ``enabled`` server. It maps to
      ``UNAUTHENTICATED``.
    * :attr:`INVALID_CREDENTIAL`: something was presented and it does not hold,
      which is "your secret is stale" -- typically a client left behind by a
      rotation (Requirement 7.13). A credential file that cannot be read at
      run time lands here too: the server cannot tell whether the caller's
      secret is valid, and answering "not valid" is the fail-closed answer
      (Requirement 7.7).
    * :attr:`NOT_IN_WHITELIST`: the secret held but this user name is not
      authorized, which is "ask the operator to add you" and needs no secret
      rotation at all.

    ``UNAUTHENTICATED`` for the first and ``PERMISSION_DENIED`` for the other
    two is the mapping the Auth_Interceptor applies; the status codes are not
    named here so that this module keeps needing nothing from :mod:`grpc`.

    The values are the exact classification strings of the Cross-Language
    Contract, so a log line, an audit record and a Go client all spell a
    refusal the same way.
    """

    MISSING_CREDENTIAL = "missing_credential"
    INVALID_CREDENTIAL = "invalid_credential"
    NOT_IN_WHITELIST = "not_in_whitelist"


class CredentialStore:
    """Holds the Operator_Secret_Set and the Operator_Whitelist.

    Both are backed by a file that is re-read whenever its
    ``(st_mtime_ns, st_size)`` fingerprint changes, so editing either file
    takes effect on the following RPC without restarting the server
    (Requirement 7.6). The parsing rule is the same for both files: every line
    that is neither blank nor a comment yields one value, stripped of its
    surrounding whitespace (Requirements 7.1, 7.2).

    All values of the secret file are accepted, which is what lets an operator
    rotate the shared secret without a downtime window (Requirement 7.12).

    Neither accessor raises on a missing or unreadable file; they report the
    failure through :attr:`CredentialFileContent.error` instead.
    :meth:`authorize_operator`, which is what the Auth_Interceptor calls, turns
    such a failure into an ERROR log plus a fail-closed
    :attr:`RejectionReason.INVALID_CREDENTIAL` rather than letting an exception
    escape an interceptor, where it would degrade into an ``UNKNOWN`` status
    code (Requirement 7.7).

    Nothing here logs a secret value; only paths and counts (Requirement 12.1).
    """

    def __init__(
        self,
        secret_file: Optional[Union[str, Path]] = None,
        whitelist_file: Optional[Union[str, Path]] = None,
    ) -> None:
        """Bind the store to its two files, without reading them.

        Construction never touches the filesystem and never fails, so a server
        can build the store before deciding whether authentication is enabled
        at all. Whether the files must exist is a separate, explicit startup
        check.

        Args:
            secret_file: Operator_Secret_File path. ``None`` or blank means no
                secret file is configured.
            whitelist_file: Operator_Whitelist_File path. ``None`` or blank
                means no whitelist is configured, in which case any user name
                holding a valid secret is accepted (Requirement 7.5).
        """
        self._secret = _CredentialFile(secret_file, "operator secret file")
        self._whitelist = _CredentialFile(whitelist_file, "operator whitelist file")

    @property
    def secret_file(self) -> Optional[Path]:
        """The configured Operator_Secret_File path, or ``None``."""
        return self._secret.path

    @property
    def whitelist_file(self) -> Optional[Path]:
        """The configured Operator_Whitelist_File path, or ``None``."""
        return self._whitelist.path

    def read_secret_set(self) -> CredentialFileContent:
        """Return the current Operator_Secret_Set.

        The file is re-read only when its fingerprint changed; otherwise the
        cached set is returned (Requirements 7.1, 7.6, 7.12).

        Returns:
            A :class:`CredentialFileContent` whose ``values`` hold every
            currently valid Operator_Secret. ``values`` is empty when the file
            is not configured or could not be read, so a caller comparing
            against it fails closed.
        """
        return self._secret.read()

    def read_whitelist(self) -> CredentialFileContent:
        """Return the current Operator_Whitelist.

        The file is re-read only when its fingerprint changed; otherwise the
        cached set is returned (Requirements 7.2, 7.6).

        Returns:
            A :class:`CredentialFileContent` whose ``values`` hold the
            whitelisted OS user names. ``configured`` is ``False`` when no
            whitelist file is configured, which callers read as "accept any
            user name that presents a valid secret" (Requirement 7.5).
        """
        return self._whitelist.read()

    def verify_secret(
        self,
        candidate: Optional[str],
        compare: Optional[SecretComparison] = None,
    ) -> bool:
        """Whether ``candidate`` is one of the current Operator_Secrets.

        Every value of the Operator_Secret_Set is compared with
        :func:`compare_secret_values`, so no comparison short-circuits on the
        first differing byte (Requirement 7.8), and *every* value is compared
        even after one has matched: the result is accumulated with ``|=`` and
        there is no ``break`` and no early ``return`` inside the loop
        (Requirement 7.9).

        **The missing ``break`` is the point, not an oversight.** Stopping at the
        match would make the verification time depend on which line of the
        Operator_Secret_File matched. That leaks no secret value, but during a
        rotation it does leak which clients are still presenting the old secret
        -- exactly the information an attacker wants in order to know whether a
        secret they hold is still live. The whole cost of removing that channel
        is not writing ``break``, which is why the property test pins the
        comparison count down (see :func:`compare_secret_values`).

        The set is re-read first when the file changed, so a rotation takes
        effect without a restart: appending a value makes it valid immediately
        and removing one makes it stop being accepted (Requirements 7.6, 7.12,
        7.13).

        Verification fails closed. An empty Operator_Secret_Set accepts nothing,
        which covers both "no secret file is configured" and "the file could not
        be read at all" -- the loop simply does not run and the result stays
        ``False``. Turning a read failure into an ERROR log and a rejection
        classification is the caller's job (Requirement 7.7), not this method's.

        Nothing here can raise. A candidate of the wrong type is rejected before
        the loop and a candidate that is not encodable by the strict UTF-8
        handler is still encoded (see :func:`_encode_secret`), because an
        exception leaving the Auth_Interceptor would reach the client as
        ``UNKNOWN`` rather than as ``PERMISSION_DENIED``.

        Args:
            candidate: The ``takler-secret`` value presented by the caller.
                ``None`` -- no secret was carried -- yields ``False``.
            compare: The per-value comparison, defaulting to
                :func:`compare_secret_values`. Resolved at call time, so a test
                may inject it here or patch the module-level default; both
                routes work and neither changes the production path.

        Returns:
            Whether ``candidate`` equals one of the current Operator_Secrets.
        """
        if compare is None:
            compare = compare_secret_values

        if isinstance(candidate, bytes):
            # Not produced by the metadata parsing, which normalizes to text,
            # but accepted rather than refused: the value is what matters and
            # rejecting on type here would be a silent authentication failure.
            candidate_bytes = candidate
        elif isinstance(candidate, str):
            candidate_bytes = _encode_secret(candidate)
        else:
            return False

        matched = False
        for secret in self.read_secret_set().values:
            matched |= compare(candidate_bytes, _encode_secret(secret))
        return matched

    def is_whitelisted(self, user: Optional[str]) -> bool:
        """Whether ``user`` may run an Operator_Command.

        The comparison is byte-exact: no case folding, no prefix or suffix
        matching, no trimming (Requirement 7.10). POSIX user names are case
        sensitive, so folding would hand ``alice``'s authority to ``Alice``, and
        a prefix match would hand it to ``alice2``. Comparing whole strings for
        equality is the same as comparing whole byte sequences, since equal text
        encodes to equal bytes.

        Unlike :meth:`verify_secret` this needs no constant-time comparison: a
        user name is not a secret. It travels in the clear in the metadata and
        appears in every audit record, so there is nothing for a timing channel
        to reveal.

        When no Operator_Whitelist_File is configured this is always ``True``:
        an absent whitelist means any user name presenting a valid secret is
        accepted (Requirement 7.5), so authorization rests on the secret alone.
        That includes a ``None`` user -- whether ``takler-user`` was carried at
        all is checked separately by the Auth_Interceptor, which rejects a
        missing one as ``missing_credential`` before the whitelist is consulted
        (Requirement 6.5), and duplicating that check here would report a
        missing key as ``not_in_whitelist``.

        A configured but unreadable whitelist yields an empty value set, so no
        user matches and the check fails closed on its own.

        Args:
            user: The ``takler-user`` value presented by the caller.

        Returns:
            Whether ``user`` belongs to the Operator_Whitelist, or ``True`` when
            no whitelist file is configured.
        """
        content = self.read_whitelist()
        if not content.configured:
            return True
        if user is None:
            return False
        return user in content.values

    def authorize_operator(
        self,
        secret: Optional[str],
        user: Optional[str],
        compare: Optional[SecretComparison] = None,
    ) -> Optional[RejectionReason]:
        """Decide whether an Operator_Command may proceed.

        This is the run-time counterpart of :meth:`validate_at_startup` and the
        single entry point the Auth_Interceptor uses for an ``OPERATOR`` level
        method: it answers with a classification instead of a status code, so
        this module stays free of :mod:`grpc`, and the interceptor maps
        :attr:`RejectionReason.MISSING_CREDENTIAL` to ``UNAUTHENTICATED`` and
        the other two to ``PERMISSION_DENIED``.

        The checks run in this order, and the first failure decides:

        ================================================= =========================
        Situation                                         Result
        ================================================= =========================
        no ``takler-secret`` or no ``takler-user``         ``MISSING_CREDENTIAL``
        a credential file cannot be read                   ERROR + ``INVALID_CREDENTIAL``
        no Operator_Secret_File configured                 ERROR + ``INVALID_CREDENTIAL``
        the secret is not in the Operator_Secret_Set       ``INVALID_CREDENTIAL``
        the user is not in the Operator_Whitelist          ``NOT_IN_WHITELIST``
        otherwise                                          ``None`` -- authorized
        ================================================= =========================

        Presence is checked before the files are touched so that an
        unconfigured client gets ``UNAUTHENTICATED`` -- "you carry no
        credentials" -- rather than a report about the server's own files, and
        so that a caller who sends nothing cannot make the server read two
        files per attempt.

        **Nothing here raises, by construction (Requirement 7.7).** An
        exception leaving an interceptor does not become the status code it
        describes; it becomes ``UNKNOWN``, which tells the client nothing and
        tells the Client_CLI to treat a refusal as a server fault. So a file
        that has been deleted, chmod-ed away or replaced mid-flight while the
        server runs is answered the same way as a wrong secret: an ERROR naming
        the path and the reason, and a refusal. Refusing rather than passing is
        the only safe reading of "the server cannot check right now" -- the
        alternative would turn ``rm`` on the secret file, or an NFS hiccup, into
        an open server.

        An unreadable file is not cached as empty either: the fingerprint is
        dropped on failure (see :class:`_CredentialFile`), so the next
        Operator_Command re-reads and a transient failure heals by itself
        without a restart.

        A missing Operator_Secret_File *path* is reported the same way even
        though :meth:`validate_at_startup` already refuses to start in that
        case. It stays reachable for a store built outside the server startup
        path, and "no secret is configured" must never mean "no secret is
        required".

        The ERROR is emitted per rejected RPC rather than once per failure
        state. A deployment where this fires repeatedly is one where every
        Operator_Command is being refused, which is worth a line each: the
        record is what tells the operator that the cause is the server's own
        file rather than their secret.

        Args:
            secret: The ``takler-secret`` value the caller presented, or
                ``None`` when the key was absent or blank.
            user: The ``takler-user`` value the caller presented, or ``None``.
            compare: The per-value comparison handed to
                :meth:`verify_secret`, for the property test that counts
                comparisons. Defaults to :func:`compare_secret_values`.

        Returns:
            ``None`` when the call is authorized, otherwise the
            :class:`RejectionReason` to report.
        """
        if secret is None or user is None:
            return RejectionReason.MISSING_CREDENTIAL

        # Both files are read before either verdict is formed: whichever of
        # them is unreadable, the answer is the same refusal, and reading both
        # means the ERROR names the file that is actually broken instead of
        # only the first one consulted. The reads are fingerprint-cached, so
        # this costs two ``stat`` calls in the normal case.
        secret_content = self.read_secret_set()
        whitelist_content = self.read_whitelist()

        unreadable = False
        for content in (secret_content, whitelist_content):
            if content.error is not None:
                # The message already carries the path and the underlying
                # reason, and carries no file content (Requirement 7.7).
                logger.error(
                    f"{content.error}; refusing the operator command "
                    f"({RejectionReason.INVALID_CREDENTIAL.value})."
                )
                unreadable = True
        if unreadable:
            return RejectionReason.INVALID_CREDENTIAL

        if not secret_content.configured:
            logger.error(
                f"No {self._secret.description} is configured; refusing the "
                f"operator command "
                f"({RejectionReason.INVALID_CREDENTIAL.value}). Set the "
                f"operator_secret_file item of the security section, or "
                f"disable authentication."
            )
            return RejectionReason.INVALID_CREDENTIAL

        if not self.verify_secret(secret, compare=compare):
            return RejectionReason.INVALID_CREDENTIAL

        if not self.is_whitelisted(user):
            return RejectionReason.NOT_IN_WHITELIST

        return None

    def validate_at_startup(
        self,
        auth_mode: AuthMode = DEFAULT_AUTH_MODE,
    ) -> None:
        """Check the credential files before the server starts serving.

        Four situations are distinguished (Requirements 7.3, 7.4, 7.5, 7.11);
        the first two are fatal and the last two only produce a WARNING:

        =============================================== ==========================
        Situation                                       Outcome
        =============================================== ==========================
        ``enabled`` and no Operator_Secret_File          ERROR + raise (7.3)
        ``enabled`` and it is missing / unreadable /     ERROR + raise (7.4)
        holds no value
        ``enabled`` and no Operator_Whitelist_File       WARNING (7.5)
        Operator_Secret_File readable or writable        WARNING (7.11)
        beyond its owner
        =============================================== ==========================

        **Why this is fatal where an unparseable Auth_Mode is not.** A bad
        ``auth_mode`` string degrades to ``disabled``, and a ``disabled`` server
        announces itself as unauthenticated in its own startup log, so nobody
        misreads the posture. Here the operator has explicitly asked for
        authentication, and the only ways to continue are both worse than
        refusing to start: serving with an empty Operator_Secret_Set would
        reject *every* Operator_Command, which looks like a client bug rather
        than a server misconfiguration, and falling back to ``disabled`` would
        leave a server the operator believes is authenticated wide open. A
        non-zero exit at startup is the one outcome that cannot be misread --
        which is why :class:`~takler.exceptions.SecurityConfigError` is raised
        rather than returned; ``takler-server`` turns it into exit code 1
        (Requirement 1.6).

        Checking here rather than on the first RPC also matters: a deployment
        error surfaces when the operator is watching the server come up, not
        hours later inside somebody else's ``takler-client`` invocation.

        The permission WARNING is emitted whatever the Auth_Mode is: a
        world-readable secret file is worth reporting even on a server that is
        not yet enforcing authentication, since the same file will be used once
        it does. It is a warning and not an error because the owner may
        legitimately have widened the group bits to share the secret with a
        second operator account, and because refusing to start over a mode bit
        would strand a server whose authentication is otherwise sound.

        Nothing is validated when authentication is off apart from those
        permissions -- an M1 deployment that configures no credential file at
        all must keep starting unchanged.

        A whitelist file that is configured but unreadable is deliberately not
        fatal: it is read again on every RPC, so the run-time fail-closed path
        (Requirement 7.7) already rejects every Operator_Command with an ERROR
        naming the path, and a transient failure at startup -- an NFS mount not
        up yet -- should not keep the server from coming up.

        Args:
            auth_mode: The resolved Auth_Mode. Passed in rather than resolved
                here because the resolution is the caller's business: the
                server has already applied the "argument > environment >
                Connect_Config > default" order (Requirement 3.5) and this
                store does not know about any of those sources.

        Raises:
            SecurityConfigError: Authentication is enabled but the
                Operator_Secret_File is unusable. The message names the
                configuration item or the path plus the reason, and never
                contains file content.
        """
        auth_enabled = auth_mode is AuthMode.ENABLED

        if auth_enabled:
            content = self.read_secret_set()
            if not content.configured:
                self._reject_configuration(
                    f"Auth_Mode is {AuthMode.ENABLED.value!r} but no "
                    f"{self._secret.description} is configured; set the "
                    f"operator_secret_file item of the security section, or "
                    f"disable authentication."
                )
            if content.error is not None:
                # Already carries the path and the underlying reason
                # (Requirement 7.4).
                self._reject_configuration(content.error)
            if not content.values:
                self._reject_configuration(
                    f"{self._secret.description} {self._secret.path} holds no "
                    f"operator secret: every line is blank or starts with "
                    f"{COMMENT_PREFIX!r}."
                )

        self._warn_on_wide_secret_file_permissions()

        if auth_enabled and not self._whitelist.configured:
            logger.warning(
                f"No {self._whitelist.description} is configured: any user "
                f"name presenting a valid operator secret is accepted, so "
                f"operator commands are authorized by the secret alone. Set "
                f"the operator_whitelist_file item of the security section to "
                f"restrict them to named users."
            )

    def _reject_configuration(self, message: str) -> None:
        """Log ``message`` as an ERROR and raise it as a configuration error.

        Both happen in one place so that a startup refusal can never be raised
        without a matching log record: the exception text reaches whoever
        started the process, the log record reaches the log file the operator
        will look at afterwards, and the two say the same thing.

        Raises:
            SecurityConfigError: Always.
        """
        logger.error(message)
        raise SecurityConfigError(message)

    def _warn_on_wide_secret_file_permissions(self) -> None:
        """Warn when the Operator_Secret_File is readable beyond its owner.

        The mode bits are reported in octal, in the form the operator would
        pass to ``chmod`` to fix them (Requirement 7.11).

        A file that cannot be stat-ed produces nothing here. When
        authentication is enabled that has already been reported as a fatal
        error by the caller, and when it is disabled an unreadable file is not
        yet a problem -- warning about the permissions of a file whose
        permissions could not be read would be noise either way.
        """
        path = self._secret.path
        if path is None:
            return

        try:
            mode = stat.S_IMODE(os.stat(path).st_mode)
        except OSError:
            return

        wide = mode & _GROUP_OTHER_READ_WRITE
        if wide:
            logger.warning(
                f"{self._secret.description} {path} has mode {mode:04o}, which "
                f"lets users other than its owner read or write it "
                f"({wide:04o}); the operator secret is only as private as this "
                f"file. Consider chmod 0600."
            )


#: Mode bits that let a user other than the file's owner read or write it.
#:
#: Only the read and write bits are tested, not the execute bits: a credential
#: file with ``+x`` set is odd but harmless, and reporting it would train the
#: reader to ignore this warning. The owner's own bits are irrelevant -- the
#: server process runs as the owner and has to be able to read the file.
_GROUP_OTHER_READ_WRITE: int = stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH


class PrivilegeLevel(enum.Enum):
    """The credentials an RPC method demands from its caller.

    Every RPC of the ``TaklerServer`` service falls into exactly one level, and
    the level alone decides what the Auth_Interceptor checks (Requirement 6.2):

    * :attr:`CHILD`: a Child_Command reported by a running job. It must carry a
      ``takler-pass`` metadata key; the interceptor only checks that the key is
      present, not what it holds (Requirements 6.4, 6.13). Comparing the value
      against the target Task's Job_Password belongs to the Zombie_Detector,
      because a *missing* password means "this caller has no credentials at
      all" while a *mismatching* one means "this caller holds the credentials
      of another job instance" -- the first must be rejected, the second is
      handled by the Zombie_Policy and may legitimately be let through.
    * :attr:`OPERATOR`: an Operator_Command, that is every Control_Command plus
      the two Query_Commands that expose the whole flow definition. It must
      carry both ``takler-secret`` and ``takler-user``, the secret must belong
      to the Operator_Secret_Set and the user name must be whitelisted
      (Requirements 6.5, 6.6, 6.7).
    * :attr:`PUBLIC`: no credentials at all, the RPC is let through before the
      metadata is even looked at (Requirement 6.8).

    The values are the lowercase names used in the rejection log records and in
    the abort details, so a level never has to be spelled out a second time.
    """

    CHILD = "child"
    OPERATOR = "operator"
    PUBLIC = "public"


#: Prefix shared by every fully qualified method name of the ``TaklerServer``
#: service, including both slashes.
#:
#: This is the form ``handler_call_details.method`` carries at runtime, and it
#: is what :data:`PRIVILEGE_BY_METHOD` is keyed by, so no normalization is
#: needed on the lookup path. The value matches
#: ``takler_pb2.DESCRIPTOR.services_by_name["TaklerServer"].full_name``, which
#: the privilege-table completeness test asserts against.
SERVICE_METHOD_PREFIX: str = "/takler_protocol.TaklerServer/"


#: Privilege_Level of every RPC method the ``TaklerServer`` service declares,
#: keyed by fully qualified method name (Requirement 6.2).
#:
#: The table is hardcoded rather than derived from a naming convention: a
#: convention would silently classify a future ``RunRequestDump`` as a Query
#: and a future ``QueryDelete`` as harmless, and the cost of getting the
#: classification wrong is an unauthenticated write path into the Bunch. An
#: explicit entry per method makes each decision reviewable in a diff.
#:
#: Not deriving it also keeps this module free of any dependency on the
#: generated stubs, so the lookup costs a dict hit and the table can be read
#: without importing protobuf.
#:
#: The counterpart of hardcoding is that the table must be kept in step with
#: ``takler.proto``. Two mechanisms cover that: at runtime an unregistered
#: method degrades to :attr:`PrivilegeLevel.OPERATOR` (see
#: :func:`privilege_for_method`), and in CI a test walks
#: ``takler_pb2.DESCRIPTOR.services_by_name["TaklerServer"].methods`` and
#: asserts every method name appears here explicitly. The runtime fallback is a
#: safety net, not a substitute for an entry.
PRIVILEGE_BY_METHOD: Dict[str, PrivilegeLevel] = {
    # Child_Commands: reported by a running job, authenticated by the
    # per-try Job_Password (Requirement 6.4).
    SERVICE_METHOD_PREFIX + "RunCommandInit": PrivilegeLevel.CHILD,
    SERVICE_METHOD_PREFIX + "RunCommandComplete": PrivilegeLevel.CHILD,
    SERVICE_METHOD_PREFIX + "RunCommandAbort": PrivilegeLevel.CHILD,
    SERVICE_METHOD_PREFIX + "RunCommandEvent": PrivilegeLevel.CHILD,
    SERVICE_METHOD_PREFIX + "RunCommandMeter": PrivilegeLevel.CHILD,
    # Control_Commands: they change the state of the Bunch, so they need the
    # shared Operator_Secret plus a whitelisted user name (Requirement 6.5).
    SERVICE_METHOD_PREFIX + "RunCommandRequeue": PrivilegeLevel.OPERATOR,
    SERVICE_METHOD_PREFIX + "RunCommandSuspend": PrivilegeLevel.OPERATOR,
    SERVICE_METHOD_PREFIX + "RunCommandResume": PrivilegeLevel.OPERATOR,
    SERVICE_METHOD_PREFIX + "RunCommandRun": PrivilegeLevel.OPERATOR,
    SERVICE_METHOD_PREFIX + "RunCommandForce": PrivilegeLevel.OPERATOR,
    SERVICE_METHOD_PREFIX + "RunCommandFreeDep": PrivilegeLevel.OPERATOR,
    SERVICE_METHOD_PREFIX + "RunCommandLoad": PrivilegeLevel.OPERATOR,
    SERVICE_METHOD_PREFIX + "RunCommandBegin": PrivilegeLevel.OPERATOR,
    # Read-only, but still Operator level: both return the entire flow
    # definition -- node paths, variables, trigger expressions -- which is
    # exactly the reconnaissance an attacker needs before sending a command.
    # The TUI is the main consumer, so enabling authentication means the TUI
    # user has to be able to read the Operator_Secret_File too.
    SERVICE_METHOD_PREFIX + "RunRequestShow": PrivilegeLevel.OPERATOR,
    SERVICE_METHOD_PREFIX + "QueryCoroutine": PrivilegeLevel.OPERATOR,
    # Liveness only: the response carries no flow information, and health
    # checks and monitoring must keep working without credentials
    # (Requirement 6.8).
    SERVICE_METHOD_PREFIX + "RunRequestPing": PrivilegeLevel.PUBLIC,
}


def privilege_for_method(
    method: str,
    table: Optional[Mapping[str, PrivilegeLevel]] = None,
) -> PrivilegeLevel:
    """Return the Privilege_Level required by a fully qualified method name.

    An unregistered method resolves to :attr:`PrivilegeLevel.OPERATOR`, the
    strictest level -- fail-closed (Requirement 6.2). The alternative defaults
    are both worse: :attr:`PrivilegeLevel.PUBLIC` would turn every forgotten
    entry into an unauthenticated hole, and raising would take the whole RPC
    down with an ``UNKNOWN`` status from inside an interceptor, which is neither
    a diagnosable error for the caller nor a decision this lookup should be
    making.

    Choosing :attr:`PrivilegeLevel.OPERATOR` over :attr:`PrivilegeLevel.CHILD`
    matters too, even though both demand credentials: Child only checks that
    ``takler-pass`` is present, which any job on the cluster can supply, while
    Operator requires a secret the caller cannot guess. A new rpc that nobody
    classified must demand the credentials of the operator who deployed the
    server, not those of an arbitrary job.

    The failure mode of this default is a new rpc that unexpectedly needs
    credentials -- visible, reported as ``UNAUTHENTICATED`` and fixed by adding
    the missing entry. The failure mode of the opposite default is an
    unauthenticated write path that nobody notices.

    Args:
        method: The method name as ``handler_call_details.method`` carries it,
            for example ``"/takler_protocol.TaklerServer/RunCommandInit"``.
        table: The lookup table, defaulting to :data:`PRIVILEGE_BY_METHOD`.
            Injectable so a test can exercise the fallback without mutating the
            module-level table, and so the interceptor can be given a narrowed
            table.

    Returns:
        The registered :class:`PrivilegeLevel`, or
        :attr:`PrivilegeLevel.OPERATOR` when ``method`` is not registered.
    """
    if table is None:
        table = PRIVILEGE_BY_METHOD
    return table.get(method, PrivilegeLevel.OPERATOR)


#: gRPC metadata key carrying the Job_Password of a Child_Command
#: (Requirement 6.1).
METADATA_KEY_JOB_PASSWORD: str = "takler-pass"

#: gRPC metadata key carrying the Operator_Secret of an Operator_Command
#: (Requirement 6.1).
METADATA_KEY_SECRET: str = "takler-secret"

#: gRPC metadata key carrying the OS user name of the caller of an
#: Operator_Command (Requirement 6.1).
METADATA_KEY_USER: str = "takler-user"

#: The three Credential_Metadata keys, in the order they are documented in the
#: Cross-Language Contract.
#:
#: All three are lowercase and none ends in ``-bin``: gRPC requires lowercase
#: header names, and a ``-bin`` suffix would declare the value to be binary,
#: while all three carry ASCII text. They are constants rather than literals
#: spelled out at each use site so the interceptor, the client and the tests
#: cannot drift apart on a key name -- a typo there would not fail loudly, it
#: would silently look like "the caller sent no credentials".
CREDENTIAL_METADATA_KEYS: Tuple[str, str, str] = (
    METADATA_KEY_JOB_PASSWORD,
    METADATA_KEY_SECRET,
    METADATA_KEY_USER,
)

#: Placeholder written to the ``user`` field of an Audit_Record when the RPC
#: carried no ``takler-user`` (Requirement 11.8).
#:
#: A fixed placeholder rather than an empty string or a missing key: every
#: Audit_Record must hold the same key set for the round-trip property to
#: hold, and a reader grepping the audit file for a user name must be able to
#: tell "nobody identified themselves" from "the field was not written".
AUDIT_UNKNOWN_USER: str = "unknown"


def _metadata_text(value: Any) -> Optional[str]:
    """Normalize one metadata value to text, or to ``None`` when unusable.

    A blank value is normalized to ``None``, so a caller sending
    ``takler-secret: ""`` is treated as sending no secret at all. This is
    fail-closed and matches what the client does at the other end: it omits the
    key entirely when the value would be blank (Requirements 8.3, 8.7). Without
    the normalization an empty string would count as "credential present" and
    would have to be rejected one layer further in, by a value comparison.

    The value itself is *not* stripped: only the blankness test looks past
    whitespace. Both the Operator_Secret and the Operator_Whitelist are matched
    byte-exactly (Requirement 7.10), so trimming here would quietly widen what
    the server accepts.

    ``bytes`` is tolerated because gRPC hands back bytes for binary metadata
    and a non-conforming client may mark a key that way; an undecodable value
    resolves to ``None`` rather than raising, since an exception escaping the
    interceptor would degrade the RPC into an ``UNKNOWN`` status instead of the
    intended ``UNAUTHENTICATED``.
    """
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(value, str):
        return None
    return None if _is_blank(value) else value


@dataclasses.dataclass(frozen=True, repr=False)
class CallCredentials:
    """The credentials and caller identity of a single RPC.

    One instance is built by the Auth_Interceptor from the invocation metadata
    and published in a context variable, from which three later points read it
    without having it threaded through their signatures: the Zombie_Detector's
    ``Z1`` check needs :attr:`job_password` (Requirement 6.13), and the
    Audit_Logger needs :attr:`user` and :attr:`peer` at the end of a handler,
    on the interceptor's rejection path and on the zombie path
    (Requirement 11.8).

    Frozen because it is shared context, not state: once the interceptor has
    published it, a handler that could rewrite ``user`` would rewrite the
    identity the audit trail is about. Freezing also makes it safe to hold the
    default instance as module-level data.

    Every field is optional. Which of them must be present is not a property of
    this class but of the method being called, and that decision belongs to the
    privilege table plus the interceptor: a Child_Command needs only
    :attr:`job_password`, an Operator_Command needs :attr:`secret` and
    :attr:`user`, and a ``PUBLIC`` method needs none of them.

    Attributes:
        job_password: The ``takler-pass`` value, or ``None``. Compared against
            the target Task_Node's Job_Password by the Zombie_Detector, never
            by the interceptor (Requirement 6.13).
        secret: The ``takler-secret`` value, or ``None``.
        user: The ``takler-user`` value, or ``None``.
        peer: The caller's network address, or ``None``. It does not travel in
            the metadata -- it is read from the gRPC ``ServicerContext`` /
            ``HandlerCallDetails`` and passed to :meth:`from_metadata`
            separately.
    """

    job_password: Optional[str] = None
    secret: Optional[str] = None
    user: Optional[str] = None
    peer: Optional[str] = None

    @classmethod
    def empty(cls) -> "CallCredentials":
        """Return credentials with every field unset.

        This is what a call that never passed through the interceptor sees; see
        :data:`_CALL_CREDENTIALS` for why that is the right default rather than
        an error.
        """
        return cls()

    @classmethod
    def from_metadata(
        cls,
        metadata: Optional[Iterable[Any]],
        peer: Optional[str] = None,
    ) -> "CallCredentials":
        """Build credentials from gRPC invocation metadata.

        The metadata is a sequence of ``(key, value)`` pairs rather than a
        mapping, which is why this walks it once instead of doing three
        lookups. Typing it structurally keeps this module free of a :mod:`grpc`
        import, so the parsing can be tested with plain tuples.

        Nothing here raises. An interceptor cannot afford an exception: it would
        surface as an ``UNKNOWN`` status code instead of the
        ``UNAUTHENTICATED`` / ``PERMISSION_DENIED`` the caller needs to tell a
        missing credential from a rejected one. Hence every hostile shape is
        absorbed: ``None`` metadata, a missing key, an entry that is not a pair,
        a value that is not text, and a blank value (see
        :func:`_metadata_text`).

        **A duplicated key: the first occurrence wins.** HTTP/2 allows a header
        name to repeat and gRPC does not collapse the repetitions, so the server
        has to pick one. First-wins is the deterministic choice that cannot be
        influenced by appending: an attacker who can only add metadata after
        what the client sent cannot replace the secret that was already there.
        A duplicate is anomalous in any case -- both the Python and the Go
        client set each key exactly once -- so this rule is about being
        predictable under a malformed input, not about supporting a use case.

        Args:
            metadata: The invocation metadata, for example
                ``handler_call_details.invocation_metadata`` or
                ``context.invocation_metadata()``. ``None`` yields empty
                credentials.
            peer: The caller's network address, taken from the gRPC context.
                ``None`` leaves :attr:`peer` unset, for a caller that fills it
                in later.

        Returns:
            A :class:`CallCredentials` holding whichever of the three keys were
            present with a non-blank value.
        """
        values: Dict[str, str] = {}
        if metadata is not None:
            for item in metadata:
                # gRPC yields 2-tuples (``grpc.aio`` yields ``Metadatum``,
                # which is one). Anything else is skipped rather than allowed
                # to raise, for the reason given above.
                try:
                    key, value = item
                except (TypeError, ValueError):
                    continue
                if not isinstance(key, str):
                    continue
                # HTTP/2 header names are case-insensitive and gRPC lowercases
                # them on the wire; folding here only makes a non-conforming
                # client behave the same as a conforming one.
                key = key.lower()
                if key not in CREDENTIAL_METADATA_KEYS or key in values:
                    continue
                text = _metadata_text(value)
                if text is not None:
                    values[key] = text

        return cls(
            job_password=values.get(METADATA_KEY_JOB_PASSWORD),
            secret=values.get(METADATA_KEY_SECRET),
            user=values.get(METADATA_KEY_USER),
            peer=peer,
        )

    def with_peer(self, peer: Optional[str]) -> "CallCredentials":
        """Return a copy carrying ``peer``.

        The peer address is not part of the metadata, so it is often known at a
        different point from the credentials themselves -- for instance when
        the metadata was parsed from ``handler_call_details`` but the address
        only becomes available from the ``ServicerContext``. Since the class is
        frozen, "add the peer later" has to mean "make a copy".
        """
        return dataclasses.replace(self, peer=peer)

    @property
    def has_job_password(self) -> bool:
        """Whether a non-blank ``takler-pass`` was carried.

        This is the whole of what the interceptor checks for a Child_Command:
        whether the value matches the target Task_Node's Job_Password is the
        Zombie_Detector's decision, because a missing password means "no
        credentials" while a mismatching one means "the credentials of another
        job instance", and only the first is an authentication failure
        (Requirement 6.13).
        """
        return self.job_password is not None

    @property
    def has_secret(self) -> bool:
        """Whether a non-blank ``takler-secret`` was carried."""
        return self.secret is not None

    @property
    def has_user(self) -> bool:
        """Whether a non-blank ``takler-user`` was carried."""
        return self.user is not None

    def audit_user(self) -> str:
        """Return the user name for a log or Audit_Record.

        Returns:
            :attr:`user`, or :data:`AUDIT_UNKNOWN_USER` when no ``takler-user``
            was carried (Requirement 11.8). A Child_Command never carries one,
            so this placeholder is the normal case on the child path, not an
            error indicator.
        """
        return self.user if self.user else AUDIT_UNKNOWN_USER

    def __repr__(self) -> str:
        """Return a representation that cannot leak a credential value.

        This override is a containment barrier, not formatting. The generated
        dataclass ``repr`` would print the Job_Password and the Operator_Secret
        in full, and this object is exactly the thing that gets passed around
        the server and thus the thing most likely to end up interpolated into a
        log record or an exception message. Requirements 12.1 and 12.3 forbid
        either value from reaching any log line or any ``str()`` of a
        server-side exception, and the only way to make that hold for code not
        yet written is for the values to be unprintable through the normal
        route.

        What is kept is what debugging actually needs: whether each secret
        field was present, plus the two non-secret fields in full. "The caller
        sent a password but it did not match" and "the caller sent no password"
        are then still distinguishable from a log.

        Reading :attr:`job_password` or :attr:`secret` explicitly still yields
        the value, as the Zombie_Detector's comparison requires. The barrier
        stops the accidental path, not the deliberate one.
        """
        return (
            f"{type(self).__name__}("
            f"job_password={'<set>' if self.has_job_password else 'None'}, "
            f"secret={'<set>' if self.has_secret else 'None'}, "
            f"user={self.user!r}, "
            f"peer={self.peer!r})"
        )


#: The Credential_Metadata of the RPC being served in the current context.
#:
#: The Auth_Interceptor parses the metadata once and publishes it here; the
#: Zombie_Detector and the Audit_Logger read it back. The alternative -- passing
#: the credentials down as arguments -- would have to thread them through
#: ``TaklerService._handle_command`` and five ``Scheduler.run_command_*``
#: methods to reach the ``Z1`` check, and the audit fields are needed at three
#: unrelated call sites, so the parameter would spread further than the feature.
#:
#: Concurrency is safe without any locking: ``grpc.aio`` serves each RPC in its
#: own asyncio task, and a task copies the current context when it is created,
#: so a ``set()`` in one RPC is invisible to every other.
#:
#: The default is :meth:`CallCredentials.empty` rather than ``None`` -- and
#: rather than no default at all, which would make :meth:`get` raise
#: :exc:`LookupError`. The Scheduler is not only reached through the
#: interceptor: the TUI, the unit tests and ``run_server_until_complete`` call
#: it directly, and none of them goes through an RPC. With an empty default
#: those callers read all-``None`` credentials, which is correct rather than
#: merely convenient: in that situation Auth_Mode is necessarily ``disabled``
#: (an ``enabled`` server rejects an RPC before the handler runs, and an
#: in-process caller sends no RPC at all), so the ``Z1`` check is skipped and
#: the absent Job_Password is never consulted (Requirement 9.4).
_CALL_CREDENTIALS: contextvars.ContextVar[CallCredentials] = contextvars.ContextVar(
    "takler_call_credentials", default=CallCredentials.empty()
)


def get_call_credentials() -> CallCredentials:
    """Return the Credential_Metadata of the RPC being served.

    Returns:
        The credentials published by the Auth_Interceptor, or
        :meth:`CallCredentials.empty` when the current code was not reached
        through an RPC. Never raises :exc:`LookupError`; see
        :data:`_CALL_CREDENTIALS`.
    """
    return _CALL_CREDENTIALS.get()


def set_call_credentials(credentials: CallCredentials) -> contextvars.Token:
    """Publish the Credential_Metadata of the RPC being served.

    Args:
        credentials: The credentials parsed from the invocation metadata.

    Returns:
        The :class:`contextvars.Token` of the assignment, for
        :func:`reset_call_credentials`. The interceptor ignores it -- the
        context of an RPC dies with the task serving that RPC -- but a test
        that sets credentials in its own context needs it to clean up.
    """
    return _CALL_CREDENTIALS.set(credentials)


def reset_call_credentials(token: contextvars.Token) -> None:
    """Undo a :func:`set_call_credentials`.

    Args:
        token: The token returned by the matching
            :func:`set_call_credentials`.
    """
    _CALL_CREDENTIALS.reset(token)


#: Text written in place of a credential value that would otherwise be echoed
#: into a rejection record (Requirements 6.12, 12.1).
REDACTED: str = "<redacted>"

#: Longest client-supplied string echoed into a rejection record or into the
#: gRPC abort details.
#:
#: Both the method name and the ``takler-user`` value are chosen by the caller,
#: and a refused caller is by definition one whose input is not trusted. gRPC
#: accepts a method name of several kilobytes and a metadata value of more, so
#: without a bound one refused RPC could write an arbitrarily large log line --
#: which is a way to fill a disk, and a way to push the interesting records out
#: of a rotated log. 200 characters is far more than the longest real method
#: name (~45) or POSIX user name (32), so nothing legitimate is ever truncated.
MAX_ECHOED_LENGTH: int = 200

#: Marker appended to a value that :func:`sanitize_echoed_value` truncated.
TRUNCATION_MARKER: str = "...(truncated)"

#: Shortest credential value that :func:`sanitize_echoed_value` will redact by
#: substring match.
#:
#: The redaction exists for one narrow case: a caller who sets its
#: ``takler-user`` -- or invokes a method named -- exactly like the secret it
#: presents, so that the value it sent comes back out through the record.
#: Substituting a *short* value would do more harm than good: a one-character
#: secret occurs inside almost every method name, and blanking it out would
#: turn every rejection record into unreadable rubble while protecting a value
#: that is guessable in a handful of attempts anyway. Below this length the
#: containment that matters is the one that is unconditional -- no field of a
#: record is ever *taken from* a credential.
MIN_REDACTED_LENGTH: int = 8


def sanitize_echoed_value(
    value: Optional[str],
    credentials: Optional["CallCredentials"] = None,
) -> str:
    """Make a caller-supplied string safe to put in a log line or abort details.

    Three things are done to it, in this order:

    1. **Control characters are escaped.** A ``takler-user`` of
       ``"alice\\nWARNING refused nothing: ok"`` would otherwise write a second,
       forged line into the log file, and a log reader cannot tell it from a
       real record. The escaping is by code point (``\\x0a``) and leaves
       printable non-ASCII alone, so a user name or a path in any script stays
       readable.
    2. **A presented credential value is redacted** when it occurs as a
       substring and is at least :data:`MIN_REDACTED_LENGTH` long. This is the
       belt to the braces of Requirements 6.12 and 12.1: no field of a rejection
       record is *taken from* a credential in the first place, and if a caller
       arranges for one to arrive through a field that is echoed, the value
       still does not reach the log or the client. It runs before the
       truncation, so a value parked past the length limit is removed rather
       than cut in half.
    3. **The result is truncated** to :data:`MAX_ECHOED_LENGTH`, so one refused
       RPC cannot write an unbounded record (see there).

    Args:
        value: The caller-supplied text, for example the method name or the
            ``takler-user`` value. ``None`` yields ``"None"``, so a caller need
            not special-case an absent field.
        credentials: The credentials the caller presented, whose
            :attr:`~CallCredentials.job_password` and
            :attr:`~CallCredentials.secret` are redacted from the result.
            ``None`` skips that step.

    Returns:
        The sanitized text, safe to interpolate into a log record or into gRPC
        status details.
    """
    if value is None:
        return "None"

    escaped = "".join(
        ch if ch.isprintable() or ch == " " else f"\\x{ord(ch):02x}" for ch in value
    )
    if credentials is not None:
        for secret in (credentials.job_password, credentials.secret):
            if secret is not None and len(secret) >= MIN_REDACTED_LENGTH:
                escaped = escaped.replace(secret, REDACTED)

    if len(escaped) > MAX_ECHOED_LENGTH:
        escaped = escaped[:MAX_ECHOED_LENGTH] + TRUNCATION_MARKER

    return escaped


#: gRPC status code each :class:`RejectionReason` is answered with
#: (Requirements 6.4, 6.5, 6.6, 6.7).
#:
#: The split follows what the caller can do about the refusal, which is also
#: what the two codes mean in the gRPC contract: ``UNAUTHENTICATED`` is "you
#: presented no credentials", ``PERMISSION_DENIED`` is "you presented
#: credentials and they do not grant this". Keeping them apart is what lets a
#: client tell "this client is not configured for an authenticated server" from
#: "this secret is stale" without parsing the message text.
#:
#: Both map to exit code 1 in the Client_CLI, so the distinction is for the
#: human reading the message, not for the exit status (Requirement 6.14).
STATUS_CODE_BY_REJECTION: Dict[RejectionReason, grpc.StatusCode] = {
    RejectionReason.MISSING_CREDENTIAL: grpc.StatusCode.UNAUTHENTICATED,
    RejectionReason.INVALID_CREDENTIAL: grpc.StatusCode.PERMISSION_DENIED,
    RejectionReason.NOT_IN_WHITELIST: grpc.StatusCode.PERMISSION_DENIED,
}


class AuthInterceptor(grpc.aio.ServerInterceptor):
    """The single place every RPC is authenticated (Requirement 6.2).

    One interceptor in front of the whole service, rather than a check inside
    each handler: a handler that forgets to check is an unauthenticated write
    path into the Bunch, and there is no way to notice it is missing. Here the
    check cannot be forgotten, because a method that nobody classified still
    resolves to :attr:`PrivilegeLevel.OPERATOR` (see
    :func:`privilege_for_method`).

    What is checked follows from the method's Privilege_Level alone:

    ==================== =================================================
    Privilege_Level      Checked when Auth_Mode is ``enabled``
    ==================== =================================================
    ``PUBLIC``           nothing, the metadata is not even parsed (6.8)
    ``CHILD``            ``takler-pass`` is present (6.4, 6.13)
    ``OPERATOR``         ``takler-secret`` and ``takler-user`` are present,
                         the secret is in the Operator_Secret_Set and the
                         user is whitelisted (6.5, 6.6, 6.7)
    ==================== =================================================

    With Auth_Mode ``disabled`` nothing is checked at all and every RPC passes,
    which is what keeps an M1 deployment working unchanged (Requirement 6.3).
    The credentials are still parsed and published in that case: the
    Zombie_Detector's ``Z2`` / ``Z3`` checks and the Audit_Logger's ``user`` /
    ``peer`` fields do not depend on authentication being on.

    **A Child_Command's password is only checked for presence, never compared.**
    The interceptor does not know which node the command targets without
    deserializing the request, and the Job_Password is per node. More
    importantly the two failures are different: an absent password means "this
    caller has no credentials", which is an authentication failure, while a
    mismatching one means "this caller holds the credentials of an earlier run
    of this task", which is a zombie and may legitimately be let through
    depending on the Zombie_Policy. So the comparison belongs to the
    Zombie_Detector (Requirement 6.13).

    Attributes:
        auth_mode: The resolved Auth_Mode. Read once per RPC so that it can be
            reassigned in a test between calls.
        credential_store: The store consulted for an ``OPERATOR`` level method.
    """

    def __init__(
        self,
        auth_mode: AuthMode = DEFAULT_AUTH_MODE,
        credential_store: Optional[CredentialStore] = None,
        audit_logger: Optional[AuditLogger] = None,
        privilege_table: Optional[Mapping[str, PrivilegeLevel]] = None,
    ) -> None:
        """Build the interceptor.

        Args:
            auth_mode: The resolved Auth_Mode (Requirement 3.5 resolves it, not
                this class). Defaults to :data:`DEFAULT_AUTH_MODE`, that is
                ``disabled``.
            credential_store: The :class:`CredentialStore` holding the
                Operator_Secret_Set and the Operator_Whitelist. ``None`` builds
                an empty store, which verifies no secret at all: an
                ``OPERATOR`` method is then always refused with an ERROR naming
                the missing configuration, never let through
                (Requirement 7.7).
            audit_logger: The Audit_Logger the rejection path writes its
                ``denied`` record to (Requirement 11.3). ``None`` skips the
                record, which is what a test or an in-process server that
                configures no auditing gets; the WARNING of Requirement 6.10 is
                emitted either way.
            privilege_table: The method name to Privilege_Level table,
                defaulting to :data:`PRIVILEGE_BY_METHOD`. Injectable for tests
                that stand up a service of their own.
        """
        self.auth_mode: AuthMode = auth_mode
        self.credential_store: CredentialStore = (
            credential_store if credential_store is not None else CredentialStore()
        )
        self._audit_logger: Optional[AuditLogger] = audit_logger
        self._privilege_table: Optional[Mapping[str, PrivilegeLevel]] = privilege_table

    async def intercept_service(
        self,
        continuation: Callable[[Any], Any],
        handler_call_details: Any,
    ) -> Any:
        """Authenticate one RPC, then either let it through or refuse it.

        Returns:
            The handler ``continuation`` produced when the call is authorized,
            or an abort handler that fails the RPC with the mapped status code
            when it is not. A refused call never reaches ``continuation``, so
            no handler runs and no node state can change (Requirement 6.9).
        """
        method = getattr(handler_call_details, "method", "")
        level = privilege_for_method(method, table=self._privilege_table)

        # PUBLIC first, before the metadata is looked at: ``ping`` is what a
        # health check and a monitoring probe call, and it has to keep working
        # on a server whose credential files are broken (Requirement 6.8).
        if level is PrivilegeLevel.PUBLIC:
            return await continuation(handler_call_details)

        credentials = CallCredentials.from_metadata(
            getattr(handler_call_details, "invocation_metadata", None)
        )

        # ``disabled``: publish and pass, with no check at all. The Auth_Mode is
        # read here rather than in ``__init__`` so a test may flip it between
        # calls on one interceptor (Requirement 6.3).
        if self.auth_mode is not AuthMode.ENABLED:
            set_call_credentials(credentials)
            return await continuation(handler_call_details)

        reason = self._reject_reason(level, credentials)
        if reason is not None:
            return self._abort_handler(method, credentials, reason)

        set_call_credentials(credentials)
        return await continuation(handler_call_details)

    def _reject_reason(
        self,
        level: PrivilegeLevel,
        credentials: CallCredentials,
    ) -> Optional[RejectionReason]:
        """Decide whether a call at ``level`` may proceed.

        Args:
            level: The Privilege_Level the method demands. ``PUBLIC`` never
                reaches here.
            credentials: The credentials parsed from the invocation metadata.

        Returns:
            ``None`` when the call is authorized, otherwise the
            :class:`RejectionReason` to report.
        """
        if level is PrivilegeLevel.CHILD:
            # Presence only; the value is the Zombie_Detector's business
            # (Requirements 6.4, 6.13).
            if not credentials.has_job_password:
                return RejectionReason.MISSING_CREDENTIAL
            return None

        # Everything else, including any method that is not registered in the
        # privilege table, is treated as OPERATOR (Requirement 6.2).
        return self.credential_store.authorize_operator(
            credentials.secret, credentials.user
        )

    def _abort_handler(
        self,
        method: str,
        credentials: CallCredentials,
        reason: RejectionReason,
    ) -> grpc.RpcMethodHandler:
        """Build the handler that refuses one RPC.

        Returning a substitute handler is the only way an ``grpc.aio``
        interceptor can refuse a call with a chosen status code. Raising from
        :meth:`intercept_service` would reach the client as ``UNKNOWN``, which
        tells it nothing about whether to fix its credentials, and the
        Client_CLI would read it as a server fault rather than as a refusal.

        The behaviour is registered as unary-unary because every rpc of the
        ``TaklerServer`` service is unary-unary, ``QueryCoroutine`` included --
        the privilege-table completeness test walks the service descriptor, so
        a future streaming rpc arrives together with a failing test, and this is
        where the matching handler flavour would have to be selected.

        The abort itself happens inside the behaviour rather than here, since
        aborting needs the ``ServicerContext`` that only exists once the call is
        dispatched. That context is also where the caller's network address
        comes from, which is why the rejection is logged there too: an address
        is most of what makes a refusal record actionable.

        Args:
            method: The fully qualified method name that was refused.
            credentials: The credentials the caller presented, for the user
                name in the log record. No value of them is ever logged.
            reason: The classification to report.

        Returns:
            An :class:`grpc.RpcMethodHandler` that fails the call with the
            status code :data:`STATUS_CODE_BY_REJECTION` maps ``reason`` to.
        """
        status_code = STATUS_CODE_BY_REJECTION[reason]
        # Only the classification and the method name, never a credential value
        # and never a hint about which check failed on the server's files -- an
        # attacker learning "the secret was right but the user is not
        # whitelisted" learns that the secret they hold is live
        # (Requirements 6.12, 12.1).
        #
        # The method name is sanitized even though it looks like server-side
        # data: it is whatever the caller put on the wire, and an unregistered
        # method is refused rather than dropped, so this string is
        # caller-controlled on exactly this path (see
        # :func:`sanitize_echoed_value`).
        safe_method = sanitize_echoed_value(method, credentials)
        details = f"{safe_method} refused: {reason.value}"

        async def abort(request: Any, context: Any) -> None:
            peer = None
            try:
                peer = context.peer()
            except Exception:  # pragma: no cover - defensive
                # A context that cannot report its peer must still be able to
                # refuse the call; the address is diagnostic, not part of the
                # decision.
                pass
            self._log_rejection(safe_method, credentials.with_peer(peer), reason)
            await context.abort(status_code, details)

        return grpc.unary_unary_rpc_method_handler(abort)

    def _log_rejection(
        self,
        method: str,
        credentials: CallCredentials,
        reason: RejectionReason,
    ) -> None:
        """Record one refusal (Requirement 6.10).

        The record carries exactly the four things a refusal has to be
        actionable from -- the method name, the ``takler-user`` value, the
        caller's network address and the classification -- and nothing else. In
        particular it carries no credential value: the user name is taken
        through :meth:`CallCredentials.audit_user` and
        :attr:`~CallCredentials.job_password` and
        :attr:`~CallCredentials.secret` are never read on this path
        (Requirements 6.10, 12.1).

        **Both caller-controlled fields are sanitized.** The method name and the
        user name arrive from the wire, and this is the one path where an
        unregistered method name and an arbitrary user name are echoed rather
        than dropped. Without the escaping a ``takler-user`` holding a newline
        would write a forged second line into the log file, which is worse than
        a missing record because it is indistinguishable from a real one; and
        without the length bound a refused caller could choose how many bytes
        each refusal costs the log. See :func:`sanitize_echoed_value`. The
        method name is sanitized by the caller, which needs the same text for
        the abort details.

        The peer address is not sanitized: it comes from the gRPC stack, not
        from the caller.

        Args:
            method: The already sanitized method name that was refused.
            credentials: The credentials the caller presented, for the user name
                and the peer address, and for the redaction.
            reason: The classification to report.
        """
        user = sanitize_echoed_value(credentials.audit_user(), credentials)
        logger.warning(
            f"refused {method}: {reason.value} (user={user}, peer={credentials.peer})"
        )
        self._audit_rejection(method, user, credentials)

    def _audit_rejection(
        self,
        method: str,
        user: str,
        credentials: CallCredentials,
    ) -> None:
        """Write the ``denied`` Audit_Record of one refusal (Requirement 11.3).

        Exactly one record per refused RPC, alongside the WARNING: the log line
        is what an operator watching the server sees, the record is what a query
        over the audit trail finds, and the two report the same refusal.

        ``event`` and ``outcome`` are both fixed -- ``denied`` -- and
        ``error_code`` is :data:`~takler.server.audit.DENIED_ERROR_CODE`, since
        a refused call never reaches a handler and therefore has no
        ``ServiceResponse.flag`` to copy (Requirement 11.7).

        ``target`` is empty: the request body is never deserialized on this path,
        so the server does not know which nodes the caller meant to act on -- and
        must not know, since parsing an unauthenticated request is work an
        unauthenticated caller could ask for at will.

        Both caller-controlled fields are the already sanitized ones, so a
        ``takler-user`` cannot smuggle a credential value or an unbounded string
        into the audit trail either (Requirements 6.12, 12.1).

        Args:
            method: The already sanitized method name that was refused.
            user: The already sanitized ``takler-user`` value, or the
                ``unknown`` placeholder (Requirement 11.8).
            credentials: The credentials the caller presented, for the peer
                address. No value of them is read here.
        """
        if self._audit_logger is None:
            return
        self._audit_logger.record(
            AuditRecord(
                timestamp=audit_timestamp(),
                event=EVENT_DENIED,
                command=audit_command_name(method),
                user=user,
                peer=audit_peer(credentials.peer),
                target=[],
                outcome=OUTCOME_DENIED,
                error_code=DENIED_ERROR_CODE,
            )
        )
