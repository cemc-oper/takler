"""Authentication support for the Takler server.

This module owns the server side of the authentication contract: the
Operator_Secret_Set and the Operator_Whitelist (:class:`CredentialStore`), the
method-name privilege table, the per-call credentials taken from the gRPC
metadata and the Auth_Interceptor that applies them.

Three independent pieces live here so far: :class:`CredentialStore` with its
file handling, the method-name privilege table
(:class:`PrivilegeLevel`, :data:`PRIVILEGE_BY_METHOD`,
:func:`privilege_for_method`), and the per-call credentials
(:class:`CallCredentials` plus the context variable that carries them from the
interceptor to the Zombie_Detector and to the Audit_Logger). The interceptor
that joins them is added on top.

All three deliberately depend on nothing but the standard library and
:mod:`takler.logging` -- not even on :mod:`grpc` -- so the parsing, the
hot-reload behaviour, the privilege lookup and the metadata parsing can all be
tested without standing up a gRPC server.

Requirements: 6.1, 6.2, 6.4, 6.5, 6.8, 6.13, 7.1, 7.2, 7.6, 7.12, 11.8, 12.1,
12.3.
"""

from __future__ import annotations

import contextvars
import dataclasses
import enum
import os
from pathlib import Path
from typing import (
    Any,
    Dict,
    FrozenSet,
    Iterable,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from takler.logging import get_logger

__all__ = [
    "AUDIT_UNKNOWN_USER",
    "COMMENT_PREFIX",
    "CREDENTIAL_METADATA_KEYS",
    "METADATA_KEY_JOB_PASSWORD",
    "METADATA_KEY_SECRET",
    "METADATA_KEY_USER",
    "PRIVILEGE_BY_METHOD",
    "SERVICE_METHOD_PREFIX",
    "CallCredentials",
    "CredentialFileContent",
    "CredentialStore",
    "PrivilegeLevel",
    "get_call_credentials",
    "privilege_for_method",
    "reset_call_credentials",
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
    failure through :attr:`CredentialFileContent.error` instead. The credential
    verification built on top of them turns such a failure into a fail-closed
    ``PERMISSION_DENIED`` with an ERROR log rather than letting an exception
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
