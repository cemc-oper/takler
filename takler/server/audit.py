"""Audit_Record schema and the Audit_Logger that writes it.

Every Control_Command, every authentication rejection and every zombie
disposition leaves exactly one structured line behind, so that "who changed
what, when" is answerable after the fact from a single file. This module owns
the two pieces that answer makes possible:

* :class:`AuditRecord` -- the eight-key record of Requirement 11.5, and its
  ``to_json_line`` serialization;
* :class:`AuditLogger` -- the single write path shared by the three record
  points (Requirements 11.2, 11.3, 11.4), which the callers reach through
  :meth:`AuditLogger.record`.

Two properties shape the implementation.

**A record is one line, whatever a node path contains.** Audit files are read
back with line-oriented tools, so a node path holding a quote, a newline or a
non-ASCII character must not be able to split one record across two lines or
make a line unparsable. :func:`json.dumps` escapes the ASCII control
characters, and ``ensure_ascii=False`` keeps the non-ASCII ones readable rather
than turning a Chinese node path into ``\\uXXXX`` noise. What ``json.dumps``
leaves raw are the three non-ASCII characters that Python nonetheless treats as
line boundaries, so :meth:`AuditRecord.to_json_line` escapes those explicitly
-- see :data:`_EXTRA_LINE_BOUNDARIES`.

**Auditing is an observability mechanism, not an availability dependency.** A
failure to write must not change what the audited RPC returns (Requirement
11.15), so :meth:`AuditLogger.record` catches *every* exception and degrades to
a WARNING on the regular logger naming the Audit_File and the reason. A server
whose audit disk filled up keeps serving commands; it just stops being able to
prove what it served.

Writing goes through the existing logging subsystem under the component name
``audit`` (Requirement 11.1) rather than through a private file handle, so the
audit sink inherits that subsystem's idempotent teardown, formatting and
component-name isolation. The backends open the audit file lazily precisely so
that this module can pre-create it with owner-only permissions first
(Requirement 11.14), instead of the handler creating it under the process umask
and being tightened afterwards through a window in which the file is readable
by everyone.

The record points themselves live elsewhere: the Network_Service's command
handler, the Auth_Interceptor and the Zombie_Detector each build an
:class:`AuditRecord` and hand it to one shared :class:`AuditLogger`.

Requirements: 11.5, 11.6, 11.7, 11.8, 11.9, 11.11, 11.14, 11.15, 11.16.
"""

from __future__ import annotations

import dataclasses
import datetime
import json
import os
from pathlib import Path
from typing import List, Optional, Union

from takler.logging import get_logger
from takler.logging.config import AUDIT_COMPONENT


__all__ = [
    "AUDIT_FILE_MODE",
    "DENIED_ERROR_CODE",
    "EVENT_CONTROL",
    "EVENT_DENIED",
    "EVENT_ZOMBIE",
    "OUTCOME_DENIED",
    "OUTCOME_ERROR",
    "OUTCOME_SUCCESS",
    "OUTCOME_ZOMBIE",
    "UNKNOWN_USER",
    "AuditLogger",
    "AuditRecord",
    "audit_timestamp",
]


#: The three ``event`` values (Requirement 11.6), one per record point. Spelled
#: out as constants so the record points and the tests asserting on them share
#: one definition instead of repeating string literals.
EVENT_CONTROL: str = "control"
EVENT_DENIED: str = "denied"
EVENT_ZOMBIE: str = "zombie"

#: The four ``outcome`` values (Requirement 11.7).
OUTCOME_SUCCESS: str = "success"
OUTCOME_ERROR: str = "error"
OUTCOME_DENIED: str = "denied"
OUTCOME_ZOMBIE: str = "zombie"

#: ``error_code`` of a record written for an authentication rejection
#: (Requirement 11.7). The rejection never reaches a command handler, so there
#: is no ``ServiceResponse.flag`` to copy and this fixed value stands in.
DENIED_ERROR_CODE: int = 43

#: Placeholder ``user`` for a call that carried no ``takler-user`` metadata key
#: (Requirement 11.8). A record with an empty user would be ambiguous between
#: "anonymous call" and "audit bug", so the absence is spelled out.
UNKNOWN_USER: str = "unknown"

#: Permissions of an Audit_File this process creates (Requirement 11.14). The
#: audit trail names the users who issued each command and the addresses they
#: came from, which is not information to hand to every account on a shared
#: login node.
AUDIT_FILE_MODE: int = 0o600

#: Characters that :func:`json.dumps` leaves raw even though Python's
#: ``str.splitlines`` treats them as line boundaries: NEXT LINE (U+0085), LINE
#: SEPARATOR (U+2028) and PARAGRAPH SEPARATOR (U+2029). ``json.dumps`` escapes
#: everything below U+0020 -- ``\n``, ``\r``, ``\v``, ``\f`` and the three
#: information separators -- but these three are above it and survive with
#: ``ensure_ascii=False``. Replacing each with its ``\uXXXX`` escape keeps the
#: JSON value bit-for-bit identical after ``json.loads`` while making the
#: "exactly one line" guarantee hold under every notion of a line break
#: (Requirement 11.5).
_EXTRA_LINE_BOUNDARIES: "tuple[str, ...]" = ("\u0085", "\u2028", "\u2029")


def audit_timestamp() -> str:
    """Return the current local time as an ISO 8601 string (Req 11.6).

    Local time rather than UTC because the audit trail is read next to the
    server's regular log and the operator's own shell history, and a reader
    correlating the three should not have to convert time zones. The value is
    naive -- no offset suffix -- matching the rest of takler's timestamps.

    Returns:
        For example ``"2026-07-15T10:30:00.123456"``.
    """
    return datetime.datetime.now().isoformat()


@dataclasses.dataclass(frozen=True)
class AuditRecord:
    """One audit trail entry: the eight keys of Requirement 11.5.

    Frozen because a record describes something that already happened. The
    three record points build one, hand it to :meth:`AuditLogger.record` and
    keep no reference; immutability makes it plain that nothing downstream can
    revise history.

    Field order is the order Requirement 11.5 lists the keys in, and
    :meth:`to_json_line` preserves it, so a human scanning an audit file sees
    the same key order on every line.

    None of the eight fields carries a Job_Password or an Operator_Secret
    (Requirement 11.11). That holds by construction here -- there is no field
    to put one in -- and the record points must not smuggle one into ``target``
    or ``command`` either.

    Attributes:
        timestamp: Local-time ISO 8601 string, normally from
            :func:`audit_timestamp` (Requirement 11.6).
        event: Which record point wrote this: :data:`EVENT_CONTROL`,
            :data:`EVENT_DENIED` or :data:`EVENT_ZOMBIE` (Requirement 11.6).
        command: Short name of the RPC method, for example ``"requeue"``.
        user: The call's ``takler-user`` metadata value, or
            :data:`UNKNOWN_USER` when it carried none (Requirement 11.8).
        peer: Network address of the caller, as gRPC reports it, for example
            ``"ipv4:10.0.0.9:51234"``.
        target: Every node path or flow name the command acted on
            (Requirement 11.9). Empty for a command with no target, such as a
            rejection that never got as far as parsing its request.
        outcome: :data:`OUTCOME_SUCCESS`, :data:`OUTCOME_ERROR`,
            :data:`OUTCOME_DENIED` or :data:`OUTCOME_ZOMBIE`
            (Requirement 11.7).
        error_code: The ``ServiceResponse.flag`` the RPC returned, or
            :data:`DENIED_ERROR_CODE` for a rejection (Requirement 11.7).
    """

    timestamp: str
    event: str
    command: str
    user: str
    peer: str
    target: List[str]
    outcome: str
    error_code: int

    def to_json_line(self) -> str:
        """Serialize to a single-line JSON object (Requirement 11.5).

        The returned text holds no line boundary of any kind, so writing it
        followed by one newline appends exactly one line to the Audit_File, and
        feeding that line back to :func:`json.loads` returns an object with all
        eight keys (the round-trip property of Requirement 11.10).

        ``ensure_ascii=False`` keeps non-ASCII node paths readable in the file
        instead of escaping them; the characters that this makes it possible to
        emit raw and that would still break a line are escaped afterwards, see
        :data:`_EXTRA_LINE_BOUNDARIES`.

        Returns:
            One line of JSON, with no trailing newline. Emitting the line is
            the logging backend's job.
        """
        line = json.dumps(dataclasses.asdict(self), ensure_ascii=False)
        for char in _EXTRA_LINE_BOUNDARIES:
            if char in line:
                line = line.replace(char, f"\\u{ord(char):04x}")
        return line


class AuditLogger:
    """The single write path for Audit_Records.

    Holds no file of its own: records go out through ``get_logger("audit")``,
    and the component name ``audit`` is what routes them to the Audit_File and
    keeps them out of the regular log (Requirements 11.1, 11.12). The
    Audit_File path is passed in only so that this class can pre-create the
    file with owner-only permissions and name it in a failure warning; the path
    must already have been handed to
    :func:`takler.logging.configure` for an audit sink to exist at all.

    One instance is shared by the three record points, which is why it carries
    no per-command state.
    """

    def __init__(
        self,
        audit_file: "Optional[Union[str, os.PathLike[str]]]" = None,
    ) -> None:
        """Build a logger for the given Audit_File.

        No file system access happens here: a server builds its Audit_Logger
        during start-up, and a start-up that fails because an audit directory
        is missing would trade a whole server for an observability detail. The
        directory and the file are created when the first record is written
        (Requirement 11.16), where a failure is already handled by degrading to
        a warning.

        Args:
            audit_file: The resolved Audit_File path, as
                :func:`takler.server.connect_config.resolve_audit_file`
                returns it, or ``None`` when no Audit_File is configured. With
                ``None`` the records still go out under the ``audit``
                component and land in whatever sinks the logging subsystem has
                (Requirement 11.13); there is simply nothing to pre-create.
        """
        self._audit_file: Optional[str] = (
            None if audit_file is None else os.fspath(audit_file)
        )
        # Whether the Audit_File has been pre-created (or found to exist).
        # Requirement 11.16 asks for this once, before the first record; the
        # backend's file handler keeps its descriptor open afterwards, so
        # repeating the check per record would cost a syscall and change
        # nothing about where the records go.
        self._prepared: bool = False

    @property
    def audit_file(self) -> Optional[str]:
        """The Audit_File path, or ``None`` when none is configured."""
        return self._audit_file

    def record(self, record: AuditRecord) -> None:
        """Write one Audit_Record; never fail the caller (Requirement 11.15).

        Every exception is caught and degraded to a WARNING on the regular
        logger, naming the Audit_File and the reason, so an RPC returns the
        same Service_Response whether or not its audit record made it to disk.
        The warning goes out under the ``server.audit`` component rather than
        ``audit``, which keeps it out of the Audit_File -- a file whose every
        line must be valid JSON is the wrong place to report that it cannot be
        written to.

        Args:
            record: The record to write.
        """
        try:
            self._prepare_audit_file()
            # Resolved per call rather than cached in ``__init__``: the adapter
            # belongs to the active logging backend, and ``configure()`` may
            # have switched backends since this instance was built.
            get_logger(AUDIT_COMPONENT).info(record.to_json_line())
        except Exception as exc:  # noqa: BLE001 - auditing must never propagate
            self._warn_write_failed(exc)

    def _prepare_audit_file(self) -> None:
        """Create the parent directory and the Audit_File, once.

        Requirement 11.16 is the parent directory; Requirement 11.14 is the
        file mode. The file is created here rather than by the backend's file
        handler because the handler would create it under the process umask and
        could only tighten it afterwards, leaving a window in which the audit
        trail is world readable. The backends open the audit file lazily for
        exactly this reason, so by the time the handler opens it the file
        already exists with :data:`AUDIT_FILE_MODE` and the handler appends.

        An Audit_File that already exists keeps its current mode: Requirement
        11.14 constrains the file *this* process creates, and silently
        rewriting the permissions of a file an operator deliberately placed
        there would be a surprise. ``O_EXCL`` is what distinguishes the two
        cases without a racy existence check.

        Raises:
            OSError: If the directory or the file cannot be created. The caller
                turns this into a warning.
        """
        if self._prepared or self._audit_file is None:
            return

        # Set before the work, not after: a path that cannot be created will
        # fail on every subsequent record too, and retrying it per command
        # would turn one warning into a flood of identical ones.
        self._prepared = True

        parent = Path(self._audit_file).parent
        # ``exist_ok`` covers both the common case and a concurrent creation.
        parent.mkdir(parents=True, exist_ok=True)

        try:
            fd = os.open(
                self._audit_file,
                os.O_CREAT | os.O_EXCL | os.O_APPEND | os.O_WRONLY,
                AUDIT_FILE_MODE,
            )
        except FileExistsError:
            return

        try:
            # ``os.open``'s mode argument is masked by the process umask, so
            # under a permissive umask the file would be born readable by
            # others. Tightening the descriptor -- not the path -- means the
            # file whose mode is fixed is provably the one just created.
            try:
                os.fchmod(fd, AUDIT_FILE_MODE)
            except (AttributeError, NotImplementedError):
                # Platform without ``fchmod``; fall back to the path, which
                # this process created exclusively a moment ago.
                os.chmod(self._audit_file, AUDIT_FILE_MODE)
        finally:
            # Closed immediately: this descriptor exists only to create the
            # file with the right mode. The records themselves are written
            # through the logging backend's own handler.
            os.close(fd)

    def _warn_write_failed(self, exc: BaseException) -> None:
        """Report a failed audit write on the regular logger (Req 11.15).

        Args:
            exc: The failure to describe. Its ``str()`` is included so the
                operator learns the reason (no space left, permission denied,
                missing directory) and not only that something went wrong.
        """
        destination = (
            repr(self._audit_file)
            if self._audit_file is not None
            else "the regular logging destinations"
        )
        try:
            get_logger("server.audit").warning(
                f"failed to write audit record to {destination}: {exc}"
            )
        except Exception:  # noqa: BLE001 - the fallback must be total too
            # The regular logger is unusable as well. There is nowhere left to
            # report to, and Requirement 11.15 is explicit that the audited RPC
            # must return what it would have returned anyway.
            pass
