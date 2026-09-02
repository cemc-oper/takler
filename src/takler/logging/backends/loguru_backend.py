"""The loguru-backed implementation of the Takler logging backend.

This module provides :class:`LoguruBackend`, the concrete
:class:`~takler.logging.backends.LoggingBackend` used when the optional
``loguru`` library is installed. It is imported lazily by the backend selector
(:func:`takler.logging.backends.select_backend`) only after ``loguru`` has been
confirmed importable, so importing this module unconditionally requires
``loguru`` to be present.

Scope of this module (task 8.1):

* **Level mapping** -- :meth:`LoguruBackend.map_level` translates a canonical
  :class:`~takler.logging.levels.LogLevel` onto loguru's own level name. loguru
  natively supports the full canonical set (``TRACE`` .. ``CRITICAL``), so the
  generic "nearest more-verbose supported level" rule (Requirement 2.5) never
  substitutes for the canonical set here; the machinery is still routed through
  the shared :func:`~takler.logging.backends.map_level` helper as the required
  safety net.
* **Bind-name adapter** -- :meth:`LoguruBackend.get_named_logger` returns a
  :class:`LoguruNamedLogger` that wraps ``loguru.logger.bind(takler_name=...)``
  so the exact component name is carried on every record and is never dropped
  or replaced (Requirements 6.2, 9.5).
* **Console sink** -- :meth:`LoguruBackend.apply_config` installs a single
  stderr console sink rendering the shared canonical layout, and is idempotent:
  it removes only the loguru handler ids this module previously added before
  installing the new set, so repeated configuration never accumulates duplicate
  destinations (Requirements 4.1, 4.3, 1.4). It never raises to the caller and
  returns an :class:`~takler.logging.errors.ApplyResult`.
* **File sink** -- :meth:`LoguruBackend.apply_config` also installs a rotating
  file sink (via ``logger.add(path, rotation=..., retention=...)``) rendering
  the same canonical layout as the console sink, so file content is identical
  to console content (Requirement 5.1) with native size/time rotation and
  count/age retention (Requirements 5.3, 5.4). Missing parent directories are
  created first; if the parent cannot be created (Requirement 5.6) or the path
  cannot be opened (Requirement 5.5), the file sink is skipped, a record naming
  the path and failure is emitted to the console sink, the console sink
  continues, and the failure is reported via the returned result -- the method
  never raises.
* **Audit sink** -- when ``config.audit_file`` is set,
  :meth:`LoguruBackend.apply_config` installs a third sink that receives *only*
  the records bound to the ``audit`` component, while the console and file sinks
  gain a filter rejecting those same records; the isolation therefore runs in
  both directions (Requirements 11.1, 11.12). With no ``audit_file`` configured
  no audit sink exists and no isolation is applied, so audit records flow to the
  console / file sinks like any other component's (Requirement 11.13). The audit
  sink renders the bare ``{message}`` -- an Audit_Record is already a complete
  JSON object carrying its own ``timestamp`` key, so a prefix would make each
  line invalid JSON -- and opens its file lazily (``delay=True``) so the first
  writer can pre-create it with owner-only permissions (Requirement 11.14). Like
  every other sink it is tracked by handler id and torn down by the same
  idempotent mechanism (Requirement 1.4).
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, List, Mapping, Optional, Union

from loguru import logger as _loguru_logger

from takler.logging.backends import LoggingBackend, NamedLogger, map_level
from takler.logging.config import AUDIT_COMPONENT
from takler.logging.errors import ApplyResult, SettingFailure
from takler.logging.levels import LEVEL_ORDER, LogLevel

if TYPE_CHECKING:
    from takler.logging.config import ResolvedConfig

__all__ = ["LoguruBackend", "LoguruNamedLogger"]

# loguru natively supports the full canonical level set, so the supported set
# is simply every canonical level. The generic ``map_level`` helper still runs
# against this set to satisfy the substitution contract (Requirement 2.5),
# which is a no-op for the canonical names.
_SUPPORTED_LEVELS = frozenset(LEVEL_ORDER)

# Canonical record layout for the loguru sinks. It mirrors the shared layout
# produced by :func:`takler.logging.formatter.format_record`
# (``YYYY-MM-DDTHH:MM:SS.mmm±HH:MM LEVEL component message``) so that console
# and file output are byte-for-byte identical (Requirement 5.1) and consistent
# across backends (Requirement 9.5).
#
# * ``{time:YYYY-MM-DDTHH:mm:ss.SSSZ}`` renders an RFC 3339 / ISO 8601
#   timestamp with millisecond precision and an explicit UTC offset (for
#   example ``2026-06-30T11:38:10.123+08:00``). loguru's time tokens are
#   case-sensitive (Pendulum-style): ``MM``/``DD``/``HH`` are month/day/hour,
#   while minute and second MUST use the lowercase ``mm``/``ss`` tokens; ``SSS``
#   is the 3-digit millisecond fraction and ``Z`` the colon-separated offset.
#   This matches ``datetime.isoformat(timespec="milliseconds")`` used by the
#   shared formatter, keeping the two backends byte-identical (Requirement 3.3).
# * ``{level.name}`` renders the recognized severity name with no padding.
# * ``{extra[takler_name]}`` renders the exact bound component name, which is
#   how loguru's name-dropping is fixed (Requirement 6.2).
# * ``{message}`` renders the message text (empty messages still emit the
#   leading fields).
_CONSOLE_FORMAT = (
    "{time:YYYY-MM-DDTHH:mm:ss.SSSZ} {level.name} {extra[takler_name]} {message}"
)

# The file sink shares the console layout verbatim so that file content is
# byte-for-byte identical to console content (Requirement 5.1).
_FILE_FORMAT = _CONSOLE_FORMAT

# The audit sink renders the bare message with no prefix: an Audit_Record is
# already a complete JSON object with its own ``timestamp`` key, so any prefix
# would make the line invalid JSON (Requirement 11.5).
_AUDIT_FORMAT = "{message}"

# Component name used to attribute internal diagnostic records (for example the
# console warning emitted when a file sink cannot be established). It mirrors
# the root component name normalization used elsewhere in the subsystem.
_ROOT_COMPONENT = "takler"


def _is_audit_record(record: "Mapping[str, object]") -> bool:
    """Return whether a loguru record belongs to the audit component.

    The component name is carried in loguru's ``extra`` dict under
    ``takler_name`` (bound by :class:`LoguruNamedLogger`); records emitted by
    code that did not go through the adapter have no such key and are therefore
    not audit records.
    """
    extra = record.get("extra") or {}
    return extra.get("takler_name") == AUDIT_COMPONENT  # type: ignore[union-attr]


def _accept_audit_only(record: "Mapping[str, object]") -> bool:
    """Sink filter accepting only audit-component records (Req 11.12)."""
    return _is_audit_record(record)


def _reject_audit(record: "Mapping[str, object]") -> bool:
    """Sink filter rejecting audit-component records (Req 11.12)."""
    return not _is_audit_record(record)


class LoguruNamedLogger(NamedLogger):
    """A :class:`~takler.logging.backends.NamedLogger` backed by loguru.

    The adapter binds the component name into loguru's ``extra`` dict via
    ``logger.bind(takler_name=component)`` so that the canonical format string
    can render the exact component name on every record, regardless of where in
    the code the record originates (Requirements 6.1, 6.2). The uniform
    ``trace``/``debug``/.../``critical`` surface is inherited from the base
    class; only :meth:`log` is implemented here.
    """

    def __init__(self, component: str, backend: "LoguruBackend") -> None:
        super().__init__(component)
        self._backend = backend
        # Bind the component name into loguru's ``extra`` so the format string
        # ``{extra[takler_name]}`` resolves to this exact name.
        self._logger = _loguru_logger.bind(takler_name=component)

    def log(
        self, level: LogLevel, message: str, *args: object, **kwargs: object
    ) -> None:
        """Emit a record at ``level`` attributed to this logger's component.

        The canonical level is mapped onto loguru's level name and forwarded to
        loguru. Records below the configured sink level are suppressed by loguru
        itself and return control without raising (Requirement 8.4).
        """
        loguru_level = self._backend.map_level(level)
        self._logger.log(loguru_level, message, *args, **kwargs)


class LoguruBackend(LoggingBackend):
    """Concrete logging backend built on the optional ``loguru`` library.

    The backend owns the loguru sinks it installs and tracks their handler ids
    so that :meth:`apply_config` can tear down only its own sinks on each call,
    keeping reconfiguration idempotent (Requirement 1.4). loguru ships with a
    default stderr handler; this backend takes that handler over once so it does
    not duplicate the canonical console sink's output.
    """

    def __init__(self) -> None:
        self._logger = _loguru_logger
        # Handler ids this module has installed, in installation order. Only
        # these ids are removed on reconfiguration so other (non-Takler) loguru
        # handlers are left untouched.
        self._handler_ids: List[int] = []
        # loguru pre-installs a default stderr handler (id 0). We remove it once
        # on first configuration so our canonical console sink is the sole
        # stderr destination and records are not emitted twice.
        self._took_over_default = False

    def map_level(self, level: LogLevel) -> Union[str, int]:
        """Map a canonical level to loguru's level name.

        Routes through the shared :func:`~takler.logging.backends.map_level`
        helper against loguru's supported set (the full canonical set), then
        returns the loguru level *name*. The substitution is a no-op for the
        canonical names but remains in place as the Requirement 2.5 safety net.
        """
        mapped = map_level(level, _SUPPORTED_LEVELS)
        return mapped.name

    def apply_config(self, config: "ResolvedConfig") -> ApplyResult:
        """Install the console and file sinks and set their level for ``config``.

        Idempotent: every previously installed Takler sink is removed before the
        new set is installed, so repeated calls never accumulate duplicate
        destinations (Requirement 1.4). On the first call the backend also
        removes loguru's default stderr handler so console output is not
        duplicated. This method never raises to the caller; it returns an
        :class:`~takler.logging.errors.ApplyResult` carrying any per-setting
        failures (Requirements 5.5, 5.6).

        When ``config.log_file`` is set, a file sink rendering the same
        canonical layout as the console sink is installed alongside it
        (Requirement 5.1), with native size/time rotation and count/age
        retention applied when configured (Requirements 5.3, 5.4). If the file
        sink cannot be established -- the parent directory cannot be created
        (Requirement 5.6) or the path cannot be opened for writing
        (Requirement 5.5) -- the file sink is skipped, a record naming the path
        and failure is emitted to the console sink, the console sink continues,
        and the failure is reported via the returned result.
        """
        self._remove_installed_handlers()
        self._take_over_default_handler()

        failures: List[SettingFailure] = []

        if config.console:
            self._install_console_sink(config)

        if config.log_file is not None:
            failure = self._install_file_sink(config)
            if failure is not None:
                failures.append(failure)

        # Audit sink last, so a failure notice can still reach the console
        # sink installed above (Requirements 11.1, 11.12).
        if config.audit_file is not None:
            failure = self._install_audit_sink(config)
            if failure is not None:
                failures.append(failure)

        return ApplyResult(applied=config, failures=failures)

    def get_named_logger(self, component: str) -> NamedLogger:
        """Return a :class:`LoguruNamedLogger` bound to ``component``."""
        return LoguruNamedLogger(component, self)

    # -- internal helpers ---------------------------------------------------

    def _remove_installed_handlers(self) -> None:
        """Remove only the loguru handlers this module previously installed.

        Other loguru handlers (if any) are deliberately left in place so the
        backend cooperates with code that manages its own loguru sinks.
        """
        for handler_id in self._handler_ids:
            try:
                self._logger.remove(handler_id)
            except ValueError:
                # Already removed (for example by a direct loguru call); ignore.
                pass
        self._handler_ids.clear()

    def _take_over_default_handler(self) -> None:
        """Remove loguru's pre-installed default stderr handler once.

        loguru auto-installs a default handler (id 0) on import. Leaving it in
        place would emit every record twice on stderr -- once in loguru's
        default format and once in the canonical layout -- so it is removed a
        single time on first configuration (Requirement 1.4).
        """
        if self._took_over_default:
            return
        try:
            self._logger.remove(0)
        except ValueError:
            # No default handler present (already removed); nothing to do.
            pass
        self._took_over_default = True

    def _install_console_sink(self, config: "ResolvedConfig") -> None:
        """Install a single stderr console sink rendering the canonical layout.

        The sink filters at the configured level mapped onto loguru's level
        name, so records below the configured level are suppressed
        (Requirements 4.1, 4.3). Colorization is disabled so the file and
        console layouts stay identical plain text (Requirement 5.1).
        """
        add_kwargs: dict = {
            "level": self.map_level(config.level),
            "format": _CONSOLE_FORMAT,
            "colorize": False,
        }
        if config.audit_file is not None:
            # Keep audit records off the console (Requirement 11.12).
            add_kwargs["filter"] = _reject_audit

        handler_id = self._logger.add(sys.stderr, **add_kwargs)
        self._handler_ids.append(handler_id)

    def _install_file_sink(self, config: "ResolvedConfig") -> Optional[SettingFailure]:
        """Install a rotating file sink, handling open/create failures gracefully.

        The file sink renders the shared canonical layout so its content is
        identical to the console sink's (Requirement 5.1) and filters at the
        configured level. ``rotation`` and ``retention`` are passed straight
        through to loguru when provided -- loguru natively accepts size strings
        (``"10 MB"``), time strings, and integers -- and omitted when ``None``
        so loguru applies its "no rotation"/"keep all" defaults (Requirements
        5.3, 5.4).

        Missing parent directories are created explicitly before the sink is
        added so that a creation failure can be caught and handled per
        Requirement 5.6 (loguru can create parents itself, but doing it here
        makes the failure observable). If the parent directory cannot be
        created (Requirement 5.6) or the path cannot be opened for writing
        (Requirement 5.5), the file sink is not established: a record naming the
        path and failure is emitted to the console sink, the console sink
        continues, and a :class:`~takler.logging.errors.SettingFailure` is
        returned. This method never raises to the caller.

        Args:
            config: The resolved configuration whose ``log_file`` is set.

        Returns:
            ``None`` when the file sink was installed, or a
            :class:`SettingFailure` describing why it could not be.
        """
        path = config.log_file
        assert path is not None  # guarded by the caller

        # Create missing parent directories explicitly so a failure here is
        # caught and handled rather than surfacing from loguru (Requirement 5.6).
        parent = os.path.dirname(os.path.abspath(path))
        try:
            if parent:
                os.makedirs(parent, exist_ok=True)
        except OSError as exc:
            reason = f"could not create parent directory for log file {path!r}: {exc}"
            self._report_file_sink_failure(reason)
            return SettingFailure("log_file", reason)

        add_kwargs: dict = {
            "level": self.map_level(config.level),
            "format": _FILE_FORMAT,
            "colorize": False,
        }
        if config.audit_file is not None:
            # Keep audit records out of the regular log file (Requirement 11.12).
            add_kwargs["filter"] = _reject_audit
        # Pass rotation/retention straight through only when provided; omitting
        # the kwarg lets loguru apply its no-rotation / keep-all defaults.
        if config.rotation is not None:
            add_kwargs["rotation"] = config.rotation
        if config.retention is not None:
            add_kwargs["retention"] = config.retention

        try:
            handler_id = self._logger.add(path, **add_kwargs)
        except Exception as exc:  # noqa: BLE001 - never raise to the caller
            reason = f"could not open log file {path!r} for writing: {exc}"
            self._report_file_sink_failure(reason)
            return SettingFailure("log_file", reason)

        # Track the file sink so reconfiguration removes it (Requirement 1.4).
        self._handler_ids.append(handler_id)
        return None

    def _install_audit_sink(self, config: "ResolvedConfig") -> Optional[SettingFailure]:
        """Install the audit sink, handling ``add`` failures gracefully.

        The sink accepts only records bound to the ``audit`` component (the
        console and file sinks reject those same records), renders the bare
        ``{message}`` so each line stays valid JSON, and defers opening the file
        to the first record (``delay=True``) so the first writer can pre-create
        it with owner-only permissions rather than loguru creating it under the
        process umask (Requirements 11.1, 11.12, 11.14). The handler id is
        tracked so reconfiguration removes it like any other sink
        (Requirement 1.4).

        Args:
            config: The resolved configuration whose ``audit_file`` is set.

        Returns:
            ``None`` when the audit sink was installed, or a
            :class:`SettingFailure` describing why it could not be.
        """
        path = config.audit_file
        assert path is not None  # guarded by the caller

        try:
            handler_id = self._logger.add(
                path,
                level=self.map_level(config.level),
                format=_AUDIT_FORMAT,
                filter=_accept_audit_only,
                delay=True,
                colorize=False,
            )
        except Exception as exc:  # noqa: BLE001 - never raise to the caller
            reason = f"could not establish audit sink at {path!r}: {exc}"
            self._report_file_sink_failure(reason)
            return SettingFailure("audit_file", reason)

        self._handler_ids.append(handler_id)
        return None

    def _report_file_sink_failure(self, reason: str) -> None:
        """Emit a WARNING naming the failure to the (already-installed) console.

        The record is emitted through the loguru logger bound to the root
        component so it flows to the console sink installed earlier in
        :meth:`apply_config`, allowing the console sink to continue while the
        file sink is skipped (Requirements 5.5, 5.6).
        """
        self._logger.bind(takler_name=_ROOT_COMPONENT).warning(reason)
