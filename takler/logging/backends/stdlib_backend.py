"""Standard-library logging backend for the Takler logging subsystem.

This module implements :class:`StdlibBackend`, the fallback backend used when
the optional ``loguru`` library is not installed (Requirement 9.2). It adapts
the canonical Takler logging model onto Python's standard-library ``logging``
module while producing records that are equivalent to the loguru backend's:
the same canonical levels, the same formatted layout, and the same exact
component attribution (Requirements 9.3, 9.5).

Design notes
------------

* **TRACE registration.** The standard library has no ``TRACE`` level, so the
  backend registers a custom ``TRACE=5`` via :func:`logging.addLevelName` once
  at construction. This makes the full canonical level set representable on
  stdlib, so :meth:`StdlibBackend.map_level` is an identity mapping onto the
  stdlib numeric levels (Requirement 2.5 needs no substitution here -- the
  generic substitution machinery in :mod:`takler.logging.backends` remains the
  safety net for artificially reduced level sets).

* **Exact component attribution.** Rather than relying on the dotted stdlib
  logger hierarchy name, the named-logger adapter attaches the requested
  component name to each record and the shared
  :func:`~takler.logging.formatter.format_record` renders that exact name. So
  ``get_named_logger("server.scheduler")`` emits records whose displayed
  component is exactly ``server.scheduler`` (Requirements 6.1, 9.5).

* **Idempotent configuration.** Every handler this backend installs is tagged
  with a sentinel attribute. :meth:`StdlibBackend.apply_config` removes only
  the handlers it previously attached to the ``takler`` logger before
  installing the new set, so repeated configuration never accumulates
  duplicate destinations (Requirement 1.4).

* **Never raises.** :meth:`StdlibBackend.apply_config` reports any setting it
  could not apply via the returned :class:`~takler.logging.errors.ApplyResult`
  instead of raising (Requirement 9.4).

File sink
---------

The file sink emits the *same* formatted content as the console sink by reusing
:class:`_TaklerStdlibFormatter` (Requirement 5.1). Missing parent directories
are created before the handler opens the file (Requirement 5.2). Rotation and
retention are derived from the resolved ``rotation``/``retention`` settings:

* **Rotation** (:meth:`_build_file_handler`):

  - ``None`` -> a plain :class:`logging.FileHandler` (no rotation).
  - an ``int`` -> a size threshold in bytes -> :class:`~logging.handlers.RotatingFileHandler`.
  - a *size string* such as ``"10 MB"``, ``"512KB"`` or a bare byte count like
    ``"1048576"`` -> :class:`~logging.handlers.RotatingFileHandler`. Recognized
    units are ``B``, ``KB``, ``MB``, ``GB``, ``TB`` (case-insensitive, optional
    space, 1024-based).
  - a *time-interval string* such as ``"1 day"``, ``"30 minutes"``,
    ``"1 hour"``, ``"midnight"`` or ``"1 week"`` -> a
    :class:`~logging.handlers.TimedRotatingFileHandler`. Recognized units are
    seconds/minutes/hours/days/weeks (singular or plural) plus the special
    ``midnight`` token; a week maps to a 7-day interval.

* **Retention** (:meth:`_retention_to_backup_count`) maps onto stdlib's
  ``backupCount``:

  - ``None`` -> ``0`` (keep all rotated files).
  - an ``int`` (or a bare-integer string such as ``"5"``) -> that many retained
    rotated files.
  - any other string -> ``0`` (keep all), since stdlib retention is expressed
    purely as a file count.

Graceful errors (Requirements 5.5, 5.6, 9.4): if the parent directory cannot be
created or the file cannot be opened for writing, the file sink is **not**
established, a record naming the path and the failure is emitted to the console
sink, the console sink continues, a :class:`SettingFailure` is recorded, and the
method never raises to the caller.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import re
import sys
from datetime import datetime
from typing import List, Optional, Tuple, Union

from takler.logging.backends import LoggingBackend, NamedLogger
from takler.logging.config import ResolvedConfig
from takler.logging.errors import ApplyResult, SettingFailure
from takler.logging.formatter import format_record
from takler.logging.levels import LEVEL_ORDER, LogLevel

__all__ = ["StdlibBackend"]

# The root logger name every Takler component logger descends from.
ROOT_LOGGER_NAME = "takler"

# Sentinel attribute marking a handler this module installed, so that
# reconfiguration removes only its own handlers (Requirement 1.4).
_MANAGED_HANDLER_FLAG = "_takler_managed_sink"

# Record attribute carrying the exact component name for the formatter.
_COMPONENT_ATTR = "takler_component"

# Size-unit multipliers (1024-based) for parsing rotation size strings such as
# ``"10 MB"``. A bare number with no unit is interpreted as a byte count.
_SIZE_UNITS = {
    "B": 1,
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
}

# ``<number><optional space><optional unit>``; the unit is matched
# case-insensitively against :data:`_SIZE_UNITS`.
_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([A-Za-z]*)\s*$")

# Human time-interval words -> the stdlib ``TimedRotatingFileHandler`` ``when``
# code and the multiplier applied to a parsed count. Weeks have no native
# ``when`` that also honours an interval cleanly, so a week is expressed as a
# 7-day interval.
_INTERVAL_UNITS = {
    "second": ("S", 1),
    "seconds": ("S", 1),
    "sec": ("S", 1),
    "minute": ("M", 1),
    "minutes": ("M", 1),
    "min": ("M", 1),
    "hour": ("H", 1),
    "hours": ("H", 1),
    "day": ("D", 1),
    "days": ("D", 1),
    "week": ("D", 7),
    "weeks": ("D", 7),
}

# ``<optional count><optional space><unit word>`` e.g. ``"1 day"``, ``"30 minutes"``.
_INTERVAL_RE = re.compile(r"^\s*(\d+)?\s*([A-Za-z]+)\s*$")


def _parse_size(value: str) -> Optional[int]:
    """Parse a rotation *size* string into a byte count, or ``None``.

    Recognizes a non-negative number optionally followed by one of the units
    ``B``/``KB``/``MB``/``GB``/``TB`` (case-insensitive, optional space,
    1024-based). A bare number is treated as a byte count. Returns ``None`` when
    ``value`` is not a size expression (so the caller can try a time interval).
    """
    match = _SIZE_RE.match(value)
    if match is None:
        return None

    number, unit = match.group(1), match.group(2).upper()
    if unit == "":
        unit = "B"
    if unit not in _SIZE_UNITS:
        return None

    return int(float(number) * _SIZE_UNITS[unit])


def _parse_interval(value: str) -> Optional[Tuple[str, int]]:
    """Parse a rotation *time-interval* string into ``(when, interval)``.

    Recognizes the special token ``"midnight"`` and ``<count> <unit>`` phrases
    where the unit is seconds/minutes/hours/days/weeks (singular or plural). The
    count defaults to ``1`` when omitted (e.g. ``"hour"`` == ``"1 hour"``).
    Returns ``None`` when ``value`` is not a recognized interval expression.
    """
    stripped = value.strip().lower()
    if stripped == "midnight":
        return ("midnight", 1)

    match = _INTERVAL_RE.match(stripped)
    if match is None:
        return None

    count_text, unit = match.group(1), match.group(2)
    if unit not in _INTERVAL_UNITS:
        return None

    count = int(count_text) if count_text else 1
    when, multiplier = _INTERVAL_UNITS[unit]
    interval = max(1, count * multiplier)
    return (when, interval)


def _retention_to_backup_count(retention: Optional[Union[str, int]]) -> int:
    """Translate a retention setting into a stdlib ``backupCount``.

    ``None`` keeps all rotated files (``0``); an ``int`` or bare-integer string
    is used directly as the retained-file count; any other string yields ``0``
    (keep all), since stdlib retention is expressed solely as a file count.
    """
    if retention is None:
        return 0
    if isinstance(retention, bool):
        # ``bool`` is an ``int`` subclass; treat it as "keep all" rather than 0/1.
        return 0
    if isinstance(retention, int):
        return max(0, retention)

    text = retention.strip()
    if text.isdigit():
        return int(text)
    return 0


def _levelno_to_loglevel(levelno: int) -> LogLevel:
    """Translate a stdlib numeric level back into a canonical :class:`LogLevel`.

    Records emitted through this backend always carry one of the canonical
    numeric values, so the exact match is the common path. For robustness
    against arbitrary numeric levels, fall back to the nearest more-verbose
    canonical level (the greatest canonical rank at or below ``levelno``),
    defaulting to the least-severe canonical level when ``levelno`` is below
    them all.

    Args:
        levelno: A standard-library numeric log level.

    Returns:
        The matching (or nearest more-verbose) canonical :class:`LogLevel`.
    """
    try:
        return LogLevel(levelno)
    except ValueError:
        at_or_below = [level for level in LEVEL_ORDER if level.value <= levelno]
        return at_or_below[-1] if at_or_below else LEVEL_ORDER[0]


class _TaklerStdlibFormatter(logging.Formatter):
    """A stdlib formatter that renders records via the shared layout.

    Delegating to :func:`~takler.logging.formatter.format_record` guarantees
    the stdlib backend's console (and, later, file) output is identical to the
    loguru backend's and uses the exact bound component name rather than the
    dotted logger hierarchy name (Requirements 3.1, 5.1, 6.1, 9.5).
    """

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created)
        level = _levelno_to_loglevel(record.levelno)
        component = getattr(record, _COMPONENT_ATTR, None) or record.name
        message = record.getMessage()
        return format_record(ts, level, component, message)


class _StdlibNamedLogger(NamedLogger):
    """A :class:`NamedLogger` adapter over a standard-library logger.

    Emits records through ``logging.getLogger("takler")`` while attaching the
    exact component name so the shared formatter attributes the record to that
    name (Requirements 6.1, 6.4). Calls below the configured level are filtered
    by the stdlib logger and return control to the caller without raising
    (Requirement 8.4).
    """

    def __init__(self, component: str, logger: logging.Logger) -> None:
        super().__init__(component)
        self._logger = logger

    def log(
        self, level: LogLevel, message: str, *args: object, **kwargs: object
    ) -> None:
        numeric_level = int(level.value)

        # Merge any caller-supplied ``extra`` with the bound component name so
        # the formatter can render the exact component (Requirement 6.1).
        extra = dict(kwargs.pop("extra", None) or {})
        extra[_COMPONENT_ATTR] = self.component

        self._logger.log(numeric_level, message, *args, extra=extra, **kwargs)


class StdlibBackend(LoggingBackend):
    """Logging backend built on the Python standard-library ``logging`` module.

    Selected when ``loguru`` is unavailable (Requirement 9.2). Produces records
    with the canonical level set, the shared formatted layout, and exact
    component attribution so behavior matches the loguru backend
    (Requirements 9.3, 9.5).
    """

    def __init__(self) -> None:
        # Register the custom TRACE level so the full canonical set is
        # representable on stdlib (the design's TRACE=5). ``addLevelName`` is
        # idempotent and safe to call repeatedly.
        logging.addLevelName(int(LogLevel.TRACE.value), LogLevel.TRACE.name)

    def map_level(self, level: LogLevel) -> int:
        """Map a canonical level to its stdlib numeric value (identity).

        Because the backend registers ``TRACE=5`` and the canonical level
        values already mirror the stdlib numeric levels, the mapping is the
        identity transform and no substitution is required (Requirement 2.5).

        Args:
            level: The canonical level to map.

        Returns:
            The stdlib numeric level value.
        """
        return int(level.value)

    def get_named_logger(self, component: str) -> NamedLogger:
        """Return an adapter that attributes records to ``component`` exactly.

        Args:
            component: The component name to attribute records to.

        Returns:
            A :class:`NamedLogger` bound to ``component`` over the ``takler``
            logger.
        """
        logger = logging.getLogger(ROOT_LOGGER_NAME)
        return _StdlibNamedLogger(component, logger)

    def apply_config(self, config: ResolvedConfig) -> ApplyResult:
        """Install the console sink and set the level on the ``takler`` logger.

        Idempotent: the handlers this module previously attached are removed
        before the new set is installed, so reconfiguration never duplicates
        destinations (Requirement 1.4). Never raises to the caller; any setting
        that cannot be applied is reported in the returned
        :class:`~takler.logging.errors.ApplyResult` (Requirement 9.4).

        Args:
            config: The fully resolved configuration to apply.

        Returns:
            An :class:`ApplyResult` capturing the applied configuration and any
            per-setting failures.
        """
        failures: List[SettingFailure] = []
        logger = logging.getLogger(ROOT_LOGGER_NAME)

        # Remove only the handlers this module installed previously so repeated
        # configuration does not accumulate duplicate sinks (Requirement 1.4).
        self._remove_managed_handlers(logger)

        # Keep records on the ``takler`` logger's own handlers only; without
        # this they would also propagate to the root logger and risk a second
        # emission per record.
        logger.propagate = False

        # Apply the level. On the unlikely failure, fall back to INFO so the
        # console keeps emitting (Requirement 9.4).
        try:
            logger.setLevel(self.map_level(config.level))
        except Exception as exc:  # noqa: BLE001 - never raise to the caller
            failures.append(SettingFailure("level", str(exc)))
            logger.setLevel(self.map_level(LogLevel.INFO))

        # Console sink (stderr), active when enabled (Requirements 4.1, 4.3).
        console_handler: Optional[logging.Handler] = None
        if config.console:
            try:
                console_handler = self._make_console_handler(config.level)
                logger.addHandler(console_handler)
            except Exception as exc:  # noqa: BLE001 - never raise to the caller
                console_handler = None
                failures.append(SettingFailure("console", str(exc)))

        # File sink with rotation/retention/parent-dir creation and graceful
        # error handling (Requirements 5.1-5.6, 9.4). On any failure we do not
        # establish the file sink, emit a console record naming the path and the
        # failure, record a SettingFailure, and continue the console sink --
        # never raising to the caller.
        if config.log_file:
            self._install_file_sink(logger, config, console_handler, failures)

        return ApplyResult(applied=config, failures=failures)

    def _install_file_sink(
        self,
        logger: logging.Logger,
        config: ResolvedConfig,
        console_handler: Optional[logging.Handler],
        failures: List[SettingFailure],
    ) -> None:
        """Establish the managed file sink, degrading gracefully on failure.

        Creates any missing parent directories (Requirement 5.2), builds a
        rotating/timed/plain handler from ``config.rotation``/``config.retention``
        sharing the console formatter so file content matches the console
        exactly (Requirement 5.1), and tags it as managed so reconfiguration
        removes only this module's handlers (Requirement 1.4).

        On a parent-directory creation failure (Requirement 5.6), a file-open
        failure (Requirement 5.5), or any other stdlib configuration/permission
        error (Requirement 9.4): the file sink is not established, a record
        naming the path and the failure is emitted to the console sink, a
        :class:`SettingFailure` is recorded, and the method returns without
        raising.

        Args:
            logger: The ``takler`` root logger to attach the file handler to.
            config: The resolved configuration (provides path/rotation/retention).
            console_handler: The managed console handler, used to emit the
                failure notice; ``None`` when the console is disabled.
            failures: The list to append a :class:`SettingFailure` to on error.
        """
        path = config.log_file
        assert path is not None  # guarded by the caller

        # Create missing parent directories before the handler opens the file
        # (Requirement 5.2). A failure here is Requirement 5.6.
        parent = os.path.dirname(path)
        if parent:
            try:
                os.makedirs(parent, exist_ok=True)
            except OSError as exc:
                reason = f"could not create parent directory: {exc}"
                self._report_file_sink_failure(console_handler, path, reason, failures)
                return

        # Build and open the handler. An open/permission failure is
        # Requirement 5.5; a bad rotation spec or any other stdlib error is
        # handled the same graceful way (Requirement 9.4).
        try:
            handler = self._build_file_handler(path, config.rotation, config.retention)
        except OSError as exc:
            reason = f"could not open file for writing: {exc}"
            self._report_file_sink_failure(console_handler, path, reason, failures)
            return
        except Exception as exc:  # noqa: BLE001 - never raise to the caller
            reason = f"could not configure file sink: {exc}"
            self._report_file_sink_failure(console_handler, path, reason, failures)
            return

        handler.setLevel(int(config.level.value))
        handler.setFormatter(_TaklerStdlibFormatter())
        setattr(handler, _MANAGED_HANDLER_FLAG, True)
        logger.addHandler(handler)

    @staticmethod
    def _build_file_handler(
        path: str,
        rotation: Optional[Union[str, int]],
        retention: Optional[Union[str, int]],
    ) -> logging.Handler:
        """Construct the appropriate (rotating/timed/plain) file handler.

        See the module docstring for the rotation/retention parsing scheme.
        The handler opens its file eagerly (``delay`` left at its default) so
        that an unopenable path surfaces as an ``OSError`` here, letting the
        caller degrade gracefully (Requirement 5.5).

        Args:
            path: The log file path.
            rotation: ``None`` (no rotation), an ``int`` byte threshold, a size
                string, or a time-interval string.
            retention: Retained rotated-file count (or ``None`` to keep all).

        Returns:
            A configured :class:`logging.Handler` writing to ``path``.

        Raises:
            OSError: If the file cannot be opened for writing.
        """
        backup_count = _retention_to_backup_count(retention)

        if rotation is None:
            return logging.FileHandler(path, encoding="utf-8")

        if isinstance(rotation, bool):
            # ``bool`` is an ``int`` subclass; it is not a meaningful size, so
            # treat it as "no rotation".
            return logging.FileHandler(path, encoding="utf-8")

        if isinstance(rotation, int):
            return logging.handlers.RotatingFileHandler(
                path,
                maxBytes=max(0, rotation),
                backupCount=backup_count,
                encoding="utf-8",
            )

        # ``rotation`` is a string: try size first, then a time interval.
        size = _parse_size(rotation)
        if size is not None:
            return logging.handlers.RotatingFileHandler(
                path,
                maxBytes=size,
                backupCount=backup_count,
                encoding="utf-8",
            )

        interval = _parse_interval(rotation)
        if interval is not None:
            when, count = interval
            return logging.handlers.TimedRotatingFileHandler(
                path,
                when=when,
                interval=count,
                backupCount=backup_count,
                encoding="utf-8",
            )

        # Unrecognized rotation spec: fall back to a plain (non-rotating) file
        # handler so a typo never silently disables logging to the file.
        return logging.FileHandler(path, encoding="utf-8")

    @staticmethod
    def _report_file_sink_failure(
        console_handler: Optional[logging.Handler],
        path: str,
        reason: str,
        failures: List[SettingFailure],
    ) -> None:
        """Record a file-sink failure and emit a notice to the console sink.

        Appends a :class:`SettingFailure` for the ``log_file`` setting and, when
        a console handler is present, emits a WARNING record naming the path and
        the failure directly through that handler so it appears regardless of
        the configured threshold (Requirements 5.5, 5.6, 9.4). Never raises.

        Args:
            console_handler: The managed console handler, or ``None`` when the
                console is disabled.
            path: The configured log file path that could not be established.
            reason: A human-readable description of the failure.
            failures: The list to append the :class:`SettingFailure` to.
        """
        message = f"could not establish file sink at {path!r}: {reason}"
        failures.append(SettingFailure("log_file", message))

        if console_handler is None:
            return

        record = logging.LogRecord(
            name=ROOT_LOGGER_NAME,
            level=logging.WARNING,
            pathname=__file__,
            lineno=0,
            msg=message,
            args=None,
            exc_info=None,
        )
        setattr(record, _COMPONENT_ATTR, ROOT_LOGGER_NAME)
        try:
            console_handler.handle(record)
        except Exception:  # noqa: BLE001 - notifying must never raise
            pass

    @staticmethod
    def _make_console_handler(level: LogLevel) -> logging.Handler:
        """Build a tagged stderr console handler using the shared formatter.

        Args:
            level: The level threshold to apply to the handler.

        Returns:
            A configured :class:`logging.StreamHandler` tagged as managed by
            this module.
        """
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(int(level.value))
        handler.setFormatter(_TaklerStdlibFormatter())
        setattr(handler, _MANAGED_HANDLER_FLAG, True)
        return handler

    @staticmethod
    def _remove_managed_handlers(logger: logging.Logger) -> None:
        """Detach and close the handlers this module previously installed.

        Args:
            logger: The logger whose managed handlers should be removed.
        """
        for handler in list(logger.handlers):
            if getattr(handler, _MANAGED_HANDLER_FLAG, False):
                logger.removeHandler(handler)
                try:
                    handler.close()
                except Exception:  # noqa: BLE001 - cleanup must not raise
                    pass
