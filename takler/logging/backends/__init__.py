"""Backend abstraction for the Takler logging subsystem.

This package normalizes the two very different logging libraries Takler can
run on -- the optional ``loguru`` library and the Python standard-library
``logging`` module -- behind a single :class:`LoggingBackend` interface. The
abstraction is the heart of the logging design: it fixes loguru's habit of
dropping the component name, handles per-backend level-name mapping, and lets
the rest of the subsystem stay backend-agnostic.

This module provides three things:

* :class:`LoggingBackend` -- the abstract base every concrete backend
  implements (``map_level``, ``apply_config``, ``get_named_logger``).
* :class:`NamedLogger` -- the adapter base returned by ``get_named_logger``,
  exposing a uniform ``trace``/``debug``/``info``/``warning``/``error``/
  ``critical`` surface regardless of backend (Requirement 8.3).
* :func:`get_backend` -- the process-wide backend selector. It attempts to
  import ``loguru`` exactly once; success selects the loguru backend, failure
  selects the standard-library backend. The choice is cached for the lifetime
  of the process and never re-evaluated (Requirements 9.1, 9.2).

It also exposes the pure :func:`map_level` helper that implements the generic
"nearest more-verbose supported level" rule (Requirement 2.5) shared by both
concrete backends.

The concrete backends (``LoguruBackend`` and ``StdlibBackend``) are imported
lazily inside :func:`get_backend` so that this module imports cleanly even
before those modules exist or when ``loguru`` is not installed.
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Iterable, Optional, Set, Union

from takler.logging.errors import ApplyResult
from takler.logging.levels import LEVEL_ORDER, LogLevel

if TYPE_CHECKING:
    from takler.logging.config import ResolvedConfig

__all__ = [
    "LoggingBackend",
    "NamedLogger",
    "map_level",
    "get_backend",
    "select_backend",
    "reset_backend",
]


def map_level(
    requested: LogLevel,
    supported: Iterable[LogLevel],
) -> LogLevel:
    """Map a requested level onto the nearest supported level.

    This implements the generic level-name mapping rule (Requirement 2.5):
    given the set of levels a backend supports, return the supported level
    with the greatest severity rank that is still less than or equal to the
    requested rank -- the nearest *more-verbose* supported level. When no
    supported level is at or below the requested rank (every supported level
    is more severe than what was requested), fall back to the nearest
    supported level overall, which is the supported level with the smallest
    rank (closest to the requested level).

    "More verbose" means a lower severity rank, toward
    :attr:`~takler.logging.levels.LogLevel.TRACE`. Ranks increase in the order
    TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL.

    Args:
        requested: The canonical level that was requested.
        supported: A non-empty iterable of the levels the backend supports.

    Returns:
        The supported :class:`~takler.logging.levels.LogLevel` selected by the
        rule above.

    Raises:
        ValueError: If ``supported`` is empty; a backend must support at least
            one level for the mapping to be well-defined.
    """
    supported_set: Set[LogLevel] = set(supported)
    if not supported_set:
        raise ValueError("map_level requires a non-empty set of supported levels")

    # Fast path: the exact level is supported.
    if requested in supported_set:
        return requested

    # Nearest more-verbose supported level: the greatest rank at or below the
    # requested rank. Scanning LEVEL_ORDER downward from the requested level
    # makes the "nearest toward TRACE" intent explicit.
    requested_index = LEVEL_ORDER.index(requested)
    for level in reversed(LEVEL_ORDER[: requested_index + 1]):
        if level in supported_set:
            return level

    # Nothing at or below the requested rank: every supported level is more
    # severe. Fall back to the nearest supported level overall, i.e. the one
    # with the smallest rank (closest to the requested level).
    return min(supported_set, key=lambda level: level.rank)


class NamedLogger(abc.ABC):
    """A component-attributed logger adapter with a uniform method surface.

    Instances are returned by :meth:`LoggingBackend.get_named_logger` and
    forward logging calls to the active backend with the component name bound,
    so that every record is attributed to the exact component name regardless
    of backend (Requirements 6.1, 6.2). The convenience methods
    (``trace``/``debug``/``info``/``warning``/``error``/``critical``) provide a
    consistent surface across backends (Requirement 8.3); calls below the
    configured level are suppressed by the backend and return control to the
    caller without raising (Requirement 8.4).

    Concrete backends implement :meth:`log`; the per-level convenience methods
    delegate to it so the level-to-method mapping lives in exactly one place.
    """

    def __init__(self, component: str) -> None:
        self.component = component

    @abc.abstractmethod
    def log(self, level: LogLevel, message: str, *args: object, **kwargs: object) -> None:
        """Emit a record at ``level`` attributed to this logger's component.

        Concrete implementations forward to the active backend. They must not
        raise when the record is suppressed by the configured level.

        Args:
            level: The canonical severity level of the record.
            message: The record's message text.
            *args: Optional positional arguments forwarded to the backend.
            **kwargs: Optional keyword arguments forwarded to the backend.
        """
        raise NotImplementedError

    def trace(self, message: str, *args: object, **kwargs: object) -> None:
        """Emit a record at :attr:`~takler.logging.levels.LogLevel.TRACE`."""
        self.log(LogLevel.TRACE, message, *args, **kwargs)

    def debug(self, message: str, *args: object, **kwargs: object) -> None:
        """Emit a record at :attr:`~takler.logging.levels.LogLevel.DEBUG`."""
        self.log(LogLevel.DEBUG, message, *args, **kwargs)

    def info(self, message: str, *args: object, **kwargs: object) -> None:
        """Emit a record at :attr:`~takler.logging.levels.LogLevel.INFO`."""
        self.log(LogLevel.INFO, message, *args, **kwargs)

    def warning(self, message: str, *args: object, **kwargs: object) -> None:
        """Emit a record at :attr:`~takler.logging.levels.LogLevel.WARNING`."""
        self.log(LogLevel.WARNING, message, *args, **kwargs)

    def error(self, message: str, *args: object, **kwargs: object) -> None:
        """Emit a record at :attr:`~takler.logging.levels.LogLevel.ERROR`."""
        self.log(LogLevel.ERROR, message, *args, **kwargs)

    def critical(self, message: str, *args: object, **kwargs: object) -> None:
        """Emit a record at :attr:`~takler.logging.levels.LogLevel.CRITICAL`."""
        self.log(LogLevel.CRITICAL, message, *args, **kwargs)


class LoggingBackend(abc.ABC):
    """Abstract base for a concrete logging backend.

    A backend adapts the canonical Takler logging model (levels, formatted
    records, resolved configuration) onto one underlying logging library. The
    two concrete implementations are ``LoguruBackend`` and ``StdlibBackend``;
    both produce equivalent named and formatted records so that behavior is
    consistent regardless of which backend is active (Requirements 9.3, 9.5).
    """

    @abc.abstractmethod
    def map_level(self, level: LogLevel) -> Union[str, int]:
        """Map a canonical level to this backend's representation.

        Substitutes the nearest more-verbose supported level when the exact
        level is unsupported (Requirement 2.5), typically by delegating to the
        module-level :func:`map_level` helper with this backend's supported
        set and then translating the result to the backend's own name or
        numeric value.

        Args:
            level: The canonical level to map.

        Returns:
            The backend-specific level representation (a level name or numeric
            value).
        """
        raise NotImplementedError

    @abc.abstractmethod
    def apply_config(self, config: "ResolvedConfig") -> ApplyResult:
        """Install the console/file sinks and set the level.

        Implementations are idempotent: they remove the sinks this module
        previously installed before installing the new set, so repeated
        configuration never accumulates duplicate destinations (Requirement
        1.4). This method never raises to the caller; it reports settings it
        could not apply via the returned :class:`~takler.logging.errors.ApplyResult`
        (Requirement 9.4).

        Args:
            config: The fully resolved configuration to apply.

        Returns:
            An :class:`~takler.logging.errors.ApplyResult` capturing the
            applied settings and any per-setting failures.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_named_logger(self, component: str) -> NamedLogger:
        """Return an adapter that emits records attributed to ``component``.

        Args:
            component: The component name to attribute records to.

        Returns:
            A :class:`NamedLogger` bound to ``component``.
        """
        raise NotImplementedError


# Module-level singleton holding the backend selected for this process. It is
# resolved lazily on the first call to :func:`get_backend` and then cached for
# the lifetime of the process (Requirements 9.1, 9.2).
_BACKEND: Optional[LoggingBackend] = None


def select_backend() -> LoggingBackend:
    """Select a concrete backend by probing for the optional ``loguru`` library.

    Attempts ``import loguru``; if it succeeds, the loguru backend is selected,
    otherwise the standard-library backend is selected (Requirements 9.1, 9.2).
    The concrete backend classes are imported lazily here so that this module
    imports cleanly regardless of whether ``loguru`` is installed and even
    before the concrete backend modules exist.

    Returns:
        A freshly constructed concrete :class:`LoggingBackend`.
    """
    try:
        import loguru  # noqa: F401
    except ImportError:
        from takler.logging.backends.stdlib_backend import StdlibBackend

        return StdlibBackend()
    else:
        from takler.logging.backends.loguru_backend import LoguruBackend

        return LoguruBackend()


def get_backend() -> LoggingBackend:
    """Return the process-wide active backend, selecting it once on first use.

    The selection runs a single time and is cached in a module-level
    singleton; subsequent calls return the same instance, keeping the backend
    choice fixed for the lifetime of the process (Requirements 9.1, 9.2).

    Returns:
        The active :class:`LoggingBackend` for this process.
    """
    global _BACKEND
    if _BACKEND is None:
        _BACKEND = select_backend()
    return _BACKEND


def reset_backend() -> None:
    """Clear the cached backend singleton.

    This is intended for tests that need to re-exercise selection; production
    code keeps the backend fixed for the process lifetime and does not call
    this.
    """
    global _BACKEND
    _BACKEND = None
