"""Canonical log level model for the Takler logging subsystem.

This module defines a backend-independent severity model used throughout the
logging package. It exists so that the severity-ordering rules
(Requirements 2.1, 2.2) and case-insensitive name parsing (Requirement 2.4)
live in one pure, testable place, independent of either the ``loguru`` or the
standard-library ``logging`` backend.

The canonical levels and their severity ranks are:

==========  ====
Name        Rank
==========  ====
TRACE       5
DEBUG       10
INFO        20
WARNING     30
ERROR       40
CRITICAL    50
==========  ====

Severity rank increases in the order TRACE, DEBUG, INFO, WARNING, ERROR,
CRITICAL. A record at level ``L`` is emitted iff ``rank(L) >= rank(configured)``.
"""

from __future__ import annotations

import enum
from typing import List

from takler.logging.errors import InvalidLogLevelError

__all__ = [
    "LogLevel",
    "LEVEL_ORDER",
    "is_enabled",
]


class LogLevel(enum.IntEnum):
    """A canonical, ordered logging severity level.

    The integer values double as severity ranks, so the enum members compare
    in severity order out of the box (``LogLevel.DEBUG < LogLevel.INFO``). The
    values intentionally mirror the standard-library ``logging`` numeric levels
    (with a custom ``TRACE=5`` below ``DEBUG``) so the stdlib backend can map
    them with an identity transform.
    """

    TRACE = 5
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50

    @property
    def rank(self) -> int:
        """The severity rank of this level (higher means more severe)."""
        return int(self.value)

    @classmethod
    def parse(cls, value: "str | LogLevel") -> "LogLevel":
        """Parse a severity name into a :class:`LogLevel`.

        Name matching is case-insensitive, so ``"info"``, ``"INFO"``, and
        ``"Info"`` all resolve to :attr:`LogLevel.INFO` (Requirement 2.4).
        Surrounding whitespace is ignored. A :class:`LogLevel` value is
        returned unchanged for convenience.

        Args:
            value: A recognized severity name (any letter case) or an existing
                :class:`LogLevel`.

        Returns:
            The matching :class:`LogLevel` member.

        Raises:
            InvalidLogLevelError: If ``value`` is not a recognized severity
                name. The error identifies the offending value
                (Requirement 2.3).
        """
        if isinstance(value, LogLevel):
            return value

        if not isinstance(value, str):
            raise InvalidLogLevelError(value)

        normalized = value.strip().upper()
        member = cls.__members__.get(normalized)
        if member is None:
            raise InvalidLogLevelError(value)
        return member


# Canonical, ordered list of levels from least to most severe. Used wherever a
# stable severity ordering is required (filtering and level-name mapping).
LEVEL_ORDER: List[LogLevel] = [
    LogLevel.TRACE,
    LogLevel.DEBUG,
    LogLevel.INFO,
    LogLevel.WARNING,
    LogLevel.ERROR,
    LogLevel.CRITICAL,
]


def is_enabled(record_level: LogLevel, configured_level: LogLevel) -> bool:
    """Return whether a record should be emitted given the configured level.

    A record is emitted if and only if its severity rank is greater than or
    equal to the configured level's rank, where ranks increase in the order
    TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL (Requirements 2.1, 2.2).

    Args:
        record_level: The severity level of the record being considered.
        configured_level: The currently configured threshold level.

    Returns:
        ``True`` if the record is at or above the configured level, otherwise
        ``False``.
    """
    return record_level.rank >= configured_level.rank
