"""Record formatting for the Takler logging subsystem.

This module provides :func:`format_record`, the single, side-effect-free
function that renders a log record into the canonical Takler layout shared by
every sink (console and file). Keeping the layout in one place guarantees that
console and file output are byte-for-byte identical (Requirement 5.1) and that
the field order, timestamp shape, and level token are consistent across both
backends (Requirements 3.1, 3.3, 3.4, 3.5, 9.5).

Canonical layout::

    YYYY-MM-DDTHH:MM:SS.mmm±HH:MM LEVEL component message

The fields appear in the order timestamp, then Log_Level name, then originating
component name, then message text, separated by single spaces. The timestamp is
an RFC 3339 / ISO 8601 string with millisecond precision and an explicit UTC
offset (for example ``2026-06-30T11:38:10.123+08:00``), so it is unambiguous
across time zones and parseable by standard log tooling.
"""

from __future__ import annotations

from datetime import datetime

from takler.logging.levels import LogLevel

__all__ = ["format_record"]

# Single space separates each field of the canonical layout.
_FIELD_SEPARATOR = " "


def format_record(
    ts: datetime,
    level: LogLevel,
    component: str,
    message: str,
) -> str:
    """Render a single log record into the canonical Takler layout.

    The output presents four fields in a fixed order separated by single
    spaces: the timestamp, the Log_Level name, the originating component
    name, and the message text (Requirement 3.1).

    The timestamp is an RFC 3339 / ISO 8601 string with millisecond precision
    and an explicit UTC offset -- ``YYYY-MM-DDTHH:MM:SS.mmm±HH:MM`` (for example
    ``2026-06-30T11:38:10.123+08:00``) -- applied identically to every record
    (Requirement 3.3). A naive ``ts`` (no ``tzinfo``) is interpreted as local
    time and given the system's current UTC offset so the rendered timestamp is
    always zone-qualified. ``datetime.isoformat`` emits a four-digit,
    zero-padded year even for years before 1000.

    The Log_Level token is the recognized severity name of ``level`` (one of
    TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL) taken from the canonical
    :class:`~takler.logging.levels.LogLevel` model (Requirement 3.4).

    When ``message`` is empty, the leading timestamp, level, and component
    fields are still emitted; only the trailing message field is empty
    (Requirement 3.5).

    Args:
        ts: The record's timestamp. If naive, it is treated as local time.
        level: The record's canonical log level.
        component: The originating Named_Logger component name.
        message: The record's message text (may be empty).

    Returns:
        The formatted record string in the canonical Takler layout.
    """
    # Attach the local UTC offset to naive timestamps so the rendered value is
    # always zone-qualified (RFC 3339 requires an offset).
    if ts.tzinfo is None:
        ts = ts.astimezone()

    timestamp = ts.isoformat(timespec="milliseconds")

    level_name = level.name

    return _FIELD_SEPARATOR.join((timestamp, level_name, component, message))
