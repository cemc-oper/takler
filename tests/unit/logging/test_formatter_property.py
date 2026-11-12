"""Property-based tests for the canonical record formatter.

Covers Property 5 from the logging-enhancement design: the formatted record
produced by :func:`takler.logging.formatter.format_record` is consistent and
well-formed across all timestamps, levels, component names, and messages.
"""

from __future__ import annotations

import re
from datetime import datetime

from hypothesis import given, settings
from hypothesis import strategies as st

from takler.logging.formatter import format_record
from takler.logging.levels import LogLevel

# The recognized canonical severity names (Requirement 3.4).
RECOGNIZED_LEVEL_NAMES = {"TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

# RFC 3339 / ISO 8601 timestamp with millisecond precision and an explicit UTC
# offset (``±HH:MM``) or the ``Z`` zulu designator (Requirement 3.3).
RFC3339_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}([+-]\d{2}:\d{2}|Z)$"
)

# Modern-era datetimes. The range is kept within whole-minute-offset territory
# and well away from ``datetime.min``/``max``: ``format_record`` zone-qualifies
# naive timestamps via ``astimezone()``, which can overflow near the datetime
# bounds and yields sub-minute (LMT) offsets for pre-standardization years.
# A logging subsystem only ever stamps "now", so a modern range is faithful.
wide_range_datetimes = st.datetimes(
    min_value=datetime(1970, 1, 1, 0, 0, 0),
    max_value=datetime(2200, 12, 31, 23, 59, 59),
)

# Component names: non-empty text. The component itself must never contain a
# space, otherwise the single-space field layout would be ambiguous; component
# names in this subsystem are identifiers like "server.scheduler".
component_names = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-",
    min_size=1,
    max_size=64,
)

# Messages: arbitrary text, including the empty string (Requirement 3.5).
messages = st.text(max_size=128)


# Feature: logging-enhancement, Property 5: Record format is consistent and well-formed
# Validates: Requirements 3.1, 3.3, 3.4, 3.5, 9.5
@settings(max_examples=200)
@given(
    ts=wide_range_datetimes,
    level=st.sampled_from(list(LogLevel)),
    component=component_names,
    message=messages,
)
def test_record_format_is_consistent_and_well_formed(ts, level, component, message):
    """format_record renders timestamp|level|component|message consistently.

    For any timestamp, level, component name, and message (including empty):
    - the four fields appear in the order timestamp, level name, component,
      message (Requirement 3.1);
    - the timestamp is an RFC 3339 / ISO 8601 string with millisecond precision
      and an explicit UTC offset (Requirement 3.3);
    - the level token is one of the recognized severity names
      (Requirement 3.4); and
    - an empty message still produces the three leading fields
      (Requirement 3.5).
    """
    result = format_record(ts, level, component, message)

    # Fields are single-space separated. The message may itself contain spaces,
    # so split only the three leading fields off the front; the remainder is the
    # message field. The leading fields (timestamp, level, component) never
    # contain spaces, so an empty message still yields four parts with an empty
    # trailing message field (Requirement 3.5).
    parts = result.split(" ", 3)
    assert len(parts) == 4
    timestamp_field, level_field, component_field, message_field = parts

    # --- field order: timestamp, level, component, message (Req 3.1) ---
    # The formatter zone-qualifies a naive timestamp as local time, exactly as
    # the production call sites supply it, then renders it with millisecond
    # precision via ``datetime.isoformat``.
    expected_dt = ts.astimezone() if ts.tzinfo is None else ts
    expected_timestamp = expected_dt.isoformat(timespec="milliseconds")
    assert timestamp_field == expected_timestamp
    assert level_field == level.name
    assert component_field == component
    assert message_field == message

    # --- timestamp is a well-formed RFC 3339 string (Req 3.3) ---
    assert RFC3339_RE.match(timestamp_field)
    # The year is always rendered as four zero-padded digits.
    assert len(timestamp_field.split("-", 1)[0]) == 4

    # --- level token is a recognized severity name (Req 3.4) ---
    assert level_field in RECOGNIZED_LEVEL_NAMES

    # --- reconstruct: the full record is exactly the joined fields ---
    assert result == " ".join((expected_timestamp, level.name, component, message))
