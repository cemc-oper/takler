"""Unit tests for :meth:`LogLevel.parse` edge cases.

These example-based tests pin down the concrete behavior of the canonical
log level parser (Task 2.3):

* every recognized severity name parses to the correct :class:`LogLevel`
  member, including mixed-case and surrounding-whitespace variants
  (Requirement 2.4);
* representative invalid strings raise :class:`InvalidLogLevelError`, and the
  raised error names the offending value (Requirements 2.3, 2.4).

They complement the property-based parsing test (Property 3) by covering
specific recognized names and concrete invalid inputs.
"""

from __future__ import annotations

import pytest

from takler.logging.errors import InvalidLogLevelError
from takler.logging.levels import LogLevel

# Each recognized canonical severity name paired with its expected member.
RECOGNIZED_NAMES = [
    ("TRACE", LogLevel.TRACE),
    ("DEBUG", LogLevel.DEBUG),
    ("INFO", LogLevel.INFO),
    ("WARNING", LogLevel.WARNING),
    ("ERROR", LogLevel.ERROR),
    ("CRITICAL", LogLevel.CRITICAL),
]


@pytest.mark.parametrize("name, expected", RECOGNIZED_NAMES)
def test_parse_recognized_uppercase_name(name: str, expected: LogLevel) -> None:
    """Each recognized name (as shipped, uppercase) parses to its member."""
    assert LogLevel.parse(name) is expected


@pytest.mark.parametrize("name, expected", RECOGNIZED_NAMES)
def test_parse_recognized_lowercase_name(name: str, expected: LogLevel) -> None:
    """Lowercase spellings parse to the same member (Requirement 2.4)."""
    assert LogLevel.parse(name.lower()) is expected


@pytest.mark.parametrize("name, expected", RECOGNIZED_NAMES)
def test_parse_recognized_mixed_case_name(name: str, expected: LogLevel) -> None:
    """Mixed-case spellings parse to the same member (Requirement 2.4)."""
    # Title-case (e.g. "Info", "Warning") is a representative mixed case.
    assert LogLevel.parse(name.title()) is expected


@pytest.mark.parametrize("name, expected", RECOGNIZED_NAMES)
def test_parse_strips_surrounding_whitespace(name: str, expected: LogLevel) -> None:
    """Surrounding whitespace is ignored when parsing a recognized name."""
    assert LogLevel.parse(f"  {name.lower()}\t") is expected


def test_parse_passes_through_loglevel_value() -> None:
    """An existing LogLevel is returned unchanged for convenience."""
    assert LogLevel.parse(LogLevel.WARNING) is LogLevel.WARNING


# Representative invalid strings: empty, near-misses, abbreviations, numeric
# strings, and typos. None of these are recognized severity names.
@pytest.mark.parametrize(
    "bad",
    ["", " ", "verbose", "warn", "10", "infoo", "notalevel", "DEBUGGER", "fatal"],
)
def test_parse_invalid_string_raises(bad: str) -> None:
    """Unrecognized strings raise InvalidLogLevelError (Requirement 2.3)."""
    with pytest.raises(InvalidLogLevelError):
        LogLevel.parse(bad)


@pytest.mark.parametrize(
    "bad",
    ["", "verbose", "warn", "10", "infoo", "notalevel", "fatal"],
)
def test_parse_invalid_string_error_names_value(bad: str) -> None:
    """The raised error identifies the offending value (Requirement 2.3).

    The error must both store the original value on ``.value`` and include a
    representation of it in the message text.
    """
    with pytest.raises(InvalidLogLevelError) as exc_info:
        LogLevel.parse(bad)

    error = exc_info.value
    # The offending value is preserved verbatim on the exception.
    assert error.value == bad
    # ...and is named in the human-readable message.
    assert repr(bad) in str(error)


def test_parse_non_string_raises_naming_value() -> None:
    """A non-string, non-LogLevel value raises and names the bad value."""
    with pytest.raises(InvalidLogLevelError) as exc_info:
        LogLevel.parse(123)  # type: ignore[arg-type]

    assert exc_info.value.value == 123
    assert repr(123) in str(exc_info.value)
