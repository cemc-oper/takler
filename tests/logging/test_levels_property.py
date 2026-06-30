"""Property-based tests for the canonical log level model.

Covers Property 3 from the logging-enhancement design: recognized severity
names parse case-insensitively, implemented by
:meth:`takler.logging.levels.LogLevel.parse`.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from takler.logging.levels import LogLevel

# The recognized canonical severity names.
LEVEL_NAMES = ["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


@st.composite
def recognized_name_with_random_case(draw: st.DrawFn):
    """Draw a recognized severity name plus a random case permutation of it.

    Returns a ``(canonical_name, permuted_name)`` tuple where ``permuted_name``
    is the same letters as ``canonical_name`` with each character independently
    upper- or lower-cased.
    """
    name = draw(st.sampled_from(LEVEL_NAMES))
    flips = draw(st.lists(st.booleans(), min_size=len(name), max_size=len(name)))
    permuted = "".join(
        ch.upper() if flip else ch.lower() for ch, flip in zip(name, flips)
    )
    return name, permuted


# Feature: logging-enhancement, Property 3: Severity names parse case-insensitively
# Validates: Requirements 2.4
@settings(max_examples=200)
@given(names=recognized_name_with_random_case())
def test_severity_names_parse_case_insensitively(names):
    """Any case permutation of a recognized name parses to the same LogLevel.

    For any recognized severity name and any case permutation of it, parsing
    yields the same :class:`LogLevel`, which equals the canonical member named
    by the (upper-cased) name.
    """
    canonical_name, permuted = names

    canonical_member = LogLevel[canonical_name]
    parsed_canonical = LogLevel.parse(canonical_name)
    parsed_permuted = LogLevel.parse(permuted)

    # The permuted form parses to the same level as the canonical name ...
    assert parsed_permuted == parsed_canonical
    # ... and both equal the canonical enum member.
    assert parsed_permuted == canonical_member
    assert parsed_canonical == canonical_member
