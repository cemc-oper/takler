"""Property-based tests for the backend level-mapping helper.

Covers Property 4 from the logging-enhancement design: the generic
"nearest more-verbose supported level" rule implemented by
:func:`takler.logging.backends.map_level` (Requirement 2.5).
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from takler.logging.backends import map_level
from takler.logging.levels import LogLevel


def _expected_mapping(requested: LogLevel, supported: set[LogLevel]) -> LogLevel:
    """Compute the expected mapped level independently of the implementation.

    Rule (Requirement 2.5): return the supported level with the greatest rank
    that is still less than or equal to the requested rank (the nearest
    more-verbose supported level). When no supported level is at or below the
    requested rank, fall back to the nearest supported level overall, i.e. the
    supported level with the smallest rank.
    """
    at_or_below = [level for level in supported if level.rank <= requested.rank]
    if at_or_below:
        return max(at_or_below, key=lambda level: level.rank)
    return min(supported, key=lambda level: level.rank)


# Feature: logging-enhancement, Property 4: Unsupported level maps to nearest more-verbose supported level
# Validates: Requirements 2.5
@settings(max_examples=100)
@given(
    requested=st.sampled_from(list(LogLevel)),
    supported=st.sets(st.sampled_from(list(LogLevel)), min_size=1),
)
def test_unsupported_level_maps_to_nearest_more_verbose(requested, supported):
    """map_level returns a supported level chosen by the nearest-rule.

    For any requested canonical level and any non-empty set of backend-supported
    levels, the result is always within the supported set and equals the
    supported level with the greatest rank at or below the requested rank
    (nearest more-verbose); when none is at or below, it equals the nearest
    supported level overall (smallest rank).
    """
    result = map_level(requested, supported)

    # The result must always be a supported level.
    assert result in supported

    # The result must match the rule, computed independently.
    assert result == _expected_mapping(requested, supported)

    # Sanity: when the requested level itself is supported, it is returned as-is.
    if requested in supported:
        assert result == requested
