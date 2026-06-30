"""Property-based test for non-string name rejection (Property 8).

Covers Property 8 from the logging-enhancement design: for any value that is
not a string, :func:`takler.logging.get_logger` raises an error that identifies
the invalid argument and returns no logger.

The design pins this down concretely (Requirement 6.5 / Error Handling table):
a non-string ``name`` raises :class:`TypeError` whose message names the ``name``
argument, and because the call raises, no logger object is ever returned.

This behavior lives entirely in the public ``get_logger`` boundary and does not
depend on the active backend, so the test exercises ``get_logger`` directly
rather than parametrizing over backends.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from takler.logging import get_logger


def _non_string_values() -> st.SearchStrategy[object]:
    """Generate values that are neither ``None`` nor ``str``.

    Covers a representative spread of non-string types: booleans, integers,
    floats (including NaN/inf), bytes, lists, dictionaries, tuples, and plain
    objects. ``None`` is excluded because it is an accepted input (it
    normalizes to the root component name), and any ``str`` instances are
    filtered out so the strategy only ever yields genuinely non-string values.
    """
    strategy = st.one_of(
        st.booleans(),
        st.integers(),
        st.floats(allow_nan=True, allow_infinity=True),
        st.binary(),
        st.lists(st.integers()),
        st.dictionaries(st.text(), st.integers()),
        st.tuples(st.integers()),
        st.builds(object),
    )
    # Defensive: ensure no str (or None) slips through to keep the property's
    # precondition ("value is not a string") strictly satisfied.
    return strategy.filter(lambda v: v is not None and not isinstance(v, str))


# Feature: logging-enhancement, Property 8: Non-string names raise an identifying error
# Validates: Requirements 6.5
@settings(max_examples=100)
@given(value=_non_string_values())
def test_non_string_names_raise_identifying_error(value):
    """A non-string ``name`` raises a TypeError naming the argument; no logger.

    For any value that is not a string (and not ``None``):
    - calling ``get_logger(value)`` raises :class:`TypeError` (Requirement 6.5);
    - the raised error identifies the offending ``name`` argument; and
    - no logger object is produced (the raise prevents any return value).
    """
    returned = None
    with pytest.raises(TypeError) as exc_info:
        # Assigning the result documents intent; the call raises before it can
        # bind, so ``returned`` stays ``None`` (no logger is returned).
        returned = get_logger(value)

    # The error must identify the invalid argument.
    assert "name" in str(exc_info.value), (
        f"TypeError message should identify the 'name' argument, "
        f"got: {exc_info.value!r}"
    )

    # No logger object was returned (the call raised before binding a result).
    assert returned is None
