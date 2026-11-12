"""Property-based test for blank/missing name normalization (Property 7).

Covers Property 7 from the logging-enhancement design: any input to
``get_logger`` that is ``None``, absent, empty, or whitespace-only normalizes
to the root Takler component name ``"takler"`` (Requirement 6.3).

Normalization happens inside ``get_logger`` *before* the active backend is
asked for a named logger, so this property is backend-independent. The test
therefore exercises whichever backend ``get_backend()`` selects for the
process; both backends' ``NamedLogger`` set ``self.component`` to the resolved
component name, so the easiest observable assertion is
``get_logger(<blank>).component == "takler"``.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from takler.logging import ROOT_COMPONENT, get_logger

# Whitespace-only strings: spaces, tabs, newlines, carriage returns, vertical
# tabs, and form feeds, mixed in any combination. ``strip()`` treats all of
# these as whitespace, so every generated value must normalize to the root
# component name (Requirement 6.3).
_WHITESPACE_ONLY = st.text(
    alphabet=" \t\n\r\x0b\x0c",
    min_size=1,
    max_size=10,
)


# Feature: logging-enhancement, Property 7: Blank or missing names normalize to the root component
# Validates: Requirements 6.3
@settings(max_examples=100)
@given(name=_WHITESPACE_ONLY)
def test_whitespace_only_names_normalize_to_root(name: str) -> None:
    """Any whitespace-only name resolves to the root component ``"takler"``."""
    logger = get_logger(name)
    assert logger.component == ROOT_COMPONENT


def test_none_name_normalizes_to_root() -> None:
    """An explicit ``None`` argument resolves to the root component."""
    assert get_logger(None).component == ROOT_COMPONENT


def test_absent_name_normalizes_to_root() -> None:
    """A no-argument call resolves to the root component."""
    assert get_logger().component == ROOT_COMPONENT


def test_empty_string_name_normalizes_to_root() -> None:
    """An empty-string name resolves to the root component."""
    assert get_logger("").component == ROOT_COMPONENT
