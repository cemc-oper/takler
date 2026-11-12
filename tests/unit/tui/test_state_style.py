"""Unit tests for :mod:`takler.tui.state_style`."""

from __future__ import annotations

import pytest

from takler.core.state import NodeStatus
from takler.tui.state_style import (
    STATE_BLOCK_STYLES,
    STATE_STYLES,
    SUSPENDED_BLOCK_STYLE,
    block_style_for,
    state_block,
    style_for,
)


def test_every_node_status_has_a_style() -> None:
    # The palette must cover the whole state machine; a new status
    # without an entry here would render in the fallback colour.
    for status in NodeStatus:
        assert status.name in STATE_STYLES
        assert status.name in STATE_BLOCK_STYLES


def test_unknown_state_falls_back_to_white() -> None:
    assert style_for("no-such-state") == "white"
    assert block_style_for("no-such-state") == "white"


def test_style_lookup_matches_tables() -> None:
    for state, style in STATE_STYLES.items():
        assert style_for(state) == style
    for state, style in STATE_BLOCK_STYLES.items():
        assert block_style_for(state) == style


def test_suspended_block_colour_is_distinct_from_state_colours() -> None:
    # The suspended overlay only reads as an overlay if it can't be
    # mistaken for an underlying state.
    assert SUSPENDED_BLOCK_STYLE not in STATE_BLOCK_STYLES.values()


def test_state_block_paints_two_glyphs_in_the_state_colour() -> None:
    text = state_block("active")
    assert text.plain == "■■ "
    # Both glyphs carry the state colour; the trailing space is unstyled.
    assert len(text.spans) == 1
    span = text.spans[0]
    assert span.start == 0 and span.end == 2
    assert str(span.style) == STATE_BLOCK_STYLES["active"]


def test_state_block_suspended_is_two_tone() -> None:
    text = state_block("queued", suspended=True)
    assert text.plain == "■■ "
    spans = text.spans
    assert len(spans) == 2
    assert (spans[0].start, spans[0].end) == (0, 1)
    assert str(spans[0].style) == SUSPENDED_BLOCK_STYLE
    assert (spans[1].start, spans[1].end) == (1, 2)
    assert str(spans[1].style) == STATE_BLOCK_STYLES["queued"]


def test_state_block_trailing_is_optional_and_unstyled() -> None:
    text = state_block("aborted", trailing="")
    assert text.plain == "■■"
    # Callers can append text without it inheriting the swatch colour.
    text.append("task1")
    assert text.plain == "■■task1"
    assert all(span.end <= 2 for span in text.spans)


@pytest.mark.parametrize("state", sorted(STATE_STYLES))
def test_state_block_renders_for_every_state(state: str) -> None:
    assert state_block(state).plain.startswith("■■")
    assert state_block(state, suspended=True).plain.startswith("■■")
