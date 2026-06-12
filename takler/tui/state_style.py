"""Single source of truth for node-state colours used across the TUI.

Palette mirrors ecFlow UI conventions:

* ``unknown``   — dim grey (uninitialised tree)
* ``queued``    — light sky blue (waiting on dependencies)
* ``submitted`` — deep sky blue (in flight, dispatched)
* ``active``    — green (running)
* ``complete``  — yellow (finished successfully)
* ``aborted``   — red (failed)
"""
from __future__ import annotations

from typing import Dict

from rich.text import Text


STATE_STYLES: Dict[str, str] = {
    "unknown": "dim",
    "queued": "bold light_sky_blue1",
    "submitted": "bold cyan1",
    "active": "bold green",
    "complete": "bold yellow",
    "aborted": "bold red",
}


# Foreground colour only, used when painting solid block glyphs that
# represent the state. ``STATE_STYLES`` keeps the ``bold`` / ``dim``
# weight which is appropriate for text but reads as visual noise on a
# filled block, so we keep colours-only here.
STATE_BLOCK_STYLES: Dict[str, str] = {
    "unknown": "grey50",
    "queued": "light_sky_blue1",
    "submitted": "cyan1",
    "active": "green",
    "complete": "yellow1",
    "aborted": "red",
}

# Dedicated colour for the leading half of a suspended swatch. Picked
# to be distinct from every entry in ``STATE_BLOCK_STYLES`` so the
# overlay reads as "suspended on top of <underlying state>".
SUSPENDED_BLOCK_STYLE: str = "orange1"


def style_for(state: str) -> str:
    """Return a Rich style string for the given state name."""
    return STATE_STYLES.get(state, "white")


def block_style_for(state: str) -> str:
    """Return the Rich colour used by :func:`state_block` for ``state``."""
    return STATE_BLOCK_STYLES.get(state, "white")


# Block glyphs used as a colour swatch in tree rows / status bar.
# We use ``■`` (Black Square, U+25A0) — a vertically centred filled
# square that leaves whitespace above and below within the cell, so
# adjacent rows no longer fuse into one tall coloured strip.
_BLOCK_GLYPH = "■"


def state_block(state: str, *, suspended: bool = False, trailing: str = " ") -> Text:
    """A small coloured rectangle representing ``state``.

    Parameters
    ----------
    state
        Node state name (e.g. ``"active"``).
    suspended
        When ``True``, paint the first half of the swatch in
        :data:`SUSPENDED_BLOCK_STYLE` while the second half keeps the
        underlying state's colour, producing a two-tone badge that
        encodes "suspended on top of <state>".
    trailing
        String appended after the block, typically a single space so
        the following text doesn't run into the swatch.

    Returns
    -------
    rich.text.Text
        Coloured block ready to be concatenated with other ``Text``.
        Only the block glyphs themselves carry colour, so callers can
        ``.append`` further text without it inheriting the swatch
        colour.
    """
    state_colour = block_style_for(state)
    text = Text()
    if suspended:
        text.append(_BLOCK_GLYPH, style=SUSPENDED_BLOCK_STYLE)
        text.append(_BLOCK_GLYPH, style=state_colour)
    else:
        text.append(_BLOCK_GLYPH * 2, style=state_colour)
    if trailing:
        text.append(trailing)
    return text
