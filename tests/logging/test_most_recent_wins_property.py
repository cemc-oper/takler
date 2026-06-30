"""Property-based test for most-recent-configuration-wins (Property 11).

Covers Property 11 from the logging-enhancement design: for any sequence of
``configure`` invocations, the effective configuration applied to records
emitted afterward equals the resolved configuration of the *final* invocation
(Requirements 1.2, 1.3).

This test exercises the public :func:`takler.logging.configure` /
:func:`takler.logging.get_logger` surface, so it runs against whichever backend
``get_backend()`` selects in the environment (loguru when installed, otherwise
the standard-library backend).

Capturing approach
------------------
Both backends bind their console sink to ``sys.stderr`` at the moment
``apply_config`` runs (the stdlib ``StreamHandler`` captures the stream at
handler construction; loguru's ``logger.add(sys.stderr, ...)`` captures the
stream object at ``add`` time). The whole sequence of ``configure`` calls is
therefore performed *inside* a ``contextlib.redirect_stderr`` block so the
console sink installed by the final invocation is bound to the in-memory
buffer. Records are then emitted (still inside the block) and the captured
output is asserted against the final invocation's level.
"""

from __future__ import annotations

import contextlib
import io
import uuid

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import takler.logging
from takler.logging import configure, get_logger
from takler.logging.levels import LogLevel

# The per-level method names exposed by every NamedLogger adapter.
_METHOD_FOR_LEVEL = {
    LogLevel.TRACE: "trace",
    LogLevel.DEBUG: "debug",
    LogLevel.INFO: "info",
    LogLevel.WARNING: "warning",
    LogLevel.ERROR: "error",
    LogLevel.CRITICAL: "critical",
}


@st.composite
def level_name(draw: st.DrawFn) -> "tuple[LogLevel, str]":
    """Draw a canonical level together with a random case permutation of its name.

    Returns a ``(LogLevel, name_string)`` pair. The name string is a
    case-permuted spelling of the level's canonical name (for example
    ``"InFo"`` for :attr:`LogLevel.INFO`), exercising the case-insensitive
    acceptance of level names (Requirement 2.4) while keeping the resolved
    level known for the assertion.
    """
    level = draw(st.sampled_from(list(LogLevel)))
    name = level.name
    flags = draw(st.lists(st.booleans(), min_size=len(name), max_size=len(name)))
    cased = "".join(ch.upper() if up else ch.lower() for ch, up in zip(name, flags))
    return level, cased


# Feature: logging-enhancement, Property 11: Most recent configuration wins
# Validates: Requirements 1.2, 1.3
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(sequence=st.lists(level_name(), min_size=1))
def test_most_recent_configuration_wins(sequence):
    """The final ``configure`` invocation governs every subsequent record.

    For any non-empty sequence of ``configure(level=...)`` invocations, the
    effective level applied to records emitted afterward equals the level of
    the final invocation: a record at level ``R`` appears iff
    ``R.rank >= final_level.rank`` (Requirements 1.2, 1.3). Earlier invocations
    in the sequence have no effect on records emitted after the final one
    returns.
    """
    final_level = sequence[-1][0]

    # Reset the "configured" flag so this example starts from a clean slate and
    # the sequence below is the only configuration that governs the records.
    takler.logging._reset_configured_state()

    # A unique marker per record so assertions never collide with output from
    # other records or earlier examples.
    markers = {level: f"recent-{level.name}-{uuid.uuid4().hex}" for level in LogLevel}

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        # Apply the full sequence of configures inside the redirect block so the
        # console sink installed by the FINAL invocation is bound to the buffer
        # (the handler binds sys.stderr at construction time).
        for _level, name in sequence:
            configure(level=name)

        # Emit one record at every level through a freshly obtained logger.
        logger = get_logger("most.recent.wins.test")
        for level in LogLevel:
            method = getattr(logger, _METHOD_FOR_LEVEL[level])
            method(markers[level])

    output = buffer.getvalue()

    for level in LogLevel:
        emitted = markers[level] in output
        expected = level.rank >= final_level.rank
        assert emitted == expected, (
            f"record level={level.name} emitted={emitted} but expected={expected}; "
            f"final configured level={final_level.name}; "
            f"sequence={[name for _l, name in sequence]!r}; output={output!r}"
        )
