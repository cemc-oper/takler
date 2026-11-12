"""Property-based test for invalid-level configuration safety (Property 2).

Covers Property 2 from the logging-enhancement design: invoking the
Logging_Configurator with a string that is not a recognized severity name
raises :class:`~takler.logging.InvalidLogLevelError` identifying the offending
value AND leaves the previously active Log_Level configuration unchanged
(Requirement 2.3).

How "prior active level unchanged" is observed
----------------------------------------------
A known prior level (DEBUG) is established with a real ``configure`` call, then
an invalid ``configure`` is attempted. Because :func:`resolve_config` parses
the explicit level *before* any sink is torn down or re-applied, the invalid
call raises without disturbing the active configuration. We confirm the prior
DEBUG level is still in force *behaviorally*: a DEBUG record emitted through
``get_logger`` afterward still reaches the console sink (it would be suppressed
if the level had been reset to, say, INFO, or if the sink had been torn down).

Both the prior ``configure(level="DEBUG")`` and the post-failure emission run
inside a single ``contextlib.redirect_stderr`` block: the console sink binds to
``sys.stderr`` at ``apply_config`` time on both backends, so configuring inside
the block binds the console sink to an in-memory buffer deterministically.
Since the invalid ``configure`` raises before changing sinks, the DEBUG console
sink bound to the buffer remains active for the emission.

This exercises whichever backend ``get_backend()`` selects for the process,
which is acceptable for this property.
"""

from __future__ import annotations

import contextlib
import io
import logging
import uuid

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import takler.logging
from takler.logging import InvalidLogLevelError, configure, get_logger
from takler.logging.config import ENV_LOG_FILE, ENV_LOG_LEVEL
from takler.logging.backends.stdlib_backend import (
    ROOT_LOGGER_NAME,
    _MANAGED_HANDLER_FLAG,
)

# The recognized canonical severity names. Parsing upper-cases and strips
# surrounding whitespace, so a generated string counts as "recognized" only
# when its stripped, upper-cased form is one of these.
RECOGNIZED = {"TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def _is_recognized(value: str) -> bool:
    """Return whether ``value`` would parse as a recognized severity name."""
    return value.strip().upper() in RECOGNIZED


# Strings outside the recognized set. Includes the empty string and
# whitespace-only strings (which are non-``None`` and therefore explicit, so
# ``configure`` parses them and they must raise). Excludes any case/whitespace
# variant of a recognized name.
invalid_level_strings = st.text().filter(lambda s: not _is_recognized(s))


def _teardown_managed_sinks() -> None:
    """Remove any sinks this subsystem installed so nothing leaks onward."""
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    for handler in list(logger.handlers):
        if getattr(handler, _MANAGED_HANDLER_FLAG, False):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:  # noqa: BLE001 - cleanup must not raise
                pass
    try:
        from loguru import logger as loguru_logger
    except Exception:  # noqa: BLE001 - loguru is optional
        return
    loguru_logger.remove()


@pytest.fixture
def clean_logging_env(monkeypatch):
    """Isolate logging state and environment for the property test.

    Clears the Takler logging environment variables so resolution is governed
    only by the explicit ``configure`` arguments under test, and tears down any
    installed sinks plus the module ``_configured`` flag afterward so no state
    leaks into other tests.
    """
    monkeypatch.delenv(ENV_LOG_LEVEL, raising=False)
    monkeypatch.delenv(ENV_LOG_FILE, raising=False)
    _teardown_managed_sinks()
    takler.logging._reset_configured_state()
    try:
        yield
    finally:
        _teardown_managed_sinks()
        takler.logging._reset_configured_state()


# Feature: logging-enhancement, Property 2: Invalid configured level raises and preserves prior level
# Validates: Requirements 2.3
@settings(
    max_examples=100,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(bad_level=invalid_level_strings)
def test_invalid_configured_level_raises_and_preserves_prior_level(
    clean_logging_env, bad_level
):
    """Invalid configured level raises naming the value and preserves prior level.

    For any string that is not a recognized severity name, ``configure(level=
    bad)`` raises :class:`InvalidLogLevelError` identifying the value, and the
    previously active level (DEBUG) is left unchanged -- shown by a DEBUG record
    still reaching the console sink afterward (Requirement 2.3).
    """
    # Reset per example so each starts from a clean default configuration.
    takler.logging._reset_configured_state()

    marker = f"prior-level-marker-{uuid.uuid4().hex}"
    buffer = io.StringIO()

    with contextlib.redirect_stderr(buffer):
        # Establish a known prior active level: DEBUG, console enabled.
        configure(level="DEBUG", console=True)

        # Attempting an invalid configure must raise and identify the value,
        # before any sink is torn down or the level changed.
        with pytest.raises(InvalidLogLevelError) as exc_info:
            configure(level=bad_level)

        # Prior DEBUG level must still be active: a DEBUG record is emitted
        # through the still-bound console sink.
        get_logger("invalid.level.test").debug(marker)

    # The raised error identifies the offending value (Requirement 2.3).
    assert exc_info.value.value == bad_level

    # The prior DEBUG configuration is unchanged: the DEBUG record appears in
    # the console output. Were the level reset (e.g. to INFO) or the sink torn
    # down by the failed call, the DEBUG marker would be absent.
    output = buffer.getvalue()
    assert marker in output, (
        f"prior DEBUG level was not preserved after an invalid configure "
        f"(bad_level={bad_level!r}); console output={output!r}"
    )
