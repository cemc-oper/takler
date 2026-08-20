"""Property-based test for invalid environment-level fallback (Property 10).

Covers Property 10 from the logging-enhancement design: for any non-empty
``TAKLER_LOG_LEVEL`` environment value that is not a recognized severity name,
the resolved default Log_Level is INFO and a WARNING record identifying the
invalid value is emitted to the Console_Sink (Requirement 7.3).

Two layers are validated for completeness:

1. **Pure resolution.** :func:`takler.logging.config.resolve_config` with an
   empty explicit map and an environment carrying the bad value resolves to
   ``LogLevel.INFO`` and surfaces the offending value on
   :attr:`ResolvedConfig.invalid_env_level` (no I/O, no warning yet).
2. **Warning emission via the public API.** With the environment variable set,
   :func:`takler.logging.configure` is invoked *inside* a
   ``contextlib.redirect_stderr`` block so the console sink binds the in-memory
   buffer at ``apply_config`` time and the subsequent warning emission is
   captured. The captured output must contain ``"WARNING"`` and the offending
   value.

Generator
---------
Invalid env values are drawn from an alphanumeric alphabet (no whitespace, so
they are never blank and never normalized away) and filtered to exclude every
recognized severity name compared case-insensitively. Because the values are
plain alphanumeric strings, ``repr(value)`` (used by the warning message) wraps
the exact characters in quotes, so the bare value remains a substring of the
captured output.
"""

from __future__ import annotations

import contextlib
import io
import os

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import takler.logging as takler_logging
from takler.logging.config import DEFAULT_LEVEL, ENV_LOG_LEVEL, resolve_config
from takler.logging.levels import LogLevel

# The recognized canonical severity names (compared case-insensitively).
LEVEL_NAMES = {"TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

# Non-empty invalid env-level values: alphanumeric (no whitespace, so never
# blank) and never a recognized severity name in any letter case.
invalid_env_values = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
    min_size=1,
    max_size=24,
).filter(lambda s: s.strip() != "" and s.strip().upper() not in LEVEL_NAMES)


# Feature: logging-enhancement, Property 10: Invalid environment level falls back to INFO with a warning
# Validates: Requirements 7.3
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(bad=invalid_env_values)
def test_invalid_env_level_falls_back_to_info_with_warning(bad):
    """Invalid TAKLER_LOG_LEVEL resolves to INFO and warns to the console.

    Layer 1 (pure): ``resolve_config({}, {ENV_LOG_LEVEL: bad})`` yields
    ``level == INFO`` and ``invalid_env_level == bad``.

    Layer 2 (public API): with the env var set, ``configure()`` invoked inside
    a ``redirect_stderr`` block emits a WARNING naming the value to the console
    sink, so the captured buffer contains both ``"WARNING"`` and ``bad``.
    """
    # --- Layer 1: pure resolution ---
    resolved = resolve_config({}, {ENV_LOG_LEVEL: bad})
    assert resolved.level == LogLevel.INFO == DEFAULT_LEVEL
    assert resolved.invalid_env_level == bad

    # --- Layer 2: warning emission via the public API ---
    saved = os.environ.get(ENV_LOG_LEVEL)
    try:
        os.environ[ENV_LOG_LEVEL] = bad
        # Re-exercise default/explicit configuration from a clean state so the
        # invalid-env warning path runs on this configure() call.
        takler_logging._reset_configured_state()

        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            # configure() must run INSIDE the buffer block so the console sink
            # binds the buffer before the warning is emitted.
            takler_logging.configure()

        output = buffer.getvalue()
        assert "WARNING" in output, (
            f"expected a WARNING record in console output, got {output!r}"
        )
        assert bad in output, (
            f"expected the invalid value {bad!r} named in the warning, got {output!r}"
        )
    finally:
        # Restore os.environ exactly and reset configured state for isolation
        # across the many Hypothesis examples.
        if saved is None:
            os.environ.pop(ENV_LOG_LEVEL, None)
        else:
            os.environ[ENV_LOG_LEVEL] = saved
        takler_logging._reset_configured_state()
