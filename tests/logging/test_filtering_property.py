"""Property-based test for level filtering (Property 1).

Covers Property 1 from the logging-enhancement design: a record is emitted if
and only if its severity rank is at or above the configured level's rank, on
every active backend, and a logging call below the configured level is
suppressed and returns control without raising.

The test is parametrized over every backend available in the environment
(stdlib always; loguru when installed) via the shared ``backend`` fixture in
``conftest.py``, which constructs the backend directly. This satisfies the
"holds on every active backend" clause (Requirement 9.3).

Capturing approach
------------------
Both backends create their console sink bound to ``sys.stderr`` at the moment
``apply_config`` runs (the stdlib ``StreamHandler`` captures the stream at
handler construction; loguru's ``logger.add(sys.stderr, ...)`` captures the
stream object at ``add`` time). So configuring the backend *inside* a
``contextlib.redirect_stderr`` block binds the console sink to an in-memory
buffer deterministically for both backends -- no dependence on pytest capture
interacting with Hypothesis's many examples.
"""

from __future__ import annotations

import contextlib
import io
import uuid

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from takler.logging.config import ResolvedConfig
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


def _emit_and_capture(
    backend, configured: LogLevel, record: LogLevel, message: str
) -> str:
    """Configure ``backend`` at ``configured`` and emit one record at ``record``.

    The backend is configured with the console sink enabled and no file sink,
    inside a ``redirect_stderr`` block so the console sink writes to an
    in-memory buffer. The emitted record's level is selected by calling the
    matching per-level method on the NamedLogger, which must never raise even
    when the level is below the configured threshold (Requirement 8.4).

    Returns the captured console output.
    """
    config = ResolvedConfig(
        level=configured,
        console=True,
        log_file=None,
        rotation=None,
        retention=None,
    )

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        backend.apply_config(config)
        logger = backend.get_named_logger("filtering.test")
        method = getattr(logger, _METHOD_FOR_LEVEL[record])
        # Calling a method below the configured level must return control
        # without raising (Requirement 8.4).
        method(message)

    return buffer.getvalue()


# Feature: logging-enhancement, Property 1: Level filtering follows severity ordering
# Validates: Requirements 2.1, 2.2, 4.3, 8.4, 9.3
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    configured=st.sampled_from(list(LogLevel)),
    record=st.sampled_from(list(LogLevel)),
)
def test_level_filtering_follows_severity_ordering(backend, configured, record):
    """A record is emitted iff rank(record) >= rank(configured), on each backend.

    For any configured level ``C`` and any record level ``R``:
    - if ``R.rank >= C.rank`` the record is emitted to the console sink
      (Requirement 2.1, 4.3);
    - otherwise it is suppressed (Requirement 2.2);
    - in both cases the logging call returns without raising (Requirement 8.4).
    This holds on every active backend (Requirement 9.3).
    """
    # A unique marker so the assertion never collides with unrelated output.
    marker = f"filter-marker-{uuid.uuid4().hex}"

    output = _emit_and_capture(backend, configured, record, marker)

    emitted = marker in output
    expected = record.rank >= configured.rank

    assert emitted == expected, (
        f"backend emitted={emitted} but expected={expected} for "
        f"configured={configured.name} record={record.name}; output={output!r}"
    )
