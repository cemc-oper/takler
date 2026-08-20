"""Property-based test for the uniform logging-method surface (Property 18).

Covers Property 18 from the logging-enhancement design: every active backend
exposes the same ``debug``/``info``/``warning``/``error`` method surface, and
calling any of those methods with a message at or above the configured level
emits exactly one corresponding record, attributes that record to the matching
severity name (``debug`` -> ``DEBUG`` and so on), and returns control to the
caller without raising (Requirements 8.2, 8.3).

The test is parametrized over every backend available in the environment
(stdlib always; loguru when installed) via the shared ``backend`` fixture in
``conftest.py``, which constructs the backend directly. This satisfies the
"across backends" clause (Requirement 9.5).

Capturing approach
------------------
Both backends bind their console sink to ``sys.stderr`` at the moment
``apply_config`` runs (the stdlib ``StreamHandler`` captures the stream at
handler construction; loguru's ``logger.add(sys.stderr, ...)`` captures the
stream object at ``add`` time). Configuring the backend *inside* a
``contextlib.redirect_stderr`` block therefore binds the console sink to an
in-memory buffer deterministically for both backends.

Parsing approach
----------------
The canonical layout is ``<RFC3339> LEVEL component message`` with single
spaces between fields, where the RFC 3339 timestamp contains no spaces. The
component name (``method.surface.test``) and the
unique uuid message token contain no whitespace, so splitting the emitted line
on a single space at most three times yields exactly
``[timestamp, level, component, message]``; field index 1 is the level name and
field index 3 is the token.
"""

from __future__ import annotations

import contextlib
import io
import uuid

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from takler.logging.config import ResolvedConfig
from takler.logging.levels import LogLevel

# The four standard logging methods every NamedLogger adapter must expose
# (Requirement 8.3) and the canonical severity name each one emits at.
_METHOD_LEVEL_NAME = {
    "debug": "DEBUG",
    "info": "INFO",
    "warning": "WARNING",
    "error": "ERROR",
}

# A component name free of whitespace so the single-space field parsing is
# unambiguous.
_COMPONENT = "method.surface.test"


# Feature: logging-enhancement, Property 18: Uniform logging-method surface across backends
# Validates: Requirements 8.2, 8.3
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(method_name=st.sampled_from(["debug", "info", "warning", "error"]))
def test_uniform_logging_method_surface_across_backends(backend, method_name):
    """Each standard method emits exactly one correctly-attributed record.

    The backend is configured at the lowest level (TRACE) so that every one of
    ``debug``/``info``/``warning``/``error`` sits at or above the configured
    level. For any active backend and any of those methods called with a
    message at or above the configured level:

    - the method exists on the returned logger (Requirement 8.3);
    - calling it returns control to the caller without raising and yields
      ``None`` (Requirement 8.2);
    - exactly one record carrying the unique token is emitted to the console
      sink (exactly one record per call, Requirement 8.2);
    - that record's level field matches the method's severity name
      (``debug`` -> ``DEBUG`` and so on).

    This holds on every active backend (Requirement 9.5).
    """
    # Configure at the lowest level so debug/info/warning/error are all at or
    # above the configured threshold.
    config = ResolvedConfig(
        level=LogLevel.TRACE,
        console=True,
        log_file=None,
        rotation=None,
        retention=None,
    )

    expected_level_name = _METHOD_LEVEL_NAME[method_name]

    # A unique marker per example so the occurrence count is unambiguous.
    marker = f"surface-marker-{uuid.uuid4().hex}"

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        backend.apply_config(config)
        logger = backend.get_named_logger(_COMPONENT)

        # Every backend must expose the standard method (Requirement 8.3).
        method = getattr(logger, method_name)

        # Calling the method must return control without raising; the adapter
        # methods return None (Requirement 8.2).
        result = method(marker)

    assert result is None, f"method {method_name!r} returned {result!r}, expected None"

    output = buffer.getvalue()

    # Exactly one emitted line carries the unique token: exactly one record per
    # call (Requirement 8.2).
    matching_lines = [line for line in output.splitlines() if marker in line]
    assert len(matching_lines) == 1, (
        f"expected exactly one record for method {method_name!r} on backend "
        f"{type(backend).__name__}, but {len(matching_lines)} line(s) carried "
        f"the marker; output={output!r}"
    )

    # The record's level field must match the method's severity name.
    parts = matching_lines[0].split(" ", 3)
    assert len(parts) == 4, (
        f"could not parse record into 4 fields: {matching_lines[0]!r}"
    )
    level_field = parts[1]
    assert level_field == expected_level_name, (
        f"method {method_name!r} emitted level field {level_field!r}, expected "
        f"{expected_level_name!r} on backend {type(backend).__name__}; "
        f"record={matching_lines[0]!r}"
    )
