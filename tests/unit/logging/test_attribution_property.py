"""Property-based test for exact component attribution (Property 6).

Covers Property 6 from the logging-enhancement design: for any component name
that is a non-empty string of at most 256 characters, the records emitted by
``get_logger(name)`` show that exact component name as the originating
Named_Logger name, on every active backend (including loguru), and repeated
calls with the same name produce records attributed to the same name.

The test is parametrized over every backend available in the environment
(stdlib always; loguru when installed) via the shared ``backend`` fixture in
``conftest.py``, which constructs the backend directly. This satisfies the
"on every active backend" clause (Requirements 6.2, 9.5).

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
spaces between fields, where the RFC 3339 timestamp
(``2026-06-30T11:38:10.123+08:00``) itself contains no spaces. To make the component field (the 3rd field) parse
unambiguously, the generated names draw from an alphabet that excludes all
whitespace (categories ``Zs``/``Zl``/``Zp``) and control/surrogate characters
(categories ``Cc``/``Cs``). With no whitespace inside the name, splitting a
captured line on a single space at most three times yields exactly
``[timestamp, level, component, message]`` and field index 2 is the verbatim
component name. Excluding whitespace also guarantees the name is non-blank, so
it is never normalized to the root component ``takler``.
"""

from __future__ import annotations

import contextlib
import io

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from takler.logging.config import ResolvedConfig
from takler.logging.levels import LogLevel

# Component names of length 1..256 drawn from an alphabet free of whitespace
# (Zs/Zl/Zp), control characters (Cc, which includes tab/newline/carriage
# return), and surrogates (Cs, which cannot be encoded to UTF-8 by the file
# sink). Excluding whitespace keeps the single-space field parsing unambiguous
# and guarantees the name is non-blank (so it is used verbatim rather than
# normalized to the root component).
_COMPONENT_NAME = st.text(
    alphabet=st.characters(blacklist_categories=("Cs", "Cc", "Zs", "Zl", "Zp")),
    min_size=1,
    max_size=256,
)


def _emit_and_capture_component(backend, name: str) -> str:
    """Emit one INFO record via ``get_named_logger(name)`` and return its component field.

    The backend is configured at TRACE with the console enabled and no file
    sink, inside a ``redirect_stderr`` block so the console sink writes to an
    in-memory buffer. The captured line is parsed as
    ``<RFC3339> LEVEL component message``; field index 2 is the component.
    """
    config = ResolvedConfig(
        level=LogLevel.TRACE,
        console=True,
        log_file=None,
        rotation=None,
        retention=None,
    )

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        backend.apply_config(config)
        logger = backend.get_named_logger(name)
        logger.info("msg")

    lines = buffer.getvalue().splitlines()
    assert len(lines) == 1, f"expected exactly one emitted line, got {lines!r}"

    # Single-space separated: [timestamp, level, component, message].
    parts = lines[0].split(" ", 3)
    assert len(parts) == 4, f"could not parse record into 4 fields: {lines[0]!r}"
    return parts[2]


# Feature: logging-enhancement, Property 6: Records are attributed to the exact component name
# Validates: Requirements 3.2, 6.1, 6.2, 6.4, 9.5
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(name=_COMPONENT_NAME)
def test_records_attributed_to_exact_component_name(backend, name):
    """Records show the exact component name, and repeated calls agree.

    For any non-empty name of at most 256 characters:
    - the component field of the emitted record equals ``name`` exactly
      (Requirements 3.2, 6.1, 6.2);
    - obtaining a second logger with the same name and emitting again yields a
      record attributed to the same component name (Requirement 6.4).
    This holds on every active backend (Requirements 6.2, 9.5).
    """
    first = _emit_and_capture_component(backend, name)
    assert first == name, (
        f"component field {first!r} does not match the exact name {name!r}"
    )

    # Repeated call with the same name must agree on the attribution.
    second = _emit_and_capture_component(backend, name)
    assert second == name, (
        f"second call attributed {second!r}, expected exact name {name!r}"
    )
    assert first == second, (
        f"repeated calls disagreed: first={first!r} second={second!r}"
    )
