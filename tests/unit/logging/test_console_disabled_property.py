"""Property-based test for console-disabled file preservation (Property 14).

Covers Property 14 from the logging-enhancement design: for any set of records
emitted while the console is disabled and a File_Sink is configured, no record
is written to the Console_Sink and every record at or above the configured
level is written to the File_Sink (Requirement 4.2).

The test is parametrized over every backend available in the environment
(stdlib always; loguru when installed) via the shared ``backend`` fixture in
``conftest.py``, which constructs the backend directly.

Capturing approach
------------------
Both backends bind their console sink to ``sys.stderr`` at the moment
``apply_config`` runs (the stdlib ``StreamHandler`` captures the stream at
handler construction; loguru's ``logger.add(sys.stderr, ...)`` captures the
stream object at ``add`` time). Configuring the backend *inside* a
``contextlib.redirect_stderr`` block therefore binds any console sink that
might be installed to an in-memory buffer for both backends. Because the
console is disabled here, the buffer must remain free of emitted record
markers.

The file sink writes to a temporary directory created with :mod:`tempfile`
inside the test body. A fresh temp directory is created and removed for every
Hypothesis example to avoid leaking files or appending across examples. After
emitting, the sinks are torn down (by reconfiguring the backend with no file
sink) so the file handler is flushed and closed before the file is read back.
"""

from __future__ import annotations

import contextlib
import io
import os
import shutil
import tempfile
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


def _teardown_sinks(backend) -> None:
    """Tear down the backend's sinks, flushing and closing the file handler.

    Reconfiguring with ``console=False`` and ``log_file=None`` triggers the
    backend's idempotent removal of its previously installed sinks, which
    closes (and therefore flushes) the file handler so the file can be read
    back reliably on both backends.
    """
    backend.apply_config(
        ResolvedConfig(
            level=LogLevel.TRACE,
            console=False,
            log_file=None,
            rotation=None,
            retention=None,
        )
    )


# A list of (level, marker) records. Each marker is unique (uuid-based) so its
# presence/absence in a sink can be checked unambiguously and never collides
# with another record or formatted metadata.
@st.composite
def _records(draw: st.DrawFn) -> "list[tuple[LogLevel, str]]":
    levels = draw(st.lists(st.sampled_from(list(LogLevel)), min_size=1, max_size=12))
    return [(level, f"REC-{uuid.uuid4().hex}") for level in levels]


# Feature: logging-enhancement, Property 14: Disabling the console preserves file output
# Validates: Requirements 4.2
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(configured=st.sampled_from(list(LogLevel)), records=_records())
def test_console_disabled_preserves_file_output(backend, configured, records):
    """Console disabled + file sink: nothing on console, level-filtered file.

    With ``console=False`` and a file sink configured at level ``configured``,
    every emitted record marker must be absent from the console buffer, while
    the file must contain exactly the markers of records whose level rank is at
    or above ``configured`` and none of those below it (Requirement 4.2). Holds
    on every active backend.
    """
    temp_dir = tempfile.mkdtemp(prefix="takler-log-noconsole-")
    log_path = os.path.join(temp_dir, "takler.log")

    try:
        config = ResolvedConfig(
            level=configured,
            console=False,
            log_file=log_path,
            rotation=None,
            retention=None,
        )

        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            backend.apply_config(config)
            for level, marker in records:
                logger = backend.get_named_logger("noconsole.test")
                getattr(logger, _METHOD_FOR_LEVEL[level])(marker)

        # Flush and close the file sink before reading it back.
        _teardown_sinks(backend)

        console_output = buffer.getvalue()
        with open(log_path, "r", encoding="utf-8") as handle:
            file_output = handle.read()

        for level, marker in records:
            # No emitted record reaches the console while it is disabled.
            assert marker not in console_output, (
                f"record marker {marker!r} (level={level.name}) appeared on the "
                f"console even though console output is disabled; "
                f"console={console_output!r}"
            )

            at_or_above = level.rank >= configured.rank
            present = marker in file_output
            assert present == at_or_above, (
                f"record level={level.name} present_in_file={present} but "
                f"expected={at_or_above}; configured level={configured.name}; "
                f"file={file_output!r}"
            )
    finally:
        _teardown_sinks(backend)
        shutil.rmtree(temp_dir, ignore_errors=True)
