"""Property-based test for console/file content equivalence (Property 13).

Covers Property 13 from the logging-enhancement design: for any set of records
emitted while both the Console_Sink and a File_Sink are active, the formatted
content written to the file matches the formatted content written to the
console (Requirement 5.1).

The test is parametrized over every backend available in the environment
(stdlib always; loguru when installed) via the shared ``backend`` fixture in
``conftest.py``, which constructs the backend directly.

Capturing approach
------------------
Both backends bind their console sink to ``sys.stderr`` at the moment
``apply_config`` runs (the stdlib ``StreamHandler`` captures the stream at
handler construction; loguru's ``logger.add(sys.stderr, ...)`` captures the
stream object at ``add`` time). Configuring the backend *inside* a
``contextlib.redirect_stderr`` block therefore binds the console sink to an
in-memory buffer deterministically for both backends.

The file sink writes to a temporary file created with :mod:`tempfile` inside
the test body. ``tmp_path`` is per-test (not per-example), so a fresh temp
directory is created and removed for every Hypothesis example to avoid leaking
files or appending across examples. After emitting, the file sink is torn down
(by reconfiguring the backend with no file sink) so its buffer is flushed and
closed before the file is read back.
"""

from __future__ import annotations

import contextlib
import io
import os
import shutil
import tempfile

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

# Text free of control characters (category Cc, which includes newline and
# carriage return) and surrogates (category Cs, which cannot be encoded to
# UTF-8 when written to the file sink). Excluding newlines keeps the
# line-by-line comparison meaningful.
_SAFE_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs", "Cc")),
    max_size=40,
)

# A single (level, component, message) record to emit. Component is non-empty
# so it is used verbatim (a blank name would normalize to the root component,
# which is irrelevant to this property).
_RECORD = st.tuples(
    st.sampled_from(list(LogLevel)),
    _SAFE_TEXT.filter(lambda s: s.strip() != ""),
    _SAFE_TEXT,
)


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


# Feature: logging-enhancement, Property 13: Console and file sinks contain identical formatted content
# Validates: Requirements 5.1
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(records=st.lists(_RECORD, min_size=1, max_size=10))
def test_console_and_file_contain_identical_content(backend, records):
    """Console and file sinks receive byte-identical formatted lines.

    With both sinks active at a low (TRACE) threshold, every emitted record is
    written to both destinations using the shared formatter. The multiset of
    formatted lines captured from the console must equal the lines read back
    from the file, with matching order (Requirement 5.1). Holds on every
    active backend (Requirements 5.1, 9.5).
    """
    temp_dir = tempfile.mkdtemp(prefix="takler-log-eq-")
    log_path = os.path.join(temp_dir, "takler.log")

    try:
        config = ResolvedConfig(
            level=LogLevel.TRACE,
            console=True,
            log_file=log_path,
            rotation=None,
            retention=None,
        )

        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            backend.apply_config(config)
            for level, component, message in records:
                logger = backend.get_named_logger(component)
                getattr(logger, _METHOD_FOR_LEVEL[level])(message)

        # Flush and close the file sink before reading it back.
        _teardown_sinks(backend)

        console_lines = buffer.getvalue().splitlines()

        with open(log_path, "r", encoding="utf-8") as handle:
            file_lines = handle.read().splitlines()

        assert console_lines == file_lines, (
            "console and file content diverged:\n"
            f"  console={console_lines!r}\n"
            f"  file   ={file_lines!r}"
        )
        # Sanity: every emitted record produced exactly one line per sink.
        assert len(file_lines) == len(records)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
