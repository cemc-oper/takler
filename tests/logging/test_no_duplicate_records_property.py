"""Property-based test for no duplicate records on reconfiguration (Property 12).

Covers Property 12 from the logging-enhancement design: for any number of
repeated Logging_Configurator invocations, a single logging call emits exactly
one record per configured destination, with no duplicates introduced by prior
invocations (Requirement 1.4).

Approach (backend-level, recommended)
-------------------------------------
This test exercises the property at the backend layer via the shared
``backend`` fixture in ``conftest.py``, which constructs each available backend
directly (stdlib always; loguru when installed). Working at this layer
directly tests the idempotent sink teardown that ``apply_config`` must perform:
every invocation has to remove the sinks installed by prior invocations before
installing the new set, so repeated configuration never accumulates duplicate
console handlers / file sinks.

The test repeats ``backend.apply_config(...)`` N times (N drawn from 1..6) with
the *same* console + file destinations, then emits exactly ONE record through a
named logger. It then asserts the marker appears on exactly one line in the
console buffer and on exactly one line in the file. If any prior invocation had
leaked a sink, the leaked sink would still be attached and the single logging
call would produce duplicate lines, failing the assertion.

Capturing approach
------------------
Both backends bind their console sink to ``sys.stderr`` at the moment
``apply_config`` runs (the stdlib ``StreamHandler`` captures the stream at
handler construction; loguru's ``logger.add(sys.stderr, ...)`` captures the
stream object at ``add`` time). Performing *all* ``apply_config`` calls inside a
``contextlib.redirect_stderr`` block therefore binds every console sink that is
installed -- including any erroneously leaked one -- to the in-memory buffer, so
duplicate console emission is observable. The final invocation in particular
binds the surviving console sink to the buffer for both backends.

The file sink writes to a temporary file created with :mod:`tempfile` inside
the test body. A fresh temp directory is created and removed for every
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


# Feature: logging-enhancement, Property 12: Reconfiguration produces no duplicate records
# Validates: Requirements 1.4
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(repeats=st.integers(min_value=1, max_value=6))
def test_reconfiguration_produces_no_duplicate_records(backend, repeats):
    """Repeated configuration yields exactly one record per destination.

    With the same console + file destinations configured ``repeats`` times and
    then a single ``info`` call emitted, the marker token must appear on
    exactly one console line and exactly one file line. Any sink leaked by a
    prior invocation would surface as a duplicate line, so a passing assertion
    confirms idempotent sink teardown (Requirement 1.4). Holds on every active
    backend.
    """
    # A token unlikely to collide with formatted metadata or other lines.
    marker = f"MARKER-{uuid.uuid4().hex}"

    temp_dir = tempfile.mkdtemp(prefix="takler-log-dup-")
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
            # Repeat configuration arbitrarily. Each invocation must tear down
            # the sinks installed by the prior one; performing every call
            # inside the redirect binds any leaked console sink to the buffer.
            for _ in range(repeats):
                backend.apply_config(config)

            # Exactly one logging call after all the reconfiguration.
            logger = backend.get_named_logger("dup.test")
            logger.info(marker)

        # Flush and close the file sink before reading it back.
        _teardown_sinks(backend)

        console_lines = buffer.getvalue().splitlines()
        with open(log_path, "r", encoding="utf-8") as handle:
            file_lines = handle.read().splitlines()

        console_hits = [line for line in console_lines if marker in line]
        file_hits = [line for line in file_lines if marker in line]

        assert len(console_hits) == 1, (
            f"expected exactly one console record after {repeats} "
            f"reconfiguration(s), got {len(console_hits)}: {console_hits!r}"
        )
        assert len(file_hits) == 1, (
            f"expected exactly one file record after {repeats} "
            f"reconfiguration(s), got {len(file_hits)}: {file_hits!r}"
        )
    finally:
        _teardown_sinks(backend)
        shutil.rmtree(temp_dir, ignore_errors=True)
