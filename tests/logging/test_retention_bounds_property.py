"""Property-based test for retention bounding the number of rotated files.

Covers Property 17 from the logging-enhancement design: for any volume of
emitted records under a configured retention limit, after rotation events the
number of retained rotated files does not exceed the configured limit and the
active log file is preserved (Requirement 5.4).

The behavioral property is parametrized over every backend available in the
environment (stdlib always; loguru when installed) via the shared ``backend``
fixture in ``conftest.py``, which constructs the chosen backend directly so the
property is exercised against each backend regardless of process-wide import
state (Requirements 9.3, 9.5).

Approach
--------
A size-based rotation is configured with a small byte threshold together with
an explicit integer retention limit ``N`` (1..5). Enough records are emitted to
force *many more* than ``N`` rotations, so the retention bound is actually
exercised rather than trivially satisfied.

* **stdlib**: ``retention=N`` maps to ``RotatingFileHandler(backupCount=N)``.
  Rotated files are named ``<path>.1`` .. ``<path>.N`` and the active file is
  ``<path>``; the number of rotated files is therefore bounded by ``N``.
* **loguru**: ``retention=N`` (an int) keeps that many rotated files. Rotated
  files carry timestamped names alongside the active file at the configured
  path.

To stay backend-agnostic, the *active* file is identified as exactly the
configured path string and every other file in the (dedicated, per-example)
directory is treated as a rotated file. The assertion ``rotated_count <= N``
holds regardless of whether a backend counts the active file toward its limit.

Each Hypothesis example uses a fresh ``tempfile.mkdtemp()`` directory and tears
the backend's sinks down afterwards (flushing/closing open handles) before the
files are counted, so nothing leaks across examples.
"""

from __future__ import annotations

import os
import shutil
import tempfile

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from takler.logging.config import ResolvedConfig
from takler.logging.levels import LogLevel

# A fixed component name and a moderately sized message payload so each emitted
# record contributes a predictable number of bytes to the active file, making
# the number of rotations comfortably exceed the retention limit.
_COMPONENT = "tests.retention"
_PAYLOAD = "retention-bound-record-payload-xxxxxxxxxx"


def _teardown_sinks(backend) -> None:
    """Tear down the backend's sinks, flushing/closing any open log file.

    Reconfiguring with the console disabled and no file sink triggers the
    backend's idempotent removal of its previously installed sinks, which
    closes (and therefore flushes) the file handler so the directory can be
    enumerated reliably on both backends.
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


# Feature: logging-enhancement, Property 17: Retention bounds the number of rotated files
# Validates: Requirements 5.4
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    retention=st.integers(min_value=1, max_value=5),
    rotation_bytes=st.integers(min_value=150, max_value=300),
    num_records=st.integers(min_value=40, max_value=80),
)
def test_retention_bounds_rotated_file_count(
    backend, retention, rotation_bytes, num_records
):
    """Retained rotated files never exceed the limit; active file survives.

    A small size-based rotation plus an integer retention limit ``N`` is
    configured, then many records are emitted to force far more than ``N``
    rotations. After tearing the sinks down, the active log file (the configured
    path) must still exist and the number of rotated files (every other file in
    the directory) must not exceed ``N`` (Requirement 5.4). Holds on every
    active backend (Requirements 5.4, 9.5).
    """
    temp_dir = tempfile.mkdtemp(prefix="takler-log-retention-")
    log_path = os.path.join(temp_dir, "takler.log")

    try:
        config = ResolvedConfig(
            level=LogLevel.TRACE,
            console=False,
            log_file=log_path,
            rotation=rotation_bytes,
            retention=retention,
        )
        backend.apply_config(config)

        logger = backend.get_named_logger(_COMPONENT)
        for index in range(num_records):
            logger.info(f"{_PAYLOAD}-{index:05d}")

        # Flush and close the file sink before enumerating the directory so the
        # rotated/active files are fully materialized on disk.
        _teardown_sinks(backend)

        # The active file is exactly the configured path; everything else in the
        # dedicated directory is a rotated file.
        all_files = [
            os.path.join(temp_dir, name) for name in os.listdir(temp_dir)
        ]
        active_exists = os.path.isfile(log_path)
        rotated_files = [path for path in all_files if path != log_path]

        # The active log file must be preserved (Requirement 5.4).
        assert active_exists, (
            f"active log file {log_path!r} was not preserved; "
            f"directory contained {sorted(os.path.basename(p) for p in all_files)!r}"
        )

        # Sanity: the configuration actually exercised rotation, otherwise the
        # bound would be trivially satisfied with zero rotated files.
        assert rotated_files, (
            "expected at least one rotated file so the retention bound is "
            f"exercised (rotation={rotation_bytes} bytes, records={num_records})"
        )

        # The retained rotated-file count must not exceed the configured limit
        # (Requirement 5.4). This holds regardless of whether a backend counts
        # the active file toward its limit.
        assert len(rotated_files) <= retention, (
            f"retained rotated files {len(rotated_files)} exceeded limit "
            f"{retention}: "
            f"{sorted(os.path.basename(p) for p in rotated_files)!r}"
        )
    finally:
        _teardown_sinks(backend)
        shutil.rmtree(temp_dir, ignore_errors=True)
