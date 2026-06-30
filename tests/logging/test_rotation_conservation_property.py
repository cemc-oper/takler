"""Property-based test for rotation conservation (Property 16).

Covers Property 16 from the logging-enhancement design: for any set of records
emitted under a configured size rotation threshold, the multiset of records
across the active and rotated log files equals the multiset of emitted records
-- no record is lost and none is duplicated (Requirement 5.3).

The behavioral property is parametrized over every backend available in the
environment (stdlib always; loguru when installed) via the shared ``backend``
fixture in ``conftest.py``, which constructs the chosen backend directly.

Testing approach
----------------
* A *small* size rotation threshold (``"200 B"``) is configured together with a
  modest-but-larger-than-one-file number of records, so several rotations occur
  for every example. The threshold and an integer retention of ``1000`` work
  for both backends: stdlib parses the size string into a
  :class:`~logging.handlers.RotatingFileHandler` ``maxBytes`` and uses the
  integer as ``backupCount``; loguru parses the size string natively and treats
  the integer retention as a maximum file count. The retention is deliberately
  large so that **no** record is dropped to retention -- the property under test
  is *rotation* conservation, not retention. (Note: a stdlib
  ``RotatingFileHandler`` with ``backupCount=0`` discards rotated files on
  rollover, so an explicit large retention is required here to keep every
  rotated file.)
* Each record carries a *unique* payload token (an index plus a per-example
  token), so any loss shows up as a missing token and any duplication shows up
  as a repeated token in the combined multiset.
* After emitting, the backend's sinks are torn down (reconfigured with no file
  sink and the console disabled) so every file handler is flushed and closed
  before the files are read back.
* All log files in the per-example temp directory are then collected: the active
  file plus every rotated file (stdlib names them ``<path>.1``, ``<path>.2``,
  ...; loguru creates timestamped siblings in the same directory). The payload
  token of each record line is extracted and the multiset
  (:class:`collections.Counter`) found across all files must equal the multiset
  of emitted payload tokens.
"""

from __future__ import annotations

import collections
import glob
import os
import shutil
import tempfile
import uuid
from typing import List

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

# A fixed, space-free component name so the message field is unambiguous when a
# record line is split back into its fields.
_COMPONENT = "tests.rotation"

# Small size threshold so several rotations occur for a modest record count.
_ROTATION = "200 B"

# Large retention so no record is dropped to retention: the property under test
# is rotation conservation, not retention. For stdlib this becomes ``backupCount``
# (so rotated files are kept rather than discarded); for loguru it is the maximum
# retained file count.
_RETENTION = 1000


def _teardown_sinks(backend) -> None:
    """Tear down the backend's sinks, flushing and closing the file handler.

    Reconfiguring with ``console=False`` and ``log_file=None`` triggers the
    backend's idempotent removal of its previously installed sinks, which closes
    (and therefore flushes) the file handler so every rotated and active file
    can be read back reliably on both backends.
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


def _extract_payloads(directory: str) -> List[str]:
    """Read every log file under ``directory`` and return their payload tokens.

    Each record line has the canonical layout ``timestamp LEVEL component
    message``; the payload token is the message field (the 4th field), which is
    extracted with a bounded split so any internal structure of the token is
    preserved. Empty lines (e.g. a trailing newline) are ignored.
    """
    payloads: List[str] = []
    for entry in sorted(glob.glob(os.path.join(directory, "*"))):
        if not os.path.isfile(entry):
            continue
        with open(entry, "r", encoding="utf-8") as handle:
            for line in handle.read().splitlines():
                if not line:
                    continue
                parts = line.split(" ", 3)
                if len(parts) < 4:
                    # Not a well-formed record line; record it verbatim so a
                    # malformed/split write would surface as a mismatch.
                    payloads.append(line)
                    continue
                payloads.append(parts[3])
    return payloads


# Feature: logging-enhancement, Property 16: Rotation preserves all records without loss or duplication
# Validates: Requirements 5.3
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(levels=st.lists(st.sampled_from(list(LogLevel)), min_size=25, max_size=60))
def test_rotation_preserves_all_records(backend, levels):
    """Size rotation loses and duplicates no record (Requirement 5.3).

    With a small size rotation threshold and a large retention, emitting many
    uniquely-tokened records forces several rotations. The multiset of payload
    tokens recovered from the active file plus every rotated file must equal the
    multiset of emitted tokens -- proving no record was lost and none was
    duplicated. Holds on every active backend.
    """
    temp_dir = tempfile.mkdtemp(prefix="takler-log-rot-")
    log_path = os.path.join(temp_dir, "takler.log")

    # A per-example token keeps payloads from colliding with anything else and
    # the index makes every record's payload unique within the example, so loss
    # or duplication is detectable in the recovered multiset.
    token = uuid.uuid4().hex
    messages = [f"rec-{index:04d}-{token}" for index in range(len(levels))]

    try:
        config = ResolvedConfig(
            level=LogLevel.TRACE,
            console=False,
            log_file=log_path,
            rotation=_ROTATION,
            retention=_RETENTION,
        )
        backend.apply_config(config)

        for level, message in zip(levels, messages):
            logger = backend.get_named_logger(_COMPONENT)
            getattr(logger, _METHOD_FOR_LEVEL[level])(message)

        # Flush and close every file sink before reading the files back.
        _teardown_sinks(backend)

        emitted = collections.Counter(messages)
        found = collections.Counter(_extract_payloads(temp_dir))

        assert found == emitted, (
            "rotation lost or duplicated records:\n"
            f"  missing (emitted but not found)   = {emitted - found}\n"
            f"  extra   (found but not emitted)   = {found - emitted}"
        )
    finally:
        _teardown_sinks(backend)
        shutil.rmtree(temp_dir, ignore_errors=True)
