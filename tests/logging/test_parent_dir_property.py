"""Property-based test for parent-directory creation by the file sink.

Covers Property 15 from the logging-enhancement design: when a file sink is
configured at a path whose parent directories do not yet exist, the subsystem
creates those parent directories before writing and the first emitted record is
written to the file (Requirement 5.2).

The behavioral property is parametrized over the available backends -- the
standard-library backend is always present, and the loguru backend is included
only when ``loguru`` is installed (guarded with
``importlib.util.find_spec("loguru")``). Each backend is constructed directly
by a fixture rather than relying on process-wide import-state selection, so the
property is exercised against both backends regardless of which one
``get_backend`` would pick.

Because Hypothesis runs many examples per test invocation, each example builds
its nested path under a fresh ``tempfile.mkdtemp()`` base (not the shared
``tmp_path`` fixture) and tears down the backend's sinks afterwards so open file
handles are not leaked across examples.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import shutil
import string
import tempfile
from pathlib import Path
from typing import List

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from takler.logging.backends import LoggingBackend
from takler.logging.config import ResolvedConfig
from takler.logging.levels import LogLevel

# Whether the optional loguru backend can be exercised in this environment.
_LOGURU_AVAILABLE = importlib.util.find_spec("loguru") is not None

# Backends to parametrize over: stdlib is always available, loguru only when
# installed (Requirements 9.3, 9.5 are satisfied by covering both).
_BACKEND_PARAMS = ["stdlib"]
if _LOGURU_AVAILABLE:
    _BACKEND_PARAMS.append("loguru")

# Safe directory/file name characters: ASCII letters, digits, underscore and
# hyphen only. Excluding "." and path separators keeps generated segments from
# becoming "."/".." or accidental nested paths, and avoids OS-invalid names.
_SAFE_CHARS = string.ascii_letters + string.digits + "_-"

# A single safe path segment (directory or file stem).
_safe_segment = st.text(alphabet=_SAFE_CHARS, min_size=1, max_size=12)

# A fixed component name and message so the file-content assertions are simple
# and unambiguous. The component contains a dot to mirror real Takler component
# names (e.g. "server.scheduler") and to confirm it is rendered verbatim.
_COMPONENT = "tests.parent_dir"
_MESSAGE = "parent-dir-record-payload"


@st.composite
def _nested_relative_paths(draw: st.DrawFn) -> List[str]:
    """Draw nested relative path components: 1..4 dir names plus a filename.

    The returned list always has at least one directory component before the
    final filename, so the parent directory of the resulting path is guaranteed
    not to exist yet under a freshly created temp base.
    """
    depth = draw(st.integers(min_value=1, max_value=4))
    directories = draw(
        st.lists(_safe_segment, min_size=depth, max_size=depth)
    )
    filename = draw(_safe_segment) + ".log"
    return [*directories, filename]


@pytest.fixture(params=_BACKEND_PARAMS)
def backend(request: pytest.FixtureRequest) -> LoggingBackend:
    """Construct the backend under test directly and tear down its sinks.

    Constructing the concrete backend bypasses the process-wide selector so the
    property runs against each available backend independently. On teardown, all
    sinks installed by this module are removed so no file handles leak.
    """
    if request.param == "stdlib":
        from takler.logging.backends.stdlib_backend import StdlibBackend

        instance: LoggingBackend = StdlibBackend()
    else:
        from takler.logging.backends.loguru_backend import LoguruBackend

        instance = LoguruBackend()

    yield instance

    _teardown_sinks(instance)


def _teardown_sinks(instance: LoggingBackend) -> None:
    """Remove every sink this module installed, flushing/closing open files.

    ``apply_config`` is idempotent and removes the Takler-managed sinks before
    installing the new set; configuring with no file sink and the console
    disabled therefore tears everything down and closes any open log file. For
    the stdlib backend we also defensively drop any leftover managed handlers on
    the ``takler`` logger.
    """
    instance.apply_config(
        ResolvedConfig(
            level=LogLevel.INFO,
            console=False,
            log_file=None,
            rotation=None,
            retention=None,
        )
    )
    # Defensive cleanup for the stdlib backend's logger-attached handlers.
    from takler.logging.backends.stdlib_backend import (
        StdlibBackend,
        ROOT_LOGGER_NAME,
    )

    if isinstance(instance, StdlibBackend):
        StdlibBackend._remove_managed_handlers(logging.getLogger(ROOT_LOGGER_NAME))


# Feature: logging-enhancement, Property 15: Missing parent directories are created before writing
# Validates: Requirements 5.2
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(parts=_nested_relative_paths())
def test_missing_parent_directories_are_created_before_writing(
    backend: LoggingBackend,
    parts: List[str],
) -> None:
    """Configuring a file sink at a path with missing parents creates them.

    For any file path whose parent directories do not yet exist, applying a
    configuration with that ``log_file`` creates the parent directories and the
    first emitted record is written to the file (Requirement 5.2).
    """
    base = tempfile.mkdtemp(prefix="takler-parentdir-")
    try:
        path = os.path.join(base, *parts)
        parent = os.path.dirname(path)

        # Precondition: the parent directory chain does not exist yet, so any
        # creation observed below is attributable to the file sink.
        assert not os.path.exists(parent)

        backend.apply_config(
            ResolvedConfig(
                level=LogLevel.INFO,
                console=False,
                log_file=path,
                rotation=None,
                retention=None,
            )
        )
        backend.get_named_logger(_COMPONENT).info(_MESSAGE)

        # The parent directories must now exist (created before writing).
        assert os.path.isdir(parent)

        # Close/flush the sink so the file content is fully on disk, then verify
        # the first record was written to the configured path.
        _teardown_sinks(backend)

        assert os.path.exists(path)
        content = Path(path).read_text(encoding="utf-8")
        assert _MESSAGE in content
        assert _COMPONENT in content
        assert "INFO" in content
    finally:
        _teardown_sinks(backend)
        shutil.rmtree(base, ignore_errors=True)
