"""Unit tests for the default logging configuration (Requirements 1.5, 4.1).

These pin down the concrete behavior of the Logging_Subsystem when no
``configure`` call has happened before the first record is emitted. In that
case :func:`takler.logging.get_logger` triggers ``_ensure_configured()``, which
applies the default configuration resolved from an environment with no
``TAKLER_LOG_*`` variables: INFO level, console sink on, no file sink
(``resolve_config({}, env)`` -> level INFO, console True, ``log_file`` None).

Concretely, the default configuration must:

* emit an INFO record to the console (stderr) -- Requirements 1.5, 4.1;
* suppress a DEBUG record under the default INFO level -- Requirement 1.5;
* establish no file sink -- Requirements 1.5, 4.1; and
* write to the standard error stream specifically, not standard out --
  Requirement 4.1.

Capturing approach
------------------
Both backends bind their console sink to ``sys.stderr`` at the moment
``apply_config`` runs (the stdlib ``StreamHandler`` captures the stream at
handler construction; loguru's ``logger.add(sys.stderr, ...)`` captures the
stream object at ``add`` time). Because the default configuration is applied
lazily *inside* the first :func:`get_logger` call, that call -- and the
subsequent emission -- must run inside the ``contextlib.redirect_stderr`` block
so the console sink binds the in-memory buffer rather than the real stderr.

State isolation
---------------
The default path is only exercised when the "configured" flag is unset and no
``TAKLER_LOG_*`` environment variable steers the resolution. An autouse fixture
clears those variables via ``monkeypatch`` and resets the module-level
"configured" flag (and tears down any installed sinks) before and after each
test so neither environment nor state leaks across tests.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import logging
import uuid
from typing import Iterator, List

import pytest

import takler.logging
from takler.logging import get_logger
from takler.logging.backends import get_backend
from takler.logging.backends.stdlib_backend import (
    ROOT_LOGGER_NAME,
    StdlibBackend,
    _MANAGED_HANDLER_FLAG,
)
from takler.logging.config import resolve_config

# Whether the optional loguru library is importable here. When it is, the
# active backend selected by ``get_backend()`` is the loguru backend; otherwise
# it is the stdlib backend. The tests below are backend-agnostic and assert on
# the public behavior either way.
LOGURU_INSTALLED = importlib.util.find_spec("loguru") is not None


def _stdlib_managed_handlers() -> List[logging.Handler]:
    """Return the handlers this subsystem installed on the ``takler`` logger."""
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    return [
        handler
        for handler in logger.handlers
        if getattr(handler, _MANAGED_HANDLER_FLAG, False)
    ]


def _clear_global_logging_state() -> None:
    """Tear down any sinks installed on either backend so nothing leaks."""
    # stdlib: detach and close the handlers this subsystem attached.
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    for handler in list(logger.handlers):
        if getattr(handler, _MANAGED_HANDLER_FLAG, False):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:  # noqa: BLE001 - cleanup must not raise
                pass

    # loguru: remove every sink from the process-wide singleton logger.
    if LOGURU_INSTALLED:
        from loguru import logger as loguru_logger

        loguru_logger.remove()


@pytest.fixture(autouse=True)
def _default_config_isolation(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Exercise the default path cleanly: no env steering, fresh state.

    Clears ``TAKLER_LOG_LEVEL`` / ``TAKLER_LOG_FILE`` so the resolution falls
    back to built-in defaults (Requirement 7.5 keeps blank/absent values from
    interfering), resets the "configured" flag so ``get_logger`` re-triggers the
    default configuration (Requirement 1.5), and tears down any installed sinks
    before and after the test.
    """
    monkeypatch.delenv("TAKLER_LOG_LEVEL", raising=False)
    monkeypatch.delenv("TAKLER_LOG_FILE", raising=False)

    _clear_global_logging_state()
    takler.logging._reset_configured_state()
    try:
        yield
    finally:
        takler.logging._reset_configured_state()
        _clear_global_logging_state()


def test_default_config_emits_info_to_console() -> None:
    """Req 1.5, 4.1: without ``configure``, an INFO record reaches the console.

    The default configuration is applied lazily inside the first ``get_logger``
    call, and the console sink binds the active ``sys.stderr`` at that moment,
    so both the ``get_logger`` call and the ``info`` emission happen inside the
    ``redirect_stderr`` block. The INFO marker must appear on stderr.
    """
    marker = f"default-info-{uuid.uuid4().hex}"

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        # The default INFO-to-console config is installed here (no prior
        # ``configure`` call), binding the console sink to ``buffer``.
        get_logger("default.config.test").info(marker)

    assert marker in buffer.getvalue()


def test_default_config_suppresses_debug() -> None:
    """Req 1.5: DEBUG is below the default INFO level and is suppressed.

    Under the default INFO level a DEBUG record must not be emitted, while an
    INFO record from the same logger is. Emitting both and asserting only INFO
    appears confirms the sink is bound and the level filter is at INFO.
    """
    info_marker = f"default-info-{uuid.uuid4().hex}"
    debug_marker = f"default-debug-{uuid.uuid4().hex}"

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        logger = get_logger("default.config.test")
        logger.info(info_marker)
        logger.debug(debug_marker)

    output = buffer.getvalue()
    assert info_marker in output
    assert debug_marker not in output


def test_default_config_has_no_file_sink() -> None:
    """Req 1.5, 4.1: the default configuration establishes no file sink.

    Backend-agnostic guarantee: the resolved default configuration carries no
    file path (so ``apply_config`` installs no file sink on either backend).
    When the active backend is the stdlib backend, additionally assert that no
    managed file handler is attached to the ``takler`` logger after the default
    configuration has been applied.
    """
    # The resolved default config (no explicit args, empty environment) has no
    # file path, so no file sink is established on any backend.
    resolved = resolve_config({}, {})
    assert resolved.log_file is None

    # Trigger the lazy default configuration via the public API.
    get_logger("default.config.test")

    # Stdlib-specific structural check: no managed FileHandler was attached.
    backend = get_backend()
    if isinstance(backend, StdlibBackend):
        file_handlers = [
            handler
            for handler in _stdlib_managed_handlers()
            if isinstance(handler, logging.FileHandler)
        ]
        assert file_handlers == []


def test_default_console_writes_to_stderr_not_stdout() -> None:
    """Req 4.1: the default console sink writes to stderr, not stdout.

    Capturing both streams and emitting one INFO record, the marker must appear
    on stderr and must be absent from stdout.
    """
    marker = f"default-stream-{uuid.uuid4().hex}"

    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    with contextlib.redirect_stdout(stdout_buffer):
        with contextlib.redirect_stderr(stderr_buffer):
            get_logger("default.config.test").info(marker)

    assert marker in stderr_buffer.getvalue()
    assert marker not in stdout_buffer.getvalue()
