"""Shared fixtures for the logging-enhancement backend property tests.

The behavioral correctness properties (filtering, console/file equivalence,
parent-directory creation, attribution, uniform method surface) must hold on
*every* active backend (Requirements 9.3, 9.5). To exercise that without
relying on process-wide import state, this module provides a ``backend``
fixture that is parametrized over the backends actually available in the
running environment and constructs the chosen backend *directly*:

* ``stdlib`` -- :class:`~takler.logging.backends.stdlib_backend.StdlibBackend`
  is always available.
* ``loguru`` -- :class:`~takler.logging.backends.loguru_backend.LoguruBackend`
  is added only when the optional ``loguru`` library is installed. The loguru
  backend module performs ``from loguru import logger`` at import time, so the
  import is guarded with :func:`importlib.util.find_spec`.

The fixture also isolates global logging state between tests. loguru's
``logger`` is a process-wide singleton, and the stdlib ``takler`` logger
accumulates handlers, so both are torn down before and after each test so that
no sink (or buffer) leaks across examples or tests.
"""

from __future__ import annotations

import importlib.util
import logging
from typing import Iterator

import pytest

from takler.logging.backends.stdlib_backend import (
    ROOT_LOGGER_NAME,
    StdlibBackend,
    _MANAGED_HANDLER_FLAG,
)

# Whether the optional loguru library is importable in this environment. The
# loguru backend can only be imported (and therefore parametrized over) when it
# is, because its module does ``from loguru import logger`` at import time.
LOGURU_INSTALLED = importlib.util.find_spec("loguru") is not None

# Backends to parametrize behavioral property tests over: stdlib is always
# present; loguru is added only when installed.
AVAILABLE_BACKENDS = ["stdlib"] + (["loguru"] if LOGURU_INSTALLED else [])


def _remove_stdlib_managed_handlers() -> None:
    """Detach and close every handler this subsystem attached to ``takler``."""
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    for handler in list(logger.handlers):
        if getattr(handler, _MANAGED_HANDLER_FLAG, False):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:  # noqa: BLE001 - cleanup must not raise
                pass


def _remove_all_loguru_sinks() -> None:
    """Remove every loguru sink so no buffer or handler leaks across tests."""
    from loguru import logger as loguru_logger

    loguru_logger.remove()


@pytest.fixture(params=AVAILABLE_BACKENDS)
def backend(request: pytest.FixtureRequest) -> Iterator[object]:
    """Yield a freshly constructed backend for each available backend kind.

    The backend is built directly (not via the process-wide selector) so the
    test exercises the specific backend regardless of import state. Global
    logging sinks are cleared before and after the test for isolation.
    """
    kind = request.param

    # Clean slate before the test.
    _remove_stdlib_managed_handlers()
    if LOGURU_INSTALLED:
        _remove_all_loguru_sinks()

    if kind == "stdlib":
        instance: object = StdlibBackend()
    elif kind == "loguru":
        from takler.logging.backends.loguru_backend import LoguruBackend

        instance = LoguruBackend()
    else:  # pragma: no cover - defensive
        raise ValueError(f"unknown backend kind: {kind!r}")

    try:
        yield instance
    finally:
        # Tear down any sinks the test installed so nothing leaks onward.
        _remove_stdlib_managed_handlers()
        if LOGURU_INSTALLED:
            _remove_all_loguru_sinks()
