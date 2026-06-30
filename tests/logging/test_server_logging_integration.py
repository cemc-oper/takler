"""Integration tests for the Takler server logging lifecycle (Requirement 10).

These tests exercise how :class:`takler.server.TaklerServer` wires the logging
subsystem into its start/stop lifecycle and how :class:`TaklerService` logs
command handling. They are deliberately *hermetic*: the scheduler and network
service are replaced with ``AsyncMock`` fakes so no real gRPC server is created
and no port is bound, and console output is captured by redirecting
``sys.stderr`` to an in-memory buffer.

Why the redirect must wrap ``await server.start()``
---------------------------------------------------
Both backends bind their console sink to ``sys.stderr`` at the moment
``configure`` (``apply_config``) runs -- the stdlib ``StreamHandler`` captures
the stream at construction and loguru's ``logger.add(sys.stderr, ...)`` captures
the stream object at ``add`` time. ``TaklerServer.start`` calls
``takler.logging.configure()`` *internally* before emitting its first record,
so the whole ``await server.start()`` call is wrapped in
``contextlib.redirect_stderr(buffer)`` to make the freshly installed console
sink bind to the in-memory buffer rather than the real stderr.

State isolation
---------------
loguru's ``logger`` is a process-wide singleton and the stdlib ``takler``
logger accumulates handlers, so an autouse fixture clears both backends'
installed sinks and resets the module-level "configured" flag before and after
each test. ``TAKLER_LOG_*`` environment variables are cleared so the default
resolution (INFO, console on, no file sink) is exercised cleanly
(Requirements 7.5, 10.2).

Covered acceptance criteria
---------------------------
* 10.1 -- ``configure`` runs before the first server record is emitted.
* 10.2 -- the default configuration derives from environment + built-ins
  (``configure()`` is invoked with no explicit arguments).
* 10.3 -- a configuration failure still allows startup to proceed with a
  console sink at INFO.
* 10.4 -- a configuration failure emits a WARNING describing the failure to the
  console.
* 10.5 -- server start emits an INFO record.
* 10.6 -- the network service emits an INFO record when handling a command.
* 10.7 -- server shutdown emits an INFO record.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import io
import logging
from typing import Iterator
from unittest import mock

import pytest

import takler.logging
import takler.server as server_mod
from takler.server import TaklerServer
from takler.server.network_service import TaklerService
from takler.server.protocol import takler_pb2
from takler.logging.backends.stdlib_backend import (
    ROOT_LOGGER_NAME,
    _MANAGED_HANDLER_FLAG,
)

# loguru is an optional backend; when installed it is the active backend and
# its process-wide singleton sinks must be cleared between tests too.
LOGURU_INSTALLED = importlib.util.find_spec("loguru") is not None


def _clear_global_logging_state() -> None:
    """Tear down every sink this subsystem installed on either backend."""
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    for handler in list(logger.handlers):
        if getattr(handler, _MANAGED_HANDLER_FLAG, False):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:  # noqa: BLE001 - cleanup must not raise
                pass

    if LOGURU_INSTALLED:
        from loguru import logger as loguru_logger

        loguru_logger.remove()


@pytest.fixture(autouse=True)
def _server_logging_isolation(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Provide a clean logging slate and clean environment for each test."""
    monkeypatch.delenv("TAKLER_LOG_LEVEL", raising=False)
    monkeypatch.delenv("TAKLER_LOG_FILE", raising=False)

    _clear_global_logging_state()
    takler.logging._reset_configured_state()
    try:
        yield
    finally:
        takler.logging._reset_configured_state()
        _clear_global_logging_state()


def _make_hermetic_server() -> TaklerServer:
    """Build a server whose scheduler/network service are no-op AsyncMocks.

    Construction does not bind any port (``TaklerService`` defers gRPC setup to
    its ``start``), and replacing both collaborators with ``AsyncMock`` means
    ``await server.start()`` / ``await server.stop()`` perform no real network
    work -- only the logging behavior under test runs.
    """
    server = TaklerServer(host="localhost", port=33999)
    server.scheduler = mock.AsyncMock()
    server.network_service = mock.AsyncMock()
    return server


def test_configure_runs_before_first_record_and_start_emits_info() -> None:
    """Req 10.1, 10.2, 10.5: configure precedes the first record; start logs INFO.

    A spy wraps ``takler.server.configure`` so it (a) snapshots the captured
    console output at the moment it is invoked -- which must not yet contain any
    "start server" record, proving configuration runs *before* the first server
    record (Req 10.1) -- and (b) records the call arguments, which must be empty
    so the configuration derives from environment + built-in defaults (Req 10.2).
    The real ``configure`` is then delegated to, installing the console sink
    bound to the in-memory buffer, and the start INFO record must appear in it
    (Req 10.5).
    """
    server = _make_hermetic_server()

    buffer = io.StringIO()
    real_configure = takler.logging.configure
    snapshots: list[str] = []
    recorded_calls: list[tuple] = []

    def spy_configure(*args, **kwargs):
        # Snapshot console output at configure time: no server record yet.
        snapshots.append(buffer.getvalue())
        recorded_calls.append((args, kwargs))
        return real_configure(*args, **kwargs)

    with contextlib.redirect_stderr(buffer):
        with mock.patch.object(server_mod, "configure", side_effect=spy_configure):
            asyncio.run(server.start())

    output = buffer.getvalue()

    # Req 10.1: configure ran exactly once and, at that moment, the first
    # server record ("start server...") had not been emitted yet.
    assert len(snapshots) == 1
    assert "start server" not in snapshots[0]

    # Req 10.2: configure was invoked with no explicit arguments, so the
    # configuration derives from environment variables + built-in defaults.
    assert recorded_calls == [((), {})]

    # Req 10.5: the server start event is recorded at INFO level.
    assert "start server..." in output
    assert "start server...done" in output
    assert "INFO" in output

    # Sanity: the hermetic collaborators were actually started (no real I/O).
    server.scheduler.start.assert_awaited_once()
    server.network_service.start.assert_awaited_once()


def test_configuration_failure_allows_startup_with_console_info_and_warning() -> None:
    """Req 10.3, 10.4, 10.5: a configure failure degrades gracefully.

    The spy raises on its first invocation (simulating a configuration failure)
    and delegates to the real ``configure`` on the fallback invocation. Startup
    must not raise; a WARNING describing the failure must reach the console
    (Req 10.4); and the start INFO records must still appear because the
    fallback installs a console sink at INFO (Req 10.3, 10.5).
    """
    server = _make_hermetic_server()

    buffer = io.StringIO()
    real_configure = takler.logging.configure
    call_count = {"n": 0}
    failure_message = "simulated logging configuration failure"

    def spy_configure(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # First call (the unguarded ``configure()``) fails.
            raise RuntimeError(failure_message)
        # Fallback call (``configure(level="INFO", console=True)``) succeeds,
        # installing the console sink bound to the buffer.
        return real_configure(*args, **kwargs)

    with contextlib.redirect_stderr(buffer):
        with mock.patch.object(server_mod, "configure", side_effect=spy_configure):
            # Req 10.3: startup proceeds without raising despite the failure.
            asyncio.run(server.start())

    output = buffer.getvalue()

    # The first call failed and a fallback call was made.
    assert call_count["n"] == 2

    # Req 10.4: a WARNING describing the configuration failure reaches console.
    assert "WARNING" in output
    assert "logging configuration failed" in output
    assert failure_message in output

    # Req 10.3, 10.5: the fallback console sink is at INFO and the start records
    # still appear.
    assert "start server..." in output
    assert "start server...done" in output

    # Startup still completed end to end.
    server.scheduler.start.assert_awaited_once()
    server.network_service.start.assert_awaited_once()


def test_command_handling_emits_info_record() -> None:
    """Req 10.6: handling a command emits an INFO record naming the command.

    A ``TaklerService`` is built with a mock scheduler and its
    ``RunCommandComplete`` handler invoked with a stub request. Logging is
    configured inside the redirect so the console sink binds to the buffer, and
    the handled command must appear in an INFO record.
    """
    scheduler = mock.MagicMock()
    service = TaklerService(scheduler=scheduler, host="[::]", port=33999)

    request = mock.MagicMock()
    request.child_options.node_path = "/flow1/task1"
    context = mock.MagicMock()

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        # Install a console sink bound to the buffer (mirrors what server start
        # does before any command is handled).
        takler.logging.configure()
        response = asyncio.run(service.RunCommandComplete(request, context))

    output = buffer.getvalue()

    # The handler logged the command-handling event at INFO.
    assert "INFO" in output
    assert "Complete: /flow1/task1" in output

    # The handler delegated to the scheduler and returned a normal response.
    scheduler.run_command_complete.assert_called_once_with("/flow1/task1")
    assert isinstance(response, takler_pb2.ServiceResponse)
    assert response.flag == 0


def test_shutdown_emits_info_record() -> None:
    """Req 10.7: server shutdown emits INFO records for the stop event."""
    server = _make_hermetic_server()

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        # Bind the console sink to the buffer the same way start would.
        takler.logging.configure()
        asyncio.run(server.stop())

    output = buffer.getvalue()

    # Req 10.7: the shutdown event is recorded at INFO level.
    assert "INFO" in output
    assert "stop server..." in output
    assert "stop server...done" in output

    # The hermetic collaborators were stopped (no real I/O).
    server.network_service.stop.assert_awaited_once()
    server.scheduler.stop.assert_awaited_once()
