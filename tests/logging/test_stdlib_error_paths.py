"""Unit tests for the stdlib backend's graceful file-sink error paths.

These cover the "never break the caller" behavior of
:class:`~takler.logging.backends.stdlib_backend.StdlibBackend` when a file sink
cannot be established:

* **Requirement 5.6** -- the parent directory of the configured path cannot be
  created.
* **Requirement 5.5** -- the configured path cannot be opened for writing.
* **Requirement 9.4** -- any other stdlib configuration/permission error while
  applying settings.

In every case the backend must:

* return from ``apply_config`` without raising;
* not establish the file sink;
* record a :class:`~takler.logging.errors.SettingFailure` for ``"log_file"``
  whose message names the offending path;
* emit a WARNING record naming the path/failure to the still-active console
  sink; and
* keep the console sink active at the configured level, with
  ``ApplyResult.applied`` reflecting the resolved config.

These are stdlib-specific scenarios, so the tests construct ``StdlibBackend()``
directly rather than parametrizing over backends. The console sink binds its
stream at handler-construction time, so tests that assert on console output wrap
``apply_config`` (and any subsequent emission) in ``contextlib.redirect_stderr``
to capture the stream deterministically.
"""

from __future__ import annotations

import contextlib
import io
import logging
import os
from pathlib import Path
from typing import Iterator, List

import pytest

from takler.logging.backends.stdlib_backend import (
    ROOT_LOGGER_NAME,
    StdlibBackend,
    _MANAGED_HANDLER_FLAG,
)
from takler.logging.config import ResolvedConfig
from takler.logging.levels import LogLevel


def _managed_handlers() -> List[logging.Handler]:
    """Return the handlers this subsystem installed on the ``takler`` logger."""
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    return [
        handler
        for handler in logger.handlers
        if getattr(handler, _MANAGED_HANDLER_FLAG, False)
    ]


def _managed_file_handlers() -> List[logging.Handler]:
    """Return the managed handlers that are file sinks (file sink established)."""
    return [h for h in _managed_handlers() if isinstance(h, logging.FileHandler)]


def _clear_managed_handlers() -> None:
    """Detach and close every managed handler on the ``takler`` logger."""
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    for handler in list(logger.handlers):
        if getattr(handler, _MANAGED_HANDLER_FLAG, False):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:  # noqa: BLE001 - cleanup must not raise
                pass


@pytest.fixture(autouse=True)
def _isolate_takler_logger() -> Iterator[None]:
    """Isolate global ``takler`` logger state before and after each test."""
    _clear_managed_handlers()
    try:
        yield
    finally:
        _clear_managed_handlers()


def _config(log_file: str, level: LogLevel = LogLevel.INFO) -> ResolvedConfig:
    """Build a console-enabled ResolvedConfig pointing the file sink at a path."""
    return ResolvedConfig(
        level=level,
        console=True,
        log_file=log_file,
        rotation=None,
        retention=None,
    )


def test_uncreatable_parent_directory_degrades_gracefully(tmp_path: Path) -> None:
    """Req 5.6: an uncreatable parent directory yields a graceful failure.

    With an existing regular file standing in for a directory ancestor, the
    parent of the configured path cannot be created. The backend must not
    raise, must record a ``log_file`` failure naming the path, must not create
    the file or establish a file sink, and must keep the console sink active.
    """
    # An ancestor that is a regular file makes the parent uncreatable.
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file, not a directory", encoding="utf-8")
    log_path = str(blocker / "sub" / "takler.log")

    backend = StdlibBackend()

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        result = backend.apply_config(_config(log_path))

    # Did not raise; the file sink was not established.
    log_failures = [f for f in result.failures if f.setting_name == "log_file"]
    assert len(log_failures) == 1
    assert log_path in log_failures[0].reason
    assert _managed_file_handlers() == []

    # No file was created on disk.
    assert not os.path.exists(log_path)

    # The console sink is still attached and usable.
    console_handlers = _managed_handlers()
    assert len(console_handlers) == 1
    assert not isinstance(console_handlers[0], logging.FileHandler)

    # Applied config reflects the resolved configuration (Req 9.4).
    assert result.applied.log_file == log_path

    # Console still emits subsequent records.
    backend.get_named_logger("tests.error_paths").info("post-failure record")
    assert "post-failure record" in buffer.getvalue()


def test_unopenable_file_path_degrades_gracefully(tmp_path: Path) -> None:
    """Req 5.5: a path that cannot be opened for writing fails gracefully.

    Pointing ``log_file`` at an existing directory makes opening it as a file
    fail. The backend must not raise, must record a ``log_file`` failure naming
    the path, must not establish a file sink, and must keep the console active.
    """
    # An existing directory cannot be opened as a file for writing.
    log_path = str(tmp_path / "a_directory")
    os.makedirs(log_path)

    backend = StdlibBackend()

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        result = backend.apply_config(_config(log_path))

    log_failures = [f for f in result.failures if f.setting_name == "log_file"]
    assert len(log_failures) == 1
    assert log_path in log_failures[0].reason

    # No file sink established; the path remains a directory (no file created).
    assert _managed_file_handlers() == []
    assert os.path.isdir(log_path)

    # Console sink remains active.
    assert len(_managed_handlers()) == 1

    # Console still emits subsequent records.
    backend.get_named_logger("tests.error_paths").info("still logging")
    assert "still logging" in buffer.getvalue()


def test_file_sink_failure_emits_console_warning_naming_path(tmp_path: Path) -> None:
    """Req 5.5/5.6: a WARNING naming the path is written to the console sink.

    Capturing stderr before ``apply_config`` (the console handler binds the
    stream at construction) lets us assert the failure notice is emitted to the
    console as a WARNING that names the offending path.
    """
    log_path = str(tmp_path / "an_existing_dir")
    os.makedirs(log_path)

    backend = StdlibBackend()

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        backend.apply_config(_config(log_path))

    console_output = buffer.getvalue()
    assert "WARNING" in console_output
    assert log_path in console_output


def test_generic_apply_failure_is_caught_and_console_continues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Req 9.4: a non-OSError build failure is caught; console stays at INFO.

    Injecting a generic (non-OSError) exception from ``_build_file_handler``
    exercises the catch-all branch: the file sink is not established, a
    ``log_file`` SettingFailure is recorded, the applied config is retained, no
    exception escapes, and the console sink continues at the configured INFO
    level.
    """
    # Parent directory exists so creation succeeds and we reach handler build.
    log_path = str(tmp_path / "logs" / "takler.log")

    def _boom(*_args: object, **_kwargs: object) -> logging.Handler:
        raise RuntimeError("synthetic apply failure")

    monkeypatch.setattr(StdlibBackend, "_build_file_handler", staticmethod(_boom))

    backend = StdlibBackend()

    buffer = io.StringIO()
    with contextlib.redirect_stderr(buffer):
        # Must not raise despite the injected non-OSError failure.
        result = backend.apply_config(_config(log_path, level=LogLevel.INFO))

    # The failure was caught and recorded against the log_file setting.
    log_failures = [f for f in result.failures if f.setting_name == "log_file"]
    assert len(log_failures) == 1
    assert log_path in log_failures[0].reason

    # No file sink established; applied config retained (Req 9.4).
    assert _managed_file_handlers() == []
    assert result.applied.level is LogLevel.INFO

    # Console sink remains active at INFO and keeps emitting.
    console_handlers = _managed_handlers()
    assert len(console_handlers) == 1
    assert console_handlers[0].level == logging.INFO
    assert logging.getLogger(ROOT_LOGGER_NAME).level == logging.INFO

    backend.get_named_logger("tests.error_paths").info("console alive")
    assert "console alive" in buffer.getvalue()
