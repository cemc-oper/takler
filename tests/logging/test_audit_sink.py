"""Tests for the audit sink of the logging subsystem (Requirements 11.1, 11.12, 11.13).

The audit sink is the third sink of the logging subsystem: when
``ResolvedConfig.audit_file`` is set, records of the ``audit`` component go
*only* to that file, and every other component's records stay *out* of it. The
isolation must hold identically on both backends (Requirement 11.13's
"equivalent behavior" contract), so the behavioral tests here run against the
shared ``backend`` fixture, which is parametrized over stdlib and (when
installed) loguru.

Capturing approach mirrors ``test_sink_equivalence_property.py``: the backend is
configured *inside* a ``contextlib.redirect_stderr`` block so that both backends
bind their console sink to an in-memory buffer, and the sinks are torn down
before the files are read back so their buffers are flushed and closed.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import tempfile

import pytest

from takler.logging.config import (
    AUDIT_COMPONENT,
    ENV_AUDIT_FILE,
    ResolvedConfig,
    resolve_config,
)
from takler.logging.levels import LogLevel

# A realistic Audit_Record: a single-line JSON object carrying its own
# ``timestamp`` key (Requirement 11.5). Any prefix added by the audit sink would
# make the line invalid JSON.
_AUDIT_LINE = json.dumps(
    {
        "timestamp": "2026-07-15T10:30:00",
        "event": "control",
        "command": "requeue",
        "user": "alice",
        "peer": "ipv4:127.0.0.1:54321",
        "target": ["/flow1/task1"],
        "outcome": "success",
        "error_code": 0,
    }
)


def _config(**overrides) -> ResolvedConfig:
    """Build a ResolvedConfig with every sink off unless overridden."""
    settings = {
        "level": LogLevel.INFO,
        "console": False,
        "log_file": None,
        "rotation": None,
        "retention": None,
        "audit_file": None,
    }
    settings.update(overrides)
    return ResolvedConfig(**settings)


def _teardown_sinks(backend) -> None:
    """Reconfigure with no sinks, flushing and closing every file handler."""
    backend.apply_config(_config())


def _read_lines(path: str) -> list:
    """Return the lines of ``path``, or ``[]`` when it was never created."""
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read().splitlines()


def test_audit_records_are_isolated_to_the_audit_file(backend) -> None:
    """Req 11.12: audit records reach only the Audit_File, in both directions.

    With the console, a regular log file and an audit file all active, an
    ``audit`` record must appear only in the audit file, and a record from any
    other component must appear only on the console and in the regular log file.
    """
    temp_dir = tempfile.mkdtemp(prefix="takler-audit-")
    log_path = os.path.join(temp_dir, "takler.log")
    audit_path = os.path.join(temp_dir, "audit.jsonl")

    try:
        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            backend.apply_config(
                _config(console=True, log_file=log_path, audit_file=audit_path)
            )
            backend.get_named_logger(AUDIT_COMPONENT).info(_AUDIT_LINE)
            backend.get_named_logger("server.scheduler").info("scheduler started")

        _teardown_sinks(backend)

        console_text = buffer.getvalue()
        log_lines = _read_lines(log_path)
        audit_lines = _read_lines(audit_path)

        # The audit record went to the audit file only.
        assert audit_lines == [_AUDIT_LINE]
        assert _AUDIT_LINE not in console_text
        assert all(_AUDIT_LINE not in line for line in log_lines)

        # The regular record went to the console and the regular file only.
        assert "scheduler started" in console_text
        assert len(log_lines) == 1
        assert "scheduler started" in log_lines[0]
        assert all("scheduler started" not in line for line in audit_lines)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_audit_line_is_the_bare_message(backend) -> None:
    """Req 11.5: each audit line is the bare message, so it is valid JSON.

    The audit sink must not add the regular ``timestamp level component``
    prefix; the line has to survive a JSON round-trip unchanged.
    """
    temp_dir = tempfile.mkdtemp(prefix="takler-audit-")
    audit_path = os.path.join(temp_dir, "audit.jsonl")

    try:
        backend.apply_config(_config(console=False, audit_file=audit_path))
        backend.get_named_logger(AUDIT_COMPONENT).info(_AUDIT_LINE)
        _teardown_sinks(backend)

        lines = _read_lines(audit_path)
        assert lines == [_AUDIT_LINE]
        assert json.loads(lines[0])["command"] == "requeue"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_audit_records_fall_back_to_regular_sinks_without_audit_file(
    backend,
) -> None:
    """Req 11.13: with no Audit_File, audit records use the configured sinks.

    No audit sink means no isolation either -- otherwise audit records would
    vanish entirely.
    """
    temp_dir = tempfile.mkdtemp(prefix="takler-audit-")
    log_path = os.path.join(temp_dir, "takler.log")

    try:
        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            backend.apply_config(_config(console=True, log_file=log_path))
            backend.get_named_logger(AUDIT_COMPONENT).info(_AUDIT_LINE)

        _teardown_sinks(backend)

        assert _AUDIT_LINE in buffer.getvalue()
        log_lines = _read_lines(log_path)
        assert len(log_lines) == 1
        assert _AUDIT_LINE in log_lines[0]
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_audit_sink_teardown_is_idempotent(backend) -> None:
    """Req 1.4: reconfiguring never duplicates or leaks the audit sink.

    Applying the same audit configuration twice must not double records, and
    reconfiguring without an ``audit_file`` must stop writing to the old file.
    """
    temp_dir = tempfile.mkdtemp(prefix="takler-audit-")
    audit_path = os.path.join(temp_dir, "audit.jsonl")

    try:
        config = _config(console=False, audit_file=audit_path)
        backend.apply_config(config)
        backend.apply_config(config)
        backend.get_named_logger(AUDIT_COMPONENT).info(_AUDIT_LINE)

        # Drop the audit sink; the record below must not reach the audit file.
        backend.apply_config(_config(console=False))
        backend.get_named_logger(AUDIT_COMPONENT).info("after teardown")
        _teardown_sinks(backend)

        assert _read_lines(audit_path) == [_AUDIT_LINE]
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.parametrize(
    "explicit, env_value, expected",
    [
        ({}, None, None),
        ({}, "", None),
        ({}, "   ", None),
        ({}, "/from/env.jsonl", "/from/env.jsonl"),
        ({"audit_file": "/explicit.jsonl"}, None, "/explicit.jsonl"),
        ({"audit_file": "/explicit.jsonl"}, "/from/env.jsonl", "/explicit.jsonl"),
    ],
)
def test_audit_file_precedence(explicit, env_value, expected) -> None:
    """Req 11.12: audit_file follows explicit > TAKLER_AUDIT_FILE > unset."""
    env = {} if env_value is None else {ENV_AUDIT_FILE: env_value}
    assert resolve_config(explicit, env).audit_file == expected


def test_audit_file_accepts_path_like(tmp_path) -> None:
    """An ``os.PathLike`` audit_file is coerced to ``str`` like ``log_file``."""
    target = tmp_path / "audit.jsonl"
    resolved = resolve_config({"audit_file": target}, {})
    assert resolved.audit_file == os.fspath(target)
