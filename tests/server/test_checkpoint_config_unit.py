"""Unit tests for the Checkpoint_Manager configuration layer.

Task 8.2 of the *m1-operational-baseline* spec introduces
``takler/server/checkpoint.py`` with the snapshot constants, the two pure
resolvers and the path properties. This file pins the boundaries that are easy
to get wrong; the exhaustive "for all inputs" assertions on the precedence
order, on rejected periods and on the backup path live in the property tests.

The WARNING for a rejected snapshot period is asserted through a captured
console sink rather than ``caplog``: the logging backend does not route records
into pytest's handler, so the sink has to be configured inside the redirection
block (same approach as the client retry unit tests).

Validates: Requirements 7.2, 7.3, 7.4, 7.5, 7.6
"""

from __future__ import annotations

import contextlib
import io
from pathlib import Path

import pytest

import takler.logging
from takler.core.bunch import Bunch
from takler.server.checkpoint import (
    BACKUP_SUFFIX,
    CHECKPOINT_FORMAT_VERSION,
    DEFAULT_CHECKPOINT_FILE,
    DEFAULT_CHECKPOINT_INTERVAL,
    EARLIEST_SUPPORTED_FORMAT_VERSION,
    MIN_CHECKPOINT_INTERVAL,
    CheckpointManager,
    _resolve_interval,
    _resolve_path,
)
from takler.server.connect_config import (
    Address,
    CheckpointSettings,
    ConnectConfig,
    Server,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config(interval=None, file=None) -> ConnectConfig:
    return ConnectConfig(
        server=Server(
            address=Address(hostname="login01", ip="10.0.0.11", port="33083")
        ),
        checkpoint=CheckpointSettings(interval=interval, file=file),
    )


def _make_manager(**kwargs) -> CheckpointManager:
    return CheckpointManager(bunch=Bunch(host="login01", port="33083"), **kwargs)


def _capturing_stderr(func):
    """Run ``func`` while capturing the console log output.

    The console sink binds to ``sys.stderr`` when the configuration is applied,
    so the configuration must happen inside the redirection block for records
    to land in the buffer.
    """
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="WARNING", console=True)
            result = func()
    finally:
        # Rebind the console sink to the real stderr, otherwise later tests
        # would log into this example's closed buffer.
        takler.logging.configure(console=True)
    return result, buffer.getvalue()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

def test_constants():
    assert CHECKPOINT_FORMAT_VERSION == 1
    assert EARLIEST_SUPPORTED_FORMAT_VERSION == 1
    assert DEFAULT_CHECKPOINT_INTERVAL == 120.0
    assert MIN_CHECKPOINT_INTERVAL == 10.0
    assert DEFAULT_CHECKPOINT_FILE == "takler.check"
    assert BACKUP_SUFFIX == ".b"
    assert EARLIEST_SUPPORTED_FORMAT_VERSION <= CHECKPOINT_FORMAT_VERSION


# ---------------------------------------------------------------------------
# Interval resolution (Requirements 7.2, 7.5)
# ---------------------------------------------------------------------------

def test_resolve_interval_defaults_when_nothing_configured():
    assert _resolve_interval() == DEFAULT_CHECKPOINT_INTERVAL
    assert _resolve_interval(None, _config()) == DEFAULT_CHECKPOINT_INTERVAL


def test_resolve_interval_uses_config_when_no_explicit_value():
    assert _resolve_interval(None, _config(interval=300.0)) == 300.0


def test_resolve_interval_explicit_wins_over_config():
    assert _resolve_interval(30.0, _config(interval=300.0)) == 30.0


def test_resolve_interval_explicit_wins_over_rejected_config_silently():
    """A rejected file value must not warn when it is overridden anyway."""
    interval, captured = _capturing_stderr(
        lambda: _resolve_interval(30.0, _config(interval=1.0))
    )

    assert interval == 30.0
    assert "WARNING" not in captured


# ---------------------------------------------------------------------------
# Interval validation (Requirement 7.6)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value", [0.0, -1.0, -0.5, 0.001, 1.0, 9.999])
def test_resolve_interval_rejects_short_and_non_positive(value):
    interval, captured = _capturing_stderr(lambda: _resolve_interval(value))

    assert interval == DEFAULT_CHECKPOINT_INTERVAL
    warnings = [line for line in captured.splitlines() if "WARNING" in line]
    assert len(warnings) == 1
    assert repr(value) in warnings[0]


def test_resolve_interval_accepts_the_minimum():
    interval, captured = _capturing_stderr(
        lambda: _resolve_interval(MIN_CHECKPOINT_INTERVAL)
    )

    assert interval == MIN_CHECKPOINT_INTERVAL
    assert "WARNING" not in captured


def test_manager_falls_back_to_default_interval_with_warning():
    manager, captured = _capturing_stderr(lambda: _make_manager(interval=5.0))

    assert manager.interval == DEFAULT_CHECKPOINT_INTERVAL
    assert len([line for line in captured.splitlines() if "WARNING" in line]) == 1


# ---------------------------------------------------------------------------
# Path resolution (Requirements 7.3, 7.5)
# ---------------------------------------------------------------------------

def test_resolve_path_defaults_to_cwd_relative_name():
    path = _resolve_path()

    assert path == Path(DEFAULT_CHECKPOINT_FILE)
    assert not path.is_absolute()


def test_resolve_path_uses_config_when_no_explicit_value():
    assert _resolve_path(None, _config(file="run/takler.check")) == Path(
        "run/takler.check"
    )


def test_resolve_path_explicit_wins_over_config():
    assert _resolve_path("/var/takler/a.check", _config(file="run/b.check")) == Path(
        "/var/takler/a.check"
    )


def test_resolve_path_accepts_path_objects(tmp_path):
    explicit = tmp_path / "state" / "takler.check"

    assert _resolve_path(explicit) == explicit


@pytest.mark.parametrize("blank", ["", "   "])
def test_resolve_path_treats_blank_as_absent(blank):
    """A blank configured path falls through to the next source."""
    assert _resolve_path(blank) == Path(DEFAULT_CHECKPOINT_FILE)
    assert _resolve_path(None, _config(file=blank)) == Path(DEFAULT_CHECKPOINT_FILE)
    assert _resolve_path(blank, _config(file="run/takler.check")) == Path(
        "run/takler.check"
    )


# ---------------------------------------------------------------------------
# Manager properties (Requirement 7.4)
# ---------------------------------------------------------------------------

def test_manager_defaults():
    manager = _make_manager()

    assert manager.interval == DEFAULT_CHECKPOINT_INTERVAL
    assert manager.checkpoint_file == Path(DEFAULT_CHECKPOINT_FILE)
    assert manager.backup_file == Path(DEFAULT_CHECKPOINT_FILE + BACKUP_SUFFIX)


def test_manager_reads_connect_config(tmp_path):
    manager = _make_manager(
        connect_config=_config(interval=600.0, file=str(tmp_path / "takler.check"))
    )

    assert manager.interval == 600.0
    assert manager.checkpoint_file == tmp_path / "takler.check"


def test_backup_file_appends_suffix(tmp_path):
    manager = _make_manager(checkpoint_file=tmp_path / "nested" / "state.check")

    assert manager.backup_file == tmp_path / "nested" / ("state.check" + BACKUP_SUFFIX)
    assert str(manager.backup_file) == str(manager.checkpoint_file) + BACKUP_SUFFIX


def test_manager_keeps_bunch_reference():
    bunch = Bunch(host="login01", port="33083")
    manager = CheckpointManager(bunch=bunch)

    assert manager.bunch is bunch
