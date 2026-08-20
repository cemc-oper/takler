"""Unit tests for the ``checkpoint`` section of :class:`ConnectConfig`.

Task 8.1 of the *m1-operational-baseline* spec adds a ``checkpoint`` section to
the ``connect.yaml`` model. Two behaviors matter to callers:

* a legacy ``connect.yaml`` carrying only the ``server`` section is still
  loadable and yields an all-``None`` :class:`CheckpointSettings`, so a missing
  section means "not configured" rather than a load failure;
* :func:`save_connect_config` writes the ``checkpoint`` section out, so a file
  produced by the server documents the configurable knobs.

``None`` (rather than the built-in defaults 120 seconds / ``takler.check``) is
the "absent" marker, which is what lets the Checkpoint_Manager apply the
precedence ``explicit argument > Connect_Config file > built-in default``.

Validates: Requirements 7.1
"""

from __future__ import annotations

import yaml

from takler.server.connect_config import (
    Address,
    CheckpointSettings,
    ConnectConfig,
    Server,
    load_connect_config,
    save_connect_config,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _server_section() -> dict:
    return {
        "address": {
            "hostname": "login01",
            "ip": "10.0.0.11",
            "port": "33083",
        }
    }


def _make_config(checkpoint: CheckpointSettings | None = None) -> ConnectConfig:
    server = Server(address=Address(**_server_section()["address"]))
    if checkpoint is None:
        return ConnectConfig(server=server)
    return ConnectConfig(server=server, checkpoint=checkpoint)


# ---------------------------------------------------------------------------
# CheckpointSettings defaults
# ---------------------------------------------------------------------------


def test_checkpoint_settings_default_to_none():
    """Both knobs default to ``None``, i.e. "not configured"."""
    settings = CheckpointSettings()

    assert settings.interval is None
    assert settings.file is None


def test_connect_config_default_checkpoint_is_not_shared():
    """Each :class:`ConnectConfig` gets its own ``checkpoint`` instance."""
    first = _make_config()
    second = _make_config()

    first.checkpoint.interval = 30.0

    assert second.checkpoint.interval is None


# ---------------------------------------------------------------------------
# Backward compatible loading
# ---------------------------------------------------------------------------


def test_load_connect_config_without_checkpoint_section(tmp_path):
    """A legacy file holding only ``server`` still loads (Requirement 7.1)."""
    file_path = tmp_path / "connect.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump({"server": _server_section()}, f)

    config = load_connect_config(file_path)

    assert config.server.address.hostname == "login01"
    assert config.server.address.ip == "10.0.0.11"
    assert config.server.address.port == "33083"
    assert config.checkpoint.interval is None
    assert config.checkpoint.file is None


def test_load_connect_config_with_checkpoint_section(tmp_path):
    """Configured values are read back from the ``checkpoint`` section."""
    file_path = tmp_path / "connect.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(
            {
                "server": _server_section(),
                "checkpoint": {"interval": 45.5, "file": "/var/takler/takler.check"},
            },
            f,
        )

    config = load_connect_config(file_path)

    assert config.checkpoint.interval == 45.5
    assert config.checkpoint.file == "/var/takler/takler.check"


def test_load_connect_config_with_partial_checkpoint_section(tmp_path):
    """A partially filled section leaves the omitted knob at ``None``."""
    file_path = tmp_path / "connect.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(
            {"server": _server_section(), "checkpoint": {"interval": 300}},
            f,
        )

    config = load_connect_config(file_path)

    assert config.checkpoint.interval == 300.0
    assert config.checkpoint.file is None


# ---------------------------------------------------------------------------
# Saving
# ---------------------------------------------------------------------------


def test_save_connect_config_writes_checkpoint_section(tmp_path):
    """``save_connect_config`` emits a ``checkpoint`` section (Requirement 7.1)."""
    file_path = tmp_path / "connect.yaml"

    save_connect_config(_make_config(), file_path)

    with open(file_path, "r") as f:
        written = yaml.safe_load(f)

    assert "checkpoint" in written
    assert written["checkpoint"] == {"interval": None, "file": None}
    assert written["server"] == _server_section()


def test_save_connect_config_round_trip_preserves_checkpoint(tmp_path):
    """Configured values survive a save / load round trip."""
    file_path = tmp_path / "connect.yaml"
    config = _make_config(CheckpointSettings(interval=120.0, file="run/takler.check"))

    save_connect_config(config, file_path)
    loaded = load_connect_config(file_path)

    assert loaded.checkpoint.interval == 120.0
    assert loaded.checkpoint.file == "run/takler.check"
    assert loaded == config
