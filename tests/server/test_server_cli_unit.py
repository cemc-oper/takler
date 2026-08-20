"""Unit tests for the ``takler-server`` entry point.

The CLI is driven through typer's ``CliRunner`` with ``TaklerServer`` and the
event loop runner replaced by fakes, so no port is ever bound: what matters here
is that ``--help`` works as the contract says and that the startup options reach
``TaklerServer`` unchanged.

Requirements: 15.3, 15.4, 6.23.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from takler.constant import DEFAULT_HOST, DEFAULT_PORT
from takler.server import cli
from takler.server.connect_config import TAKLER_CONNECT_FILE


runner = CliRunner()

#: Every option the entry point must offer (requirement 15.4 requires at least
#: host, port and the Connect_Config file path).
EXPECTED_OPTIONS = [
    "--host",
    "--port",
    "--config",
    "--checkpoint-file",
    "--checkpoint-interval",
    "--exception-policy",
]

#: Wide terminal so long option names are not wrapped in the help output.
WIDE = {"COLUMNS": "200", "TERM": "dumb"}

#: Repository root holding the ``takler`` package, used as the working directory
#: of the ``python -m takler.server`` subprocess.
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class FakeServer:
    """Records the constructor arguments instead of building a real server."""

    instances: list = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        FakeServer.instances.append(self)


@pytest.fixture
def fake_server(monkeypatch):
    """Replace ``TaklerServer`` and the loop runner, and clear the env var."""
    FakeServer.instances = []
    served = []

    monkeypatch.delenv(TAKLER_CONNECT_FILE, raising=False)
    monkeypatch.setattr(cli, "TaklerServer", FakeServer)
    monkeypatch.setattr(cli, "serve_forever", lambda server: served.append(server))
    return served


def write_connect_config(path: Path, hostname: str, port: str) -> Path:
    """Write a minimal ``connect.yaml`` holding only the ``server`` section."""
    content = {
        "server": {
            "address": {"hostname": hostname, "ip": "127.0.0.1", "port": port}
        }
    }
    with open(path, "w") as f:
        yaml.safe_dump(content, f)
    return path


# --help ------------------------------------------------------------


def test_help_exits_zero_and_lists_the_startup_options():
    result = runner.invoke(cli.app, ["--help"], env=WIDE)

    assert result.exit_code == 0
    for option in EXPECTED_OPTIONS:
        assert option in result.output


def test_host_and_port_help_carry_the_restart_precondition():
    """Requirement 6.23 is an operational precondition, so it is in the help."""
    result = runner.invoke(cli.app, ["--help"], env=WIDE)

    assert result.exit_code == 0
    for text in ("submitted", "active", "checkpoint"):
        assert text in cli.HOST_HELP
        assert text in cli.PORT_HELP


def test_python_dash_m_help_matches():
    """``python -m takler.server --help`` is the same CLI (requirement 15.3)."""
    completed = subprocess.run(
        [sys.executable, "-m", "takler.server", "--help"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, **WIDE},
    )

    assert completed.returncode == 0
    for option in EXPECTED_OPTIONS:
        assert option in completed.stdout


# option pass-through ------------------------------------------------


def test_options_are_passed_to_the_server(fake_server, tmp_path):
    checkpoint_file = tmp_path / "sub" / "takler.check"

    result = runner.invoke(
        cli.app,
        [
            "--host", "login_a06",
            "--port", "34567",
            "--checkpoint-file", str(checkpoint_file),
            "--checkpoint-interval", "45",
            "--exception-policy", "fail_fast",
        ],
        env=WIDE,
    )

    assert result.exit_code == 0
    kwargs = FakeServer.instances[0].kwargs
    assert kwargs["host"] == "login_a06"
    assert kwargs["port"] == 34567
    assert kwargs["checkpoint_file"] == checkpoint_file
    assert kwargs["checkpoint_interval"] == 45.0
    assert kwargs["exception_policy"] == "fail_fast"
    # The server is actually run, not just constructed.
    assert fake_server == [FakeServer.instances[0]]


def test_defaults_are_used_when_nothing_is_given(fake_server):
    result = runner.invoke(cli.app, [], env=WIDE)

    assert result.exit_code == 0
    kwargs = FakeServer.instances[0].kwargs
    assert kwargs["host"] == DEFAULT_HOST
    assert kwargs["port"] == int(DEFAULT_PORT)
    assert kwargs["connect_config"] is None
    assert kwargs["checkpoint_file"] is None
    assert kwargs["checkpoint_interval"] is None
    assert kwargs["exception_policy"] is None


# Connect_Config resolution ------------------------------------------


def test_config_option_provides_host_port_and_config_object(fake_server, tmp_path):
    write_connect_config(tmp_path / "connect.yaml", "login_b01", "35001")

    result = runner.invoke(
        cli.app, ["--config", str(tmp_path / "connect.yaml")], env=WIDE
    )

    assert result.exit_code == 0
    kwargs = FakeServer.instances[0].kwargs
    assert (kwargs["host"], kwargs["port"]) == ("login_b01", 35001)
    # The whole config object is handed over, so the checkpoint section is
    # visible to the checkpoint manager as well.
    assert kwargs["connect_config"].server.address.hostname == "login_b01"


def test_command_line_address_wins_over_the_config_file(fake_server, tmp_path):
    write_connect_config(tmp_path / "connect.yaml", "login_b01", "35001")

    result = runner.invoke(
        cli.app,
        ["--config", str(tmp_path / "connect.yaml"), "--host", "login_a06"],
        env=WIDE,
    )

    assert result.exit_code == 0
    kwargs = FakeServer.instances[0].kwargs
    assert (kwargs["host"], kwargs["port"]) == ("login_a06", 35001)


def test_env_var_is_used_when_the_option_is_omitted(fake_server, tmp_path, monkeypatch):
    config_path = write_connect_config(tmp_path / "connect.yaml", "login_c02", "35002")
    monkeypatch.setenv(TAKLER_CONNECT_FILE, str(config_path))

    result = runner.invoke(cli.app, [], env=WIDE)

    assert result.exit_code == 0
    kwargs = FakeServer.instances[0].kwargs
    assert (kwargs["host"], kwargs["port"]) == ("login_c02", 35002)


def test_unreadable_env_config_is_ignored_but_a_bad_option_is_not(
    fake_server, tmp_path, monkeypatch
):
    """A stale env var must not abort start-up; an explicit ``--config`` must."""
    monkeypatch.setenv(TAKLER_CONNECT_FILE, str(tmp_path / "missing.yaml"))

    result = runner.invoke(cli.app, [], env=WIDE)
    assert result.exit_code == 0
    assert FakeServer.instances[0].kwargs["connect_config"] is None

    result = runner.invoke(
        cli.app, ["--config", str(tmp_path / "missing.yaml")], env=WIDE
    )
    assert result.exit_code != 0
