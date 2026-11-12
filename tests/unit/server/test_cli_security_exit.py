"""Unit tests for the security exit path of the ``takler-server`` entry point.

A security configuration that cannot be used must stop the start-up: one line on
standard error naming what is wrong, and exit code 1 -- never a silent fall back
to a plaintext port (requirements 1.4, 1.5, 1.6).

The tests drive the real ``serve_forever`` through typer's ``CliRunner`` with
``TaklerServer`` replaced by :class:`FakeServer`, so no port is ever bound. The
fake resolves the TLS pair with the real
:func:`takler.server.tls.build_server_credentials`, the way the network service
does, which keeps the messages asserted here the ones an operator actually sees
while leaving the service wiring out of the picture.

Requirements: 1.4, 1.5, 1.6, 1.10.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from takler.server import cli
from takler.server.connect_config import TAKLER_CONNECT_FILE
from takler.server.tls import build_server_credentials


runner = CliRunner()

#: The two options added for the TLS pair (requirement 1.10).
TLS_OPTIONS = ["--tls-cert", "--tls-key"]

#: Wide terminal so long option names are not wrapped in the help output.
WIDE = {"COLUMNS": "200", "TERM": "dumb"}


class FakeServer:
    """Stands in for ``TaklerServer``, resolving only the TLS pair.

    ``start()`` calls :func:`build_server_credentials` with exactly the sources
    the real server has -- the explicit pair from the command line and the
    ``security`` section of the Connect_Config -- so a half configured or
    unreadable pair raises the same ``SecurityConfigError`` here as it does in a
    real start-up (requirements 1.4, 1.5).
    """

    instances: list = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.started = False
        self.ran = False
        self.stopped = False
        FakeServer.instances.append(self)

    async def start(self) -> None:
        self.started = True
        connect_config = self.kwargs["connect_config"]
        build_server_credentials(
            None if connect_config is None else connect_config.security,
            self.kwargs["tls_cert_file"],
            self.kwargs["tls_key_file"],
        )

    async def run(self) -> None:
        self.ran = True

    async def stop(self) -> None:
        self.stopped = True


@pytest.fixture
def fake_server(monkeypatch):
    """Replace ``TaklerServer`` and clear a possibly inherited config path."""
    FakeServer.instances = []
    monkeypatch.delenv(TAKLER_CONNECT_FILE, raising=False)
    monkeypatch.setattr(cli, "TaklerServer", FakeServer)
    return FakeServer.instances


def write_pem(path: Path, name: str) -> Path:
    """Write a placeholder PEM file.

    The content only has to exist: every case below fails either before the
    files are read (half configuration) or while parsing them, so no real
    key pair is needed. Certificate generation belongs to the TLS end-to-end
    test.
    """
    path.write_text(f"-----BEGIN CERTIFICATE-----\n{name}\n-----END CERTIFICATE-----\n")
    return path


def write_connect_config(path: Path, security: dict) -> Path:
    """Write a ``connect.yaml`` holding a ``server`` and a ``security`` section."""
    content = {
        "server": {
            "address": {"hostname": "login_b01", "ip": "127.0.0.1", "port": "35001"}
        },
        "security": security,
    }
    with open(path, "w") as f:
        yaml.safe_dump(content, f)
    return path


# half configured pair -----------------------------------------------


def test_half_configured_pair_exits_one_and_describes_the_failure(
    fake_server, tmp_path
):
    """Only ``--tls-cert``: exit code 1, and the line names both halves."""
    cert = write_pem(tmp_path / "server.crt", "cert")

    result = runner.invoke(cli.app, ["--tls-cert", str(cert)], env=WIDE)

    assert result.exit_code == 1
    stderr = result.stderr
    assert "security configuration error" in stderr
    # The operator has to learn from this single line which half was seen and
    # which one is missing (requirement 1.4).
    assert str(cert) in stderr
    assert "--tls-key" in stderr


def test_a_security_error_does_not_shut_down_the_half_started_server(
    fake_server, tmp_path
):
    """The failure is caught above ``_serve``, so no shutdown flow runs.

    ``start()`` raised before the services were up; running the shutdown flow
    over them would be stopping things that were never started.
    """
    cert = write_pem(tmp_path / "server.crt", "cert")

    result = runner.invoke(cli.app, ["--tls-cert", str(cert)], env=WIDE)

    assert result.exit_code == 1
    server = fake_server[0]
    assert server.started is True
    assert server.ran is False
    assert server.stopped is False


# unreadable files ---------------------------------------------------


def test_missing_certificate_file_exits_one(fake_server, tmp_path):
    key = write_pem(tmp_path / "server.key", "key")
    missing = tmp_path / "absent.crt"

    result = runner.invoke(
        cli.app, ["--tls-cert", str(missing), "--tls-key", str(key)], env=WIDE
    )

    assert result.exit_code == 1
    assert str(missing) in result.stderr


def test_unparseable_pair_exits_one(fake_server, tmp_path):
    """Placeholder PEM content is not a usable pair, so the start-up stops."""
    cert = write_pem(tmp_path / "server.crt", "cert")
    key = write_pem(tmp_path / "server.key", "key")

    result = runner.invoke(
        cli.app, ["--tls-cert", str(cert), "--tls-key", str(key)], env=WIDE
    )

    assert result.exit_code == 1
    assert str(cert) in result.stderr


# precedence over the Connect_Config ---------------------------------


def test_tls_options_are_passed_to_the_server(fake_server, tmp_path):
    cert = write_pem(tmp_path / "server.crt", "cert")
    key = write_pem(tmp_path / "server.key", "key")

    runner.invoke(cli.app, ["--tls-cert", str(cert), "--tls-key", str(key)], env=WIDE)

    kwargs = fake_server[0].kwargs
    assert kwargs["tls_cert_file"] == cert
    assert kwargs["tls_key_file"] == key


def test_command_line_pair_wins_over_the_connect_config(fake_server, tmp_path):
    """``--tls-cert`` / ``--tls-key`` override the ``security`` section (1.10)."""
    config_cert = write_pem(tmp_path / "config.crt", "config cert")
    config_key = write_pem(tmp_path / "config.key", "config key")
    config = write_connect_config(
        tmp_path / "connect.yaml",
        {
            "server_cert_file": str(config_cert),
            "server_key_file": str(config_key),
        },
    )
    cli_cert = tmp_path / "cli.crt"
    cli_key = tmp_path / "cli.key"

    result = runner.invoke(
        cli.app,
        [
            "--config",
            str(config),
            "--tls-cert",
            str(cli_cert),
            "--tls-key",
            str(cli_key),
        ],
        env=WIDE,
    )

    # The command line paths do not exist, so the failure is about them: the
    # config file values were never consulted for either half.
    assert result.exit_code == 1
    assert str(cli_cert) in result.stderr
    assert str(config_cert) not in result.stderr
    assert str(config_key) not in result.stderr


def test_the_connect_config_fills_in_what_the_command_line_omits(fake_server, tmp_path):
    """One half from the command line, the other from the config file."""
    config_key = write_pem(tmp_path / "config.key", "config key")
    config = write_connect_config(
        tmp_path / "connect.yaml", {"server_key_file": str(config_key)}
    )
    cli_cert = tmp_path / "cli.crt"

    result = runner.invoke(
        cli.app, ["--config", str(config), "--tls-cert", str(cli_cert)], env=WIDE
    )

    # Both halves resolved, so this is an unreadable-file failure rather than a
    # half configuration one.
    assert result.exit_code == 1
    assert "half configured" not in result.stderr
    assert str(cli_cert) in result.stderr


# --help -------------------------------------------------------------


def test_help_exits_zero_and_lists_the_tls_options():
    result = runner.invoke(cli.app, ["--help"], env=WIDE)

    assert result.exit_code == 0
    for option in TLS_OPTIONS:
        assert option in result.output
