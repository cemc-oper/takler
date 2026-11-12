"""Unit tests for the TUI entry point's connection resolution.

``main`` itself launches the Textual loop and is not unit-tested; the
pure ``_resolve`` / ``_load_config`` helpers are.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from takler.server.connect_config import TAKLER_CONNECT_FILE, save_connect_config
from takler.tui.__main__ import _load_config, _resolve


@pytest.fixture
def clean_env(monkeypatch):
    for name in (
        "TAKLER_HOST",
        "TAKLER_PORT",
        "TAKLER_TRANSPORT",
        TAKLER_CONNECT_FILE,
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_explicit_host_and_port_win(clean_env) -> None:
    host, port, transport, config = _resolve(None, "myhost", "1234")
    assert host == "myhost"
    assert port == "1234"
    assert transport == "grpc"
    assert config is None


def test_connect_file_supplies_defaults(clean_env, tmp_path: Path) -> None:
    from takler.server.connect_config import (
        Address,
        ConnectConfig,
        Server,
    )

    connect_file = tmp_path / "connect.yaml"
    save_connect_config(
        ConnectConfig(
            server=Server(
                address=Address(hostname="cfg-host", ip="127.0.0.1", port="9999")
            )
        ),
        connect_file,
    )
    host, port, transport, config = _resolve(str(connect_file), None, None)
    assert host == "cfg-host"
    assert port == "9999"
    assert transport == "grpc"
    assert config is not None


def test_connect_file_from_the_environment(clean_env, tmp_path: Path) -> None:
    from takler.server.connect_config import (
        Address,
        ConnectConfig,
        Server,
    )

    connect_file = tmp_path / "connect.yaml"
    save_connect_config(
        ConnectConfig(
            server=Server(
                address=Address(hostname="env-host", ip="127.0.0.1", port="8888")
            )
        ),
        connect_file,
    )
    clean_env.setenv(TAKLER_CONNECT_FILE, str(connect_file))
    host, port, _, config = _resolve(None, None, None)
    assert (host, port) == ("env-host", "8888")
    assert config is not None


def test_load_config_without_any_source_returns_none(clean_env) -> None:
    assert _load_config(None) is None


def test_transport_env_var_is_resolved(clean_env) -> None:
    clean_env.setenv("TAKLER_TRANSPORT", "http")
    _, _, transport, _ = _resolve(None, "myhost", "1234")
    assert transport == "http"
