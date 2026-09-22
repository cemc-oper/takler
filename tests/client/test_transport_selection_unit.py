"""Unit tests for the client transport selection (M3 task 8).

What is pinned here:

* :func:`~takler.client.transport.resolve_transport` -- the precedence chain
  explicit argument > Connect_Config ``server.transport`` > ``TAKLER_TRANSPORT``
  > gRPC, the case/whitespace tolerance, and the degrade-with-WARNING shape of
  an unrecognized name;
* :func:`~takler.client.transport.build_client_transport` -- the factory
  builds the implementation the name selects, the HTTP import is lazy, and a
  missing ``takler[http]`` extra is a clear error naming the fix;
* ``TaklerServiceClient`` -- the selection is wired through the constructor,
  and an injected transport still wins over every name source;
* :func:`~takler.client.cli.get_host_and_prot` -- with ``http`` selected, the
  Connect_Config port level answers the HTTP listener's port.
"""

from __future__ import annotations

import contextlib
import io
import sys
from pathlib import Path
from typing import Optional

import pytest

import takler.logging
from takler.client.cli import get_host_and_prot
from takler.client.grpc_transport import GrpcTransport
from takler.client.service_client import TaklerServiceClient
from takler.client.transport import (
    ENV_TRANSPORT,
    build_client_transport,
    resolve_transport,
)
from takler.exceptions import TaklerError
from takler.server.connect_config import (
    Address,
    ConnectConfig,
    HttpSettings,
    Server,
)

GRPC_PORT = "33083"
HTTP_PORT = "33084"


def make_config(
    transport: Optional[str] = None, with_http: bool = True
) -> ConnectConfig:
    """A Connect_Config with both listeners and an optional selection."""
    return ConnectConfig(
        server=Server(
            address=Address(hostname="login01", ip="10.0.0.11", port=GRPC_PORT),
            http=(HttpSettings(host="0.0.0.0", port=HTTP_PORT) if with_http else None),
            transport=transport,
        )
    )


@pytest.fixture
def captured_console_log():
    """Capture takler's console log output (the logging backend does not
    route records into pytest's ``caplog``)."""
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="WARNING", console=True)
            yield buffer
    finally:
        takler.logging.configure(console=True)


def warning_lines(captured: io.StringIO) -> list:
    return [line for line in captured.getvalue().splitlines() if "WARNING" in line]


def clean_env(monkeypatch) -> None:
    """Drop every environment variable the selection reads."""
    monkeypatch.delenv(ENV_TRANSPORT, raising=False)


# resolve_transport -----------------------------------------------------------


def test_default_is_grpc(monkeypatch) -> None:
    clean_env(monkeypatch)

    assert resolve_transport(None, None) == "grpc"


def test_explicit_argument_wins_over_config_and_env(monkeypatch) -> None:
    monkeypatch.setenv(ENV_TRANSPORT, "grpc")

    assert resolve_transport("http", make_config(transport="grpc")) == "http"


def test_config_field_wins_over_env(monkeypatch) -> None:
    monkeypatch.setenv(ENV_TRANSPORT, "grpc")

    assert resolve_transport(None, make_config(transport="http")) == "http"


def test_env_applies_without_explicit_or_config(monkeypatch) -> None:
    monkeypatch.setenv(ENV_TRANSPORT, "http")

    assert resolve_transport(None, None) == "http"
    assert resolve_transport(None, make_config(transport=None)) == "http"


def test_names_are_case_and_whitespace_tolerant(monkeypatch) -> None:
    clean_env(monkeypatch)

    assert resolve_transport("  HTTP ", None) == "http"


def test_blank_values_fall_through(monkeypatch) -> None:
    monkeypatch.setenv(ENV_TRANSPORT, "http")

    assert resolve_transport("  ", make_config(transport=""), None) == "http"


def test_an_unrecognized_name_degrades_with_a_warning(
    monkeypatch, captured_console_log
) -> None:
    clean_env(monkeypatch)

    assert resolve_transport("carrier-pigeon", None) == "grpc"
    warnings = warning_lines(captured_console_log)
    assert len(warnings) == 1
    assert "carrier-pigeon" in warnings[0]


def test_an_unrecognized_env_name_falls_through_to_the_default(
    monkeypatch, captured_console_log
) -> None:
    monkeypatch.setenv(ENV_TRANSPORT, "smoke-signal")

    assert resolve_transport(None, None) == "grpc"
    warnings = warning_lines(captured_console_log)
    assert len(warnings) == 1
    assert "smoke-signal" in warnings[0]


def test_an_unrecognized_config_name_falls_through_to_the_env(
    monkeypatch, captured_console_log
) -> None:
    monkeypatch.setenv(ENV_TRANSPORT, "http")

    assert resolve_transport(None, make_config(transport="drum")) == "http"
    assert len(warning_lines(captured_console_log)) == 1


# build_client_transport -------------------------------------------------------


def _factory_kwargs() -> dict:
    return dict(
        host="login01",
        port=GRPC_PORT,
        single_timeout=10.0,
        retry_window=None,
        clock=None,
        sleep=None,
        ca_file=None,
        server_name=None,
        secret_file=None,
    )


def test_factory_builds_the_grpc_transport() -> None:
    transport = build_client_transport("grpc", **_factory_kwargs())

    assert isinstance(transport, GrpcTransport)


def test_factory_builds_the_http_transport() -> None:
    pytest.importorskip("httpx")
    from takler.client.http_transport import HttpTransport

    transport = build_client_transport("http", **_factory_kwargs())

    assert isinstance(transport, HttpTransport)


def test_factory_rejects_an_unknown_name() -> None:
    with pytest.raises(ValueError, match="unknown transport name"):
        build_client_transport("drum", **_factory_kwargs())


def test_factory_names_the_missing_http_extra(monkeypatch) -> None:
    """Selecting ``http`` without the extra is a clear error, not an
    ``ImportError``: the message names ``takler[http]``."""
    monkeypatch.delitem(sys.modules, "takler.client.http_transport", raising=False)
    monkeypatch.setitem(sys.modules, "httpx", None)

    with pytest.raises(TaklerError, match=r"takler\[http\]"):
        build_client_transport("http", **_factory_kwargs())


# TaklerServiceClient wiring ----------------------------------------------------


def test_client_defaults_to_the_grpc_transport(monkeypatch) -> None:
    clean_env(monkeypatch)

    client = TaklerServiceClient(host="login01", port=GRPC_PORT)

    assert isinstance(client.transport, GrpcTransport)


def test_client_builds_the_transport_named_explicitly(monkeypatch) -> None:
    pytest.importorskip("httpx")
    from takler.client.http_transport import HttpTransport

    clean_env(monkeypatch)

    client = TaklerServiceClient(host="login01", port=HTTP_PORT, transport_name="http")

    assert isinstance(client.transport, HttpTransport)


def test_client_reads_the_selection_from_the_connect_config(monkeypatch) -> None:
    pytest.importorskip("httpx")
    from takler.client.http_transport import HttpTransport

    clean_env(monkeypatch)

    client = TaklerServiceClient(
        host="login01",
        port=HTTP_PORT,
        connect_config=make_config(transport="http"),
    )

    assert isinstance(client.transport, HttpTransport)


def test_client_reads_the_selection_from_the_environment(monkeypatch) -> None:
    pytest.importorskip("httpx")
    from takler.client.http_transport import HttpTransport

    monkeypatch.setenv(ENV_TRANSPORT, "http")

    client = TaklerServiceClient(host="login01", port=HTTP_PORT)

    assert isinstance(client.transport, HttpTransport)


def test_an_injected_transport_wins_over_every_name_source(monkeypatch) -> None:
    monkeypatch.setenv(ENV_TRANSPORT, "http")
    injected = GrpcTransport(host="login01", port=GRPC_PORT)

    client = TaklerServiceClient(
        host="login01",
        port=HTTP_PORT,
        connect_config=make_config(transport="http"),
        transport_name="http",
        transport=injected,
    )

    assert client.transport is injected


def test_client_forwards_the_resolved_tls_and_secret_knobs(
    monkeypatch, tmp_path: Path
) -> None:
    """The HTTP transport receives the same resolved knobs the gRPC one
    would: they are resolved once, ahead of the factory."""
    pytest.importorskip("httpx")

    clean_env(monkeypatch)
    monkeypatch.delenv("TAKLER_TLS_CA_FILE", raising=False)
    monkeypatch.delenv("TAKLER_TLS_SERVER_NAME", raising=False)
    monkeypatch.delenv("TAKLER_SECRET_FILE", raising=False)
    secret_file = tmp_path / "operator.secret"
    secret_file.write_text("unused-in-this-test\n")

    client = TaklerServiceClient(
        host="login01",
        port=HTTP_PORT,
        transport_name="http",
        ca_file=" ca.pem ",
        server_name=" login01.short ",
        secret_file=str(secret_file),
    )

    assert client.transport.ca_file == "ca.pem"
    assert client.transport.server_name == "login01.short"
    assert client.transport.secret_file == str(secret_file)


# get_host_and_prot: the HTTP listener's port ------------------------------------


def test_http_selection_dials_the_http_port_of_the_config(monkeypatch) -> None:
    monkeypatch.delenv("TAKLER_HOST", raising=False)
    monkeypatch.delenv("TAKLER_PORT", raising=False)

    host, port = get_host_and_prot(None, None, make_config(), transport_name="http")

    assert host == "login01"
    assert port == HTTP_PORT


def test_http_selection_without_an_http_section_dials_the_grpc_port(
    monkeypatch,
) -> None:
    monkeypatch.delenv("TAKLER_HOST", raising=False)
    monkeypatch.delenv("TAKLER_PORT", raising=False)

    host, port = get_host_and_prot(
        None, None, make_config(with_http=False), transport_name="http"
    )

    assert port == GRPC_PORT


def test_grpc_selection_dials_the_grpc_port(monkeypatch) -> None:
    monkeypatch.delenv("TAKLER_HOST", raising=False)
    monkeypatch.delenv("TAKLER_PORT", raising=False)

    _, port = get_host_and_prot(None, None, make_config(), transport_name="grpc")

    assert port == GRPC_PORT


def test_an_explicit_port_wins_over_the_http_selection(monkeypatch) -> None:
    monkeypatch.delenv("TAKLER_HOST", raising=False)
    monkeypatch.delenv("TAKLER_PORT", raising=False)

    _, port = get_host_and_prot(None, "9999", make_config(), transport_name="http")

    assert port == "9999"
