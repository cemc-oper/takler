"""Unit tests for the ``TaklerServer`` side of the HTTP transport.

``test_http_transport_unit.py`` covers the transport itself; this file pins
the wiring that mounts it: the ``server.http`` section of the Connect_Config
is the only switch, the transport shares the server's Auth_Gate (and through
it the credential store) and audit logger, a missing ``takler[http]`` extra
is a startup error naming the fix, and the listener's TLS pair is resolved
and validated before any listener starts -- half configured, unreadable or
mismatched aborts the start-up rather than degrading to plaintext.
"""

from __future__ import annotations

# ruff: noqa: E402 -- the imports below intentionally follow the importorskip
# guard, so a checkout without the ``http`` extra skips this module instead
# of failing at collection.

import datetime
import sys
from pathlib import Path
from typing import Tuple

import pytest

pytest.importorskip(
    "fastapi", reason="the HTTP transport lives behind the takler[http] extra"
)

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from takler.exceptions import SecurityConfigError, TaklerError
from takler.server import TaklerServer
from takler.server.connect_config import (
    Address,
    ConnectConfig,
    HttpSettings,
    SecuritySettings,
    Server,
)
from takler.server.http_transport import HttpTransport

GRPC_PORT = "33083"
HTTP_PORT = "33084"


def make_config(
    http: HttpSettings | None = None,
    security: SecuritySettings | None = None,
) -> ConnectConfig:
    """A Connect_Config whose ``server.http`` section is ``http``."""
    server = Server(
        address=Address(hostname="login01", ip="10.0.0.11", port=GRPC_PORT),
        http=http,
    )
    kwargs = {"server": server}
    if security is not None:
        kwargs["security"] = security
    return ConnectConfig(**kwargs)


def write_self_signed_pair(directory: Path, name: str) -> Tuple[Path, Path]:
    """Write a fresh self-signed certificate and its private key.

    Returns:
        The ``(certificate path, private key path)`` pair.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )

    cert_path = directory / f"{name}.crt"
    key_path = directory / f"{name}.key"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


# mounting --------------------------------------------------------------------


def test_no_http_section_means_no_http_transport() -> None:
    server = TaklerServer(host="login01", port=GRPC_PORT)

    assert server.http_transport is None


def test_http_section_builds_the_transport_sharing_the_gate() -> None:
    server = TaklerServer(
        host="login01",
        port=GRPC_PORT,
        connect_config=make_config(http=HttpSettings(port=HTTP_PORT)),
    )

    transport = server.http_transport
    assert isinstance(transport, HttpTransport)
    assert transport.host == "0.0.0.0"
    assert transport.port == int(HTTP_PORT)
    # One gate, one store, one audit logger across both transports.
    assert transport.gate is server.auth_gate
    assert server.auth_interceptor.gate is server.auth_gate
    assert server.auth_gate.credential_store is server.credential_store
    assert transport.handlers.audit_logger is server.audit_logger
    # The gRPC side is untouched.
    assert server.grpc_transport is not None


def test_http_section_without_the_extra_is_a_startup_error(
    monkeypatch,
) -> None:
    """A gRPC-only installation (no ``takler[http]``) that enables the HTTP
    listener fails with a message naming the extra, not a raw ImportError."""
    monkeypatch.setitem(sys.modules, "fastapi", None)

    with pytest.raises(TaklerError, match=r"takler\[http\]"):
        TaklerServer(
            host="login01",
            port=GRPC_PORT,
            connect_config=make_config(http=HttpSettings(port=HTTP_PORT)),
        )


# TLS resolution ----------------------------------------------------------------


def test_plaintext_http_when_no_pair_is_configured_anywhere() -> None:
    server = TaklerServer(
        host="login01",
        port=GRPC_PORT,
        connect_config=make_config(http=HttpSettings(port=HTTP_PORT)),
    )

    server._start_security()

    assert server.http_transport.tls_cert_file is None
    assert server.http_transport.tls_key_file is None


def test_http_tls_falls_back_to_the_grpc_pair(tmp_path) -> None:
    """One certificate covers both listeners: with no pair of its own, the
    HTTP listener inherits the pair the gRPC listener resolved."""
    cert_path, key_path = write_self_signed_pair(tmp_path, "server")
    security = SecuritySettings(
        server_cert_file=str(cert_path), server_key_file=str(key_path)
    )
    server = TaklerServer(
        host="login01",
        port=GRPC_PORT,
        connect_config=make_config(
            http=HttpSettings(port=HTTP_PORT), security=security
        ),
    )

    server._start_security()

    assert server.http_transport.tls_cert_file == str(cert_path)
    assert server.http_transport.tls_key_file == str(key_path)


def test_http_tls_pair_wins_over_the_grpc_pair(tmp_path) -> None:
    grpc_cert, grpc_key = write_self_signed_pair(tmp_path, "grpc")
    http_cert, http_key = write_self_signed_pair(tmp_path, "http")
    security = SecuritySettings(
        server_cert_file=str(grpc_cert), server_key_file=str(grpc_key)
    )
    server = TaklerServer(
        host="login01",
        port=GRPC_PORT,
        connect_config=make_config(
            http=HttpSettings(
                port=HTTP_PORT,
                tls_cert_file=str(http_cert),
                tls_key_file=str(http_key),
            ),
            security=security,
        ),
    )

    server._start_security()

    assert server.http_transport.tls_cert_file == str(http_cert)
    assert server.http_transport.tls_key_file == str(http_key)
    # The gRPC listener keeps its own pair.
    assert server.grpc_transport.tls_cert_file == str(grpc_cert)


def test_a_half_http_tls_pair_aborts_the_startup(tmp_path) -> None:
    cert_path, _ = write_self_signed_pair(tmp_path, "server")
    server = TaklerServer(
        host="login01",
        port=GRPC_PORT,
        connect_config=make_config(
            http=HttpSettings(port=HTTP_PORT, tls_cert_file=str(cert_path))
        ),
    )

    with pytest.raises(SecurityConfigError, match="tls_key_file"):
        server._start_security()


def test_an_unreadable_http_tls_pair_aborts_the_startup(tmp_path) -> None:
    server = TaklerServer(
        host="login01",
        port=GRPC_PORT,
        connect_config=make_config(
            http=HttpSettings(
                port=HTTP_PORT,
                tls_cert_file=str(tmp_path / "no-such.crt"),
                tls_key_file=str(tmp_path / "no-such.key"),
            )
        ),
    )

    with pytest.raises(SecurityConfigError, match="no-such.crt"):
        server._start_security()


def test_a_mismatched_http_tls_pair_aborts_the_startup(tmp_path) -> None:
    cert_path, _ = write_self_signed_pair(tmp_path, "one")
    _, key_path = write_self_signed_pair(tmp_path, "two")
    server = TaklerServer(
        host="login01",
        port=GRPC_PORT,
        connect_config=make_config(
            http=HttpSettings(
                port=HTTP_PORT,
                tls_cert_file=str(cert_path),
                tls_key_file=str(key_path),
            )
        ),
    )

    with pytest.raises(SecurityConfigError):
        server._start_security()


def test_http_section_round_trips_through_the_yaml(tmp_path) -> None:
    """A saved connect.yaml keeps the ``http`` section loadable."""
    from takler.server.connect_config import load_connect_config, save_connect_config

    file_path = tmp_path / "connect.yaml"
    save_connect_config(
        make_config(http=HttpSettings(host="::", port=HTTP_PORT)), file_path
    )

    loaded = load_connect_config(file_path)

    assert loaded.server.http is not None
    assert loaded.server.http.host == "::"
    assert loaded.server.http.port == HTTP_PORT
    assert loaded.server.http.tls_cert_file is None
