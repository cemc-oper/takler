"""Unit tests for :func:`takler.server.tls.build_server_credentials`.

These example-based tests pin down the four-row decision table of the server
TLS credential construction added by Task 3.1 of the *m2-security* spec:

* neither the certificate nor the private key configured -> ``None``, so the
  caller binds a plaintext port (Requirement 1.1, negative case);
* both configured -> a :class:`grpc.ServerCredentials` built from the pair, with
  the key first inside the pair (Requirement 1.1);
* exactly one configured -> :class:`~takler.exceptions.SecurityConfigError`
  whose message names the configured setting and the missing one
  (Requirement 1.4);
* a file that is missing, unreadable or unparseable -> ``SecurityConfigError``
  whose message carries the path and the reason (Requirement 1.5).

A configured ``client_ca_file`` only earns a WARNING and is never handed to
gRPC, since this version does not verify client certificates
(Requirements 1.8, 1.9).

The certificate/key pair is a self-signed one generated into a temporary
directory, so the parsing checks run against real PEM material rather than
against fixture blobs committed to the repository.

Validates: Requirements 1.1, 1.4, 1.5, 1.8, 1.9
"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Tuple
from unittest import mock

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

import takler.server.tls as tls_module
from takler.exceptions import SecurityConfigError
from takler.server.connect_config import SecuritySettings
from takler.server.tls import (
    CERT_SETTING_NAME,
    KEY_SETTING_NAME,
    build_server_credentials,
)


# ---------------------------------------------------------------------------
# Helpers and fixtures
# ---------------------------------------------------------------------------


def _write_self_signed_pair(directory: Path, name: str) -> Tuple[Path, Path]:
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


@pytest.fixture(scope="module")
def tls_pair(tmp_path_factory) -> Tuple[Path, Path]:
    """A usable certificate/key pair, generated once for the whole module."""
    directory = tmp_path_factory.mktemp("tls")
    return _write_self_signed_pair(directory, "server")


def _capture_warnings(call):
    """Run ``call`` while spying on the ``server.tls`` logger.

    Returns ``(result, warning_messages)``. Spying on the module logger keeps
    the assertion independent of the active logging backend and of pytest's
    stream capture, as in ``test_exception_policy_config.py``.
    """
    with mock.patch.object(tls_module.logger, "warning") as warn:
        result = call()
    return result, [c.args[0] if c.args else "" for c in warn.call_args_list]


# ---------------------------------------------------------------------------
# Row 1: neither half configured
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "settings, kwargs",
    [
        (SecuritySettings(), {}),
        (None, {}),
        # Blank and whitespace-only paths count as "not configured" at both
        # levels, so neither shadows the other.
        (SecuritySettings(server_cert_file="", server_key_file="   "), {}),
        (SecuritySettings(), {"cert_file": "  ", "key_file": ""}),
    ],
    ids=["all-none", "no-settings", "blank-settings", "blank-arguments"],
)
def test_no_tls_configured_returns_none(settings, kwargs):
    assert build_server_credentials(settings, **kwargs) is None


# ---------------------------------------------------------------------------
# Row 2: both halves configured
# ---------------------------------------------------------------------------


def test_both_configured_builds_credentials(tls_pair):
    cert_path, key_path = tls_pair
    settings = SecuritySettings(
        server_cert_file=str(cert_path), server_key_file=str(key_path)
    )

    credentials = build_server_credentials(settings)

    assert isinstance(credentials, grpc.ServerCredentials)


def test_pair_is_key_then_certificate(tls_pair):
    """``ssl_server_credentials`` takes ``(private_key, certificate_chain)``.

    Swapping the two builds a credentials object without complaint and only
    fails per connection at handshake time, so the order is asserted here
    rather than left to the end-to-end test.
    """
    cert_path, key_path = tls_pair
    settings = SecuritySettings(
        server_cert_file=str(cert_path), server_key_file=str(key_path)
    )

    with mock.patch.object(
        tls_module.grpc, "ssl_server_credentials", autospec=True
    ) as factory:
        build_server_credentials(settings)

    (pair_list,), kwargs = factory.call_args
    assert pair_list == [(key_path.read_bytes(), cert_path.read_bytes())]
    # No mTLS parameters: this version does not verify client certificates
    # (Requirement 1.8).
    assert kwargs == {}


def test_explicit_arguments_win_over_settings(tls_pair, tmp_path):
    """The Server_CLI options are the highest precedence source (1.10)."""
    cert_path, key_path = tls_pair
    settings = SecuritySettings(
        server_cert_file=str(tmp_path / "not-there.crt"),
        server_key_file=str(tmp_path / "not-there.key"),
    )

    credentials = build_server_credentials(
        settings, cert_file=cert_path, key_file=str(key_path)
    )

    assert isinstance(credentials, grpc.ServerCredentials)


# ---------------------------------------------------------------------------
# Rows 3 and 4: exactly one half configured
# ---------------------------------------------------------------------------


def test_certificate_without_key_is_rejected(tls_pair):
    cert_path, _ = tls_pair

    with pytest.raises(SecurityConfigError) as excinfo:
        build_server_credentials(SecuritySettings(server_cert_file=str(cert_path)))

    message = str(excinfo.value)
    assert CERT_SETTING_NAME in message
    assert KEY_SETTING_NAME in message
    assert str(cert_path) in message


def test_key_without_certificate_is_rejected(tls_pair):
    _, key_path = tls_pair

    with pytest.raises(SecurityConfigError) as excinfo:
        build_server_credentials(None, key_file=key_path)

    message = str(excinfo.value)
    assert CERT_SETTING_NAME in message
    assert KEY_SETTING_NAME in message
    assert str(key_path) in message


# ---------------------------------------------------------------------------
# Row 5: a file that cannot be read or parsed
# ---------------------------------------------------------------------------


def test_missing_file_reports_path_and_reason(tls_pair, tmp_path):
    _, key_path = tls_pair
    missing = tmp_path / "absent.crt"

    with pytest.raises(SecurityConfigError) as excinfo:
        build_server_credentials(
            SecuritySettings(
                server_cert_file=str(missing), server_key_file=str(key_path)
            )
        )

    message = str(excinfo.value)
    assert str(missing) in message
    assert "FileNotFoundError" in message


def test_unreadable_file_reports_path(tls_pair, tmp_path):
    cert_path, key_path = tls_pair
    unreadable = tmp_path / "unreadable.key"
    unreadable.write_bytes(key_path.read_bytes())
    unreadable.chmod(0o000)

    try:
        with pytest.raises(SecurityConfigError) as excinfo:
            build_server_credentials(
                SecuritySettings(
                    server_cert_file=str(cert_path),
                    server_key_file=str(unreadable),
                )
            )
    finally:
        unreadable.chmod(0o600)

    message = str(excinfo.value)
    assert str(unreadable) in message
    assert "PermissionError" in message


def test_unparseable_file_reports_path(tls_pair, tmp_path):
    _, key_path = tls_pair
    garbage = tmp_path / "garbage.crt"
    garbage.write_text("this is not a certificate\n")

    with pytest.raises(SecurityConfigError) as excinfo:
        build_server_credentials(
            SecuritySettings(
                server_cert_file=str(garbage), server_key_file=str(key_path)
            )
        )

    assert str(garbage) in str(excinfo.value)


def test_empty_file_is_rejected(tls_pair, tmp_path):
    cert_path, _ = tls_pair
    empty = tmp_path / "empty.key"
    empty.write_text("   \n")

    with pytest.raises(SecurityConfigError) as excinfo:
        build_server_credentials(
            SecuritySettings(
                server_cert_file=str(cert_path), server_key_file=str(empty)
            )
        )

    assert str(empty) in str(excinfo.value)


def test_mismatched_pair_is_rejected_at_startup(tls_pair, tmp_path):
    """A key that does not belong to the certificate stops the start-up.

    Its only other symptom would be every single client failing to connect,
    with nothing in the server log naming the cause.
    """
    cert_path, _ = tls_pair
    _, other_key = _write_self_signed_pair(tmp_path, "other")

    with pytest.raises(SecurityConfigError) as excinfo:
        build_server_credentials(
            SecuritySettings(
                server_cert_file=str(cert_path), server_key_file=str(other_key)
            )
        )

    message = str(excinfo.value)
    assert str(cert_path) in message
    assert str(other_key) in message


# ---------------------------------------------------------------------------
# client_ca_file: warn, do not verify (Requirements 1.8, 1.9)
# ---------------------------------------------------------------------------


def test_client_ca_file_warns_and_continues(tls_pair, tmp_path):
    cert_path, key_path = tls_pair
    client_ca = tmp_path / "client-ca.crt"
    client_ca.write_bytes(cert_path.read_bytes())
    settings = SecuritySettings(
        server_cert_file=str(cert_path),
        server_key_file=str(key_path),
        client_ca_file=str(client_ca),
    )

    credentials, warnings = _capture_warnings(
        lambda: build_server_credentials(settings)
    )

    assert isinstance(credentials, grpc.ServerCredentials)
    assert any(
        "client_ca_file" in message and str(client_ca) in message
        for message in warnings
    ), warnings


def test_client_ca_file_not_configured_does_not_warn(tls_pair):
    cert_path, key_path = tls_pair
    settings = SecuritySettings(
        server_cert_file=str(cert_path), server_key_file=str(key_path)
    )

    _, warnings = _capture_warnings(lambda: build_server_credentials(settings))

    assert warnings == []
