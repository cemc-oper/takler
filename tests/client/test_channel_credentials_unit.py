"""Unit tests for the client side TLS channel added by Task 3.4 of *m2-security*.

Two units are covered:

* :func:`takler.client.service_client.build_channel_credentials` -- a configured
  CA certificate yields channel credentials (Requirement 2.1), no CA yields
  ``None`` so the caller stays plaintext (Requirement 2.2), and a missing, empty
  or unparseable file raises an :class:`~takler.exceptions.InvalidRequestError`
  naming the path and the reason (Requirement 2.6);
* :meth:`takler.client.service_client.TaklerServiceClient.create_channel` -- it
  picks ``insecure_channel`` or ``secure_channel`` from that return value and
  adds the ``grpc.ssl_target_name_override`` option exactly when a server name
  override is configured (Requirement 2.4).

The CA material is a real self-signed certificate generated into a temporary
directory, so the parsing checks run against PEM material OpenSSL accepts rather
than against a fixture blob.

Validates: Requirements 2.1, 2.2, 2.4, 2.6
"""

from __future__ import annotations

import datetime
from pathlib import Path

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from takler.client.service_client import (
    SSL_TARGET_NAME_OVERRIDE_OPTION,
    TaklerServiceClient,
    build_channel_credentials,
)
from takler.exceptions import InvalidRequestError, TaklerError


# ---------------------------------------------------------------------------
# Helpers and fixtures
# ---------------------------------------------------------------------------


def _write_ca_certificate(path: Path) -> Path:
    """Write a fresh self-signed certificate usable as a root of trust."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "takler-test-ca")])
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    return path


@pytest.fixture(scope="module")
def ca_file(tmp_path_factory) -> Path:
    """A usable CA certificate, generated once for the whole module."""
    directory = tmp_path_factory.mktemp("client_tls")
    return _write_ca_certificate(directory / "ca.crt")


@pytest.fixture(autouse=True)
def no_tls_environment(monkeypatch):
    """Keep an ambient TLS configuration out of the constructor's resolution."""
    monkeypatch.delenv("TAKLER_TLS_CA_FILE", raising=False)
    monkeypatch.delenv("TAKLER_TLS_SERVER_NAME", raising=False)


class FakeChannel:
    """A channel stand-in recording how it was built."""

    def __init__(self, address, credentials=None, options=None):
        self.address = address
        self.credentials = credentials
        self.options = options

    def close(self):  # pragma: no cover - not exercised here
        pass


# ---------------------------------------------------------------------------
# build_channel_credentials
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("configured", [None, "", "   "])
def test_no_ca_file_means_no_credentials(configured):
    """Requirement 2.2: absent and blank both mean "connect unencrypted"."""
    assert build_channel_credentials(configured) is None


def test_ca_file_yields_channel_credentials(ca_file):
    """Requirement 2.1: a configured CA becomes the channel's root of trust."""
    credentials = build_channel_credentials(str(ca_file))

    assert isinstance(credentials, grpc.ChannelCredentials)


def test_ca_file_is_stripped(ca_file):
    """Surrounding whitespace in the configured path is not part of it."""
    assert build_channel_credentials(f"  {ca_file}  ") is not None


def test_missing_ca_file_reports_path_and_reason(tmp_path):
    """Requirement 2.6: a non existent file names the path and the reason."""
    missing = tmp_path / "absent.crt"

    with pytest.raises(InvalidRequestError) as excinfo:
        build_channel_credentials(str(missing))

    message = str(excinfo.value)
    assert str(missing) in message
    assert "FileNotFoundError" in message


def test_unreadable_ca_file_reports_path_and_reason(tmp_path, ca_file):
    """Requirement 2.6: an unreadable file names the path and the reason."""
    unreadable = tmp_path / "unreadable.crt"
    unreadable.write_bytes(ca_file.read_bytes())
    unreadable.chmod(0o000)

    try:
        with pytest.raises(InvalidRequestError) as excinfo:
            build_channel_credentials(str(unreadable))
    finally:
        unreadable.chmod(0o600)

    message = str(excinfo.value)
    assert str(unreadable) in message
    assert "PermissionError" in message


def test_empty_ca_file_reports_path(tmp_path):
    """An empty file is a configuration mistake, not "no CA configured"."""
    empty = tmp_path / "empty.crt"
    empty.write_bytes(b"   \n")

    with pytest.raises(InvalidRequestError) as excinfo:
        build_channel_credentials(str(empty))

    assert str(empty) in str(excinfo.value)


def test_unparseable_ca_file_reports_path_and_reason(tmp_path):
    """Requirement 2.6: content that is not PEM fails before the handshake.

    This is the case gRPC itself would accept silently, so the whole point is
    that it is rejected here, with the path in the message, instead of costing
    the command its Retry_Window later.
    """
    garbage = tmp_path / "garbage.crt"
    garbage.write_text("this is not a certificate\n")

    with pytest.raises(InvalidRequestError) as excinfo:
        build_channel_credentials(str(garbage))

    message = str(excinfo.value)
    assert str(garbage) in message
    assert "SSLError" in message or "Error" in message


def test_failure_type_maps_to_the_request_error_exit_code(tmp_path):
    """The raised type is a ``TaklerError`` whose exit code is 1."""
    from takler.client.exit_code import EXIT_REQUEST_ERROR, exit_code_for_exception

    missing = tmp_path / "absent.crt"
    with pytest.raises(TaklerError) as excinfo:
        build_channel_credentials(str(missing))

    assert exit_code_for_exception(excinfo.value) == EXIT_REQUEST_ERROR


# ---------------------------------------------------------------------------
# create_channel
# ---------------------------------------------------------------------------


def test_create_channel_stays_insecure_without_ca(monkeypatch):
    """Requirement 2.2: unchanged M1 behaviour when no CA is configured."""
    built = {}

    def fake_insecure_channel(address):
        built["address"] = address
        return FakeChannel(address)

    monkeypatch.setattr("grpc.insecure_channel", fake_insecure_channel)
    monkeypatch.setattr(
        "grpc.secure_channel",
        lambda *args, **kwargs: pytest.fail("secure_channel must not be used"),
    )

    client = TaklerServiceClient(host="h", port=1)
    client.create_channel()

    assert built["address"] == "h:1"
    assert isinstance(client.channel, FakeChannel)


def test_create_channel_uses_tls_with_ca(monkeypatch, ca_file):
    """Requirement 2.1: a configured CA turns the channel into a TLS one."""
    monkeypatch.setattr(
        "grpc.insecure_channel",
        lambda *args, **kwargs: pytest.fail("insecure_channel must not be used"),
    )
    monkeypatch.setattr("grpc.secure_channel", FakeChannel)

    client = TaklerServiceClient(host="h", port=1, ca_file=str(ca_file))
    client.create_channel()

    assert client.channel.address == "h:1"
    assert isinstance(client.channel.credentials, grpc.ChannelCredentials)
    # No override configured, so no option is added.
    assert client.channel.options == []


def test_create_channel_adds_server_name_override(monkeypatch, ca_file):
    """Requirement 2.4: the override becomes the host name verified against."""
    monkeypatch.setattr("grpc.secure_channel", FakeChannel)

    client = TaklerServiceClient(
        host="login-a06.hpc.example",
        port=33083,
        ca_file=str(ca_file),
        server_name="login_a06",
    )
    client.create_channel()

    assert client.channel.options == [(SSL_TARGET_NAME_OVERRIDE_OPTION, "login_a06")]


def test_create_channel_ignores_blank_server_name(monkeypatch, ca_file):
    """A blank override is "not configured", not an empty target name."""
    monkeypatch.setattr("grpc.secure_channel", FakeChannel)

    client = TaklerServiceClient(host="h", port=1, ca_file=str(ca_file))
    client.server_name = "   "
    client.create_channel()

    assert client.channel.options == []


def test_constructor_resolves_ca_file_from_environment(monkeypatch, ca_file):
    """Requirements 2.3, 2.5: the environment is the second precedence level."""
    monkeypatch.setenv("TAKLER_TLS_CA_FILE", str(ca_file))
    monkeypatch.setenv("TAKLER_TLS_SERVER_NAME", "login_a06")

    client = TaklerServiceClient(host="h", port=1)

    assert client.ca_file == str(ca_file)
    assert client.server_name == "login_a06"


def test_create_channel_raises_before_any_rpc(tmp_path):
    """Requirement 2.6: an unusable CA fails at channel creation time."""
    client = TaklerServiceClient(host="h", port=1, ca_file=str(tmp_path / "absent.crt"))

    with pytest.raises(InvalidRequestError):
        client.create_channel()

    assert client.channel is None
