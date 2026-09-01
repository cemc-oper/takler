"""Tests for the secure / insecure port choice and the security start-up log.

Three things are pinned down here, and each of them is a decision that has no
other visible symptom:

* ``TaklerService.start()`` binds its listen address with the server credentials
  when TLS is configured and in plaintext when it is not (Requirements 1.1,
  1.2). A mistake in this branch does not fail: it serves the wrong thing
  successfully, which is exactly why it is asserted on the calls themselves
  rather than through a client.
* the start-up says which of the two happened -- an INFO naming the address and
  the certificate file, or a WARNING naming the address and the fact that the
  transport is not encrypted (Requirements 1.3, 1.7).
* ``TaklerServer`` builds the Auth_Interceptor before the service exists and
  hands it over at construction time, because ``grpc.aio`` accepts interceptors
  nowhere else, and reports the resulting Auth_Mode -- INFO with the
  Zombie_Policy and the whitelist path when enabled, WARNING spelling out the
  consequence when disabled (Requirements 3.11, 3.12).

The gRPC server object is faked throughout: what is being tested is which of
``add_secure_port`` / ``add_insecure_port`` gets called with what, and binding a
real port would neither make that observable nor keep the test hermetic.

Requirements: 1.1, 1.2, 1.3, 1.7, 3.11, 3.12.
"""

from __future__ import annotations

import asyncio
import datetime
from pathlib import Path
from typing import List, Tuple
from unittest import mock

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

import takler.server as server_module
import takler.server.network_service as network_service_module
from takler.server import TaklerServer
from takler.server.auth import AuthInterceptor
from takler.server.connect_config import (
    AuthMode,
    ZombiePolicy,
    generate_connect_config,
)
from takler.server.network_service import TaklerService
from takler.server.scheduler import Scheduler
from takler.core import Bunch


# ---------------------------------------------------------------------------
# Helpers and fixtures
# ---------------------------------------------------------------------------


def _write_self_signed_pair(directory: Path) -> Tuple[Path, Path]:
    """Write a fresh self-signed certificate and its private key."""
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

    cert_path = directory / "server.crt"
    key_path = directory / "server.key"
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
    return _write_self_signed_pair(tmp_path_factory.mktemp("tls"))


@pytest.fixture
def secret_file(tmp_path) -> Path:
    """A usable Operator_Secret_File with owner-only permissions.

    Owner-only on purpose: a wider mode would add a permission WARNING of its
    own to the captured log output (Requirement 7.11) and blur the assertions
    about the Auth_Mode records.
    """
    path = tmp_path / "operator.secret"
    path.write_text("s3cret\n")
    path.chmod(0o600)
    return path


class FakeGrpcServer:
    """Records what a ``grpc.aio.Server`` would have been asked to do."""

    def __init__(self, interceptors) -> None:
        self.interceptors = interceptors
        self.insecure_addresses: List[str] = []
        self.secure_bindings: List[Tuple[str, object]] = []
        self.started = False
        self.handlers: List[object] = []

    def add_generic_rpc_handlers(self, handlers) -> None:
        self.handlers.extend(handlers)

    def add_registered_method_handlers(self, service_name, method_handlers) -> None:
        # Newer grpc generated stubs call this in addition to
        # ``add_generic_rpc_handlers``; the fake only has to tolerate it.
        self.handlers.append((service_name, method_handlers))

    def add_insecure_port(self, address: str) -> int:
        self.insecure_addresses.append(address)
        return 0

    def add_secure_port(self, address: str, credentials) -> int:
        self.secure_bindings.append((address, credentials))
        return 0

    async def start(self) -> None:
        self.started = True


def _start_service(service: TaklerService) -> Tuple[FakeGrpcServer, List[str]]:
    """Run ``service.start()`` against a fake gRPC server.

    Returns:
        The fake server and every message logged by the service module during
        the start-up, each one prefixed with its level so a test can tell an
        INFO from a WARNING.
    """
    created: List[FakeGrpcServer] = []

    def fake_server(*args, **kwargs):
        created.append(FakeGrpcServer(kwargs.get("interceptors")))
        return created[-1]

    records: List[str] = []

    def record(level):
        return lambda message, *a, **k: records.append(f"{level}:{message}")

    with mock.patch.object(network_service_module.grpc.aio, "server", fake_server):
        with (
            mock.patch.object(network_service_module.logger, "info", record("INFO")),
            mock.patch.object(
                network_service_module.logger, "warning", record("WARNING")
            ),
        ):
            asyncio.run(service.start())

    return created[0], records


def _build_service(**kwargs) -> TaklerService:
    """A service wired to an empty scheduler, which start-up never touches."""
    return TaklerService(
        scheduler=Scheduler(bunch=Bunch(host="login01", port="33083")),
        host="[::]",
        port=33083,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Port binding (Requirements 1.1, 1.2, 1.3, 1.7)
# ---------------------------------------------------------------------------


def test_no_credentials_binds_an_insecure_port_and_warns():
    service = _build_service()

    fake, records = _start_service(service)

    assert fake.insecure_addresses == ["[::]:33083"]
    assert fake.secure_bindings == []

    warnings = [line for line in records if line.startswith("WARNING:")]
    assert len(warnings) == 1
    assert "[::]:33083" in warnings[0]
    assert "unencrypted" in warnings[0]


def test_credentials_bind_a_secure_port_and_log_the_certificate_path(tls_pair):
    cert_file, key_file = tls_pair
    credentials = grpc.ssl_server_credentials(
        [(key_file.read_bytes(), cert_file.read_bytes())]
    )
    service = _build_service(
        server_credentials=credentials, tls_cert_file=str(cert_file)
    )

    fake, records = _start_service(service)

    assert fake.insecure_addresses == []
    assert fake.secure_bindings == [("[::]:33083", credentials)]

    # No plaintext warning, and the certificate path is named at INFO
    # (Requirement 1.7).
    assert [line for line in records if line.startswith("WARNING:")] == []
    tls_lines = [line for line in records if "with TLS" in line]
    assert len(tls_lines) == 1
    assert "[::]:33083" in tls_lines[0]
    assert str(cert_file) in tls_lines[0]


def test_interceptors_reach_the_grpc_server_at_construction_time():
    interceptor = AuthInterceptor()
    service = _build_service(interceptors=[interceptor])

    fake, _ = _start_service(service)

    # ``grpc.aio`` cannot register an interceptor after the fact, so the only
    # correct moment is the ``grpc.aio.server()`` call itself.
    assert fake.interceptors == (interceptor,)
    assert fake.started


def test_no_interceptors_is_an_empty_tuple_not_none():
    # A server built without interceptors must still be a valid argument for
    # ``grpc.aio.server()``, and the attribute must be iterable for callers that
    # inspect it.
    assert _build_service().interceptors == ()


# ---------------------------------------------------------------------------
# TaklerServer wiring and the Auth_Mode record (Requirements 3.11, 3.12)
# ---------------------------------------------------------------------------


def _capture_server_start(server: TaklerServer) -> List[str]:
    """Run ``server.start()`` with its three services stubbed out.

    Returns every message the ``server`` module logged, prefixed with its level.
    Only the start-up log is of interest here, so nothing needs to bind a port
    or read a snapshot.
    """

    async def _noop_async() -> None:
        return None

    server.scheduler.start = _noop_async
    server.network_service.start = _noop_async
    server.checkpoint_manager.start = _noop_async
    server.checkpoint_manager.restore = lambda: None

    records: List[str] = []

    def record(level):
        return lambda message, *a, **k: records.append(f"{level}:{message}")

    with (
        mock.patch.object(server_module.logger, "info", record("INFO")),
        mock.patch.object(server_module.logger, "warning", record("WARNING")),
    ):
        asyncio.run(server.start())

    return records


def test_server_hands_its_auth_interceptor_to_the_service():
    server = TaklerServer(host="login01", port=33083)

    assert isinstance(server.auth_interceptor, AuthInterceptor)
    assert server.network_service.interceptors == (server.auth_interceptor,)
    # The interceptor shares the store the server validates at start-up, so a
    # hot-reloaded secret file is seen by the interceptor without re-wiring.
    assert server.auth_interceptor.credential_store is server.credential_store


def test_disabled_auth_mode_warns_about_unauthenticated_control_commands(monkeypatch):
    monkeypatch.setenv("TAKLER_AUTH_MODE", "disabled")
    server = TaklerServer(host="login01", port=33083)

    records = _capture_server_start(server)

    warnings = [
        line
        for line in records
        if line.startswith("WARNING:") and "authentication is disabled" in line
    ]
    assert len(warnings) == 1
    assert "control command" in warnings[0]


def test_enabled_auth_mode_logs_the_effective_security_settings(
    monkeypatch, secret_file, tmp_path
):
    whitelist_file = tmp_path / "operators.txt"
    whitelist_file.write_text("alice\n")

    monkeypatch.setenv("TAKLER_AUTH_MODE", "enabled")
    monkeypatch.setenv("TAKLER_ZOMBIE_POLICY", "fob")

    connect_config = generate_connect_config()
    connect_config.security.operator_secret_file = str(secret_file)
    connect_config.security.operator_whitelist_file = str(whitelist_file)

    server = TaklerServer(host="login01", port=33083, connect_config=connect_config)
    assert server.auth_mode is AuthMode.ENABLED
    assert server.zombie_policy is ZombiePolicy.FOB

    records = _capture_server_start(server)

    lines = [line for line in records if "authentication enabled" in line]
    assert len(lines) == 1
    assert lines[0].startswith("INFO:")
    assert "enabled" in lines[0]
    assert "fob" in lines[0]
    assert str(whitelist_file) in lines[0]
    assert "authentication is disabled" not in "".join(records)


def test_tls_credentials_reach_the_service_during_start(monkeypatch, tls_pair):
    cert_file, key_file = tls_pair
    server = TaklerServer(
        host="login01",
        port=33083,
        tls_cert_file=cert_file,
        tls_key_file=key_file,
    )

    # Not built at construction time: reading the pair may abort the start-up,
    # which has to happen where the Server_CLI can turn it into exit code 1.
    assert server.network_service.server_credentials is None

    _capture_server_start(server)

    assert server.network_service.server_credentials is not None
    assert server.network_service.tls_cert_file == str(cert_file)
