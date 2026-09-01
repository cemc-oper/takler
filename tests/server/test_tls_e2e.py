"""TLS over the wire, from a real server port to a real client channel.

Everything else about TLS is unit tested: ``tests/server/test_tls_credentials_unit.py``
covers turning configuration into credentials, ``tests/server/test_service_secure_port.py``
covers which port-binding call the service makes, and
``tests/client/test_credential_injection.py`` covers the client's channel choice
with a fake ``grpc``. None of them proves the two halves interoperate -- a
certificate the server accepts but no client can verify, or a channel option
that is spelled correctly but ignored, passes all of them. This file therefore
runs an actual ``TaklerServer`` on an actual port with an actual certificate and
dials it with the actual client.

Three claims, each of which only a real handshake can settle:

* a client that has the CA can drive both command families end to end -- one
  Control_Command that changes state on the server and two Query_Commands
  (Requirement 2.1). Auth_Mode stays at its default ``disabled``, so what is
  under test is the transport and nothing else;
* a client that has *no* CA talking to a TLS server does not hang and does not
  report something misleading: the handshake fails, the Call_Wrapper treats it
  as a transport failure like any other, and once the (shortened) Retry_Window
  is spent the Client_CLI exits 4 with one stderr line naming the server address
  and how many attempts it made (Requirements 2.2, 2.7, 2.8). That line is the
  only thing an operator sees when the server was upgraded to TLS and a job
  script was not, so it has to point at the server rather than at the network;
* the certificate host name override really overrides. A certificate issued for
  a name that is not the address being dialed fails verification, and succeeds
  with ``server_name`` set (Requirement 2.4). This is the HPC case the option
  exists for: a certificate registered for the login node's name, dialed by
  address.

The server runs in a background event-loop thread because ``TaklerServer`` is
``asyncio`` and ``TaklerServiceClient`` is blocking; the same split as in
``tests/server/test_zombie_after_requeue.py``.

Validates: Requirements 2.1, 2.2, 2.4, 2.7, 2.8, 16.1
"""

from __future__ import annotations

import asyncio
import datetime
import ipaddress
import re
import socket
import threading
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from typer.testing import CliRunner

from takler.client import cli
from takler.client.service_client import TaklerServiceClient
from takler.core import Flow
from takler.exceptions import ClientConnectionError
from takler.server import TaklerServer


#: Loopback keeps the test hermetic: no name resolution, no traffic leaving the
#: machine.
LOCALHOST = "127.0.0.1"

#: Name the "wrong host name" certificate is issued for. ``.invalid`` is
#: reserved by RFC 2606, so it can never resolve to anything.
OTHER_HOST_NAME = "takler-server.invalid"

FLOW_NAME = "flow1"
TASK_PATH = "/flow1/task1"

#: Main-loop interval of the in-process server. The scheduler notices
#: ``should_stop`` only between two iterations, so a long interval would make
#: the fixture teardown wait that long.
TEST_MAIN_LOOP_INTERVAL = 0.05

#: Retry_Window handed to the clients that are expected to fail, in seconds.
#: The default for a Query_Command is 60 seconds, which is the right number in
#: production and an unacceptable one in a test; one second still lets a real
#: retry happen, so the "total attempts" of the give-up line is a number the
#: Call_Wrapper counted rather than a constant.
SHORT_RETRY_WINDOW = "1"

runner = CliRunner()


# ---------------------------------------------------------------------------
# Certificates
# ---------------------------------------------------------------------------


def write_self_signed_pair(
    directory: Path,
    common_name: str,
    dns_names: Tuple[str, ...] = (),
    ip_addresses: Tuple[str, ...] = (),
) -> Tuple[Path, Path]:
    """Write a self-signed certificate and its private key into ``directory``.

    Self-signed on purpose: the client trusts the certificate itself as its root
    of trust, so the same file is the server's certificate and the client's CA.
    A real deployment has an internal CA; a test that built one would be testing
    ``cryptography``, not takler.

    ``dns_names`` and ``ip_addresses`` go into the Subject Alternative Name
    extension, which is what a modern TLS client verifies the dialed host
    against -- the Common Name alone is not enough for either OpenSSL or
    BoringSSL, so a certificate meant to verify must name its host here.

    Args:
        directory: Where the two files are written.
        common_name: Subject and issuer Common Name.
        dns_names: DNS entries of the Subject Alternative Name extension.
        ip_addresses: IP entries of the Subject Alternative Name extension.

    Returns:
        The certificate path and the private key path, both PEM encoded.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.datetime.now(datetime.timezone.utc)

    alternative_names: List[x509.GeneralName] = [
        x509.DNSName(name) for name in dns_names
    ]
    alternative_names.extend(
        x509.IPAddress(ipaddress.ip_address(address)) for address in ip_addresses
    )

    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
    )
    if alternative_names:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(alternative_names), critical=False
        )
    certificate = builder.sign(key, hashes.SHA256())

    cert_path = directory / f"{common_name}.crt"
    key_path = directory / f"{common_name}.key"
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
def loopback_pair(tmp_path_factory) -> Tuple[Path, Path]:
    """A certificate that verifies for the address the tests dial.

    Module scoped because generating an RSA key is by far the slowest thing in
    this file, and nothing here mutates the pair.
    """
    return write_self_signed_pair(
        tmp_path_factory.mktemp("tls-loopback"),
        common_name="localhost",
        dns_names=("localhost",),
        ip_addresses=(LOCALHOST,),
    )


@pytest.fixture(scope="module")
def other_name_pair(tmp_path_factory) -> Tuple[Path, Path]:
    """A certificate issued for a name that is not the dialed address."""
    return write_self_signed_pair(
        tmp_path_factory.mktemp("tls-other-name"),
        common_name=OTHER_HOST_NAME,
        dns_names=(OTHER_HOST_NAME,),
    )


# ---------------------------------------------------------------------------
# The server under test
# ---------------------------------------------------------------------------


def free_port() -> int:
    """Return a currently free TCP port.

    Binding port 0 and reading the assigned port back is the portable way to get
    a port that is free right now; a hard-coded one would collide with a
    parallel test run or with a developer's own server.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def build_flow() -> Flow:
    """One begun, suspended flow holding a single task.

    Suspended at the root so the scheduler main loop -- which really runs here --
    cannot submit the dependency-free task while a test is asserting on it.
    ``check_dependencies`` stops at a suspended node, which freezes the tree
    without freezing the loop.
    """
    flow = Flow(FLOW_NAME)
    flow.add_task("task1")
    flow.begin()
    flow.suspend()
    return flow


class ServedServer:
    """Runs a :class:`TaklerServer` in a background event-loop thread.

    ``TaklerServer`` is an ``asyncio`` server and ``TaklerServiceClient`` -- and
    with it the Client_CLI -- is fully blocking, so calling the client from the
    loop that has to answer it would deadlock on the first command. Owning the
    loop in a thread keeps the test body plain synchronous code.
    """

    def __init__(self, server: TaklerServer) -> None:
        self.server = server
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._error: Optional[BaseException] = None

    @property
    def port(self) -> str:
        return self.server.network_service.port

    def start(self, timeout: float = 15.0) -> "ServedServer":
        self._thread = threading.Thread(
            target=self._thread_main, name="takler-tls-test-server", daemon=True
        )
        self._thread.start()
        if not self._ready.wait(timeout):
            raise TimeoutError(f"takler server did not start within {timeout} seconds")
        if self._error is not None:
            raise self._error
        return self

    def stop(self, timeout: float = 20.0) -> None:
        if self._thread is None:
            return
        if self._loop is not None and self._thread.is_alive():
            future = asyncio.run_coroutine_threadsafe(self.server.stop(), self._loop)
            try:
                future.result(timeout=timeout)
            except Exception:  # noqa: BLE001 - teardown must not mask a failure
                pass
        self._thread.join(timeout=timeout)
        self._thread = None

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._serve())
        except BaseException as exc:  # noqa: BLE001 - reported from start()
            self._error = exc
        finally:
            # Unblock ``start()`` even when startup failed, so the test fails
            # with the real error instead of a timeout.
            self._ready.set()

    async def _serve(self) -> None:
        self.server.scheduler.interval_main_loop = TEST_MAIN_LOOP_INTERVAL
        self._loop = asyncio.get_running_loop()
        await self.server.start()
        run_task = self._loop.create_task(self.server.run(), name="takler.test.server")
        # The port is bound once ``start()`` returned, so a client may dial.
        self._ready.set()
        await run_task


def serve_with_tls(pair: Tuple[Path, Path], tmp_path: Path) -> ServedServer:
    """Start a TLS server holding one flow, on a free loopback port.

    Auth_Mode is left at its default ``disabled``: this file is about the
    transport, and requiring credentials would mean a failure could come from
    either layer.
    """
    cert_file, key_file = pair
    server = TaklerServer(
        host=LOCALHOST,
        port=free_port(),
        tls_cert_file=cert_file,
        tls_key_file=key_file,
        checkpoint_file=tmp_path / "takler.check",
    )
    server.bunch.add_flow(build_flow())
    return ServedServer(server).start()


@pytest.fixture
def tls_server(loopback_pair, tmp_path) -> Iterator[ServedServer]:
    """A started TLS server whose certificate verifies for ``127.0.0.1``."""
    running = serve_with_tls(loopback_pair, tmp_path)
    try:
        yield running
    finally:
        running.stop()


@pytest.fixture
def other_name_tls_server(other_name_pair, tmp_path) -> Iterator[ServedServer]:
    """A started TLS server whose certificate names a different host."""
    running = serve_with_tls(other_name_pair, tmp_path)
    try:
        yield running
    finally:
        running.stop()


def client_for(
    running: ServedServer,
    ca_file: Optional[Path] = None,
    server_name: Optional[str] = None,
) -> TaklerServiceClient:
    """A client dialing ``running``, with the given transport security.

    The Retry_Window is pinned to zero -- one attempt, no retry -- for every
    client built here: a handshake that fails, fails the same way on every
    attempt, so the default 60 seconds would only make a failing test slow. The
    one place where retrying matters is the Client_CLI test below, which is
    about the give-up line itself.
    """
    return TaklerServiceClient(
        host=LOCALHOST,
        port=running.port,
        ca_file=None if ca_file is None else str(ca_file),
        server_name=server_name,
        retry_window=0,
    )


# ---------------------------------------------------------------------------
# A client holding the CA drives both command families (Requirement 2.1)
# ---------------------------------------------------------------------------


def test_client_with_the_ca_runs_a_control_and_a_query_command(
    tls_server, loopback_pair
):
    cert_file, _ = loopback_pair
    client = client_for(tls_server, ca_file=cert_file)
    task = tls_server.server.bunch.find_node(TASK_PATH)
    assert not task.is_suspended()

    # Control_Command: the assertion is on the server's state, not on the
    # response flag, so a server that answered without acting would fail.
    client.suspend([TASK_PATH])
    assert task.is_suspended()

    # Query_Commands: ``ping``, which carries no payload, and ``show``, which
    # brings the whole bunch back over the encrypted channel and parses it.
    ping_response = client.ping()
    assert ping_response is not None

    show_response = client.show(
        show_trigger=False,
        show_parameter=False,
        show_limit=False,
        show_event=False,
        show_meter=False,
    )
    assert FLOW_NAME in show_response.output

    # Requirement 2.8: TLS changes the transport, not the Call_Wrapper. A
    # successful call still leaves no channel behind.
    assert client.channel is None


# ---------------------------------------------------------------------------
# A client without the CA (Requirements 2.2, 2.7, 2.8)
# ---------------------------------------------------------------------------


def test_client_without_the_ca_builds_a_plaintext_channel_and_cannot_connect(
    tls_server,
):
    """The unencrypted channel of Requirement 2.2 really is unencrypted.

    Which is why it cannot talk to a TLS server: the server refuses the
    plaintext handshake, and the Call_Wrapper reports an unreachable server
    after its window is spent (Requirement 2.7).
    """
    client = client_for(tls_server)

    with pytest.raises(ClientConnectionError) as excinfo:
        client.ping()

    message = str(excinfo.value)
    assert f"{LOCALHOST}:{tls_server.port}" in message
    assert client.channel is None


def test_client_cli_without_the_ca_exits_four_naming_address_and_attempts(
    monkeypatch, tls_server
):
    """What a job script sees when the server got TLS and the script did not.

    Requirements 2.7, 2.8: exit code 4, one stderr line, and that line names the
    server address and the number of attempts the shortened Retry_Window
    allowed. Without the attempt count the line reads like a network outage;
    with it, it reads like a client that never got through.
    """
    # The command must resolve "no CA" from the environment as well, so a
    # developer machine that exports either variable cannot mask the case.
    monkeypatch.delenv("TAKLER_TLS_CA_FILE", raising=False)
    monkeypatch.delenv("TAKLER_CONNECT_FILE", raising=False)
    monkeypatch.delenv("NO_TAKLER", raising=False)

    result = runner.invoke(
        cli.app,
        ["ping", "--host", LOCALHOST, "--port", str(tls_server.port)],
        env={"TAKLER_TIMEOUT": SHORT_RETRY_WINDOW},
    )

    assert result.exit_code == 4

    lines = [line for line in result.stderr.splitlines() if line.strip()]
    assert len(lines) == 1
    assert f"{LOCALHOST}:{tls_server.port}" in lines[0]
    assert "Traceback" not in result.stderr

    attempts = re.search(r"after (\d+) attempts", lines[0])
    assert attempts is not None, lines[0]
    assert int(attempts.group(1)) >= 1


# ---------------------------------------------------------------------------
# The certificate host name override (Requirement 2.4)
# ---------------------------------------------------------------------------


def test_a_certificate_for_another_host_name_fails_without_the_override(
    other_name_tls_server, other_name_pair
):
    """The control case, without which the next test proves nothing.

    The client has the right CA and still cannot connect, because the
    certificate names ``takler-server.invalid`` and the client dialed
    ``127.0.0.1``. This is the situation the override exists for.
    """
    cert_file, _ = other_name_pair
    client = client_for(other_name_tls_server, ca_file=cert_file)

    with pytest.raises(ClientConnectionError):
        client.ping()


def test_server_name_override_makes_the_handshake_succeed(
    other_name_tls_server, other_name_pair
):
    cert_file, _ = other_name_pair
    client = client_for(
        other_name_tls_server, ca_file=cert_file, server_name=OTHER_HOST_NAME
    )

    # Same server, same certificate, same CA as the test above: the override is
    # the only difference, so it is what carried the handshake.
    response = client.ping()
    assert response is not None
