"""Unit tests for client side credential injection (Task 10.2 of *m2-security*).

Three units meet here:

* :meth:`takler.client.service_client.TaklerServiceClient._build_metadata` --
  which Credential_Metadata keys a logical call carries, per
  :class:`~takler.client.retry.CommandKind` (Requirements 8.1 - 8.5, 8.7);
* the ``resolve_*`` family of :mod:`takler.client.credentials` -- where the CA
  file, the certificate host name override and the Operator_Secret_File come
  from (Requirements 2.3, 2.5, 8.6);
* the Client_CLI's ``NO_TAKLER`` short circuit, which must stay exactly as M1
  left it (Requirement 8.10).

The metadata assertions go through the real ``run_command_*`` methods with a spy
stub, not through ``_build_metadata`` alone: what matters is that the metadata
the Call_Wrapper builds actually reaches every attempt of the RPC
(Requirement 8.1).

No test names, log assertions or failure messages carry a credential value; the
secrets are generated per test and only ever appear inside an assertion, and the
"nothing leaks" test asserts the absence of exactly those generated values
(Requirements 8.9, 12.7).

Validates: Requirements 2.3, 2.5, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9,
8.10, 12.7
"""

from __future__ import annotations

import contextlib
import io
import secrets
from types import SimpleNamespace

import grpc
import pytest
from typer.testing import CliRunner

import takler.logging
from takler.client import cli
from takler.client.credentials import (
    ENV_JOB_PASSWORD,
    ENV_SECRET_FILE,
    ENV_TLS_CA_FILE,
    ENV_TLS_SERVER_NAME,
    METADATA_KEY_JOB_PASSWORD,
    METADATA_KEY_SECRET,
    METADATA_KEY_USER,
    current_user_name,
    resolve_ca_file,
    resolve_secret_file,
    resolve_server_name,
)
from takler.client.retry import CommandKind
from takler.client.service_client import TaklerServiceClient
from takler.exceptions import ClientConnectionError
from takler.server.connect_config import (
    Address,
    ConnectConfig,
    SecuritySettings,
    Server,
)


runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers and fixtures
# ---------------------------------------------------------------------------


class UnavailableRpcError(grpc.RpcError):
    """A retryable ``grpc.RpcError``, so one logical call spans two attempts."""

    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.UNAVAILABLE

    def details(self) -> str:
        return "no server"


class SpyRpc:
    """A stub method recording the ``metadata`` of every attempt it receives."""

    def __init__(self, flag: int = 0):
        self.response = SimpleNamespace(flag=flag, message="", output="")
        self.metadata_calls: list = []

    def __call__(self, request, timeout=None, metadata=None):
        self.metadata_calls.append(metadata)
        return self.response


@pytest.fixture(autouse=True)
def clean_credential_environment(monkeypatch):
    """Keep the developer's own environment out of the resolution under test."""
    for name in (
        ENV_JOB_PASSWORD,
        ENV_SECRET_FILE,
        ENV_TLS_CA_FILE,
        ENV_TLS_SERVER_NAME,
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def new_secret() -> str:
    """A fresh Operator_Secret, generated the way a real one would be."""
    return secrets.token_urlsafe(24)


@pytest.fixture
def new_password() -> str:
    """A fresh Job_Password, generated the way ``increment_try_no`` does."""
    return secrets.token_urlsafe(32)


def _secret_file(tmp_path, *lines: str):
    """Write an Operator_Secret_File holding ``lines`` and return its path."""
    path = tmp_path / "secret.txt"
    path.write_text("\n".join(lines) + "\n")
    path.chmod(0o600)
    return path


def make_client(**kwargs) -> TaklerServiceClient:
    """A client that never retries, so one logical call is one attempt."""
    kwargs.setdefault("host", "localhost")
    kwargs.setdefault("port", 33083)
    kwargs.setdefault("retry_window", 0.0)
    return TaklerServiceClient(**kwargs)


def child_metadata(client: TaklerServiceClient) -> list:
    """Run one Child_Command through the Call_Wrapper, return its metadata."""
    spy = SpyRpc()
    client.stub = SimpleNamespace(RunCommandComplete=spy)
    client.run_command_complete(node_path="/flow1/task1")
    assert len(spy.metadata_calls) == 1
    return spy.metadata_calls[0]


def operator_metadata(client: TaklerServiceClient) -> list:
    """Run one Control_Command through the Call_Wrapper, return its metadata."""
    spy = SpyRpc()
    client.stub = SimpleNamespace(RunCommandRequeue=spy)
    client.run_command_requeue(node_path=["/flow1"])
    assert len(spy.metadata_calls) == 1
    return spy.metadata_calls[0]


def capturing_stderr(func):
    """Run ``func`` while capturing the console log output.

    The console sink binds to ``sys.stderr`` when the configuration is applied,
    so configuring happens inside the redirection block. Mirrors
    ``tests/server/test_checkpoint_restore_unit.py``.
    """
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="DEBUG", console=True)
            result = func()
    finally:
        takler.logging.configure(console=True)
    return result, buffer.getvalue()


def _config(**security) -> ConnectConfig:
    """A Connect_Config whose ``security`` section holds ``security``."""
    return ConnectConfig(
        server=Server(
            address=Address(hostname="login01", ip="10.0.0.11", port="33083")
        ),
        security=SecuritySettings(**security),
    )


# ---------------------------------------------------------------------------
# Child_Command: takler-pass
# ---------------------------------------------------------------------------


def test_child_command_carries_the_job_password(monkeypatch, new_password):
    """Requirement 8.2: ``TAKLER_PASS`` travels as ``takler-pass``."""
    monkeypatch.setenv(ENV_JOB_PASSWORD, new_password)

    metadata = child_metadata(make_client())

    assert metadata == [(METADATA_KEY_JOB_PASSWORD, new_password)]


@pytest.mark.parametrize("configured", [None, "", "   ", "\t\n"])
def test_child_command_omits_the_key_when_unset_or_blank(monkeypatch, configured):
    """Requirement 8.3: unset and blank both mean "send it without the key"."""
    if configured is not None:
        monkeypatch.setenv(ENV_JOB_PASSWORD, configured)

    metadata = child_metadata(make_client())

    assert metadata == []


def test_child_command_carries_no_operator_credential(
    monkeypatch, tmp_path, new_password, new_secret
):
    """The two credential sets stay disjoint even when both are available."""
    monkeypatch.setenv(ENV_JOB_PASSWORD, new_password)
    client = make_client(secret_file=str(_secret_file(tmp_path, new_secret)))

    keys = [key for key, _ in child_metadata(client)]

    assert keys == [METADATA_KEY_JOB_PASSWORD]


def test_every_attempt_of_one_call_carries_the_same_metadata(
    monkeypatch, new_password, fake_clock
):
    """Requirement 8.1: the Call_Wrapper hands the metadata to each attempt."""
    monkeypatch.setenv(ENV_JOB_PASSWORD, new_password)

    class FailingRpc(SpyRpc):
        def __call__(self, request, timeout=None, metadata=None):
            self.metadata_calls.append(metadata)
            raise UnavailableRpcError()

    client = TaklerServiceClient(
        host="localhost",
        port=33083,
        retry_window=3.0,
        clock=fake_clock,
        sleep=fake_clock.sleep,
    )
    spy = FailingRpc()

    with pytest.raises(ClientConnectionError):
        client._call("complete", spy, "req", CommandKind.CHILD)

    assert len(spy.metadata_calls) > 1
    for metadata in spy.metadata_calls:
        assert metadata == [(METADATA_KEY_JOB_PASSWORD, new_password)]


# ---------------------------------------------------------------------------
# Operator_Command: takler-secret and takler-user
# ---------------------------------------------------------------------------


def test_operator_command_carries_secret_and_user(tmp_path, new_secret):
    """Requirements 8.4, 8.5: both operator keys travel together."""
    client = make_client(secret_file=str(_secret_file(tmp_path, new_secret)))

    metadata = operator_metadata(client)

    assert dict(metadata) == {
        METADATA_KEY_SECRET: new_secret,
        METADATA_KEY_USER: current_user_name(),
    }


def test_operator_command_reads_the_first_usable_line(tmp_path, new_secret):
    """Requirement 8.5: blanks and ``#`` comments are skipped, whitespace cut."""
    other = secrets.token_urlsafe(24)
    path = _secret_file(tmp_path, "", "   ", "# a comment", f"  {new_secret}  ", other)
    client = make_client(secret_file=str(path))

    assert dict(operator_metadata(client))[METADATA_KEY_SECRET] == new_secret


def test_operator_command_without_secret_file_carries_only_the_user():
    """Requirement 8.7: no secret file configured is not a failure."""
    metadata = operator_metadata(make_client())

    assert metadata == [(METADATA_KEY_USER, current_user_name())]


def test_query_command_carries_the_operator_credentials(tmp_path, new_secret):
    """A Query_Command is an Operator_Command as far as the client is concerned."""
    client = make_client(secret_file=str(_secret_file(tmp_path, new_secret)))

    metadata = client._build_metadata(CommandKind.QUERY)

    assert [key for key, _ in metadata] == [METADATA_KEY_SECRET, METADATA_KEY_USER]


def test_missing_secret_file_warns_and_still_sends(tmp_path):
    """Requirement 8.8: a WARNING naming the path, and the call goes ahead."""
    absent = tmp_path / "absent.txt"
    client = make_client(secret_file=str(absent))

    metadata, captured = capturing_stderr(lambda: operator_metadata(client))

    assert metadata == [(METADATA_KEY_USER, current_user_name())]
    warnings = [line for line in captured.splitlines() if "WARNING" in line]
    assert len(warnings) == 1
    assert str(absent) in warnings[0]
    assert "FileNotFoundError" in warnings[0] or "No such file" in warnings[0]


def test_unreadable_secret_file_warns_and_still_sends(tmp_path, new_secret):
    """Requirement 8.8: an unreadable file is reported, not raised."""
    path = _secret_file(tmp_path, new_secret)
    path.chmod(0o000)
    client = make_client(secret_file=str(path))

    try:
        metadata, captured = capturing_stderr(lambda: operator_metadata(client))
    finally:
        path.chmod(0o600)

    assert metadata == [(METADATA_KEY_USER, current_user_name())]
    warnings = [line for line in captured.splitlines() if "WARNING" in line]
    assert len(warnings) == 1
    assert str(path) in warnings[0]
    assert "PermissionError" in warnings[0] or "Permission denied" in warnings[0]


def test_secret_file_without_any_usable_line_warns_and_still_sends(tmp_path):
    """Requirement 8.8: a file of blanks and comments holds no secret."""
    path = _secret_file(tmp_path, "", "  ", "# only a comment")
    client = make_client(secret_file=str(path))

    metadata, captured = capturing_stderr(lambda: operator_metadata(client))

    assert metadata == [(METADATA_KEY_USER, current_user_name())]
    assert len([line for line in captured.splitlines() if "WARNING" in line]) == 1


# ---------------------------------------------------------------------------
# Requirements 8.9, 12.7: no credential value reaches the log
# ---------------------------------------------------------------------------


def test_no_credential_value_appears_in_the_log(
    monkeypatch, tmp_path, new_password, new_secret
):
    """Requirements 8.9, 12.7: neither credential is logged, at any level."""
    monkeypatch.setenv(ENV_JOB_PASSWORD, new_password)
    path = _secret_file(tmp_path, new_secret)
    client = make_client(secret_file=str(path))

    def run() -> None:
        child_metadata(client)
        operator_metadata(client)

    _, captured = capturing_stderr(run)

    assert new_password not in captured
    assert new_secret not in captured


# ---------------------------------------------------------------------------
# The three resolve_* functions and their four precedence levels
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "resolve, env_name, field",
    [
        (resolve_ca_file, ENV_TLS_CA_FILE, "ca_file"),
        (resolve_server_name, ENV_TLS_SERVER_NAME, "server_name"),
        (resolve_secret_file, ENV_SECRET_FILE, "operator_secret_file"),
    ],
    ids=["ca_file", "server_name", "secret_file"],
)
class TestResolvePrecedence:
    """Requirements 2.3, 2.5, 8.6: four levels, blank counting as absent."""

    def test_explicit_argument_wins(self, resolve, env_name, field, monkeypatch):
        monkeypatch.setenv(env_name, "from-env")

        resolved = resolve("from-explicit", _config(**{field: "from-config"}))

        assert resolved == "from-explicit"

    def test_environment_wins_over_config(self, resolve, env_name, field, monkeypatch):
        monkeypatch.setenv(env_name, "from-env")

        assert resolve(None, _config(**{field: "from-config"})) == "from-env"

    def test_config_wins_over_nothing(self, resolve, env_name, field):
        assert resolve(None, _config(**{field: "from-config"})) == "from-config"

    def test_unconfigured_resolves_to_none(self, resolve, env_name, field):
        assert resolve(None, None) is None
        assert resolve(None, _config()) is None

    @pytest.mark.parametrize("blank", ["", "   ", "\t"])
    def test_blank_falls_through_to_the_next_level(
        self, resolve, env_name, field, blank, monkeypatch
    ):
        monkeypatch.setenv(env_name, blank)

        assert resolve(blank, _config(**{field: "from-config"})) == "from-config"
        assert resolve(blank, _config(**{field: blank})) is None

    def test_resolved_value_is_stripped(self, resolve, env_name, field):
        assert resolve("  padded  ", None) == "padded"


def test_constructor_resolves_all_three_paths(monkeypatch, tmp_path):
    """The client resolves the three knobs once, at construction time."""
    monkeypatch.setenv(ENV_SECRET_FILE, "from-env-secret")

    client = TaklerServiceClient(
        host="h",
        port=1,
        connect_config=_config(
            ca_file="from-config-ca",
            server_name="from-config-name",
            operator_secret_file="from-config-secret",
        ),
    )

    assert client.ca_file == "from-config-ca"
    assert client.server_name == "from-config-name"
    assert client.secret_file == "from-env-secret"


# ---------------------------------------------------------------------------
# Requirement 8.10: NO_TAKLER
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "args",
    [
        ["init", "--task-id", "1", "--node-path", "/flow1/task1"],
        ["complete", "--node-path", "/flow1/task1"],
        ["abort", "--node-path", "/flow1/task1"],
        ["event", "--node-path", "/flow1/task1", "--event-name", "e"],
        [
            "meter",
            "--node-path",
            "/flow1/task1",
            "--meter-name",
            "m",
            "--meter-value",
            "1",
        ],
    ],
)
def test_no_takler_exits_zero_without_opening_a_channel(
    monkeypatch, new_password, args
):
    """Requirement 8.10: the M1 short circuit survives credential injection.

    Credentials are deliberately available here: a job script running under
    ``NO_TAKLER`` exports ``TAKLER_PASS`` just like any other, and that must
    still not cause a connection attempt.
    """
    opened: list = []
    monkeypatch.setattr(
        "grpc.insecure_channel", lambda *a, **k: opened.append(a) or SimpleNamespace()
    )
    monkeypatch.setattr(
        "grpc.secure_channel", lambda *a, **k: opened.append(a) or SimpleNamespace()
    )

    result = runner.invoke(
        cli.app, args, env={"NO_TAKLER": "1", ENV_JOB_PASSWORD: new_password}
    )

    assert result.exit_code == 0
    assert opened == []
    assert "NO_TAKLER" in result.stdout
