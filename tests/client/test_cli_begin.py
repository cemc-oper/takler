"""Unit tests for the Client_CLI ``begin`` subcommand.

``takler/client/cli.py`` is driven through typer's ``CliRunner`` with the
service client replaced by a fake, so no gRPC server is involved: what matters
here is that the arguments the operator typed reach the Service_Client
unchanged, in particular that an omitted ``FLOW_NAME`` becomes the empty string
that means "all flows".

Requirements: 8.16.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from takler.client import cli
from takler.exceptions import ClientConnectionError


runner = CliRunner()


class FakeResponse:
    def __init__(self, flag: int = 0, message: str = ""):
        self.flag = flag
        self.message = message


class FakeClient:
    """Records the ``begin`` call and replays a fixed outcome."""

    instances: list = []

    def __init__(self, host=None, port=None, outcome=None):
        self.host = host
        self.port = port
        self.outcome = outcome
        self.calls = []
        FakeClient.instances.append(self)

    def begin(self, **kwargs):
        self.calls.append(("begin", kwargs))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


@pytest.fixture
def fake_client(monkeypatch):
    FakeClient.instances = []
    holder = {"outcome": FakeResponse(flag=0)}

    def factory(host=None, port=None):
        return FakeClient(host=host, port=port, outcome=holder["outcome"])

    monkeypatch.setattr(cli, "TaklerServiceClient", factory)
    return holder


def stderr_lines(result) -> list:
    return [line for line in result.stderr.splitlines() if line.strip()]


def test_begin_passes_flow_name_and_default_force(fake_client):
    result = runner.invoke(cli.app, ["begin", "flow1"])

    assert result.exit_code == 0
    assert FakeClient.instances[0].calls == [
        ("begin", {"flow_name": "flow1", "force": False})
    ]


def test_begin_passes_force_when_requested(fake_client):
    result = runner.invoke(cli.app, ["begin", "flow1", "--force"])

    assert result.exit_code == 0
    assert FakeClient.instances[0].calls == [
        ("begin", {"flow_name": "flow1", "force": True})
    ]


def test_begin_without_flow_name_sends_empty_string(fake_client):
    """An omitted ``FLOW_NAME`` is the wire form of "all flows"."""
    result = runner.invoke(cli.app, ["begin"])

    assert result.exit_code == 0
    assert FakeClient.instances[0].calls == [
        ("begin", {"flow_name": "", "force": False})
    ]


def test_begin_uses_the_given_host_and_port(fake_client):
    result = runner.invoke(
        cli.app, ["begin", "flow1", "--host", "somehost", "--port", "12345"]
    )

    assert result.exit_code == 0
    client = FakeClient.instances[0]
    assert (client.host, client.port) == ("somehost", "12345")


def test_begin_ignores_no_takler(fake_client):
    """``begin`` is a Control_Command, so ``NO_TAKLER`` does not skip it."""
    result = runner.invoke(cli.app, ["begin", "flow1"], env={"NO_TAKLER": "1"})

    assert result.exit_code == 0
    assert FakeClient.instances[0].calls == [
        ("begin", {"flow_name": "flow1", "force": False})
    ]


def test_begin_rejection_flag_becomes_one_stderr_line_and_exit_code(fake_client):
    """Requirement 8.11 rejection comes back as a flag, not as an exception."""
    fake_client["outcome"] = FakeResponse(
        flag=10, message="flow flow1 has already begun"
    )

    result = runner.invoke(cli.app, ["begin", "flow1"])

    assert result.exit_code == 1
    lines = stderr_lines(result)
    assert len(lines) == 1
    assert "flow1" in lines[0]
    assert "Traceback" not in result.stderr


def test_begin_unreachable_server_exits_four(fake_client):
    fake_client["outcome"] = ClientConnectionError(
        "server localhost:33083 is unreachable after 3 attempts, "
        "last gRPC status UNAVAILABLE"
    )

    result = runner.invoke(cli.app, ["begin"])

    assert result.exit_code == 4
    assert len(stderr_lines(result)) == 1
