"""Exit code and error output tests for the Client_CLI.

``takler/client/cli.py`` is exercised through typer's ``CliRunner`` with the
service client replaced by a fake, so no gRPC server is involved: what matters
here is the translation from "response flag" / "raised exception" to "one stderr
line plus an exit code".

Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

import takler.logging
from takler.client import cli
from takler.exceptions import ClientConnectionError, NodeNotFoundError


runner = CliRunner()


class FakeResponse:
    def __init__(self, flag: int = 0, message: str = ""):
        self.flag = flag
        self.message = message


class FakeClient:
    """Stands in for ``TaklerServiceClient``; every command replays one outcome.

    ``outcome`` is either a response returned by the command or an exception
    raised by it. The class records the constructor arguments and the calls so a
    test can assert the command was (or was not) attempted.
    """

    instances: list = []

    def __init__(self, host=None, port=None, outcome=None):
        self.host = host
        self.port = port
        self.outcome = outcome
        self.calls = []
        FakeClient.instances.append(self)

    def _run(self, name, **kwargs):
        self.calls.append((name, kwargs))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome

    def __getattr__(self, name):
        # Any command name (complete, requeue, ping, ...) replays the outcome.
        def command(**kwargs):
            return self._run(name, **kwargs)

        return command


@pytest.fixture
def fake_client(monkeypatch):
    """Install a ``FakeClient`` factory whose outcome the test chooses."""
    FakeClient.instances = []
    holder = {"outcome": FakeResponse(flag=0)}

    def factory(host=None, port=None):
        return FakeClient(host=host, port=port, outcome=holder["outcome"])

    monkeypatch.setattr(cli, "TaklerServiceClient", factory)
    return holder


def stderr_lines(result) -> list:
    return [line for line in result.stderr.splitlines() if line.strip()]


# -- success ----------------------------------------------------------


def test_successful_command_exits_zero(fake_client):
    fake_client["outcome"] = FakeResponse(flag=0)

    result = runner.invoke(cli.app, ["complete", "--node-path", "/flow1/task1"])

    assert result.exit_code == 0
    assert stderr_lines(result) == []
    assert FakeClient.instances[0].calls == [
        ("complete", {"node_path": "/flow1/task1"})
    ]


def test_query_response_without_flag_exits_zero(fake_client):
    """``ping`` returns a response with no ``flag`` field at all."""
    fake_client["outcome"] = object()

    result = runner.invoke(cli.app, ["ping"])

    assert result.exit_code == 0
    assert stderr_lines(result) == []


# -- business failure: flag -> exit code ------------------------------


def test_request_error_flag_exits_one_with_one_stderr_line(fake_client):
    fake_client["outcome"] = FakeResponse(flag=10, message="node not found: /a")

    result = runner.invoke(cli.app, ["requeue", "/a"])

    assert result.exit_code == 1
    lines = stderr_lines(result)
    assert len(lines) == 1
    assert "node_not_found" in lines[0]
    assert "node not found: /a" in lines[0]
    assert "Traceback" not in result.stderr


def test_internal_server_error_flag_exits_three(fake_client):
    fake_client["outcome"] = FakeResponse(flag=99, message="KeyError: 'x'")

    result = runner.invoke(cli.app, ["requeue", "/a"])

    assert result.exit_code == 3
    lines = stderr_lines(result)
    assert len(lines) == 1
    assert "internal_error" in lines[0]
    assert "KeyError: 'x'" in lines[0]


def test_multiline_server_message_is_folded_into_one_line(fake_client):
    fake_client["outcome"] = FakeResponse(flag=10, message="first\nsecond")

    result = runner.invoke(cli.app, ["requeue", "/a"])

    assert result.exit_code == 1
    lines = stderr_lines(result)
    assert len(lines) == 1
    assert "first second" in lines[0]


# -- client side failure ----------------------------------------------


def test_unreachable_server_exits_four_with_address_and_attempts(fake_client):
    fake_client["outcome"] = ClientConnectionError(
        "server localhost:33083 is unreachable after 17 attempts, "
        "last gRPC status UNAVAILABLE"
    )

    result = runner.invoke(cli.app, ["complete", "--node-path", "/flow1/task1"])

    assert result.exit_code == 4
    lines = stderr_lines(result)
    assert len(lines) == 1
    assert "localhost:33083" in lines[0]
    assert "17 attempts" in lines[0]
    assert "Traceback" not in result.stderr


def test_takler_error_exits_one(fake_client):
    fake_client["outcome"] = NodeNotFoundError("no such node: /a", node_path="/a")

    result = runner.invoke(cli.app, ["requeue", "/a"])

    assert result.exit_code == 1
    lines = stderr_lines(result)
    assert len(lines) == 1
    assert "NodeNotFoundError" in lines[0]
    assert "no such node: /a" in lines[0]


def test_unexpected_exception_exits_three_without_traceback(fake_client):
    fake_client["outcome"] = RuntimeError("something unforeseen")

    result = runner.invoke(cli.app, ["requeue", "/a"])

    assert result.exit_code == 3
    lines = stderr_lines(result)
    assert len(lines) == 1
    assert "RuntimeError" in lines[0]
    assert "something unforeseen" in lines[0]
    assert "Traceback" not in result.stderr


def test_unexpected_exception_traceback_goes_to_the_log_file(
    fake_client, monkeypatch, tmp_path
):
    log_file = tmp_path / "client.log"
    monkeypatch.setenv("TAKLER_LOG_FILE", str(log_file))
    fake_client["outcome"] = RuntimeError("something unforeseen")

    try:
        result = runner.invoke(cli.app, ["requeue", "/a"])
    finally:
        takler.logging.configure(console=True)

    assert result.exit_code == 3
    assert "Traceback" not in result.stderr

    content = log_file.read_text()
    assert "Traceback (most recent call last)" in content
    assert "RuntimeError: something unforeseen" in content


# -- NO_TAKLER --------------------------------------------------------


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
def test_no_takler_skips_the_server_call_and_exits_zero(fake_client, args):
    """Requirement 10.5: no communication happens, the process exits 0."""
    fake_client["outcome"] = ClientConnectionError("would have failed")

    result = runner.invoke(cli.app, args, env={"NO_TAKLER": "1"})

    assert result.exit_code == 0
    assert FakeClient.instances == []
    assert "NO_TAKLER" in result.stdout
