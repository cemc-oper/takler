"""Boundary unit tests for the Call_Wrapper and channel lifecycle.

``takler/client/service_client.py`` is exercised here with a fake stub, so no
gRPC server is involved: the interesting behaviour (timeout argument, retry,
status code mapping, channel lifetime, ``show`` response parsing) is all client
side. The exhaustive "for all inputs" assertions belong to the property tests.

Requirements: 9.1, 9.2, 9.5, 9.6, 9.7, 9.8, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6.
"""

from __future__ import annotations

import contextlib
import io
import json

import grpc
import pytest

import takler.logging
from takler.client.retry import DEFAULT_SINGLE_TIMEOUT, CommandKind
from takler.client.service_client import TaklerServiceClient
from takler.core import Bunch, Flow
from takler.exceptions import (
    ClientConnectionError,
    InvalidRequestError,
    NodeNotFoundError,
    PermissionDeniedError,
    ServerResponseError,
    TransportError,
)


class FakeRpcError(grpc.RpcError):
    """A ``grpc.RpcError`` with a controllable status code."""

    def __init__(self, code: grpc.StatusCode, details: str = "boom"):
        super().__init__(f"{code}: {details}")
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


class FakeResponse:
    def __init__(self, flag: int = 0, message: str = "", output: str = ""):
        self.flag = flag
        self.message = message
        self.output = output


class FakeRpc:
    """A stub method that replays ``outcomes`` and records its calls."""

    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = []
        #: The ``metadata`` argument of each call, in call order. The
        #: Call_Wrapper hands Credential_Metadata to every attempt (m2
        #: requirement 8.1); what it contains is asserted in
        #: ``test_credential_injection.py``, this only keeps the double
        #: accepting the real call signature.
        self.metadata_calls = []

    def __call__(self, request, timeout=None, metadata=None):
        self.calls.append((request, timeout))
        self.metadata_calls.append(metadata)
        outcome = self.outcomes[min(len(self.calls), len(self.outcomes)) - 1]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class FakeChannel:
    def __init__(self):
        self.closed = 0

    def unary_unary(self, *args, **kwargs):
        """Let ``TaklerServerStub`` bind its methods against this channel."""
        return FakeRpc(FakeResponse())

    def close(self):
        self.closed += 1


def make_client(fake_clock, retry_window=60.0):
    return TaklerServiceClient(
        host="localhost",
        port=33083,
        retry_window=retry_window,
        clock=fake_clock,
        sleep=fake_clock.sleep,
    )


def call_capturing_stderr(client, rpc, retry_window=None):
    """Run ``_call`` while capturing the console log output.

    The console sink binds to ``sys.stderr`` when the configuration is applied,
    so configuring has to happen inside the redirection block. Mirrors
    ``tests/client/test_retry_unit.py``.
    """
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="WARNING", console=True)
            try:
                result = client._call(
                    "complete", rpc, FakeResponse(), CommandKind.CHILD
                )
                error = None
            except BaseException as exc:  # noqa: BLE001 - returned to caller
                result, error = None, exc
    finally:
        takler.logging.configure(console=True)
    return result, error, buffer.getvalue()


# -- _call: timeout and success ---------------------------------------


def test_call_passes_single_timeout_and_returns_response(fake_clock):
    client = make_client(fake_clock)
    response = FakeResponse(flag=0)
    rpc = FakeRpc(response)

    assert client._call("complete", rpc, "req", CommandKind.CHILD) is response
    assert rpc.calls == [("req", DEFAULT_SINGLE_TIMEOUT)]


def test_call_uses_configured_single_timeout(fake_clock):
    client = TaklerServiceClient(
        host="h",
        port=1,
        single_timeout=2.5,
        retry_window=0.0,
        clock=fake_clock,
        sleep=fake_clock.sleep,
    )
    rpc = FakeRpc(FakeResponse())

    client._call("ping", rpc, "req", CommandKind.QUERY)

    assert rpc.calls[0][1] == 2.5


def test_call_does_not_retry_business_failure(fake_clock):
    """flag != 0 is returned unchanged, without a second attempt (9.7)."""
    client = make_client(fake_clock)
    response = FakeResponse(flag=10, message="no such node")
    rpc = FakeRpc(response)

    result = client._call("complete", rpc, "req", CommandKind.CHILD)

    assert result is response
    assert len(rpc.calls) == 1
    assert fake_clock.slept == []


# -- _call: retry -----------------------------------------------------


def test_call_retries_retryable_status_then_succeeds(fake_clock):
    client = make_client(fake_clock, retry_window=60.0)
    response = FakeResponse()
    rpc = FakeRpc(
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED),
        response,
    )

    result, error, captured = call_capturing_stderr(client, rpc)

    assert error is None
    assert result is response
    assert len(rpc.calls) == 3
    assert fake_clock.slept == [1.0, 2.0]
    warnings = [line for line in captured.splitlines() if "WARNING" in line]
    assert len(warnings) == 2
    for line in warnings:
        assert "localhost:33083" in line
        assert "complete" in line
        assert "elapsed=" in line
    assert "UNAVAILABLE" in warnings[0]
    assert "DEADLINE_EXCEEDED" in warnings[1]


def test_call_raises_client_connection_error_when_window_exhausted(fake_clock):
    client = make_client(fake_clock, retry_window=3.0)
    rpc = FakeRpc(FakeRpcError(grpc.StatusCode.UNAVAILABLE))

    _, error, captured = call_capturing_stderr(client, rpc)

    assert isinstance(error, ClientConnectionError)
    text = str(error)
    assert "localhost:33083" in text
    assert f"{len(rpc.calls)} attempts" in text
    assert "UNAVAILABLE" in text
    # 1 + 2 fills the 3 second window exactly, so the third failure gives up.
    assert fake_clock.slept == [1.0, 2.0]
    assert len(rpc.calls) == 3
    assert len([line for line in captured.splitlines() if "WARNING" in line]) == 2


def test_call_zero_window_makes_a_single_attempt(fake_clock):
    client = make_client(fake_clock, retry_window=0.0)
    rpc = FakeRpc(FakeRpcError(grpc.StatusCode.UNAVAILABLE))

    with pytest.raises(ClientConnectionError):
        client._call("complete", rpc, "req", CommandKind.CHILD)

    assert len(rpc.calls) == 1
    assert fake_clock.slept == []


# -- _call: status code mapping ---------------------------------------


@pytest.mark.parametrize(
    "code, expected",
    [
        (grpc.StatusCode.INVALID_ARGUMENT, InvalidRequestError),
        (grpc.StatusCode.NOT_FOUND, NodeNotFoundError),
        (grpc.StatusCode.PERMISSION_DENIED, PermissionDeniedError),
        (grpc.StatusCode.UNAUTHENTICATED, PermissionDeniedError),
    ],
)
def test_call_maps_non_retryable_status_without_retry(fake_clock, code, expected):
    client = make_client(fake_clock)
    rpc = FakeRpc(FakeRpcError(code, details="bad request"))

    with pytest.raises(expected) as excinfo:
        client._call("complete", rpc, "req", CommandKind.CHILD)

    assert len(rpc.calls) == 1
    assert fake_clock.slept == []
    text = str(excinfo.value)
    assert code.name in text
    assert "localhost:33083" in text
    assert "bad request" in text


def test_call_maps_unclassified_status_to_transport_error(fake_clock):
    client = make_client(fake_clock)
    rpc = FakeRpc(FakeRpcError(grpc.StatusCode.INTERNAL))

    with pytest.raises(TransportError) as excinfo:
        client._call("complete", rpc, "req", CommandKind.CHILD)

    assert not isinstance(excinfo.value, ClientConnectionError)
    assert len(rpc.calls) == 1
    assert "INTERNAL" in str(excinfo.value)


# -- channel lifecycle -----------------------------------------------


def test_close_channel_is_idempotent_without_channel():
    client = TaklerServiceClient(host="h", port=1)
    assert client.channel is None

    client.close_channel()
    client.close_channel()

    assert client.channel is None
    assert client.stub is None


def test_close_channel_clears_channel_and_stub():
    client = TaklerServiceClient(host="h", port=1)
    channel = FakeChannel()
    client.channel = channel
    client.stub = object()

    client.close_channel()

    assert channel.closed == 1
    assert client.channel is None
    assert client.stub is None


def test_guarded_closes_channel_when_body_raises(monkeypatch):
    client = TaklerServiceClient(host="h", port=1)
    channel = FakeChannel()
    monkeypatch.setattr("grpc.insecure_channel", lambda address: channel)

    def body():
        assert client.channel is channel
        raise NodeNotFoundError("no such node")

    with pytest.raises(NodeNotFoundError):
        client._guarded(body)

    assert channel.closed == 1
    assert client.channel is None
    assert client.stub is None


# -- run_request_show ------------------------------------------------


class ShowClient(TaklerServiceClient):
    """A client whose ``show`` RPC returns a canned output."""

    def __init__(self, output: str):
        super().__init__(host="localhost", port=33083, retry_window=0.0)
        self.stub = type(
            "S", (), {"RunRequestShow": FakeRpc(FakeResponse(flag=0, output=output))}
        )()

    def run_show(self, **kwargs):
        options = dict(
            show_trigger=False,
            show_parameter=False,
            show_limit=False,
            show_event=False,
            show_meter=False,
        )
        options.update(kwargs)
        return self.run_request_show(**options)


def test_show_error_prefix_raises_server_response_error():
    output = "error: no such flow"
    client = ShowClient(output)

    with pytest.raises(ServerResponseError) as excinfo:
        client.run_show()

    assert output in str(excinfo.value)


def test_show_invalid_json_raises_with_first_200_characters():
    output = "x" * 500
    client = ShowClient(output)

    with pytest.raises(ServerResponseError) as excinfo:
        client.run_show()

    text = str(excinfo.value)
    assert output[:200] in text
    assert output not in text


def test_show_valid_json_prints_node_tree(capsys):
    flow = Flow("flow1")
    container = flow.add_container("container1")
    container.add_task("task1")
    bunch = Bunch()
    bunch.add_flow(flow)
    client = ShowClient(json.dumps(bunch.to_dict()))

    client.run_show()

    printed = capsys.readouterr().out
    assert "flow1" in printed
    assert "container1" in printed
    assert "task1" in printed


def test_command_prints_error_classification_name(fake_clock, capsys):
    client = make_client(fake_clock, retry_window=0.0)
    client.stub = type(
        "S",
        (),
        {"RunCommandComplete": FakeRpc(FakeResponse(flag=10, message="no such node"))},
    )()

    response = client.run_command_complete(node_path="/flow1/task1")

    assert response.flag == 10
    assert "node_not_found" in capsys.readouterr().out
