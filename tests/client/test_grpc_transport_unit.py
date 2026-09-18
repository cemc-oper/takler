"""Unit tests for the client-side ``GrpcTransport.call`` path.

The Call_Wrapper (``_call``: timeout, retry, status-code mapping) is covered
in ``tests/client/test_service_client_unit.py`` and the credential wiring in
``tests/client/test_credential_injection.py``; what this file pins is the
part ``call`` adds on top of both: the payload is encoded into the pb2
request of the command, the stub method named by the codec's method table is
the one invoked, the CommandKind is derived from the command rather than
passed by the caller, and the pb2 response comes back as the command's
response DTO.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, Tuple

import pytest

from takler.client.grpc_transport import GrpcTransport
from takler.client.retry import CommandKind
from takler.protocol.commands import (
    RESPONSE_TYPE_BY_COMMAND,
    Command,
    PingResponse,
    ServiceResponse,
    ShowResponse,
)
from takler.server.protocol import adapter, takler_pb2
from takler.server.protocol.adapter import GRPC_METHOD_BY_COMMAND

#: Command -> (canonical payload, pb2 request type). Mirrors the table in
#: ``tests/server/test_grpc_codec.py``; both are literals so a field renamed
#: on one side fails here rather than silently agreeing.
PAYLOAD_AND_TYPE_BY_COMMAND: Dict[Command, Tuple[Dict[str, Any], Any]] = {
    Command.INIT: (
        {"node_path": "/flow1/task1", "task_id": "12345"},
        takler_pb2.InitCommand,
    ),
    Command.COMPLETE: (
        {"node_path": "/flow1/task1"},
        takler_pb2.CompleteCommand,
    ),
    Command.ABORT: (
        {"node_path": "/flow1/task1", "reason": "boom"},
        takler_pb2.AbortCommand,
    ),
    Command.EVENT: (
        {"node_path": "/flow1/task1", "event_name": "event1"},
        takler_pb2.EventCommand,
    ),
    Command.METER: (
        {"node_path": "/flow1/task1", "meter_name": "meter1", "meter_value": "50"},
        takler_pb2.MeterCommand,
    ),
    Command.REQUEUE: ({"node_paths": ["/flow1"]}, takler_pb2.RequeueCommand),
    Command.SUSPEND: ({"node_paths": ["/flow1"]}, takler_pb2.SuspendCommand),
    Command.RESUME: ({"node_paths": ["/flow1"]}, takler_pb2.ResumeCommand),
    Command.RUN: (
        {"node_paths": ["/flow1/task1"], "force": True},
        takler_pb2.RunCommand,
    ),
    Command.FORCE: (
        {"paths": ["/flow1/task1"], "state": "queued", "recursive": True},
        takler_pb2.ForceCommand,
    ),
    Command.FREE_DEP: (
        {"paths": ["/flow1/task1"], "dep_type": "all"},
        takler_pb2.FreeDepCommand,
    ),
    Command.LOAD: (
        {"flow_type": "json", "flow_bytes": b"{}"},
        takler_pb2.LoadCommand,
    ),
    Command.BEGIN: ({"flow_name": "flow1", "force": False}, takler_pb2.BeginCommand),
    Command.SHOW: (
        {
            "show_trigger": True,
            "show_parameter": True,
            "show_limit": True,
            "show_event": True,
            "show_meter": True,
        },
        takler_pb2.ShowRequest,
    ),
    Command.PING: ({}, takler_pb2.PingRequest),
    Command.COROUTINE: ({}, takler_pb2.CoroutineRequest),
}


class RecordingStub:
    """A stand-in ``TaklerServerStub`` recording every invocation."""

    def __init__(self):
        self.invocations = []

    def __getattr__(self, name: str):
        def rpc(request, timeout=None, metadata=None):
            self.invocations.append((name, request))
            # One namespace shaped like every response message, so the
            # response decoding of any command finds its fields.
            return SimpleNamespace(flag=0, message="", output="", coroutines=[])

        return rpc


def make_transport() -> GrpcTransport:
    """A transport with no retrying, so one logical call is one attempt."""
    transport = GrpcTransport(host="localhost", port=33083, retry_window=0.0)
    transport.stub = RecordingStub()
    return transport


@pytest.mark.parametrize("command", sorted(PAYLOAD_AND_TYPE_BY_COMMAND, key=str))
def test_call_encodes_the_payload_and_invokes_the_method_of_the_command(
    command: Command,
) -> None:
    payload, request_type = PAYLOAD_AND_TYPE_BY_COMMAND[command]
    transport = make_transport()

    response = transport.call(command, payload)

    assert len(transport.stub.invocations) == 1
    method_name, request = transport.stub.invocations[0]
    assert method_name == GRPC_METHOD_BY_COMMAND[command]
    assert isinstance(request, request_type)
    assert isinstance(response, RESPONSE_TYPE_BY_COMMAND[command])
    if isinstance(response, ServiceResponse):
        assert response.flag == 0


@pytest.mark.parametrize(
    "command, expected",
    [
        (Command.COMPLETE, CommandKind.CHILD),
        (Command.REQUEUE, CommandKind.CONTROL),
        (Command.PING, CommandKind.QUERY),
    ],
)
def test_call_derives_the_command_kind_from_the_command(
    command: Command, expected: CommandKind, monkeypatch
) -> None:
    """The retry classification is the transport's business, not the call
    site's: ``call`` looks it up from the command."""
    transport = make_transport()
    seen = []
    real_call = transport._call

    def spy(operation_name, rpc, request, kind):
        seen.append((operation_name, kind))
        return real_call(operation_name, rpc, request, kind)

    monkeypatch.setattr(transport, "_call", spy)

    transport.call(command, PAYLOAD_AND_TYPE_BY_COMMAND[command][0])

    assert seen == [(command.value, expected)]


def test_call_decodes_the_query_response_types() -> None:
    transport = make_transport()
    transport.stub = SimpleNamespace(
        RunRequestShow=lambda request, timeout=None, metadata=None: (
            takler_pb2.ShowResponse(output="{}")
        ),
        RunRequestPing=lambda request, timeout=None, metadata=None: (
            takler_pb2.PingResponse()
        ),
    )

    show = transport.call(Command.SHOW, PAYLOAD_AND_TYPE_BY_COMMAND[Command.SHOW][0])
    ping = transport.call(Command.PING, {})

    assert isinstance(show, ShowResponse)
    assert show.output == "{}"
    assert isinstance(ping, PingResponse)


def test_open_and_close_drive_the_channel_lifecycle(monkeypatch) -> None:
    """``open`` creates channel and stub, ``close`` releases both and is safe
    to repeat (requirement 11.4)."""
    created = []

    class FakeChannel:
        def __init__(self):
            self.closed = 0

        def unary_unary(self, *args, **kwargs):
            return lambda request, timeout=None, metadata=None: (
                takler_pb2.ServiceResponse(flag=0)
            )

        def close(self):
            self.closed += 1

    monkeypatch.setattr(
        "grpc.insecure_channel",
        lambda address: created.append(address) or FakeChannel(),
    )
    transport = GrpcTransport(host="localhost", port=33083)

    transport.open()

    assert created == ["localhost:33083"]
    assert transport.channel is not None
    assert transport.stub is not None

    transport.close()
    transport.close()

    assert transport.channel is None
    assert transport.stub is None


def test_transport_implements_the_client_transport_protocol() -> None:
    transport = make_transport()
    for method in ("open", "close", "call"):
        assert callable(getattr(transport, method))


def test_payload_tables_cover_every_command() -> None:
    assert set(PAYLOAD_AND_TYPE_BY_COMMAND) == set(Command)
    assert set(GRPC_METHOD_BY_COMMAND) == set(Command)
    assert set(adapter.GRPC_METHOD_BY_COMMAND) == set(Command)
