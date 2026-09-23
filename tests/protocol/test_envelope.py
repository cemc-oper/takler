"""Unit tests for the command envelope of ``takler.protocol``.

The envelope is the HTTP wire form and the carrier of the message metadata
(``version`` / ``trace_id`` / ``target`` / ``auth``); these tests pin its
shape, the trace-id rules and the payload (de)serialization round trips.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from takler.protocol import (
    PROTOCOL_VERSION,
    AuthInfo,
    BeginCommand,
    Command,
    CompleteCommand,
    Envelope,
    LoadCommand,
    MeterCommand,
    ServiceResponse,
    BatchResponse,
    ShowResponse,
)


def test_envelope_defaults():
    envelope = Envelope(command=Command.PING, payload={})
    assert envelope.version == PROTOCOL_VERSION
    assert envelope.target is None
    assert envelope.auth is None


def test_trace_id_is_generated_per_envelope():
    """Generated at birth, 32 lowercase hex characters, unique."""
    first = Envelope(command=Command.PING, payload={})
    second = Envelope(command=Command.PING, payload={})
    assert len(first.trace_id) == 32
    int(first.trace_id, 16)  # raises unless it is hex
    assert first.trace_id != second.trace_id


def test_target_is_reserved_but_settable():
    envelope = Envelope(command=Command.PING, payload={}, target="server-b")
    assert envelope.target == "server-b"


def test_auth_carries_the_three_credentials():
    auth = AuthInfo(job_password="pw", secret="s3cret", user="nwp")
    envelope = Envelope(command=Command.COMPLETE, payload={}, auth=auth)
    assert envelope.auth.job_password == "pw"
    assert envelope.auth.secret == "s3cret"
    assert envelope.auth.user == "nwp"
    # Every credential is optional: which ones a call must carry is the
    # privilege layer's decision.
    assert AuthInfo().model_dump() == {
        "job_password": None,
        "secret": None,
        "user": None,
    }


def test_unknown_command_is_rejected_at_envelope_validation():
    with pytest.raises(ValidationError):
        Envelope(command="detonate", payload={})


def test_unknown_envelope_field_is_rejected():
    with pytest.raises(ValidationError):
        Envelope(command=Command.PING, payload={}, trce_id="typo")


def test_for_request_builds_the_payload():
    envelope = Envelope.for_request(
        Command.METER,
        MeterCommand(node_path="/flow1/task1", meter_name="step", meter_value=10),
    )
    assert envelope.command is Command.METER
    assert envelope.payload == {
        "node_path": "/flow1/task1",
        "meter_name": "step",
        "meter_value": 10,
    }


def test_for_request_rejects_a_mismatched_dto():
    with pytest.raises(TypeError):
        Envelope.for_request(Command.METER, CompleteCommand(node_path="/flow1"))


def test_for_response_echoes_the_trace_id():
    request_envelope = Envelope.for_request(
        Command.BEGIN, BeginCommand(flow_name="flow1")
    )
    response_envelope = Envelope.for_response(
        Command.BEGIN,
        BatchResponse(flag=0, message="", results=[]),
        trace_id=request_envelope.trace_id,
    )
    assert response_envelope.trace_id == request_envelope.trace_id


def test_for_response_rejects_a_mismatched_dto():
    with pytest.raises(TypeError):
        Envelope.for_response(Command.SHOW, ServiceResponse(), trace_id="ab")


def test_parse_request_round_trip_with_validation():
    """The wire form of meter carries the value as a string; parse coerces it."""
    envelope = Envelope(
        command=Command.METER,
        payload={
            "node_path": "/flow1/task1",
            "meter_name": "step",
            "meter_value": "42",
        },
    )
    request = envelope.parse_request()
    assert isinstance(request, MeterCommand)
    assert request.meter_value == 42


def test_parse_request_rejects_a_bad_payload():
    envelope = Envelope(
        command=Command.METER,
        payload={"node_path": "/flow1/task1", "meter_name": "step"},
    )
    with pytest.raises(ValidationError):
        envelope.parse_request()


def test_parse_response_round_trip():
    envelope = Envelope.for_response(
        Command.SHOW, ShowResponse(output="{}"), trace_id="ab" * 16
    )
    response = envelope.parse_response()
    assert isinstance(response, ShowResponse)
    assert response.output == "{}"


def test_json_round_trip_with_load_command_bytes():
    """A load request survives model_dump_json -> model_validate_json.

    This is the HTTP transport's round trip: the flow bytes cross as base64
    text inside the JSON payload.
    """
    envelope = Envelope.for_request(
        Command.LOAD, LoadCommand(flow_bytes=b'{"name": "flow1"}')
    )
    assert envelope.payload["flow_bytes"] == "eyJuYW1lIjogImZsb3cxIn0="
    restored = Envelope.model_validate_json(envelope.model_dump_json())
    assert restored == envelope
    request = restored.parse_request()
    assert request.flow_bytes == b'{"name": "flow1"}'
