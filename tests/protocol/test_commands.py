"""Unit tests for the command DTOs of ``takler.protocol``.

Covers the construction and the validation of every command's request and
response DTO (m3-tasks 任务 3 acceptance: DTO 单测覆盖全部命令的构造与校验).

The literal tables at the top follow the cross-language contract rule: they
are transcribed by hand and never derived from the models they police, so a
wrong edit to a model fails here instead of passing silently. Two tests at
the bottom import ``takler_pb2`` as well -- tests are not subject to the
layering rule of the package under test -- because the pb2 contract table
and the DTO literal table could otherwise be edited *together* and drift
from the DTO enums without anything failing.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from takler.protocol import (
    REQUEST_TYPE_BY_COMMAND,
    RESPONSE_TYPE_BY_COMMAND,
    AbortCommand,
    BeginCommand,
    Command,
    CompleteCommand,
    Coroutine,
    CoroutineRequest,
    CoroutineResponse,
    DepType,
    EventCommand,
    ForceCommand,
    ForceState,
    FreeDepCommand,
    InitCommand,
    LoadCommand,
    MeterCommand,
    PingRequest,
    PingResponse,
    ProtocolModel,
    RequeueCommand,
    ResumeCommand,
    RunCommand,
    ServiceResponse,
    ShowRequest,
    ShowResponse,
    SuspendCommand,
)
from takler.server.protocol import takler_pb2

#: The sixteen command names, transcribed from the CLI surface (which is what
#: the HTTP path ``/v1/commands/{command}`` speaks).
CONTRACT_COMMAND_NAMES = {
    "init",
    "complete",
    "abort",
    "event",
    "meter",
    "requeue",
    "suspend",
    "resume",
    "run",
    "force",
    "free-dep",
    "load",
    "begin",
    "show",
    "ping",
    "coroutine",
}

#: The names ForceState and DepType take, transcribed from the proto enums.
CONTRACT_FORCE_STATE_NAMES = {
    "unknown",
    "complete",
    "queued",
    "submitted",
    "active",
    "aborted",
    "clear",
    "set",
}
CONTRACT_DEP_TYPE_NAMES = {"all", "trigger", "time"}


# ---------------------------------------------------------------------------
# The command surface
# ---------------------------------------------------------------------------


def test_command_names_match_the_contract():
    assert {command.value for command in Command} == CONTRACT_COMMAND_NAMES
    assert len(Command) == 16


def test_command_count_matches_the_rpc_count():
    """A new RPC without a Command member (or vice versa) fails here.

    The pb2 contract test and this file's literal table could be updated
    together and still leave ``Command`` behind; counting against the
    descriptor closes that hole.
    """
    service = takler_pb2.DESCRIPTOR.services_by_name["TaklerServer"]
    assert len(Command) == len(service.methods)


def test_request_type_map_covers_exactly_the_commands():
    assert set(REQUEST_TYPE_BY_COMMAND) == set(Command)
    for request_type in REQUEST_TYPE_BY_COMMAND.values():
        assert issubclass(request_type, ProtocolModel)


def test_response_type_map_covers_exactly_the_commands():
    assert set(RESPONSE_TYPE_BY_COMMAND) == set(Command)
    for response_type in RESPONSE_TYPE_BY_COMMAND.values():
        assert issubclass(response_type, ProtocolModel)


def test_thirteen_commands_answer_service_response():
    queries = {Command.SHOW, Command.PING, Command.COROUTINE}
    for command, response_type in RESPONSE_TYPE_BY_COMMAND.items():
        if command in queries:
            assert response_type is not ServiceResponse
        else:
            assert response_type is ServiceResponse


# ---------------------------------------------------------------------------
# The enums
# ---------------------------------------------------------------------------


def test_force_state_names_match_the_contract():
    assert {state.value for state in ForceState} == CONTRACT_FORCE_STATE_NAMES


def test_dep_type_names_match_the_contract():
    assert {dep_type.value for dep_type in DepType} == CONTRACT_DEP_TYPE_NAMES


def test_force_state_names_match_pb2():
    """The DTO enum and the proto enum must name the same states.

    Both are pinned against literals elsewhere, but those two pins could be
    edited together; this closes the triangle.
    """
    enum = takler_pb2.ForceCommand.DESCRIPTOR.enum_types_by_name["ForceState"]
    assert {state.value for state in ForceState} == {
        value.name for value in enum.values
    }


def test_dep_type_names_match_pb2():
    enum = takler_pb2.FreeDepCommand.DESCRIPTOR.enum_types_by_name["DepType"]
    assert {dep_type.value for dep_type in DepType} == {
        value.name for value in enum.values
    }


# ---------------------------------------------------------------------------
# Request construction, one case per command
# ---------------------------------------------------------------------------


def test_init_command():
    request = InitCommand(node_path="/flow1/task1", task_id="12345")
    assert request.node_path == "/flow1/task1"
    assert request.task_id == "12345"


def test_complete_command():
    request = CompleteCommand(node_path="/flow1/task1")
    assert request.node_path == "/flow1/task1"


def test_abort_command():
    request = AbortCommand(node_path="/flow1/task1", reason="job failed")
    assert request.reason == "job failed"
    # The reason is optional on the wire.
    assert AbortCommand(node_path="/flow1/task1").reason == ""


def test_event_command():
    request = EventCommand(node_path="/flow1/task1", event_name="ready")
    assert request.event_name == "ready"


def test_meter_command_coerces_the_string_value():
    """The str -> int conversion moved here from the scheduler."""
    request = MeterCommand(
        node_path="/flow1/task1", meter_name="progress", meter_value="50"
    )
    assert request.meter_value == 50
    # A negative value is legal: the range check is the meter's, not the DTO's.
    request = MeterCommand(
        node_path="/flow1/task1", meter_name="progress", meter_value="-3"
    )
    assert request.meter_value == -3


def test_requeue_suspend_resume_commands():
    for cls in (RequeueCommand, SuspendCommand, ResumeCommand):
        request = cls(node_paths=["/flow1", "/flow2/task1"])
        assert request.node_paths == ["/flow1", "/flow2/task1"]


def test_run_command():
    request = RunCommand(node_paths=["/flow1/task1"], force=True)
    assert request.force is True
    assert RunCommand(node_paths=["/flow1/task1"]).force is False


def test_force_command():
    request = ForceCommand(paths=["/flow1/task1"], state="complete")
    # The state name leaves the DTO as the enum member.
    assert request.state is ForceState.COMPLETE
    assert request.recursive is True
    request = ForceCommand(
        paths=["/flow1/task1:ready"], state=ForceState.CLEAR, recursive=False
    )
    assert request.state is ForceState.CLEAR
    assert request.recursive is False


def test_free_dep_command():
    request = FreeDepCommand(paths=["/flow1/task1"])
    assert request.dep_type is DepType.ALL
    request = FreeDepCommand(paths=["/flow1/task1"], dep_type="time")
    assert request.dep_type is DepType.TIME


def test_load_command():
    request = LoadCommand(flow_bytes=b'{"name": "flow1"}')
    assert request.flow_type == "json"
    assert request.flow_bytes == b'{"name": "flow1"}'


def test_load_command_json_form_is_base64():
    """In the JSON envelope the flow bytes travel base64-encoded."""
    request = LoadCommand(flow_bytes=b'{"name": "flow1"}')
    dumped = request.model_dump(mode="json")
    assert dumped["flow_bytes"] == "eyJuYW1lIjogImZsb3cxIn0="
    assert LoadCommand.model_validate(dumped) == request


def test_begin_command():
    # No flow name is the "all flows" form of the protocol.
    request = BeginCommand()
    assert request.flow_name == ""
    assert request.force is False
    request = BeginCommand(flow_name="flow1", force=True)
    assert request.flow_name == "flow1"
    assert request.force is True


def test_show_request_defaults_match_the_python_cli():
    request = ShowRequest()
    assert request.show_trigger is False
    assert request.show_parameter is False
    assert request.show_limit is True
    assert request.show_event is True
    assert request.show_meter is True


def test_ping_and_coroutine_requests_carry_nothing():
    assert PingRequest().model_dump() == {}
    assert CoroutineRequest().model_dump() == {}


# ---------------------------------------------------------------------------
# Response construction
# ---------------------------------------------------------------------------


def test_service_response_defaults_to_success():
    response = ServiceResponse()
    assert response.flag == 0
    assert response.message == ""
    response = ServiceResponse(flag=10, message="node is not found: /flow1")
    assert response.flag == 10


def test_show_response():
    assert ShowResponse(output="{}").output == "{}"
    assert ShowResponse().output == ""


def test_ping_response_carries_nothing():
    assert PingResponse().model_dump() == {}


def test_coroutine_response():
    response = CoroutineResponse(
        coroutines=[Coroutine(name="main", description="<coroutine object>")]
    )
    assert response.coroutines[0].name == "main"
    assert CoroutineResponse().coroutines == []


# ---------------------------------------------------------------------------
# Validation failures
# ---------------------------------------------------------------------------


def test_meter_command_rejects_a_non_numeric_value():
    with pytest.raises(ValidationError):
        MeterCommand(node_path="/flow1/task1", meter_name="progress", meter_value="abc")
    # A float string is not an int, even a parseable one.
    with pytest.raises(ValidationError):
        MeterCommand(
            node_path="/flow1/task1", meter_name="progress", meter_value="3.14"
        )


def test_force_command_rejects_an_unknown_state():
    with pytest.raises(ValidationError):
        ForceCommand(paths=["/flow1/task1"], state="bogus")


def test_free_dep_command_rejects_an_unknown_dep_type():
    with pytest.raises(ValidationError):
        FreeDepCommand(paths=["/flow1/task1"], dep_type="bogus")


def test_missing_required_field_is_a_validation_error():
    with pytest.raises(ValidationError):
        InitCommand(node_path="/flow1/task1")


def test_unknown_field_is_a_validation_error():
    """``extra="forbid"``: a misspelled field fails loudly, not silently."""
    with pytest.raises(ValidationError):
        CompleteCommand(node_path="/flow1/task1", nodepath="/flow1/task1")
