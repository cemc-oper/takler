"""Contract tests for the client direction of the gRPC codec.

``takler.server.protocol.adapter`` converts both ways: server inbound
(``request_from_pb2`` / ``response_to_pb2``, covered by the gRPC boundary
tests in ``tests/server/test_handlers_grpc_boundary.py``) and client outbound
(``request_to_pb2`` / ``response_from_pb2``), which is what this file pins.

The properties that matter:

* every one of the sixteen commands has a converter in both directions and a
  gRPC method name, and the method names agree with both the ``takler.proto``
  service descriptor and the handler layer's canonical operation names;
* ``request_to_pb2`` puts every payload field into the right proto field --
  the flattened ``ChildCommandOptions`` wrapper rebuilt, the enum *names*
  resolved to wire numbers, the repeated path fields re-pluralized;
* ``request_to_pb2`` does not validate: a non-numeric ``meter_value`` crosses
  the wire untouched, because classifying it is the server's answer and both
  clients must answer it the same way;
* ``response_from_pb2`` builds the response DTO of the command's response
  type.
"""

from __future__ import annotations

import pytest

from takler.protocol.commands import (
    Command,
    CoroutineResponse,
    PingResponse,
    ServiceResponse,
    ShowResponse,
)
from takler.server.handlers import METHOD_NAME_BY_COMMAND
from takler.server.protocol import adapter, takler_pb2


#: The canonical payload of every command: every field the command's request
#: DTO declares, in wire shape (enum fields as names, ``meter_value`` as the
#: string the caller passed).
PAYLOAD_BY_COMMAND = {
    Command.INIT: {"node_path": "/flow1/task1", "task_id": "12345"},
    Command.COMPLETE: {"node_path": "/flow1/task1"},
    Command.ABORT: {"node_path": "/flow1/task1", "reason": "boom"},
    Command.EVENT: {"node_path": "/flow1/task1", "event_name": "event1"},
    Command.METER: {
        "node_path": "/flow1/task1",
        "meter_name": "meter1",
        "meter_value": "50",
    },
    Command.REQUEUE: {"node_paths": ["/flow1/task1", "/flow1/task2"]},
    Command.SUSPEND: {"node_paths": ["/flow1/task1"]},
    Command.RESUME: {"node_paths": ["/flow1"]},
    Command.RUN: {"node_paths": ["/flow1/task1"], "force": True},
    Command.FORCE: {
        "paths": ["/flow1/task1"],
        "state": "queued",
        "recursive": False,
    },
    Command.FREE_DEP: {"paths": ["/flow1/task1"], "dep_type": "time"},
    Command.LOAD: {"flow_type": "json", "flow_bytes": b'{"flow1": {}}'},
    Command.BEGIN: {"flow_name": "flow1", "force": True},
    Command.SHOW: {
        "show_trigger": True,
        "show_parameter": False,
        "show_limit": True,
        "show_event": False,
        "show_meter": True,
    },
    Command.PING: {},
    Command.COROUTINE: {},
}


# -- coverage of the tables ---------------------------------------------------


def test_every_command_has_a_client_direction_converter_and_a_method_name():
    for command in Command:
        assert command in PAYLOAD_BY_COMMAND, command
        assert command in adapter.GRPC_METHOD_BY_COMMAND, command
        message = adapter.request_to_pb2(command, PAYLOAD_BY_COMMAND[command])
        assert message is not None


def test_method_names_match_the_service_descriptor_and_the_handler_table():
    descriptor_names = {
        method.name
        for method in takler_pb2.DESCRIPTOR.services_by_name["TaklerServer"].methods
    }
    assert set(adapter.GRPC_METHOD_BY_COMMAND.values()) == descriptor_names
    assert adapter.GRPC_METHOD_BY_COMMAND == METHOD_NAME_BY_COMMAND


# -- request_to_pb2 -----------------------------------------------------------


def test_request_to_pb2_rebuilds_the_child_options_wrapper():
    message = adapter.request_to_pb2(
        Command.INIT, {"node_path": "/flow1/task1", "task_id": "12345"}
    )

    assert message.child_options.node_path == "/flow1/task1"
    assert message.task_id == "12345"


def test_request_to_pb2_passes_a_non_numeric_meter_value_untouched():
    """No validation on the client side of the wire: ``abc`` travels verbatim
    and the server classifies it, exactly as when the client built the pb2
    message itself."""
    message = adapter.request_to_pb2(
        Command.METER,
        {"node_path": "/f/t", "meter_name": "m", "meter_value": "abc"},
    )

    assert message.meter_value == "abc"


def test_request_to_pb2_resolves_enum_names_to_wire_numbers():
    message = adapter.request_to_pb2(
        Command.FORCE,
        {"paths": ["/flow1/task1"], "state": "queued", "recursive": False},
    )
    assert message.state == takler_pb2.ForceCommand.ForceState.Value("queued")
    assert list(message.path) == ["/flow1/task1"]
    assert message.recursive is False

    message = adapter.request_to_pb2(
        Command.FREE_DEP, {"paths": ["/flow1"], "dep_type": "time"}
    )
    assert message.dep_type == takler_pb2.FreeDepCommand.DepType.Value("time")


def test_request_to_pb2_rejects_an_unknown_enum_name_like_the_client_did():
    """``Value()`` raises ``ValueError`` for a name the enum does not declare --
    the same failure the client produced when it resolved the name itself."""
    with pytest.raises(ValueError):
        adapter.request_to_pb2(
            Command.FORCE,
            {"paths": ["/flow1/task1"], "state": "bogus", "recursive": True},
        )


def test_request_to_pb2_maps_the_remaining_fields():
    message = adapter.request_to_pb2(
        Command.RUN, {"node_paths": ["/a", "/b"], "force": True}
    )
    assert list(message.node_path) == ["/a", "/b"]
    assert message.force is True

    message = adapter.request_to_pb2(
        Command.LOAD, {"flow_type": "json", "flow_bytes": b"bytes"}
    )
    assert message.flow_type == "json"
    assert message.flow == b"bytes"

    message = adapter.request_to_pb2(
        Command.BEGIN, {"flow_name": "flow1", "force": True}
    )
    assert message.flow_name == "flow1"
    assert message.force is True

    message = adapter.request_to_pb2(
        Command.SHOW,
        {
            "show_trigger": True,
            "show_parameter": False,
            "show_limit": True,
            "show_event": False,
            "show_meter": True,
        },
    )
    assert message.show_trigger is True
    assert message.show_parameter is False
    assert message.show_limit is True
    assert message.show_event is False
    assert message.show_meter is True


@pytest.mark.parametrize("command", sorted(PAYLOAD_BY_COMMAND, key=str))
def test_request_round_trips_through_the_server_direction(command: Command):
    """payload -> pb2 -> DTO lands on the DTO the fields describe.

    The meter payload carries a numeric *string*: the DTO coercion to ``int``
    is the server side's documented behaviour, so the round trip asserts the
    coerced value.
    """
    payload = PAYLOAD_BY_COMMAND[command]
    message = adapter.request_to_pb2(command, payload)
    request = adapter.request_from_pb2(command, message)

    dumped = request.model_dump()
    for key, value in payload.items():
        expected = value
        if command is Command.METER and key == "meter_value":
            expected = int(value)
        if isinstance(expected, (list, tuple)):
            assert list(dumped[key]) == list(value)
        elif hasattr(dumped[key], "value"):  # the ForceState / DepType enums
            assert dumped[key].value == value
        else:
            assert dumped[key] == expected


# -- response_from_pb2 ---------------------------------------------------------


def test_response_from_pb2_builds_a_service_response():
    message = takler_pb2.ServiceResponse(flag=10, message="no such node")

    response = adapter.response_from_pb2(Command.COMPLETE, message)

    assert isinstance(response, ServiceResponse)
    assert response.flag == 10
    assert response.message == "no such node"


def test_response_from_pb2_builds_the_query_responses():
    response = adapter.response_from_pb2(
        Command.SHOW, takler_pb2.ShowResponse(output="{}")
    )
    assert isinstance(response, ShowResponse)
    assert response.output == "{}"

    response = adapter.response_from_pb2(Command.PING, takler_pb2.PingResponse())
    assert isinstance(response, PingResponse)

    response = adapter.response_from_pb2(
        Command.COROUTINE,
        takler_pb2.CoroutineResponse(
            coroutines=[takler_pb2.Coroutine(name="t1", description="<task>")]
        ),
    )
    assert isinstance(response, CoroutineResponse)
    assert [(c.name, c.description) for c in response.coroutines] == [("t1", "<task>")]
