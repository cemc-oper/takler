import pytest
from pydantic import ValidationError

from takler.core import Bunch, HaltCause, ServerStatus
from takler.core.bunch import ServerConfig, ServerState
from takler.core.state import NodeStatus
from takler.exceptions import InvalidRequestError


def test_config_and_state_round_trip_independently():
    config = ServerConfig(host="login_b01", port="31071")
    state = ServerState(
        status=ServerStatus.halted,
        status_reason="manual pause",
        halt_causes=[HaltCause.manual],
    )

    assert ServerConfig.from_dict(config.to_dict()) == config
    assert ServerState.from_dict(state.to_dict()) == state
    assert config.to_dict() == {"host": "login_b01", "port": "31071"}
    assert state.to_dict() == {
        "status": "halted",
        "status_reason": "manual pause",
        "halt_causes": ["manual"],
    }


def test_bunch_config_and_state_are_not_shared():
    first = Bunch(host="login01", port="33083")
    second = Bunch(host="login02", port="44084")
    first.server_config.host = "changed"
    first.server_state = ServerState(
        status=ServerStatus.halted,
        status_reason="manual pause",
        halt_causes=[HaltCause.manual],
    )

    assert second.server_config.host == "login02"
    assert second.server_state.status is ServerStatus.running
    assert second.server_state.halt_causes == []
    assert first.get_node_status() is NodeStatus.unknown


@pytest.mark.parametrize("status", ["unknown", "complete", 1, None])
def test_unknown_service_status_is_rejected(status):
    with pytest.raises(ValidationError):
        ServerState.from_dict(
            {"status": status, "status_reason": None, "halt_causes": []}
        )


@pytest.mark.parametrize(
    "data",
    [
        {"status": "running", "status_reason": "stale", "halt_causes": []},
        {"status": "halted", "status_reason": None, "halt_causes": ["manual"]},
        {"status": "halted", "status_reason": "paused", "halt_causes": []},
        {
            "status": "halted",
            "status_reason": "paused",
            "halt_causes": ["manual", "manual"],
        },
        {"status": "halted", "status_reason": "paused", "halt_causes": ["bad"]},
    ],
)
def test_inconsistent_server_state_is_rejected(data):
    with pytest.raises(ValidationError):
        ServerState.from_dict(data)


def test_missing_service_state_fields_are_rejected():
    with pytest.raises(InvalidRequestError):
        ServerState.from_dict({"status": "running"})


def test_server_status_is_not_a_node_status():
    assert not isinstance(ServerStatus.running, NodeStatus)
    with pytest.raises(TypeError):
        _ = ServerStatus.running < ServerStatus.halted
