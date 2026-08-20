import pytest
from pydantic import ValidationError

from takler.core import Parameter, parameter, Bunch
from takler.constant import DEFAULT_HOST, DEFAULT_PORT
from takler.core.bunch import ServerState
from .conftest import FlowWithParameter


class BunchWithParameter(FlowWithParameter):
    bunch1: Bunch


@pytest.fixture
def bunch_with_parameter(flow_with_parameter) -> BunchWithParameter:
    flow1 = flow_with_parameter.flow1

    bunch = Bunch("bunch1")
    bunch.add_flow(flow1)

    bunch_with_parameter = BunchWithParameter(
        **flow_with_parameter.model_dump(), bunch1=bunch
    )

    return bunch_with_parameter


# ----------------------
# ServerState
# ----------------------


def test_server_state_create_default():
    server_state = ServerState()
    assert server_state.host == DEFAULT_HOST
    assert server_state.port == DEFAULT_PORT
    assert server_state.server_parameters == []


def test_server_state_create_with_options():
    server_state = ServerState(host="192.168.1.2", port="8080")
    assert server_state.host == "192.168.1.2"
    assert server_state.port == "8080"
    assert server_state.server_parameters == []


@pytest.mark.parametrize(
    "host, port",
    [
        (12345, "8080"),
        ("192.168.1.2", 8080),
        (127, 8080),
    ],
)
def test_server_state_create_with_invalid_type(host, port):
    with pytest.raises(ValidationError):
        ServerState(host=host, port=port)


def test_server_state_set_host():
    server_state = ServerState()
    server_state.host = "192.168.1.2"
    assert server_state.host == "192.168.1.2"

    server_state.host = None
    assert server_state.host == DEFAULT_HOST

    with pytest.raises(ValidationError):
        server_state.host = 12345


def test_server_state_set_port():
    server_state = ServerState()
    server_state.port = "8080"
    assert server_state.port == "8080"

    server_state.port = None
    assert server_state.port == DEFAULT_PORT

    with pytest.raises(ValidationError):
        server_state.port = 8080


def test_server_state_setup():
    server_state = ServerState()
    server_state.setup()

    assert len(server_state.server_parameters) == 3
    assert server_state.server_parameters[0] == Parameter(
        parameter.TAKLER_HOST, server_state.host
    )
    assert server_state.server_parameters[1] == Parameter(
        parameter.TAKLER_PORT, server_state.port
    )
    assert server_state.server_parameters[2] == Parameter(parameter.TAKLER_HOME, ".")


def test_server_state_find_parameter():
    server_state = ServerState(host="192.168.1.2", port="8080")
    server_state.setup()
    assert server_state.find_parameter(parameter.TAKLER_HOST) == Parameter(
        parameter.TAKLER_HOST, server_state.host
    )
    assert server_state.find_parameter(parameter.TAKLER_PORT) == Parameter(
        parameter.TAKLER_PORT, server_state.port
    )
    assert server_state.find_parameter(parameter.TAKLER_HOME) == Parameter(
        parameter.TAKLER_HOME, "."
    )


# ----------------------------
# Bunch
# ----------------------------


def test_bunch_generated_parameter(bunch_with_parameter):
    bunch = bunch_with_parameter.bunch1
    assert bunch.find_generated_parameter(parameter.TAKLER_HOST) == Parameter(
        parameter.TAKLER_HOST, "localhost"
    )
    assert bunch.find_generated_parameter(parameter.TAKLER_PORT) == Parameter(
        parameter.TAKLER_PORT, "33083"
    )
    assert bunch.find_parent_parameter(parameter.TAKLER_HOME) == Parameter(
        parameter.TAKLER_HOME, "."
    )

    assert bunch.find_parent_parameter("NOT_EXIST") is None


def test_task_find_bunch_parameter(bunch_with_parameter):
    task1 = bunch_with_parameter.task1
    assert task1.find_parent_parameter(parameter.TAKLER_PORT) == Parameter(
        parameter.TAKLER_PORT, "33083"
    )
    assert task1.find_parameter(parameter.TAKLER_PORT) is None


def test_container_find_bunch_parameter(bunch_with_parameter):
    container1 = bunch_with_parameter.container1
    assert container1.find_parent_parameter(parameter.TAKLER_HOME) == Parameter(
        parameter.TAKLER_HOME, "."
    )
    assert container1.find_parameter(parameter.TAKLER_HOME) is None


def test_flow_find_bunch_parameter(bunch_with_parameter):
    flow1 = bunch_with_parameter.flow1
    assert flow1.find_parent_parameter(parameter.TAKLER_HOME) == Parameter(
        parameter.TAKLER_HOME, "."
    )
    assert flow1.find_parameter(parameter.TAKLER_HOME) is None
