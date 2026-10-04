import pytest
from pydantic import ValidationError

from takler.core import Parameter, parameter, Bunch
from takler.constant import DEFAULT_HOST, DEFAULT_PORT
from takler.core.bunch import ServerConfig
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
# ServerConfig
# ----------------------


def test_server_config_create_default():
    config = ServerConfig()
    assert config.host == DEFAULT_HOST
    assert config.port == DEFAULT_PORT


def test_server_config_create_with_options():
    config = ServerConfig(host="192.168.1.2", port="8080")
    assert config.host == "192.168.1.2"
    assert config.port == "8080"


@pytest.mark.parametrize(
    "host, port",
    [
        (12345, "8080"),
        ("192.168.1.2", 8080),
        (127, 8080),
    ],
)
def test_server_config_create_with_invalid_type(host, port):
    with pytest.raises(ValidationError):
        ServerConfig(host=host, port=port)


def test_server_config_set_host():
    config = ServerConfig()
    config.host = "192.168.1.2"
    assert config.host == "192.168.1.2"

    config.host = None
    assert config.host == DEFAULT_HOST

    with pytest.raises(ValidationError):
        config.host = 12345


def test_server_config_set_port():
    config = ServerConfig()
    config.port = "8080"
    assert config.port == "8080"

    config.port = None
    assert config.port == DEFAULT_PORT

    with pytest.raises(ValidationError):
        config.port = 8080


def test_server_config_generated_parameters():
    config = ServerConfig()
    assert config.generated_parameters() == {
        parameter.TAKLER_HOST: Parameter(parameter.TAKLER_HOST, config.host),
        parameter.TAKLER_PORT: Parameter(parameter.TAKLER_PORT, config.port),
        parameter.TAKLER_HOME: Parameter(parameter.TAKLER_HOME, "."),
    }


def test_server_config_find_parameter():
    config = ServerConfig(host="192.168.1.2", port="8080")
    assert config.find_parameter(parameter.TAKLER_HOST) == Parameter(
        parameter.TAKLER_HOST, config.host
    )
    assert config.find_parameter(parameter.TAKLER_PORT) == Parameter(
        parameter.TAKLER_PORT, config.port
    )
    assert config.find_parameter(parameter.TAKLER_HOME) == Parameter(
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
