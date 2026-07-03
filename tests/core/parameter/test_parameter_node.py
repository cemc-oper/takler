import pytest

from takler.core import Parameter
from takler.core.node import Node


def test_add_parameter_with_single_value():
    node = Node("node1")

    param = node.add_parameter("ECF_HOME", "/home/johndoe")
    assert param.name == "ECF_HOME"
    assert param.value == "/home/johndoe"
    assert param == Parameter(name="ECF_HOME", value="/home/johndoe")
    assert len(node.user_parameters) == 1
    assert node.user_parameters["ECF_HOME"] == Parameter(name="ECF_HOME", value="/home/johndoe")

    node.add_parameter("NODES", 4)
    assert node.user_parameters["NODES"] == Parameter(name="NODES", value=4)

    node.add_parameter("TIME_INTERVAL", 0.1)
    assert node.user_parameters["TIME_INTERVAL"] == Parameter(name="TIME_INTERVAL", value=0.1)

    node.add_parameter("FLAG_UPLOAD", True)
    assert node.user_parameters["FLAG_UPLOAD"] == Parameter(name="FLAG_UPLOAD", value=True)


def test_add_parameter_with_invalid_value_type():
    node = Node("node1")
    with pytest.raises(TypeError):
        node.add_parameter("TYPHOON_ID", None)

    with pytest.raises(TypeError):
        node.add_parameter("TYPHOON_INFO", dict(typhoon_id="W2501", typhoon_name="TYPH1"))

    with pytest.raises(TypeError):
        node.add_parameter("TYPHOON_INFO", ["W2501", "TYPH1"])


def test_add_parameter_dict():
    node = Node("node1")
    node.add_parameter(
        {
            "ECF_HOME": "/home/johndoe",
            "NODES": 4,
            "TIME_INTERVAL": 0.1,
            "FLAG_UPLOAD": True,
        }
    )

    assert len(node.user_parameters) == 4
    assert node.user_parameters["ECF_HOME"] == Parameter(name ="ECF_HOME", value="/home/johndoe")
    assert node.user_parameters["NODES"] == Parameter(name="NODES", value=4)
    assert node.user_parameters["TIME_INTERVAL"] == Parameter(name="TIME_INTERVAL", value=0.1)
    assert node.user_parameters["FLAG_UPLOAD"] == Parameter(name="FLAG_UPLOAD", value=True)


def test_add_parameter_dict_with_value():
    node = Node("node1")
    with pytest.raises(TypeError):
        node.add_parameter(
            {
                "ECF_HOME": "/home/johndoe",
                "NODES": 4,
                "TIME_INTERVAL": 0.1,
                "FLAG_UPLOAD": True,
            },
            "value",
        )


def test_add_parameter_list():
    node = Node("node1")
    node.add_parameter(
        [
            Parameter(name="ECF_HOME", value="/home/johndoe"),
            Parameter(name="NODES", value=4),
            Parameter(name="TIME_INTERVAL", value=0.1),
            Parameter(name="FLAG_UPLOAD", value=True),
        ]
    )

    assert len(node.user_parameters) == 4
    assert node.user_parameters["ECF_HOME"] == Parameter(name="ECF_HOME", value="/home/johndoe")
    assert node.user_parameters["NODES"] == Parameter(name="NODES", value=4)
    assert node.user_parameters["TIME_INTERVAL"] == Parameter(name="TIME_INTERVAL", value=0.1)
    assert node.user_parameters["FLAG_UPLOAD"] == Parameter(name="FLAG_UPLOAD", value=True)


def test_add_parameter_list_with_value():
    node = Node("node1")

    with pytest.raises(TypeError):
        node.add_parameter(
            [
                Parameter(name="ECF_HOME", value="/home/johndoe"),
                Parameter(name="NODES", value=4),
                Parameter(name="TIME_INTERVAL", value=0.1),
                Parameter(name="FLAG_UPLOAD", value=True),
            ],
            "value",
        )
