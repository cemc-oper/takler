from takler.core import Parameter
from takler.core.bunch import ServerState


def test_server_state_to_dict():
    server_state = ServerState(
        server_parameters=[
            Parameter("int_param", 1),
            Parameter("float_param", 0.25),
            Parameter("str_param", "arrived"),
            Parameter("bool_param", True)
        ],
        host="login_b01",
        port="31071",
    )
    assert server_state.to_dict() == dict(
        parameters=[
            dict(name="int_param", value=1),
            dict(name="float_param", value=0.25),
            dict(name="str_param", value="arrived"),
            dict(name="bool_param", value=True)
        ],
        host="login_b01",
        port="31071"
    )


def test_server_state_from_json():
    d = dict(
        parameters=[
            dict(name="int_param", value=1),
            dict(name="float_param", value=0.25),
            dict(name="str_param", value="arrived"),
            dict(name="bool_param", value=True)
        ],
        host="login_b01",
        port="31071"
    )
    expected_server_state = ServerState(
        server_parameters=[
            Parameter("int_param", 1),
            Parameter("float_param", 0.25),
            Parameter("str_param", "arrived"),
            Parameter("bool_param", True)
        ],
        host="login_b01",
        port="31071",
    )

    server_state = ServerState.from_dict(d)
    assert server_state == expected_server_state