from takler.core import Parameter


def test_int_parameter_to_dict():
    param = Parameter("int_param", 1)
    assert param.to_dict() == dict(name="int_param", value=1)


def test_int_parameter_from_dict():
    d = dict(name="int_param", value=1)
    assert Parameter.from_dict(d) == Parameter("int_param", 1)


def test_str_parameter_to_dict():
    param = Parameter("str_param", "arrived")
    assert param.to_dict() == dict(name="str_param", value="arrived")


def test_str_parameter_from_dict():
    d = dict(name="str_param", value="arrived")
    assert Parameter.from_dict(d) == Parameter(name="str_param", value="arrived")


def test_float_parameter_to_dict():
    param = Parameter("float_param", 0.25)
    assert param.to_dict() == dict(name="float_param", value=0.25)


def test_float_parameter_from_dict():
    d = dict(name="float_param", value=0.25)
    assert Parameter.from_dict(d) == Parameter(name="float_param", value=0.25)


def test_bool_parameter_to_dict():
    param = Parameter("bool_param", True)
    assert param.to_dict() == dict(name="bool_param", value=True)


def test_bool_parameter_from_dict():
    d = dict(
        name="bool_param",
        value=True,
    )
    assert Parameter.from_dict(d) == Parameter(
        name="bool_param",
        value=True,
    )
