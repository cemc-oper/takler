import pytest

from takler.core import Parameter


def test_parameter_create_str():
    p = Parameter("OBS_SOURCE", "CMADAAS")
    assert p.name == "OBS_SOURCE"
    assert p.value == "CMADAAS"


def test_parameter_create_parameter():
    p = Parameter("FORECAST_LENGTH", 24)
    assert p.name == "FORECAST_LENGTH"
    assert p.value == 24


def test_parameter_create__float():
    p = Parameter("FORECAST_DAY", 3.5)
    assert p.name == "FORECAST_DAY"
    assert p.value == 3.5


def test_parameter_create_bool():
    p = Parameter("FLAG_UPLOAD", True)
    assert p.name == "FLAG_UPLOAD"
    assert p.value


def test_parameter_create_none():
    p = Parameter("TYPHOON_ID", None)
    assert p.name == "TYPHOON_ID"
    assert p.value is None


@pytest.mark.parametrize(
    "test_case",
    [
        (10, 20, 20),
        (10, 20.5, 20.5),
        (10, "20", "20"),
        (10, True, True),
        ("GLOBAL", 20, 20),
        ("GLOBAL", 20.5, 20.5),
        ("GLOBAL", "20", "20"),
        ("GLOBAL", True, True),
        (True, 20, 20),
        (True, 20.5, 20.5),
        (True, "20", "20"),
        (True, False, False),
    ],
)
def test_parameter_set_value(test_case):
    original_value, new_value, expected_value = test_case
    p = Parameter("SOME_PARAM", original_value)
    assert p.value == original_value
    p.value = new_value
    assert p.value == expected_value


def test_parameter_equal():
    p1 = Parameter("FORECAST_LENGTH", 24)
    p2 = Parameter("FORECAST_LENGTH", 24)
    assert p1 == p2

    p1 = Parameter("FORECAST_LENGTH", 24)
    p2 = Parameter("FORECAST_LENGTH", 24.0)
    assert p1 == p2


def test_parameter_not_equal_name():
    p1 = Parameter("FORECAST_LENGTH", 24)
    p2 = Parameter("FORECAST_DAY", 24)
    assert p1 != p2


def test_parameter_not_equal_value():
    p1 = Parameter("FORECAST_LENGTH", 24)
    p2 = Parameter("FORECAST_LENGTH", 36)
    assert p1 != p2

    p1 = Parameter("FORECAST_LENGTH", 24)
    p2 = Parameter("FORECAST_LENGTH", "24")
    assert p1 != p2


def test_parameter_not_equal():
    p1 = Parameter("FORECAST_LENGTH", 24)
    p2 = Parameter("OBS_SOURCE", "CMADAAS")
    assert p1 != p2
