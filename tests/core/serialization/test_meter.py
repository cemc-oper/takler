import pytest

from takler.core import Meter, SerializationType


def test_meter_to_dict():
    meter = Meter("meter1", 0, 100)
    assert meter.to_dict() == dict(name="meter1", min_value=0, max_value=100, value=0)

    meter.value = 10
    assert meter.to_dict() == dict(name="meter1", min_value=0, max_value=100, value=10)


def test_meter_from_dict():
    d = dict(name="meter1", min_value=0, max_value=100)
    assert Meter.from_dict(d, method=SerializationType.Tree) == Meter(
        name="meter1", min_value=0, max_value=100
    )

    with pytest.raises(KeyError):
        Meter.from_dict(d)

    with pytest.raises(KeyError):
        Meter.from_dict(d, method=SerializationType.Status)

    value_d = dict(name="meter2", min_value=0, max_value=100, value=10)
    expected_meter = Meter(
        name="meter2",
        min_value=0,
        max_value=100,
    )
    assert Meter.from_dict(value_d, method=SerializationType.Tree) == expected_meter

    expected_meter.value = 10
    assert Meter.from_dict(value_d) == expected_meter
    assert Meter.from_dict(value_d, method=SerializationType.Status) == expected_meter
