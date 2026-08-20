import datetime

import pytest

from takler.core import TimeAttribute, SerializationType


def test_time_attr_to_dict():
    time_attr = TimeAttribute("10:00")
    assert time_attr.to_dict() == dict(
        time="10:00",
        free=False,
    )

    time_attr.set_free()
    assert time_attr.to_dict() == dict(time="10:00", free=True)

    time_attr = TimeAttribute(datetime.time(11, 10))
    assert time_attr.to_dict() == dict(
        time="11:10",
        free=False,
    )


def test_time_attr_from_dict():
    d = dict(
        time="12:00",
    )
    assert TimeAttribute.from_dict(d, method=SerializationType.Tree) == TimeAttribute(
        "12:00"
    )

    with pytest.raises(KeyError):
        TimeAttribute.from_dict(d)
    with pytest.raises(KeyError):
        TimeAttribute.from_dict(d, method=SerializationType.Status)

    d = dict(time="12:00", free=True)
    time_attr = TimeAttribute("12:00")
    time_attr.set_free()
    assert TimeAttribute.from_dict(d) == time_attr
    assert TimeAttribute.from_dict(d, method=SerializationType.Status) == time_attr

    assert TimeAttribute.from_dict(d, method=SerializationType.Tree) != time_attr
    assert TimeAttribute.from_dict(d, method=SerializationType.Tree) == TimeAttribute(
        "12:00"
    )
