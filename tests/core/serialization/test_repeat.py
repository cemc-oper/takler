import pytest

from takler.core import Repeat, RepeatDate, SerializationType


def test_repeat_date_to_dict():
    repeat_date = RepeatDate("TAKLER_DATE", "20221101", "20221110")
    assert repeat_date.to_dict() == dict(
        name="TAKLER_DATE",
        start_date="20221101",
        end_date="20221110",
        step=1,
        value=20221101,
        class_type="RepeatDate",
    )

    repeat_date.increment()

    assert repeat_date.to_dict() == dict(
        name="TAKLER_DATE",
        start_date="20221101",
        end_date="20221110",
        step=1,
        value=20221102,
        class_type="RepeatDate",
    )


def test_repeat_date_from_dict():
    d = dict(
        name="TAKLER_DATE",
        start_date="20221101",
        end_date="20221110",
        step=1,
        class_type="RepeatDate",
    )

    assert RepeatDate.from_dict(d, method=SerializationType.Tree) == RepeatDate(
        "TAKLER_DATE", "20221101", "20221110"
    )

    with pytest.raises(KeyError):
        RepeatDate.from_dict(d)
    with pytest.raises(KeyError):
        RepeatDate.from_dict(d, method=SerializationType.Status)

    d = dict(
        name="TAKLER_DATE",
        start_date="20221101",
        end_date="20221110",
        step=1,
        class_type="RepeatDate",
        value=20221105,
    )
    repeat_date = RepeatDate("TAKLER_DATE", "20221101", "20221110")
    repeat_date.change(20221105)

    assert RepeatDate.from_dict(d) == repeat_date
    assert RepeatDate.from_dict(d, method=SerializationType.Status) == repeat_date
    assert RepeatDate.from_dict(d, method=SerializationType.Tree) == RepeatDate(
        "TAKLER_DATE", "20221101", "20221110"
    )


def test_repeat_to_dict():
    repeat_date = RepeatDate("TAKLER_DATE", "20221101", "20221110")
    repeat = Repeat(repeat_date)

    assert repeat.to_dict() == dict(
        r=dict(
            name="TAKLER_DATE",
            start_date="20221101",
            end_date="20221110",
            step=1,
            value=20221101,
            class_type="RepeatDate",
        )
    )


def test_repeat_from_dict():
    d = dict(
        r=dict(
            name="TAKLER_DATE",
            start_date="20221101",
            end_date="20221110",
            step=1,
            class_type="RepeatDate",
        )
    )

    assert Repeat.from_dict(d, method=SerializationType.Tree) == Repeat(
        RepeatDate("TAKLER_DATE", "20221101", "20221110")
    )

    with pytest.raises(KeyError):
        Repeat.from_dict(d)
    with pytest.raises(KeyError):
        Repeat.from_dict(d, method=SerializationType.Status)

    d = dict(
        r=dict(
            name="TAKLER_DATE",
            start_date="20221101",
            end_date="20221110",
            step=1,
            class_type="RepeatDate",
            value=20221105,
        )
    )

    repeat = Repeat(RepeatDate("TAKLER_DATE", "20221101", "20221110"))
    repeat.change(20221105)

    assert Repeat.from_dict(d) == repeat
    assert Repeat.from_dict(d, method=SerializationType.Status) == repeat
    assert Repeat.from_dict(d, method=SerializationType.Tree) == Repeat(
        RepeatDate("TAKLER_DATE", "20221101", "20221110")
    )
