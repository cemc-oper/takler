"""
`SerializationType` 成员集合的回归测试。

checkpoint 复用 `SerializationType.Status`，不引入新的取值；本测试锁定枚举成员集合，
使后续无意新增取值时立刻失败。

_Requirements: 6.17_
"""
from takler.core.util import SerializationType


def test_serialization_type_members_are_exactly_tree_and_status():
    assert set(SerializationType) == {SerializationType.Tree, SerializationType.Status}
    assert [member.name for member in SerializationType] == ["Tree", "Status"]


def test_serialization_type_values():
    assert SerializationType.Tree.value == "tree"
    assert SerializationType.Status.value == "status"
