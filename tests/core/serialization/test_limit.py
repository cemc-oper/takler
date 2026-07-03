import pytest

from takler.core import Limit, Task, SerializationType
from takler.core.limit import InLimit, InLimitManager


def test_limit_to_dict():
    limit = Limit("upload_limit", 10)
    assert limit.to_dict() == dict(
        name="upload_limit",
        limit=10,
        value=0,
        node_paths=list(),
    )

    limit.increment(1, "/flow1/task1")
    assert limit.to_dict() == dict(
        name="upload_limit",
        limit=10,
        value=1,
        node_paths=["/flow1/task1"]
    )

    limit.increment(1, "/flow1/task2")
    assert limit.to_dict() == dict(
        name="upload_limit",
        limit=10,
        value=2,
        node_paths=[
            "/flow1/task1",
            "/flow1/task2",
        ]
    )

    limit.decrement(1, "/flow1/task1")
    assert limit.to_dict() == dict(
        name="upload_limit",
        limit=10,
        value=1,
        node_paths=[
            "/flow1/task2"
        ]
    )


def test_limit_from_dict():
    d = dict(
        name="upload_limit",
        limit=10
    )
    assert Limit.from_dict(d, method=SerializationType.Tree) == Limit("upload_limit", 10)
    with pytest.raises(KeyError):
        Limit.from_dict(d)
    with pytest.raises(KeyError):
        Limit.from_dict(d, method=SerializationType.Status)

    d = dict(
        name="upload_limit",
        limit=10,
        value=1,
        node_paths=["/flow1/task1"]
    )

    limit = Limit("upload_limit", 10)
    limit.increment(1, "/flow1/task1")
    assert Limit.from_dict(d) == limit
    assert Limit.from_dict(d, method=SerializationType.Status) == limit
    assert Limit.from_dict(d, method=SerializationType.Tree) == Limit("upload_limit", 10)

    d = dict(
        name="upload_limit",
        limit=10,
        value=2,
        node_paths=[
            "/flow1/task1",
            "/flow1/task2",
        ]
    )
    limit.increment(1, "/flow1/task2")
    assert Limit.from_dict(d) == limit
    assert Limit.from_dict(d, method=SerializationType.Status)
    assert Limit.from_dict(d, method=SerializationType.Tree) == Limit("upload_limit", 10)

    d = dict(
        name="upload_limit",
        limit=10,
        value=1,
        node_paths=[
            "/flow1/task2"
        ]
    )
    limit.decrement(1, "/flow1/task1")
    assert Limit.from_dict(d) == limit
    assert Limit.from_dict(d, method=SerializationType.Status)
    assert Limit.from_dict(d, method=SerializationType.Tree) == Limit("upload_limit", 10)


def test_in_limit_to_dict():
    limit = Limit("upload_limit", 10)
    in_limit = InLimit("upload_limit")
    in_limit.set_limit(limit)

    assert in_limit.to_dict() == dict(
        limit_name="upload_limit",
        tokens=1,
        node_path=None,
    )


def test_in_limit_from_dict():
    d = dict(
        limit_name="upload_limit",
        tokens=1,
        node_path=None,
    )
    assert InLimit.from_dict(d, method=SerializationType.Tree) == InLimit(
        limit_name="upload_limit", tokens=1, node_path=None)
    assert InLimit.from_dict(d, method=SerializationType.Status) == InLimit(
        limit_name="upload_limit", tokens=1, node_path=None)
    assert InLimit.from_dict(d) == InLimit(limit_name="upload_limit", tokens=1, node_path=None)


def test_in_limit_manager_to_dict():
    limit = Limit("upload_limit", 10)
    in_limit = InLimit("upload_limit")
    in_limit.set_limit(limit)
    limit_2 = Limit("run_limit", 5)
    in_limit_2 = InLimit("run_limit")
    in_limit_2.set_limit(limit_2)

    node = Task("task1")
    in_limit_manager = InLimitManager(node)
    in_limit_manager.add_in_limit(in_limit)
    assert in_limit_manager.to_dict() == dict(
        in_limit_list=[
            dict(
                limit_name="upload_limit",
                tokens=1,
                node_path=None,
            )
        ]
    )

    in_limit_manager.add_in_limit(in_limit_2)
    assert in_limit_manager.to_dict() == dict(
        in_limit_list=[
            dict(
                limit_name="upload_limit",
                tokens=1,
                node_path=None,
            ),
            dict(
                limit_name="run_limit",
                tokens=1,
                node_path=None,
            )
        ]
    )


def test_in_limit_manager_from_dict():
    d = dict(
        in_limit_list=[
            dict(
                limit_name="upload_limit",
                tokens=1,
                node_path=None,
            )
        ]
    )

    expected_node = Task("task1")
    expected_node.add_in_limit("upload_limit")

    node = Task("task1")
    InLimitManager.fill_from_dict(d, node=node)
    assert node.in_limit_manager == expected_node.in_limit_manager

    expected_node.add_in_limit("run_limit")
    d = dict(
        in_limit_list=[
            dict(
                limit_name="upload_limit",
                tokens=1,
                node_path=None,
            ),
            dict(
                limit_name="run_limit",
                tokens=1,
                node_path=None,
            )
        ]
    )

    node = Task("task1")
    InLimitManager.fill_from_dict(d, node=node)
    assert node.in_limit_manager == expected_node.in_limit_manager
