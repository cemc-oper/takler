"""
Node level runtime states which are only restored with ``SerializationType.Status``:

* ``Limit``'s ``value`` and ``node_paths``
* ``TimeAttribute``'s ``free`` latch
* ``Node.is_complete_triggered``
"""

import pytest

from takler.core import Flow, Task, SerializationType


@pytest.fixture
def task_dict():
    """
    Dict of a task with a used limit, a freed time attribute and a fired complete trigger.
    """
    return dict(
        name="task1",
        state=dict(
            status=5,
            suspended=False,
        ),
        class_type=dict(
            module="takler.core.task_node",
            name="Task",
        ),
        complete_trigger="./task2 == complete",
        is_complete_triggered=True,
        limits=[
            dict(
                name="limit1",
                limit=10,
                node_paths=["/flow1/task1", "/flow1/task2"],
                value=2,
            ),
        ],
        times=[
            dict(time="12:00", free=True),
            dict(time="18:00", free=False),
        ],
        task_id=None,
        aborted_reason=None,
        try_no=0,
    )


def test_status_restores_limit_value_and_node_paths(task_dict):
    task = Task.from_dict(task_dict, method=SerializationType.Status)

    limit = task.find_limit("limit1")
    assert limit.limit == 10
    assert limit.value == 2
    assert limit.node_paths == {"/flow1/task1", "/flow1/task2"}
    assert limit.node is task


def test_status_restores_time_free_latch(task_dict):
    task = Task.from_dict(task_dict, method=SerializationType.Status)

    assert [t.time.strftime("%H:%M") for t in task.times] == ["12:00", "18:00"]
    assert [t.free for t in task.times] == [True, False]


def test_status_restores_is_complete_triggered(task_dict):
    task = Task.from_dict(task_dict, method=SerializationType.Status)

    assert task.is_complete_triggered is True


def test_tree_resets_runtime_state(task_dict):
    task = Task.from_dict(task_dict, method=SerializationType.Tree)

    limit = task.find_limit("limit1")
    assert limit.limit == 10
    assert limit.value == 0
    assert limit.node_paths == set()
    assert limit.node is task

    assert [t.free for t in task.times] == [False, False]
    assert task.is_complete_triggered is False


def test_to_dict_writes_runtime_state():
    flow1 = Flow("flow1")
    with flow1.add_task("task1") as task1:
        task1.add_limit("limit1", 10)
        task1.add_time("12:00")

    limit = task1.find_limit("limit1")
    limit.increment(1, task1.node_path)
    task1.times[0].set_free()
    task1.is_complete_triggered = True

    d = task1.to_dict()

    assert d["limits"] == [
        dict(name="limit1", limit=10, node_paths=["/flow1/task1"], value=1),
    ]
    assert d["times"] == [dict(time="12:00", free=True)]
    assert d["is_complete_triggered"] is True


def test_to_dict_omits_default_is_complete_triggered():
    flow1 = Flow("flow1")
    with flow1.add_task("task1") as task1:
        pass

    assert "is_complete_triggered" not in task1.to_dict()


def test_status_round_trip_keeps_runtime_state():
    flow1 = Flow("flow1")
    with flow1.add_task("task1") as task1:
        task1.add_limit("limit1", 5)
        task1.add_time("09:30")
        task1.add_time("21:30")

    limit = task1.find_limit("limit1")
    limit.increment(1, "/flow1/task1")
    limit.increment(1, "/flow1/task2")
    task1.times[1].set_free()
    task1.is_complete_triggered = True

    restored = Task.from_dict(task1.to_dict(), method=SerializationType.Status)

    assert restored.to_dict() == task1.to_dict()

    restored_limit = restored.find_limit("limit1")
    assert restored_limit.value == 2
    assert restored_limit.node_paths == {"/flow1/task1", "/flow1/task2"}
    assert [t.free for t in restored.times] == [False, True]
    assert restored.is_complete_triggered is True


def test_add_limit_object_rejects_duplicate_name():
    from takler.core.limit import Limit

    flow1 = Flow("flow1")
    with flow1.add_task("task1") as task1:
        task1.add_limit("limit1", 5)

    with pytest.raises(RuntimeError):
        task1.add_limit_object(Limit("limit1", 10))

    assert len(task1.limits) == 1
