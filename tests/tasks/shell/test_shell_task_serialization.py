from pathlib import Path

import pytest

from takler.core import SerializationType
from takler.core.node import Node
from takler.tasks import ShellScriptTask


def test_to_dict_with_str_script_path():
    task = ShellScriptTask("task1", "/home/johndoe/flow1/task1.takler")
    d = task.to_dict()
    assert d["script_path"] == "/home/johndoe/flow1/task1.takler"
    assert d["class_type"] == dict(
        module="takler.tasks.shell.shell_script_task",
        name="ShellScriptTask",
    )
    # Task fields are still written out
    assert d["task_id"] is None
    assert d["try_no"] == 0


def test_to_dict_with_path_script_path():
    script_path = Path("/home/johndoe/flow1/task1.takler")
    task = ShellScriptTask("task1", script_path)
    d = task.to_dict()
    assert d["script_path"] == str(script_path)
    assert isinstance(d["script_path"], str)


def test_to_dict_with_none_script_path():
    task = ShellScriptTask("task1")
    d = task.to_dict()
    assert "script_path" in d
    assert d["script_path"] is None


@pytest.mark.parametrize("method", [SerializationType.Status, SerializationType.Tree])
def test_from_dict_restore_script_path(method):
    task = ShellScriptTask("task1", "/home/johndoe/flow1/task1.takler")
    task.task_id = "123456"
    task.try_no = 1
    d = task.to_dict()

    restored = Node.from_dict(d, method=method)

    assert isinstance(restored, ShellScriptTask)
    assert restored.script_path == "/home/johndoe/flow1/task1.takler"
    assert restored.to_dict()["script_path"] == d["script_path"]


@pytest.mark.parametrize("method", [SerializationType.Status, SerializationType.Tree])
def test_from_dict_restore_none_script_path(method):
    task = ShellScriptTask("task1")
    d = task.to_dict()

    restored = Node.from_dict(d, method=method)

    assert restored.script_path is None


def test_bunch_tasks_round_trip(shell_task_bunch):
    task2 = shell_task_bunch.task2
    d = task2.to_dict()

    restored = Node.from_dict(d, method=SerializationType.Status)

    assert restored.script_path == task2.script_path
    assert restored.to_dict() == d
