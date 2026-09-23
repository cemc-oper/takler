"""Tree reads definitions without consulting runtime fields, before begin."""

import copy
import json
from pathlib import Path

import pytest

from takler.core import Flow, NodeStatus, SerializationType, Task
from takler.tasks import ShellScriptTask


@pytest.fixture(params=[Task, ShellScriptTask], ids=["task", "shell"])
def flow_data(request):
    # Independent saved data, never produced by the reader/writer under test.
    source = json.loads(
        (Path(__file__).parents[3] / "fixtures" / "checkpoint_root_v1.json").read_text()
    )["bunch"]
    flow = source["flows"][0]
    task = flow["children"][0]
    for key in ("events", "meters", "limits", "in_limit_manager", "repeat", "times"):
        task[key] = copy.deepcopy(source[key])
    task.update(
        default_node_status=2,
        is_complete_triggered=True,
        aborted_reason="previous attempt failed",
        job_password="UNTRUSTED_WIRE_PASSWORD",
    )
    task["class_type"] = {
        "module": request.param.__module__,
        "name": request.param.__name__,
    }
    if request.param is ShellScriptTask:
        task["script_path"] = "/scripts/task.sh"
    flow["children"] = [
        {
            "name": "c",
            "class_type": {
                "module": "takler.core.node_container",
                "name": "NodeContainer",
            },
            "state": {"status": 5, "suspended": True},
            "children": [task],
        }
    ]
    task["limits"][0]["node_paths"] = ["/f/c/t"]
    return flow, request.param


def assert_fresh(flow, task_type):
    task = flow.find_node("/f/c/t")
    assert type(task) is task_type
    assert task.get_flow() is flow
    assert task.task_id is None
    assert task.try_no == 0
    assert task.aborted_reason is None
    assert task.job_password is None
    for node in (flow, flow.children[0], task):
        assert node.state.node_status is NodeStatus.unknown
        assert node.state.suspended is False
    assert flow.begun is False
    assert all(value is None for value in flow.calendar.to_dict().values())
    assert task.default_node_status is NodeStatus.complete
    assert task.events[0].initial_value is True
    assert task.events[0].value is True
    assert task.meters[0].value == task.meters[0].min_value == 10
    assert task.limits[0].value == 0
    assert task.limits[0].node_paths == set()
    assert task.limits[0].node is task
    assert task.in_limit_manager.node is task
    assert task.in_limit_manager.in_limit_list[0].tokens == 2
    assert task.repeat.r.value == 20260901
    assert task.trigger_expression.free is False
    assert task.complete_trigger_expression.free is False
    assert task.is_complete_triggered is False
    assert task.times[0].free is False
    assert task.find_parameter("CHILD_NULL").value is None
    if task_type is ShellScriptTask:
        assert task.script_path == "/scripts/task.sh"


@pytest.mark.parametrize("runtime", ["present", "missing", "invalid"])
def test_tree_ignores_runtime_without_begin(flow_data, runtime, monkeypatch):
    data, task_type = flow_data
    original = copy.deepcopy(data)
    task = data["children"][0]["children"][0]
    runtime_fields = [
        (data, ["state", "begun", "calendar"]),
        (data["children"][0], ["state"]),
        (
            task,
            [
                "state",
                "task_id",
                "try_no",
                "aborted_reason",
                "job_password",
                "trigger_free",
                "complete_trigger_free",
                "is_complete_triggered",
            ],
        ),
        (task["events"][0], ["value"]),
        (task["meters"][0], ["value"]),
        (task["limits"][0], ["value", "node_paths"]),
        (task["repeat"]["r"], ["value"]),
        (task["times"][0], ["free"]),
    ]
    for obj, keys in runtime_fields:
        for key in keys:
            if runtime == "missing":
                obj.pop(key, None)
            elif runtime == "invalid":
                obj[key] = {"must_not_be_read": True}
    expected_input = copy.deepcopy(data)

    def unexpected_lifecycle(*args, **kwargs):
        pytest.fail("Tree decoding must not begin, requeue or execute a task")

    monkeypatch.setattr(Flow, "begin", unexpected_lifecycle)
    monkeypatch.setattr(Flow, "requeue", unexpected_lifecycle)
    monkeypatch.setattr(Task, "run", unexpected_lifecycle)
    monkeypatch.setattr(Task, "requeue", unexpected_lifecycle)
    restored = Flow.from_dict(data, method=SerializationType.Tree)
    assert_fresh(restored, task_type)
    assert data == expected_input
    assert original["children"][0]["children"][0]["try_no"] == 2


def test_status_preserves_runtime_but_never_reads_embedded_password(flow_data):
    data, task_type = flow_data
    restored = Flow.from_dict(data, method=SerializationType.Status)
    task = restored.find_node("/f/c/t")
    assert type(task) is task_type
    assert task.task_id == "123"
    assert task.try_no == 2
    assert task.aborted_reason == "previous attempt failed"
    assert task.job_password is None  # CheckpointManager restores its separate map.
    expected = copy.deepcopy(data)
    del expected["children"][0]["children"][0]["job_password"]
    assert restored.to_dict() == expected
    assert task.events[0].value is False
    assert task.meters[0].value == 42
    assert task.limits[0].value == 2
    assert task.repeat.r.value == 20260903
    assert task.trigger_expression.free and task.complete_trigger_expression.free
    assert task.times[0].free and task.is_complete_triggered
    assert restored.begun is True
