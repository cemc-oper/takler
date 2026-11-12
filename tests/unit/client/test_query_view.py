"""Untrusted show documents must remain data, including unknown plugin types."""

import builtins
import io
import json

import pytest

from takler.query import parse_show
from takler.visitor import print_show


def document():
    return {
        "name": "server",
        "user_parameters": [{"name": "ROOT", "value": 7}],
        "generated_parameters": [{"name": "HOST", "value": "server"}],
        "flows": [
            {
                "name": "flow",
                "type_id": "uninstalled.Flow",
                "node_kind": "flow",
                "state": {"status": 3, "suspended": False},
                "children": [
                    {
                        "name": "task",
                        "type_id": "uninstalled.Task",
                        "node_kind": "task",
                        "state": {"status": 5, "suspended": True},
                    }
                ],
            }
        ],
    }


def test_no_import_registry_or_constructor(monkeypatch):
    from takler.core import Bunch
    from takler.serialization.registry import TypeRegistry

    def forbidden(*args, **kwargs):
        pytest.fail("show must not load execution code")

    data = document()
    data["flows"][0]["class_type"] = {"module": "attacker", "class": "Execute"}
    data["flows"][0]["children"][0].update(module="attacker", **{"class": "Execute"})
    payload = json.dumps(data)
    with monkeypatch.context() as patch:
        patch.setattr(Bunch, "from_dict", forbidden)
        patch.setattr(TypeRegistry, "by_id", forbidden)
        patch.setattr(builtins, "__import__", forbidden)
        snapshot = parse_show(payload)
        output = io.StringIO()
        print_show(snapshot, output, show_parameter=True)
    assert "|- flow [queued]" in output.getvalue()
    assert "  |- task [suspend (active)]" in output.getvalue()
    assert "param ROOT '7'" in output.getvalue()
    assert snapshot.get("/flow/task").class_name == "uninstalled.Task"
    assert not hasattr(snapshot.get("/flow/task"), "run")


def test_parameter_types_precedence_null_and_redaction():
    data = document()
    flow = data["flows"][0]
    task = flow["children"][0]
    flow["user_parameters"] = [
        {"name": "SHARED", "value": "parent"},
        {"name": "N", "value": "parent"},
    ]
    task["generated_parameters"] = [
        {"name": "SHARED", "value": 9},
        {"name": "LOCAL", "value": 2},
    ]
    task["user_parameters"] = [
        {"name": "LOCAL", "value": False},
        {"name": "N", "value": None},
        {"name": "TAKLER_PASS", "value": "never-visible"},
        {"name": "CUSTOM", "value": "unsafe"},
        {"name": "LITERAL", "value": "<redacted>"},
    ]
    task["redacted_parameters"] = ["CUSTOM"]
    snapshot = parse_show(json.dumps(data))
    assert snapshot.resolve_parameter("/flow/task", "ROOT").value == 7
    assert snapshot.resolve_parameter("/flow/task", "LOCAL").value is False
    assert snapshot.resolve_parameter("/flow/task", "SHARED").value == 9
    assert snapshot.resolve_parameter("/flow/task", "N").value is None
    assert snapshot.resolve_parameter("/flow/task", "MISSING") is None
    assert snapshot.lookup_parameter("/flow/task", "N") is None
    assert snapshot.lookup_parameter("/flow/task", "CUSTOM") is None
    assert snapshot.resolve_parameter("/flow/task", "CUSTOM").redacted
    assert not snapshot.resolve_parameter("/flow/task", "LITERAL").redacted
    assert snapshot.lookup_parameter("/flow/task", "LITERAL") == "<redacted>"
    assert "never-visible" not in repr(snapshot)
    assert "unsafe" not in repr(snapshot)


@pytest.mark.parametrize(
    "change",
    [
        lambda d: d.update(name=None),
        lambda d: d.update(flows={}),
        lambda d: d["flows"][0].update(name="../x"),
        lambda d: d["flows"][0].update(state={"status": True, "suspended": False}),
        lambda d: d["flows"][0].update(state={"status": 99, "suspended": False}),
        lambda d: d["flows"][0].update(state={"status": 1, "suspended": "false"}),
        lambda d: d["flows"].append(d["flows"][0].copy()),
        lambda d: d["flows"][0].update(try_no="1"),
        lambda d: d["flows"][0].update(trigger={}),
        lambda d: d.update(user_parameters=[{"name": "X", "value": {}}]),
        lambda d: d.update(user_parameters=[{"name": "X"}]),
        lambda d: d.update(user_parameters=[{"name": "X", "value": 1}] * 2),
        lambda d: d["flows"][0].update(events=[{}]),
    ],
)
def test_invalid_query_is_rejected(change):
    data = document()
    change(data)
    with pytest.raises(ValueError, match="invalid_query"):
        parse_show(json.dumps(data))


@pytest.mark.parametrize("payload", ['{"name":"s","name":"t"}', '{"name":"s","x":NaN}'])
def test_duplicate_and_nonfinite_json_rejected(payload):
    with pytest.raises(ValueError):
        parse_show(payload)


def test_unknown_kind_has_generic_actions_only():
    from takler.tui.menu import applicable_actions

    data = document()
    del data["flows"][0]["children"][0]["node_kind"]
    node = parse_show(json.dumps(data)).get("/flow/task")
    actions = {action.id for action in applicable_actions(node)}
    assert "run" not in actions
    assert {"suspend", "resume"} <= actions


def test_sensitive_repeat_generated_value_is_also_redacted():
    data = document()
    data["flows"][0]["repeat"] = {"r": {"name": "TAKLER_PASS", "value": "PRIVATE"}}
    snapshot = parse_show(json.dumps(data))
    assert "PRIVATE" not in snapshot.get("/flow").repeat
