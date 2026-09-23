from pathlib import Path

import pytest

from takler.core import (
    Bunch,
    Flow,
    Task,
    NodeContainer,
    NodeStatus,
    Parameter,
    RepeatDate,
    task,
)
from takler.schema import DefinitionError, parse_definition
from takler.serialization import export_definition
from takler.tasks.shell import ShellScriptTask


def test_all_builtin_definitions_preserved_while_runtime_is_excluded(monkeypatch):
    bunch = Bunch("root", host="DEPLOYMENT_HOST", port="9123")
    bunch.add_parameter("TAKLER_HOME", "/explicit")
    bunch.add_parameter(Parameter("nullable", None))
    flow = Flow("f")
    bunch.add_flow(flow)
    group = NodeContainer("g")
    flow.append_child(group)
    shell = ShellScriptTask("s", Path("/missing/script.sh"))
    group.append_child(shell)
    group.append_child(Task("plain"))
    shell.set_default_node_status(NodeStatus.complete)
    event = shell.add_event("ready", True)
    meter = shell.add_meter("progress", 10, 100)
    limit = flow.add_limit("pool", 5)
    shell.add_in_limit("pool", "/f", 2)
    shell.add_repeat(RepeatDate("D", "20260101", "20260105", 2))
    clock = shell.add_time("06:00")
    shell.add_trigger("/f/g/plain == complete")
    shell.add_complete_trigger("/f/g/plain == aborted")
    flow.begin()
    shell.state.node_status = NodeStatus.active
    shell.state.suspended = True
    shell.task_id = "RUNTIME_ID"
    shell.try_no = 19
    shell.job_password = "SECRET_PASSWORD"
    shell.aborted_reason = "RUNTIME_REASON"
    shell.trigger_expression.free = True
    shell.complete_trigger_expression.free = True
    shell.is_complete_triggered = True
    event.value = False
    meter.value = 80
    limit.value = 2
    limit.node_paths.add("/f/g/s")
    shell.repeat.increment()
    clock.free = True
    before = bunch.to_dict()
    monkeypatch.setattr(
        Path, "open", lambda *a, **kw: pytest.fail("export read a file")
    )
    monkeypatch.setattr(
        shell, "to_dict", lambda: pytest.fail("mixed serialization used")
    )
    result = export_definition(bunch)
    text = result.model_dump_json()
    for forbidden in (
        "RUNTIME_ID",
        "RUNTIME_REASON",
        "SECRET_PASSWORD",
        "DEPLOYMENT_HOST",
        '"state"',
        '"try_no"',
        '"job_password"',
        '"calendar"',
        '"begun"',
        '"free"',
        '"node_paths"',
        '"class_type"',
    ):
        assert forbidden not in text
    root = result.root
    assert [(p.name, p.value) for p in root.user_parameters] == [
        ("TAKLER_HOME", "/explicit"),
        ("nullable", None),
    ]
    exported = root.flows[0].children[0].children[0]
    assert exported.default_node_status == "complete"
    assert exported.events[0].initial_value is True
    assert exported.meters[0].min_value == 10
    assert exported.meters[0].max_value == 100
    assert root.flows[0].limits[0].limit == 5
    assert exported.in_limits[0].tokens == 2
    assert exported.repeat.start_date == "20260101"
    assert exported.repeat.step == 2
    assert exported.script_path == "/missing/script.sh"
    assert exported.times[0].time == "06:00"
    assert exported.trigger == "/f/g/plain == complete"
    assert exported.complete_trigger == "/f/g/plain == aborted"
    assert parse_definition(text) == result
    monkeypatch.undo()
    assert bunch.to_dict() == before
    assert shell.job_password == "SECRET_PASSWORD"


@pytest.mark.parametrize(
    "attribute,value",
    [
        ("events", [object()]),
        ("meters", [object()]),
        ("limits", [object()]),
        ("times", [object()]),
        ("children", [object()]),
        ("trigger_expression", object()),
        ("complete_trigger_expression", object()),
        ("repeat", object()),
        ("default_node_status", NodeStatus.complete),
    ],
)
def test_unsupported_root_attributes(attribute, value):
    bunch = Bunch()
    setattr(bunch, attribute, value)
    with pytest.raises(DefinitionError, match="unsupported_root_attribute"):
        export_definition(bunch)


def test_root_in_limit_rejected():
    bunch = Bunch()
    bunch.add_in_limit("pool")
    with pytest.raises(DefinitionError, match="unsupported_root_attribute"):
        export_definition(bunch)


def test_exact_types_required():
    class CustomTask(Task):
        pass

    @task("local")
    def local():
        pass

    for node in (CustomTask("custom"), local()):
        flow = Flow("f")
        flow.append_child(node)
        with pytest.raises(DefinitionError, match="unregistered_type"):
            export_definition(flow)


def test_root_must_be_flow_or_bunch():
    with pytest.raises(DefinitionError, match="invalid_structure"):
        export_definition(Task("t"))


def test_defaults_and_deployment_are_not_exported():
    root = export_definition(Bunch()).root
    assert root.name == ""
    assert root.user_parameters == []
    assert root.flows == []


def test_unknown_root_and_repeat_types_rejected():
    class CustomFlow(Flow):
        pass

    with pytest.raises(DefinitionError, match="unregistered_type"):
        export_definition(CustomFlow("f"))

    class CustomRepeat(RepeatDate):
        pass

    flow = Flow("f")
    flow.add_repeat(CustomRepeat("D", "20260101", "20260102"))
    with pytest.raises(DefinitionError, match="unregistered_type"):
        export_definition(flow)


def test_export_rejects_unrepresentable_time_instead_of_truncating():
    import datetime

    flow = Flow("f")
    flow.add_time(datetime.time(6, 0, 30))
    with pytest.raises(DefinitionError, match="invalid_field"):
        export_definition(flow)
