import copy
import importlib
import json
from pathlib import Path

import pytest

from takler.core import Bunch, Flow, NodeStatus, Task, RepeatDate
from takler.schema import DefinitionError
from takler.schema.definition import DefinitionModel
from takler.serialization import (
    export_definition,
    build_definition,
    builtin_registry,
    NodeRegistration,
)
from takler.serialization.registry import using_registry
from takler.serialization.runtime import export_runtime, restore_runtime
from takler.server.checkpoint import CheckpointManager


def test_build_resets_runtime_and_rebinds_every_reference(monkeypatch):
    bunch = Bunch()
    bunch.add_parameter("GLOBAL", "value")
    flow = bunch.add_flow("f")
    group = flow.add_container("g")
    task = group.add_task("t")
    flow.add_limit("slots", 3)
    task.add_in_limit("slots", "/f", 2)
    task.add_event("ready", True).value = False
    task.add_meter("progress", 10, 20).value = 19
    task.add_repeat(RepeatDate("D", "20260101", "20260105", 2))
    task.repeat.increment()
    task.add_time("06:00").free = True
    task.add_trigger("/f/g/t == complete")
    task.trigger_expression.free = True
    flow.begin()
    task.state.suspended = True
    task.task_id = "old"
    task.try_no = 3
    task.job_password = "SECRET"
    before = bunch.to_dict()
    doc = export_definition(bunch)

    def forbidden(*a, **kw):
        pytest.fail("construction performed IO or scheduling")

    monkeypatch.setattr(Flow, "begin", forbidden)
    monkeypatch.setattr(Task, "run", forbidden)
    monkeypatch.setattr(Task, "requeue", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    rebuilt = build_definition(doc)
    task2 = rebuilt.find_node("/f/g/t")
    assert task2.get_bunch() is rebuilt
    assert task2.get_flow().begun is False
    assert task2.state.node_status is NodeStatus.unknown
    assert not task2.state.suspended
    assert (task2.task_id, task2.try_no, task2.job_password) == (None, 0, None)
    assert task2.events[0].value is True
    assert task2.meters[0].value == 10
    assert task2.repeat.r.value == 20260101
    assert not task2.times[0].free and not task2.trigger_expression.free
    assert task2.trigger_expression.ast is not None
    limit = rebuilt.find_flow("f").limits[0]
    assert task2.in_limit_manager.in_limit_list[0].limit is limit
    assert limit.value == 0 and limit.node_paths == set()
    assert task2.find_parent_parameter("GLOBAL").value == "value"
    assert bunch.to_dict() == before


def test_cross_flow_reference_validation_is_read_only():
    online = Bunch()
    provider = online.add_flow("provider")
    limit = provider.add_limit("pool", 2)
    candidate = Flow("consumer")
    candidate.add_task("t").add_in_limit("pool", "/provider", 2)
    before = online.to_dict()
    built = build_definition(export_definition(candidate), existing_bunch=online)
    assert built.bunch is None
    assert built.children[0].in_limit_manager.in_limit_list[0].limit is limit
    assert online.to_dict() == before
    with pytest.raises(DefinitionError, match="unresolved_reference"):
        build_definition(export_definition(candidate))
    candidate.children[0].in_limit_manager.in_limit_list[0].tokens = 3
    with pytest.raises(DefinitionError, match="invalid_reference"):
        build_definition(export_definition(candidate), existing_bunch=online)
    assert online.to_dict() == before


def test_replacement_reference_does_not_bind_to_old_same_name_flow():
    online = Bunch()
    old = online.add_flow("f")
    old.add_limit("pool", 2)
    candidate = Flow("f")
    candidate.add_task("t").add_in_limit("pool", "/f")
    with pytest.raises(DefinitionError, match="unresolved_reference"):
        build_definition(export_definition(candidate), existing_bunch=online)


def test_invalid_expression_and_old_documents_fail_without_import(monkeypatch):
    flow = Flow("f")
    flow.add_trigger("invalid ???")
    with pytest.raises(DefinitionError, match="invalid_field"):
        build_definition(export_definition(flow))

    def forbidden(*a, **kw):
        pytest.fail("data driven import")

    monkeypatch.setattr(importlib, "import_module", forbidden)
    for data in (
        {"class_type": {"module": "evil", "name": "Run"}, "name": "f"},
        {"type_id": "evil.flow", "name": "f"},
    ):
        with pytest.raises(DefinitionError):
            Flow.from_dict(data)
        with pytest.raises(DefinitionError):
            build_definition(data)


class CustomTask(Task):
    def __init__(self, name, command):
        super().__init__(name)
        self.command = command
        self.count = 0


class CustomDefinition(DefinitionModel):
    command: str


class CustomRuntime(DefinitionModel):
    count: int


def registration(**changes):
    fields = dict(
        type_id="example.custom",
        python_type=CustomTask,
        kind="task",
        definition_schema=CustomDefinition,
        export_definition=lambda node: {"command": node.command},
        construct=lambda name, data: CustomTask(name, data.command),
        runtime_schema=CustomRuntime,
        export_runtime=lambda node: {"count": node.count},
        restore_runtime=lambda node, data: setattr(node, "count", data.count),
        query=lambda node: {"command": node.command, "count": node.count},
    )
    fields.update(changes)
    return NodeRegistration(**fields)


def test_registered_extension_definition_and_runtime_roundtrip():
    registry = builtin_registry()
    registry.register(registration())
    bunch = Bunch()
    task = bunch.add_flow("f").add_task(CustomTask("t", "echo example"))
    task.count = 7
    doc = export_definition(bunch, registry=registry)
    rebuilt = build_definition(doc, registry=registry)
    restored = rebuilt.find_node("/f/t")
    assert type(restored) is CustomTask
    assert restored.command == "echo example" and restored.count == 0
    with using_registry(registry):
        snapshot = export_runtime(bunch)
    restored = restore_runtime(snapshot, registry=registry).find_node("/f/t")
    assert type(restored) is CustomTask and restored.count == 7
    with pytest.raises(DefinitionError, match="unknown_type"):
        restore_runtime(snapshot)
    bad = doc.model_dump(mode="json")
    bad["root"]["flows"][0]["children"][0]["type_data"]["job_password"] = "SECRET"
    with pytest.raises(DefinitionError):
        build_definition(bad, registry=registry)


@pytest.mark.parametrize(
    "missing",
    [
        "definition_schema",
        "export_definition",
        "construct",
        "runtime_schema",
        "export_runtime",
        "restore_runtime",
    ],
)
def test_missing_codec_is_explicit(missing):
    registry = builtin_registry()
    registry.register(registration(**{missing: None}))
    bunch = Bunch()
    bunch.add_flow("f").add_task(CustomTask("t", "cmd"))
    complete = builtin_registry()
    complete.register(registration())
    if missing in {"definition_schema", "export_definition"}:
        with pytest.raises(DefinitionError, match="missing_codec"):
            export_definition(bunch, registry=registry)
    elif missing == "construct":
        doc = export_definition(bunch, registry=complete)
        with pytest.raises(DefinitionError, match="missing_codec"):
            build_definition(doc, registry=registry)
    elif missing in {"runtime_schema", "export_runtime"}:
        with (
            using_registry(registry),
            pytest.raises(DefinitionError, match="missing_codec"),
        ):
            export_runtime(bunch)
    else:
        with using_registry(complete):
            snapshot = export_runtime(bunch)
        with pytest.raises(DefinitionError, match="missing_codec"):
            restore_runtime(snapshot, registry=registry)


def test_duplicate_and_reserved_registration_rejected():
    registry = builtin_registry()
    registry.register(registration())
    for entry in (
        registration(),
        registration(type_id="takler.custom"),
        registration(type_id="other.custom"),
    ):
        with pytest.raises(DefinitionError, match="invalid_registration"):
            registry.register(entry)


@pytest.mark.parametrize(
    "corrupt",
    [
        "unknown_type",
        "missing_runtime",
        "bad_state",
        "bad_limit",
        "duplicate",
        "missing_password",
        "unknown_password",
        "version1",
    ],
)
def test_checkpoint_invalid_primary_falls_back_atomically(tmp_path, corrupt):
    bunch = Bunch("good")
    flow = bunch.add_flow("f")
    flow.add_limit("pool", 2)
    task = flow.add_task("t")
    task.add_in_limit("pool", "/f")
    flow.begin()
    task.run()
    writer = CheckpointManager(bunch, checkpoint_file=tmp_path / "snapshot")
    good = json.loads(writer.build_payload())
    writer.backup_file.write_text(json.dumps(good))
    bad = copy.deepcopy(good)
    bad["bunch"]["name"] = "bad"
    t = bad["bunch"]["flows"][0]["children"][0]
    if corrupt == "unknown_type":
        t["type_id"] = "evil.Task"
    if corrupt == "missing_runtime":
        del t["trigger_free"]
    if corrupt == "bad_state":
        t["state"]["status"] = True
    if corrupt == "bad_limit":
        bad["bunch"]["flows"][0]["limits"][0]["value"] = 0
    if corrupt == "duplicate":
        bad["bunch"]["flows"].append(copy.deepcopy(bad["bunch"]["flows"][0]))
    if corrupt == "missing_password":
        bad["job_passwords"] = {}
    if corrupt == "unknown_password":
        bad["job_passwords"]["/missing"] = "SECRET"
    if corrupt == "version1":
        bad["format_version"] = 1
    writer.checkpoint_file.write_text(json.dumps(bad))
    live = Bunch("live", host="current", port="9000")
    live.add_flow("old")
    deployment = live.server_state
    reader = CheckpointManager(live, checkpoint_file=writer.checkpoint_file)
    assert reader.restore()
    assert live.name == "good" and list(live.flows) == ["f"]
    assert live.server_state is deployment
    restored = live.find_node("/f/t")
    assert restored.get_bunch() is live
    assert restored.job_password == task.job_password
    assert restored.in_limit_manager.in_limit_list[0].limit is live.flows["f"].limits[0]
    before = live.to_dict()
    reader.backup_file.unlink()
    assert not reader.restore()
    assert live.to_dict() == before


def test_forward_cross_flow_trigger_references_bind_to_candidate_tree():
    bunch = Bunch()
    left = bunch.add_flow("left").add_task("t")
    right = bunch.add_flow("right").add_task("t")
    right.add_event("ready")
    left.add_trigger("/right/t:ready == 1")
    built = build_definition(export_definition(bunch))
    rebuilt_left = built.find_node("/left/t")
    rebuilt_right = built.find_node("/right/t")
    assert (
        rebuilt_left.trigger_expression.ast.left.node._reference_node is rebuilt_right
    )
    assert not rebuilt_left.evaluate_trigger()
    rebuilt_right.events[0].value = True
    assert rebuilt_left.evaluate_trigger()
    assert not right.events[0].value


def test_codec_failure_does_not_disclose_values_or_mutate_online_tree():
    registry = builtin_registry()

    def broken(name, data):
        raise RuntimeError("SECRET_DATA")

    registry.register(registration(construct=broken))
    source = Bunch()
    source.add_flow("f").add_task(CustomTask("t", "SECRET_DATA"))
    doc = export_definition(source, registry=registry)
    live = Bunch()
    old = live.add_flow("keep")
    with pytest.raises(DefinitionError) as error:
        build_definition(doc, registry=registry, existing_bunch=live)
    assert "SECRET_DATA" not in str(error.value)
    assert live.flows == {"keep": old}


def test_explicit_factory_can_rebuild_a_registered_local_task():
    from takler.core import task
    from takler.schema.definition import DefinitionModel

    @task("local")
    def local():
        pass

    instance = local()

    class Empty(DefinitionModel):
        pass

    registry = builtin_registry()
    registry.register(
        NodeRegistration(
            "example.local",
            type(instance),
            "task",
            definition_schema=Empty,
            export_definition=lambda node: {},
            construct=lambda name, data: type(instance)(),
            runtime_schema=Empty,
            export_runtime=lambda node: {},
            restore_runtime=lambda node, data: None,
        )
    )
    flow = Flow("f")
    flow.add_task(instance)
    built = build_definition(
        export_definition(flow, registry=registry), registry=registry
    )
    assert type(built.children[0]) is type(instance)


@pytest.mark.parametrize(
    "change",
    [
        {"state": {"status": 8, "suspended": False}},
        {"try_no": -1},
        {"try_no": True},
        {"task_id": 3},
        {"trigger_free": True},
        {"calendar": {}},
        {"runtime_data": {"x": 1}},
        {"events": [{"name": "e", "initial_value": False}]},
        {"meters": [{"name": "m", "min_value": 0, "max_value": 1, "value": 2}]},
        {"times": [{"time": "06:00", "free": 1}]},
        {
            "repeat": {
                "r": {
                    "type_id": "takler.repeat.date",
                    "name": "D",
                    "start_date": "20260101",
                    "end_date": "20260103",
                    "step": 2,
                    "value": 20260102,
                }
            }
        },
    ],
)
def test_checkpoint_runtime_is_strict(change):
    bunch = Bunch()
    bunch.add_flow("f").add_task("t")
    data = export_runtime(bunch)
    data["flows"][0]["children"][0].update(change)
    with pytest.raises(DefinitionError):
        restore_runtime(data)


def test_definition_type_data_cannot_contain_nested_credentials():
    class Nested(DefinitionModel):
        options: dict[str, str]

    registry = builtin_registry()
    registry.register(
        registration(
            definition_schema=Nested,
            export_definition=lambda node: {"options": {"job_password": "SECRET"}},
        )
    )
    flow = Flow("f")
    flow.add_task(CustomTask("t", "cmd"))
    with pytest.raises(DefinitionError, match="forbidden_field"):
        export_definition(flow, registry=registry)


def test_register_rejects_wrong_kind_and_permissive_schema():
    from pydantic import BaseModel

    class Permissive(BaseModel):
        command: str

    registry = builtin_registry()
    for entry in (
        registration(kind="container"),
        registration(definition_schema=Permissive),
    ):
        with pytest.raises(DefinitionError, match="invalid_registration"):
            registry.register(entry)


def test_checkpoint_uses_registered_codec_for_shell_subclass():
    from takler.tasks.shell import ShellScriptTask

    class CustomShell(ShellScriptTask):
        def to_dict(self):
            pytest.fail("checkpoint called an uncontrolled node serializer")

    class ScriptDefinition(DefinitionModel):
        script: str

    class Empty(DefinitionModel):
        pass

    registry = builtin_registry()
    registry.register(
        NodeRegistration(
            "example.shell",
            CustomShell,
            "task",
            definition_schema=ScriptDefinition,
            export_definition=lambda node: {"script": str(node.script_path)},
            construct=lambda name, data: CustomShell(name, data.script),
            runtime_schema=Empty,
            export_runtime=lambda node: {},
            restore_runtime=lambda node, data: None,
        )
    )
    bunch = Bunch()
    bunch.add_flow("f").add_task(CustomShell("t", "/missing/script"))
    with using_registry(registry):
        snapshot = export_runtime(bunch)
    item = snapshot["flows"][0]["children"][0]
    assert "script_path" not in item
    assert item["type_data"] == {"script": "/missing/script"}
    restored = restore_runtime(snapshot, registry=registry).find_node("/f/t")
    assert type(restored) is CustomShell
    assert restored.script_path == "/missing/script"
