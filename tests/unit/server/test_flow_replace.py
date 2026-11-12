"""R0-13 isolated preparation, final gate, references and checkpoint semantics."""

import asyncio
import json
from unittest.mock import Mock

import pytest

from takler.core import Bunch, Flow, NodeStatus, RepeatDate
from takler.exceptions import (
    ExpressionSyntaxError,
    FlowStateError,
    InvalidNodePathError,
    InvalidRequestError,
    NodeNotFoundError,
    NodeTypeError,
    UnsupportedValueError,
)
from takler.protocol.commands import ReplaceCommand
from takler.serialization import export_definition
from takler.server.scheduler import Scheduler
from takler.server import flow_replace
from takler.server.checkpoint import CheckpointManager
from takler.tasks import ShellScriptTask
from takler.tasks.shell.shell_runner import ShellRunner


def definition(name="f", **fields):
    return dict(
        kind="takler.definition",
        schema_version=1,
        root=dict(type_id="takler.flow", name=name, **fields),
    )


def replace(bunch, data=None, path="/f"):
    data = definition() if data is None else data
    if isinstance(data, Flow):
        data = export_definition(data).model_dump(mode="json")
    return Scheduler(bunch).run_command_replace(
        ReplaceCommand(
            target_path=path,
            flow_bytes=data if isinstance(data, bytes) else json.dumps(data).encode(),
        )
    )


@pytest.mark.parametrize(
    "status",
    [NodeStatus.unknown, NodeStatus.queued, NodeStatus.complete, NodeStatus.aborted],
)
@pytest.mark.parametrize("begun", [False, True])
@pytest.mark.parametrize("suspended", [False, True])
@pytest.mark.parametrize("default", ["queued", "complete"])
def test_replace_initializes_new_definition(status, begun, suspended, default):
    bunch = Bunch()
    bunch.add_parameter("ROOT", "inherited")
    old = bunch.add_flow("f")
    task = old.add_task("t")
    if begun:
        old.begin()
    old.default_node_status = (
        NodeStatus.complete if default == "queued" else NodeStatus.queued
    )
    old.state.node_status = status
    old.state.suspended = suspended
    task.state.suspended = True
    task.task_id, task.try_no, task.job_password = "old", 7, "SECRET"
    new = Flow("f")
    new.default_node_status = NodeStatus[default]
    task2 = new.add_task("t")
    task2.add_event("ready", True)
    task2.add_meter("progress", 10, 20)
    task2.add_repeat(RepeatDate("D", "20260101", "20260103"))
    task2.add_time("06:00")
    task2.add_trigger("/f/t == complete", parse=False)
    task2.add_complete_trigger("/f/t:ready == 1", parse=False)
    result = replace(bunch, new)
    loaded = bunch.find_flow("f")
    current = loaded.children[0]
    assert (
        result.flag == 0
        and result.message == "flow replaced in memory; checkpoint pending"
    )
    assert loaded is not old and loaded.bunch is bunch and old.bunch is None
    assert loaded.begun and loaded.calendar.initial_time is not None
    assert loaded.state.suspended is suspended
    assert current.state.node_status is NodeStatus[default]
    assert not current.state.suspended
    assert (
        current.task_id,
        current.try_no,
        current.job_password,
        current.aborted_reason,
    ) == (None, 0, None, None)
    assert current.events[0].value is True and current.meters[0].value == 10
    assert current.repeat.r.value == 20260101
    assert not current.times[0].free and not current.trigger_expression.free
    assert (
        not current.complete_trigger_expression.free
        and not current.is_complete_triggered
    )
    assert current.find_parent_parameter("ROOT").value == "inherited"


@pytest.mark.parametrize("status", [NodeStatus.active, NodeStatus.submitted])
@pytest.mark.parametrize("location", ["root", "child", "deep"])
@pytest.mark.parametrize("suspended", [False, True])
def test_every_running_node_blocks_even_when_aggregate_hides_it(
    status, location, suspended
):
    bunch = Bunch()
    old = bunch.add_flow("f")
    child = old.add_container("g")
    deep = child.add_task("t")
    old.state.node_status = NodeStatus.aborted
    old.state.suspended = suspended
    dict(root=old, child=child, deep=deep)[location].state.node_status = status
    before = bunch.to_dict()
    with pytest.raises(FlowStateError, match="running node"):
        replace(bunch)
    assert bunch.to_dict() == before and bunch.find_flow("f") is old


@pytest.mark.parametrize(
    "path,error",
    [
        ("", InvalidNodePathError),
        ("f", InvalidNodePathError),
        ("/", InvalidNodePathError),
        ("/f/", InvalidNodePathError),
        ("/f//t", InvalidNodePathError),
        ("/f/../t", InvalidNodePathError),
        ("/f/./t", InvalidNodePathError),
        ("/f:t", InvalidNodePathError),
        ("/f\0", InvalidNodePathError),
        ("/missing", NodeNotFoundError),
        ("/f/missing", NodeNotFoundError),
        ("/f/t", NodeTypeError),
    ],
)
def test_invalid_target(path, error):
    bunch = Bunch()
    bunch.add_flow("f").add_task("t")
    before = bunch.to_dict()
    with pytest.raises(error):
        replace(bunch, path=path)
    assert bunch.to_dict() == before


@pytest.mark.parametrize(
    "data,error",
    [
        (b"bad json", InvalidRequestError),
        ({"name": "f"}, InvalidRequestError),
        (definition("different"), InvalidRequestError),
        (definition(begun=True), InvalidRequestError),
        (dict(definition(), schema_version=2), UnsupportedValueError),
        (
            dict(definition(), root=dict(type_id="evil.flow", name="f")),
            UnsupportedValueError,
        ),
        (
            dict(definition(), root=dict(type_id="takler.bunch", name="b", flows=[])),
            InvalidRequestError,
        ),
        (definition(trigger="???"), ExpressionSyntaxError),
        (definition(trigger="/missing == complete"), InvalidRequestError),
    ],
)
def test_invalid_definition_leaves_tree_untouched(data, error):
    bunch = Bunch()
    old = bunch.add_flow("f")
    before = bunch.to_dict()
    with pytest.raises(error):
        replace(bunch, data)
    assert bunch.to_dict() == before and bunch.find_flow("f") is old


@pytest.mark.parametrize(
    "change", ["active", "submitted", "identity", "suspended", "reservation"]
)
def test_final_gate_observes_changes_after_build(monkeypatch, change):
    bunch = Bunch()
    old = bunch.add_flow("f")
    task = old.add_task("t")
    old.add_limit("pool", 3)
    original = flow_replace.build_definition

    def build(*a, **kw):
        result = original(*a, **kw)
        if change == "identity":
            bunch.add_flow("f")
        elif change == "suspended":
            old.suspend()
        elif change == "reservation":
            old.limits[0].increment(1, "/other/task")
        else:
            task.state.node_status = NodeStatus[change]
        return result

    monkeypatch.setattr(flow_replace, "build_definition", build)
    if change == "suspended":
        replace(bunch)
        assert bunch.find_flow("f").state.suspended
    else:
        with pytest.raises(FlowStateError):
            replace(bunch)
        assert (
            bunch.find_flow("f") is old
            if change != "identity"
            else bunch.find_flow("f") is not old
        )


def reference_tree():
    bunch = Bunch()
    old = bunch.add_flow("f")
    old.add_limit("pool", 3)
    old.add_event("ready", False)
    consumer = bunch.add_flow("consumer").add_task("t")
    consumer.add_in_limit("pool", "/f", 2)
    consumer.in_limit_manager.resolve_in_limit_references()
    consumer.add_trigger("/f:ready == 1", parse=False)
    consumer.add_complete_trigger("/f == complete", parse=False)
    from takler.serialization.builder import bind_expressions

    bind_expressions(bunch)
    return bunch, old, consumer


def test_updates_external_trigger_variable_and_limit_caches():
    bunch, old, consumer = reference_tree()
    old_ast = consumer.trigger_expression.ast
    new = Flow("f")
    new.add_limit("pool", 4)
    new.add_event("ready", True)
    new.default_node_status = NodeStatus.complete
    replace(bunch, new)
    current = bunch.find_flow("f")
    assert consumer.trigger_expression.ast is not old_ast
    assert consumer.trigger_expression.evaluate()
    assert consumer.complete_trigger_expression.evaluate()
    assert consumer.in_limit_manager.in_limit_list[0].limit is current.limits[0]
    consumer.increment_in_limit(set())
    assert current.limits[0].value == 2 and old.limits[0].value == 0


@pytest.mark.parametrize("missing", ["limit", "capacity", "event", "node"])
def test_reference_failure_preserves_old_caches(missing):
    bunch, old, consumer = reference_tree()
    if missing == "node":
        old.add_task("child")
        consumer.add_trigger("/f/child == complete", parse=False)
    before = bunch.to_dict()
    ast = consumer.trigger_expression.ast
    new = Flow("f")
    if missing != "limit":
        new.add_limit("pool", 1 if missing == "capacity" else 3)
    if missing != "event":
        new.add_event("ready", True)
    with pytest.raises(InvalidRequestError):
        replace(bunch, new)
    assert bunch.to_dict() == before
    assert consumer.trigger_expression.ast is ast
    assert consumer.in_limit_manager.in_limit_list[0].limit is old.limits[0]


@pytest.mark.parametrize("external", [False, True])
def test_occupied_limits_are_not_lost(external):
    bunch, old, _ = reference_tree()
    holder = bunch.add_flow("provider") if external else old
    limit = holder.add_limit("extra", 2)
    limit.increment(1, "/f/t" if external else "/consumer/t")
    before = bunch.to_dict()
    with pytest.raises(FlowStateError):
        replace(bunch, export_definition(old).model_dump(mode="json"))
    assert bunch.to_dict() == before


def test_initialization_failure_is_safe_and_offline(monkeypatch):
    bunch = Bunch()
    old = bunch.add_flow("f")
    provider = bunch.add_flow("provider")
    provider.add_limit("pool", 2)
    before = bunch.to_dict()

    def fail(flow, *a, **kw):
        assert flow.bunch is None
        assert flow.in_limit_manager.in_limit_list[0].limit is None
        raise RuntimeError("SECRET")

    monkeypatch.setattr(Flow, "begin", fail)
    with pytest.raises(RuntimeError, match="preparation failed") as error:
        replace(
            bunch,
            definition(in_limits=[dict(limit_name="pool", node_path="/provider")]),
        )
    assert "SECRET" not in str(error.value)
    assert bunch.to_dict() == before and bunch.find_flow("f") is old


def test_stale_shell_callback_after_real_replace(monkeypatch):
    bunch = Bunch()
    old = bunch.add_flow("f")
    task = old.add_task(ShellScriptTask("t"))
    callbacks = []
    monkeypatch.setattr(task, "create_job_script", lambda: "unused")
    monkeypatch.setattr(
        ShellRunner, "spawn", lambda self, **kw: callbacks.append(kw["on_failure"])
    )
    old.begin()
    task.run()
    # Leave the token intact to exercise ownership independently of token reset.
    task.state.node_status = NodeStatus.complete
    old.state.node_status = NodeStatus.complete
    new = Flow("f")
    new.add_task(ShellScriptTask("t"))
    replace(bunch, new)
    before = bunch.to_dict()
    task.on_job_failure = Mock()
    callbacks[0](RuntimeError("late"))
    task.on_job_failure.assert_not_called()
    assert bunch.to_dict() == before


def test_success_precedes_checkpoint_and_later_save_restores_new_tree(tmp_path):
    bunch = Bunch()
    bunch.add_flow("f")
    path = tmp_path / "state.check"
    manager = CheckpointManager(bunch=bunch, checkpoint_file=path)
    new = Flow("f")
    new.add_task("new")
    assert replace(bunch, new).flag == 0
    assert not path.exists()

    async def save_periodically():
        # Exercise the actual periodic loop, stopping after its first write.
        saved = asyncio.Event()
        original = manager.write_checkpoint_async

        async def write():
            result = await original()
            saved.set()
            return result

        manager.interval = 0.01
        manager.write_checkpoint_async = write
        await manager.start()
        await asyncio.wait_for(saved.wait(), timeout=5)
        manager._snapshot_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await manager._snapshot_task

    asyncio.run(save_periodically())
    restored = Bunch()
    assert CheckpointManager(bunch=restored, checkpoint_file=path).restore()
    assert restored.find_flow("f").begun
    assert restored.find_node("/f/new") is not None


def test_repeated_replace_reinitializes_instead_of_deduplicating():
    bunch = Bunch()
    bunch.add_flow("f")
    replace(bunch)
    first = bunch.find_flow("f")
    replace(bunch)
    assert bunch.find_flow("f") is not first
    assert bunch.find_flow("f").calendar.initial_time >= first.calendar.initial_time


def test_candidate_begin_does_not_touch_external_limit_or_submit(monkeypatch):
    from pathlib import Path
    from takler.core import Task

    bunch = Bunch()
    bunch.add_flow("f")
    provider = bunch.add_flow("provider")
    provider.add_limit("pool", 3)
    provider.limits[0].increment(1, "/provider/active")
    before = provider.to_dict()

    def forbidden(*a, **kw):
        pytest.fail("replace performed IO or submitted a job")

    monkeypatch.setattr(Task, "run", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    monkeypatch.setattr(ShellRunner, "spawn", forbidden)
    new = Flow("f")
    new.add_task(ShellScriptTask("t")).add_in_limit("pool", "/provider")
    replace(bunch, new)
    assert provider.to_dict() == before
    assert (
        bunch.find_node("/f/t").in_limit_manager.in_limit_list[0].limit
        is provider.limits[0]
    )


def test_final_gate_runs_after_reference_preparation(monkeypatch):
    bunch, old, consumer = reference_tree()
    before_ast = consumer.trigger_expression.ast
    original = flow_replace._reference_updates

    def prepare(*args):
        updates = original(*args)
        old.state.node_status = NodeStatus.active
        return updates

    monkeypatch.setattr(flow_replace, "_reference_updates", prepare)
    with pytest.raises(FlowStateError):
        replace(bunch, export_definition(old).model_dump(mode="json"))
    assert bunch.find_flow("f") is old
    assert consumer.trigger_expression.ast is before_ast
    assert consumer.in_limit_manager.in_limit_list[0].limit is old.limits[0]


def test_registered_constructor_failure_maps_to_safe_internal_error():
    from takler.core import Task
    from takler.schema.definition import DefinitionModel
    from takler.serialization import builtin_registry, NodeRegistration
    from takler.serialization.registry import using_registry

    class CustomTask(Task):
        pass

    class Empty(DefinitionModel):
        pass

    def fail(name, data):
        raise RuntimeError("PRIVATE_VALUE")

    registry = builtin_registry()
    registry.register(
        NodeRegistration(
            type_id="example.task",
            python_type=CustomTask,
            kind="task",
            definition_schema=Empty,
            construct=fail,
        )
    )
    bunch = Bunch()
    old = bunch.add_flow("f")
    with using_registry(registry):
        with pytest.raises(RuntimeError, match="preparation failed") as error:
            replace(
                bunch,
                definition(
                    children=[dict(type_id="example.task", name="t", type_data={})]
                ),
            )
    assert "PRIVATE_VALUE" not in str(error.value)
    assert bunch.find_flow("f") is old


def test_reference_preparation_failure_keeps_all_online_caches(monkeypatch):
    bunch, old, consumer = reference_tree()
    before = bunch.to_dict()
    old_ast = consumer.trigger_expression.ast
    original = flow_replace._reference_updates

    def fail(*args):
        original(*args)
        raise RuntimeError("injected failure after reference preparation")

    monkeypatch.setattr(flow_replace, "_reference_updates", fail)
    with pytest.raises(RuntimeError):
        replace(bunch, export_definition(old).model_dump(mode="json"))
    assert bunch.to_dict() == before
    assert consumer.trigger_expression.ast is old_ast
    assert consumer.in_limit_manager.in_limit_list[0].limit is old.limits[0]


@pytest.mark.parametrize(
    "root",
    [
        {"name": "f"},
        {"type_id": 7, "name": "f"},
        {"type_id": "takler.task", "name": "f"},
        {"type_id": "takler.container", "name": "f"},
        {
            "type_id": "takler.flow",
            "name": "f",
            "children": [{"type_id": "takler.flow", "name": "nested"}],
        },
        {
            "type_id": "takler.bunch",
            "name": "b",
            "flows": [
                {"type_id": "takler.flow", "name": "f"},
                {"type_id": "takler.flow", "name": "g"},
            ],
        },
    ],
)
def test_bad_structure_is_request_error_not_unsupported_type(root):
    bunch = Bunch()
    old = bunch.add_flow("f")
    with pytest.raises(InvalidRequestError) as error:
        replace(bunch, dict(definition(), root=root))
    assert type(error.value) is InvalidRequestError
    assert bunch.find_flow("f") is old
