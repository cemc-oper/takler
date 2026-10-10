"""R3-06: a pinned state is complete and isolated across writer boundaries."""

import asyncio
import json
from datetime import datetime

import pytest
from pydantic import Field

from takler.core import Bunch, Flow, NodeStatus, RepeatDate, Task
from takler.protocol.commands import (
    Command,
    CompleteCommand,
    EventCommand,
    InitCommand,
    LoadCommand,
    MeterCommand,
    RequeueCommand,
    ReplaceCommand,
    RunCommand,
    ServerHaltCommand,
)
from takler.schema.definition import DefinitionModel
from takler.serialization.checkpoint import capture_checkpoint
from takler.serialization.definition import export_definition
from takler.serialization.registry import (
    NodeRegistration,
    builtin_registry,
    using_registry,
)
from takler.serialization.version import VersionStore, VersionUnavailable
from takler.server.handlers import CommandHandlers
from takler.server.checkpoint import CheckpointManager
from takler.server.scheduler import Scheduler
from takler.tasks.shell import ShellScriptTask


def _tree():
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    limit = flow.add_limit("slots", 1)
    task = flow.add_task(Task("t"))
    task.add_in_limit("slots", "/f")
    task.add_event("ready")
    task.add_meter("progress", 0, 100)
    flow.begin()
    return bunch, flow, task, limit


def test_attempt_limit_unknown_and_child_share_one_fixed_state():
    bunch, flow, task, limit = _tree()
    scheduler = Scheduler(bunch)
    before = scheduler.version_store.pin()
    scheduler.query_changes.begin()
    assert task.run()
    task.mark_submission_unknown("reply lost")
    with pytest.raises(VersionUnavailable, match="state_changing"):
        scheduler.version_store.pin()
    scheduler.query_changes.end()
    scheduler.version_store.flush()
    submitted = scheduler.version_store.pin()

    assert submitted.state_version == before.state_version + 1
    saved = submitted.checkpoint_document()
    live = capture_checkpoint(bunch)
    assert saved.definition == live.definition
    assert saved.runtime == live.runtime
    assert saved.secrets == live.secrets
    assert saved.runtime.nodes["/f/t"].submission_unknown_reason == "reply lost"
    assert saved.runtime.nodes["/f"].limit_usage["slots"].value == limit.value == 1
    assert before.checkpoint_document().runtime.nodes["/f/t"].attempt_id is None
    public = submitted.public_node("/f/t")
    assert public["submission_unknown_reason"] == "reply lost"
    assert task.job_password.encode() not in submitted.node("/f/t").public

    asyncio.run(
        scheduler.run_command_init(
            InitCommand(node_path="/f/t", attempt_id=task.attempt_id, task_id="rid")
        )
    )
    active = scheduler.version_store.pin()
    assert active.public_node("/f/t")["task_id"] == "rid"
    assert (
        active.checkpoint_document().runtime.nodes["/f"].limit_usage["slots"].value == 1
    )
    assert submitted.public_node("/f/t")["submission_unknown_reason"] == "reply lost"


def test_structure_replacement_preserves_old_root_and_paths():
    bunch, _, _, _ = _tree()
    scheduler = Scheduler(bunch)
    old = scheduler.version_store.pin()
    loaded = Flow("new")
    loaded.add_task("child")
    scheduler.run_command_load(
        LoadCommand(flow_bytes=export_definition(loaded).model_dump_json().encode())
    )
    after_load = scheduler.version_store.pin()
    assert "/new/child" in after_load.index
    assert after_load.public_node("/")["child_paths"] == ["/f", "/new"]

    replacement = Flow("f")
    replacement.add_task("other")
    scheduler.run_command_replace(
        ReplaceCommand(
            target_path="/f",
            flow_bytes=export_definition(replacement).model_dump_json().encode(),
        )
    )
    replaced = scheduler.version_store.pin()
    assert "/f/t" not in replaced.index
    assert "/f/other" in replaced.index
    assert "/f/t" in old.index and "/new" not in old.index
    assert set(replaced.checkpoint_document().runtime.nodes) == set(replaced.index)


def test_same_path_child_object_replacement_refreshes_record():
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task(Task("t"))
    task.add_event("old")
    scheduler = Scheduler(bunch)
    before = scheduler.version_store.pin()
    replacement = Task("t")
    replacement.add_event("new")
    replacement.definition_generation = flow.definition_generation
    flow.update_child("t", replacement)
    after = scheduler.version_store.pin()
    assert "old" in before.checkpoint_document().runtime.nodes["/f/t"].event_values
    assert "new" in after.checkpoint_document().runtime.nodes["/f/t"].event_values


def test_child_meter_event_completion_requeue_and_parent_aggregation():
    bunch, _, task, limit = _tree()
    scheduler = Scheduler(bunch)
    assert task.run()
    submitted = scheduler.version_store.pin()
    attempt = task.attempt_id
    scheduler.run_command_event(
        EventCommand(
            node_path="/f/t",
            source_task_path="/f/t",
            attempt_id=attempt,
            event_name="ready",
        )
    )
    scheduler.run_command_meter(
        MeterCommand(
            node_path="/f/t",
            source_task_path="/f/t",
            attempt_id=attempt,
            meter_name="progress",
            meter_value=65,
        )
    )
    changed = scheduler.version_store.pin().checkpoint_document()
    assert changed.runtime.nodes["/f/t"].event_values["ready"] is True
    assert changed.runtime.nodes["/f/t"].meter_values["progress"] == 65
    scheduler.run_command_complete(
        CompleteCommand(node_path="/f/t", attempt_id=attempt)
    )
    complete = scheduler.version_store.pin()
    saved = complete.checkpoint_document()
    assert saved.runtime.nodes["/f/t"].state.status == NodeStatus.complete.value
    assert saved.runtime.nodes["/f"].limit_usage["slots"].value == limit.value == 0
    assert complete.public_node("/f")["state"]["status"] == NodeStatus.complete.value
    assert (
        submitted.checkpoint_document().runtime.nodes["/f/t"].state.status
        == NodeStatus.submitted.value
    )
    scheduler.run_command_requeue(RequeueCommand(node_paths=["/f/t"]))
    requeued = scheduler.version_store.pin().checkpoint_document()
    assert requeued.runtime.nodes["/f/t"].event_values["ready"] is False
    assert requeued.runtime.nodes["/f/t"].meter_values["progress"] == 0


def test_calendar_repeat_free_dependency_and_service_are_versioned():
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task(Task("t"))
    task.add_time("12:34")
    task.add_repeat(RepeatDate("DAY", 20260101, 20260102))
    scheduler = Scheduler(bunch)
    old = scheduler.version_store.pin()
    flow.begin()
    task.free_dependencies("time")
    prepared = scheduler.version_store.pin().checkpoint_document()
    assert prepared.runtime.nodes["/f"].calendar is not None
    assert prepared.runtime.nodes["/f/t"].time_free["12:34"] is True
    assert prepared.runtime.nodes["/f/t"].repeat_value == 20260101
    scheduler.run_command_server_halt(ServerHaltCommand())
    halted = scheduler.version_store.pin()
    assert halted.public_node("/")["server_status"]["status"] == "halted"
    assert old.public_node("/")["server_status"]["status"] == "running"


def test_automatic_flow_processing_and_checkpoint_outcome_commit_once(tmp_path):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    flow.add_task(Task("t"))
    flow.begin()
    scheduler = Scheduler(bunch)
    before = scheduler.version_store.pin()
    scheduler.query_changes.begin()
    scheduler._process_flow("f", flow, datetime.now())
    scheduler.query_changes.end()
    scheduler.version_store.flush()
    current = scheduler.version_store.pin()
    assert current.state_version == before.state_version + 1
    assert (
        current.checkpoint_document().runtime.nodes["/f/t"].state.status
        == NodeStatus.submitted.value
    )

    manager = CheckpointManager(bunch, checkpoint_file=tmp_path / "state.check")
    manager.query_scheduler = scheduler
    scheduler.checkpoint_manager = manager
    manager._record_outcome(1, False, False, "", "storage full")
    failed = scheduler.version_store.pin()
    assert failed.public_node("/")["server_status"]["status"] == "halted"
    assert failed.checkpoint_document().runtime.server_state.status == "halted"
    assert current.public_node("/")["server_status"]["status"] == "running"


def test_ancestor_context_and_generated_paths_use_fixed_values(tmp_path):
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    flow.add_parameter("TAKLER_HOME", str(tmp_path / "old"))
    task = flow.add_task(ShellScriptTask("t", "script.takler"))
    scheduler = Scheduler(bunch)
    old = scheduler.version_store.pin()
    flow.add_parameter("TAKLER_HOME", str(tmp_path / "new"))
    new = scheduler.version_store.pin()

    def job(version):
        row = version.public_node("/f/t")
        return next(
            item["value"]
            for item in row["generated_parameters"]
            if item["name"] == "TAKLER_JOB"
        )

    assert str(tmp_path / "old") in job(old)
    assert str(tmp_path / "new") in job(new)
    assert (
        str(tmp_path / "old")
        in old.public_node("/f/t")["ancestor_context"][0]["user"][0]["value"]
    )
    assert task.node_path == "/f/t"
    hidden = Scheduler(bunch, query_redacted_parameters=("TAKLER_HOME",))
    hidden_row = hidden.version_store.pin().public_node("/f/t")
    assert str(tmp_path / "new") not in json.dumps(hidden_row)
    assert (
        next(
            item["value"]
            for item in hidden_row["generated_parameters"]
            if item["name"] == "TAKLER_JOB"
        )
        == "<redacted>"
    )


def test_derived_shell_paths_respect_individual_redaction(tmp_path):
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    flow.add_parameter("TAKLER_HOME", str(tmp_path))
    flow.add_task(ShellScriptTask("t", "script.takler"))
    scheduler = Scheduler(
        bunch, query_redacted_parameters=("TAKLER_JOB", "TAKLER_JOBOUT")
    )
    public = scheduler.version_store.pin().public_node("/f/t")
    generated = {item["name"]: item["value"] for item in public["generated_parameters"]}
    assert generated["TAKLER_JOB"] == "<redacted>"
    assert generated["TAKLER_JOBOUT"] == "<redacted>"


def test_partial_failure_publishes_actual_side_effect():
    bunch, _, task, _ = _tree()
    scheduler = Scheduler(bunch)
    original = scheduler.version_store.pin()

    def partial(path, force=False):
        task.set_node_status(NodeStatus.queued)
        task.set_meter("progress", 42)
        raise RuntimeError("after mutation")

    scheduler._run_node = partial
    result = asyncio.run(
        CommandHandlers(scheduler).handle(Command.RUN, RunCommand(node_paths=["/f/t"]))
    )
    assert result.flag != 0
    fixed = scheduler.version_store.pin()
    assert fixed.state_version == original.state_version + 1
    assert (
        fixed.checkpoint_document().runtime.nodes["/f/t"].meter_values["progress"] == 42
    )
    assert (
        original.checkpoint_document().runtime.nodes["/f/t"].meter_values["progress"]
        == 0
    )


def test_nested_plugin_runtime_is_frozen_and_private():
    class PluginTask(Task):
        def __init__(self, name, command):
            super().__init__(name)
            self.command = command
            self.payload = {"nested": [1, 2]}

    class PluginDefinition(DefinitionModel):
        command: str

    class PluginRuntime(DefinitionModel):
        payload: dict[str, list[int]] = Field(default_factory=dict)

    registry = builtin_registry()
    registry.register(
        NodeRegistration(
            type_id="example.version",
            python_type=PluginTask,
            kind="task",
            definition_schema=PluginDefinition,
            export_definition=lambda node: {"command": node.command},
            construct=lambda name, data: PluginTask(name, data.command),
            runtime_schema=PluginRuntime,
            export_runtime=lambda node: {"payload": node.payload},
            restore_runtime=lambda node, data: setattr(node, "payload", data.payload),
        )
    )
    with using_registry(registry):
        bunch = Bunch()
        plugin = bunch.add_flow(Flow("f")).add_task(PluginTask("p", "echo"))
        scheduler = Scheduler(bunch)
        old = scheduler.version_store.pin()
        plugin.payload["nested"].append(3)
        scheduler.notify_query_change("/f/p", "runtime")
        new = scheduler.version_store.pin()
        assert json.loads(old.node("/f/p").runtime)["runtime_data"]["payload"] == {
            "nested": [1, 2]
        }
        assert json.loads(new.node("/f/p").runtime)["runtime_data"]["payload"] == {
            "nested": [1, 2, 3]
        }
        assert new.checkpoint_document(registry=registry).runtime.nodes[
            "/f/p"
        ].runtime_data == {"payload": {"nested": [1, 2, 3]}}
        with pytest.raises(VersionUnavailable, match="unsupported_plugin_detail"):
            new.public_node("/f/p")


def test_failed_update_never_exposes_half_version():
    bunch = Bunch()
    task = bunch.add_flow(Flow("f")).add_task(Task("t"))
    scheduler = Scheduler(bunch)
    old = scheduler.version_store.pin()
    task.add_parameter("bad", "ok")
    task.user_parameters["bad"].value = object()
    with pytest.raises(VersionUnavailable, match="capture_failed"):
        scheduler.version_store.pin()
    assert old.public_node("/f/t")["user_parameters"] == []
    task.user_parameters["bad"].value = "fixed"
    scheduler.version_store.rebuild(state_version=scheduler.query_changes.revision)
    assert scheduler.version_store.pin().public_node("/f/t")["user_parameters"] == [
        {"name": "bad", "value": "fixed"}
    ]


def test_initial_budget_has_explicit_not_ready_state():
    store = VersionStore(Bunch(), status_provider=lambda: {}, max_bytes=1)
    assert not store.ready
    assert store.error == "version_memory_limit"
    with pytest.raises(VersionUnavailable, match="version_memory_limit"):
        store.pin()


def test_query_journal_overflow_does_not_drop_version_changes():
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    tasks = [flow.add_task(f"t{index:04d}") for index in range(2100)]
    scheduler = Scheduler(bunch)
    before = scheduler.version_store.pin()
    scheduler.query_changes.begin()
    for task in tasks:
        task.set_node_status_only(NodeStatus.complete)
    scheduler.query_changes.end()
    scheduler.version_store.flush()
    after = scheduler.version_store.pin()
    assert scheduler.query_changes.since(before.state_version)[0].changes[0].path == "/"
    assert before.public_node("/f/t2099")["state"]["status"] == NodeStatus.unknown.value
    assert after.public_node("/f/t0000")["state"]["status"] == NodeStatus.complete.value
    assert after.public_node("/f/t2099")["state"]["status"] == NodeStatus.complete.value
