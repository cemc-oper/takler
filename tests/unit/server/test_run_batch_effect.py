"""RUN acceptance skips unused success evidence, preserving failure effects."""

import asyncio
from unittest.mock import Mock

import pytest

from takler.core import Bunch, Task, NodeStatus
from takler.exceptions import FlowStateError
from takler.protocol.commands import Command, RunCommand, SuspendCommand
from takler.schema.definition import DefinitionModel
from takler.serialization.registry import (
    NodeRegistration,
    builtin_registry,
    using_registry,
)
from takler.server import batch
from takler.server.connect_config import ExceptionPolicy
from takler.server.handlers import CommandHandlers
from takler.server.scheduler import Scheduler


@pytest.fixture
def extension():
    class PluginTask(Task):
        pass

    class PluginDefinition(DefinitionModel):
        pass

    registry = builtin_registry()
    registry.register(
        NodeRegistration(
            "example.run_effect",
            PluginTask,
            "task",
            definition_schema=PluginDefinition,
            export_definition=lambda node: {},
            construct=lambda name, data: PluginTask(name),
        )
    )
    with using_registry(registry):
        bunch = Bunch()
        flow = bunch.add_flow("f")
        source = flow.add_task(PluginTask("a"))
        following = flow.add_task("b")
        unrelated = flow.add_task("c")
        flow.begin()
        yield Scheduler(bunch), source, following, unrelated


def test_successful_run_retains_only_pre_snapshot(extension, monkeypatch):
    scheduler, source, _, _ = extension
    original = batch._snapshot
    snapshot = Mock(wraps=original)
    monkeypatch.setattr(batch, "_snapshot", snapshot)
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.flag == 0 and result.results[0].effect == "applied"
    assert source.try_no == 1 and source.attempt_id and source.job_password
    assert source.state.node_status is NodeStatus.submitted
    assert snapshot.call_count == 1


def test_successful_custom_run_without_serialized_change_is_applied(
    extension, monkeypatch
):
    scheduler, source, _, _ = extension
    monkeypatch.setattr(source, "run", lambda: True)
    before = scheduler.bunch.to_dict()
    snapshot = Mock(wraps=batch._snapshot)
    monkeypatch.setattr(batch, "_snapshot", snapshot)
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.results[0].effect == "applied" and result.flag == 0
    assert scheduler.bunch.to_dict() == before
    assert snapshot.call_count == 1


def test_post_success_serializer_failure_is_irrelevant(extension, monkeypatch):
    scheduler, source, _, _ = extension
    original = source.to_dict
    calls = []

    def serializer():
        calls.append(source.attempt_id)
        if source.attempt_id:
            raise RuntimeError("post-operation serialization unavailable")
        return original()

    monkeypatch.setattr(source, "to_dict", serializer)
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.flag == 0 and result.results[0].effect == "applied"
    assert calls == [None]


@pytest.mark.parametrize("hook", ["before_run", "do_run", "after_run"])
def test_known_extension_failure_keeps_none_or_partial_and_continues(
    extension, monkeypatch, hook
):
    scheduler, source, following, _ = extension

    def reject():
        raise FlowStateError("rejected")

    monkeypatch.setattr(source, hook, reject)
    result = scheduler.run_command_run(
        RunCommand(node_paths=[source.node_path, following.node_path])
    )
    assert [r.flag for r in result.results] == [14, 0]
    assert [r.effect for r in result.results] == [
        "none" if hook == "before_run" else "partial",
        "applied",
    ]
    assert source.try_no == (0 if hook == "before_run" else 1)
    assert (source.attempt_id is None) == (hook == "before_run")
    assert following.state.node_status is NodeStatus.submitted


def test_untracked_off_target_extension_mutation_remains_partial(
    extension, monkeypatch
):
    scheduler, source, following, unrelated = extension

    def reject_after_mutation():
        unrelated.state.suspended = (
            True  # No query-change hint; full evidence is required.
        )
        raise FlowStateError("failed after changing another target")

    monkeypatch.setattr(source, "before_run", reject_after_mutation)
    snapshot = Mock(wraps=batch._snapshot)
    monkeypatch.setattr(batch, "_snapshot", snapshot)
    result = scheduler.run_command_run(
        RunCommand(node_paths=[source.node_path, following.node_path])
    )
    assert [r.flag for r in result.results] == [14, 0]
    assert [r.effect for r in result.results] == ["partial", "applied"]
    assert [r.index for r in result.results] == [0, 1]
    assert [r.target for r in result.results] == [source.node_path, following.node_path]
    assert source.try_no == 0 and source.attempt_id is None
    assert unrelated.state.suspended  # No rollback of an extension's effects.
    assert following.state.node_status is NodeStatus.submitted
    assert snapshot.call_count == 3  # Failed item before/after, successful item before.


def test_aliased_extension_data_is_still_deepcopied(extension, monkeypatch):
    scheduler, source, _, _ = extension
    bag = {"value": 0}
    original = source.to_dict

    def serializer():
        return {**original(), "extension_bag": bag}

    def reject_after_mutation():
        bag["value"] = 1
        raise FlowStateError("changed extension data")

    monkeypatch.setattr(source, "to_dict", serializer)
    monkeypatch.setattr(source, "before_run", reject_after_mutation)
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.results[0].effect == "partial"
    assert bag["value"] == 1 and source.try_no == 0


@pytest.mark.parametrize("snapshots", [[None, {}], [{}, None, {}]])
def test_unavailable_failure_evidence_remains_unknown(
    extension, monkeypatch, snapshots
):
    scheduler, source, following, _ = extension

    def reject():
        raise FlowStateError("rejected")

    monkeypatch.setattr(source, "before_run", reject)
    monkeypatch.setattr(batch, "_snapshot", Mock(side_effect=snapshots))
    result = scheduler.run_command_run(
        RunCommand(node_paths=[source.node_path, following.node_path])
    )
    assert [r.effect for r in result.results] == ["unknown", "applied"]
    assert [r.flag for r in result.results] == [14, 0]
    assert following.state.node_status is NodeStatus.submitted


@pytest.mark.parametrize("hook", ["before_run", "do_run", "after_run"])
def test_unexpected_extension_failure_delays_fail_fast_and_keeps_effects(
    extension, monkeypatch, hook
):
    scheduler, source, following, unrelated = extension
    events = []

    def fail():
        events.append("failed")
        unrelated.state.suspended = True
        raise RuntimeError("SECRET-MUST-NOT-LEAK")

    def succeed():
        events.append("continued")
        return True

    monkeypatch.setattr(source, hook, fail)
    monkeypatch.setattr(following, "do_run", succeed)
    audit = Mock()
    handler = CommandHandlers(
        scheduler,
        exception_policy=ExceptionPolicy.FAIL_FAST,
        fatal_shutdown=lambda: events.append("shutdown"),
        audit_logger=audit,
    )
    result = asyncio.run(
        handler.handle(
            Command.RUN, RunCommand(node_paths=[source.node_path, following.node_path])
        )
    )
    assert [r.flag for r in result.results] == [99, 0]
    assert [r.effect for r in result.results] == ["unknown", "applied"]
    assert events == ["failed", "continued", "shutdown"]
    assert source.attempt_id and unrelated.state.suspended
    assert following.state.node_status is NodeStatus.submitted
    assert (
        "SECRET"
        not in result.model_dump_json() + audit.record.call_args.args[0].to_json_line()
    )


def test_malformed_target_has_no_snapshot_and_next_target_still_runs(
    extension, monkeypatch
):
    scheduler, _, following, _ = extension
    snapshot = Mock(wraps=batch._snapshot)
    monkeypatch.setattr(batch, "_snapshot", snapshot)
    result = scheduler.run_command_run(
        RunCommand(node_paths=["bad", following.node_path])
    )
    assert [r.flag for r in result.results] == [11, 0]
    assert [r.effect for r in result.results] == ["none", "applied"]
    assert snapshot.call_count == 1


def test_successful_non_run_still_compares_before_and_after(extension, monkeypatch):
    scheduler, _, following, _ = extension
    following.state.suspended = True
    snapshot = Mock(wraps=batch._snapshot)
    monkeypatch.setattr(batch, "_snapshot", snapshot)
    result = scheduler.run_command_suspend(
        SuspendCommand(node_paths=[following.node_path])
    )
    assert result.flag == 0 and result.results[0].effect == "none"
    assert snapshot.call_count == 2
