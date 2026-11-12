"""Publication must reject invalid live views before touching valid files."""

import asyncio
from uuid import uuid4

import pytest

from takler.core import Bunch, HaltCause, NodeStatus
from takler.core.bunch import ServerState
from takler.protocol.commands import CompleteCommand, ServerResumeCommand
from takler.serialization.checkpoint import capture_checkpoint, restore_checkpoint
from takler.server.checkpoint import CheckpointManager
from takler.server.scheduler import Scheduler


def _running_manager(tmp_path):
    bunch = Bunch()
    flow = bunch.add_flow("f")
    limit = flow.add_limit("slots", 2)
    task = flow.add_task("t")
    reference = task.add_in_limit("slots", "/f")
    flow.begin()
    assert task.run()
    manager = CheckpointManager(
        bunch, checkpoint_file=tmp_path / "checkpoint", auth_mode="disabled"
    )
    assert manager.write_checkpoint()
    assert manager.write_checkpoint()
    return manager, flow, task, limit, reference


def _cross_flow_relative_reference(flow, task, limit, reference):
    flow.bunch.add_flow("other").add_limit("slots", 1)
    reference.node_path = "../other"


@pytest.mark.parametrize(
    "damage",
    [
        lambda flow, task, limit, reference: limit.reset(),
        lambda flow, task, limit, reference: setattr(limit, "value", 3),
        lambda flow, task, limit, reference: setattr(task, "attempt_id", None),
        lambda flow, task, limit, reference: setattr(
            task, "definition_generation", str(uuid4())
        ),
        lambda flow, task, limit, reference: setattr(
            reference, "node_path", "/missing"
        ),
        lambda flow, task, limit, reference: setattr(
            flow.add_task("duplicate"), "name", "t"
        ),
        lambda flow, task, limit, reference: task.add_trigger("/missing == complete"),
        lambda flow, task, limit, reference: task.add_trigger("/f/t:missing == 1"),
        lambda flow, task, limit, reference: task.add_trigger("invalid ???"),
        _cross_flow_relative_reference,
    ],
    ids=[
        "occupancy",
        "capacity",
        "identity",
        "generation",
        "reference",
        "structure",
        "trigger reference",
        "trigger variable",
        "trigger syntax",
        "relative cross-flow reference",
    ],
)
def test_invalid_capture_preserves_both_real_files_and_reports_failure(
    tmp_path, damage
):
    manager, flow, task, limit, reference = _running_manager(tmp_path)
    main = manager.checkpoint_file.read_bytes()
    backup = manager.backup_file.read_bytes()
    damage(flow, task, limit, reference)

    assert manager.write_checkpoint() is False
    assert manager.write_checkpoint() is False
    assert manager.checkpoint_file.read_bytes() == main
    assert manager.backup_file.read_bytes() == backup
    assert manager.checkpoint_failure_reason == "snapshot validation error"
    assert HaltCause.checkpoint_failure in manager.bunch.server_state.halt_causes
    scheduler = Scheduler(manager.bunch)
    scheduler.checkpoint_manager = manager
    assert scheduler.run_command_server_resume(ServerResumeCommand()).flag == 33
    assert "snapshot validation error" in scheduler.server_status_response().message
    restored = restore_checkpoint(manager.checkpoint_file.read_bytes())
    assert restored.find_node("/f/t").state.node_status is NodeStatus.submitted
    assert restored.find_flow("f").find_limit("slots").value == 1


def test_valid_child_report_during_validation_pause_keeps_other_halt_causes(tmp_path):
    manager, _, task, limit, _ = _running_manager(tmp_path)
    manager.bunch.server_state = ServerState.from_causes(
        [HaltCause.manual, HaltCause.recovery]
    )
    limit.reset()
    assert manager.write_checkpoint() is False
    assert set(manager.bunch.server_state.halt_causes) == {
        HaltCause.manual,
        HaltCause.recovery,
        HaltCause.checkpoint_failure,
    }
    scheduler = Scheduler(manager.bunch)
    scheduler.run_command_complete(
        CompleteCommand(node_path=task.node_path, attempt_id=task.attempt_id)
    )
    assert task.state.node_status is NodeStatus.complete
    assert manager.write_checkpoint() is True
    assert manager.bunch.server_state.halt_causes == [
        HaltCause.manual,
        HaltCause.recovery,
    ]


def test_failed_newer_validation_supersedes_unpublished_older_capture(tmp_path):
    manager, _, _, limit, _ = _running_manager(tmp_path)
    main = manager.checkpoint_file.read_bytes()
    backup = manager.backup_file.read_bytes()
    older_sequence, payload, _, error = manager._capture_for_write()
    assert error is None and payload is not None
    limit.reset()
    assert manager.write_checkpoint() is False
    assert manager._publish_captured(older_sequence, payload) == (False, False, None)
    assert manager.checkpoint_file.read_bytes() == main
    assert manager.backup_file.read_bytes() == backup
    assert HaltCause.checkpoint_failure in manager.bunch.server_state.halt_causes


def test_invalid_final_capture_reports_shutdown_failure_without_replacing_files(
    tmp_path,
):
    manager, _, _, limit, _ = _running_manager(tmp_path)
    main = manager.checkpoint_file.read_bytes()
    backup = manager.backup_file.read_bytes()
    limit.reset()
    assert asyncio.run(manager.stop()) is False
    assert manager.checkpoint_file.read_bytes() == main
    assert manager.backup_file.read_bytes() == backup
    assert HaltCause.checkpoint_failure in manager.bunch.server_state.halt_causes


def test_capture_validation_never_rebuilds_tree_or_runs_restore_hooks(
    tmp_path, monkeypatch
):
    manager, _, _, _, _ = _running_manager(tmp_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("capture rebuilt an executable tree")

    monkeypatch.setattr("takler.serialization.checkpoint.build_definition", forbidden)
    assert capture_checkpoint(manager.bunch)
    assert manager.write_checkpoint()


def test_valid_status_and_variable_trigger_roundtrip():
    bunch = Bunch()
    flow = bunch.add_flow("f")
    gate = flow.add_task("gate")
    gate.add_event("ready")
    task = flow.add_task("t")
    task.add_trigger("/f/gate == complete and /f/gate:ready == set")
    restored = restore_checkpoint(capture_checkpoint(bunch).model_dump())
    assert restored.find_node("/f/t").trigger_expression.expression_str == (
        "/f/gate == complete and /f/gate:ready == set"
    )
