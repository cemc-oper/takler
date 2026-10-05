"""Recovery opens only a validated checkpoint, with scheduling paused."""

import asyncio
import copy
import json
from pathlib import Path
from unittest import mock

import pytest

from takler.core import NodeStatus
from takler.exceptions import AttemptMismatchError
from takler.protocol.commands import CompleteCommand, ServerResumeCommand
from takler.server import TaklerServer
from takler.server.checkpoint import CheckpointRestoreError


FIXTURE = Path(__file__).parents[1] / "fixtures" / "checkpoint_v3.json"


def _snapshot():
    return json.loads(FIXTURE.read_text())


def _server(tmp_path):
    return TaklerServer(
        host="current.invalid", port=9000, checkpoint_file=tmp_path / "checkpoint"
    )


def test_backup_report_and_recovery_gate_with_matching_child(tmp_path):
    data = _snapshot()
    server = _server(tmp_path)
    server.checkpoint_manager.backup_file.write_text(json.dumps(data))
    server.checkpoint_manager.checkpoint_file.write_text("{broken")

    assert server.checkpoint_manager.restore() is True
    server.scheduler.last_checkpoint_at = server.checkpoint_manager.last_checkpoint_at
    server.scheduler.restore_summary = server.checkpoint_manager.restore_summary
    response = server.scheduler.server_status_response()
    assert response.status == "halted"
    assert response.halt_causes == ["recovery"]
    assert response.last_checkpoint_at == data["saved_at"]
    assert response.restore_summary == {
        "source": "backup",
        "saved_at": data["saved_at"],
        "fallback": True,
        "flow_count": 1,
        "node_count": 4,
        "in_flight_count": 2,
        "snapshot_status": "running",
        "snapshot_status_reason": None,
        "effective_status": "halted",
        "effective_status_reason": "recovery pending",
    }
    queued = server.bunch.find_node("/f/q")
    with pytest.raises(Exception, match="halted"):
        server.scheduler._run_node("/f/q")
    assert queued.try_no == 0
    assert queued.attempt_id is None
    with pytest.raises(AttemptMismatchError):
        server.scheduler.run_command_complete(
            CompleteCommand(
                node_path="/f/q",
                attempt_id="99999999-9999-4999-8999-999999999999",
            )
        )
    submitted = server.bunch.find_node("/f/s")
    active = server.bunch.find_node("/f/a")
    assert submitted.state.node_status is NodeStatus.submitted
    assert active.state.node_status is NodeStatus.active
    server.scheduler.run_command_complete(
        CompleteCommand(node_path="/f/s", attempt_id=submitted.attempt_id)
    )
    assert submitted.state.node_status is NodeStatus.complete
    assert (
        server.scheduler.run_command_server_resume(ServerResumeCommand()).status
        == "running"
    )


@pytest.mark.parametrize("damage", ["unknown_type", "missing_secrets"])
def test_all_invalid_candidates_refuse_start_before_services(tmp_path, damage):
    server = _server(tmp_path)
    data = _snapshot()
    if damage == "unknown_type":
        data["definition"]["root"]["flows"][0]["type_id"] = "unknown.flow"
    else:
        data["secrets"]["job_passwords"] = {}
    server.checkpoint_manager.checkpoint_file.write_text(json.dumps(data))
    server.checkpoint_manager.backup_file.write_text(json.dumps(copy.deepcopy(data)))
    server.scheduler.start = mock.AsyncMock()
    server.grpc_transport.start = mock.AsyncMock()

    with pytest.raises(CheckpointRestoreError) as exc:
        asyncio.run(server.start())
    assert "password" not in str(exc.value)
    server.scheduler.start.assert_not_awaited()
    server.grpc_transport.start.assert_not_awaited()
    assert server.bunch.flows == {}


def test_no_checkpoint_is_new_running_service(tmp_path):
    server = _server(tmp_path)
    assert server.checkpoint_manager.restore() is False
    assert server.bunch.server_state.status.value == "running"
    assert server.checkpoint_manager.restore_summary is None


def test_broken_checkpoint_symlink_is_recovery_mode(tmp_path):
    server = _server(tmp_path)
    server.checkpoint_manager.checkpoint_file.symlink_to(tmp_path / "missing")
    with pytest.raises(CheckpointRestoreError):
        server.checkpoint_manager.restore()
    assert server.bunch.flows == {}


def test_server_start_exposes_restore_report_before_accepting_requests(tmp_path):
    server = _server(tmp_path)
    data = _snapshot()
    server.checkpoint_manager.checkpoint_file.write_text(json.dumps(data))
    server.scheduler.start = mock.AsyncMock()
    server.grpc_transport.start = mock.AsyncMock()
    server.checkpoint_manager.start = mock.AsyncMock()

    asyncio.run(server.start())

    response = server.scheduler.server_status_response()
    assert response.restore_summary["source"] == "primary"
    assert response.restore_summary["snapshot_status"] == "running"
    assert response.restore_summary["effective_status"] == "halted"
    assert response.last_checkpoint_at == data["saved_at"]
    server.scheduler.start.assert_awaited_once()


def test_diagnostic_log_does_not_change_selected_snapshot(tmp_path):
    server = _server(tmp_path)
    data = _snapshot()
    server.checkpoint_manager.checkpoint_file.write_text(json.dumps(data))
    (tmp_path / "takler.log").write_text(
        "2099-01-01 completed /f/s and started unknown attempt\n"
    )
    assert server.checkpoint_manager.restore() is True
    assert server.bunch.find_node("/f/s").state.node_status is NodeStatus.submitted
    assert server.bunch.find_node("/f/q").attempt_id is None
