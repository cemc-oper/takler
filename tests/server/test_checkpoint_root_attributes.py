"""Root checkpoint restore must preserve current deployment and saved storage."""

import json
from pathlib import Path

import pytest

from takler.serialization.runtime import export_runtime
from takler.core import Bunch
from takler.server.checkpoint import CheckpointManager

from .test_checkpoint_restore_unit import _capturing_stderr

FIXTURE = Path(__file__).parents[1] / "fixtures" / "checkpoint_root_v2.json"


def test_checkpoint_restores_root_storage_without_restoring_deployment(tmp_path):
    data = json.loads(FIXTURE.read_text())
    bunch = Bunch(host="current.invalid", port="9000")
    server_state = bunch.server_state
    manager = CheckpointManager(bunch, checkpoint_file=tmp_path / "checkpoint")
    manager.checkpoint_file.write_text(json.dumps(data))
    assert manager.restore() is True
    assert manager.bunch is bunch
    assert bunch.server_state is server_state
    expected = data["bunch"]
    expected["server_state"] = server_state.to_dict()
    assert export_runtime(bunch) == expected
    task = bunch.find_node("/f/t")
    assert task.get_bunch() is bunch
    assert task.find_parent_parameter("ROOT_SETTING").value == "inherited"
    assert task.find_parent_parameter("NULL_SETTING").value is None
    assert task.find_parent_parameter("TAKLER_HOST").value == "current.invalid"
    assert task.find_parent_parameter("TAKLER_PORT").value == "9000"
    assert task.find_parent_parameter("TAKLER_HOME").value == "/jobs"
    assert task.job_password == "FIXED_TEST_PASSWORD_NOT_A_SECRET"
    assert bunch.limits[0].node is bunch
    assert bunch.in_limit_manager.node is bunch
    assert task.trigger_expression.free and task.complete_trigger_expression.free
    # Verify the independent fixture survives the real checkpoint writer too.
    assert manager.write_checkpoint() is True
    assert json.loads(manager.checkpoint_file.read_text())["bunch"] == expected


def test_address_overrides_at_every_level_are_removed_and_reported(tmp_path):
    data = json.loads(FIXTURE.read_text())
    root = data["bunch"]
    flow = root["flows"][0]
    task = flow["children"][0]
    container = {
        "name": "c",
        "type_id": "takler.container",
        "state": {"status": 1, "suspended": False},
        "trigger_free": False,
        "complete_trigger_free": False,
        "is_complete_triggered": False,
    }
    flow["children"].append(container)
    for node in (root, flow, container, task):
        node.setdefault("user_parameters", []).extend(
            [
                {"name": "TAKLER_HOST", "value": "DO_NOT_LOG_OLD_VALUE"},
                {"name": "TAKLER_PORT", "value": "DO_NOT_LOG_OLD_VALUE"},
            ]
        )
    bunch = Bunch(host="current.invalid", port="9000")
    manager = CheckpointManager(bunch, checkpoint_file=tmp_path / "checkpoint")
    manager.checkpoint_file.write_text(json.dumps(data))
    result, log = _capturing_stderr(manager.restore)
    assert result is True
    assert log.count("deployment parameter conflict") == 8
    assert "DO_NOT_LOG_OLD_VALUE" not in log
    for node in (
        bunch,
        bunch.find_flow("f"),
        bunch.find_node("/f/c"),
        bunch.find_node("/f/t"),
    ):
        assert "TAKLER_HOST" not in node.user_parameters
        assert "TAKLER_PORT" not in node.user_parameters
        assert node.find_parent_parameter("TAKLER_HOST").value == "current.invalid"
        assert node.find_parent_parameter("TAKLER_PORT").value == "9000"


@pytest.mark.parametrize("version", [None, 0, -1, 1, 3, 2.0, True, "2"])
def test_unsupported_version_falls_back_without_leaking_root_attributes(
    tmp_path, version
):
    data = json.loads(FIXTURE.read_text())
    manager = CheckpointManager(Bunch(), checkpoint_file=tmp_path / "checkpoint")
    manager.backup_file.write_text(json.dumps(data))
    data["format_version"] = version
    data["bunch"]["name"] = "bad-primary"
    manager.checkpoint_file.write_text(json.dumps(data))
    assert manager.restore() is True
    assert manager.bunch.name == "root"
