"""Independent v3 fixture checks root storage and deployment separation."""

import copy
import json
from pathlib import Path

import pytest

from takler.core import Bunch, ServerStatus
from takler.serialization.checkpoint import capture_checkpoint, restore_checkpoint
from takler.server.checkpoint import CheckpointManager

FIXTURE = Path(__file__).parents[1] / "fixtures" / "checkpoint_v3.json"


def _data():
    return json.loads(FIXTURE.read_text())


def test_checkpoint_restores_root_storage_without_restoring_deployment(tmp_path):
    data = _data()
    bunch = Bunch(host="current.invalid", port="9000")
    config = bunch.server_config
    manager = CheckpointManager(bunch, checkpoint_file=tmp_path / "checkpoint")
    manager.checkpoint_file.write_text(json.dumps(data))

    assert manager.restore() is True
    assert manager.bunch is bunch
    assert bunch.name == "root"
    assert bunch.server_config is config
    assert bunch.server_config.host == "current.invalid"
    assert bunch.server_config.port == "9000"
    assert bunch.server_state.status is ServerStatus.halted
    assert bunch.server_state.status_reason == "recovery pending"
    assert bunch.limits[0].value == 2
    assert bunch.limits[0].node_paths == {"/f/s", "/f/a"}
    assert bunch.events[0].value is True
    assert bunch.meters[0].value == 4
    assert bunch.repeat.r.value == 20261004
    assert bunch.times[0].free is True
    assert bunch.complete_trigger_expression.free is True
    assert bunch.in_limit_manager.in_limit_list[0].limit is bunch.limits[0]
    for path in ("/f/s", "/f/a"):
        task = bunch.find_node(path)
        assert task.get_bunch() is bunch
        assert task.find_parent_parameter("ROOT_SETTING").value == "inherited"
        assert task.find_parent_parameter("TAKLER_HOME").value == "/jobs"
        assert task.find_parent_parameter("TAKLER_HOST").value == "current.invalid"
        assert task.find_parent_parameter("TAKLER_PORT").value == "9000"
        assert task.in_limit_manager.in_limit_list[0].limit is bunch.limits[0]
    assert bunch.find_node("/f/s").job_password == "fixture-submitted-password"
    assert bunch.find_node("/f/a").job_password == "fixture-active-password"
    assert bunch.find_node("/f/q").job_password is None
    assert bunch.trigger_expression is not None
    assert bunch.in_limit_manager.node is bunch

    assert manager.write_checkpoint() is True
    written = json.loads(manager.checkpoint_file.read_text())
    assert (
        restore_checkpoint(written).find_node("/f/s").attempt_id
        == data["runtime"]["nodes"]["/f/s"]["attempt_id"]
    )
    assert written["runtime"]["nodes"]["/"]["limit_usage"]["slots"]["value"] == 2
    assert written["secrets"] == data["secrets"]
    assert "fixture-submitted-password" not in json.dumps(written["definition"])
    assert "fixture-active-password" not in json.dumps(written["runtime"])


def test_address_changes_but_checkpoint_service_state_is_restored(tmp_path):
    data = _data()
    data["runtime"]["server_state"] = {
        "status": "halted",
        "status_reason": "manual pause",
        "halt_causes": ["manual"],
    }
    bunch = Bunch(host="current.invalid", port="9000")
    manager = CheckpointManager(bunch, checkpoint_file=tmp_path / "checkpoint")
    manager.checkpoint_file.write_text(json.dumps(data))
    assert manager.restore() is True
    assert bunch.server_state.status is ServerStatus.halted
    assert bunch.server_state.status_reason == "recovery pending"
    assert [cause.value for cause in bunch.server_state.halt_causes] == [
        "manual",
        "recovery",
    ]
    assert bunch.server_config.host == "current.invalid"


def test_unknown_snapshot_service_status_falls_back_to_backup(tmp_path):
    data = _data()
    manager = CheckpointManager(Bunch(), checkpoint_file=tmp_path / "checkpoint")
    manager.backup_file.write_text(json.dumps(data))
    data["runtime"]["server_state"]["status"] = "complete"
    manager.checkpoint_file.write_text(json.dumps(data))
    assert manager.restore() is True
    assert manager.bunch.server_state.status is ServerStatus.halted
    assert manager.restore_summary["source"] == "backup"


@pytest.mark.parametrize("version", [None, 0, -1, 1, 2, 4, 3.0, True, "3"])
def test_unsupported_version_falls_back_without_leaking_root_attributes(
    tmp_path, version
):
    data = _data()
    manager = CheckpointManager(Bunch(), checkpoint_file=tmp_path / "checkpoint")
    manager.backup_file.write_text(json.dumps(data))
    bad = copy.deepcopy(data)
    bad["format_version"] = version
    bad["definition"]["root"]["name"] = "bad-primary"
    manager.checkpoint_file.write_text(json.dumps(bad))
    assert manager.restore() is True
    assert manager.bunch.name == "root"


def test_independent_fixture_round_trip_is_stable():
    data = _data()
    restored = restore_checkpoint(data)
    exported = capture_checkpoint(restored).model_dump(mode="json", exclude_unset=True)
    assert (
        capture_checkpoint(restore_checkpoint(exported)).model_dump(
            mode="json", exclude_unset=True
        )["definition"]
        == exported["definition"]
    )
    assert (
        exported["runtime"]["nodes"]["/f/s"]["attempt_id"]
        == data["runtime"]["nodes"]["/f/s"]["attempt_id"]
    )
    assert exported["secrets"] == data["secrets"]
    assert exported["source_address"] != data["source_address"]
    assert restore_checkpoint(capture_checkpoint(restored).model_dump()).name == "root"
