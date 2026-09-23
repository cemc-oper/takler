"""Independent snapshot data exercises both the reader and subsequent writer."""

import json
from pathlib import Path

import pytest

from takler.serialization.runtime import export_runtime
from takler.core import Bunch, SerializationType, Task
from takler.exceptions import InvalidRequestError

FIXTURE = Path(__file__).parents[3] / "fixtures" / "checkpoint_root_v2.json"


def test_bunch_restores_all_stored_root_attributes_and_references():
    source = json.loads(FIXTURE.read_text())["bunch"]
    restored = Bunch.from_dict(source)
    assert export_runtime(restored) == source
    task = restored.find_node("/f/t")
    assert task.get_bunch() is restored
    assert task.find_parent_parameter("ROOT_SETTING").value == "inherited"
    assert task.find_parent_parameter("NULL_SETTING").value is None
    assert task.find_parameter("CHILD_NULL").value is None
    assert task.find_parent_parameter("TAKLER_HOME").value == "/jobs"
    assert restored.limits[0].node is restored
    assert restored.in_limit_manager.node is restored
    assert restored.trigger_expression.ast is None
    # A fresh AST must bind to the reconstructed tree, not the old root.
    assert restored.evaluate_trigger() is True
    assert task.evaluate_trigger() is True
    assert task.complete_trigger_expression.free is True


def test_restore_root_replaces_attributes_and_preserves_deployment_and_flows():
    bunch = Bunch(host="current.invalid", port="9000")
    flow = bunch.add_flow("existing")
    deployment = bunch.server_state
    data = json.loads(FIXTURE.read_text())["bunch"]
    for _ in range(2):
        bunch.restore_root_attributes(data)
        assert len(bunch.events) == len(bunch.limits) == len(bunch.times) == 1
        assert bunch.server_state is deployment
        assert bunch.flows == {"existing": flow}
        assert flow.bunch is bunch
    bunch.restore_root_attributes(
        {"name": "empty", "state": {"status": 1, "suspended": False}}
    )
    assert bunch.user_parameters == {}
    assert bunch.events == bunch.meters == bunch.limits == bunch.times == []
    assert bunch.trigger_expression is None
    assert bunch.repeat is None


def test_invalid_root_does_not_partially_modify_live_bunch():
    bunch = Bunch("original")
    bunch.add_parameter("KEEP", "value")
    data = json.loads(FIXTURE.read_text())["bunch"]
    data["times"] = [{"time": "invalid"}]
    before = bunch.to_dict()
    with pytest.raises(ValueError):
        bunch.restore_root_attributes(data)
    assert bunch.to_dict() == before


@pytest.mark.parametrize("method", [SerializationType.Status, SerializationType.Tree])
def test_trigger_free_optional_defaults_and_mode(method):
    data = json.loads(FIXTURE.read_text())["bunch"]["flows"][0]["children"][0]
    task = Task.from_dict(data, method=method)
    assert task.trigger_expression.free is (method == SerializationType.Status)
    assert task.complete_trigger_expression.free is (method == SerializationType.Status)
    del data["trigger_free"]
    del data["complete_trigger_free"]
    task = Task.from_dict(data, method=method)
    assert task.trigger_expression.free is False
    assert task.complete_trigger_expression.free is False


@pytest.mark.parametrize("key", ["trigger_free", "complete_trigger_free"])
@pytest.mark.parametrize("value", [None, 1, "true"])
def test_invalid_free_latch_is_rejected(key, value):
    data = json.loads(FIXTURE.read_text())["bunch"]["flows"][0]["children"][0]
    data[key] = value
    with pytest.raises(InvalidRequestError, match=key):
        Task.from_dict(data)


@pytest.mark.parametrize("key", ["trigger", "complete_trigger"])
def test_free_latch_requires_expression(key):
    data = json.loads(FIXTURE.read_text())["bunch"]["flows"][0]["children"][0]
    del data[key]
    with pytest.raises(InvalidRequestError, match="requires an expression"):
        Task.from_dict(data)
