"""Network load is an isolated, insert-only definition boundary."""

import json
from unittest.mock import patch

import pytest

from takler.core import Bunch, Flow, NodeStatus
from takler.exceptions import FlowStateError, InvalidRequestError
from takler.protocol.commands import LoadCommand
from takler.serialization import export_definition
from takler.server.scheduler import Scheduler


def document(name="new", **fields):
    return dict(
        kind="takler.definition",
        schema_version=1,
        root=dict(type_id="takler.flow", name=name, **fields),
    )


def load(scheduler, data):
    scheduler.run_command_load(
        LoadCommand(
            flow_bytes=data if isinstance(data, bytes) else json.dumps(data).encode()
        )
    )


@pytest.mark.parametrize("status", list(NodeStatus))
def test_duplicate_preserves_runtime_and_identity(status):
    bunch = Bunch("b")
    old = bunch.add_flow("existing")
    task = old.add_task("task")
    old.begin()
    old.state.node_status = status
    old.suspend()
    task.task_id = "job"
    task.job_password = "private"
    task.add_limit("slots", 2)
    before = bunch.to_dict()
    with pytest.raises(FlowStateError, match="already exists"):
        load(Scheduler(bunch), document("existing"))
    assert bunch.find_flow("existing") is old
    assert old.find_node("/existing/task") is task
    assert task.job_password == "private"
    assert bunch.to_dict() == before


@pytest.mark.parametrize(
    "data",
    [
        b"not json",
        b"\xff",
        b"{} {}",
        b'{"kind":1,"kind":2}',
        {},
        {"name": "legacy", "class_type": "malicious.Type"},
        dict(document(), schema_version=2),
        document(state={"node_status": "active"}),
        document(children=[dict(type_id="evil.task", name="task")]),
        document(children=[dict(type_id="takler.task", name="task", task_id="secret")]),
        document(in_limits=[dict(limit_name="missing")]),
        document(trigger="/missing == complete"),
        dict(document(), root=dict(type_id="takler.bunch", name="b", flows=[])),
    ],
)
def test_rejected_input_has_no_online_side_effects(data):
    bunch = Bunch("b")
    old = bunch.add_flow("existing")
    before = bunch.to_dict()
    with patch("importlib.import_module", side_effect=AssertionError("dynamic import")):
        with pytest.raises(InvalidRequestError):
            load(Scheduler(bunch), data)
    assert bunch.flows == {"existing": old}
    assert bunch.to_dict() == before


def test_export_load_binds_cross_flow_references_without_occupying_limit():
    bunch = Bunch("b")
    existing = bunch.add_flow("existing")
    existing.add_limit("slots", 2)
    flow = Flow("new")
    task = flow.add_task("task")
    task.add_in_limit("slots", "/existing")
    task.add_trigger("/existing == complete", parse=False)
    load(Scheduler(bunch), export_definition(flow).model_dump(mode="json"))
    loaded = bunch.find_flow("new")
    assert loaded.bunch is bunch
    assert not loaded.begun
    assert loaded.calendar.initial_time is None
    new_task = loaded.find_node("/new/task")
    assert new_task.in_limit_manager.in_limit_list[0].limit is existing.find_limit(
        "slots"
    )
    assert existing.find_limit("slots").value == 0
    assert new_task.state.node_status is NodeStatus.unknown
