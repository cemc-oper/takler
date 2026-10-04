"""Attempt identity survives snapshots and changes with definition replacement."""

import json
from uuid import UUID

import pytest

from takler.core import Bunch, Flow
from takler.protocol.commands import ForceCommand, ReplaceCommand
from takler.query import parse_show
from takler.serialization import export_definition
from takler.serialization.query import project_show
from takler.serialization.runtime import export_runtime
from takler.serialization.runtime import restore_runtime
from takler.schema import DefinitionError
from takler.server.checkpoint import CheckpointManager
from takler.server.scheduler import Scheduler


def test_checkpoint_preserves_attempt_without_restarting_it(tmp_path):
    bunch = Bunch()
    flow = bunch.add_flow("f")
    node = flow.add_task("t")
    flow.begin()
    node.run()
    node.init("rid-from-child")
    attempt = node.attempt_id
    generation = node.definition_generation
    password = node.job_password
    saved = export_runtime(bunch)
    path = tmp_path / "checkpoint"
    assert CheckpointManager(bunch, checkpoint_file=path).write_checkpoint()

    restored = Bunch()
    assert CheckpointManager(restored, checkpoint_file=path).restore()
    current = restored.find_node("/f/t")
    assert current.attempt_id == attempt
    assert current.definition_generation == generation
    assert current.job_password == password
    assert current.task_id == "rid-from-child"
    assert current.try_no == 1
    assert export_runtime(restored)["flows"] == saved["flows"]
    public = project_show(restored)
    assert public["flows"][0]["children"][0]["attempt_id"] == attempt
    assert parse_show(json.dumps(public)).get("/f/t").attempt_id == attempt
    assert node.attempt_id == attempt  # exporting and querying did not allocate


def test_force_queued_clears_current_identity_before_the_next_run():
    bunch = Bunch()
    flow = bunch.add_flow("f")
    node = flow.add_task("t")
    flow.begin()
    node.run()
    previous = node.attempt_id

    Scheduler(bunch).run_command_force(ForceCommand(paths=["/f/t"], state="queued"))
    assert node.attempt_id is None
    assert node.job_password is None
    assert node.try_no == 1
    node.run()
    assert node.attempt_id != previous
    assert node.try_no == 2


def test_replace_assigns_a_new_definition_generation():
    bunch = Bunch()
    old = bunch.add_flow("f")
    old.add_task("t")
    old_generation = old.definition_generation
    replacement = Flow("f")
    replacement.add_task("t")
    document = export_definition(replacement).model_dump(mode="json")

    Scheduler(bunch).run_command_replace(
        ReplaceCommand(target_path="/f", flow_bytes=json.dumps(document).encode())
    )
    new_flow = bunch.find_flow("f")
    assert new_flow.definition_generation != old_generation
    assert str(UUID(new_flow.definition_generation)) == new_flow.definition_generation
    assert new_flow.children[0].definition_generation == new_flow.definition_generation


@pytest.mark.parametrize(
    "field,value",
    [
        ("attempt_id", "not-a-uuid"),
        ("definition_generation", "not-a-uuid"),
        ("submitted_at", "2026-10-04T09:00:00"),
    ],
)
def test_runtime_rejects_invalid_identity_and_observation_time(field, value):
    bunch = Bunch()
    flow = bunch.add_flow("f")
    node = flow.add_task("t")
    flow.begin()
    node.run()
    runtime = export_runtime(bunch)
    runtime["flows"][0]["children"][0][field] = value
    with pytest.raises(DefinitionError, match="invalid_runtime"):
        restore_runtime(runtime)
