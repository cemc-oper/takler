"""FIX-01 contracts for Scheduler transitions and checkpoint publication.

The strict xfails record defects assigned to FIX-02/03. Remove each marker when
the corresponding implementation makes the asserted contract pass.
"""

import json

import pytest

from takler.core import Bunch, Flow, NodeStatus, RepeatDate
from takler.exceptions import FlowStateError
from takler.protocol.commands import (
    BeginCommand,
    ForceCommand,
    ForceState,
    ReplaceCommand,
    RequeueCommand,
    RunCommand,
)
from takler.serialization.checkpoint import capture_checkpoint, restore_checkpoint
from takler.serialization.definition import export_definition
from takler.server.checkpoint import CheckpointManager
from takler.server.scheduler import Scheduler


TASK_PATH = "/f/c/t"
LIFECYCLE_BUG = pytest.mark.xfail(
    strict=True, reason="FIX-02: lifecycle must release or retain correct holders"
)
PUBLICATION_BUG = pytest.mark.xfail(
    strict=True, reason="FIX-03: invalid view must not replace valid checkpoint"
)


def limited_scheduler(capacity=1, active=False):
    bunch = Bunch()
    flow = bunch.add_flow("f")
    limit = flow.add_limit("slots", capacity)
    container = flow.add_container("c")
    container.add_in_limit("slots")
    task = container.add_task("t")
    scheduler = Scheduler(bunch)
    assert scheduler.run_command_begin(BeginCommand(flow_name="f")).flag == 0
    assert task.run()
    if active:
        task.init("job")
    assert task.state.node_status is (
        NodeStatus.active if active else NodeStatus.submitted
    )
    assert (limit.value, limit.node_paths) == (1, {TASK_PATH})
    return scheduler, flow, container, task, limit


def assert_recoverable(bunch, limit_value, holders):
    restored = restore_checkpoint(capture_checkpoint(bunch).model_dump(mode="json"))
    restored_limit = restored.find_flow("f").find_limit("slots")
    assert (restored_limit.value, restored_limit.node_paths) == (
        limit_value,
        holders,
    )
    return restored


@pytest.mark.parametrize("active", [False, True])
def test_in_flight_tree_roundtrips_before_requeue(active):
    scheduler, _, _, task, _ = limited_scheduler(active=active)
    restored = assert_recoverable(scheduler.bunch, 1, {TASK_PATH})
    assert restored.find_node(TASK_PATH).state.node_status is task.state.node_status
    task.complete()
    assert_recoverable(scheduler.bunch, 0, set())


@LIFECYCLE_BUG
@pytest.mark.parametrize("active", [False, True])
@pytest.mark.parametrize("target", [TASK_PATH, "/f/c", "/f"])
def test_scheduler_requeue_releases_descendant_holders(target, active):
    scheduler, _, _, task, limit = limited_scheduler(active=active)
    assert scheduler.run_command_requeue(RequeueCommand(node_paths=[target])).flag == 0
    assert task.state.node_status is NodeStatus.queued
    assert task.attempt_id is None
    assert (limit.value, limit.node_paths) == (0, set())
    assert task.check_dependencies()
    assert_recoverable(scheduler.bunch, 0, set())


@pytest.mark.parametrize("active", [False, True])
@pytest.mark.parametrize("target,recursive", [(TASK_PATH, False), ("/f/c", True)])
def test_force_queued_releases_descendant_holders(target, recursive, active):
    scheduler, _, _, task, limit = limited_scheduler(active=active)
    response = scheduler.run_command_force(
        ForceCommand(paths=[target], state=ForceState.QUEUED, recursive=recursive)
    )
    assert response.flag == 0
    assert task.state.node_status is NodeStatus.queued
    assert (limit.value, limit.node_paths) == (0, set())
    assert_recoverable(scheduler.bunch, 0, set())


@pytest.mark.parametrize("state", [ForceState.COMPLETE, ForceState.ABORTED])
def test_force_terminal_releases_holder(state):
    scheduler, _, _, task, limit = limited_scheduler(active=True)
    response = scheduler.run_command_force(ForceCommand(paths=[TASK_PATH], state=state))
    assert response.flag == 0
    assert task.state.node_status is NodeStatus[state.value]
    assert (limit.value, limit.node_paths) == (0, set())
    assert_recoverable(scheduler.bunch, 0, set())


@LIFECYCLE_BUG
@pytest.mark.parametrize("state", [ForceState.SUBMITTED, ForceState.ACTIVE])
def test_force_in_flight_from_queued_preserves_recoverable_identity_and_holder(state):
    scheduler, _, _, task, limit = limited_scheduler()
    assert (
        scheduler.run_command_force(
            ForceCommand(paths=[TASK_PATH], state=ForceState.QUEUED)
        ).flag
        == 0
    )
    before = capture_checkpoint(scheduler.bunch).model_dump(mode="json")
    response = scheduler.run_command_force(ForceCommand(paths=[TASK_PATH], state=state))
    if response.flag:
        assert (
            capture_checkpoint(scheduler.bunch).model_dump(mode="json")["runtime"]
            == before["runtime"]
        )
        return
    assert task.state.node_status is NodeStatus[state.value]
    assert (limit.value, limit.node_paths) == (1, {TASK_PATH})
    assert_recoverable(scheduler.bunch, 1, {TASK_PATH})


@LIFECYCLE_BUG
@pytest.mark.parametrize("active", [False, True])
def test_force_begin_resets_in_flight_descendant_holders(active):
    scheduler, _, _, task, limit = limited_scheduler(active=active)
    assert (
        scheduler.run_command_begin(BeginCommand(flow_name="f", force=True)).flag == 0
    )
    assert task.state.node_status is NodeStatus.queued
    assert (limit.value, limit.node_paths) == (0, set())
    assert_recoverable(scheduler.bunch, 0, set())


def test_status_only_requires_explicit_limit_reconciliation():
    scheduler, _, _, task, limit = limited_scheduler()
    task.set_node_status_only(NodeStatus.queued)
    assert (limit.value, limit.node_paths) == (1, {TASK_PATH})
    task.update_limits()
    assert (limit.value, limit.node_paths) == (0, set())
    assert_recoverable(scheduler.bunch, 0, set())


def test_parent_and_local_limits_roundtrip_with_shared_reference():
    bunch = Bunch()
    flow = bunch.add_flow("f")
    shared = flow.add_limit("slots", 2)
    container = flow.add_container("c")
    container.add_in_limit("slots")
    task = container.add_task("t")
    local = task.add_limit("local", 1)
    task.add_in_limit("local")
    task.add_in_limit("slots", "/f")
    assert Scheduler(bunch).run_command_begin(BeginCommand(flow_name="f")).flag == 0
    assert task.run()
    assert (shared.value, shared.node_paths) == (1, {TASK_PATH})
    assert (local.value, local.node_paths) == (1, {TASK_PATH})
    restored = assert_recoverable(bunch, 1, {TASK_PATH})
    restored_local = restored.find_node(TASK_PATH).find_limit("local")
    assert (restored_local.value, restored_local.node_paths) == (1, {TASK_PATH})
    task.complete()
    assert (shared.value, local.value) == (0, 0)
    assert_recoverable(bunch, 0, set())


@LIFECYCLE_BUG
def test_operator_run_at_capacity_rejects_without_partial_mutation():
    bunch = Bunch()
    flow = bunch.add_flow("f")
    limit = flow.add_limit("slots", 1)
    container = flow.add_container("c")
    container.add_in_limit("slots")
    first = container.add_task("first")
    second = container.add_task("second")
    scheduler = Scheduler(bunch)
    assert scheduler.run_command_begin(BeginCommand(flow_name="f")).flag == 0
    assert first.run()
    assert not second.check_dependencies()
    response = scheduler.run_command_run(
        RunCommand(node_paths=[second.node_path], force=True)
    )
    assert response.flag != 0
    assert second.state.node_status is NodeStatus.queued
    assert (limit.value, limit.node_paths) == (1, {first.node_path})
    assert_recoverable(bunch, 1, {first.node_path})


def test_repeat_rollover_releases_holder_and_roundtrips():
    scheduler, flow, _, task, limit = limited_scheduler()
    flow.add_repeat(RepeatDate("day", 20261007, 20261008))
    task.complete()
    assert flow.repeat.r.value == 20261008
    assert task.state.node_status is NodeStatus.queued
    assert (limit.value, limit.node_paths) == (0, set())
    assert_recoverable(scheduler.bunch, 0, set())


def test_replace_idle_flow_preserves_external_holder_and_roundtrips():
    bunch = Bunch()
    provider = bunch.add_flow("provider")
    limit = provider.add_limit("slots", 2)
    consumer = bunch.add_flow("f")
    task = consumer.add_task("t")
    task.add_in_limit("slots", "/provider")
    scheduler = Scheduler(bunch)
    assert scheduler.run_command_begin(BeginCommand(flow_name="provider")).flag == 0
    assert scheduler.run_command_begin(BeginCommand(flow_name="f")).flag == 0
    assert task.run()
    assert (limit.value, limit.node_paths) == (1, {"/f/t"})
    # An in-flight replacement must leave the old tree and holder untouched.
    new = Flow("f")
    new.add_task("replacement")
    payload = json.dumps(export_definition(new).model_dump(mode="json")).encode()
    command = ReplaceCommand(target_path="/f", flow_bytes=payload)
    with pytest.raises(FlowStateError, match="running node"):
        scheduler.run_command_replace(command)
    assert bunch.find_node("/f/t") is task
    assert (limit.value, limit.node_paths) == (1, {"/f/t"})
    task.complete()
    assert scheduler.run_command_replace(command).flag == 0
    assert (limit.value, limit.node_paths) == (0, set())
    restored = restore_checkpoint(capture_checkpoint(bunch).model_dump(mode="json"))
    assert restored.find_node("/f/replacement") is not None


@LIFECYCLE_BUG
def test_delete_ancestor_in_limit_with_active_descendants_is_atomic():
    scheduler, _, container, task, limit = limited_scheduler(capacity=2)
    other = container.add_task("other")
    assert other.run()
    before = (
        list(container.in_limit_manager.in_limit_list),
        2,
        {TASK_PATH, other.node_path},
    )
    with pytest.raises(FlowStateError):
        container.in_limit_manager.delete_in_limit("slots")
    assert container.in_limit_manager.in_limit_list == before[0]
    assert (limit.value, limit.node_paths) == before[1:]
    task.complete()
    other.complete()
    assert (limit.value, limit.node_paths) == (0, set())
    assert container.in_limit_manager.delete_in_limit("slots")
    assert_recoverable(scheduler.bunch, 0, set())


@PUBLICATION_BUG
def test_two_invalid_saves_preserve_recoverable_main_and_backup(tmp_path):
    scheduler, _, _, task, limit = limited_scheduler()
    manager = CheckpointManager(
        scheduler.bunch,
        checkpoint_file=tmp_path / "checkpoint.json",
        auth_mode="disabled",
    )
    assert manager.write_checkpoint()
    assert manager.write_checkpoint()
    main_before = manager.checkpoint_file.read_bytes()
    backup_before = manager.backup_file.read_bytes()

    assert (
        scheduler.run_command_requeue(RequeueCommand(node_paths=[TASK_PATH])).flag == 0
    )
    assert task.state.node_status is NodeStatus.queued
    # FIX-02 will make this command release the token. Until then, FIX-03 must
    # reject the inconsistent view before either valid file is replaced.
    if (limit.value, limit.node_paths) == (0, set()):
        limit.increment(1, TASK_PATH)
    assert [manager.write_checkpoint(), manager.write_checkpoint()] == [False, False]
    assert manager.checkpoint_file.read_bytes() == main_before
    assert manager.backup_file.read_bytes() == backup_before
    restored = CheckpointManager(
        Bunch(), checkpoint_file=manager.checkpoint_file, auth_mode="disabled"
    )
    assert restored.restore()
    assert restored.bunch.find_node(TASK_PATH).state.node_status is NodeStatus.submitted
