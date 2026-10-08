"""R2-06: visible writes produce bounded, session-local query hints."""

import asyncio
import json

from takler.core import Bunch, Flow, NodeStatus, RepeatDate
from takler.protocol.commands import (
    Command,
    ReplaceCommand,
    RunCommand,
    ServerHaltCommand,
)
from takler.serialization import export_definition
from takler.server.checkpoint import CheckpointManager
from takler.server.handlers import CommandHandlers
from takler.server.query_changes import (
    MAX_CHANGE_BATCHES,
    MAX_CHANGES_PER_BATCH,
    QueryChanges,
)
from takler.server.scheduler import Scheduler


def _scheduler():
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task("t")
    task.add_event("e")
    task.add_meter("m", 0, 100)
    return Scheduler(bunch), flow, task


def test_noop_and_sparse_changes_do_not_scan_tree():
    scheduler, flow, task = _scheduler()
    tracker = scheduler.query_changes
    start = tracker.revision
    assert tracker.since(start) == ()
    task.set_event("e", False)
    task.set_meter("m", 0)
    task.suspend()
    task.suspend()
    assert tracker.revision == start + 1
    assert tracker.since(start)[0].changes[0].path == "/f/t"
    assert tracker.since(start)[0].changes[0].groups == frozenset({"summary"})
    task.set_meter("m", 7)
    assert tracker.since(start + 1)[0].changes[0].groups == frozenset({"runtime"})
    flow.add_task("new")
    assert tracker.since(start + 2)[0].changes[0].invalidate


def test_failed_command_commits_partial_side_effect(monkeypatch):
    scheduler, flow, task = _scheduler()
    tracker = scheduler.query_changes
    start = tracker.revision

    def partial(path, force=False):
        task.set_node_status(NodeStatus.queued)
        raise RuntimeError("failure after state write")

    monkeypatch.setattr(scheduler, "_run_node", partial)
    result = asyncio.run(
        CommandHandlers(scheduler).handle(Command.RUN, RunCommand(node_paths=["/f/t"]))
    )
    assert result.flag != 0
    assert tracker.revision == start + 1
    assert tracker.since(start)[0].changes[0].path == "/f/t"
    assert tracker.since(start)[0].changes[0].groups == frozenset({"summary"})


def test_service_change_and_new_session():
    scheduler, _, _ = _scheduler()
    start = scheduler.query_changes.revision
    asyncio.run(
        CommandHandlers(scheduler).handle(Command.SERVER_HALT, ServerHaltCommand())
    )
    assert scheduler.query_changes.since(start)[0].changes[0].groups == frozenset(
        {"service"}
    )
    successor = Scheduler(scheduler.bunch)
    assert successor.query_service.session_id != scheduler.query_service.session_id
    assert successor.query_changes.revision == 1
    assert successor.query_changes.since(start) == ()


def test_bounded_window_expires_old_revision():
    tracker = QueryChanges()
    for index in range(MAX_CHANGE_BATCHES + 1):
        tracker.mark(f"/f/t{index}", "runtime")
    assert tracker.since(1) is None
    assert len(tracker.since(tracker.oldest_revision)) == MAX_CHANGE_BATCHES
    assert tracker.since(tracker.revision) == ()


def test_large_batch_falls_back_to_root_invalidation():
    tracker = QueryChanges()
    tracker.begin()
    for index in range(MAX_CHANGES_PER_BATCH + 1):
        tracker.mark(f"/f/t{index}", "summary")
    tracker.end()
    assert tracker.since(1)[0].changes[0].path == "/"
    assert tracker.since(1)[0].changes[0].invalidate


def test_ancestor_parameter_change_invalidates_descendant_detail():
    scheduler, flow, _ = _scheduler()
    start = scheduler.query_changes.revision
    flow.add_parameter("shared", "value")
    change = scheduler.query_changes.since(start)[0].changes[0]
    assert change.path == "/f"
    assert change.groups == frozenset({"parameters"})
    assert change.invalidate


def test_extension_can_invalidate_scope_without_projecting_tree():
    scheduler, _, task = _scheduler()
    start = scheduler.query_changes.revision
    task.notify_query_change(invalidate=True)
    assert scheduler.query_changes.since(start)[0].changes[0].invalidate


def test_replace_invalidates_old_generation():
    scheduler, old_flow, _ = _scheduler()
    old_generation = old_flow.definition_generation
    candidate = Flow("f")
    candidate.add_task("replacement")
    payload = json.dumps(export_definition(candidate).model_dump(mode="json")).encode()
    start = scheduler.query_changes.revision
    result = scheduler.run_command_replace(
        ReplaceCommand(target_path="/f", flow_bytes=payload)
    )
    assert result.flag == 0
    assert scheduler.bunch.find_flow("f").definition_generation != old_generation
    changes = scheduler.query_changes.since(start)[0].changes
    assert len(changes) == 1 and changes[0].path == "/f"
    assert changes[0].invalidate
    latest = scheduler.query_changes.revision
    old_flow.add_task("stale")
    assert scheduler.query_changes.revision == latest


def test_checkpoint_outcome_tracks_service_detail(tmp_path):
    scheduler, _, _ = _scheduler()
    manager = CheckpointManager(
        scheduler.bunch, checkpoint_file=tmp_path / "state.check"
    )
    manager.query_scheduler = scheduler
    scheduler.checkpoint_manager = manager
    start = scheduler.query_changes.revision
    manager._record_outcome(1, False, False, "", "storage full")
    assert scheduler.query_changes.since(start)[0].changes[0].groups == frozenset(
        {"service"}
    )
    assert scheduler.query_revision % 2 == 0


def test_task_attempt_repeat_and_parent_aggregation_are_tracked():
    scheduler, flow, task = _scheduler()
    flow.add_repeat(RepeatDate("day", 20260101, 20260102))
    flow.begin()
    start = scheduler.query_changes.revision
    assert task.run()
    task.complete()
    assert flow.repeat.value() == 20260102
    hints = [
        change
        for batch in scheduler.query_changes.since(start)
        for change in batch.changes
    ]
    assert any(change.path == "/f/t" and "summary" in change.groups for change in hints)
    assert any(change.path == "/f/t" and "runtime" in change.groups for change in hints)
    assert any(change.path == "/f" and "summary" in change.groups for change in hints)
    assert any(change.path == "/f" and "runtime" in change.groups for change in hints)


def test_deep_node_notification_uses_iterative_root_lookup():
    flow = Flow("f")
    deepest = flow
    for index in range(1100):
        deepest = deepest.add_container(f"c{index}")
    bunch = Bunch()
    bunch.add_flow(flow)
    scheduler = Scheduler(bunch)
    start = scheduler.query_changes.revision
    deepest.suspend()
    change = scheduler.query_changes.since(start)[0].changes[0]
    assert change.path == "/" and change.invalidate
