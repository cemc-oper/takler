"""R3-11: independent state references across fixed writes and query leases.

The expected rows below are written from the scenario, not from a checkpoint,
version record, or query serializer.  Each published artifact is checked again
after the live tree has advanced.
"""

import asyncio
import errno
import json
import threading
from dataclasses import dataclass
from uuid import uuid4

import pytest

from takler.client.full_snapshot import FullSnapshotClient
from takler.client.query_client import QueryClient
from takler.core import Bunch, Flow, HaltCause, NodeStatus, RepeatDate, Task
from takler.exceptions import AttemptMismatchError, QueryResponseError
from takler.protocol.commands import (
    CompleteCommand,
    EventCommand,
    InitCommand,
    MeterCommand,
    ReplaceCommand,
    ServerHaltCommand,
)
from takler.protocol.query_v1 import (
    CancelFullSnapshotRequest,
    FullSnapshotChunkRequest,
    FullSnapshotRequest,
    PageRequest,
    SinceRequest,
)
from takler.serialization.checkpoint import restore_checkpoint
from takler.serialization.definition import export_definition
from takler.server.checkpoint import CheckpointManager
from takler.server.scheduler import Scheduler


@dataclass(frozen=True)
class Reference:
    paths: frozenset[str]
    status: str
    attempt: str | None
    password: str | None
    task_id: str | None
    unknown: str | None
    event: bool
    meter: int
    slots: int
    begun: bool
    job_path: str | None
    runtime_probe: str | None


def _reference(
    *,
    paths,
    status,
    attempt=None,
    password=None,
    task_id=None,
    unknown=None,
    event=False,
    meter=0,
    slots=0,
    begun=True,
    job_path=None,
    runtime_probe=None,
):
    return Reference(
        frozenset(paths),
        status,
        attempt,
        password,
        task_id,
        unknown,
        event,
        meter,
        slots,
        begun,
        job_path,
        runtime_probe,
    )


def _runtime_probe(tree):
    """Read the live domain objects without any checkpoint or query exporter."""
    flow = tree.find_node("/f")
    task = tree.find_node("/f/t")
    limit = flow.find_limit("slots")
    fields = (
        "attempt_id",
        "task_id",
        "try_no",
        "job_password",
        "submission_unknown_reason",
        "submitted_at",
        "started_at",
        "ended_at",
        "aborted_reason",
        "job_path",
        "jobout_path",
        "orvix_info_path",
    )
    sample = {
        "flow": {
            "begun": flow.begun,
            "calendar_initial": flow.calendar.initial_time.isoformat(),
            "limit": (limit.value, sorted(limit.node_paths)),
            "parameters": {
                name: item.value for name, item in flow.user_parameters.items()
            },
        },
        "task": {
            "status": task.state.node_status.name,
            "suspended": task.state.suspended,
            "event": task.find_event("ready").value,
            "meter": task.find_meter("progress").value,
            "parameters": {
                name: item.value for name, item in task.user_parameters.items()
            },
            **{field: getattr(task, field) for field in fields},
        },
    }
    # Credentials are intentionally omitted from terminal checkpoint records.
    if task.state.node_status in (NodeStatus.complete, NodeStatus.aborted):
        sample["task"]["job_password"] = None
    return json.dumps(sample, sort_keys=True)


def _assert_restored(data: bytes, expected: Reference):
    tree = restore_checkpoint(data)
    flow = tree.find_node("/f")
    task = tree.find_node("/f/t")
    assert set(json.loads(data)["runtime"]["nodes"]) == expected.paths
    assert flow.begun is expected.begun
    assert flow.calendar.initial_time is not None
    assert task.state.node_status.value == NodeStatus[expected.status].value
    assert task.attempt_id == expected.attempt
    assert task.job_password == expected.password
    assert task.task_id == expected.task_id
    assert task.submission_unknown_reason == expected.unknown
    assert task.job_path == expected.job_path
    assert task.find_event("ready").value is expected.event
    assert task.find_meter("progress").value == expected.meter
    assert flow.find_limit("slots").value == expected.slots
    assert flow.find_limit("slots").node_paths == (
        {"/f/t"} if expected.slots else set()
    )
    assert tree.find_node("/f/other").check_dependencies() is (expected.slots == 0)
    if expected.job_path:
        from pathlib import Path

        from takler.serialization.artifact_path_index import ArtifactPathIndex

        index = ArtifactPathIndex(tree)
        assert index.conflicts(tree.find_node("/f/other"), {Path(expected.job_path)})
    if expected.runtime_probe is not None:
        assert _runtime_probe(tree) == expected.runtime_probe
    return tree


def _matrix(tmp_path):
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    flow.add_limit("slots", 1)
    task = flow.add_task(Task("t"))
    task.add_in_limit("slots", "/f")
    task.add_event("ready")
    task.add_meter("progress", 0, 100)
    flow.add_task(Task("other")).add_in_limit("slots", "/f")
    flow.begin()
    scheduler = Scheduler(bunch)
    manager = CheckpointManager(bunch, checkpoint_file=tmp_path / "state.check")
    manager.query_scheduler = scheduler
    scheduler.checkpoint_manager = manager
    return scheduler, manager, task


def test_published_versions_survive_child_changes_and_late_worker(tmp_path):
    scheduler, manager, task = _matrix(tmp_path)
    assert task.run()
    task.mark_submission_unknown("reply lost")
    task.job_path = str(tmp_path / "attempt.job")
    old_attempt, old_password = task.attempt_id, task.job_password
    before = _reference(
        paths={"/", "/f", "/f/t", "/f/other"},
        status="submitted",
        attempt=old_attempt,
        password=old_password,
        unknown="reply lost",
        slots=1,
        job_path=task.job_path,
        runtime_probe=_runtime_probe(scheduler.bunch),
    )
    entered, release = asyncio.Event(), asyncio.Event()
    write = manager._write_fixed_background

    async def delayed(sequence, version, saved_at):
        entered.set()
        await release.wait()
        return await write(sequence, version, saved_at)

    manager._write_fixed_background = delayed

    async def run():
        pending = asyncio.create_task(manager.write_checkpoint_async())
        await entered.wait()
        with pytest.raises(AttemptMismatchError):
            scheduler.run_command_complete(
                CompleteCommand(node_path="/f/t", attempt_id=str(uuid4()))
            )
        await scheduler.run_command_init(
            InitCommand(node_path="/f/t", attempt_id=old_attempt, task_id="rid-1")
        )
        scheduler.run_command_event(
            EventCommand(
                node_path="/f/t",
                source_task_path="/f/t",
                attempt_id=old_attempt,
                event_name="ready",
            )
        )
        scheduler.run_command_meter(
            MeterCommand(
                node_path="/f/t",
                source_task_path="/f/t",
                attempt_id=old_attempt,
                meter_name="progress",
                meter_value=67,
            )
        )
        scheduler.bunch.find_node("/f").add_task(Task("later"))
        release.set()
        assert await pending
        await manager._drain_in_flight_write()

    asyncio.run(run())
    first = manager.checkpoint_file.read_bytes()
    _assert_restored(first, before)
    active = _reference(
        paths=before.paths | {"/f/later"},
        status="active",
        attempt=old_attempt,
        password=old_password,
        task_id="rid-1",
        event=True,
        meter=67,
        slots=1,
        job_path=task.job_path,
        runtime_probe=_runtime_probe(scheduler.bunch),
    )
    manager._write_fixed_background = write
    assert manager.write_checkpoint()
    second = manager.checkpoint_file.read_bytes()
    _assert_restored(second, active)
    assert manager.backup_file.read_bytes() == first

    scheduler.run_command_complete(
        CompleteCommand(node_path="/f/t", attempt_id=old_attempt)
    )
    assert manager.write_checkpoint()
    complete = _reference(
        paths=active.paths,
        status="complete",
        attempt=old_attempt,
        task_id="rid-1",
        event=True,
        meter=67,
        slots=0,
        job_path=task.job_path,
        runtime_probe=_runtime_probe(scheduler.bunch),
    )
    third = manager.checkpoint_file.read_bytes()
    _assert_restored(third, complete)
    task.requeue()
    assert task.run()
    new_attempt, new_password = task.attempt_id, task.job_password
    assert new_attempt != old_attempt and new_password != old_password
    with pytest.raises(AttemptMismatchError):
        scheduler.run_command_complete(
            CompleteCommand(node_path="/f/t", attempt_id=old_attempt)
        )
    assert manager.write_checkpoint()
    current = _reference(
        paths=active.paths,
        status="submitted",
        attempt=new_attempt,
        password=new_password,
        slots=1,
        runtime_probe=_runtime_probe(scheduler.bunch),
    )
    fourth = manager.checkpoint_file.read_bytes()
    restored = _assert_restored(fourth, current)
    with pytest.raises(AttemptMismatchError):
        Scheduler(restored).run_command_complete(
            CompleteCommand(node_path="/f/t", attempt_id=old_attempt)
        )
    for data, reference in ((first, before), (second, active), (third, complete)):
        _assert_restored(data, reference)


def test_consecutive_write_faults_retain_last_good_reference_and_backup(tmp_path):
    scheduler, manager, task = _matrix(tmp_path)
    base = _reference(
        paths={"/", "/f", "/f/t", "/f/other"},
        status="queued",
        runtime_probe=_runtime_probe(scheduler.bunch),
    )
    assert manager.write_checkpoint()
    original = manager.checkpoint_file.read_bytes()
    _assert_restored(original, base)
    assert task.run()
    expected_new = _reference(
        paths=base.paths,
        status="submitted",
        attempt=task.attempt_id,
        password=task.job_password,
        slots=1,
        runtime_probe=_runtime_probe(scheduler.bunch),
    )

    def storage_fault(prepared):
        raise OSError(errno.ENOSPC, "secret path")

    replace = manager._replace_checkpoint
    manager._replace_checkpoint = storage_fault
    for _ in range(2):
        assert manager.write_checkpoint() is False
        assert manager.checkpoint_failure_reason == "storage full"
        assert HaltCause.checkpoint_failure in manager.bunch.server_state.halt_causes
        assert manager.checkpoint_file.read_bytes() == original
        _assert_restored(manager.checkpoint_file.read_bytes(), base)
    manager._replace_checkpoint = replace
    assert manager.write_checkpoint()
    _assert_restored(manager.checkpoint_file.read_bytes(), expected_new)
    assert manager.backup_file.read_bytes() == original
    manager.checkpoint_file.write_bytes(b"{corrupt")
    recovered = CheckpointManager(Bunch(), checkpoint_file=manager.checkpoint_file)
    assert recovered.restore()
    assert recovered.restore_summary["source"] == "backup"
    assert recovered.bunch.find_node("/f/t").attempt_id is None
    assert recovered.bunch.find_node("/f").find_limit("slots").value == 0


def test_flow_replace_drops_old_identity_without_changing_prior_file(tmp_path):
    scheduler, manager, task = _matrix(tmp_path)
    assert task.run()
    old_attempt = task.attempt_id
    assert manager.write_checkpoint()
    earlier = manager.checkpoint_file.read_bytes()
    before = _reference(
        paths={"/", "/f", "/f/t", "/f/other"},
        status="submitted",
        attempt=old_attempt,
        password=task.job_password,
        slots=1,
    )
    scheduler.run_command_complete(
        CompleteCommand(node_path="/f/t", attempt_id=old_attempt)
    )
    replacement = Flow("f")
    replacement.add_task(Task("replacement"))
    result = scheduler.run_command_replace(
        ReplaceCommand(
            target_path="/f",
            flow_bytes=export_definition(replacement).model_dump_json().encode(),
        )
    )
    assert result.flag == 0
    assert manager.write_checkpoint()
    current = manager.checkpoint_file.read_bytes()
    expected_paths = {"/", "/f", "/f/replacement"}
    assert set(json.loads(current)["runtime"]["nodes"]) == expected_paths
    restored = restore_checkpoint(current)
    assert restored.find_node("/f/t") is None
    assert restored.find_node("/f/replacement").state.node_status is NodeStatus.queued
    _assert_restored(earlier, before)
    assert manager.backup_file.read_bytes() == earlier


def test_repeat_and_begin_transition_have_separate_recovery_references(tmp_path):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task(Task("repeated"))
    task.add_repeat(RepeatDate("DAY", 20260101, 20260102))
    flow.begin()
    manager = CheckpointManager(bunch, checkpoint_file=tmp_path / "repeat.check")
    scheduler = Scheduler(bunch)
    manager.query_scheduler = scheduler
    assert task.run()
    attempt = task.attempt_id
    assert manager.write_checkpoint()
    first = manager.checkpoint_file.read_bytes()
    first_tree = restore_checkpoint(first)
    first_task = first_tree.find_node("/f/repeated")
    assert first_task.repeat.value() == 20260101
    assert first_task.attempt_id == attempt
    scheduler.run_command_complete(
        CompleteCommand(node_path="/f/repeated", attempt_id=attempt)
    )
    assert task.repeat.value() == 20260102
    assert task.attempt_id is None
    assert manager.write_checkpoint()
    second_tree = restore_checkpoint(manager.checkpoint_file.read_bytes())
    second_task = second_tree.find_node("/f/repeated")
    assert second_task.repeat.value() == 20260102
    assert second_task.attempt_id is None
    assert second_tree.find_node("/f").begun
    assert manager.backup_file.read_bytes() == first
    assert restore_checkpoint(first).find_node("/f/repeated").repeat.value() == 20260101


@pytest.mark.parametrize("fault", ["validation", "worker"])
def test_failed_preparation_recovers_from_last_published_state(
    tmp_path, monkeypatch, fault
):
    scheduler, manager, task = _matrix(tmp_path)
    previous = _reference(
        paths={"/", "/f", "/f/t", "/f/other"},
        status="queued",
        runtime_probe=_runtime_probe(scheduler.bunch),
    )
    assert manager.write_checkpoint()
    original = manager.checkpoint_file.read_bytes()
    assert task.run()
    advanced = _reference(
        paths=previous.paths,
        status="submitted",
        attempt=task.attempt_id,
        password=task.job_password,
        slots=1,
        runtime_probe=_runtime_probe(scheduler.bunch),
    )
    if fault == "validation":
        monkeypatch.setattr(
            "takler.server.checkpoint._prepare_fixed_version",
            lambda *args: (_ for _ in ()).throw(ValueError("bad fixed view")),
        )
        assert manager.write_checkpoint() is False
    else:

        async def crashed(*args):
            raise RuntimeError("worker crashed")

        monkeypatch.setattr(manager, "_write_fixed_background", crashed)
        asyncio.run(manager.write_checkpoint_async())
        assert manager.checkpoint_failure_reason == "snapshot validation error"
    assert manager.checkpoint_file.read_bytes() == original
    _assert_restored(original, previous)
    assert HaltCause.checkpoint_failure in manager.bunch.server_state.halt_causes
    monkeypatch.undo()
    assert manager.write_checkpoint()
    _assert_restored(manager.checkpoint_file.read_bytes(), advanced)


class _Transport:
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.requests = []
        self.mutate = lambda request, reply: reply

    def query(self, request):
        self.requests.append(request)
        service = self.scheduler.query_service
        if isinstance(request, FullSnapshotRequest):
            reply = asyncio.run(service.full_snapshots.page(request))
        elif isinstance(request, FullSnapshotChunkRequest):
            reply = asyncio.run(service.full_snapshots.chunk(request))
        elif isinstance(request, CancelFullSnapshotRequest):
            reply = asyncio.run(service.full_snapshots.cancel(request))
        elif isinstance(request, SinceRequest):
            reply = asyncio.run(service.since(request))
        elif isinstance(request, PageRequest):
            reply = asyncio.run(service.page(request))
        else:
            reply = service.capabilities()
        return self.mutate(request, reply)


def test_incremental_summary_matches_new_page_and_full_version(tmp_path):
    scheduler, _, task = _matrix(tmp_path)
    transport = _Transport(scheduler)
    client = QueryClient(transport)
    initial = client.read_snapshot(scope_path="/f")
    assert initial.get("/f/t").status == "queued"
    assert task.run()
    task.mark_submission_unknown("reply lost")
    attempt = task.attempt_id

    def compare(previous, *, status, unknown):
        delta = client.read_since(previous)
        fresh = QueryClient(transport).read_snapshot(scope_path="/f")
        detailed = FullSnapshotClient(transport).read_full_snapshot(scope_path="/f")
        assert delta.revision == fresh.revision
        assert delta.ordered_paths == fresh.ordered_paths
        for path in fresh.ordered_paths:
            assert delta.get(path) == fresh.get(path)
            row = detailed.nodes_by_path[path]
            summary = fresh.get(path)
            assert row["summary"]["status"] == summary.status
            assert row["summary"]["attempt_id"] == summary.attempt_id
            assert row["summary"]["node_kind"] == summary.node_kind
        assert delta.get("/f/t").status == status
        assert delta.get("/f/t").submission_unknown is unknown
        assert delta.get("/f/t").attempt_id == attempt
        return delta

    unknown = compare(initial, status="submitted", unknown=True)
    asyncio.run(
        scheduler.run_command_init(
            InitCommand(node_path="/f/t", attempt_id=attempt, task_id="rid")
        )
    )
    active = compare(unknown, status="active", unknown=None)
    scheduler.run_command_complete(
        CompleteCommand(node_path="/f/t", attempt_id=attempt)
    )
    complete = compare(active, status="complete", unknown=None)
    assert complete.get("/f").status == "queued"
    assert scheduler._force_path("/f/other", "complete", recursive=False)
    aggregated = compare(complete, status="complete", unknown=None)
    assert aggregated.get("/f").status == "complete"


def test_full_detail_reference_remains_fixed_across_pages_and_blobs(tmp_path):
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    flow.add_parameter("COLOR", "blue")
    flow.add_parameter("TAKLER_SECRET", "hidden-value")
    task = flow.add_task(Task("t"))
    task.add_parameter("PAYLOAD", "α" * 50_000)
    task.add_event("ready")
    task.add_meter("progress", 0, 100)
    for index in range(650):
        flow.add_task(Task(f"filler{index:04d}"))
    flow.begin()
    original_calendar = flow.calendar.initial_time.isoformat()
    scheduler = Scheduler(bunch)
    transport = _Transport(scheduler)
    first_page = True

    def mutate(request, reply):
        nonlocal first_page
        if isinstance(request, FullSnapshotRequest) and first_page:
            assert reply.kind == "full_snapshot_page" and not reply.complete
            first_page = False
            flow.add_parameter("COLOR", "red")
            task.add_parameter("PAYLOAD", "new")
            task.set_event("ready", True)
            task.set_meter("progress", 91)
            task.run()
            flow.add_task(Task("late"))
            flow.requeue_calendar()
            scheduler.run_command_server_halt(ServerHaltCommand())
        return reply

    transport.mutate = mutate
    snapshot = FullSnapshotClient(transport).read_full_snapshot(scope_path="/f")
    assert snapshot.completion.total_nodes == 652
    assert "/f/late" not in snapshot.nodes_by_path
    assert (
        snapshot.completion.state_version < scheduler.version_store.pin().state_version
    )
    assert any(
        isinstance(request, FullSnapshotChunkRequest) for request in transport.requests
    )
    flow_row = snapshot.nodes_by_path["/f"]
    task_row = snapshot.nodes_by_path["/f/t"]
    assert {"summary", "definition", "parameters", "runtime", "artifacts"} == (
        {"summary"} | set(task_row["groups"])
    )
    assert set(task_row["groups"]["definition"]) == {
        "trigger",
        "complete_trigger",
        "script_path",
        "times",
        "repeat",
        "events",
        "meters",
        "limits",
        "in_limits",
    }
    assert set(task_row["groups"]["runtime"]) == {
        "begun",
        "calendar",
        "task_id",
        "try_no",
        "attempt_id",
        "submitted_at",
        "started_at",
        "ended_at",
        "aborted_reason",
        "submission_unknown_reason",
        "events",
        "meters",
        "limits",
        "repeat",
        "status",
        "event_values",
        "meter_values",
        "limit_usage",
        "time_free",
        "repeat_value",
        "trigger_free",
        "complete_trigger_free",
    }
    assert set(task_row["groups"]["artifacts"]) == {
        "job_ref",
        "jobout_ref",
        "orvix_info_path",
        "not_applicable",
    }
    assert task_row["summary"]["name"] == "t"
    assert task_row["summary"]["suspended"] is False
    assert task_row["groups"]["definition"]["events"] == [
        {"name": "ready", "initial_value": False}
    ]
    assert task_row["groups"]["definition"]["meters"] == [
        {"name": "progress", "min_value": 0, "max_value": 100}
    ]
    assert task_row["groups"]["runtime"]["events"] == [
        {"name": "ready", "value": False}
    ]
    assert task_row["groups"]["runtime"]["meters"] == [
        {"name": "progress", "min_value": 0, "max_value": 100, "value": 0}
    ]
    assert {
        item["name"]: item["value"]
        for item in task_row["groups"]["parameters"]["generated"]
    } == {
        "TASK": "t",
        "TAKLER_NAME": "/f/t",
        "TAKLER_RID": None,
        "TAKLER_ATTEMPT_ID": None,
        "TAKLER_TRY_NO": 0,
    }
    assert task_row["groups"]["artifacts"]["not_applicable"] is False
    assert (
        next(
            x for x in flow_row["groups"]["parameters"]["user"] if x["name"] == "COLOR"
        )["value"]
        == "blue"
    )
    secret = next(
        x
        for x in flow_row["groups"]["parameters"]["user"]
        if x["name"] == "TAKLER_SECRET"
    )
    assert secret == {"name": "TAKLER_SECRET", "source_path": "/f", "redacted": True}
    assert (
        next(
            x
            for x in task_row["groups"]["parameters"]["user"]
            if x["name"] == "PAYLOAD"
        )["value"]
        == "α" * 50_000
    )
    assert task_row["groups"]["runtime"]["event_values"] == {"ready": False}
    assert task_row["groups"]["runtime"]["meter_values"] == {"progress": 0}
    assert task_row["summary"]["attempt_id"] is None
    assert task_row["groups"]["runtime"]["status"] == "queued"
    assert (
        flow_row["groups"]["runtime"]["calendar"]["initial_time"] == original_calendar
    )
    assert snapshot.completion.service["status"] == "running"
    assert scheduler.bunch.server_state.status.value == "halted"
    assert "hidden-value" not in json.dumps(snapshot.nodes_by_path)
    assert not scheduler.query_service.full_snapshots._leases


def test_full_detail_budget_failure_never_returns_completion(tmp_path, monkeypatch):
    from takler.client import full_snapshot

    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    flow.add_task(Task("t")).add_parameter("PAYLOAD", "x" * 100_000)
    scheduler = Scheduler(bunch)
    client = FullSnapshotClient(_Transport(scheduler))
    monkeypatch.setattr(full_snapshot, "MAX_ASSEMBLED_BYTES", 100)
    with pytest.raises(QueryResponseError, match="assembly budget"):
        client.read_full_snapshot(scope_path="/f")
    assert not scheduler.query_service.full_snapshots._leases


def test_full_detail_duplicate_page_and_cancel_release_lease():
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    for index in range(650):
        flow.add_task(Task(f"t{index:04d}"))
    scheduler = Scheduler(bunch)
    transport = _Transport(scheduler)
    first = None

    def duplicate(request, reply):
        nonlocal first
        if isinstance(request, FullSnapshotRequest):
            if first is None:
                first = reply
            else:
                return first
        return reply

    transport.mutate = duplicate
    with pytest.raises(Exception, match="page order changed"):
        FullSnapshotClient(transport).read_full_snapshot(scope_path="/f")
    assert not scheduler.query_service.full_snapshots._leases
    assert any(
        isinstance(request, CancelFullSnapshotRequest) for request in transport.requests
    )

    transport.mutate = lambda request, reply: reply
    seen = 0

    def cancelled():
        nonlocal seen
        seen += 1
        return seen > 1

    with pytest.raises(QueryResponseError, match="cancelled"):
        FullSnapshotClient(transport).read_full_snapshot(
            scope_path="/f",
            cancelled=cancelled,
        )
    assert not scheduler.query_service.full_snapshots._leases


def test_full_detail_old_lease_expires_across_checkpoint_restart(tmp_path):
    scheduler, manager, _ = _matrix(tmp_path)
    flow = scheduler.bunch.find_node("/f")
    for index in range(650):
        flow.add_task(Task(f"extra{index:04d}"))
    request = FullSnapshotRequest(
        kind="full_snapshot_request",
        schema_version=1,
        scope_path="/f",
    )
    first = asyncio.run(scheduler.query_service.full_snapshots.page(request))
    assert first.kind == "full_snapshot_page" and first.next_cursor
    assert manager.write_checkpoint()
    fresh = CheckpointManager(Bunch(), checkpoint_file=manager.checkpoint_file)
    assert fresh.restore()
    restarted = Scheduler(fresh.bunch)
    continued = asyncio.run(
        restarted.query_service.full_snapshots.page(
            request.model_copy(
                update={
                    "full_snapshot_id": first.full_snapshot_id,
                    "cursor": first.next_cursor,
                }
            )
        )
    )
    assert continued.kind == "error" and continued.code == "snapshot_expired"
    assert not restarted.query_service.full_snapshots._leases


def test_unread_full_detail_lease_is_evicted_without_partial_success():
    bunch = Bunch()
    bunch.add_flow(Flow("f")).add_task(Task("t"))
    service = Scheduler(bunch).query_service.full_snapshots
    request = FullSnapshotRequest(kind="full_snapshot_request", schema_version=1)
    started, release = threading.Event(), threading.Event()
    render = service._render_page

    def stalled(lease):
        if not started.is_set():
            started.set()
            assert release.wait(5)
        return render(lease)

    service._render_page = stalled

    async def exercise():
        oldest = asyncio.create_task(service.page(request))
        assert await asyncio.to_thread(started.wait, 5)
        second = await service.page(request)
        third = await service.page(request)
        release.set()
        expired = await oldest
        assert second.kind == third.kind == "full_snapshot_page"
        assert expired.kind == "error" and expired.code == "snapshot_expired"
        assert len(service._leases) == 2
        for reply in (second, third):
            cancelled = await service.cancel(
                CancelFullSnapshotRequest(
                    kind="cancel_full_snapshot_request",
                    schema_version=1,
                    session_id=reply.session_id,
                    full_snapshot_id=reply.full_snapshot_id,
                )
            )
            assert cancelled.cancelled
        assert not service._leases

    asyncio.run(exercise())
