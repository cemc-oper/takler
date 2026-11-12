"""An uncertain external submit must keep its current attempt and capacity."""

import asyncio
import json
from unittest.mock import patch

import pytest

from takler.core import Bunch, Flow, NodeStatus, Task
from takler.exceptions import FlowStateError
from takler.protocol.commands import (
    AbortCommand,
    CompleteCommand,
    InitCommand,
    ReplaceCommand,
    ServerHaltCommand,
    ServerResumeCommand,
)
from takler.protocol.query_v1 import PageRequest, SinceRequest
from takler.serialization.checkpoint import capture_checkpoint, restore_checkpoint
from takler.serialization import export_definition
from takler.serialization.query_projection import (
    iter_query_summaries,
    project_query_detail,
)
from takler.server.scheduler import Scheduler
from takler.tasks.shell import ShellScriptTask
from takler.tasks.shell.shell_runner import ShellRunner


def _submitted(tmp_path, *, with_limit=True):
    tmp_path.mkdir(parents=True, exist_ok=True)
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    flow.add_parameter("TAKLER_HOME", str(tmp_path))
    flow.add_parameter("TAKLER_SHELL_JOB_CMD", 'orvix submit "{{ TAKLER_JOB }}"')
    script = tmp_path / "source.takler"
    script.write_text("echo {{ TAKLER_ATTEMPT_ID }}\n")
    task = flow.add_task(ShellScriptTask("t", script))
    limit = None
    if with_limit:
        limit = flow.add_limit("slots", 1)
        task.add_in_limit("slots", "/f")
        sibling = flow.add_task(Task("sibling"))
        sibling.add_in_limit("slots", "/f")
    flow.begin()
    callbacks = []
    with patch.object(ShellRunner, "spawn", lambda self, **kw: callbacks.append(kw)):
        assert task.run()
    return bunch, flow, task, limit, callbacks[0]


def _unknown(tmp_path, *, with_limit=True):
    bunch, flow, task, limit, callbacks = _submitted(tmp_path, with_limit=with_limit)
    callbacks["on_failure"](TimeoutError("reply lost"))
    return bunch, flow, task, limit, callbacks


def test_unknown_is_queryable_and_survives_checkpoint(tmp_path):
    bunch, flow, task, limit, callbacks = _unknown(tmp_path)
    attempt = task.attempt_id
    assert task.state.node_status is NodeStatus.submitted
    assert task.submission_unknown_reason == "TimeoutError: reply lost"
    assert task.ended_at is None
    assert limit.value == 1
    assert not flow.find_node("/f/sibling").check_dependencies()

    summary = next(row for row, _ in iter_query_summaries(bunch) if row.path == "/f/t")
    assert summary.submission_unknown is True
    detail = project_query_detail(bunch, "/f/t", ["runtime", "artifacts"])
    assert detail.runtime.submission_unknown_reason == task.submission_unknown_reason
    assert detail.artifacts.job_ref.path == task.job_path

    callbacks["on_failure"](RuntimeError("later failure"))
    assert task.submission_unknown_reason == "TimeoutError: reply lost"
    saved = capture_checkpoint(bunch)
    restored = restore_checkpoint(saved)
    current = restored.find_node("/f/t")
    assert current.attempt_id == attempt
    assert current.submission_unknown_reason == task.submission_unknown_reason
    assert current.job_path == task.job_path
    assert restored.find_node("/f").find_limit("slots").value == 1
    assert not restored.find_node("/f/sibling").check_dependencies()
    asyncio.run(
        Scheduler(restored).run_command_init(
            InitCommand(node_path="/f/t", attempt_id=attempt, task_id="restored-job")
        )
    )
    assert current.state.node_status is NodeStatus.active
    assert current.submission_unknown_reason is None
    assert restored.find_node("/f").find_limit("slots").value == 1
    Scheduler(restored).run_command_complete(
        CompleteCommand(node_path="/f/t", attempt_id=attempt)
    )
    assert restored.find_node("/f").find_limit("slots").value == 0


def test_unknown_blocks_automatic_and_bulk_reset_until_verified(tmp_path):
    bunch, flow, task, limit, callbacks = _unknown(tmp_path)
    scheduler = Scheduler(bunch)
    attempt = task.attempt_id
    for operation in (
        task.requeue,
        flow.requeue,
        lambda: flow.begin(force=True),
        lambda: scheduler._run_node("/f/t", force=True),
        lambda: scheduler._force_path("/f/t", "queued", recursive=False),
        lambda: scheduler._force_path("/f", "complete", recursive=True),
    ):
        with pytest.raises(FlowStateError, match="unknown external submission"):
            operation()
        assert task.attempt_id == attempt
        assert task.state.node_status is NodeStatus.submitted
        assert limit.value == 1

    replacement = Flow("f")
    replacement.add_task("t")
    with pytest.raises(FlowStateError, match="replace blocked"):
        scheduler.run_command_replace(
            ReplaceCommand(
                target_path="/f",
                flow_bytes=json.dumps(
                    export_definition(replacement).model_dump(mode="json")
                ).encode(),
            )
        )
    assert bunch.find_flow("f") is flow

    # A direct terminal force is an explicit operator disposition after
    # checking the external scheduler and the job's recorded artifact paths.
    assert scheduler._force_path("/f/t", "aborted", recursive=False)
    assert task.submission_unknown_reason is None
    assert task.state.node_status is NodeStatus.aborted
    assert limit.value == 0
    task.requeue()
    assert task.attempt_id is None
    assert flow.find_node("/f/sibling").check_dependencies()
    callbacks["on_failure"](RuntimeError("stale callback"))
    assert task.state.node_status is NodeStatus.queued


def test_child_evidence_resolves_unknown_without_releasing_active_capacity(tmp_path):
    bunch, _, task, limit, callbacks = _unknown(tmp_path)
    scheduler = Scheduler(bunch)
    attempt = task.attempt_id
    asyncio.run(
        scheduler.run_command_init(
            InitCommand(node_path="/f/t", attempt_id=attempt, task_id="external-42")
        )
    )
    assert task.state.node_status is NodeStatus.active
    assert task.submission_unknown_reason is None
    assert limit.value == 1
    callbacks["on_failure"](RuntimeError("wrapper exited after init"))
    assert task.state.node_status is NodeStatus.active
    scheduler.run_command_complete(
        CompleteCommand(node_path="/f/t", attempt_id=attempt)
    )
    assert task.state.node_status is NodeStatus.complete
    assert limit.value == 0


def test_child_abort_and_confirmed_nonstart_release_current_attempt(tmp_path):
    bunch, _, task, limit, _ = _unknown(tmp_path)
    Scheduler(bunch).run_command_abort(
        AbortCommand(node_path="/f/t", attempt_id=task.attempt_id, reason="job failed")
    )
    assert task.state.node_status is NodeStatus.aborted
    assert task.submission_unknown_reason is None
    assert limit.value == 0

    _, _, fresh, fresh_limit, callbacks = _submitted(tmp_path / "next")
    callbacks["on_skipped"]()
    assert fresh.state.node_status is NodeStatus.aborted
    assert fresh.submission_unknown_reason is None
    assert fresh_limit.value == 0


def test_late_complete_without_init_resolves_unknown(tmp_path):
    bunch, _, task, limit, callbacks = _unknown(tmp_path)
    attempt = task.attempt_id
    Scheduler(bunch).run_command_complete(
        CompleteCommand(node_path="/f/t", attempt_id=attempt)
    )
    assert task.state.node_status is NodeStatus.complete
    assert task.submission_unknown_reason is None
    assert limit.value == 0
    callbacks["on_failure"](RuntimeError("late wrapper failure"))
    assert task.state.node_status is NodeStatus.complete


def test_unknown_without_limit_still_rejects_forced_rerun(tmp_path):
    bunch, _, task, _, _ = _unknown(tmp_path, with_limit=False)
    with pytest.raises(FlowStateError, match="unknown external submission"):
        Scheduler(bunch)._run_node("/f/t", force=True)
    assert task.try_no == 1


@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("orvix metadata write failed"),
        TimeoutError("submit timed out"),
        RuntimeError("submit reply lost"),
    ],
)
def test_external_failure_modes_keep_capacity(tmp_path, failure):
    bunch, _, task, limit, callbacks = _submitted(tmp_path)
    callbacks["on_failure"](failure)
    assert task.state.node_status is NodeStatus.submitted
    assert task.submission_unknown_reason
    assert limit.value == 1
    with pytest.raises(FlowStateError, match="unknown external submission"):
        Scheduler(bunch)._run_node("/f/t", force=True)


def test_unknown_diagnostic_redacts_current_job_password(tmp_path):
    bunch, _, task, _, callbacks = _submitted(tmp_path)
    password = task.job_password
    callbacks["on_failure"](RuntimeError(f"lost reply for {password}"))
    assert password not in task.submission_unknown_reason
    assert "<redacted>" in task.submission_unknown_reason
    detail = project_query_detail(bunch, "/f/t", ["runtime"])
    assert password not in detail.runtime.submission_unknown_reason
    assert (
        password
        not in capture_checkpoint(bunch).runtime.nodes["/f/t"].submission_unknown_reason
    )


def test_unknown_change_appears_in_incremental_summary(tmp_path):
    async def exercise():
        bunch, _, task, _, callbacks = _submitted(tmp_path)
        service = Scheduler(bunch).query_service
        page = await service.page(
            PageRequest(
                kind="page_request",
                schema_version=1,
                scope_path="/f",
                field_groups=["summary"],
            )
        )
        callbacks["on_failure"](TimeoutError("reply lost"))
        assert task.submission_unknown_reason is not None
        reply = await service.since(
            SinceRequest(
                kind="since_request",
                schema_version=1,
                scope_path="/f",
                field_groups=["summary"],
                session_id=page.session_id,
                query_id=page.query_id,
                since_revision=page.base_revision,
            )
        )
        assert reply.kind == "since"
        assert any(
            op.path == "/f/t" and op.node.submission_unknown is True
            for op in reply.operations
            if op.op == "upsert_summary"
        )

    asyncio.run(exercise())


def test_halt_and_resume_preserve_unknown_reservation(tmp_path):
    bunch, _, task, limit, _ = _unknown(tmp_path)
    scheduler = Scheduler(bunch)
    scheduler.run_command_server_halt(ServerHaltCommand())
    restored = restore_checkpoint(capture_checkpoint(bunch))
    assert restored.find_node("/f/t").submission_unknown_reason
    assert restored.find_node("/f").find_limit("slots").value == 1
    scheduler.run_command_server_resume(ServerResumeCommand())
    assert task.submission_unknown_reason
    assert limit.value == 1
