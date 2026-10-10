"""R3 baseline: desired behavior for defects assigned to later R3 tasks.

Strict xfails keep each regression executable until its owning fix lands.
The process entry point is fake; these tests never submit an external job.
"""

import asyncio
import threading
import time
from dataclasses import replace
from unittest.mock import patch

import pytest

from takler.core import Bunch, Flow, Task
from takler.protocol.query_v1 import DetailRequest, PageRequest, SinceRequest
from takler.serialization.checkpoint import capture_checkpoint, restore_checkpoint
from takler.server.scheduler import Scheduler
from takler.tasks.shell import ShellScriptTask
from takler.tasks.shell.shell_runner import ShellRunner


def _shell_tree(tmp_path):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    flow.add_parameter("TAKLER_HOME", str(tmp_path))
    script = tmp_path / "source.takler"
    script.write_text("echo {{ TAKLER_ATTEMPT_ID }}\n")
    task = flow.add_task(ShellScriptTask("t", script))
    flow.begin()
    return bunch, flow, task


def _page_request():
    return PageRequest(
        kind="page_request",
        schema_version=1,
        scope_path="/f",
        field_groups=["summary"],
    )


def test_requeued_attempt_never_enters_process_start(tmp_path):
    async def exercise():
        _, _, task = _shell_tree(tmp_path)
        commands = []

        async def fake_process(argv):
            commands.append(argv[-1])

        with patch("takler.tasks.shell.shell_runner.run_process", fake_process):
            assert task.run()
            old_job = task.job_path
            task.requeue()
            assert task.run()
            new_job = task.job_path
            for _ in range(4):
                await asyncio.sleep(0)
        assert old_job != new_job
        assert len(commands) == 1
        assert new_job in commands[0]
        assert old_job not in commands[0]

    asyncio.run(exercise())


@pytest.mark.xfail(strict=True, reason="R3-03: unknown submit releases limit")
def test_unknown_external_submit_keeps_capacity_after_restore(tmp_path):
    bunch, flow, task = _shell_tree(tmp_path)
    flow.add_parameter("TAKLER_SHELL_JOB_CMD", 'orvix submit "{{ TAKLER_JOB }}"')
    limit = flow.add_limit("slots", 1)
    task.add_in_limit("slots", "/f")
    next_task = flow.add_task(Task("next"))
    next_task.add_in_limit("slots", "/f")
    next_task.requeue()
    callbacks = []
    with patch.object(ShellRunner, "spawn", lambda self, **kw: callbacks.append(kw)):
        assert task.run()
        assert limit.value == 1
        attempt = task.attempt_id
        callbacks[0]["on_failure"](RuntimeError("submission result lost"))

    assert task.attempt_id == attempt
    assert limit.value == 1
    assert not next_task.check_dependencies()
    restored = restore_checkpoint(capture_checkpoint(bunch))
    restored_flow = restored.find_node("/f")
    restored_task = restored.find_node("/f/t")
    assert restored_task.attempt_id == attempt
    assert restored_flow.find_limit("slots").value == 1


@pytest.mark.xfail(strict=True, reason="R3-04: detail lease expires while since works")
def test_unchanged_session_can_read_detail_after_page_ttl():
    async def exercise():
        bunch = Bunch()
        flow = bunch.add_flow(Flow("f"))
        flow.add_task("t")
        service = Scheduler(bunch).query_service
        first = await service.page(_page_request())
        assert first.kind == "page"
        request = DetailRequest(
            kind="detail_request",
            schema_version=1,
            session_id=first.session_id,
            query_id=first.query_id,
            path="/f/t",
            flow_generation=flow.definition_generation,
            attempt_id=None,
            field_groups=["runtime"],
        )
        assert (await service.detail(request)).kind == "detail"
        service._snapshots[first.snapshot_id] = replace(
            service._snapshots[first.snapshot_id], expires=time.monotonic() - 1
        )
        since = await service.since(
            SinceRequest(
                kind="since_request",
                schema_version=1,
                scope_path="/f",
                field_groups=["summary"],
                session_id=first.session_id,
                query_id=first.query_id,
                since_revision=first.base_revision,
            )
        )
        assert since.kind == "since" and since.unchanged
        assert (await service.detail(request)).kind == "detail"

    asyncio.run(exercise())


@pytest.mark.xfail(
    strict=True, reason="R3-08: unrelated write invalidates scoped capture"
)
def test_unrelated_write_does_not_exhaust_scoped_capture():
    async def exercise():
        bunch = Bunch()
        bunch.add_flow(Flow("f")).add_task("t")
        unrelated = bunch.add_flow(Flow("unrelated")).add_task("t")
        service = Scheduler(bunch).query_service
        entered = threading.Event()
        release = threading.Event()
        original = service._capture

        def delayed(*args):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("capture barrier timed out")
            return original(*args)

        with patch.object(service, "_capture", delayed):
            pending = asyncio.create_task(service.page(_page_request()))
            try:
                deadline = time.monotonic() + 5
                while not entered.is_set():
                    if pending.done() or time.monotonic() >= deadline:
                        raise AssertionError("capture did not reach barrier")
                    await asyncio.sleep(0.001)
                unrelated.suspend()
            finally:
                release.set()
            reply = await asyncio.wait_for(pending, 5)
        assert reply.kind == "page"

    asyncio.run(exercise())
