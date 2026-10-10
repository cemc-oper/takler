"""R2-08 incremental summary batches and reset boundaries."""

import asyncio
import time
from dataclasses import replace

from takler.core import Bunch, Flow, NodeStatus
from takler.protocol.query_v1 import PageRequest, SinceRequest, encode_query_document
from takler.server.auth import (
    CallCredentials,
    reset_call_credentials,
    set_call_credentials,
)
from takler.server.query_changes import MAX_CHANGE_BATCHES, MAX_CHANGE_SECONDS
from takler.server.scheduler import Scheduler


def _page_request(**changes):
    values = dict(kind="page_request", schema_version=1, field_groups=["summary"])
    values.update(changes)
    return PageRequest(**values)


def _since_request(page, **changes):
    values = dict(
        kind="since_request",
        schema_version=1,
        field_groups=["summary"],
        session_id=page.session_id,
        query_id=page.query_id,
        since_revision=page.base_revision,
    )
    values.update(changes)
    return SinceRequest(**values)


def test_unchanged_and_sparse_summary_without_tree_scan(monkeypatch):
    async def exercise():
        bunch = Bunch("root")
        flow = bunch.add_flow(Flow("f"))
        task = flow.add_task("t")
        flow.add_task("other")
        service = Scheduler(bunch).query_service
        page = await service.page(_page_request())
        request = _since_request(page)

        def fail_scan(*args, **kwargs):
            raise AssertionError("unchanged query scanned the tree")

        monkeypatch.setattr(
            "takler.server.query_service.iter_query_summaries", fail_scan
        )
        unchanged = await service.since(request)
        assert unchanged.kind == "since"
        assert unchanged.unchanged and unchanged.operations == []
        assert len(encode_query_document(unchanged)) < 1024
        monkeypatch.undo()

        task.set_node_status(NodeStatus.queued)
        changed = await service.since(request)
        assert changed.kind == "since" and not changed.unchanged
        assert changed.target_revision > changed.from_revision
        assert [op.path for op in changed.operations] == ["/f", "/f/t"]
        assert all(op.node.status == "queued" for op in changed.operations)
        assert len(encode_query_document(changed)) < 8192

    asyncio.run(exercise())


def test_scope_depth_and_filtered_empty_batch():
    async def exercise():
        bunch = Bunch("root")
        f = bunch.add_flow(Flow("f"))
        nested = f.add_container("c").add_task("t")
        other = bunch.add_flow(Flow("other")).add_task("x")
        service = Scheduler(bunch).query_service
        page = await service.page(_page_request(scope_path="/f", depth=1))
        request = _since_request(page, scope_path="/f", depth=1)
        nested.set_node_status_only(NodeStatus.queued)
        other.set_node_status_only(NodeStatus.queued)
        reply = await service.since(request)
        assert reply.kind == "since"
        assert reply.target_revision > reply.from_revision
        assert not reply.unchanged and reply.operations == []

    asyncio.run(exercise())


def test_unrequested_runtime_group_advances_only_waterline():
    async def exercise():
        bunch = Bunch("root")
        task = bunch.add_flow(Flow("f")).add_task("t")
        task.add_meter("m", 0, 100)
        service = Scheduler(bunch).query_service
        page = await service.page(_page_request())
        task.set_meter("m", 3)
        reply = await service.since(_since_request(page))
        assert reply.kind == "since"
        assert reply.target_revision > page.base_revision
        assert not reply.unchanged and reply.operations == []

    asyncio.run(exercise())


def test_summary_upsert_clears_old_attempt_identity():
    async def exercise():
        bunch = Bunch("root")
        task = bunch.add_flow(Flow("f")).add_task("t")
        service = Scheduler(bunch).query_service
        page = await service.page(_page_request())
        assert task.run()
        active = await service.since(_since_request(page))
        active_task = next(op for op in active.operations if op.path == "/f/t")
        assert active_task.node.attempt_id
        task.requeue()
        cleared = await service.since(
            _since_request(page, since_revision=active.target_revision)
        )
        cleared_task = next(op for op in cleared.operations if op.path == "/f/t")
        assert cleared_task.node.attempt_id is None

    asyncio.run(exercise())


def test_structure_and_expired_history_require_reset():
    async def exercise():
        bunch = Bunch("root")
        flow = bunch.add_flow(Flow("f"))
        task = flow.add_task("t")
        task.add_meter("m", 0, MAX_CHANGE_BATCHES + 1)
        scheduler = Scheduler(bunch)
        service = scheduler.query_service
        page = await service.page(_page_request(scope_path="/f"))
        request = _since_request(page, scope_path="/f")
        flow.add_task("new")
        assert (await service.since(request)).reason == "structure_changed"
        for value in range(1, MAX_CHANGE_BATCHES + 2):
            task.set_meter("m", value)
        assert (await service.since(request)).reason == "history_expired"

    asyncio.run(exercise())


def test_local_structure_changes_invalidate_only_affected_subtree():
    async def exercise():
        bunch = Bunch("root")
        flow = bunch.add_flow(Flow("f"))
        flow.add_task("old")
        scheduler = Scheduler(bunch)
        service = scheduler.query_service
        page = await service.page(_page_request())
        request = _since_request(page)
        flow.add_task("new")
        reply = await service.since(request)
        assert reply.kind == "since"
        assert [(op.op, op.path) for op in reply.operations] == [
            ("invalidate_scope", "/f")
        ]
        next_revision = reply.target_revision
        flow.delete_child("old")
        after_delete = await service.since(
            request.model_copy(update={"since_revision": next_revision})
        )
        assert [(op.op, op.path) for op in after_delete.operations] == [
            ("invalidate_scope", "/f")
        ]
        bunch.add_flow(Flow("other"))
        after_load = await service.since(
            request.model_copy(update={"since_revision": after_delete.target_revision})
        )
        assert [(op.op, op.path) for op in after_load.operations] == [
            ("invalidate_scope", "/other")
        ]
        bunch.delete_flow("other")
        after_remove = await service.since(
            request.model_copy(update={"since_revision": after_load.target_revision})
        )
        assert [(op.op, op.path) for op in after_remove.operations] == [
            ("invalidate_scope", "/other")
        ]

    asyncio.run(exercise())


def test_detail_definition_and_inherited_parameters_invalidate_summary_cache():
    async def exercise():
        bunch = Bunch("root")
        flow = bunch.add_flow(Flow("f"))
        task = flow.add_task("t")
        service = Scheduler(bunch).query_service
        root = await service.page(_page_request())
        selected = await service.page(_page_request(scope_path="/f/t"))
        flow.add_parameter("INHERITED", "new")
        local = await service.since(_since_request(root))
        assert [(op.op, op.path) for op in local.operations] == [
            ("invalidate_scope", "/f")
        ]
        reset = await service.since(_since_request(selected, scope_path="/f/t"))
        assert reset.kind == "reset" and reset.reason == "structure_changed"
        next_revision = local.target_revision
        task.add_event("new_event")
        definition = await service.since(
            _since_request(root, since_revision=next_revision)
        )
        assert [(op.op, op.path) for op in definition.operations] == [
            ("invalidate_scope", "/f/t")
        ]

    asyncio.run(exercise())


def test_time_expiry_and_query_identity():
    async def exercise():
        bunch = Bunch("root")
        task = bunch.add_flow(Flow("f")).add_task("t")
        scheduler = Scheduler(bunch)
        service = scheduler.query_service
        page = await service.page(_page_request(scope_path="/f"))
        request = _since_request(page, scope_path="/f")
        assert (
            await service.since(request.model_copy(update={"session_id": "old"}))
        ).reason == "session_changed"
        assert (
            await service.since(request.model_copy(update={"query_id": "old"}))
        ).reason == "scope_changed"
        assert (
            await service.since(
                request.model_copy(update={"since_revision": page.base_revision + 1})
            )
        ).reason == "identity_changed"
        task.set_node_status(NodeStatus.queued)
        tracker = scheduler.query_changes
        tracker._batches[0] = replace(
            tracker._batches[0], created_at=time.monotonic() - MAX_CHANGE_SECONDS - 1
        )
        assert (await service.since(request)).reason == "history_expired"

    asyncio.run(exercise())


def test_delta_pages_keep_target_and_replay_after_later_change():
    async def exercise():
        bunch = Bunch("root")
        flow = bunch.add_flow(Flow("f"))
        tasks = [flow.add_task(f"t{i:04}") for i in range(1500)]
        scheduler = Scheduler(bunch)
        service = scheduler.query_service
        page = await service.page(_page_request())
        request = _since_request(page)
        scheduler.query_changes.begin()
        for task in tasks:
            task.set_node_status_only(NodeStatus.queued)
        scheduler.query_changes.end()
        first = await service.since(request)
        assert first.kind == "since" and first.next_cursor
        assert len(first.operations) <= 2048
        assert len(encode_query_document(first)) <= 262144
        target = first.target_revision
        tasks[0].set_node_status_only(NodeStatus.complete)
        cursor = first.next_cursor
        seen = {op.path for op in first.operations}
        while cursor:
            next_request = request.model_copy(update={"cursor": cursor})
            reply = await service.since(next_request)
            replay = await service.since(next_request)
            assert encode_query_document(reply) == encode_query_document(replay)
            assert reply.target_revision == target
            assert all(op.node.status == "queued" for op in reply.operations)
            seen.update(op.path for op in reply.operations)
            cursor = reply.next_cursor
        assert len(seen) == 1500
        assert (await service.since(request)).target_revision > target

    asyncio.run(exercise())


def test_delta_capture_keeps_fixed_version_during_concurrent_write():
    async def exercise():
        import threading

        bunch = Bunch("root")
        task = bunch.add_flow(Flow("f")).add_task("t")
        service = Scheduler(bunch).query_service
        page = await service.page(_page_request())
        task.set_node_status_only(NodeStatus.queued)
        entered = threading.Event()
        release = threading.Event()
        build = service._build_delta_pages

        def delayed(*args):
            entered.set()
            release.wait(timeout=5)
            return build(*args)

        service._build_delta_pages = delayed
        pending = asyncio.create_task(service.since(_since_request(page)))
        assert await asyncio.to_thread(entered.wait, 5)
        task.set_node_status_only(NodeStatus.complete)
        release.set()
        result = await pending
        assert result.kind == "since"
        assert result.operations[0].node.status == "queued"

    asyncio.run(exercise())


def test_flow_name_and_scope_path_have_same_query_identity():
    async def exercise():
        bunch = Bunch("root")
        bunch.add_flow(Flow("f"))
        service = Scheduler(bunch).query_service
        page = await service.page(_page_request(flow_name="f"))
        reply = await service.since(_since_request(page, scope_path="/f"))
        assert reply.kind == "since" and reply.unchanged

    asyncio.run(exercise())


def test_delta_budget_returns_reset(monkeypatch):
    async def exercise():
        bunch = Bunch("root")
        task = bunch.add_flow(Flow("f")).add_task("t")
        service = Scheduler(bunch).query_service
        page = await service.page(_page_request())
        task.set_node_status_only(NodeStatus.queued)
        monkeypatch.setattr("takler.server.query_service.MAX_DELTA_BATCH_BYTES", 100)
        reply = await service.since(_since_request(page))
        assert reply.kind == "reset" and reply.reason == "budget_exceeded"

    asyncio.run(exercise())


def test_changed_credentials_cannot_reuse_query_identity():
    async def exercise():
        bunch = Bunch("root")
        bunch.add_flow(Flow("f"))
        service = Scheduler(bunch).query_service
        page = await service.page(_page_request())
        token = set_call_credentials(CallCredentials(user="other", secret="new"))
        try:
            reply = await service.since(_since_request(page))
            assert reply.kind == "reset" and reply.reason == "scope_changed"
            assert reply.reset_scope == "/"
        finally:
            reset_call_credentials(token)

    asyncio.run(exercise())
