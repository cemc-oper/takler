"""Concurrent readers share bounded fixed summary content and private views."""

import asyncio
from dataclasses import replace
import threading
import weakref

import pytest

from takler.core import Bunch, Flow
from takler.protocol.query_v1 import decode_query_document
from takler.server import query_service as module
from takler.server.auth import (
    CallCredentials,
    reset_call_credentials,
    set_call_credentials,
)
from takler.server.scheduler import Scheduler
from tests.server.test_query_service import page_request


def service(tasks=3):
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    for i in range(tasks):
        flow.add_task(f"t{i}")
    scheduler = Scheduler(bunch)
    return scheduler, scheduler.query_service, flow


def pause_capture(monkeypatch, query, failure=None):
    entered, proceed = threading.Event(), threading.Event()
    calls = []
    original = query._capture

    def capture(*args):
        calls.append(args[6])
        entered.set()
        assert proceed.wait(5)
        if failure is not None:
            raise failure
        return original(*args)

    monkeypatch.setattr(query, "_capture", capture)
    return entered, proceed, calls


async def settled(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait(), 3)


def assert_released(query):
    assert (
        not query._captures
        and not query._owner_reserved
        and not query._content_captures
    )
    assert query._reserved == query._capture_readers == 0


def test_three_baselines_share_one_capture_and_keep_complete_fixed_pages(monkeypatch):
    async def run():
        scheduler, query, flow = service(2300)
        entered, proceed, calls = pause_capture(monkeypatch, query)
        request = page_request()
        readers = [asyncio.create_task(query.page_encoded(request)) for _ in range(3)]
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            # Both old and new implementations have accepted their work before
            # the blocked worker is released. The old third request is refused.
            await asyncio.sleep(0.02)
            proceed.set()
            replies = await asyncio.gather(*readers)
            first = [decode_query_document(raw) for raw in replies]
            assert all(page.kind == "page" for page in first)
            assert len(calls) == 1
            assert all(raw is replies[0] for raw in replies)
            assert len(query._snapshots) == 1
            assert_released(query)
            flow.add_task("later")
            for page in first:
                paths = [node.path for node in page.nodes]
                while page.next_cursor:
                    raw = await query.page_encoded(
                        request.model_copy(update={"cursor": page.next_cursor})
                    )
                    page = decode_query_document(raw)
                    assert page.snapshot_id == first[0].snapshot_id
                    assert page.base_revision == first[0].base_revision
                    paths.extend(node.path for node in page.nodes)
                assert len(paths) == len(set(paths)) == 2302
                assert "/f/later" not in paths
        finally:
            proceed.set()
            query.close()

    asyncio.run(run())


def test_retained_reuse_does_not_extend_page_expiry_or_retain_old_root(monkeypatch):
    async def run():
        scheduler, query, flow = service()
        clock = [100.0]
        monkeypatch.setattr(module, "_monotonic", lambda: clock[0])
        try:
            root = scheduler.version_store.pin()
            reference = weakref.ref(root)
            first = await query.page(page_request())
            expiry = query._snapshots[first.snapshot_id].expires
            clock[0] += 10
            same = await query.page(page_request())
            assert same.snapshot_id == first.snapshot_id
            assert query._snapshots[first.snapshot_id].expires == expiry
            assert same.expires_at == first.expires_at
            assert same is not first and same.nodes[0] is not first.nodes[0]
            assert query._query_leases[first.query_id].expires > expiry
            flow.add_task("new")
            latest = await query.page(page_request())
            assert latest.snapshot_id != first.snapshot_id
            assert latest.base_revision > first.base_revision
            del root
            await asyncio.sleep(0)
            assert reference() is None
            assert (
                first.snapshot_id in query._snapshots
            )  # Old encoded replay remains valid.
            clock[0] += module.SNAPSHOT_SECONDS + 1
            rebuilt = await query.page(page_request())
            assert rebuilt.snapshot_id != latest.snapshot_id
        finally:
            query.close()

    asyncio.run(run())


def test_equal_revision_and_counter_do_not_replace_root_identity():
    async def run():
        scheduler, query, _ = service()
        try:
            first = await query.page(page_request())
            old = scheduler.version_store.pin()
            scheduler.version_store.current = replace(old)
            next_page = await query.page(page_request())
            assert next_page.base_revision == first.base_revision
            assert next_page.snapshot_id != first.snapshot_id
        finally:
            query.close()

    asyncio.run(run())


def test_nearly_expired_retained_pages_do_not_start_a_new_complete_read(monkeypatch):
    async def run():
        _, query, _ = service()
        clock = [100.0]
        monkeypatch.setattr(module, "_monotonic", lambda: clock[0])
        try:
            first = await query.page(page_request())
            expiry = query._snapshots[first.snapshot_id].expires
            clock[0] += module.SNAPSHOT_SECONDS / 2 + 1
            fresh = await query.page(page_request())
            assert fresh.snapshot_id != first.snapshot_id
            assert query._snapshots[first.snapshot_id].expires == expiry
            assert query._snapshots[fresh.snapshot_id].expires > expiry
        finally:
            query.close()

    asyncio.run(run())


@pytest.mark.parametrize("other", ["owner", "scope", "depth"])
def test_pending_reuse_isolated_by_owner_and_selection(monkeypatch, other):
    async def run():
        _, query, _ = service()
        entered, proceed, calls = pause_capture(monkeypatch, query)
        first = asyncio.create_task(query.page(page_request()))
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            if other == "owner":
                token = set_call_credentials(
                    CallCredentials(user="other", secret="other")
                )
                try:
                    second = asyncio.create_task(query.page(page_request()))
                finally:
                    reset_call_credentials(token)
            else:
                selected = (
                    page_request(scope_path="/f")
                    if other == "scope"
                    else page_request(depth=0)
                )
                second = asyncio.create_task(query.page(selected))
            await settled(lambda: query._reserved == 2)
            proceed.set()
            a, b = await asyncio.gather(first, second)
            assert len(calls) == (1 if other == "owner" else 2)
            assert a.kind == b.kind == "page"
            assert a.query_id != b.query_id and a.snapshot_id != b.snapshot_id
            assert_released(query)
        finally:
            proceed.set()
            query.close()

    asyncio.run(run())


def test_other_root_and_selection_keep_existing_owner_capture_quota(monkeypatch):
    async def run():
        scheduler, query, flow = service()
        entered, proceed, calls = pause_capture(monkeypatch, query)
        first = asyncio.create_task(query.page(page_request()))
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            flow.add_task("new")
            second = asyncio.create_task(query.page(page_request()))
            await settled(lambda: len(calls) == 2)
            denied = await query.page(page_request(scope_path="/f"))
            assert denied.code == "resource_exhausted"
            assert query._reserved == module.MAX_OWNER_SNAPSHOTS == 2
            proceed.set()
            a, b = await asyncio.gather(first, second)
            assert a.base_revision < b.base_revision and a.snapshot_id != b.snapshot_id
            assert len(query._snapshots) == 2
            assert_released(query)
        finally:
            proceed.set()
            query.close()

    asyncio.run(run())


def test_shared_reader_waiting_is_bounded(monkeypatch):
    async def run():
        _, query, _ = service()
        entered, proceed, calls = pause_capture(monkeypatch, query)
        monkeypatch.setattr(module, "MAX_SUMMARY_READERS", 4)
        readers = [asyncio.create_task(query.page(page_request())) for _ in range(4)]
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            await settled(lambda: query._capture_readers == 3)
            assert (await query.page(page_request())).code == "resource_exhausted"
            assert (
                query._reserved + query._capture_readers == module.MAX_SUMMARY_READERS
            )
            proceed.set()
            assert all(p.kind == "page" for p in await asyncio.gather(*readers))
            assert len(calls) == 1
            assert_released(query)
        finally:
            proceed.set()
            query.close()

    asyncio.run(run())


@pytest.mark.parametrize("cancel_all", [False, True])
def test_cancellation_releases_only_its_reader_and_abandons_unused_pages(
    monkeypatch, cancel_all
):
    async def run():
        _, query, _ = service()
        entered, proceed, _ = pause_capture(monkeypatch, query)
        readers = [asyncio.create_task(query.page(page_request())) for _ in range(3)]
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            await settled(lambda: query._capture_readers == 2)
            for reader in readers if cancel_all else readers[:1]:
                reader.cancel()
            await settled(
                lambda: (
                    next(iter(query._captures.values())).readers
                    == (0 if cancel_all else 2)
                )
            )
            assert query._reserved == 1 and not query._snapshots
            if cancel_all:
                readers[0].cancel()  # Cleanup must survive repeated cancellation.
            proceed.set()
            results = await asyncio.gather(*readers, return_exceptions=True)
            assert isinstance(results[0], asyncio.CancelledError)
            if cancel_all:
                assert all(isinstance(r, asyncio.CancelledError) for r in results)
                assert not query._snapshots
            else:
                assert all(r.kind == "page" for r in results[1:])
            await settled(lambda: not query._captures)
            assert_released(query)
        finally:
            proceed.set()
            query.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "failure,code",
    [
        (ValueError("bad"), "invalid_request"),
        (MemoryError("full"), "resource_exhausted"),
        (RuntimeError("unavailable"), "resource_exhausted"),
    ],
)
def test_shared_capture_failure_is_detached_and_releases_reservations(
    monkeypatch, failure, code
):
    async def run():
        _, query, _ = service()
        entered, proceed, calls = pause_capture(monkeypatch, query, failure)
        readers = [asyncio.create_task(query.page(page_request())) for _ in range(3)]
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            await settled(lambda: query._capture_readers == 2)
            proceed.set()
            replies = await asyncio.gather(*readers)
            assert all(r.code == code for r in replies)
            assert len({id(r) for r in replies}) == 3 and len(calls) == 1
            assert_released(query)
            assert not query._snapshots
        finally:
            proceed.set()
            query.close()

    asyncio.run(run())


def test_idle_expiry_releases_pages_without_expiring_scope_authorization(monkeypatch):
    async def run():
        _, query, _ = service()
        monkeypatch.setattr(module, "SNAPSHOT_SECONDS", 0.02)
        try:
            first = await query.page(page_request())
            assert query._snapshots
            await settled(lambda: not query._snapshots)
            assert first.query_id in query._query_leases
            assert query._snapshot_expiry is None
        finally:
            query.close()

    asyncio.run(run())


def test_close_prevents_late_capture_publication(monkeypatch):
    async def run():
        _, query, _ = service()
        entered, proceed, _ = pause_capture(monkeypatch, query)
        readers = [asyncio.create_task(query.page(page_request())) for _ in range(3)]
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            await settled(lambda: query._capture_readers == 2)
            query.close()
            proceed.set()
            replies = await asyncio.gather(*readers)
            assert all(r.code == "snapshot_expired" for r in replies)
            assert not query._snapshots and not query._query_leases
            assert query._snapshot_expiry is None
            assert_released(query)
        finally:
            proceed.set()
            query.close()

    asyncio.run(run())


async def read_as(query, user, request):
    token = set_call_credentials(
        CallCredentials(user=f"user{user}", secret=f"key{user}")
    )
    try:
        return await query.page(request)
    finally:
        reset_call_credentials(token)


@pytest.mark.parametrize("distinct_scopes", [False, True])
def test_twenty_cold_views_share_content_with_private_replay(
    monkeypatch, distinct_scopes
):
    async def run():
        scheduler, query, flow = service(32)
        monkeypatch.setattr(module, "MAX_PAGE_NODES", 3)
        entered, proceed, calls = pause_capture(monkeypatch, query)
        scopes = [f"/f/t{i}" if distinct_scopes else "/f" for i in range(20)]
        requests = [page_request(scope_path=scope) for scope in scopes]
        readers = [
            asyncio.create_task(read_as(query, i, request))
            for i, request in enumerate(requests)
        ]
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            await settled(lambda: query._reserved == 20)
            expected_workers = 2 if distinct_scopes else 1
            await settled(lambda: len(calls) == expected_workers)
            refused = await read_as(query, 20, requests[0])
            assert refused.code == "resource_exhausted"
            proceed.set()
            views = await asyncio.gather(*readers)
            assert all(page.kind == "page" for page in views)
            assert len({page.query_id for page in views}) == 20
            assert len({page.snapshot_id for page in views}) == 20
            assert len(calls) == len(set(scopes))
            contents = {s.content for s in query._snapshots.values()}
            assert len(contents) == len(set(scopes))
            assert query._retained_size() == (
                scheduler.query_changes.estimated_bytes
                + sum(c.size for c in contents)
                + 20 * module.SUMMARY_VIEW_BYTES
            )
            if distinct_scopes:
                assert [p.nodes[0].path for p in views] == scopes
            else:
                assert len({p.next_cursor for p in views}) == 20
                assert (
                    await read_as(
                        query,
                        1,
                        page_request(scope_path="/f", cursor=views[0].next_cursor),
                    )
                ).code == "invalid_request"
                # Walk with the last reader after a write; each new cursor is
                # private and the old pages remain a consistent complete view.
                flow.add_task("new")
                page = views[-1]
                paths = [n.path for n in page.nodes]
                while page.next_cursor:
                    request = page_request(scope_path="/f", cursor=page.next_cursor)
                    page = await read_as(query, 19, request)
                    assert page == await read_as(query, 19, request)
                    assert page.snapshot_id == views[-1].snapshot_id
                    paths.extend(n.path for n in page.nodes)
                assert set(paths) == {"/f", *(f"/f/t{i}" for i in range(32))}
                assert len(paths) == 33
            roots = await asyncio.gather(
                *(read_as(query, i, page_request(depth=0)) for i in range(20))
            )
            assert all([n.path for n in page.nodes] == ["/"] for page in roots)
            assert len(query._snapshots) == len(query._query_leases) == 40
            assert_released(query)
        finally:
            proceed.set()
            query.close()

    asyncio.run(run())


def test_redaction_change_separates_contents_and_private_cursors(monkeypatch):
    async def run():
        scheduler, query, _ = service()
        monkeypatch.setattr(module, "MAX_PAGE_NODES", 1)
        try:
            first = await read_as(query, 0, page_request())
            scheduler.query_redacted_parameters = ("SECRET",)
            other = await read_as(query, 0, page_request())
            assert other.query_id != first.query_id
            assert (
                query._snapshots[first.snapshot_id].content
                is not query._snapshots[other.snapshot_id].content
            )
            assert (
                await read_as(query, 0, page_request(cursor=first.next_cursor))
            ).code == "invalid_request"
        finally:
            query.close()

    asyncio.run(run())


def test_parallel_content_builds_charge_pages_before_publication(monkeypatch):
    async def run():
        scheduler, query, _ = service()
        sizes = []
        ready = asyncio.Event()
        reserve = query._reserve_summary_page
        normal_limit = module.MAX_RETAINED_BYTES

        async def meeting(build, size):
            sizes.append(size)
            if len(sizes) == 2:
                # Either content fits by itself with both private views,
                # while their combined pages exceed this retained budget.
                limit = (
                    scheduler.query_changes.estimated_bytes
                    + 2 * module.SUMMARY_VIEW_BYTES
                    + max(sizes)
                )
                monkeypatch.setattr(module, "MAX_RETAINED_BYTES", limit)
                ready.set()
            await asyncio.wait_for(ready.wait(), 3)
            await reserve(build, size)

        monkeypatch.setattr(query, "_reserve_summary_page", meeting)
        try:
            requests = [page_request(scope_path=f"/f/t{i}") for i in range(2)]
            views = await asyncio.gather(
                *(read_as(query, i, request) for i, request in enumerate(requests))
            )
            assert sorted(p.kind for p in views) == ["error", "page"]
            loser = next(i for i, p in enumerate(views) if p.kind == "error")
            assert views[loser].code == "resource_exhausted"
            assert query._retained_size() <= module.MAX_RETAINED_BYTES
            assert_released(query)
            monkeypatch.setattr(module, "MAX_RETAINED_BYTES", normal_limit)
            assert (await read_as(query, loser, requests[loser])).kind == "page"
        finally:
            query.close()

    asyncio.run(run())


def test_origin_owner_cancels_while_another_owner_reads_shared_pages(monkeypatch):
    async def run():
        _, query, _ = service()
        monkeypatch.setattr(module, "MAX_PAGE_NODES", 1)
        entered, proceed, calls = pause_capture(monkeypatch, query)
        first = asyncio.create_task(read_as(query, 0, page_request()))
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            other = asyncio.create_task(read_as(query, 1, page_request()))
            await settled(lambda: query._reserved == 2)
            first.cancel()
            await settled(lambda: sum(v.readers for v in query._captures.values()) == 1)
            proceed.set()
            with pytest.raises(asyncio.CancelledError):
                await first
            second = await other
            assert len(calls) == 1 and len(query._snapshots) == 1
            assert (
                query._snapshots[second.snapshot_id].content.original_snapshot_id
                not in query._snapshots
            )
            request = page_request(cursor=second.next_cursor)
            assert (await read_as(query, 1, request)).page_index == 1
            assert (await read_as(query, 0, request)).code == "invalid_request"
            assert_released(query)
        finally:
            proceed.set()
            query.close()

    asyncio.run(run())


def test_distinct_inflight_roots_are_bounded_and_shutdown_skips_queue(monkeypatch):
    async def run():
        _, query, flow = service()
        _, proceed, calls = pause_capture(monkeypatch, query)
        readers = []
        try:
            for i in range(module.MAX_SUMMARY_CAPTURE_ROOTS):
                flow.add_task(f"version{i}")
                readers.append(asyncio.create_task(read_as(query, i, page_request())))
                await settled(lambda: query._reserved == len(readers))
            await settled(lambda: len(calls) == module.MAX_CAPTURE_WORKERS)
            flow.add_task("extra")
            assert (
                await read_as(query, 20, page_request())
            ).code == "resource_exhausted"
            query.close()
            proceed.set()
            replies = await asyncio.gather(*readers)
            assert all(p.code == "snapshot_expired" for p in replies)
            assert len(calls) == module.MAX_CAPTURE_WORKERS
            assert_released(query)
        finally:
            proceed.set()
            query.close()

    asyncio.run(run())
