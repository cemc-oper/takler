"""R3-04: live detail authorization outlives bounded page snapshots."""

import asyncio


from takler.client.query_client import QueryClient
from takler.core import Bunch, Flow
from takler.protocol.query_v1 import (
    CapabilitiesRequest,
    DetailRequest,
    PageRequest,
    SinceRequest,
)
from takler.server.auth import (
    CallCredentials,
    reset_call_credentials,
    set_call_credentials,
)
from takler.server.query_service import (
    QUERY_LEASE_BYTES,
    QUERY_LEASE_SECONDS,
    SNAPSHOT_SECONDS,
    SUMMARY_VIEW_BYTES,
)
from takler.server.scheduler import Scheduler


def _page(scope):
    return PageRequest(
        kind="page_request",
        schema_version=1,
        scope_path=scope,
        field_groups=["summary"],
    )


def _since(page, scope):
    return SinceRequest(
        kind="since_request",
        schema_version=1,
        session_id=page.session_id,
        query_id=page.query_id,
        scope_path=scope,
        field_groups=["summary"],
        since_revision=page.base_revision,
    )


def _detail(page, flow):
    return DetailRequest(
        kind="detail_request",
        schema_version=1,
        session_id=page.session_id,
        query_id=page.query_id,
        path=f"/{flow.name}/t",
        flow_generation=flow.definition_generation,
        attempt_id=None,
        field_groups=["runtime"],
    )


def test_unchanged_poll_renews_scope_across_two_page_lifetimes(monkeypatch):
    async def exercise():
        clock = [1000.0]
        monkeypatch.setattr("takler.server.query_service._monotonic", lambda: clock[0])
        bunch = Bunch()
        flow = bunch.add_flow(Flow("f"))
        flow.add_task("t")
        service = Scheduler(bunch).query_service
        page = await service.page(_page("/f"))
        request = _since(page, "/f")
        for _ in range(3):
            clock[0] += SNAPSHOT_SECONDS - 1
            reply = await service.since(request)
            assert reply.kind == "since" and reply.unchanged
        assert clock[0] > 1000 + 2 * SNAPSHOT_SECONDS
        assert page.snapshot_id not in service._snapshots
        assert (await service.detail(_detail(page, flow))).kind == "detail"
        clock[0] += QUERY_LEASE_SECONDS + 1
        assert (await service.detail(_detail(page, flow))).code == "snapshot_expired"
        assert (await service.since(request)).reason == "history_expired"
        rebuilt = await service.page(_page("/f"))
        assert (await service.detail(_detail(rebuilt, flow))).kind == "detail"

    asyncio.run(exercise())


def test_python_client_keeps_detail_after_two_page_lifetimes(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr("takler.server.query_service._monotonic", lambda: clock[0])
    bunch = Bunch()
    bunch.add_flow(Flow("f")).add_task("t")
    service = Scheduler(bunch).query_service

    class LocalTransport:
        def query(self, request):
            if isinstance(request, SinceRequest):
                return asyncio.run(service.since(request))
            if isinstance(request, PageRequest):
                return asyncio.run(service.page(request))
            if isinstance(request, DetailRequest):
                return asyncio.run(service.detail(request))
            if isinstance(request, CapabilitiesRequest):
                return service.capabilities()
            raise AssertionError(type(request))

    client = QueryClient(LocalTransport())
    view = client.read_snapshot(scope_path="/f")
    for _ in range(3):
        clock[0] += SNAPSHOT_SECONDS - 1
        assert client.read_since(view) is view
    assert view.snapshot_id not in service._snapshots
    assert client.read_detail(view, "/f/t", ["runtime"]).kind == "detail"


def test_other_scopes_preserve_pages_and_scope_authorization():
    async def exercise():
        bunch = Bunch()
        flows = [bunch.add_flow(Flow(f"f{i}")) for i in range(3)]
        for flow in flows:
            flow.add_task("t")
        service = Scheduler(bunch).query_service
        first = await service.page(_page("/f0"))
        same_scope_client = await service.page(_page("/f0"))
        assert same_scope_client.query_id == first.query_id
        for index in (1, 2):
            assert (await service.page(_page(f"/f{index}"))).kind == "page"
        assert first.snapshot_id in service._snapshots
        assert (await service.since(_since(first, "/f0"))).kind == "since"
        assert (await service.detail(_detail(first, flows[0]))).kind == "detail"
        assert (
            await service.detail(
                _detail(first, flows[1]).model_copy(update={"query_id": first.query_id})
            )
        ).code == "permission_denied"
        assert len(service._query_leases) == 3

        token = set_call_credentials(CallCredentials(user="other", secret="changed"))
        try:
            assert (
                await service.detail(_detail(first, flows[0]))
            ).code == "snapshot_expired"
            assert (await service.since(_since(first, "/f0"))).reason == "scope_changed"
        finally:
            reset_call_credentials(token)
        restarted = Scheduler(bunch).query_service
        assert (
            await restarted.detail(_detail(first, flows[0]))
        ).code == "snapshot_expired"
        assert (await restarted.since(_since(first, "/f0"))).reason == "session_changed"

    asyncio.run(exercise())


def test_many_scopes_under_one_owner_keep_their_earlier_authorization():
    async def exercise():
        bunch = Bunch()
        for index in range(12):
            bunch.add_flow(Flow(f"f{index}")).add_task("t")
        service = Scheduler(bunch).query_service
        try:
            pages = [
                await service.page(_page(flow.node_path))
                for flow in bunch.flows.values()
            ]
            assert all(page.kind == "page" for page in pages)
            for page, flow in zip(pages, bunch.flows.values()):
                assert (await service.since(_since(page, flow.node_path))).unchanged
                assert (await service.detail(_detail(page, flow))).kind == "detail"
        finally:
            service.close()

    asyncio.run(exercise())


def test_new_lease_obeys_byte_budget_and_releases_it_at_expiry(monkeypatch):
    async def exercise():
        clock = [1000.0]
        monkeypatch.setattr("takler.server.query_service._monotonic", lambda: clock[0])
        bunch = Bunch()
        flow = bunch.add_flow(Flow("f"))
        flow.add_task("t")
        service = Scheduler(bunch).query_service
        try:
            first = await service.page(_page("/f"))
            retained = service._retained_size()
            # A second reader can reuse the encoded pages. Leave room for
            # its page handle, but one byte too little for its scope lease.
            lease_size = QUERY_LEASE_BYTES + len(b"/f")
            assert service._query_lease_bytes == lease_size
            monkeypatch.setattr(
                "takler.server.query_service.MAX_RETAINED_BYTES",
                retained + SUMMARY_VIEW_BYTES + lease_size - 1,
            )
            token = set_call_credentials(CallCredentials(user="other", secret="key"))
            try:
                denied = await service.page(_page("/f"))
                assert denied.code == "resource_exhausted"
                assert service._retained_size() == retained
            finally:
                reset_call_credentials(token)
            assert (await service.detail(_detail(first, flow))).kind == "detail"
            clock[0] += max(QUERY_LEASE_SECONDS, SNAPSHOT_SECONDS) + 1
            service._purge()
            assert service._query_lease_bytes == 0
            assert (await service.page(_page("/f"))).kind == "page"
        finally:
            service.close()

    asyncio.run(exercise())


def test_idle_monitors_rebuild_while_active_monitors_keep_their_waterline(monkeypatch):
    async def exercise():
        clock = [1000.0]
        monkeypatch.setattr("takler.server.query_service._monotonic", lambda: clock[0])
        bunch = Bunch()
        flow = bunch.add_flow(Flow("f"))
        flow.add_task("t")
        service = Scheduler(bunch).query_service
        owners = [CallCredentials(user="monitor", secret=f"key-{i}") for i in range(20)]

        async def read_as(owner, operation, request):
            token = set_call_credentials(owner)
            try:
                return await operation(request)
            finally:
                reset_call_credentials(token)

        try:
            pages = await asyncio.gather(
                *(read_as(owner, service.page, _page("/f")) for owner in owners)
            )
            assert all(page.kind == "page" for page in pages)
            assert len({page.snapshot_id for page in pages}) == len(owners)
            clock[0] += QUERY_LEASE_SECONDS - 5
            for owner, page in zip(owners[::2], pages[::2]):
                assert (
                    await read_as(owner, service.since, _since(page, "/f"))
                ).unchanged

            # All original page bytes expire; only renewed scopes survive.
            clock[0] += 10
            for index, (owner, page) in enumerate(zip(owners, pages)):
                delta = await read_as(owner, service.since, _since(page, "/f"))
                if index % 2 == 0:
                    assert delta.kind == "since" and delta.unchanged
                else:
                    assert delta.kind == "reset" and delta.reason == "history_expired"
            assert not service._snapshots
            assert len(service._query_leases) == 10

            for owner, old in zip(owners[1::2], pages[1::2]):
                new = await read_as(owner, service.page, _page("/f"))
                assert new.kind == "page" and new.snapshot_id != old.snapshot_id
                assert (
                    await read_as(owner, service.detail, _detail(new, flow))
                ).kind == "detail"
            assert len(service._query_leases) == len(owners)
        finally:
            service.close()

    asyncio.run(exercise())
