"""Exercise the real HTTP listener's start, run and stop lifecycle."""

# ruff: noqa: E402 -- HTTP imports follow the optional dependency guards.

import asyncio

import pytest

pytest.importorskip(
    "fastapi", reason="the HTTP transport lives behind the takler[http] extra"
)
httpx = pytest.importorskip("httpx", reason="the HTTP test driver posts through httpx")

from takler.core import Bunch
from takler.server.http_transport import API_PREFIX, HttpTransport
from takler.server.scheduler import Scheduler


def _serve_once(transport: HttpTransport) -> None:
    """start -> ping over a real socket -> stop, awaiting the run task."""

    async def main():
        await transport.start()
        run_task = asyncio.create_task(transport.run())
        port = transport._server.servers[0].sockets[0].getsockname()[1]
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
                for _ in range(10):
                    try:
                        response = await client.post(
                            f"{API_PREFIX}/commands/ping",
                            json={
                                "version": "1",
                                "trace_id": "0" * 32,
                                "command": "ping",
                                "payload": {},
                            },
                        )
                        break
                    except httpx.ConnectError:
                        await asyncio.sleep(0.05)
                else:
                    raise AssertionError(
                        "the HTTP listener never accepted a connection"
                    )
            assert response.status_code == 200
            assert response.json()["command"] == "ping"
        finally:
            await transport.stop()
            await asyncio.wait_for(run_task, timeout=10)

    asyncio.run(main())


@pytest.mark.integration
def test_start_run_stop_serves_real_http() -> None:
    """The full uvicorn lifecycle: ``start`` binds, ``run`` serves until
    ``stop``, and a real socket round trip answers ping. Port 0 lets the OS
    pick the port."""
    transport = HttpTransport(
        scheduler=Scheduler(bunch=Bunch(name="bunch")), host="127.0.0.1", port=0
    )

    _serve_once(transport)


@pytest.mark.integration
def test_start_with_a_taken_port_raises_instead_of_exiting() -> None:
    """uvicorn's own bind path answers a taken port with ``sys.exit``; this
    transport binds the socket itself, so the failure is an ordinary
    ``OSError`` out of ``start()`` and the process -- and its unified
    shutdown path -- survives."""
    first = HttpTransport(
        scheduler=Scheduler(bunch=Bunch(name="bunch")), host="127.0.0.1", port=0
    )

    async def main():
        await first.start()
        port = first._server.servers[0].sockets[0].getsockname()[1]
        second = HttpTransport(
            scheduler=Scheduler(bunch=Bunch(name="bunch")),
            host="127.0.0.1",
            port=port,
        )
        with pytest.raises(OSError):
            await second.start()
        await first.stop()

    asyncio.run(main())
