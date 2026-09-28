"""Shared wire cases cross the actual ASGI request boundary before dispatch."""

import asyncio
import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from takler.protocol.commands import Command, RESPONSE_TYPE_BY_COMMAND, BATCH_COMMANDS
from takler.server.http_transport import create_app
from tests.protocol.test_wire import VECTORS


async def send(raw, command, content_type="application/json"):
    handler = SimpleNamespace(
        dispatch=AsyncMock(
            return_value=RESPONSE_TYPE_BY_COMMAND[Command(command)](
                **(
                    {"flag": 0, "message": "", "results": []}
                    if Command(command) in BATCH_COMMANDS
                    else {}
                )
            )
        )
    )
    app = create_app(handler)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            f"/v1/commands/{command}",
            content=raw,
            headers={"content-type": content_type},
        )
    return response, handler.dispatch.await_count


@pytest.mark.parametrize(
    "case", [v for v in VECTORS if v["direction"] == "request"], ids=lambda c: c["id"]
)
def test_request_vectors_at_http_boundary(case):
    response, count = asyncio.run(
        send(case["raw_body"].encode("utf-8"), case["command"], case["content_type"])
    )
    assert response.status_code == (
        200 if case["expected_wire_valid"] else case["expected_rejection_status"]
    )
    assert count == int(case["expected_wire_valid"])


@pytest.mark.parametrize(
    "case",
    [v for v in VECTORS if v["direction"] == "request" and v["expected_wire_valid"]],
    ids=lambda c: c["command"],
)
def test_every_required_payload_field_is_checked_before_dispatch(case):
    for field in case["request"]["payload"]:
        for bad in [None, {}, 1.5]:
            value = copy.deepcopy(case["request"])
            value["payload"][field] = bad
            response, count = asyncio.run(send(json.dumps(value), case["command"]))
            assert response.status_code == 422 and count == 0
        value = copy.deepcopy(case["request"])
        del value["payload"][field]
        response, count = asyncio.run(send(json.dumps(value), case["command"]))
        assert response.status_code == 422 and count == 0


@pytest.mark.parametrize("command", [Command.PING, Command.SHOW, Command.COROUTINE])
def test_query_execution_error_is_http_500(command, monkeypatch):
    from tests.server.conftest import build_scheduler
    from takler.server.handlers import CommandHandlers
    from takler.protocol.commands import REQUEST_TYPE_BY_COMMAND
    from takler.protocol.envelope import Envelope

    handlers = CommandHandlers(build_scheduler())

    def fail(*args):
        raise RuntimeError("PRIVATE_QUERY_FAILURE")

    monkeypatch.setattr(handlers, "_run", fail)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(handlers)),
            base_url="http://test",
        ) as client:
            return await client.post(
                f"/v1/commands/{command.value}",
                json=Envelope.for_request(
                    command, REQUEST_TYPE_BY_COMMAND[command]()
                ).model_dump(mode="json"),
            )

    response = asyncio.run(run())
    assert response.status_code == 500
    assert response.json() == {"detail": "query execution failed"}


@pytest.mark.parametrize("raw", [b"\xff", b"\xef\xbb\xbf{}", b'"\xff"'])
def test_invalid_utf8_and_bom_never_dispatch(raw):
    response, count = asyncio.run(send(raw, "ping"))
    assert response.status_code == 422 and count == 0
