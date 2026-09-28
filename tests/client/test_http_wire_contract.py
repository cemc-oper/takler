"""The response vectors exercise the real Python HTTP client decoding path."""

import json

import httpx
import pytest

from takler.client.http_transport import HttpTransport
from takler.exceptions import ServerResponseError
from takler.protocol.commands import Command
from tests.protocol.test_wire import VECTORS


@pytest.mark.parametrize(
    "case", [v for v in VECTORS if v["direction"] == "response"], ids=lambda c: c["id"]
)
def test_response_vectors_at_client_boundary(case):
    calls = []

    def answer(request):
        calls.append(request)
        trace = json.loads(request.content)["trace_id"]
        raw = case["raw_body"].replace(case["request"]["trace_id"], trace)
        return httpx.Response(
            case["http_status"],
            content=raw.encode("utf-8"),
            headers={"content-type": case["content_type"]},
        )

    transport = HttpTransport("localhost", 33083, retry_window=600)
    transport.client = httpx.Client(
        base_url="http://test", transport=httpx.MockTransport(answer)
    )
    try:
        if case["expected_wire_valid"]:
            transport.call(
                Command(case["request"]["command"]), case["request"]["payload"]
            )
        else:
            with pytest.raises(ServerResponseError):
                transport.call(
                    Command(case["request"]["command"]), case["request"]["payload"]
                )
        assert len(calls) == 1
    finally:
        transport.close()


def test_tls_verification_is_not_retried(fake_clock):
    calls = []

    def fail(request):
        calls.append(request)
        raise httpx.ConnectError("CERTIFICATE_VERIFY_FAILED")

    from takler.exceptions import InvalidRequestError

    transport = HttpTransport(
        "localhost", 33083, retry_window=600, clock=fake_clock, sleep=fake_clock.sleep
    )
    transport.client = httpx.Client(
        base_url="http://test", transport=httpx.MockTransport(fail)
    )
    try:
        with pytest.raises(InvalidRequestError, match="TLS"):
            transport.call(Command.PING, {})
        assert len(calls) == 1 and fake_clock.slept == []
    finally:
        transport.close()


@pytest.mark.parametrize("raw", [b"\xff", b"\xef\xbb\xbf{}", b'"\xff"'])
def test_invalid_utf8_and_bom_are_protocol_failures(raw):
    calls = []

    def answer(request):
        calls.append(request)
        return httpx.Response(
            200, content=raw, headers={"content-type": "application/json"}
        )

    transport = HttpTransport("localhost", 33083, retry_window=600)
    transport.client = httpx.Client(
        base_url="http://test", transport=httpx.MockTransport(answer)
    )
    try:
        with pytest.raises(ServerResponseError):
            transport.call(Command.PING, {})
        assert len(calls) == 1
    finally:
        transport.close()
