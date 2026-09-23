import json
from types import SimpleNamespace

import grpc
import httpx
import pytest

from takler.client.grpc_transport import GrpcTransport
from takler.client.http_transport import HttpTransport
from takler.client.service_client import TaklerServiceClient
from takler.exceptions import ClientConnectionError, ServerResponseError
from takler.protocol.batch import validate_batch_response
from takler.protocol.commands import BATCH_COMMANDS, BatchResponse, Command
from takler.server.protocol.adapter import GRPC_METHOD_BY_COMMAND
from takler.server.protocol import takler_pb2


def response():
    return BatchResponse(
        flag=16,
        message="processed=3 succeeded=2 failed=1",
        results=[
            dict(
                index=i,
                target=t,
                flag=f,
                message="result",
                effect="none" if f else "applied",
            )
            for i, t, f in [(0, "/f/a", 0), (1, "/missing", 10), (2, "/f/b", 0)]
        ],
    )


def test_prints_all_results_before_summary(capsys):
    TaklerServiceClient._print_response(response())
    out = capsys.readouterr().out
    assert (
        out.index("[0]")
        < out.index("[1]")
        < out.index("[2]")
        < out.index("processed=3")
    )
    assert "node_not_found effect=none" in out


@pytest.mark.parametrize("change", ["index", "target", "count", "aggregate"])
def test_rejects_inconsistent_results(change):
    r = response()
    if change == "index":
        r.results[0].index = 4
    if change == "target":
        r.results[0].target = "/wrong"
    if change == "count":
        r.results.pop()
    if change == "aggregate":
        r.flag = 0
    with pytest.raises(ServerResponseError):
        validate_batch_response(
            Command.RUN, {"node_paths": ["/f/a", "/missing", "/f/b"]}, r
        )


class Unavailable(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "lost response"


@pytest.mark.parametrize("command", list(BATCH_COMMANDS))
@pytest.mark.parametrize("wire", ["grpc", "http"])
def test_mutating_batch_never_retries(command, wire):
    calls = []
    payload = {"node_paths": ["/f/a"], "force": False}
    if command in (Command.SUSPEND, Command.RESUME, Command.REQUEUE):
        payload.pop("force")
    if command == Command.FORCE:
        payload = {"paths": ["/f/a"], "state": "complete", "recursive": True}
    if command == Command.FREE_DEP:
        payload = {"paths": ["/f/a"], "dep_type": "all"}
    if command == Command.BEGIN:
        payload = {"flow_name": "f", "force": False}
    if wire == "grpc":
        transport = GrpcTransport("localhost", 33083, retry_window=600)

        def fail(*args, **kwargs):
            calls.append(1)
            raise Unavailable()

        transport.stub = SimpleNamespace(**{GRPC_METHOD_BY_COMMAND[command]: fail})
    else:
        transport = HttpTransport("localhost", 33083, retry_window=600)

        def fail(request):
            calls.append(1)
            return httpx.Response(503)

        transport.client = httpx.Client(
            transport=httpx.MockTransport(fail), base_url="http://localhost"
        )
    try:
        with pytest.raises(ClientConnectionError):
            transport.call(command, payload)
        assert calls == [1]
    finally:
        transport.close()


@pytest.mark.parametrize("wire", ["grpc", "http"])
def test_mixed_response_survives_transport(wire):
    r = response()
    payload = {"node_paths": [x.target for x in r.results], "force": False}
    if wire == "grpc":
        transport = GrpcTransport("localhost", 33083)
        transport.stub = SimpleNamespace(
            RunCommandRun=lambda *a, **kw: takler_pb2.BatchResponse(**r.model_dump())
        )
    else:
        transport = HttpTransport("localhost", 33083)

        def answer(request):
            envelope = json.loads(request.content)
            envelope["payload"] = r.model_dump()
            return httpx.Response(200, json=envelope)

        transport.client = httpx.Client(
            transport=httpx.MockTransport(answer), base_url="http://localhost"
        )
    try:
        assert transport.call(Command.RUN, payload) == r
    finally:
        transport.close()
