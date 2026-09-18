"""End-to-end: one server process serving gRPC and HTTP side by side.

The acceptance scenario of M3 task 7: a ``TaklerServer`` whose
``connect.yaml`` carries a ``server.http`` section mounts the HTTP transport
next to the gRPC one, each on its own port, both backed by the same bunch.
This module proves exactly that over real sockets -- nothing is faked:

* ``ping`` is answered on both ports;
* a ``load`` posted to the HTTP port becomes visible to a ``show`` issued
  over gRPC, i.e. both transports drive the same scheduler and bunch.

The scaffolding is the integration suite's ``ServerRunner`` (the server owns
its event loop in a background thread, the test body stays synchronous),
reached through the ``server_runner_factory`` fixture.
"""

from __future__ import annotations

# ruff: noqa: E402 -- the takler imports below intentionally follow the
# importorskip guards, so a checkout without the ``http`` extra skips this
# module instead of failing at collection.

import base64
import json

import pytest

pytest.importorskip(
    "fastapi", reason="the HTTP transport lives behind the takler[http] extra"
)
httpx = pytest.importorskip(
    "httpx", reason="the HTTP end-to-end probe posts through httpx"
)

from takler.core import Flow
from takler.server.connect_config import (
    Address,
    ConnectConfig,
    HttpSettings,
    Server,
)

LOCALHOST = "127.0.0.1"


def _connect_config(grpc_port: int, http_port: int) -> ConnectConfig:
    """A Connect_Config mounting HTTP on its own port next to gRPC."""
    return ConnectConfig(
        server=Server(
            address=Address(hostname=LOCALHOST, ip=LOCALHOST, port=str(grpc_port)),
            http=HttpSettings(host=LOCALHOST, port=str(http_port)),
        )
    )


def test_server_serves_grpc_and_http_in_one_process(
    server_runner_factory, free_port_fixture
) -> None:
    grpc_port = free_port_fixture()
    http_port = free_port_fixture()
    runner = server_runner_factory(
        port=grpc_port,
        connect_config=_connect_config(grpc_port, http_port),
    )
    assert runner.server.http_transport is not None

    # ping is answered on both ports.
    client = runner.make_client()
    client.start()
    client.run_request_ping()
    with httpx.Client(base_url=f"http://{LOCALHOST}:{http_port}") as http:
        ping = http.post("/v1/commands/ping", json={"command": "ping", "payload": {}})
        assert ping.status_code == 200
        assert ping.json()["command"] == "ping"

        # A command over HTTP mutates the bunch the gRPC side reads.
        flow = Flow("flow_http")
        flow.add_task("task1")
        flow_bytes = json.dumps(flow.to_dict()).encode("utf-8")
        load = http.post(
            "/v1/commands/load",
            json={
                "command": "load",
                "payload": {
                    "flow_type": "json",
                    # The JSON envelope carries raw bytes base64 encoded.
                    "flow_bytes": base64.b64encode(flow_bytes).decode("ascii"),
                },
            },
        )
        assert load.status_code == 200
        assert load.json()["payload"]["flag"] == 0

    show = client.run_request_show(
        show_trigger=False,
        show_parameter=False,
        show_limit=False,
        show_event=False,
        show_meter=False,
    )
    client.close_channel()
    assert "flow_http" in show.output
