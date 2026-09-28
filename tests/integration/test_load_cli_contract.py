"""Real CLI load contract; set TAKLER_LOAD_GO_CLIENT to add the paired Go CLI."""

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from takler.core import Flow
from takler.protocol.error_code import error_name_for_code
from takler.serialization import export_definition
from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server

CLIENTS = ["python"] + (["go"] if os.environ.get("TAKLER_LOAD_GO_CLIENT") else [])


@pytest.mark.parametrize("client", CLIENTS)
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_load_cli_contract(
    client, transport, server_runner_factory, free_port_fixture, tmp_path
):
    grpc_port, http_port = free_port_fixture(), free_port_fixture()
    runner = server_runner_factory(
        port=grpc_port,
        connect_config=ConnectConfig(
            server=Server(
                address=Address(
                    hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)
                ),
                http=HttpSettings(host="127.0.0.1", port=str(http_port)),
            )
        ),
    )
    executable = (
        [str(Path(sys.executable).with_name("takler-client-py"))]
        if client == "python"
        else [str(Path(os.environ["TAKLER_LOAD_GO_CLIENT"]).resolve())]
    )
    env = dict(
        os.environ,
        TAKLER_HOST="127.0.0.1",
        TAKLER_PORT=str(http_port if transport == "http" else grpc_port),
        TAKLER_TRANSPORT=transport,
        TAKLER_TIMEOUT="0",
        NO_PROXY="127.0.0.1,localhost",
        no_proxy="127.0.0.1,localhost",
    )

    def run(data, flag):
        path = tmp_path / "flow.json"
        path.write_bytes(data)
        result = subprocess.run(
            [*executable, "load", str(path)],
            env=env,
            text=True,
            capture_output=True,
            timeout=20,
        )
        output = result.stdout + result.stderr
        assert result.returncode == (0 if flag == 0 else 1), output
        assert error_name_for_code(flag) in output, output

    # Independent minimal fixture, including Unicode, exercises raw file bytes.
    data = dict(
        kind="takler.definition",
        schema_version=1,
        root=dict(
            type_id="takler.flow",
            name="forecast",
            user_parameters=[dict(name="LABEL", value="预报")],
            children=[dict(type_id="takler.task", name="task")],
        ),
    )
    run(json.dumps(data, ensure_ascii=False).encode(), 0)
    flow = runner.server.bunch.find_flow("forecast")
    assert flow is not None and not flow.begun
    assert flow.calendar.initial_time is None
    assert flow.find_node("/forecast/task") is not None
    before = runner.server.bunch.to_dict()
    run(json.dumps(data).encode(), 14)
    assert runner.server.bunch.find_flow("forecast") is flow
    assert runner.server.bunch.to_dict() == before
    for invalid in [
        b"not json",
        b'{"name":"legacy"}',
        json.dumps(dict(data, schema_version=9)).encode(),
        json.dumps(dict(data, root=dict(type_id="unknown.flow", name="bad"))).encode(),
        json.dumps(
            dict(data, root=dict(type_id="takler.flow", name="bad", begun=True))
        ).encode(),
    ]:
        run(invalid, 15)
        assert runner.server.bunch.to_dict() == before
    # The public Python exporter feeds either CLI unchanged.
    run(export_definition(Flow("exported")).model_dump_json().encode(), 0)
    assert not runner.server.bunch.find_flow("exported").begun
