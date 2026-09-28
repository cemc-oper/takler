"""Real operator-authorized replace across paired CLIs and both transports."""

import getpass
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from takler.core import NodeStatus
from takler.server.connect_config import (
    Address,
    ConnectConfig,
    HttpSettings,
    Server,
    SecuritySettings,
)

CLIENTS = ["python"] + (["go"] if os.environ.get("TAKLER_REPLACE_GO_CLIENT") else [])


@pytest.mark.parametrize("client", CLIENTS)
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_replace_cli_contract(
    client,
    transport,
    server_runner_factory,
    free_port_fixture,
    tmp_path,
    monkeypatch,
    capfd,
):
    grpc_port, http_port = free_port_fixture(), free_port_fixture()
    secret = tmp_path / "secret"
    secret.write_text("OPERATOR_PRIVATE\n")
    secret.chmod(0o600)
    whitelist = tmp_path / "users"
    whitelist.write_text(getpass.getuser() + "\n")
    audit = tmp_path / "audit.jsonl"
    runner = server_runner_factory(
        port=grpc_port,
        connect_config=ConnectConfig(
            server=Server(
                address=Address(
                    hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)
                ),
                http=HttpSettings(host="127.0.0.1", port=str(http_port)),
            ),
            security=SecuritySettings(
                auth_mode="enabled",
                operator_secret_file=str(secret),
                operator_whitelist_file=str(whitelist),
                audit_file=str(audit),
            ),
        ),
    )
    bunch = runner.server.bunch
    old = bunch.add_flow("f")
    old.add_task("old")
    old.suspend()
    executable = (
        [str(Path(sys.executable).with_name("takler-client-py"))]
        if client == "python"
        else [str(Path(os.environ["TAKLER_REPLACE_GO_CLIENT"]).resolve())]
    )
    env = {k: v for k, v in os.environ.items() if not k.startswith("TAKLER_")}
    env.update(
        TAKLER_HOST="127.0.0.1",
        TAKLER_PORT=str(http_port if transport == "http" else grpc_port),
        TAKLER_TRANSPORT=transport,
        TAKLER_TIMEOUT="60",
        TAKLER_SECRET_FILE=str(secret),
        NO_PROXY="127.0.0.1,localhost",
        no_proxy="127.0.0.1,localhost",
    )
    doc = dict(
        kind="takler.definition",
        schema_version=1,
        root=dict(
            type_id="takler.flow",
            name="f",
            children=[dict(type_id="takler.task", name="new")],
        ),
    )
    path = tmp_path / "flow.json"

    def run(data=doc, target="/f", expected=0, authorized=True, args=None):
        if isinstance(data, bytes):
            path.write_bytes(data)
        else:
            path.write_text(json.dumps(data))
        child_env = dict(env)
        if not authorized:
            child_env.pop("TAKLER_SECRET_FILE")
            child_env["TAKLER_PASS"] = "FAKE_CHILD_PASSWORD"
        result = subprocess.run(
            [
                *executable,
                "replace",
                *(args if args is not None else [target, str(path)]),
            ],
            env=child_env,
            text=True,
            capture_output=True,
            timeout=15,
        )
        output = result.stdout + result.stderr
        assert result.returncode == expected, output
        assert (
            "OPERATOR_PRIVATE" not in output
            and "PRIVATE_INJECTED" not in output
            and "PRIVATE_INVALID_JSON" not in output
        )
        return output

    run(expected=1, authorized=False)
    run(b"PRIVATE_INVALID_JSON", expected=1)
    run(b"\xff\xfe", expected=1)
    run(dict(doc, root=dict(type_id="takler.task", name="f")), expected=1)
    assert bunch.find_flow("f") is old
    for target in ["/missing", "/f/old", "/f/", "relative"]:
        run(target=target, expected=1)
    run(dict(doc, root=dict(type_id="takler.flow", name="other")), expected=1)
    run(
        dict(doc, root=dict(type_id="takler.bunch", name="b", flows=[doc["root"]])),
        expected=1,
    )
    run(dict(doc, schema_version=7), expected=1)
    run(dict(doc, root=dict(type_id="evil.flow", name="f")), expected=1)
    for state in [NodeStatus.active, NodeStatus.submitted]:
        old.children[0].state.node_status = state
        run(expected=1)
    old.children[0].state.node_status = NodeStatus.complete
    original = runner.server.scheduler.run_command_replace

    def fail(command):
        raise RuntimeError("PRIVATE_INJECTED")

    monkeypatch.setattr(runner.server.scheduler, "run_command_replace", fail)
    run(expected=3)
    assert "PRIVATE_INJECTED" not in capfd.readouterr().err
    monkeypatch.setattr(runner.server.scheduler, "run_command_replace", original)
    assert bunch.find_flow("f") is old
    output = run()
    assert "checkpoint pending" in output
    current = bunch.find_flow("f")
    assert current is not old and current.begun and current.state.suspended
    assert bunch.find_node("/f/new") is not None
    run(expected=1, args=["/f", str(tmp_path / "absent.json")])
    run(expected=1, args=["/f"])
    run(expected=1, args=["/f", str(path), "--force"])
    records = [json.loads(line) for line in audit.read_text().splitlines()]
    controls = [
        r for r in records if r["command"] == "replace" and r["event"] == "control"
    ]
    assert controls and {r["error_code"] for r in controls} >= {
        0,
        10,
        11,
        12,
        13,
        14,
        15,
        99,
    }
    for record in controls:
        assert record["user"] == getpass.getuser()
        assert record["target"] and record["reason"]
    denied = [
        r for r in records if r["event"] == "denied" and r["command"] == "replace"
    ]
    assert len(denied) == 1
    assert denied[0]["target"] == ["/f"] and denied[0]["reason"] == "missing_credential"
    assert (
        "OPERATOR_PRIVATE" not in audit.read_text()
        and "PRIVATE_INJECTED" not in audit.read_text()
    )
