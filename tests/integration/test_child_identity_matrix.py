"""Both CLIs report one task attempt through both live server transports."""

import os
import shutil
import subprocess
from pathlib import Path

import grpc
import httpx
import pytest
from typer.testing import CliRunner

from takler.client import cli
from takler.core import Flow, NodeStatus
from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server
from takler.server.protocol import takler_pb2, takler_pb2_grpc


@pytest.fixture(scope="module")
def go_client_binary(tmp_path_factory):
    if shutil.which("go") is None:
        pytest.skip("Go toolchain is unavailable")
    repo = Path(__file__).resolve().parents[3] / "takler-client"
    binary = tmp_path_factory.mktemp("go-client") / "takler_client"
    subprocess.run(
        ["go", "build", "-o", str(binary), "main.go"], cwd=repo, check=True, timeout=90
    )
    return binary


@pytest.mark.parametrize("transport", ["grpc", "http"])
@pytest.mark.parametrize("language", ["python", "go"])
def test_child_identity_success_and_old_attempt_rejection(
    language,
    transport,
    server_runner_factory,
    free_port_fixture,
    go_client_binary,
    monkeypatch,
):
    grpc_port = free_port_fixture()
    http_port = free_port_fixture()
    config = ConnectConfig(
        server=Server(
            address=Address(hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)),
            http=HttpSettings(host="127.0.0.1", port=str(http_port)),
            transport=transport,
        )
    )
    server = server_runner_factory(port=grpc_port, connect_config=config).server
    flow = Flow("identity")
    container = flow.add_container("c")
    container.add_event("ready")
    container.add_meter("progress", 0, 100)
    task = container.add_task("t")
    abort_task = container.add_task("a")
    other = Flow("other")
    other.add_event("ready")
    server.bunch.add_flow(flow)
    server.bunch.add_flow(other)
    flow.begin()
    flow.suspend()
    task.increment_try_no()
    old_attempt = task.attempt_id
    task.increment_try_no()
    task.set_node_status(NodeStatus.submitted)
    abort_task.increment_try_no()
    abort_task.set_node_status(NodeStatus.submitted)

    env = os.environ.copy()
    for name in ("TAKLER_CONNECT_FILE", "TAKLER_PASS", "NO_TAKLER"):
        env.pop(name, None)
    env["TAKLER_TRANSPORT"] = transport
    env["TAKLER_NAME"] = task.node_path
    env["TAKLER_ATTEMPT_ID"] = task.attempt_id
    env["NO_PROXY"] = env["no_proxy"] = "localhost,127.0.0.1,::1"
    port = http_port if transport == "http" else grpc_port

    if transport == "http":
        with httpx.Client(trust_env=False) as raw_client:
            response = raw_client.post(
                f"http://127.0.0.1:{http_port}/v1/commands/complete",
                json={
                    "version": "1",
                    "command": "complete",
                    "trace_id": "0" * 32,
                    "payload": {"node_path": task.node_path},
                },
                timeout=5,
            )
        assert response.status_code == 200
        assert response.json()["payload"]["flag"] == 32
    else:
        with grpc.insecure_channel(
            f"127.0.0.1:{grpc_port}", options=[("grpc.enable_http_proxy", 0)]
        ) as channel:
            response = takler_pb2_grpc.TaklerServerStub(channel).RunCommandComplete(
                takler_pb2.CompleteCommand(
                    child_options=takler_pb2.ChildCommandOptions(
                        node_path=task.node_path
                    )
                ),
                timeout=5,
            )
            assert response.flag == 32
    assert task.state.node_status is NodeStatus.submitted

    def call(command, *options, attempt_id=None):
        args = [command, "--host", "127.0.0.1", "--port", str(port), *options]
        call_env = dict(env)
        if attempt_id is not None:
            call_env["TAKLER_ATTEMPT_ID"] = attempt_id
        if language == "python":
            result = CliRunner().invoke(cli.app, args, env=call_env)
            return result.exit_code, result.stderr
        result = subprocess.run(
            [str(go_client_binary), *args],
            env=call_env,
            text=True,
            capture_output=True,
            timeout=15,
        )
        return result.returncode, result.stderr

    variants = [
        ("init", ["--node-path", task.node_path, "--task-id", "job"]),
        ("complete", ["--node-path", task.node_path]),
        ("abort", ["--node-path", task.node_path, "--reason", "failed"]),
        ("event", ["--node-path", container.node_path, "--event-name", "ready"]),
        (
            "meter",
            [
                "--node-path",
                container.node_path,
                "--meter-name",
                "progress",
                "--meter-value",
                "50",
            ],
        ),
    ]
    for command, options in variants:
        code, stderr = call(command, *options, attempt_id=old_attempt)
        assert code == 1, (language, transport, command, stderr)
        assert "attempt_mismatch" in stderr
        assert task.state.node_status is NodeStatus.submitted
        assert container.find_event("ready").value is False
        assert container.find_meter("progress").value == 0

    for invalid in ("", "NOT-A-UUID"):
        code, stderr = call(
            "complete", "--node-path", task.node_path, attempt_id=invalid
        )
        assert code == 1 and "attempt_id" in stderr
    code, stderr = call(
        "event", "--node-path", other.node_path, "--event-name", "ready"
    )
    assert code == 1 and "invalid_request" in stderr
    assert other.find_event("ready").value is False

    for command, options in (variants[0], variants[3], variants[4], variants[1]):
        code, stderr = call(command, *options)
        assert code == 0, (language, transport, command, stderr)
        if command == "init":
            started_at = task.started_at
            code, stderr = call(command, *options)
            assert code == 0 and task.started_at == started_at
            code, stderr = call(
                "init", "--node-path", task.node_path, "--task-id", "another-job"
            )
            assert code == 3 and "zombie" in stderr
            assert task.task_id == "job"
    assert task.state.node_status is NodeStatus.complete
    assert container.find_event("ready").value is True
    assert container.find_meter("progress").value == 50
    code, stderr = call("complete", "--node-path", task.node_path)
    assert code == 1 and "attempt_mismatch" in stderr
    code, stderr = call(
        "abort",
        "--node-path",
        abort_task.node_path,
        "--attempt-id",
        abort_task.attempt_id,
        "--reason",
        "failed",
    )
    assert code == 0, (language, transport, stderr)
    assert abort_task.state.node_status is NodeStatus.aborted
