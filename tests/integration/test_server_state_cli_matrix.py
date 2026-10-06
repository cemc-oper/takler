"""Both paired CLIs enforce service control over real gRPC and HTTP sockets."""

import getpass
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from takler.server.connect_config import (
    Address,
    ConnectConfig,
    HttpSettings,
    SecuritySettings,
    Server,
)


@pytest.fixture(scope="module")
def go_client_binary(tmp_path_factory):
    if shutil.which("go") is None:
        pytest.skip("Go toolchain is unavailable")
    repo = Path(__file__).resolve().parents[3] / "takler-client"
    if not (repo / "go.mod").exists():
        pytest.skip("paired Go checkout is unavailable")
    binary = tmp_path_factory.mktemp("go-state-client") / "takler_client"
    subprocess.run(
        ["go", "build", "-o", str(binary), "main.go"], cwd=repo, check=True, timeout=90
    )
    return binary


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_service_state_cli_matrix(
    language,
    transport,
    server_runner_factory,
    free_port_fixture,
    tmp_path,
    go_client_binary,
):
    grpc_port, http_port = free_port_fixture(), free_port_fixture()
    secret = tmp_path / "secret"
    secret.write_text("OPERATOR_PRIVATE\n")
    secret.chmod(0o600)
    whitelist = tmp_path / "users"
    whitelist.write_text(getpass.getuser() + "\n")
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
                audit_file=str(tmp_path / "audit.jsonl"),
            ),
        ),
    )
    executable = (
        [sys.executable, "-m", "takler.client"]
        if language == "python"
        else [str(go_client_binary)]
    )
    env = {
        key: value for key, value in os.environ.items() if not key.startswith("TAKLER_")
    }
    env.update(
        TAKLER_HOST="127.0.0.1",
        TAKLER_PORT=str(http_port if transport == "http" else grpc_port),
        TAKLER_TRANSPORT=transport,
        TAKLER_TIMEOUT="0",
        TAKLER_SECRET_FILE=str(secret),
        NO_PROXY="127.0.0.1,localhost,::1",
        no_proxy="127.0.0.1,localhost,::1",
    )

    def call(command, *, authorized=True):
        child_env = dict(env)
        if not authorized:
            child_env.pop("TAKLER_SECRET_FILE")
            child_env["TAKLER_PASS"] = "NOT_AN_OPERATOR_SECRET"
        result = subprocess.run(
            [*executable, command],
            cwd=Path(__file__).parents[2],
            env=child_env,
            text=True,
            capture_output=True,
            timeout=15,
        )
        assert "OPERATOR_PRIVATE" not in result.stdout + result.stderr
        return result

    def assert_state(command, status):
        result = call(command)
        assert result.returncode == 0, result.stdout + result.stderr
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        assert payload["flag"] == 0 and payload["status"] == status
        return payload

    assert_state("server-status", "running")
    denied = call("server-halt", authorized=False)
    assert denied.returncode == 1, denied.stdout + denied.stderr
    assert runner.server.bunch.server_state.status.value == "running"
    assert_state("server-halt", "halted")
    assert_state("server-halt", "halted")
    assert "manual" in assert_state("server-status", "halted")["halt_causes"]
    denied = call("server-resume", authorized=False)
    assert denied.returncode == 1, denied.stdout + denied.stderr
    assert_state("server-resume", "running")
    assert_state("server-resume", "running")
