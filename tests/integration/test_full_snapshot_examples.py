"""Run the documented Python/Go examples on a three-node, idle local tree."""

import asyncio
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server
from tests.integration.test_query_a_matrix import _environment


@pytest.fixture(scope="module")
def go_snapshot_example(tmp_path_factory):
    repo = Path(__file__).resolve().parents[3] / "takler-client"
    if shutil.which("go") is None or not (repo / "go.mod").is_file():
        pytest.skip("requires Go and the paired takler-client checkout")
    binary = tmp_path_factory.mktemp("snapshot-example") / "snapshot"
    subprocess.run(
        ["go", "build", "-p", "1", "-o", str(binary), "./examples/snapshot"],
        cwd=repo,
        check=True,
        timeout=90,
    )
    return binary


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["http", "grpc"])
def test_documented_full_snapshot_examples(
    language, transport, request, server_runner_factory, free_ports_fixture
):
    if language == "go":
        go_snapshot_example = request.getfixturevalue("go_snapshot_example")
    grpc_port, http_port = free_ports_fixture(2)
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

    async def install():
        scheduler = runner.server.scheduler
        async with scheduler.query_state_lock:
            scheduler.query_changes.begin()
            try:
                bunch = runner.server.bunch
                bunch.add_parameter("ROOT_VALUE", "继承")
                task = bunch.add_flow("flow").add_task("task")
                task.add_parameter("LABEL", "示例")
            finally:
                scheduler.query_changes.end()
            scheduler.version_store.flush()

    asyncio.run_coroutine_threadsafe(install(), runner._loop).result(timeout=10)
    port = http_port if transport == "http" else grpc_port
    if language == "python":
        example = (
            Path(__file__).resolve().parents[2] / "doc/examples/query/full_snapshot.py"
        )
        command = [
            sys.executable,
            str(example),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--transport",
            transport,
        ]
    else:
        command = [
            str(go_snapshot_example),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--transport",
            transport,
        ]
    for scope, stream in [("/flow", False), ("/", True), ("/missing", True)]:
        result = subprocess.run(
            command + ["--scope", scope] + (["--stream"] if stream else []),
            env=_environment(transport, port),
            capture_output=True,
            text=True,
            timeout=20,
        )
        if scope == "/missing":
            assert result.returncode != 0
            assert '"complete"' not in result.stdout
            continue
        assert result.returncode == 0, result.stderr
        if stream:
            rows = [json.loads(line) for line in result.stdout.splitlines()]
            assert "complete" in rows[-1]
            completion = rows[-1]["complete"]
            nodes = {row["node"]["path"]: row["node"] for row in rows[:-1]}
        else:
            view = json.loads(result.stdout)
            completion, nodes = view["completion"], view["nodes_by_path"]
        assert set(nodes) == (
            {"/", "/flow", "/flow/task"} if stream else {"/flow", "/flow/task"}
        )
        assert completion["scope_path"] == scope
        assert completion["total_nodes"] == len(nodes)
        assert len(completion["sha256"]) == 64
        assert completion["service"] is not None
        if not stream:
            assert completion["ancestor_context"][0]["source_path"] == "/"
        label = nodes["/flow/task"]["groups"]["parameters"]["user"]
        assert {item["name"]: item["value"] for item in label}["LABEL"] == "示例"
        assert not runner.server.scheduler.query_service.full_snapshots._leases
