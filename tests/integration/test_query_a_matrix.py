"""R2-12A paired initial-query matrix over real HTTP and gRPC sockets."""

from dataclasses import replace
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import pytest

from takler.protocol.query_v1 import (
    DetailRequest,
    FailureReply,
    PageRequest,
    encode_query_document,
)
from takler.server.connect_config import (
    Address,
    ConnectConfig,
    HttpSettings,
    SecuritySettings,
    Server,
)

FIRST_ATTEMPT = "11111111-1111-4111-8111-111111111111"
SECOND_ATTEMPT = "22222222-2222-4222-8222-222222222222"


@pytest.fixture(scope="module")
def go_query_binary(tmp_path_factory):
    paired_binary = os.environ.get("TAKLER_QUERY_GO_CLIENT")
    if paired_binary:
        binary = Path(paired_binary)
        assert binary.is_file(), "paired Go client binary is missing"
        return binary
    if shutil.which("go") is None:
        if os.environ.get("TAKLER_REQUIRE_PAIRED_QUERY"):
            pytest.fail("paired query matrix requires Go")
        pytest.skip("Go toolchain is unavailable")
    repo = Path(__file__).resolve().parents[3] / "takler-client"
    if not (repo / "go.mod").exists():
        if os.environ.get("TAKLER_REQUIRE_PAIRED_QUERY"):
            pytest.fail("paired Go checkout is missing")
        pytest.skip("paired Go checkout is unavailable")
    binary = tmp_path_factory.mktemp("go-query-a") / "takler_client"
    subprocess.run(
        ["go", "build", "-o", str(binary), "main.go"], cwd=repo, check=True, timeout=90
    )
    return binary


@pytest.fixture
def query_a_server(server_runner_factory, free_port_fixture):
    grpc_port, http_port = free_port_fixture(), free_port_fixture()
    runner = server_runner_factory(
        port=grpc_port,
        interval_main_loop=2.0,
        connect_config=ConnectConfig(
            server=Server(
                address=Address(
                    hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)
                ),
                http=HttpSettings(host="127.0.0.1", port=str(http_port)),
            ),
            security=SecuritySettings(query_redacted_parameters=["PRIVATE"]),
        ),
    )
    bunch = runner.server.bunch
    bunch.add_parameter("ROOT", "inherited")
    flow = bunch.add_flow("flow")
    flow.add_parameter("PRIVATE", "never-print-this-secret")
    for index in range(2300):
        flow.add_task(f"task{index:04d}")
    selected = flow.children[0]
    selected.try_no = 1
    selected.attempt_id = FIRST_ATTEMPT
    selected.task_id = "rid"
    selected.add_parameter("PAYLOAD", "x" * 300_000)
    selected.add_parameter("CLEARED", "before")
    return runner, grpc_port, http_port, flow, selected


def _environment(transport, port):
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("TAKLER_")
        and key.lower() not in {"http_proxy", "https_proxy", "all_proxy", "grpc_proxy"}
    }
    env.update(
        TAKLER_HOST="127.0.0.1",
        TAKLER_PORT=str(port),
        TAKLER_TRANSPORT=transport,
        TAKLER_TIMEOUT="0",
        NO_PROXY="127.0.0.1,localhost",
        no_proxy="127.0.0.1,localhost",
    )
    return env


def _executable(language, request):
    if language == "python":
        return [sys.executable, "-m", "takler.client"]
    return [str(request.getfixturevalue("go_query_binary"))]


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_full_snapshot_cli_streams_fixed_details_and_completion(
    language, transport, query_a_server, request
):
    _, grpc_port, http_port, flow, selected = query_a_server
    for index in range(2300, 4100):
        flow.add_task(f"task{index:04d}")
    port = http_port if transport == "http" else grpc_port
    result = subprocess.run(
        [
            *_executable(language, request),
            "snapshot",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--scope",
            "/flow",
            "--stream",
        ],
        env=_environment(transport, port),
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stderr
    assert "never-print-this-secret" not in result.stdout
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    assert len(lines) == 4102
    assert lines[0]["node"]["path"] == "/flow"
    assert lines[-1]["complete"]["total_nodes"] == 4101
    assert lines[1]["node"]["groups"]["parameters"]["user"][0]["value"] == "x" * 300_000


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_full_snapshot_cli_assembles_selected_root(
    language, transport, query_a_server, request
):
    _, grpc_port, http_port, _, selected = query_a_server
    port = http_port if transport == "http" else grpc_port
    result = subprocess.run(
        [
            *_executable(language, request),
            "snapshot",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--scope",
            "/",
            "--depth",
            "0",
            "--fields",
            "summary,service",
        ],
        env=_environment(transport, port),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value["completion"]["total_nodes"] == 1
    assert list(value["nodes_by_path"]) == ["/"]
    assert value["completion"]["service"]["status"] == "running"


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_paired_cli_complete_pages_detail_and_failures(
    language, transport, query_a_server, request
):
    _, grpc_port, http_port, _, _ = query_a_server
    port = http_port if transport == "http" else grpc_port
    command = _executable(language, request)
    env = _environment(transport, port)

    def call(*args):
        return subprocess.run(
            [*command, "show", *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

    summary = call("--scope", "/flow")
    assert summary.returncode == 0, summary.stderr
    assert summary.stdout.count("|- task") == 2300
    assert "|- flow [" in summary.stdout
    assert "PAYLOAD" not in summary.stdout
    assert "never-print-this-secret" not in summary.stdout + summary.stderr

    depth = call("--scope", "/flow", "--depth", "0")
    assert depth.returncode == 0, depth.stderr
    assert "|- flow [" in depth.stdout and "|- task" not in depth.stdout

    detail = call("--scope", "/flow/task0000", "--show-parameter")
    assert detail.returncode == 0, detail.stderr
    assert "param PAYLOAD '" + "x" * 300_000 + "'" in detail.stdout
    assert "never-print-this-secret" not in detail.stdout + detail.stderr
    assert "details are sampled live" in detail.stdout

    missing = call("--scope", "/missing")
    assert missing.returncode != 0
    assert "|- " not in missing.stdout


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_paired_cli_rejects_mid_page_and_detail_identity_faults(
    language, transport, query_a_server, request
):
    runner, grpc_port, http_port, _, _ = query_a_server
    port = http_port if transport == "http" else grpc_port
    executable = _executable(language, request)
    command = [
        *executable,
        "show",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
    ]
    service = runner.server.scheduler.query_service
    original_page, original_detail = service.page_encoded, service.detail

    async def expired_second_page(request):
        if request.cursor:
            return encode_query_document(
                FailureReply(
                    kind="error",
                    schema_version=1,
                    code="snapshot_expired",
                    message="injected continuation expiry",
                )
            )
        return await original_page(request)

    async def wrong_detail_identity(request):
        response = await original_detail(request)
        if response.kind == "detail":
            return response.model_copy(update={"query_id": "wrong-query"})
        return response

    try:
        service.page_encoded = expired_second_page
        page_failure = subprocess.run(
            command,
            env=_environment(transport, port),
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert page_failure.returncode != 0
        assert "|- " not in page_failure.stdout
        service.page_encoded = original_page
        service.detail = wrong_detail_identity
        detail_failure = subprocess.run(
            command + ["--scope", "/flow/task0000", "--show-parameter"],
            env=_environment(transport, port),
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert detail_failure.returncode != 0
        assert "|- " not in detail_failure.stdout
        assert "PAYLOAD" not in detail_failure.stdout
    finally:
        service.page_encoded, service.detail = original_page, original_detail


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_paired_cli_unreachable_has_no_partial_view(
    language, transport, free_port_fixture, request
):
    port = free_port_fixture()
    executable = _executable(language, request)
    env = _environment(transport, port)
    env["TAKLER_TIMEOUT"] = "1"
    result = subprocess.run(
        [*executable, "show", "--host", "127.0.0.1", "--port", str(port)],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode != 0
    assert "|- " not in result.stdout


@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_wire_snapshot_replay_live_detail_and_expiry(transport, query_a_server):
    runner, grpc_port, http_port, flow, selected = query_a_server
    client = runner.make_client(transport_name=transport)
    if transport == "http":
        client.port = http_port
    client.start()
    try:
        request = PageRequest(
            kind="page_request", schema_version=1, field_groups=["summary"]
        )
        first = client.transport.query(request)
        assert first.kind == "page" and first.next_cursor
        selected.user_parameters["CLEARED"].value = None
        flow.add_task("late")
        cursor = first.next_cursor
        paths = [node.path for node in first.nodes]
        pages = 1
        while cursor:
            continuation = request.model_copy(update={"cursor": cursor})
            page = client.transport.query(continuation)
            assert encode_query_document(page) == encode_query_document(
                client.transport.query(continuation)
            )
            assert (page.session_id, page.query_id, page.snapshot_id, page.as_of) == (
                first.session_id,
                first.query_id,
                first.snapshot_id,
                first.as_of,
            )
            assert page.page_index == pages
            paths.extend(node.path for node in page.nodes)
            cursor = page.next_cursor
            pages += 1
        assert pages > 1 and len(paths) == 2302
        assert len(set(paths)) == len(paths) and "/flow/late" not in paths

        detail_request = DetailRequest(
            kind="detail_request",
            schema_version=1,
            session_id=first.session_id,
            query_id=first.query_id,
            path="/flow/task0000",
            flow_generation=flow.definition_generation,
            attempt_id=FIRST_ATTEMPT,
            field_groups=["parameters"],
        )
        detail = client.transport.query(detail_request)
        assert detail.kind == "detail" and detail.sample_mode == "live"
        assert detail.as_of >= first.as_of
        assert detail.blob_refs
        assert [item.source_path for item in detail.groups.parameters.ancestors] == [
            "/flow",
            "/",
        ]
        snapshot = client.run_query_snapshot()
        resolved = client.run_query_detail(snapshot, "/flow/task0000", ["parameters"])
        assert resolved.blob_refs == []
        assert any(
            item.name == "CLEARED" and item.value is None
            for item in resolved.groups.parameters.user
        )
        assert resolved.groups.parameters.ancestors[0].user[0].redacted is True
        assert any(
            item.name == "PAYLOAD" and item.value == "x" * 300_000
            for item in resolved.groups.parameters.user
        )
        assert "never-print-this-secret" not in encode_query_document(detail).decode()

        stale = client.transport.query(
            detail_request.model_copy(update={"attempt_id": "old-attempt"})
        )
        assert stale.code == "stale_identity"
        flow.definition_generation = "replacement-generation"
        assert client.transport.query(detail_request).code == "stale_identity"
        flow.definition_generation = detail_request.flow_generation
        selected.attempt_id = SECOND_ATTEMPT
        assert client.transport.query(detail_request).code == "stale_identity"
        outside = client.transport.query(
            detail_request.model_copy(update={"path": "/other/task"})
        )
        assert outside.code == "stale_identity"

        service = runner.server.scheduler.query_service
        for identity, stored in list(service._snapshots.items()):
            service._snapshots[identity] = replace(stored, expires=time.monotonic() - 1)
        expired = client.transport.query(
            request.model_copy(update={"cursor": first.next_cursor})
        )
        assert expired.code == "snapshot_expired"
        assert client.transport.query(detail_request).code == "stale_identity"
        selected.attempt_id = FIRST_ATTEMPT
        assert client.transport.query(detail_request).kind == "detail"
        service._query_leases[first.query_id] = replace(
            service._query_leases[first.query_id], expires=time.monotonic() - 1
        )
        assert client.transport.query(detail_request).code == "snapshot_expired"
    finally:
        client.close_channel()
