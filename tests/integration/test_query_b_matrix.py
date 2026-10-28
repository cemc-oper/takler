"""R2-12B paired delta differential and fault matrix."""

from dataclasses import replace
from io import StringIO
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import sys
import time

import pytest

from takler.client.query_client import print_query_show
from takler.core import Flow, NodeStatus, RepeatDate
from takler.exceptions import QueryResponseError
from takler.protocol.commands import (
    LoadCommand,
    ReplaceCommand,
    ServerHaltCommand,
    ServerResumeCommand,
)
from takler.protocol.query_v1 import FailureReply
from takler.serialization import export_definition
from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server
from takler.server.query_changes import MAX_CHANGE_SECONDS
from tests.integration.test_query_a_matrix import _environment


SEED = 2026100812


@pytest.fixture(scope="module")
def go_binary(tmp_path_factory):
    supplied = os.environ.get("TAKLER_QUERY_GO_CLIENT")
    if supplied:
        binary = Path(supplied)
        assert binary.is_file(), "paired Go binary is missing"
        return binary
    repo = Path(__file__).resolve().parents[3] / "takler-client"
    if shutil.which("go") is None or not (repo / "go.mod").exists():
        if os.environ.get("TAKLER_REQUIRE_PAIRED_QUERY"):
            pytest.fail("paired Go query checkout or toolchain is missing")
        pytest.skip("paired Go query checkout or toolchain is unavailable")
    binary = tmp_path_factory.mktemp("go-query-b") / "takler_client"
    subprocess.run(
        ["go", "build", "-o", str(binary), "main.go"], cwd=repo, check=True, timeout=90
    )
    return binary


@pytest.fixture
def query_b_server(server_runner_factory, free_port_fixture):
    grpc_port, http_port = free_port_fixture(), free_port_fixture()
    runner = server_runner_factory(
        port=grpc_port,
        interval_main_loop=1.0,
        connect_config=ConnectConfig(
            server=Server(
                address=Address(
                    hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)
                ),
                http=HttpSettings(host="127.0.0.1", port=str(http_port)),
            )
        ),
    )
    flow = runner.server.bunch.add_flow("flow")
    tasks = [flow.add_task(f"task{index:04d}") for index in range(32)]
    tasks[0].add_meter("progress", 0, 100)
    tasks[0].add_event("ready")
    tasks[0].add_parameter("CLEARED", "before")
    return runner, grpc_port, http_port, flow, tasks


def _sync(language, binary, transport, port, *, scope="/flow", polls=1):
    executable = (
        [sys.executable, "-m", "takler.client"]
        if language == "python"
        else [str(binary)]
    )
    interval = "0" if language == "python" else "0s"
    return subprocess.run(
        [
            *executable,
            "sync",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--scope",
            scope,
            "--polls",
            str(polls),
            "--interval",
            interval,
        ],
        env=_environment(transport, port),
        capture_output=True,
        text=True,
        timeout=45,
    )


def _summary_lines(output):
    return [line for line in output.splitlines() if "|- " in line]


def _sync_revision(output):
    assert output.startswith("# revision=")
    return int(output[len("# revision=") :].split(None, 1)[0])


def _fresh_lines(runner, transport, http_port, scope):
    client = runner.make_client(transport_name=transport)
    if transport == "http":
        client.port = http_port
    client.start()
    try:
        for attempt in range(3):
            try:
                snapshot = client.run_query_snapshot(scope_path=scope)
                break
            except QueryResponseError as exc:
                if (
                    exc.code != "resource_exhausted"
                    or exc.message
                    not in {
                        "query state changed during capture",
                        "query state is changing",
                    }
                    or attempt == 2
                ):
                    raise
                time.sleep(0.1 * (attempt + 1))
        output = StringIO()
        print_query_show(snapshot, lambda *_: None, output)
        return _summary_lines(output.getvalue()), snapshot
    finally:
        client.close_channel()


def _definition(flow):
    return json.dumps(export_definition(flow).model_dump(mode="json")).encode()


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_seeded_delta_sequence_matches_fresh_projection(
    language, transport, query_b_server, go_binary
):
    runner, grpc_port, http_port, flow, tasks = query_b_server
    port = http_port if transport == "http" else grpc_port
    actions = [
        "status",
        "meter",
        "event",
        "suspend",
        "parameter",
        "definition",
        "add",
        "delete",
        "repeat",
        "plugin",
        "clear",
        "status_again",
    ]
    random.Random(SEED).shuffle(actions)
    service = runner.server.scheduler.query_service
    original_since = service.since
    seen = []

    def mutate(action):
        if action == "status":
            tasks[0].set_node_status(NodeStatus.queued)
        elif action == "status_again":
            tasks[0].set_node_status(NodeStatus.complete)
        elif action == "meter":
            tasks[0].set_meter("progress", 42)
        elif action == "event":
            tasks[0].set_event("ready", True)
        elif action == "suspend":
            tasks[1].suspend()
        elif action == "parameter":
            flow.add_parameter("INHERITED", "value")
        elif action == "definition":
            tasks[1].add_event("added")
        elif action == "add":
            flow.add_task("late")
        elif action == "delete":
            flow.delete_child("task0031")
        elif action == "repeat":
            flow.add_repeat(RepeatDate("day", 20260101, 20260102))
        elif action == "plugin":
            tasks[2].notify_query_change(invalidate=True)
        elif action == "clear":
            tasks[0].user_parameters["CLEARED"].value = None
            tasks[0].notify_query_change("parameters", invalidate=True)

    async def changed_since(request):
        if request.cursor is None and len(seen) < len(actions):
            action = actions[len(seen)]
            mutate(action)
            seen.append(action)
        return await original_since(request)

    service.since = changed_since
    try:
        result = _sync(language, go_binary, transport, port, polls=len(actions))
    finally:
        service.since = original_since
    assert result.returncode == 0, f"seed={SEED} {actions}: {result.stderr}"
    assert seen == actions
    expected, snapshot = _fresh_lines(runner, transport, http_port, "/flow")
    assert _sync_revision(result.stdout) == snapshot.revision
    assert _summary_lines(result.stdout) == expected, f"seed={SEED} {actions}"
    assert snapshot.get("/flow/late") is not None
    assert snapshot.get("/flow/task0031") is None


@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_each_seeded_revision_matches_independent_full_snapshot(
    transport, query_b_server
):
    runner, _, http_port, flow, tasks = query_b_server
    client = runner.make_client(transport_name=transport)
    if transport == "http":
        client.port = http_port
    client.start()
    randomizer = random.Random(SEED + 1)
    actions = [
        "status",
        "meter",
        "event",
        "parameter",
        "definition",
        "add",
        "remove",
        "plugin",
    ] * 4
    randomizer.shuffle(actions)
    dynamic = []
    initial_removed = False
    try:
        view = client.run_query_snapshot(scope_path="/flow")
        for index, action in enumerate(actions):
            if action == "status":
                tasks[0].set_node_status_only(
                    randomizer.choice([NodeStatus.queued, NodeStatus.complete])
                )
            elif action == "meter":
                tasks[0].set_meter("progress", randomizer.randrange(101))
            elif action == "event":
                tasks[0].set_event("ready", bool(randomizer.randrange(2)))
            elif action == "parameter":
                flow.add_parameter(f"P{index}", "value")
            elif action == "definition":
                tasks[1].add_event(f"E{index}")
            elif action == "add":
                name = f"dynamic{index}"
                flow.add_task(name)
                dynamic.append(name)
            elif action == "remove":
                if dynamic:
                    flow.delete_child(dynamic.pop())
                elif not initial_removed:
                    flow.delete_child("task0031")
                    initial_removed = True
                else:
                    name = f"dynamic{index}"
                    flow.add_task(name)
                    dynamic.append(name)
            elif action == "plugin":
                tasks[2].notify_query_change(invalidate=True)
            try:
                updated = client.run_query_since(view)
            except QueryResponseError as exc:
                assert exc.code == "reset_required", (SEED + 1, index, action)
                view = client.run_query_snapshot(scope_path="/flow")
            else:
                view = (
                    client.run_query_snapshot(scope_path="/flow")
                    if updated.stale_scopes
                    else updated
                )
            _, oracle = _fresh_lines(runner, transport, http_port, "/flow")
            assert view.revision == oracle.revision, (SEED + 1, index, action)
            assert dict(view.nodes) == dict(oracle.nodes), (SEED + 1, index, action)
            assert view.ordered_paths == oracle.ordered_paths
    finally:
        client.close_channel()


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
@pytest.mark.parametrize("reason", ["session", "history"])
def test_paired_reset_rebuilds_after_session_or_window_loss(
    language, transport, reason, query_b_server, go_binary
):
    runner, grpc_port, http_port, _, tasks = query_b_server
    port = http_port if transport == "http" else grpc_port
    service = runner.server.scheduler.query_service
    tracker = runner.server.scheduler.query_changes
    original_since = service.since
    calls = 0

    async def reset_since(request):
        nonlocal calls
        if request.cursor is None:
            calls += 1
            if calls == 1:
                tasks[0].set_node_status_only(NodeStatus.queued)
                if reason == "session":
                    service.session_id = "restarted-query-session"
                else:
                    tracker._batches[-1] = replace(
                        tracker._batches[-1],
                        created_at=time.monotonic() - MAX_CHANGE_SECONDS - 1,
                    )
        return await original_since(request)

    service.since = reset_since
    try:
        result = _sync(language, go_binary, transport, port)
    finally:
        service.since = original_since
    assert result.returncode == 0, result.stderr
    assert calls == 1
    expected, snapshot = _fresh_lines(runner, transport, http_port, "/flow")
    assert _summary_lines(result.stdout) == expected
    assert snapshot.get("/flow/task0000").status == "queued"


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_paired_load_and_replace_rebuilds_exact_full_view(
    language, transport, query_b_server, go_binary
):
    runner, grpc_port, http_port, _, _ = query_b_server
    port = http_port if transport == "http" else grpc_port
    control = runner.make_client(transport_name=transport)
    if transport == "http":
        control.port = http_port
    control.start()
    try:
        # Keep the replacement queued until the final views have been compared.
        assert control.server_halt().flag == 0
    finally:
        control.close_channel()
    service = runner.server.scheduler.query_service
    original_since = service.since
    calls = 0

    async def changed_since(request):
        nonlocal calls
        if request.cursor is None:
            calls += 1
            if calls == 1:
                candidate = Flow("loaded")
                candidate.add_task("new")
                runner.server.scheduler.run_command_load(
                    LoadCommand(flow_bytes=_definition(candidate))
                )
            elif calls == 2:
                candidate = Flow("flow")
                candidate.add_task("replacement")
                reply = runner.server.scheduler.run_command_replace(
                    ReplaceCommand(
                        target_path="/flow", flow_bytes=_definition(candidate)
                    )
                )
                assert reply.flag == 0
        return await original_since(request)

    service.since = changed_since
    try:
        result = _sync(language, go_binary, transport, port, scope="/", polls=2)
    finally:
        service.since = original_since
    assert result.returncode == 0, result.stderr
    assert calls == 2
    expected, snapshot = _fresh_lines(runner, transport, http_port, "/")
    assert _sync_revision(result.stdout) == snapshot.revision
    assert _summary_lines(result.stdout) == expected
    assert snapshot.get("/loaded/new") is not None
    assert snapshot.get("/flow/replacement") is not None
    assert snapshot.get("/flow/task0000") is None


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
@pytest.mark.parametrize("fault", ["error", "reordered", "conflicting_replay"])
def test_paired_partial_delta_failure_prints_no_view(
    language, transport, fault, query_b_server, go_binary
):
    runner, grpc_port, http_port, flow, _ = query_b_server
    port = http_port if transport == "http" else grpc_port
    tasks = [flow.add_task(f"extra{index:04d}") for index in range(1500)]
    tracker = runner.server.scheduler.query_changes
    service = runner.server.scheduler.query_service
    original_since = service.since
    first_page = None
    continuation = 0

    async def broken_since(request):
        nonlocal first_page, continuation
        if request.cursor is None:
            tracker.begin()
            for task in tasks:
                task.set_node_status_only(NodeStatus.queued)
            tracker.end()
            first_page = await original_since(request)
            return first_page
        continuation += 1
        if fault == "error":
            return FailureReply(
                kind="error",
                schema_version=1,
                code="resource_exhausted",
                message="injected continuation fault",
            )
        if fault == "reordered":
            return first_page.model_copy(update={"page_index": 7})
        return first_page.model_copy(update={"batch_id": "wrong-batch"})

    service.since = broken_since
    try:
        result = _sync(language, go_binary, transport, port)
    finally:
        service.since = original_since
    assert first_page.kind == "since" and first_page.next_cursor
    assert continuation == 1
    assert result.returncode != 0
    assert _summary_lines(result.stdout) == []


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_paired_expired_delta_cursor_rebuilds_full_view(
    language, transport, query_b_server, go_binary
):
    runner, grpc_port, http_port, flow, _ = query_b_server
    port = http_port if transport == "http" else grpc_port
    tasks = [flow.add_task(f"extra{index:04d}") for index in range(1500)]
    tracker = runner.server.scheduler.query_changes
    service = runner.server.scheduler.query_service
    original_since = service.since
    first_page = None
    continuation = 0

    async def expired_since(request):
        nonlocal first_page, continuation
        if request.cursor is None:
            tracker.begin()
            for task in tasks:
                task.set_node_status_only(NodeStatus.queued)
            tracker.end()
            first_page = await original_since(request)
            return first_page
        continuation += 1
        batch = service._delta_batches[first_page.batch_id]
        service._delta_batches[first_page.batch_id] = replace(
            batch, expires=time.monotonic() - 1
        )
        return await original_since(request)

    service.since = expired_since
    try:
        result = _sync(language, go_binary, transport, port)
    finally:
        service.since = original_since
    assert first_page.kind == "since" and first_page.next_cursor
    assert continuation == 1
    assert result.returncode == 0, result.stderr
    expected, snapshot = _fresh_lines(runner, transport, http_port, "/flow")
    assert _summary_lines(result.stdout) == expected
    assert snapshot.get("/flow/extra0000").status == "queued"


@pytest.mark.parametrize("language", ["python", "go"])
@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_change_during_delta_pagination_is_seen_on_next_poll(
    language, transport, query_b_server, go_binary
):
    runner, grpc_port, http_port, flow, _ = query_b_server
    port = http_port if transport == "http" else grpc_port
    tasks = [flow.add_task(f"extra{index:04d}") for index in range(1500)]
    tracker = runner.server.scheduler.query_changes
    service = runner.server.scheduler.query_service
    original_since = service.since
    first_batch = None
    since_starts = 0
    frozen_statuses = []

    async def changing_since(request):
        nonlocal first_batch, since_starts
        if request.cursor is None:
            since_starts += 1
            if since_starts == 1:
                tracker.begin()
                for task in tasks:
                    task.set_node_status_only(NodeStatus.queued)
                tracker.end()
                first_batch = await original_since(request)
                tasks[0].set_node_status_only(NodeStatus.complete)
                reply = first_batch
            else:
                reply = await original_since(request)
        else:
            reply = await original_since(request)
            assert reply.batch_id == first_batch.batch_id
            assert reply.target_revision == first_batch.target_revision
        if reply.kind == "since" and reply.batch_id == first_batch.batch_id:
            frozen_statuses.extend(
                op.node.status
                for op in reply.operations
                if op.path == "/flow/extra0000" and op.op == "upsert_summary"
            )
        return reply

    service.since = changing_since
    try:
        result = _sync(language, go_binary, transport, port, polls=2)
    finally:
        service.since = original_since
    assert first_batch.kind == "since" and first_batch.next_cursor
    assert since_starts == 2
    assert frozen_statuses == ["queued"]
    assert result.returncode == 0, result.stderr
    expected, snapshot = _fresh_lines(runner, transport, http_port, "/flow")
    assert _summary_lines(result.stdout) == expected
    assert snapshot.get("/flow/extra0000").status == "complete"


@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_restart_discards_query_session_but_restores_checkpoint(
    transport, server_runner_factory, free_port_fixture, tmp_path
):
    checkpoint = tmp_path / "restart.check"
    grpc_port, http_port = free_port_fixture(), free_port_fixture()

    def config(grpc, http):
        return ConnectConfig(
            server=Server(
                address=Address(hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc)),
                http=HttpSettings(host="127.0.0.1", port=str(http)),
            )
        )

    runner = server_runner_factory(
        port=grpc_port,
        checkpoint_file=checkpoint,
        connect_config=config(grpc_port, http_port),
    )
    task = runner.server.bunch.add_flow("flow").add_task("task")
    task.set_node_status_only(NodeStatus.queued)
    client = runner.make_client(transport_name=transport)
    if transport == "http":
        client.port = http_port
    client.start()
    old = client.run_query_snapshot(scope_path="/flow")
    client.close_channel()
    assert runner.server.checkpoint_manager.write_checkpoint()
    runner.stop()
    assert checkpoint.is_file()

    next_grpc, next_http = free_port_fixture(), free_port_fixture()
    successor = server_runner_factory(
        port=next_grpc,
        checkpoint_file=checkpoint,
        connect_config=config(next_grpc, next_http),
    )
    client.port = next_http if transport == "http" else next_grpc
    client.start()
    try:
        with pytest.raises(QueryResponseError) as exc:
            client.run_query_since(old)
        assert exc.value.code == "reset_required"
        assert exc.value.reset_scope == "/flow"
        rebuilt = client.run_query_snapshot(scope_path="/flow")
        assert rebuilt.session_id != old.session_id
        assert rebuilt.get("/flow/task").status == "queued"
        assert successor.server.bunch.find_node("/flow/task") is not None
    finally:
        client.close_channel()


@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_live_detail_invalidation_and_service_outcomes(transport, query_b_server):
    runner, _, http_port, flow, tasks = query_b_server
    client = runner.make_client(transport_name=transport)
    if transport == "http":
        client.port = http_port
    client.start()
    try:
        initial = client.run_query_snapshot()
        detail = client.run_query_detail(initial, "/flow/task0000", ["parameters"])
        assert (
            next(
                item.value
                for item in detail.groups.parameters.user
                if item.name == "CLEARED"
            )
            == "before"
        )

        flow.add_parameter("INHERITED", "later")
        invalidated = client.run_query_since(initial)
        assert invalidated.stale_scopes == ("/flow",)
        with pytest.raises(QueryResponseError) as exc:
            client.run_query_detail(invalidated, "/flow/task0000", ["parameters"])
        assert exc.value.code == "stale_identity"
        rebuilt = client.run_query_snapshot()
        detail = client.run_query_detail(rebuilt, "/flow/task0000", ["parameters"])
        assert any(
            item.name == "INHERITED" and item.value == "later"
            for source in detail.groups.parameters.ancestors
            for item in source.user
        )

        tasks[0].user_parameters["CLEARED"].value = None
        tasks[0].notify_query_change("parameters", invalidate=True)
        assert client.run_query_since(rebuilt).stale_scopes == ("/flow/task0000",)
        rebuilt = client.run_query_snapshot()
        detail = client.run_query_detail(rebuilt, "/flow/task0000", ["parameters"])
        assert any(
            item.name == "CLEARED" and item.value is None
            for item in detail.groups.parameters.user
        )

        scheduler = runner.server.scheduler
        scheduler.run_command_server_halt(ServerHaltCommand())
        halted = client.run_query_since(rebuilt)
        assert halted.revision > rebuilt.revision and not halted.changed_paths
        root = client.run_query_detail(halted, "/", ["service"])
        assert root.groups.service.status == "halted"
        scheduler.run_command_server_resume(ServerResumeCommand())
        resumed = client.run_query_since(halted)
        root = client.run_query_detail(resumed, "/", ["service"])
        assert root.groups.service.status == "running"

        runner.server.checkpoint_manager._record_outcome(
            1, False, False, "", "storage full"
        )
        failed_save = client.run_query_since(resumed)
        assert failed_save.revision > resumed.revision
        root = client.run_query_detail(failed_save, "/", ["service"])
        assert root.groups.service.status == "halted"
        assert root.groups.service.status_reason == "checkpoint failure"
        assert "storage full" in root.groups.service.message
    finally:
        client.close_channel()
