"""Real SIGKILL windows: only the selected snapshot survives a lost process."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time
from uuid import uuid4

import pytest

from takler.client.service_client import TaklerServiceClient
from takler.query import parse_show


pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(sys.platform != "linux", reason="requires SIGKILL"),
]
HELPER = Path(__file__).parent / "helpers" / "crash_server.py"


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for process barrier")


class CrashServer:
    def __init__(self, work):
        self.work = work
        self.work.mkdir()
        self.grpc_port = _free_port()
        self.http_port = _free_port()
        self.process = None
        self.sequence = 0

    def start(self, fresh):
        for directory in ("commands", "acks"):
            path = self.work / directory
            if path.exists():
                shutil.rmtree(path)
            path.mkdir()
        (self.work / "ready").unlink(missing_ok=True)
        self.sequence = 0
        env = {k: v for k, v in os.environ.items() if not k.startswith("TAKLER_")}
        env["NO_PROXY"] = env["no_proxy"] = "127.0.0.1,localhost,::1"
        log = (self.work / "server.log").open("a", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                [
                    sys.executable,
                    str(HELPER),
                    str(self.work),
                    str(self.grpc_port),
                    str(self.http_port),
                    "fresh" if fresh else "restore",
                ],
                cwd=Path(__file__).parents[2],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        finally:
            log.close()

        def ready():
            if self.process.poll() is not None:
                raise AssertionError((self.work / "server.log").read_text())
            return (self.work / "ready").exists()

        try:
            _wait_until(ready, 15)
        except Exception:
            self.kill()
            raise
        return self

    def kill(self):
        if self.process is not None and self.process.poll() is None:
            os.kill(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=10)
        self.process = None

    def barrier(self, operation):
        self.sequence += 1
        name = f"{self.sequence:04d}.json"
        (self.work / "commands" / name).write_text(
            json.dumps({"operation": operation}), encoding="utf-8"
        )
        ack = self.work / "acks" / name
        _wait_until(lambda: ack.exists() or self.process.poll() is not None)
        if not ack.exists():
            raise AssertionError((self.work / "server.log").read_text())
        return json.loads(ack.read_text(encoding="utf-8"))["success"]

    def client(self, transport="grpc"):
        return TaklerServiceClient(
            host="127.0.0.1",
            port=self.http_port if transport == "http" else self.grpc_port,
            transport_name=transport,
            retry_window=0,
            single_timeout=5,
        )

    def show(self, transport="grpc"):
        with redirect_stdout(io.StringIO()):
            response = self.client(transport).show()
        return parse_show(response.output)

    def status(self, transport="grpc"):
        with redirect_stdout(io.StringIO()):
            return self.client(transport).server_status()

    def submissions(self, task_name=None):
        path = self.work / "submissions.log"
        lines = path.read_text().splitlines() if path.exists() else []
        return len(lines) if task_name is None else lines.count(task_name)

    def wait_for_submissions(self, count):
        _wait_until(lambda: self.submissions() >= count)
        assert self.submissions() == count

    def cli(self, language, transport, *arguments, go_binary=None):
        command = (
            [sys.executable, "-m", "takler.client"]
            if language == "python"
            else [str(go_binary)]
        )
        env = {k: v for k, v in os.environ.items() if not k.startswith("TAKLER_")}
        env.update(
            TAKLER_HOST="127.0.0.1",
            TAKLER_PORT=str(self.http_port if transport == "http" else self.grpc_port),
            TAKLER_TRANSPORT=transport,
            TAKLER_TIMEOUT="0",
            NO_PROXY="127.0.0.1,localhost,::1",
            no_proxy="127.0.0.1,localhost,::1",
        )
        return subprocess.run(
            [*command, *arguments],
            cwd=Path(__file__).parents[2],
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )


@pytest.fixture
def crashed_server(tmp_path):
    server = CrashServer(tmp_path / "run").start(fresh=True)
    try:
        yield server
    finally:
        server.kill()


@pytest.fixture(scope="module")
def go_binary(tmp_path_factory):
    if shutil.which("go") is None:
        pytest.skip("Go toolchain is unavailable")
    repo = Path(__file__).resolve().parents[3] / "takler-client"
    if not (repo / "go.mod").exists():
        pytest.skip("paired Go checkout is unavailable")
    binary = tmp_path_factory.mktemp("r1-13-go") / "takler_client"
    subprocess.run(
        ["go", "build", "-o", str(binary), "main.go"],
        cwd=repo,
        check=True,
        timeout=90,
    )
    return binary


def _assert_recovered(server, expected_state, count):
    snapshot = server.show()
    assert snapshot.service.status == "halted"
    assert "recovery" in snapshot.service.causes
    assert snapshot.get("/flow/a").state == expected_state
    assert server.submissions() == count
    time.sleep(0.15)
    assert server.submissions() == count


def test_queued_snapshot_then_external_submit_and_lost_log(crashed_server, go_binary):
    server = crashed_server
    assert server.barrier("checkpoint")
    result = server.cli("python", "grpc", "run", "/flow/a")
    assert result.returncode == 0, result.stdout + result.stderr
    server.wait_for_submissions(1)
    server.kill()

    # Neither deleting logs nor adding a claimed success can change recovery.
    log = server.work / "server.log"
    log.unlink()
    log.write_text("success: /flow/a complete\n", encoding="utf-8")
    server.start(fresh=False)
    _assert_recovered(server, "queued", 1)
    assert server.show().get("/flow/a").attempt_id is None
    for language in ("python", "go"):
        for transport in ("grpc", "http"):
            status = server.cli(
                language, transport, "server-status", go_binary=go_binary
            )
            assert status.returncode == 0, status.stdout + status.stderr
            assert '"status":"halted"' in status.stdout
            denied = server.cli(
                language, transport, "run", "/flow/a", go_binary=go_binary
            )
            assert denied.returncode != 0
            assert server.submissions() == 1


def test_active_snapshot_then_child_completion_is_lost(crashed_server):
    server = crashed_server
    assert server.cli("python", "grpc", "run", "/flow/a").returncode == 0
    server.wait_for_submissions(1)
    attempt = server.show().get("/flow/a").attempt_id
    assert attempt
    init = server.cli(
        "python",
        "grpc",
        "init",
        "--node-path",
        "/flow/a",
        "--task-id",
        "job-a",
        "--attempt-id",
        attempt,
    )
    assert init.returncode == 0, init.stdout + init.stderr
    assert server.barrier("checkpoint")
    completed = server.cli(
        "python",
        "http",
        "complete",
        "--node-path",
        "/flow/a",
        "--attempt-id",
        attempt,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert server.show().get("/flow/a").state == "complete"
    server.kill()
    server.start(fresh=False)
    _assert_recovered(server, "active", 1)
    assert server.show().get("/flow/a").attempt_id == attempt


def test_lost_http_response_does_not_prove_run_failed(crashed_server):
    server = crashed_server
    assert server.barrier("checkpoint")
    body = json.dumps(
        {
            "version": "1",
            "command": "run",
            "trace_id": uuid4().hex,
            "payload": {"node_paths": ["/flow/a"], "force": False},
        }
    ).encode()
    request = (
        f"POST /v1/commands/run HTTP/1.1\r\nHost: 127.0.0.1\r\n"
        f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode() + body
    with socket.create_connection(("127.0.0.1", server.http_port), timeout=5) as sock:
        sock.sendall(request)
        sock.shutdown(socket.SHUT_WR)
        server.wait_for_submissions(1)
        # The request executed; deliberately close without reading its response.
    server.kill()
    server.start(fresh=False)
    _assert_recovered(server, "queued", 1)


def test_checkpoint_capture_before_concurrent_external_submit(crashed_server):
    server = crashed_server
    assert server.barrier("checkpoint_async_block")
    _wait_until(lambda: (server.work / "checkpoint_entered").exists())
    result = server.cli("python", "grpc", "run", "/flow/a")
    assert result.returncode == 0, result.stdout + result.stderr
    server.wait_for_submissions(1)
    assert server.barrier("checkpoint_release")
    assert server.show().get("/flow/a").state == "submitted"
    server.kill()
    server.start(fresh=False)
    _assert_recovered(server, "queued", 1)


def test_partial_batch_has_external_effects_absent_from_snapshot(crashed_server):
    server = crashed_server
    assert server.barrier("checkpoint")
    result = server.cli("python", "grpc", "run", "/flow/a", "/missing", "/flow/b")
    assert result.returncode == 1
    assert "batch_failed" in result.stderr
    server.wait_for_submissions(2)
    assert server.submissions("a") == 1
    assert server.submissions("b") == 1
    server.kill()
    server.start(fresh=False)
    _assert_recovered(server, "queued", 2)
    snapshot = server.show()
    assert snapshot.get("/flow/b").state == "queued"
    assert snapshot.get("/flow/b").attempt_id is None
    assert snapshot.get("/missing") is None


def test_corrupt_primary_falls_back_to_older_backup(crashed_server):
    server = crashed_server
    assert server.barrier("checkpoint")
    assert server.cli("python", "grpc", "run", "/flow/a").returncode == 0
    server.wait_for_submissions(1)
    assert server.barrier("checkpoint")
    assert (server.work / "state.check.b").exists()
    server.kill()
    (server.work / "state.check").write_text("{corrupt", encoding="utf-8")
    server.start(fresh=False)
    _assert_recovered(server, "queued", 1)
    assert server.status().restore_summary["source"] == "backup"


@pytest.mark.parametrize(
    ("operation", "expected_state"),
    [("fail_checkpoint", "queued"), ("fail_checkpoint_late", "submitted")],
)
def test_failed_save_restores_whichever_complete_snapshot_was_published(
    crashed_server, operation, expected_state
):
    server = crashed_server
    assert server.barrier("checkpoint")
    assert server.cli("python", "grpc", "run", "/flow/a").returncode == 0
    server.wait_for_submissions(1)
    assert server.barrier(operation) is False
    assert "checkpoint_failure" in server.status().halt_causes
    denied = server.cli("python", "http", "run", "/flow/b")
    assert denied.returncode != 0
    server.kill()
    server.start(fresh=False)
    _assert_recovered(server, expected_state, 1)
    assert server.show().get("/flow/b").state == "queued"
