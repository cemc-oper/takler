"""R2-12B: paired 100k no-change and sparse sync over real transports."""

import argparse
import json
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import time

from takler.core import NodeStatus
from takler.protocol.commands import ServerHaltCommand
from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server
from query_a_matrix_100k import (
    GO_BINARY,
    child_environment,
    make_tree,
    summary_digest,
)


def run_matrix():
    assert GO_BINARY.is_file(), "build repo/takler-client/bin/takler_client first"
    fixtures = runpy.run_path(str(Path.cwd() / "tests/integration/conftest.py"))
    ServerRunner, free_port = fixtures["ServerRunner"], fixtures["free_port"]
    bunch, tasks = make_tree(100_000, False)
    grpc_port, http_port = free_port(), free_port()
    config = ConnectConfig(
        server=Server(
            address=Address(hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)),
            http=HttpSettings(host="127.0.0.1", port=str(http_port)),
        )
    )
    records = []
    with tempfile.TemporaryDirectory() as temporary:
        runner = ServerRunner(
            checkpoint_file=Path(temporary) / "takler.check",
            port=grpc_port,
            connect_config=config,
            interval_main_loop=10.0,
        )
        runner.start()
        try:
            for flow in bunch.flows.values():
                runner.server.bunch.add_flow(flow)
            runner.server.scheduler.run_command_server_halt(ServerHaltCommand())
            service = runner.server.scheduler.query_service
            original_page, original_since = service.page, service.since
            page_calls = since_calls = 0
            inject = changed = False

            async def counted_page(request):
                nonlocal page_calls
                page_calls += 1
                return await original_page(request)

            async def changed_since(request):
                nonlocal changed, since_calls
                since_calls += 1
                if inject and not changed and request.cursor is None:
                    tasks[0].set_node_status_only(NodeStatus.queued)
                    changed = True
                return await original_since(request)

            service.page = counted_page
            service.since = changed_since
            for scenario in ("unchanged", "sparse"):
                for transport in ("grpc", "http"):
                    port = grpc_port if transport == "grpc" else http_port
                    env = child_environment(transport)
                    for language, executable in (
                        ("python", [sys.executable, "-m", "takler.client"]),
                        ("go", [str(GO_BINARY)]),
                    ):
                        command = [
                            *executable,
                            "sync",
                            "--host",
                            "127.0.0.1",
                            "--port",
                            str(port),
                            "--polls",
                            "1",
                            "--interval",
                            "0" if language == "python" else "0s",
                        ]
                        for attempt in range(3):
                            tasks[0].set_node_status_only(NodeStatus.complete)
                            inject = scenario == "sparse"
                            changed = False
                            before_pages, before_since = page_calls, since_calls
                            with tempfile.TemporaryFile(mode="w+t") as output:
                                started = time.perf_counter()
                                result = subprocess.run(
                                    command,
                                    stdout=output,
                                    stderr=subprocess.PIPE,
                                    text=True,
                                    env=env,
                                    timeout=90,
                                )
                                elapsed = time.perf_counter() - started
                                if result.returncode == 0:
                                    digest = summary_digest(output)
                                    output.seek(0)
                                    rendered = output.read()
                                    break
                            if (
                                "resource_exhausted" not in result.stderr
                                or attempt == 2
                            ):
                                raise RuntimeError(result.stderr)
                            time.sleep(0.2)
                        assert since_calls - before_since == 1
                        assert changed == inject
                        assert ("task0000 [queued]" in rendered) == inject
                        records.append(
                            {
                                "scenario": scenario,
                                "language": language,
                                "transport": transport,
                                "nodes": 100_101,
                                "page_calls": page_calls - before_pages,
                                "since_calls": since_calls - before_since,
                                "summary_sha256": digest,
                                "seconds": round(elapsed, 3),
                                "capture_retries": attempt,
                            }
                        )
        finally:
            runner.stop()
    for scenario in ("unchanged", "sparse"):
        assert (
            len(
                {
                    item["summary_sha256"]
                    for item in records
                    if item["scenario"] == scenario
                }
            )
            == 1
        )
    assert records[0]["summary_sha256"] != records[-1]["summary_sha256"]
    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    options = parser.parse_args()
    payload = {"matrix": run_matrix(), "python": sys.version.split()[0]}
    raw = json.dumps(payload, indent=2) + "\n"
    if options.output:
        options.output.write_text(raw)
    print(raw)
