"""R2-12A: Python/Go x HTTP/gRPC over the fixed 100k real server tree.

Run from repo/takler after building repo/takler-client/bin/takler_client::

    uv run python scripts/query_a_matrix_100k.py

Use --output PATH to retain the JSON result. One server is used per tree shape.
"""

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import time

from takler.core import Bunch, Flow, NodeStatus
from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server


GO_BINARY = Path(
    os.environ.get(
        "TAKLER_QUERY_GO_CLIENT",
        Path.cwd().parent / "takler-client/bin/takler_client",
    )
)


def wide_value(index):
    return "".join(
        hashlib.sha256(f"r2-01:{index}:{part}".encode()).hexdigest()
        for part in range(32)
    )


def make_tree(size, wide):
    """The fixed R2-01 100k fixture, kept in the executable paired checkout."""
    bunch = Bunch(name="benchmark", host="127.0.0.1", port="0")
    tasks = []
    for flow_no in range((size + 999) // 1000):
        flow = Flow(f"flow{flow_no:03d}")
        flow.add_parameter("FLOW_LABEL", f"batch-{flow_no:03d}")
        for task_no in range(min(1000, size - flow_no * 1000)):
            task = flow.add_task(f"task{task_no:04d}")
            task.state.node_status = NodeStatus.complete
            if wide:
                task.add_parameter("PAYLOAD", wide_value(flow_no * 1000 + task_no))
            tasks.append(task)
        bunch.add_flow(flow)
        flow.requeue_calendar()
        flow.begun = True
    return bunch, tasks


def child_environment(transport):
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("TAKLER_")
        and key.lower() not in {"http_proxy", "https_proxy", "all_proxy", "grpc_proxy"}
    }
    env.update(
        TAKLER_TRANSPORT=transport,
        TAKLER_TIMEOUT="0",
        NO_PROXY="127.0.0.1,localhost",
        no_proxy="127.0.0.1,localhost",
    )
    return env


def checked_show(command, env, output):
    for attempt in range(3):
        output.seek(0)
        output.truncate()
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
            return elapsed, attempt
        if "resource_exhausted" not in result.stderr or attempt == 2:
            raise RuntimeError(f"{' '.join(command[:2])}: {result.stderr}")
        time.sleep(0.2)
    raise AssertionError("unreachable")


def summary_digest(output):
    output.seek(0)
    digest = hashlib.sha256()
    flows = tasks = 0
    first = last = None
    for line in output:
        if "|- " not in line:
            continue
        if first is None:
            first = line.strip()
        last = line.strip()
        digest.update(line.encode())
        if "|- flow" in line:
            flows += 1
        elif "|- task" in line:
            tasks += 1
    assert (flows, tasks) == (100, 100_000), (flows, tasks)
    assert first.startswith("|- flow000 ") and last.startswith("|- task0999 ")
    return digest.hexdigest()


def install_tree(runner, bunch):
    """Publish the fixture on the server's writer loop in one transaction."""

    async def install():
        scheduler = runner.server.scheduler
        async with scheduler.query_state_lock:
            scheduler.query_changes.begin()
            try:
                for flow in bunch.flows.values():
                    runner.server.bunch.add_flow(flow)
            finally:
                scheduler.query_changes.end()
            scheduler.version_store.flush()
            scheduler.version_store.pin()

    asyncio.run_coroutine_threadsafe(install(), runner._loop).result(timeout=90)


def measure(wide, ServerRunner, free_port, make_tree):
    bunch, tasks = make_tree(100_000, wide)
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
            install_tree(runner, bunch)
            service = runner.server.scheduler.query_service
            page_calls = 0
            original_page = service.page_encoded

            async def counted_page(request):
                nonlocal page_calls
                page_calls += 1
                return await original_page(request)

            service.page_encoded = counted_page
            for transport in ("grpc", "http"):
                port = http_port if transport == "http" else grpc_port
                env = child_environment(transport)
                for language, executable in (
                    ("python", [sys.executable, "-m", "takler.client"]),
                    ("go", [str(GO_BINARY)]),
                ):
                    command = [
                        *executable,
                        "show",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                    ]
                    before = page_calls
                    with tempfile.TemporaryFile(mode="w+t") as output:
                        elapsed, retries = checked_show(command, env, output)
                        digest = summary_digest(output)
                    pages = page_calls - before
                    assert pages > 1, pages
                    selected_chars = None
                    if wide:
                        with tempfile.TemporaryFile(mode="w+t") as output:
                            checked_show(
                                command
                                + [
                                    "--scope",
                                    "/flow000/task0000",
                                    "--show-parameter",
                                ],
                                env,
                                output,
                            )
                            output.seek(0)
                            rendered = output.read()
                        expected = tasks[0].user_parameters["PAYLOAD"].value
                        assert f"param PAYLOAD '{expected}'" in rendered
                        selected_chars = len(expected)
                    records.append(
                        {
                            "wide": wide,
                            "language": language,
                            "transport": transport,
                            "nodes": 100_101,
                            "page_calls": pages,
                            "summary_sha256": digest,
                            "seconds": round(elapsed, 3),
                            "capture_retries": retries,
                            "selected_detail_chars": selected_chars,
                        }
                    )
        finally:
            runner.stop()
    assert len({item["summary_sha256"] for item in records}) == 1
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    options = parser.parse_args()
    assert GO_BINARY.is_file(), "build repo/takler-client/bin/takler_client first"
    fixtures = runpy.run_path(str(Path.cwd() / "tests/integration/conftest.py"))
    records = []
    for wide in (False, True):
        records.extend(
            measure(wide, fixtures["ServerRunner"], fixtures["free_port"], make_tree)
        )
    assert len({item["summary_sha256"] for item in records}) == 1
    payload = {"matrix": records, "python": sys.version.split()[0]}
    raw = json.dumps(payload, indent=2) + "\n"
    if options.output:
        options.output.write_text(raw)
    print(raw)


if __name__ == "__main__":
    main()
