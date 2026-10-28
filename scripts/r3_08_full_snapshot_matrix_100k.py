"""100k full-detail CLI matrix over real Python/Go HTTP/gRPC sockets."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import time

from query_a_matrix_100k import child_environment, make_tree
from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server


def measure(
    size: int, wide: bool, runner_type, free_port, go_binary: Path, assembly_only: bool
) -> list[dict]:
    bunch, _ = make_tree(size, wide)
    grpc_port, http_port = free_port(), free_port()
    config = ConnectConfig(
        server=Server(
            address=Address(hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)),
            http=HttpSettings(host="127.0.0.1", port=str(http_port)),
        )
    )
    records = []
    with tempfile.TemporaryDirectory() as temporary:
        runner = runner_type(
            checkpoint_file=Path(temporary) / "takler.check",
            port=grpc_port,
            connect_config=config,
            interval_main_loop=10.0,
        )
        runner.start()
        try:
            for flow in bunch.flows.values():
                # This is the completed, static correctness shape. Keep its
                # calendar fixed while four sequential readers compare nodes.
                flow.begun = False
                runner.server.bunch.add_flow(flow)

            # The fixture attaches whole prebuilt flows outside the command
            # handler. Publish that setup transaction before timing readers.
            async def finish_setup():
                return runner.server.scheduler.version_store.pin().state_version

            asyncio.run_coroutine_threadsafe(finish_setup(), runner._loop).result(
                timeout=90
            )
            service = runner.server.scheduler.query_service.full_snapshots
            page_calls = [0]
            chunk_calls = [0]
            original_page, original_chunk = service.page, service.chunk

            async def counted_page(request):
                page_calls[0] += 1
                return await original_page(request)

            async def counted_chunk(request):
                chunk_calls[0] += 1
                return await original_chunk(request)

            service.page = counted_page
            service.chunk = counted_chunk
            for transport in ("grpc", "http"):
                port = http_port if transport == "http" else grpc_port
                env = child_environment(transport)
                for language, executable in (
                    ("python", [sys.executable, "-m", "takler.client"]),
                    ("go", [str(go_binary)]),
                ):
                    if assembly_only and (language, transport) != ("go", "http"):
                        continue
                    command = [
                        *executable,
                        "snapshot",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                    ]
                    if not assembly_only:
                        command.append("--stream")
                    before_pages, before_chunks = page_calls[0], chunk_calls[0]
                    started = time.perf_counter()
                    with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as output:
                        completed = subprocess.run(
                            command,
                            env=env,
                            stdout=output,
                            stderr=subprocess.PIPE,
                            text=True,
                            timeout=180,
                        )
                        elapsed = time.perf_counter() - started
                        if assembly_only:
                            if wide:
                                if (
                                    completed.returncode == 0
                                    or "resource_exhausted" not in completed.stderr
                                ):
                                    raise RuntimeError(
                                        f"wide Go assembly did not reject budget: {completed.stderr}"
                                    )
                            elif completed.returncode:
                                raise RuntimeError(
                                    f"standard Go assembly failed: {completed.stderr}"
                                )
                            records.append(
                                {
                                    "wide": wide,
                                    "language": language,
                                    "transport": transport,
                                    "mode": "assembly",
                                    "elapsed_s": round(elapsed, 3),
                                    "complete": completed.returncode == 0,
                                    "error_code": "resource_exhausted"
                                    if wide
                                    else None,
                                    "stdout_bytes": output.tell(),
                                    "page_calls": page_calls[0] - before_pages,
                                    "chunk_calls": chunk_calls[0] - before_chunks,
                                }
                            )
                            continue
                        if completed.returncode:
                            raise RuntimeError(
                                f"{language}/{transport}/{wide}: {completed.stderr}"
                            )
                        output.seek(0)
                        node_digest = hashlib.sha256()
                        nodes = 0
                        completion = None
                        for line in output:
                            item = json.loads(line)
                            if "node" in item:
                                encoded = json.dumps(
                                    item["node"],
                                    sort_keys=True,
                                    ensure_ascii=False,
                                    separators=(",", ":"),
                                ).encode()
                                node_digest.update(encoded)
                                node_digest.update(b"\n")
                                nodes += 1
                            elif "complete" in item:
                                completion = item["complete"]
                            else:
                                raise RuntimeError("unknown stream record")
                        if completion is None or completion["total_nodes"] != nodes:
                            raise RuntimeError("incomplete full snapshot stream")
                    records.append(
                        {
                            "wide": wide,
                            "language": language,
                            "transport": transport,
                            "elapsed_s": round(elapsed, 3),
                            "nodes": nodes,
                            "node_sha256": node_digest.hexdigest(),
                            "page_calls": page_calls[0] - before_pages,
                            "chunk_calls": chunk_calls[0] - before_chunks,
                            "completion_bytes": completion["total_bytes"],
                            "completion_sha256": completion["sha256"],
                            "rejected_requests": 0,
                            "retries": 0,
                        }
                    )
        finally:
            runner.stop()
    if not assembly_only and len({item["node_sha256"] for item in records}) != 1:
        raise RuntimeError("Python/Go HTTP/gRPC full-detail nodes differ")
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=100000)
    parser.add_argument("--wide", action="store_true")
    parser.add_argument("--only-go-assembly", action="store_true")
    parser.add_argument(
        "--go-binary",
        type=Path,
        default=Path.cwd().parent / "takler-client/bin/takler_client",
    )
    parser.add_argument("--output", type=Path)
    options = parser.parse_args()
    if not options.go_binary.is_file():
        raise RuntimeError("build repo/takler-client/bin/takler_client first")
    fixtures = runpy.run_path(str(Path.cwd() / "tests/integration/conftest.py"))
    records = measure(
        options.size,
        options.wide,
        fixtures["ServerRunner"],
        fixtures["free_port"],
        options.go_binary,
        options.only_go_assembly,
    )
    raw = json.dumps(records, indent=2) + "\n"
    if options.output:
        options.output.write_text(raw)
    print(raw)


if __name__ == "__main__":
    main()
