#!/usr/bin/env bash
# Verify published wheel dependencies in separate environments, without dev groups.
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

uv build --wheel --out-dir "$work_dir/dist" "$project_root"
wheel="$(find "$work_dir/dist" -maxdepth 1 -name 'takler-*.whl' -print -quit)"
test -n "$wheel"

for variant in default http tui; do
    environment="$work_dir/$variant"
    uv venv --python "${TAKLER_INSTALL_PYTHON:-$(command -v python)}" "$environment"
    requirement="$wheel"
    if [[ "$variant" != default ]]; then
        requirement="$wheel[$variant]"
    fi
    uv pip install --python "$environment/bin/python" "$requirement"
    "$environment/bin/python" - "$variant" "$work_dir/$variant.check" <<'PY'
import asyncio
import importlib
from pathlib import Path
import socket
import sys

variant = sys.argv[1]
for module in ("takler", "takler.server.cli", "takler.client.cli"):
    importlib.import_module(module)
if variant == "http":
    for module in ("fastapi", "uvicorn", "httpx", "takler.server.http_transport", "takler.client.http_transport"):
        importlib.import_module(module)
elif variant == "tui":
    for module in ("textual", "rich", "takler.tui.__main__"):
        importlib.import_module(module)

from takler.server import TaklerServer

with socket.socket() as probe:
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]

async def smoke_server():
    server = TaklerServer(host="127.0.0.1", port=port, checkpoint_file=Path(sys.argv[2]))
    server.scheduler.interval_main_loop = 0.05
    await server.start()
    run_task = asyncio.create_task(server.run())
    await asyncio.sleep(0.05)
    await server.stop()
    await run_task

asyncio.run(smoke_server())
PY
    "$environment/bin/takler-server" --help > /dev/null
    "$environment/bin/takler-client-py" --help > /dev/null
    "$environment/bin/takler-client-py" server-status --help > /dev/null
    if [[ "$variant" == tui ]]; then
        "$environment/bin/takler-tui" --help > /dev/null
    fi
    echo "$variant wheel install and entry points: OK"
done
