"""Entry point: ``python -m takler.tui``.

Connects to a running takler server and opens the Textual UI.

Connection resolution (highest priority first):

1. ``--connect-file`` / ``$TAKLER_CONNECT_FILE`` — YAML file written by
   :func:`takler.server.connect_config.save_connect_config`.
2. ``--host`` / ``--port`` (or ``$TAKLER_HOST`` / ``$TAKLER_PORT``).
3. defaults from :mod:`takler.constant`.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import typer

from takler.constant import DEFAULT_HOST, DEFAULT_PORT
from takler.server.connect_config import (
    TAKLER_CONNECT_FILE,
    load_connect_config,
)

from .app import TaklerTuiApp
from .service import TaklerTuiService


app = typer.Typer(
    add_completion=False,
    help="Textual TUI client for a takler server.",
)


def _resolve(
    connect_file: Optional[str],
    host: Optional[str],
    port: Optional[str],
) -> tuple[str, str]:
    """Resolve (host, port) from CLI flags + env."""

    file_path: Optional[str] = connect_file or os.environ.get(TAKLER_CONNECT_FILE)
    if file_path:
        config = load_connect_config(Path(file_path))
        return config.server.address.hostname, config.server.address.port

    resolved_host = host or os.environ.get("TAKLER_HOST") or DEFAULT_HOST
    resolved_port = port or os.environ.get("TAKLER_PORT") or DEFAULT_PORT
    return resolved_host, str(resolved_port)


@app.command()
def main(
    host: Optional[str] = typer.Option(
        None, "--host", help="takler server host (or env TAKLER_HOST)."
    ),
    port: Optional[str] = typer.Option(
        None, "--port", help="takler server port (or env TAKLER_PORT)."
    ),
    connect_file: Optional[str] = typer.Option(
        None,
        "--connect-file",
        help="path to a connect.yaml (or env TAKLER_CONNECT_FILE).",
    ),
) -> None:
    """Launch the TUI."""
    resolved_host, resolved_port = _resolve(connect_file, host, port)
    service = TaklerTuiService(host=resolved_host, port=resolved_port)
    TaklerTuiApp(service=service).run()


if __name__ == "__main__":
    app()
