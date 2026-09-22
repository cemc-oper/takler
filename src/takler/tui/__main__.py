"""Entry point: ``python -m takler.tui``.

Connects to a running takler server and opens the Textual UI.

Connection resolution (highest priority first), shared with the CLI's
:func:`takler.client.cli.get_host_and_prot`:

1. ``--host`` / ``--port``.
2. ``--connect-file`` / ``$TAKLER_CONNECT_FILE`` — YAML file written by
   :func:`takler.server.connect_config.save_connect_config`.
3. ``$TAKLER_HOST`` / ``$TAKLER_PORT``.
4. defaults from :mod:`takler.constant`.

Which transport carries the connection is resolved by
:func:`takler.client.transport.resolve_transport` — the Connect_Config's
``server.transport`` field, then ``$TAKLER_TRANSPORT``, then gRPC. With
``http`` selected the config's ``server.http.port`` is the port resolved
above, and the TUI itself stays protocol-agnostic: every call goes through
the client transport abstraction (M3 task 8).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Tuple

import typer

from takler.client.cli import get_host_and_prot
from takler.client.transport import resolve_transport
from takler.server.connect_config import (
    ConnectConfig,
    TAKLER_CONNECT_FILE,
    load_connect_config,
)

from .app import TaklerTuiApp
from .service import TaklerTuiService


app = typer.Typer(
    add_completion=False,
    help="Textual TUI client for a takler server.",
)


def _load_config(connect_file: Optional[str]) -> Optional[ConnectConfig]:
    """Load the Connect_Config named by the flag or the environment, if any."""
    file_path: Optional[str] = connect_file or os.environ.get(TAKLER_CONNECT_FILE)
    if not file_path:
        return None
    return load_connect_config(Path(file_path))


def _resolve(
    connect_file: Optional[str],
    host: Optional[str],
    port: Optional[str],
) -> Tuple[str, str, Optional[str], Optional[ConnectConfig]]:
    """Resolve (host, port, transport_name, connect_config) from CLI flags + env."""
    connect_config = _load_config(connect_file)
    transport_name = resolve_transport(None, connect_config)
    resolved_host, resolved_port = get_host_and_prot(
        host, port, connect_config, transport_name=transport_name
    )
    return resolved_host, str(resolved_port), transport_name, connect_config


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
    resolved_host, resolved_port, transport_name, connect_config = _resolve(
        connect_file, host, port
    )
    service = TaklerTuiService(
        host=resolved_host,
        port=resolved_port,
        transport_name=transport_name,
        connect_config=connect_config,
    )
    TaklerTuiApp(service=service).run()


if __name__ == "__main__":
    app()
