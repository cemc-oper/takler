"""The ``takler-server`` command line interface.

The entry point is deliberately thin: it resolves the startup options, builds a
:class:`~takler.server.TaklerServer` and drives it through ``asyncio.run``.
Everything else -- snapshot restore, the scheduler main loop, the gRPC service,
the periodic and final snapshots -- already lives in ``TaklerServer``.

Two things here are contracts rather than implementation details:

* ``takler-server --help`` prints the startup options and exits with ``0``
  (requirements 15.3, 15.4). ``python -m takler.server`` is the same CLI.
* ``SIGTERM`` / ``SIGINT`` are routed to :meth:`TaklerServer.stop`, not left to
  the default handlers. The default ``SIGINT`` behaviour raises
  ``KeyboardInterrupt`` out of ``asyncio.run`` and the default ``SIGTERM``
  behaviour kills the process outright; in both cases the shutdown snapshot
  written by ``CheckpointManager.stop()`` would be lost (requirement 5.9).

``--host`` / ``--port`` additionally carry the operational precondition of
requirement 6.23: when restarting from a snapshot that holds submitted or
active jobs, they must name the same address the snapshot recorded, because the
job scripts of those in-flight jobs already hold the old address and will report
their outcome there. That precondition constrains the address this process
actually uses, no matter whether it came from the command line or from the
Connect_Config file.

Requirements: 15.3, 15.4, 5.9, 6.23.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from pathlib import Path
from typing import Optional

import typer

from takler.constant import DEFAULT_HOST, DEFAULT_PORT
from takler.logging import get_logger
from takler.server import TaklerServer
from takler.server.connect_config import (
    TAKLER_CONNECT_FILE,
    TAKLER_EXCEPTION_POLICY,
    ConnectConfig,
    load_connect_config,
)


logger = get_logger("server.cli")


#: Shared tail of the ``--host`` / ``--port`` help text, spelling out the
#: operational precondition of requirement 6.23.
_ADDRESS_HELP_TAIL = (
    "when restarting from a checkpoint holding submitted or active jobs, this "
    "must be the same value the checkpoint recorded, otherwise those jobs "
    "report to the old address and stay stuck"
)

HOST_HELP = (
    f"host announced to clients and to job scripts, defaults to the "
    f"connect.yaml value or {DEFAULT_HOST}; {_ADDRESS_HELP_TAIL}"
)
PORT_HELP = (
    f"port the gRPC service listens on, defaults to the connect.yaml value "
    f"or {DEFAULT_PORT}; {_ADDRESS_HELP_TAIL}"
)
CONFIG_HELP = (
    f"path to a connect.yaml providing host / port and the checkpoint section, "
    f"or use env var {TAKLER_CONNECT_FILE}"
)
CHECKPOINT_FILE_HELP = (
    "checkpoint file path; overrides the connect.yaml value, "
    "defaults to takler.check in the current working directory"
)
CHECKPOINT_INTERVAL_HELP = (
    "seconds between two checkpoints; overrides the connect.yaml value, "
    "defaults to 120, values below 10 are rejected and fall back to 120"
)
EXCEPTION_POLICY_HELP = (
    f"how unexpected exceptions are handled, resilient or fail_fast, "
    f"or use env var {TAKLER_EXCEPTION_POLICY}; defaults to resilient"
)


app = typer.Typer(
    add_completion=False,
    help="Start a takler server.",
)


def resolve_connect_config(config: Optional[Path] = None) -> Optional[ConnectConfig]:
    """Load the Connect_Config file, if there is one to load.

    Precedence is ``--config`` > ``TAKLER_CONNECT_FILE`` > no file at all
    (requirement 15.4). An explicitly given ``--config`` that cannot be read is
    an error the operator must see, so it propagates; a stale
    ``TAKLER_CONNECT_FILE`` inherited from the environment is only reported as a
    WARNING, because letting it abort a start-up the operator fully specified on
    the command line would be worse than starting without it.

    Parameters
    ----------
    config
        Value of ``--config``, or ``None`` when the option was omitted.

    Returns
    -------
    Optional[ConnectConfig]
        The loaded configuration, or ``None`` when no file was available.
    """
    if config is not None:
        return load_connect_config(config)

    env_path = os.environ.get(TAKLER_CONNECT_FILE)
    if env_path is None or env_path.strip() == "":
        return None

    try:
        return load_connect_config(env_path)
    except Exception as exc:  # noqa: BLE001 - a stale env var must not abort start-up
        logger.warning(
            f"ignoring {TAKLER_CONNECT_FILE}={env_path!r}: {type(exc).__name__}: {exc}"
        )
        return None


def resolve_address(
    host: Optional[str],
    port: Optional[int],
    connect_config: Optional[ConnectConfig],
) -> tuple[str, int]:
    """Resolve the address this process will use.

    Precedence is command line option > Connect_Config file > the defaults of
    :mod:`takler.constant`, which are also what the client CLI falls back to, so
    an unconfigured server and an unconfigured client still meet. Requirement
    6.23 applies to the resolved values regardless of which source produced them.
    """
    result_host = host
    result_port = port

    if connect_config is not None:
        address = connect_config.server.address
        if result_host is None:
            result_host = address.hostname
        if result_port is None:
            result_port = int(address.port)

    if result_host is None:
        result_host = DEFAULT_HOST
    if result_port is None:
        result_port = int(DEFAULT_PORT)

    return result_host, result_port


def _install_signal_handlers(server: TaklerServer) -> None:
    """Route ``SIGTERM`` / ``SIGINT`` to :meth:`TaklerServer.stop`.

    ``stop()`` is a coroutine, so the handler schedules it as a task instead of
    awaiting it: the running loop then walks the normal shutdown flow, ending
    with the final snapshot (requirement 5.9). ``TaklerServer`` guards that flow
    so repeated signals are harmless.

    ``loop.add_signal_handler`` is unavailable on some platforms (Windows), in
    which case the signal keeps its default behaviour and the shutdown snapshot
    is best-effort.
    """
    loop = asyncio.get_running_loop()

    def request_stop(sig: signal.Signals) -> None:
        logger.info(f"received {sig.name}, stopping server...")
        loop.create_task(server.stop(), name="takler.server.stop")

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_stop, sig)
        except (NotImplementedError, RuntimeError):
            logger.warning(
                f"cannot install a {sig.name} handler on this platform; "
                f"the shutdown checkpoint may be skipped"
            )


async def _serve(server: TaklerServer) -> None:
    """Start the server, run it until stopped, and stop it cleanly."""
    _install_signal_handlers(server)
    await server.start()
    try:
        await server.run()
    finally:
        # ``run()`` already goes through the shared shutdown flow, and that flow
        # runs at most once, so this only matters when ``run()`` was interrupted
        # (e.g. cancellation): the final snapshot still gets written.
        with contextlib.suppress(Exception):
            await server.stop()


def serve_forever(server: TaklerServer) -> None:
    """Run ``server`` in a fresh event loop until it stops.

    Kept as a separate function so tests can exercise option handling without
    binding a port.
    """
    asyncio.run(_serve(server))


@app.command()
def serve(
    host: Optional[str] = typer.Option(None, "--host", help=HOST_HELP),
    port: Optional[int] = typer.Option(None, "--port", help=PORT_HELP),
    config: Optional[Path] = typer.Option(None, "--config", help=CONFIG_HELP),
    checkpoint_file: Optional[Path] = typer.Option(
        None, "--checkpoint-file", help=CHECKPOINT_FILE_HELP
    ),
    checkpoint_interval: Optional[float] = typer.Option(
        None, "--checkpoint-interval", help=CHECKPOINT_INTERVAL_HELP
    ),
    exception_policy: Optional[str] = typer.Option(
        None, "--exception-policy", help=EXCEPTION_POLICY_HELP
    ),
) -> None:
    """Start a takler server: restore the last checkpoint, then serve."""
    connect_config = resolve_connect_config(config)
    resolved_host, resolved_port = resolve_address(host, port, connect_config)

    server = TaklerServer(
        host=resolved_host,
        port=resolved_port,
        exception_policy=exception_policy,
        connect_config=connect_config,
        checkpoint_file=checkpoint_file,
        checkpoint_interval=checkpoint_interval,
    )
    serve_forever(server)


def main() -> None:
    """Console script entry point of ``takler-server``."""
    app()


if __name__ == "__main__":
    main()
