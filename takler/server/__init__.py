import asyncio
import contextlib
from pathlib import Path
from typing import Union, Optional

from takler.core import Bunch, NodeStatus
from takler.logging import configure, get_logger

from .scheduler import Scheduler
from .network_service import TaklerService
from .checkpoint import CheckpointManager
from .connect_config import ConnectConfig, ExceptionPolicy, resolve_exception_policy


logger = get_logger("server")


class TaklerServer:
    """
    Takler server which will create three members when init:

    * bunch: A bunch for flows.
    * scheduler: A scheduler to check dependencies in loop.
    * network service: A gRPC server to receive client command.
    * checkpoint manager: owns the Checkpoint_File of this server process.
    """
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[Union[str, int]] = None,
        exception_policy: "Optional[Union[str, ExceptionPolicy]]" = None,
        connect_config: "Optional[ConnectConfig]" = None,
        checkpoint_file: Optional[Union[str, Path]] = None,
        checkpoint_interval: Optional[float] = None,
    ):
        """Build the bunch and the three services that operate on it.

        Args:
            host: Host name announced to clients and to job scripts.
            port: Port the gRPC service listens on.
            exception_policy: Explicit exception-handling policy, highest
                precedence (Requirement 2.6).
            connect_config: Loaded ``connect.yaml``, consulted for the snapshot
                settings when the explicit arguments below are not provided
                (Requirement 7.5).
            checkpoint_file: Explicit Checkpoint_File path, highest precedence.
                ``None`` leaves the resolution to ``CheckpointManager``, which
                falls back to ``connect_config`` and then to ``takler.check``
                relative to the current working directory (Requirement 7.3).
            checkpoint_interval: Explicit snapshot period in seconds, highest
                precedence (Requirement 7.2).
        """
        # Resolve the effective exception-handling policy following the source
        # precedence: explicit argument > ``TAKLER_EXCEPTION_POLICY`` env var >
        # built-in default ``RESILIENT`` (Requirement 2.6).
        self.exception_policy: ExceptionPolicy = resolve_exception_policy(exception_policy)

        # Shared fatal-error signal. In ``FAIL_FAST`` mode the scheduler / service
        # request a clean server exit by triggering this event; ``run()`` waits on
        # it alongside the scheduler task so both modes share one clean shutdown
        # path (Requirements 2.4, 2.5, 3.4).
        self._fatal_error_event: asyncio.Event = asyncio.Event()
        # Guard so the clean shutdown flow runs at most once, whether it is
        # reached through ``stop()`` or through ``run()`` after the scheduler
        # task completes / the fatal-error event fires.
        self._stopped: bool = False

        port_str = str(port)
        self.bunch: Bunch = Bunch(host=host, port=port_str)
        self.scheduler: Scheduler = Scheduler(
            bunch=self.bunch,
            exception_policy=self.exception_policy,
            fatal_shutdown=self._trigger_fatal_shutdown,
        )
        self.network_service: TaklerService = TaklerService(
            scheduler=self.scheduler,
            host="[::]",
            port=port,
            exception_policy=self.exception_policy,
            fatal_shutdown=self._trigger_fatal_shutdown,
        )
        # The manager keeps a reference to the same live bunch the scheduler and
        # the network service hold, so a restored snapshot is visible to both
        # without any of them being re-wired (Requirements 5.1, 6.1).
        self.checkpoint_manager: CheckpointManager = CheckpointManager(
            bunch=self.bunch,
            checkpoint_file=checkpoint_file,
            interval=checkpoint_interval,
            connect_config=connect_config,
        )

    def _trigger_fatal_shutdown(self):
        """Signal that the server must exit (the ``FAIL_FAST`` path).

        Sets the shared fatal-error event so that ``run()`` -- which waits on it
        together with the scheduler task -- wakes up and drives the shared clean
        shutdown flow. Safe to call more than once.
        """
        self._fatal_error_event.set()

    async def start(self):
        """
        Start services:

        * configure logging
        * restore the bunch from the Checkpoint_File
        * start scheduler
        * start network service
        * start the periodic snapshot task

        The order is the contract. ``restore()`` runs synchronously before the
        scheduler is started, so the very first dependency resolution already
        sees the restored node tree (Requirement 6.1), and the periodic snapshot
        task is created last, so its first write cannot race the restore
        (Requirement 5.1).
        """
        # Configure logging before the first server record is emitted so that
        # startup, command, and shutdown activity is captured at the configured
        # level and destinations. No explicit arguments are passed, so the
        # configuration is derived from environment variables and built-in
        # defaults (Requirements 10.1, 10.2).
        #
        # Configuration is guarded so that any failure does not abort server
        # startup: on failure we fall back to a console sink at INFO, emit a
        # WARNING describing the failure, and let startup proceed
        # (Requirements 10.3, 10.4).
        try:
            configure()
        except Exception as exc:  # noqa: BLE001 - never let logging abort startup
            try:
                configure(level="INFO", console=True)
            except Exception:  # noqa: BLE001 - keep the fallback itself robust
                # Even the fallback configuration failed; do not let startup
                # crash. The logging subsystem applies a default INFO-to-console
                # configuration lazily on first use, so logging still works.
                pass
            get_logger("server").warning(
                f"logging configuration failed, falling back to console at "
                f"INFO: {exc}"
            )

        logger.info("start server...")
        # Restore before the scheduler main loop exists (Requirement 6.1).
        # ``restore()`` never raises: an unusable snapshot degrades to the backup
        # file and then to an empty bunch, so startup always proceeds.
        self.checkpoint_manager.restore()
        await self.scheduler.start()
        await self.network_service.start()
        await self.checkpoint_manager.start()
        logger.info("start server...done")

    async def run(self):
        """
        Run services:

        * run network service
        * run scheduler

        ``run()`` returns when either the scheduler task finishes (e.g. after a
        normal ``stop()``) or the shared fatal-error event fires (the
        ``FAIL_FAST`` path). In both cases it drives the same clean shutdown flow
        as :meth:`stop` before returning, so the two modes share one identical
        clean shutdown path (Requirements 2.4, 2.5, 3.4).
        """
        loop = asyncio.get_running_loop()
        loop.create_task(self.network_service.run(), name="takler.server.network_service")

        scheduler_task = loop.create_task(self.scheduler.run(), name="takler.server.scheduler")
        # ``asyncio.wait`` requires awaitables wrapped as tasks/futures, so wrap
        # the event wait in its own task and wait for whichever finishes first.
        fatal_task = asyncio.ensure_future(self._fatal_error_event.wait())

        try:
            await asyncio.wait(
                {scheduler_task, fatal_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            # Tidy up the fatal-error waiter if it did not complete (scheduler
            # finished first), so it does not linger as a pending task.
            if not fatal_task.done():
                fatal_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await fatal_task

        # Whichever branch woke us up, go through the shared clean shutdown flow.
        await self._shutdown()

    async def _shutdown(self):
        """
        Clean shutdown flow shared by :meth:`stop` and :meth:`run`.

        Stops the network service, the scheduler and the checkpoint manager
        without raising. Guarded by ``self._stopped`` so it runs at most once
        even when both an external ``stop()`` call and ``run()`` reach it
        (Requirement 3.4).

        The checkpoint manager is stopped last on purpose: its ``stop()`` writes
        the final snapshot, and by then no RPC handler and no scheduler pass can
        still change the bunch, so that snapshot is a consistent view of a
        quiesced tree (Requirement 5.9).
        """
        if self._stopped:
            return
        self._stopped = True
        logger.info("stop server...")
        await self.network_service.stop()
        await self.scheduler.stop()
        # Cancels the periodic task and writes the last snapshot; never raises.
        await self.checkpoint_manager.stop()
        logger.info("stop server...done")

    async def stop(self):
        """
        Stop all services:

        * stop network service
        * stop scheduler
        """
        # Record the server shutdown event at INFO level through the named
        # "server" logger, mirroring the start event (Requirement 10.7).
        await self._shutdown()


async def run_server_until_complete(server: TaklerServer, check_interval: int = 10):
    """
    Start and run takler server until all flows in bunch are complete.

    Parameters
    ----------
    server
    check_interval
        check interval seconds

    Examples
    --------
    Run a simple flow.

    >>> import asyncio
    >>> from takler.core import Flow
    >>> from takler.server import TaklerServer, run_server_until_complete
    >>> server = TaklerServer(host="login_a06", port=33083)
    >>> flow = Flow("flow1")
    >>> task1 = flow.add_task("task1")
    >>> server.bunch.add_flow(flow)
    >>> flow.begin()
    >>> asyncio.run(run_server_until_complete(server))

    """
    await start_server(server)

    await wait_server_until_complete(server, check_interval)

    await stop_server(server)


async def start_server(server: TaklerServer):
    """
    Start server, and run the server in current running loop.

    Parameters
    ----------
    server
        takler server
    """
    await server.start()
    loop = asyncio.get_running_loop()
    task = loop.create_task(server.run(), name="takler.server")
    return task


async def wait_server_until_complete(server: TaklerServer, check_interval: int = 10):
    """
    Loop check until all flows in bunch are complete.

    Parameters
    ----------
    server
        takler server with some flows.
    check_interval
        sleep seconds between checks.
    """
    while True:
        status = server.bunch.get_node_status()
        if status == NodeStatus.complete:
            break

        await asyncio.sleep(check_interval)


async def stop_server(server: TaklerServer, seconds_before_stop: int = 10):
    """
    Stop takler server.

    Parameters
    ----------
    server
        takler server.
    seconds_before_stop
        sleep seconds before stop the server.
    """
    logger.info(f"all flows are complete, about to exit, sleep for {seconds_before_stop} seconds...")
    await asyncio.sleep(seconds_before_stop)
    logger.info("stop server...")
    await server.stop()
    logger.info("stop server...done")
