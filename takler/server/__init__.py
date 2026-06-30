import asyncio
from typing import Union, Optional

from takler.core import Bunch, NodeStatus
from takler.logging import configure, get_logger

from .scheduler import Scheduler
from .network_service import TaklerService


logger = get_logger("server")


class TaklerServer:
    """
    Takler server which will create three members when init:

    * bunch: A bunch for flows.
    * scheduler: A scheduler to check dependencies in loop.
    * network service: A gRPC server to receive client command.
    """
    def __init__(self, host: Optional[str] = None, port: Optional[Union[str, int]] = None):
        port_str = str(port)
        self.bunch: Bunch = Bunch(host=host, port=port_str)
        self.scheduler: Scheduler = Scheduler(bunch=self.bunch)
        self.network_service: TaklerService = TaklerService(
            scheduler=self.scheduler, host="[::]", port=port
        )

    async def start(self):
        """
        Start services:

        * configure logging
        * start scheduler
        * start network service
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
        await self.scheduler.start()
        await self.network_service.start()
        logger.info("start server...done")

    async def run(self):
        """
        Run services:

        * run network service
        * run scheduler
        """
        loop = asyncio.get_running_loop()
        loop.create_task(self.network_service.run(), name="takler.server.network_service")

        scheduler_task = loop.create_task(self.scheduler.run(), name="takler.server.scheduler")
        await scheduler_task

    async def stop(self):
        """
        Stop all services:

        * stop network service
        * stop scheduler
        """
        # Record the server shutdown event at INFO level through the named
        # "server" logger, mirroring the start event (Requirement 10.7).
        logger.info("stop server...")
        await self.network_service.stop()
        await self.scheduler.stop()
        logger.info("stop server...done")


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
    >>> flow.requeue()
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
