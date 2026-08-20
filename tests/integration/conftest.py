"""Shared fixtures for the client <-> server end-to-end integration tests.

These tests exercise the *real* wire path: a ``TaklerServer`` bound to a real
port in this very test process, driven by a real ``TaklerServiceClient`` over
gRPC (Requirement 16.8). That combination needs one piece of scaffolding,
because the two halves have incompatible concurrency models:

* ``TaklerServer`` is an ``asyncio`` server -- ``grpc.aio`` -- so it needs a
  running event loop for as long as it serves,
* ``TaklerServiceClient`` is fully blocking -- ``grpc`` sync API -- so calling
  it from inside that same event loop would block the loop that has to answer
  the call, and the test would deadlock on its very first command.

:class:`ServerRunner` resolves this by owning the event loop in a **background
thread**: the server runs there, and the test body stays plain synchronous code
which may call the blocking client directly. Nothing is faked or mocked -- the
servicer is reached through a real socket.

There is no ``pytest-asyncio`` in this project (see ``pyproject.toml``), which
is the other reason the loop is driven explicitly with ``asyncio.run`` inside
that thread rather than by an async test.
"""

from __future__ import annotations

import asyncio
import socket
import threading
from pathlib import Path
from typing import Iterator, Optional, Union

import pytest

from takler.client.service_client import TaklerServiceClient
from takler.server import TaklerServer


#: Host the server announces and the client dials. Loopback keeps the test
#: hermetic: no name resolution, no traffic leaving the machine.
LOCALHOST: str = "127.0.0.1"

#: Main-loop interval used by the in-process server. It has to be short: the
#: scheduler notices ``should_stop`` only between two iterations, so a long
#: interval would make ``stop()`` -- and with it the fixture teardown -- wait
#: that long.
TEST_MAIN_LOOP_INTERVAL: float = 0.05

#: Per-attempt RPC deadline and Retry_Window used by the test clients. The
#: server is up and local, so a command either succeeds immediately or the test
#: has found a real problem; a short window keeps a failure a fast failure
#: instead of a hung test (the default child-command window is 86400 seconds).
TEST_SINGLE_TIMEOUT: float = 10.0
TEST_RETRY_WINDOW: float = 10.0


def free_port() -> int:
    """Return a currently free TCP port.

    Binding port 0 and reading back the assigned port is the portable way to
    pick a port that is free *right now*; a hard-coded port would make the
    suite fail whenever it happens to be taken (by a developer's own server,
    or by another test running in parallel).
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


class ServerRunner:
    """Runs a real :class:`TaklerServer` in a background event-loop thread.

    Usage::

        runner = ServerRunner(checkpoint_file=tmp_path / "takler.check")
        runner.start()
        try:
            client = runner.make_client()
            client.start()
            client.run_request_ping()
        finally:
            runner.stop()

    Attributes:
        host: Host the server announces to clients and job scripts.
        port: Port the gRPC service listens on.
        checkpoint_file: Checkpoint_File of this server. Always passed
            explicitly by the tests: the built-in default resolves to
            ``takler.check`` in the current working directory, and a test using
            it would drop a snapshot into the repository.
        server: The live server object, available once :meth:`start` returned.
            The test body reads flow / node state straight off
            ``runner.server.bunch``, which is the same object the servicer
            mutates.
    """

    def __init__(
        self,
        checkpoint_file: Union[str, Path],
        host: str = LOCALHOST,
        port: Optional[int] = None,
        interval_main_loop: float = TEST_MAIN_LOOP_INTERVAL,
        **server_kwargs,
    ) -> None:
        self.host: str = host
        self.port: int = free_port() if port is None else int(port)
        self.checkpoint_file: Path = Path(checkpoint_file)
        self.interval_main_loop: float = interval_main_loop
        self.server_kwargs: dict = server_kwargs

        self.server: Optional[TaklerServer] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready: threading.Event = threading.Event()
        self._error: Optional[BaseException] = None

    # Lifecycle ------------------------------------------------------------

    @property
    def listen_address(self) -> str:
        """str: address the client should dial."""
        return f"{self.host}:{self.port}"

    def start(self, timeout: float = 15.0) -> "ServerRunner":
        """Start the server and return once it accepts connections."""
        self._thread = threading.Thread(
            target=self._thread_main, name="takler-test-server", daemon=True
        )
        self._thread.start()

        if not self._ready.wait(timeout):
            raise TimeoutError(
                f"takler server did not start within {timeout} seconds"
            )
        if self._error is not None:
            raise self._error
        return self

    def stop(self, timeout: float = 20.0) -> None:
        """Stop the server and join its thread; safe to call more than once."""
        if self._thread is None:
            return
        if self._loop is not None and self.server is not None and self._thread.is_alive():
            # ``stop()`` must run *on* the server's loop, hence the
            # thread-safe hand-off. ``TaklerServer._shutdown`` is idempotent,
            # so a second call is harmless.
            future = asyncio.run_coroutine_threadsafe(self.server.stop(), self._loop)
            try:
                future.result(timeout=timeout)
            except Exception:  # noqa: BLE001 - teardown must not mask a test failure
                pass
        self._thread.join(timeout=timeout)
        self._thread = None
        if self._error is not None:
            error, self._error = self._error, None
            raise error

    def make_client(self, **kwargs) -> TaklerServiceClient:
        """Build a blocking client pointed at this server."""
        kwargs.setdefault("single_timeout", TEST_SINGLE_TIMEOUT)
        kwargs.setdefault("retry_window", TEST_RETRY_WINDOW)
        return TaklerServiceClient(host=self.host, port=self.port, **kwargs)

    # Internals ------------------------------------------------------------

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._serve())
        except BaseException as exc:  # noqa: BLE001 - reported from start()/stop()
            self._error = exc
        finally:
            # Unblock ``start()`` even when startup failed, so the test fails
            # with the real error instead of a timeout.
            self._ready.set()

    async def _serve(self) -> None:
        server = TaklerServer(
            host=self.host,
            port=self.port,
            checkpoint_file=self.checkpoint_file,
            **self.server_kwargs,
        )
        server.scheduler.interval_main_loop = self.interval_main_loop
        self.server = server
        self._loop = asyncio.get_running_loop()

        await server.start()
        run_task = self._loop.create_task(server.run(), name="takler.test.server")
        # The port is bound once ``start()`` returned, so the client may dial.
        self._ready.set()
        await run_task


@pytest.fixture
def takler_server(tmp_path: Path) -> Iterator[ServerRunner]:
    """A started :class:`ServerRunner`, stopped at the end of the test."""
    runner = ServerRunner(checkpoint_file=tmp_path / "takler.check")
    runner.start()
    try:
        yield runner
    finally:
        runner.stop()
