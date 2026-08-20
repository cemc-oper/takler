"""Thin wrapper around :class:`TaklerServiceClient`.

The CLI service client prints to stdout and creates / closes a gRPC
channel on every call. The TUI wants the raw payload (for the show
response) and a single long-lived channel; this module provides both.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Tuple, Union

from takler.client.service_client import TaklerServiceClient
from takler.server.protocol import takler_pb2
from takler.logging import get_logger


logger = get_logger("tui.service")


class TaklerTuiService:
    """A reusable gRPC client that returns structured payloads.

    The connection is opened once on first use and reused until
    :meth:`close` is called or the object is garbage collected.
    """

    def __init__(self, host: str, port: Union[int, str]):
        self._inner = TaklerServiceClient(host=host, port=port)
        self._connected = False

    @property
    def host(self) -> str:
        return self._inner.host

    @property
    def port(self) -> str:
        return self._inner.port

    @property
    def listen_address(self) -> str:
        return self._inner.listen_address

    def _ensure_open(self) -> None:
        if not self._connected:
            self._inner.start()
            self._connected = True

    def close(self) -> None:
        if self._connected:
            try:
                self._inner.shutdown()
            finally:
                self._connected = False

    def __enter__(self) -> "TaklerTuiService":
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # -- Queries -----------------------------------------------------

    def show(
        self,
        show_parameter: bool = True,
        show_trigger: bool = True,
        show_limit: bool = True,
        show_event: bool = True,
        show_meter: bool = True,
    ) -> str:
        """Return the raw ``show`` text payload."""
        self._ensure_open()
        response = self._inner.stub.RunRequestShow(
            takler_pb2.ShowRequest(
                show_trigger=show_trigger,
                show_parameter=show_parameter,
                show_limit=show_limit,
                show_event=show_event,
                show_meter=show_meter,
            )
        )
        return response.output

    def ping(self) -> Tuple[bool, str]:
        """Return ``(ok, message)``.

        Tries hard not to raise so callers can render a status bar.
        """
        try:
            start = datetime.now()
            self._ensure_open()
            self._inner.stub.RunRequestPing(takler_pb2.PingRequest())
            elapsed = datetime.now() - start
            return True, f"pong in {elapsed}"
        except Exception as exc:  # pragma: no cover - network errors
            self._connected = False
            return False, f"ping failed: {exc}"

    # -- Control commands -------------------------------------------

    def requeue(self, paths: List[str]) -> None:
        self._ensure_open()
        self._inner.run_command_requeue(node_path=paths)

    def suspend(self, paths: List[str]) -> None:
        self._ensure_open()
        self._inner.run_command_suspend(node_path=paths)

    def resume(self, paths: List[str]) -> None:
        self._ensure_open()
        self._inner.run_command_resume(node_path=paths)

    def run(self, paths: List[str], force: bool = False) -> None:
        self._ensure_open()
        self._inner.run_command_run(node_path=paths, force=force)

    def force_state(
        self, paths: List[str], state: str, recursive: bool = False
    ) -> None:
        self._ensure_open()
        self._inner.run_command_force(
            variable_paths=paths, state=state, recursive=recursive
        )

    def free_dep(self, paths: List[str], dep_type: str = "all") -> None:
        self._ensure_open()
        self._inner.run_command_free_dep(node_paths=paths, dep_type=dep_type)
