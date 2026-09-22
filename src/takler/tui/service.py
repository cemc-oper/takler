"""Thin wrapper around :class:`~takler.client.TaklerServiceClient`.

The CLI service client prints to stdout and creates / closes a connection on
every call. The TUI wants the raw payload (for the show response) and a
single long-lived connection; this module provides both.

Everything on the wire goes through the client's
:class:`~takler.client.transport.ClientTransport` abstraction (M3 task 8):
the TUI never touches a stub, a generated class or a protocol detail, so the
transport the client was built with -- gRPC by default, HTTP when the
Connect_Config or ``TAKLER_TRANSPORT`` selects it -- carries its calls
without the TUI knowing which.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Tuple, Union

from takler.client.service_client import TaklerServiceClient
from takler.logging import get_logger
from takler.protocol.commands import Command
from takler.server.connect_config import ConnectConfig


logger = get_logger("tui.service")


class TaklerTuiService:
    """A reusable service client that returns structured payloads.

    The connection is opened once on first use and reused until
    ``close()`` is called or the object is garbage collected.
    """

    def __init__(
        self,
        host: str,
        port: Union[int, str],
        transport_name: Optional[str] = None,
        connect_config: Optional[ConnectConfig] = None,
    ):
        self._inner = TaklerServiceClient(
            host=host,
            port=port,
            connect_config=connect_config,
            transport_name=transport_name,
        )
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
        response = self._inner.transport.call(
            Command.SHOW,
            {
                "show_trigger": show_trigger,
                "show_parameter": show_parameter,
                "show_limit": show_limit,
                "show_event": show_event,
                "show_meter": show_meter,
            },
        )
        return response.output

    def ping(self) -> Tuple[bool, str]:
        """Return ``(ok, message)``.

        Tries hard not to raise so callers can render a status bar.
        """
        try:
            start = datetime.now()
            self._ensure_open()
            self._inner.transport.call(Command.PING, {})
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
