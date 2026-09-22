"""The client used by ``takler-client-py``, the TUI and user scripts.

``TaklerServiceClient`` is the command surface: one method per command, the
console printing of a command's outcome and the ``show`` response parsing.
Everything the wire imposes -- the connection lifecycle, the request/response
encoding, the Credential_Metadata, the retry loop with its per-attempt
deadline and status-code mapping -- lives in the
:class:`~takler.client.transport.ClientTransport` the client delegates to
(requirement 9.1), a :class:`~takler.client.grpc_transport.GrpcTransport`
unless another transport is injected. The command methods therefore speak
DTOs and payload dicts only; they never touch a generated class.

Note the split between the ``xxx()`` methods, which own a connection for the
duration of one command (:meth:`TaklerServiceClient._guarded`, requirements
11.4 - 11.6), and the ``run_command_xxx`` / ``run_request_xxx`` methods,
which assume an already established one: the TUI keeps one connection open
across many calls and only uses the latter.

Business failures are not retried and not raised: a response carrying a non
zero ``flag`` is handed back to the caller unchanged (requirement 9.7), and
the CLI turns its Error_Code into an exit code.

Requirements: 9.1, 9.5, 9.6, 9.7, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6,
2.1, 2.2, 2.4, 2.6, 2.8, 8.1, 8.2, 8.3, 8.4, 8.5, 8.7, 8.8, 8.9.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from typing import Callable, List, Optional, TypeVar, Union

from takler.client.credentials import (
    resolve_ca_file,
    resolve_secret_file,
    resolve_server_name,
)
from takler.client.grpc_transport import (
    SSL_TARGET_NAME_OVERRIDE_OPTION,
    build_channel_credentials,
)
from takler.client.retry import DEFAULT_SINGLE_TIMEOUT
from takler.client.transport import (
    ClientTransport,
    build_client_transport,
    resolve_transport,
)
from takler.constant import DEFAULT_HOST, DEFAULT_PORT
from takler.core import Bunch
from takler.exceptions import ServerResponseError
from takler.protocol.commands import Command
from takler.protocol.error_code import error_name_for_code
from takler.server.connect_config import ConnectConfig
from takler.visitor import pre_order_travel, PrintVisitor

__all__ = [
    "SSL_TARGET_NAME_OVERRIDE_OPTION",
    "TaklerServiceClient",
    "build_channel_credentials",
]


T = TypeVar("T")

#: ``ShowResponse.output`` starting with this prefix carries an error text
#: instead of a serialized Bunch (requirement 11.1).
SHOW_ERROR_PREFIX: str = "error:"

#: How much of an unparseable ``output`` goes into the exception message
#: (requirement 11.2). Enough to identify the payload, short enough for a
#: single terminal line's worth of context.
SHOW_SNIPPET_LENGTH: int = 200


class TaklerServiceClient:
    """The command surface of the takler client.

    Notes
    -----
    If HPC login node's name is used, should set an environment to use native DNS resolver.

        export GRPC_DNS_RESOLVER=native

    Or use GOLANG version client.
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: Union[int, str] = DEFAULT_PORT,
        single_timeout: float = DEFAULT_SINGLE_TIMEOUT,
        retry_window: Optional[float] = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        ca_file: Optional[str] = None,
        server_name: Optional[str] = None,
        secret_file: Optional[str] = None,
        connect_config: Optional[ConnectConfig] = None,
        transport: Optional[ClientTransport] = None,
        transport_name: Optional[str] = None,
    ):
        """
        Parameters
        ----------
        host
            Server host.
        port
            Server port.
        single_timeout
            Per attempt deadline in seconds, handed to every call
            (requirement 9.2).
        retry_window
            Retry_Window in seconds. ``None`` means "resolve it from
            ``TAKLER_TIMEOUT`` and the command kind" (requirements 9.9 - 9.11).
        clock
            Monotonic time source used to measure the Retry_Window.
        sleep
            Blocking sleep used between retries. Together with ``clock`` this is
            the injection point that lets tests span a long outage instantly.
        ca_file
            CA certificate file the client trusts, as the highest precedence
            source of :func:`~takler.client.credentials.resolve_ca_file`
            (m2 requirement 2.3). ``None`` falls through to
            ``TAKLER_TLS_CA_FILE``, then to ``connect_config``, then to "no CA",
            which means an unencrypted channel (m2 requirement 2.2).
        server_name
            Host name the server certificate is verified against, as the highest
            precedence source of
            :func:`~takler.client.credentials.resolve_server_name`
            (m2 requirement 2.5).
        secret_file
            Operator_Secret_File the client reads its ``takler-secret`` from, as
            the highest precedence source of
            :func:`~takler.client.credentials.resolve_secret_file`
            (m2 requirement 8.6). ``None`` falls through to
            ``TAKLER_SECRET_FILE``, then to ``connect_config``, then to "no
            shared secret", which means Operator_Commands carry no
            ``takler-secret`` and the server decides whether to refuse them
            (m2 requirement 8.7).
        connect_config
            A loaded Connect_Config whose ``security`` section supplies the
            third precedence level of all three values, or ``None`` when no
            config file is in play. Passed in rather than loaded here, so that
            the address, the TLS knobs and the secret file path come from the
            same parse of the same file.
        transport
            The transport to call through. ``None`` builds the transport
            selected by :func:`~takler.client.transport.resolve_transport`
            from ``transport_name``, ``connect_config`` and the environment
            -- a :class:`~takler.client.grpc_transport.GrpcTransport` unless
            ``http`` is selected (M3 task 8, ``takler[http]``). Injectable so
            a test can drive the command surface without a wire.
        transport_name
            The highest precedence source of the transport selection
            (``"grpc"`` or ``"http"``); ``None`` falls through to the
            ``transport`` field of the Connect_Config ``server`` section,
            then to ``TAKLER_TRANSPORT``, then to gRPC. Ignored when
            ``transport`` is injected.
        """
        if transport is None:
            # Both resolutions read the environment and the config, neither
            # of which changes during one command, so they happen once here
            # rather than per channel or per call.
            transport = build_client_transport(
                resolve_transport(transport_name, connect_config),
                host=host,
                port=port,
                single_timeout=single_timeout,
                retry_window=retry_window,
                clock=clock,
                sleep=sleep,
                ca_file=resolve_ca_file(ca_file, connect_config),
                server_name=resolve_server_name(server_name, connect_config),
                secret_file=resolve_secret_file(secret_file, connect_config),
            )
        self.transport: ClientTransport = transport

    # Address and TLS knobs delegate to the transport, so the long-standing
    # attribute surface stays valid -- and stays writable, which is what the
    # TUI and the tests use -- no matter which transport sits behind it.

    @property
    def host(self) -> str:
        return self.transport.host

    @host.setter
    def host(self, value: str) -> None:
        self.transport.host = value

    @property
    def port(self) -> str:
        return self.transport.port

    @port.setter
    def port(self, value: Union[int, str]) -> None:
        self.transport.port = str(value)

    def set_host_port(self, host: str, port: Union[int, str]):
        self.host = host
        self.port = port

    @property
    def listen_address(self) -> str:
        """
        str: server's listen address
        """
        return self.transport.listen_address

    @property
    def ca_file(self) -> Optional[str]:
        return self.transport.ca_file

    @ca_file.setter
    def ca_file(self, value: Optional[str]) -> None:
        self.transport.ca_file = value

    @property
    def server_name(self) -> Optional[str]:
        return self.transport.server_name

    @server_name.setter
    def server_name(self, value: Optional[str]) -> None:
        self.transport.server_name = value

    @property
    def secret_file(self) -> Optional[str]:
        return self.transport.secret_file

    @secret_file.setter
    def secret_file(self, value: Optional[str]) -> None:
        self.transport.secret_file = value

    @property
    def channel(self):
        """The gRPC channel of the transport, when it has one.

        A gRPC-specific escape hatch: the TUI drives the stub directly for its
        polling loop, which predates the transport abstraction and is folded
        into it with the TUI's own transport migration. ``None`` when no
        channel is open or the transport keeps none.
        """
        return getattr(self.transport, "channel", None)

    @channel.setter
    def channel(self, value) -> None:
        self.transport.channel = value

    @property
    def stub(self):
        """The gRPC stub of the transport, when it has one.

        Same escape hatch as :attr:`channel`.
        """
        return getattr(self.transport, "stub", None)

    @stub.setter
    def stub(self, value) -> None:
        self.transport.stub = value

    # Connection lifecycle -----------------------------------------------

    def create_channel(self):
        """Open the transport's channel. See ``GrpcTransport.create_channel``."""
        self.transport.create_channel()

    def create_stub(self):
        return self.transport.create_stub()

    def start(self):
        """Open the connection."""
        self.transport.open()

    def shutdown(self):
        self.transport.close()

    def close_channel(self):
        """
        Close the connection, at most once.

        Calling it without an established channel returns silently
        (requirement 11.4), which is what makes the ``try/finally`` in
        :meth:`_guarded` safe even when the failure happened before the channel
        existed.
        """
        self.transport.close()

    def _guarded(self, body: Callable[[], T]) -> T:
        """Run ``body`` with an established connection, always closing it.

        The ``finally`` is what requirement 11.6 asks for: an exception raised
        by the command reaches the caller only after the channel has been
        closed, so a failing command cannot leak a channel.
        """
        self.start()
        try:
            return body()
        finally:
            self.close_channel()

    @staticmethod
    def _print_response(response) -> None:
        """Print the response's Error_Code classification name.

        The raw ``flag`` integer used to be printed, which told the operator
        nothing; the classification name is readable both for humans and for
        scripts that grep the output.
        """
        print(f"received: {error_name_for_code(response.flag)}")

    # Child command -------------------------------------------------

    def init(self, node_path: str, task_id: str):
        return self._guarded(
            lambda: self.run_command_init(node_path=node_path, task_id=task_id)
        )

    def run_command_init(self, node_path: str, task_id: str):
        response = self.transport.call(
            Command.INIT,
            {"node_path": node_path, "task_id": task_id},
        )
        self._print_response(response)
        return response

    def complete(self, node_path: str):
        return self._guarded(lambda: self.run_command_complete(node_path=node_path))

    def run_command_complete(self, node_path: str):
        response = self.transport.call(
            Command.COMPLETE,
            {"node_path": node_path},
        )
        self._print_response(response)
        return response

    def abort(self, node_path: str, reason: str):
        return self._guarded(
            lambda: self.run_command_abort(node_path=node_path, reason=reason)
        )

    def run_command_abort(self, node_path: str, reason: str):
        response = self.transport.call(
            Command.ABORT,
            {"node_path": node_path, "reason": reason},
        )
        self._print_response(response)
        return response

    def event(self, node_path: str, event_name: str):
        return self._guarded(
            lambda: self.run_command_event(node_path=node_path, event_name=event_name)
        )

    def run_command_event(self, node_path: str, event_name: str):
        response = self.transport.call(
            Command.EVENT,
            {"node_path": node_path, "event_name": event_name},
        )
        self._print_response(response)
        return response

    def meter(self, node_path: str, meter_name: str, meter_value: str):
        return self._guarded(
            lambda: self.run_command_meter(
                node_path=node_path,
                meter_name=meter_name,
                meter_value=meter_value,
            )
        )

    def run_command_meter(self, node_path: str, meter_name: str, meter_value: str):
        response = self.transport.call(
            Command.METER,
            {
                "node_path": node_path,
                "meter_name": meter_name,
                "meter_value": meter_value,
            },
        )
        self._print_response(response)
        return response

    # Control command ----------------------------------------------------

    def requeue(self, node_path: List[str]):
        return self._guarded(lambda: self.run_command_requeue(node_path=node_path))

    def run_command_requeue(self, node_path: List[str]):
        response = self.transport.call(
            Command.REQUEUE,
            {"node_paths": list(node_path)},
        )
        self._print_response(response)
        return response

    def suspend(self, node_path: List[str]):
        return self._guarded(lambda: self.run_command_suspend(node_path=node_path))

    def run_command_suspend(self, node_path: List[str]):
        response = self.transport.call(
            Command.SUSPEND,
            {"node_paths": list(node_path)},
        )
        self._print_response(response)
        return response

    def resume(self, node_path: List[str]):
        return self._guarded(lambda: self.run_command_resume(node_path=node_path))

    def run_command_resume(self, node_path: List[str]):
        response = self.transport.call(
            Command.RESUME,
            {"node_paths": list(node_path)},
        )
        self._print_response(response)
        return response

    def run(self, node_path: List[str], force: bool):
        return self._guarded(
            lambda: self.run_command_run(node_path=node_path, force=force)
        )

    def run_command_run(self, node_path: List[str], force: bool):
        response = self.transport.call(
            Command.RUN,
            {"node_paths": list(node_path), "force": force},
        )
        self._print_response(response)
        return response

    def force(self, variable_paths: List[str], state: str, recursive: bool):
        return self._guarded(
            lambda: self.run_command_force(
                variable_paths=variable_paths,
                state=state,
                recursive=recursive,
            )
        )

    def run_command_force(self, variable_paths: List[str], state: str, recursive: bool):
        response = self.transport.call(
            Command.FORCE,
            {
                "paths": list(variable_paths),
                "state": state,
                "recursive": recursive,
            },
        )
        self._print_response(response)
        return response

    def free_dep(self, node_paths: List[str], dep_type: str):
        return self._guarded(
            lambda: self.run_command_free_dep(node_paths=node_paths, dep_type=dep_type)
        )

    def run_command_free_dep(self, node_paths: List[str], dep_type: str):
        response = self.transport.call(
            Command.FREE_DEP,
            {"paths": list(node_paths), "dep_type": dep_type},
        )
        self._print_response(response)
        return response

    def load(self, flow_file_path: str):
        return self._guarded(
            lambda: self.run_command_load(flow_file_path=flow_file_path)
        )

    def run_command_load(self, flow_file_path: str):
        flow_type = "json"
        with open(flow_file_path, "rb") as f:
            flow_bytes = f.read()
        response = self.transport.call(
            Command.LOAD,
            {"flow_type": flow_type, "flow_bytes": flow_bytes},
        )
        self._print_response(response)
        return response

    def begin(self, flow_name: str = "", force: bool = False):
        return self._guarded(
            lambda: self.run_command_begin(flow_name=flow_name, force=force)
        )

    def run_command_begin(self, flow_name: str = "", force: bool = False):
        """Start the given flow, or every flow when ``flow_name`` is empty.

        An empty ``flow_name`` is the wire form of "all flows" (requirement
        8.1), so it is also the default here: a script that wants to begin the
        whole bunch does not have to spell the empty string out.
        """
        response = self.transport.call(
            Command.BEGIN,
            {"flow_name": flow_name, "force": force},
        )
        self._print_response(response)
        return response

    # Query command ----------------------------------------------------

    def show(
        self,
        show_parameter: bool = False,
        show_trigger: bool = True,
        show_limit: bool = True,
        show_event: bool = True,
        show_meter: bool = True,
    ):
        return self._guarded(
            lambda: self.run_request_show(
                show_trigger=show_trigger,
                show_parameter=show_parameter,
                show_limit=show_limit,
                show_event=show_event,
                show_meter=show_meter,
            )
        )

    def run_request_show(
        self,
        show_trigger: bool,
        show_parameter: bool,
        show_limit: bool,
        show_event: bool,
        show_meter: bool,
    ):
        """
        Print the server's bunch tree.

        Raises
        ------
        ServerResponseError
            When ``output`` carries an error text (requirement 11.1) or is not
            valid JSON (requirement 11.2). Both used to surface as a
            ``json.JSONDecodeError`` traceback, which told the operator nothing
            about the server having refused the request.
        """
        response = self.transport.call(
            Command.SHOW,
            {
                "show_trigger": show_trigger,
                "show_parameter": show_parameter,
                "show_limit": show_limit,
                "show_event": show_event,
                "show_meter": show_meter,
            },
        )

        output = response.output
        if output.startswith(SHOW_ERROR_PREFIX):
            raise ServerResponseError(f"server returned an error for show: {output}")

        try:
            bunch_dict = json.loads(output)
        except json.JSONDecodeError as exc:
            raise ServerResponseError(
                f"show response is not valid json: {output[:SHOW_SNIPPET_LENGTH]}"
            ) from exc

        bunch = Bunch.from_dict(bunch_dict)
        for name, flow in bunch.flows.items():
            pre_order_travel(
                flow,
                PrintVisitor(
                    stream=sys.stdout,
                    show_parameter=show_parameter,
                    show_trigger=show_trigger,
                    show_limit=show_limit,
                    show_event=show_event,
                    show_meter=show_meter,
                ),
            )
        return response

    def ping(self):
        start_time = datetime.now()

        def body():
            response = self.run_request_ping()
            end_time = datetime.now()
            print(
                f"ping server ({self.listen_address}) succeeded "
                f"in {end_time - start_time}."
            )
            return response

        return self._guarded(body)

    def run_request_ping(self):
        return self.transport.call(Command.PING, {})

    def coroutine(self):
        return self._guarded(self.run_query_coroutine)

    def run_query_coroutine(self):
        response = self.transport.call(Command.COROUTINE, {})

        for task in response.coroutines:
            print(f"{task.name}\t{task.description}")
        return response
