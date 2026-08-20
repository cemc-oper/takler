"""The gRPC client used by ``takler-client-py``, the TUI and user scripts.

Every RPC goes through the Call_Wrapper :meth:`TaklerServiceClient._call`
(requirement 9.1), which owns the three cross cutting concerns that used to be
absent from the individual RPC methods:

* a per attempt deadline, so a wedged connection cannot block a job script
  forever (requirement 9.2),
* backoff retry on transport level failures until the Retry_Window is exhausted
  (requirements 9.3 - 9.6),
* mapping of gRPC status codes to takler exceptions (requirement 9.8).

Business failures are *not* retried: a response carrying a non zero ``flag`` is
handed back to the caller unchanged (requirement 9.7), and the CLI turns its
Error_Code into an exit code.

Channel lifetime is handled by :meth:`TaklerServiceClient._guarded`, which
closes the channel on the way out even when the command raises (requirements
11.4 - 11.6). Note the split between the ``xxx()`` methods, which own a channel
for the duration of one command, and the ``run_command_xxx`` /
``run_request_xxx`` methods, which assume an already established channel: the
TUI keeps one channel open across many calls and only uses the latter.

Requirements: 9.1, 9.2, 9.5, 9.6, 9.7, 9.8, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from typing import Any, Callable, List, Optional, TypeVar, Union

import grpc

from takler.client.retry import (
    DEFAULT_SINGLE_TIMEOUT,
    NON_RETRYABLE_EXCEPTION_BY_STATUS,
    RETRYABLE_STATUS_CODES,
    CommandKind,
    RetryPolicy,
    resolve_retry_window,
)
from takler.constant import DEFAULT_HOST, DEFAULT_PORT
from takler.core import Bunch
from takler.exceptions import (
    ClientConnectionError,
    ServerResponseError,
    TransportError,
)
from takler.logging import get_logger
from takler.server.protocol import takler_pb2
from takler.server.protocol.error_code import error_name_for_code
from takler.server.protocol.takler_pb2_grpc import TaklerServerStub
from takler.visitor import pre_order_travel, PrintVisitor


logger = get_logger("client")

T = TypeVar("T")

#: ``ShowResponse.output`` starting with this prefix carries an error text
#: instead of a serialized Bunch (requirement 11.1).
SHOW_ERROR_PREFIX: str = "error:"

#: How much of an unparseable ``output`` goes into the exception message
#: (requirement 11.2). Enough to identify the payload, short enough for a
#: single terminal line's worth of context.
SHOW_SNIPPET_LENGTH: int = 200


def _status_code(exc: grpc.RpcError) -> Optional[grpc.StatusCode]:
    """Return the gRPC status code of ``exc``, or ``None`` when unavailable.

    ``grpc.RpcError`` only guarantees ``code()`` on the ``Call`` flavour of the
    error, so a defensive lookup keeps a malformed error from masking the
    original failure with an ``AttributeError``.
    """
    code_getter = getattr(exc, "code", None)
    if code_getter is None:
        return None
    try:
        return code_getter()
    except Exception:  # pragma: no cover - defensive
        return None


def _status_name(code: Optional[grpc.StatusCode]) -> str:
    """Return a printable name for ``code``, tolerating ``None``."""
    return getattr(code, "name", None) or str(code)


def _status_details(exc: grpc.RpcError) -> str:
    """Return the server supplied details of ``exc``, or ``""``."""
    details_getter = getattr(exc, "details", None)
    if details_getter is None:
        return ""
    try:
        return details_getter() or ""
    except Exception:  # pragma: no cover - defensive
        return ""


class TaklerServiceClient:
    """
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
    ):
        """
        Parameters
        ----------
        host
            gRPC server host.
        port
            gRPC server port.
        single_timeout
            Per attempt deadline in seconds, handed to every RPC
            (requirement 9.2).
        retry_window
            Retry_Window in seconds. ``None`` means "resolve it from
            ``TAKLER_TIMEOUT`` and the command kind" (requirements 9.9 - 9.11).
        clock
            Monotonic time source used to measure the Retry_Window.
        sleep
            Blocking sleep used between retries. Together with ``clock`` this is
            the injection point that lets tests span a long outage instantly.
        """
        self.host: str = host
        self.port: str = str(port)
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[TaklerServerStub] = None
        self.single_timeout: float = single_timeout
        self.retry_window: Optional[float] = retry_window
        self.clock: Callable[[], float] = clock
        self.sleep: Callable[[float], None] = sleep

    def set_host_port(self, host: str, port: Union[int, str]):
        self.host = host
        self.port = str(port)

    @property
    def listen_address(self) -> str:
        """
        str: gRPC server's listen address
        """
        return f'{self.host}:{self.port}'

    def create_channel(self):
        self.channel = grpc.insecure_channel(self.listen_address)

    def close_channel(self):
        """
        Close the channel and drop the stub, at most once.

        Calling it without an established channel returns silently
        (requirement 11.4), which is what makes the ``try/finally`` in
        :meth:`_guarded` safe even when the failure happened before the channel
        existed.
        """
        if self.channel is None:
            self.stub = None
            return
        self.channel.close()
        self.channel = None
        self.stub = None

    def create_stub(self):
        self.stub = TaklerServerStub(self.channel)
        return self.stub

    def start(self):
        self.create_channel()
        self.create_stub()

    def shutdown(self):
        self.close_channel()

    # Call wrapper -------------------------------------------------

    def _retry_policy(self, kind: CommandKind) -> RetryPolicy:
        """Build the policy for one logical call of ``kind``."""
        retry_window = self.retry_window
        if retry_window is None:
            retry_window = resolve_retry_window(kind)
        return RetryPolicy(
            retry_window=retry_window,
            single_timeout=self.single_timeout,
            clock=self.clock,
            sleep=self.sleep,
        )

    def _call(
            self,
            operation_name: str,
            rpc: Callable[..., T],
            request: Any,
            kind: CommandKind,
    ) -> T:
        """Invoke ``rpc`` with timeout, retry and error mapping.

        Parameters
        ----------
        operation_name
            The command name used in log messages, for example ``complete``.
        rpc
            The stub method to call; invoked as
            ``rpc(request, timeout=single_timeout)``.
        request
            The protobuf request message.
        kind
            Command classification selecting the default Retry_Window.

        Returns
        -------
        The response returned by the stub, including responses whose ``flag``
        is non zero: a business failure is not a transport failure, so it is
        never retried and never turned into an exception here (requirement 9.7).

        Raises
        ------
        InvalidRequestError, NodeNotFoundError, PermissionDeniedError
            For status codes where retrying cannot help (requirement 9.8).
        ClientConnectionError
            When the Retry_Window is exhausted (requirement 9.5).
        TransportError
            For any other gRPC status code.
        """
        policy = self._retry_policy(kind)
        started = policy.clock()
        attempt = 0

        while True:
            attempt += 1
            try:
                return rpc(request, timeout=policy.single_timeout)
            except grpc.RpcError as exc:
                code = _status_code(exc)
                status_name = _status_name(code)
                details = _status_details(exc)

                # Non retryable status codes: the request itself is wrong, so
                # spending the retry window on it only delays the error.
                exception_type = NON_RETRYABLE_EXCEPTION_BY_STATUS.get(code)
                if exception_type is not None:
                    raise exception_type(
                        f"{operation_name} on server {self.listen_address} "
                        f"failed with gRPC status {status_name}: {details}"
                    ) from exc

                if code not in RETRYABLE_STATUS_CODES:
                    raise TransportError(
                        f"{operation_name} on server {self.listen_address} "
                        f"failed with gRPC status {status_name}: {details}"
                    ) from exc

                elapsed = policy.clock() - started
                delay = policy.next_delay(attempt, elapsed)
                if delay is None:
                    raise ClientConnectionError(
                        f"server {self.listen_address} is unreachable after "
                        f"{attempt} attempts, "
                        f"last gRPC status {status_name}"
                    ) from exc

                logger.warning(
                    f"retry {operation_name} to {self.listen_address}: "
                    f"elapsed={elapsed:.1f}s, status={status_name}"
                )
                policy.sleep(delay)

    def _guarded(self, body: Callable[[], T]) -> T:
        """Run ``body`` with an established channel, always closing it.

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
        response = self._call(
            "init",
            self.stub.RunCommandInit,
            takler_pb2.InitCommand(
                child_options=takler_pb2.ChildCommandOptions(
                    node_path=node_path,
                ),
                task_id=task_id
            ),
            CommandKind.CHILD,
        )
        self._print_response(response)
        return response

    def complete(self, node_path: str):
        return self._guarded(
            lambda: self.run_command_complete(node_path=node_path)
        )

    def run_command_complete(self, node_path: str):
        response = self._call(
            "complete",
            self.stub.RunCommandComplete,
            takler_pb2.CompleteCommand(
                child_options=takler_pb2.ChildCommandOptions(
                    node_path=node_path,
                )
            ),
            CommandKind.CHILD,
        )
        self._print_response(response)
        return response

    def abort(self, node_path: str, reason: str):
        return self._guarded(
            lambda: self.run_command_abort(node_path=node_path, reason=reason)
        )

    def run_command_abort(self, node_path: str, reason: str):
        response = self._call(
            "abort",
            self.stub.RunCommandAbort,
            takler_pb2.AbortCommand(
                child_options=takler_pb2.ChildCommandOptions(
                    node_path=node_path,
                ),
                reason=reason
            ),
            CommandKind.CHILD,
        )
        self._print_response(response)
        return response

    def event(self, node_path: str, event_name: str):
        return self._guarded(
            lambda: self.run_command_event(
                node_path=node_path, event_name=event_name
            )
        )

    def run_command_event(self, node_path: str, event_name: str):
        response = self._call(
            "event",
            self.stub.RunCommandEvent,
            takler_pb2.EventCommand(
                child_options=takler_pb2.ChildCommandOptions(
                    node_path=node_path,
                ),
                event_name=event_name,
            ),
            CommandKind.CHILD,
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
        response = self._call(
            "meter",
            self.stub.RunCommandMeter,
            takler_pb2.MeterCommand(
                child_options=takler_pb2.ChildCommandOptions(
                    node_path=node_path,
                ),
                meter_name=meter_name,
                meter_value=meter_value,
            ),
            CommandKind.CHILD,
        )
        self._print_response(response)
        return response

    # Control command ----------------------------------------------------

    def requeue(self, node_path: List[str]):
        return self._guarded(
            lambda: self.run_command_requeue(node_path=node_path)
        )

    def run_command_requeue(self, node_path: List[str]):
        response = self._call(
            "requeue",
            self.stub.RunCommandRequeue,
            takler_pb2.RequeueCommand(
                node_path=node_path
            ),
            CommandKind.CONTROL,
        )
        self._print_response(response)
        return response

    def suspend(self, node_path: List[str]):
        return self._guarded(
            lambda: self.run_command_suspend(node_path=node_path)
        )

    def run_command_suspend(self, node_path: List[str]):
        response = self._call(
            "suspend",
            self.stub.RunCommandSuspend,
            takler_pb2.SuspendCommand(
                node_path=node_path
            ),
            CommandKind.CONTROL,
        )
        self._print_response(response)
        return response

    def resume(self, node_path: List[str]):
        return self._guarded(
            lambda: self.run_command_resume(node_path=node_path)
        )

    def run_command_resume(self, node_path: List[str]):
        response = self._call(
            "resume",
            self.stub.RunCommandResume,
            takler_pb2.SuspendCommand(
                node_path=node_path
            ),
            CommandKind.CONTROL,
        )
        self._print_response(response)
        return response

    def run(self, node_path: List[str], force: bool):
        return self._guarded(
            lambda: self.run_command_run(node_path=node_path, force=force)
        )

    def run_command_run(self, node_path: List[str], force: bool):
        response = self._call(
            "run",
            self.stub.RunCommandRun,
            takler_pb2.RunCommand(
                force=force,
                node_path=node_path
            ),
            CommandKind.CONTROL,
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
        response = self._call(
            "force",
            self.stub.RunCommandForce,
            takler_pb2.ForceCommand(
                state=takler_pb2.ForceCommand.ForceState.Value(state),
                recursive=recursive,
                path=variable_paths,
            ),
            CommandKind.CONTROL,
        )
        self._print_response(response)
        return response

    def free_dep(self, node_paths: List[str], dep_type: str):
        return self._guarded(
            lambda: self.run_command_free_dep(
                node_paths=node_paths, dep_type=dep_type
            )
        )

    def run_command_free_dep(self, node_paths: List[str], dep_type: str):
        response = self._call(
            "free-dep",
            self.stub.RunCommandFreeDep,
            takler_pb2.FreeDepCommand(
                dep_type=takler_pb2.FreeDepCommand.DepType.Value(dep_type),
                path=node_paths,
            ),
            CommandKind.CONTROL,
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
        response = self._call(
            "load",
            self.stub.RunCommandLoad,
            takler_pb2.LoadCommand(
                flow_type=flow_type,
                flow=flow_bytes
            ),
            CommandKind.CONTROL,
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
        response = self._call(
            "begin",
            self.stub.RunCommandBegin,
            takler_pb2.BeginCommand(
                flow_name=flow_name,
                force=force,
            ),
            CommandKind.CONTROL,
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
            show_meter: bool
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
        response = self._call(
            "show",
            self.stub.RunRequestShow,
            takler_pb2.ShowRequest(
                show_trigger=show_trigger,
                show_parameter=show_parameter,
                show_limit=show_limit,
                show_event=show_event,
                show_meter=show_meter,
            ),
            CommandKind.QUERY,
        )

        output = response.output
        if output.startswith(SHOW_ERROR_PREFIX):
            raise ServerResponseError(
                f"server returned an error for show: {output}"
            )

        try:
            bunch_dict = json.loads(output)
        except json.JSONDecodeError as exc:
            raise ServerResponseError(
                f"show response is not valid json: "
                f"{output[:SHOW_SNIPPET_LENGTH]}"
            ) from exc

        bunch = Bunch.from_dict(bunch_dict)
        for name, flow in bunch.flows.items():
            pre_order_travel(flow, PrintVisitor(
                stream=sys.stdout,
                show_parameter=show_parameter,
                show_trigger=show_trigger,
                show_limit=show_limit,
                show_event=show_event,
                show_meter=show_meter,
            ))
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
        return self._call(
            "ping",
            self.stub.RunRequestPing,
            takler_pb2.PingRequest(),
            CommandKind.QUERY,
        )

    def coroutine(self):
        return self._guarded(self.run_query_coroutine)

    def run_query_coroutine(self):
        response = self._call(
            "coroutine",
            self.stub.QueryCoroutine,
            takler_pb2.CoroutineRequest(),
            CommandKind.QUERY,
        )

        for task in response.coroutines:
            print(f"{task.name}\t{task.description}")
        return response
