from typing import Callable, Optional
import asyncio
import inspect

import grpc

from takler.server.protocol import takler_pb2, takler_pb2_grpc
from takler.server.protocol.error_code import error_code_for_exception
from takler.logging import get_logger
from takler.server.scheduler import Scheduler
from takler.server.connect_config import ExceptionPolicy, DEFAULT_EXCEPTION_POLICY


logger = get_logger("server.service")


def _command_error_response(exc: Exception) -> "takler_pb2.ServiceResponse":
    """Build the error representation of a command handler's response.

    RESILIENT command handlers convert a caught scheduler-operation exception
    into a ``ServiceResponse`` with a non-zero ``flag`` and a descriptive
    ``message`` (Requirement 2.3), reusing the existing response type without
    touching ``takler.proto``.

    ``flag`` carries the Error_Code that classifies ``exc``: an
    Exception_Hierarchy type maps to its dedicated non-zero code, a
    ``TaklerError`` without a dedicated code maps to the generic code 1, and
    anything else maps to the internal-server-error code (Requirements 3.4,
    3.5). Success responses keep ``flag=0`` (Requirement 3.3), and failures stay
    non-zero for clients that only test ``flag != 0``.
    """
    return takler_pb2.ServiceResponse(
        flag=error_code_for_exception(exc),
        message=f"{type(exc).__name__}: {exc}",
    )


class TaklerService(takler_pb2_grpc.TaklerServerServicer):
    """
    RPC 服务端，响应客户端命令。

    Attributes
    ----------
    scheduler : Scheduler
        A link to the scheduler. Service use it to run commands.
    host : str
        Service host
    port : int
        Service port
    """

    def __init__(
        self,
        scheduler: Scheduler,
        host: str = None,
        port: int = None,
        exception_policy: Optional[ExceptionPolicy] = None,
        fatal_shutdown: Optional[Callable[[], None]] = None,
    ):
        self.scheduler: Scheduler = scheduler
        if host is None:
            host = "[::]"
        if port is None:
            port = 33083
        self.host: str = host
        self.port: int = port
        self.grpc_server: Optional[grpc.aio.Server] = None
        # Exception-handling policy and fatal-shutdown trigger are threaded in
        # from ``TaklerServer`` (task 3.2). The RPC command/query handlers
        # consult them through :meth:`_handle_command` (task 3.4): in RESILIENT
        # mode a caught exception is logged and converted into an error response
        # of the handler's existing response type; in FAIL_FAST mode it is
        # logged and then the shared fatal-shutdown trigger is fired so the
        # server exits through its unified clean-shutdown path.
        self.exception_policy: ExceptionPolicy = (
            exception_policy
            if exception_policy is not None
            else DEFAULT_EXCEPTION_POLICY
        )
        self.fatal_shutdown: Optional[Callable[[], None]] = fatal_shutdown

    @property
    def listen_address(self) -> str:
        """
        str: gRPC server's listen address
        """
        return f"{self.host}:{self.port}"

    async def start(self):
        """
        Start gRPC server.
        """
        self.grpc_server = grpc.aio.server()
        takler_pb2_grpc.add_TaklerServerServicer_to_server(self, self.grpc_server)
        self.grpc_server.add_insecure_port(self.listen_address)
        await self.grpc_server.start()
        logger.info(f"service started: {self.listen_address}")

    async def run(self):
        """
        Wait until gRPC server is terminated.
        """
        await self.grpc_server.wait_for_termination()

    async def stop(self):
        """
        Stop gRPC server with time limit.
        """
        logger.info("service shutting down..")
        await self.grpc_server.stop(5)
        logger.info("service shutting down..done")

    # Exception boundary -------------------------------------------------

    def _trigger_fatal_shutdown(self):
        """Invoke the fatal-shutdown trigger if one was provided.

        In ``FAIL_FAST`` mode the service asks the owning ``TaklerServer`` to
        exit through its unified clean-shutdown path. If no trigger was wired in
        (e.g. the service is used standalone in tests), this is a no-op.
        """
        if self.fatal_shutdown is not None:
            self.fatal_shutdown()

    async def _handle_command(
        self,
        operation_name: str,
        request_info: str,
        op: Callable,
        error_response: Optional[Callable[[Exception], object]] = None,
    ):
        """Run a handler body behind the RPC exception boundary.

        This is the unified wrapping helper for every ``RunCommand*`` /
        ``RunRequest*`` / ``QueryCoroutine`` handler. It wraps the part of the
        handler that calls a scheduler operation so each handler avoids
        repeating the same try/except boilerplate.

        On the success path (no exception) ``op`` runs to completion and its
        result -- the handler's normal success response -- is returned
        unchanged, so ``flag=0`` command responses and the query success
        responses are preserved (Requirements 3.2, 3.3).

        On an unexpected exception the failure is always logged with enough
        context to diagnose it -- the operation name, key request fields and the
        exception type/message with a traceback (Requirement 2.7). Then, by
        policy:

        * ``RESILIENT`` (default): return the error representation of the
          handler's response type (a non-zero ``flag`` ``ServiceResponse`` for
          command handlers, or the error/empty representation of the query
          response type), keeping the server running (Requirement 2.3).
        * ``FAIL_FAST``: fire the shared fatal-shutdown trigger so the server
          exits through its unified clean-shutdown path; an error response is
          still returned to the client before the server shuts down
          (Requirement 2.5).

        Parameters
        ----------
        operation_name
            RPC method name, used for diagnostic context.
        request_info
            Key request fields (e.g. node path), used for diagnostic context.
        op
            Zero-argument callable running the scheduler operation and building
            the success response. May be a coroutine function / return an
            awaitable.
        error_response
            Callable mapping the caught exception to the error representation of
            this handler's response type. Defaults to the command-style
            non-zero ``flag`` ``ServiceResponse``.
        """
        if error_response is None:
            error_response = _command_error_response
        try:
            result = op()
            if inspect.isawaitable(result):
                result = await result
            return result
        except Exception as exc:  # noqa: BLE001 - boundary is intentional
            logger.error(
                f"error handling RPC {operation_name} ({request_info}): "
                f"{type(exc).__name__}: {exc}",
                exc_info=True,
            )
            if self.exception_policy is ExceptionPolicy.FAIL_FAST:
                logger.critical(
                    f"fail-fast policy active after RPC {operation_name} "
                    f"failure; shutting server down"
                )
                self._trigger_fatal_shutdown()
            return error_response(exc)

    # Child command -----------------------------------------------------

    async def RunCommandInit(self, request, context):
        node_path = request.child_options.node_path
        task_id = request.task_id

        async def op():
            logger.info(f"Init: {node_path} with {task_id}")
            await self.scheduler.run_command_init(node_path, task_id)
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandInit", f"node_path={node_path}, task_id={task_id}", op
        )

    async def RunCommandComplete(self, request, context):
        node_path = request.child_options.node_path

        def op():
            logger.info(f"Complete: {node_path}")
            self.scheduler.run_command_complete(node_path)
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandComplete", f"node_path={node_path}", op
        )

    async def RunCommandAbort(self, request: takler_pb2.AbortCommand, context):
        node_path = request.child_options.node_path
        reason = request.reason

        def op():
            logger.info(f"Abort: {node_path}")
            self.scheduler.run_command_abort(node_path, reason=reason)
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandAbort", f"node_path={node_path}", op
        )

    async def RunCommandEvent(self, request, context):
        node_path = request.child_options.node_path
        event_name = request.event_name

        def op():
            logger.info(f"Event set: {node_path}:{event_name}")
            self.scheduler.run_command_event(node_path, event_name)
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandEvent", f"node_path={node_path}, event_name={event_name}", op
        )

    async def RunCommandMeter(self, request: takler_pb2.MeterCommand, context):
        node_path = request.child_options.node_path
        meter = request.meter_name
        value = request.meter_value

        def op():
            logger.info(f"Meter set: {node_path}:{meter} {value}")
            self.scheduler.run_command_meter(node_path, meter, value)
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandMeter",
            f"node_path={node_path}, meter={meter}, value={value}",
            op,
        )

    # Control command -------------------------------------------------------------

    async def RunCommandRequeue(self, request: takler_pb2.RequeueCommand, context):
        node_path_list = request.node_path

        def op():
            for node_path in node_path_list:
                logger.info(f"Requeue: {node_path}")
                self.scheduler.run_command_requeue(node_path)
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandRequeue", f"node_path={list(node_path_list)}", op
        )

    async def RunCommandSuspend(self, request: takler_pb2.SuspendCommand, context):
        node_paths = request.node_path

        def op():
            for node_path in node_paths:
                logger.info(f"Suspend: {node_path}")
                self.scheduler.run_command_suspend(node_path)
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandSuspend", f"node_path={list(node_paths)}", op
        )

    async def RunCommandResume(self, request: takler_pb2.SuspendCommand, context):
        node_paths = request.node_path

        def op():
            for node_path in node_paths:
                logger.info(f"Resume: {node_path}")
                self.scheduler.run_command_resume(node_path)
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandResume", f"node_path={list(node_paths)}", op
        )

    async def RunCommandRun(self, request, context):
        node_paths = request.node_path
        force = request.force

        def op():
            for node_path in node_paths:
                result = self.scheduler.run_command_run(node_path, force=force)
                if result:
                    logger.info(f"Run: {node_path}")
                else:
                    logger.info(f"Run has error: {node_path}")
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandRun", f"node_path={list(node_paths)}, force={force}", op
        )

    async def RunCommandForce(self, request: takler_pb2.ForceCommand, context):
        paths = request.path
        state = takler_pb2.ForceCommand.ForceState.Name(request.state)
        recursive = request.recursive

        def op():
            for variable_path in paths:
                result = self.scheduler.run_command_force(
                    variable_path, state=state, recursive=recursive
                )
                if result:
                    logger.info(f"Force: {variable_path} {state}")
                else:
                    logger.info(f"Force has error: {variable_path} {state}")
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandForce",
            f"path={list(paths)}, state={state}, recursive={recursive}",
            op,
        )

    async def RunCommandFreeDep(self, request: takler_pb2.FreeDepCommand, context):
        paths = request.path
        dep_type = takler_pb2.FreeDepCommand.DepType.Name(request.dep_type)

        def op():
            for path in paths:
                self.scheduler.run_command_free_dep(path, dep_type)
                logger.info(f"Free Dep: {dep_type} {path}")
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandFreeDep", f"path={list(paths)}, dep_type={dep_type}", op
        )

    async def RunCommandBegin(self, request: takler_pb2.BeginCommand, context):
        flow_name = request.flow_name
        force = request.force

        def op():
            logger.info(f"Begin: {flow_name} force={force}")
            self.scheduler.run_command_begin(flow_name, force=force)
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandBegin", f"flow_name={flow_name}, force={force}", op
        )

    async def RunCommandLoad(self, request: takler_pb2.LoadCommand, context):
        flow_type = request.flow_type
        flow_bytes = request.flow

        def op():
            logger.info("Load flow from bytes...")
            self.scheduler.run_command_load(flow_type=flow_type, flow_bytes=flow_bytes)
            return takler_pb2.ServiceResponse(flag=0, message="")

        return await self._handle_command(
            "RunCommandLoad", f"flow_type={flow_type}", op
        )

    # Query command -----------------------------------------------------

    async def RunRequestShow(self, request: takler_pb2.ShowRequest, context):
        def op():
            output = self.scheduler.handle_request_show(
                show_parameter=request.show_parameter,
                show_trigger=request.show_trigger,
                show_limit=request.show_limit,
                show_event=request.show_event,
                show_meter=request.show_meter,
            )
            return takler_pb2.ShowResponse(output=output)

        # Query handlers reuse their existing response type for errors: write
        # the error into ``ShowResponse.output`` (no new protocol fields).
        return await self._handle_command(
            "RunRequestShow",
            "show",
            op,
            error_response=lambda exc: takler_pb2.ShowResponse(
                output=f"error: {type(exc).__name__}: {exc}"
            ),
        )

    async def RunRequestPing(self, request, context):
        def op():
            return takler_pb2.PingResponse()

        return await self._handle_command(
            "RunRequestPing",
            "ping",
            op,
            error_response=lambda exc: takler_pb2.PingResponse(),
        )

    async def QueryCoroutine(self, request, context):
        def op():
            loop = asyncio.get_running_loop()
            tasks = []
            for t in asyncio.all_tasks(loop=loop):
                task = takler_pb2.Coroutine(
                    name=t.get_name(),
                    description=repr(t.get_coro()),
                )
                tasks.append(task)
            return takler_pb2.CoroutineResponse(coroutines=tasks)

        return await self._handle_command(
            "QueryCoroutine",
            "coroutine",
            op,
            error_response=lambda exc: takler_pb2.CoroutineResponse(),
        )
