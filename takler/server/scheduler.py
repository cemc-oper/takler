import asyncio
import time
import datetime
import json
from queue import Queue
from typing import Callable, Optional

from takler.core import Bunch, Task, NodeStatus, Event, Flow, SerializationType
from takler.core.node import Node
from takler.exceptions import (
    FlowStateError,
    InvalidRequestError,
    NodeNotFoundError,
    NodeTypeError,
    UnsupportedValueError,
)
from takler.logging import get_logger
from takler.server.connect_config import ExceptionPolicy, DEFAULT_EXCEPTION_POLICY


logger = get_logger("server.scheduler")


DEFAULT_INTERVAL_LOOP_SECONDS = 10.0


class Scheduler:
    """
    定时调度器，定时遍历所有 Flow，运行满足依赖条件的任务，同时还负责执行 Flow 操作。

    Attributes
    ----------
    bunch : Bunch
        Scheduler has only one bunch.
    interval_main_loop : float
        time interval to check flow dependencies, unit is seconds.
    """
    def __init__(
        self,
        bunch: Bunch,
        interval_main_loop: float = DEFAULT_INTERVAL_LOOP_SECONDS,
        exception_policy: Optional[ExceptionPolicy] = None,
        fatal_shutdown: Optional[Callable[[], None]] = None,
    ):
        self.bunch: Bunch = bunch
        self.interval_main_loop: float = interval_main_loop
        self.command_queue: Queue = Queue()
        self.should_stop: bool = False
        # Exception-handling policy and fatal-shutdown trigger are threaded in
        # from ``TaklerServer`` (task 3.2). They are stored here so the main
        # loop can consult them once the per-flow exception boundary lands in
        # task 3.3; for now they are kept for forward compatibility and do not
        # alter behaviour.
        self.exception_policy: ExceptionPolicy = (
            exception_policy if exception_policy is not None else DEFAULT_EXCEPTION_POLICY
        )
        self.fatal_shutdown: Optional[Callable[[], None]] = fatal_shutdown

    async def start(self):
        pass

    async def run(self):
        """
        Start main loop.
        """
        await self.main_loop()
        await self.shutdown()

    async def shutdown(self):
        """
        Called after main loop is done, unset ``should_stop`` flag.
        """
        self.should_stop = False

    async def main_loop(self):
        """
        Main loop of scheduler.

        Travel bunch until ``should_stop`` flag is set.
        """
        while not self.should_stop:
            # logger.debug("main loop...")
            start_time = time.time()

            # Process every flow behind its own exception boundary so that one
            # flow's failure cannot abort the whole iteration (or, in
            # ``FAIL_FAST`` mode, terminate the process without a clean
            # shutdown). ``dict`` is snapshotted with ``list(...)`` so a flow
            # mutation mid-iteration cannot raise ``RuntimeError``.
            time_now = datetime.datetime.now()
            fatal = False
            for name, flow in list(self.bunch.flows.items()):
                try:
                    self._process_flow(name, flow, time_now)
                except Exception as exc:  # noqa: BLE001 - boundary is intentional
                    # Unified diagnostic log (task 3.5): always record the
                    # operation identifier (flow name) plus the error detail
                    # (exception type + message) with a traceback, before any
                    # policy-specific action, regardless of the current policy
                    # (Requirement 2.7). This mirrors the RPC boundary in
                    # ``TaklerService._handle_command``.
                    logger.error(
                        f"unexpected exception while processing flow {name!r}: "
                        f"{type(exc).__name__}: {exc}",
                        exc_info=True,
                    )
                    if self.exception_policy is ExceptionPolicy.FAIL_FAST:
                        # FAIL_FAST: after logging the detail, note the
                        # policy-driven shutdown and trigger the unified clean
                        # exit path.
                        logger.critical(
                            f"fail-fast policy active after flow {name!r} "
                            f"failure; shutting server down"
                        )
                        self._trigger_fatal_shutdown()
                        fatal = True
                        break
                    else:
                        # RESILIENT (default): skip only this flow, keeping the
                        # loop running for the rest.
                        continue

            if fatal:
                # Fail-fast triggered: leave the main loop so the server can go
                # through its unified clean-shutdown path.
                break

            elapsed = time.time() - start_time
            if elapsed > self.interval_main_loop:
                logger.warning(f"elapse time ({elapsed:.2f}) seconds is larger than main loop interval ({self.interval_main_loop} seconds)")
                duration = 0
            else:
                duration = self.interval_main_loop - elapsed

            await asyncio.sleep(duration)

    async def stop(self):
        """
        Stop scheduler by set ``should_stop`` flag and wait until main loop unset ``should_stop`` flag

        This method should only be called once.
        """
        logger.info("scheduler shutting down...")
        self.should_stop = True

        while self.should_stop:
            await asyncio.sleep(0.1)
        logger.info("scheduler shutting down...done")

    def _trigger_fatal_shutdown(self):
        """Invoke the fatal-shutdown trigger if one was provided.

        In ``FAIL_FAST`` mode the scheduler asks the owning ``TaklerServer`` to
        exit through its unified clean-shutdown path. If no trigger was wired in
        (e.g. the scheduler is used standalone in tests), this is a no-op beyond
        the caller leaving the main loop.
        """
        if self.fatal_shutdown is not None:
            self.fatal_shutdown()

    def _process_flow(self, name: str, flow: Flow, time_now: datetime.datetime):
        """Process a single flow: update its calendar and resolve dependencies.

        This encapsulates the per-flow work that was previously spread across
        the ``update_calendar`` loop and :meth:`travel_bunch` in ``main_loop``.
        On the success path (no exception) it behaves identically to calling
        ``flow.update_calendar(time_now)`` followed by
        ``flow.resolve_dependencies()`` for that flow, keeping the two in
        lock-step (Requirement 3.1).

        Parameters
        ----------
        name
            flow name, used for diagnostic context by the caller.
        flow
            the flow to process.
        time_now
            current time used to update the flow's calendar.
        """
        if not flow.begun:
            # Requirement 8.9: an unbegun flow is skipped entirely, both its
            # calendar update and its dependency resolution.
            #
            # This is required, not merely an optimization: the calendar of a
            # flow which has not begun has all its fields set to ``None``, so
            # ``Calendar.update`` would raise ``TypeError`` on
            # ``self.flow_time += self.increment``. Since ``run_command_load``
            # no longer requeues the flow (Requirement 8.8), "in bunch but not
            # begun" is a regular state (between ``load`` and ``begin``) rather
            # than an edge case.
            return
        flow.update_calendar(time_now)
        flow.resolve_dependencies()

    def _require_begun(self, node: Optional[Node]):
        """Reject control operations on a node whose flow has not begun.

        The main loop skips un-begun flows (see :meth:`_process_flow`), so
        rewriting the status of their nodes would never be resolved: a silent
        no-op. This guard turns that into an explicit error (Requirement 8.10).

        It must be called **after the node is located and before any status is
        written**, which makes "the node and all its descendants keep their
        status" hold by construction, without rollback logic.

        A node whose root is not a ``Flow`` (``get_flow()`` returns ``None``, a
        bare node tree) is not guarded.

        Parameters
        ----------
        node
            the node the operation targets. ``None`` is a no-op, so callers may
            pass an optional lookup result.

        Raises
        ------
        FlowStateError
            If the node belongs to a flow which has not begun.
        """
        if node is None:
            return
        flow = node.get_flow()
        if flow is not None and not flow.begun:
            raise FlowStateError(
                f"flow is not begun: {flow.name}", flow_name=flow.name
            )

    def travel_bunch(self):
        """
        Travel all flows in bunch to resolve dependencies.

        This function will submit tasks which fit its dependencies.

        Notes
        -----
        是否使用异步函数遍历工作流？
        """
        for name, flow in self.bunch.flows.items():
            flow.resolve_dependencies()

    # Child command -------------------------------------------------

    async def run_command_init(self, node_path: str, task_id: str):
        """
        Init the ``Task`` node, call child method ``init``.

        Parameters
        ----------
        node_path
            node path string of a task, starting with "/", such as /flow1/container1/task1.
        task_id
            An ID to identify the task, will be set into parameter ``TAKLER_RID``.

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        NodeTypeError
            If node is not a ``Task``.

        Notes
        -----
        是否使用异步函数执行客户端命令？
        """
        node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(f"node is not found: {node_path}", node_path=node_path)

        if isinstance(node, Task):
            node.init(task_id)
        else:
            raise NodeTypeError(f"node must be Task: {node_path}", node_path=node_path)

    def run_command_complete(self, node_path: str):
        """
        Set the node to complete status, call child method ``complete``.

        Parameters
        ----------
        node_path
            node path string of a task, staring with "/"

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        NodeTypeError
            If node is not a ``Task``.

        Returns
        -------

        """
        node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(f"node is not found: {node_path}", node_path=node_path)

        if isinstance(node, Task):
            node.complete()
        else:
            raise NodeTypeError(f"node must be Task: {node_path}", node_path=node_path)

    def run_command_abort(self, node_path: str, reason: Optional[str] = None):
        """
        Set task to aborted status with aborted reason

        Parameters
        ----------
        node_path
            node path string of a task, staring with "/"

        reason
            describe why task is aborted.
        Raises
        ------
        NodeNotFoundError
            If node is not found.
        NodeTypeError
            If node is not a ``Task``.

        Returns
        -------

        """
        node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(f"node is not found: {node_path}", node_path=node_path)

        if isinstance(node, Task):
            node.abort(reason)
        else:
            raise NodeTypeError(f"node must be Task: {node_path}", node_path=node_path)

    def run_command_event(self, node_path: str, event_name: str):
        """
        Set the event in a node, call child method ``set_event``.

        Parameters
        ----------
        node_path
            node path string of a task, staring with "/"
        event_name
            event name

        Returns
        -------

        """
        node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(f"node is not found: {node_path}", node_path=node_path)

        node.set_event(event_name, True)

    def run_command_meter(self, node_path: str, meter_name: str, meter_value: str):
        """
        Change meter value, call child method ``meter``.

        Parameters
        ----------
        node_path
            node path string of a task, staring with "/"
        meter_name
            meter name
        meter_value
            meter value

        Returns
        -------

        """
        node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(f"node is not found: {node_path}", node_path=node_path)

        node.set_meter(meter_name, int(meter_value))

    # Control -------------------------------------------------

    def run_command_requeue(self, node_path: str):
        """
        Requeue the node.

        Parameters
        ----------
        node_path
            node path string.

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        FlowStateError
            If the node belongs to a flow which has not begun (Requirement 8.10).

        Returns
        -------

        """
        node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(f"node is not found: {node_path}", node_path=node_path)

        self._require_begun(node)

        node.requeue()

    def run_command_suspend(self, node_path: str):
        """
        Suspend a node.

        Parameters
        ----------
        node_path
            node path string.

        Raises
        ------
        NodeNotFoundError
            If node is not found.

        Returns
        -------

        """
        node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(f"node is not found: {node_path}", node_path=node_path)

        node.suspend()

    def run_command_resume(self, node_path: str):
        """
        Resume a node from suspended status.

        Parameters
        ----------
        node_path
            node path string.

        Raises
        ------
        NodeNotFoundError
            If node is not found.

        Returns
        -------

        """
        node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(f"node is not found: {node_path}", node_path=node_path)

        node.resume()

    def run_command_run(self, node_path: str, force: bool = False) -> bool:
        """
        Run the ``Task`` node when task node is not in submitted or active status.
        If force is set, run the task regardless of task status.

        Parameters
        ----------
        node_path
            node path string of a task.
        force
            run in force mode.

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        FlowStateError
            If the node belongs to a flow which has not begun (Requirement 8.10).

        Returns
        -------
        bool
            return True if call task's run method.
        """
        node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(f"node is not found: {node_path}", node_path=node_path)

        self._require_begun(node)

        if not isinstance(node, Task):
            logger.warning(f"node path is not a Task: {node_path}")
            return False
        if not force:
            status = node.state.node_status
            if status in (NodeStatus.submitted, NodeStatus.active):
                # don't run
                return False

        node.run()
        return True

    def run_command_force(self, variable_path: str, state: str, recursive: bool = False) -> bool:
        """
        Force node or event to some state.

        For node:

        Set node status to some state. If recursive is set, set all its children node also.

        For event:

        Set (``set``) or unset (``clear``) event.

        Parameters
        ----------
        variable_path
            Path for a ``Node`` or an ``Event``.
        state
            ``NodeState`` string if ``variable_path`` is a node, "clear" or "set" if event
        recursive
            If ``variable_path`` is a node, set state for the node and all its descendant nodes.


        Raises
        ------
        NodeNotFoundError
            If variable path is not found.
        UnsupportedValueError
            If state is not a ``NodeStatus`` name for a node, or is not `set`
            or `clear` for an ``Event``.
        FlowStateError
            If the host node belongs to a flow which has not begun
            (Requirement 8.10).

        Returns
        -------
        bool
        """
        variable = self.bunch.find_path(variable_path)
        if variable is None:
            raise NodeNotFoundError(
                f"path is not found: {variable_path}", node_path=variable_path
            )

        # ``variable_path`` may point at an event or a meter, which carry no
        # back-reference to their owner, so the host node is resolved from the
        # node part of the path and the guard is applied to it.
        if isinstance(variable, Node):
            host_node = variable
        else:
            host_node = self.bunch.find_node(variable_path.split(":")[0])
        self._require_begun(host_node)

        if isinstance(variable, Node):
            try:
                node_status = NodeStatus[state]
            except KeyError as exc:
                raise UnsupportedValueError(
                    f"state {state} is not supported for Node: {variable_path}",
                    value=state,
                ) from exc
            if recursive:
                variable.sink_status_change(node_status)
            else:
                variable.set_node_status(node_status)
            return True
        elif isinstance(variable, Event):
            if state == "set":
                variable.value = True
            elif state == "clear":
                variable.value = False
            else:
                raise UnsupportedValueError(
                    f"state {state} is not supported for Event", value=state
                )
            return True
        return True

    def run_command_free_dep(self, node_path: str, dep_type: str):
        """
        Free dependencies of the node.

        Parameters
        ----------
        node_path
        dep_type
            sell ``Node.free_dependencies``

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        FlowStateError
            If the node belongs to a flow which has not begun (Requirement 8.10).

        Returns
        -------

        """
        node: Node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(f"node is not found: {node_path}", node_path=node_path)

        self._require_begun(node)

        node.free_dependencies(dep_type)

    def run_command_load(self, flow_type: str, flow_bytes: bytes):
        """
        Load a new flow into bunch from string bytes.

        Parameters
        ----------
        flow_type
            type of flow, support:

                * json: json string

        flow_bytes
            string bytes of flow's definition.

        Raises
        ------
        UnsupportedValueError
            If ``flow_type`` is not supported.
        InvalidRequestError
            If the flow definition is not valid json.

        Returns
        -------
        None
        """
        if flow_type == "json":
            logger.info("load json flow...")
            try:
                flow_dict = json.loads(flow_bytes)
            except json.JSONDecodeError as exc:
                raise InvalidRequestError(
                    f"flow definition is not valid json: {exc}"
                ) from exc
            flow: Flow = Flow.from_dict(d=flow_dict, method=SerializationType.Tree)
            self.bunch.add_flow(flow)
            # The flow is deliberately left un-begun (Requirement 8.8): loading
            # only registers the definition, ``run_command_begin`` starts it.
            logger.info(f"load json flow...done [flow name: {flow.name}]")
        else:
            logger.warning(f"flow type {flow_type} is not supported for command load.")
            raise UnsupportedValueError(
                f"flow type {flow_type} is not supported for command load.",
                value=flow_type,
            )

    def run_command_begin(self, flow_name: Optional[str] = None, force: bool = False):
        """
        Begin one flow, or all flows in bunch.

        Beginning a flow starts its calendar with the current time, resets its node
        tree and marks it as begun. Only a begun flow is processed by the main loop
        (see :meth:`_process_flow`).

        Parameters
        ----------
        flow_name
            name of the flow to begin. ``None`` or an empty string means all flows
            in bunch (Requirement 8.1).
        force
            begin again a flow which has already begun.

        Raises
        ------
        NodeNotFoundError
            If ``flow_name`` is given but is not a flow of the bunch (Requirement 8.13).
        FlowStateError
            If any target flow has already begun and ``force`` is not set
            (Requirement 8.11).

        Notes
        -----
        The "all flows" form is all-or-nothing: every target flow is checked
        before any of them is begun, so a single already-begun flow makes the
        whole command fail without changing any node status (Requirement 8.11
        requires the node status of the offending flow to be unchanged, and
        failing atomically avoids leaving the bunch half begun). Use ``force``
        to (re)begin flows regardless of their current begun state
        (Requirement 8.12).
        """
        if flow_name:
            flow = self.bunch.find_flow(flow_name)
            if flow is None:
                raise NodeNotFoundError(
                    f"flow is not found: {flow_name}", node_path=f"/{flow_name}"
                )
            flows = [flow]
        else:
            flows = list(self.bunch.flows.values())

        if not force:
            # Check every flow first so the command either begins all its
            # targets or changes nothing at all.
            for flow in flows:
                if flow.begun:
                    raise FlowStateError(
                        f"flow is already begun: {flow.name}", flow_name=flow.name
                    )

        for flow in flows:
            logger.info(f"begin flow [flow name: {flow.name}, force: {force}]")
            flow.begin(force=force)

    # Query -------------------------------------------------

    def handle_request_show(
            self,
            show_parameter: bool,
            show_trigger: bool,
            show_limit: bool,
            show_event: bool,
            show_meter: bool,
    ) -> str:
        bunch_dict = self.bunch.to_dict()
        bunch_json_str = json.dumps(bunch_dict)

        return bunch_json_str
