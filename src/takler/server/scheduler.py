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
from takler.protocol.commands import (
    AbortCommand,
    BeginCommand,
    CompleteCommand,
    EventCommand,
    ForceCommand,
    FreeDepCommand,
    InitCommand,
    LoadCommand,
    MeterCommand,
    RequeueCommand,
    ResumeCommand,
    RunCommand,
    ShowRequest,
    SuspendCommand,
)
from takler.server.connect_config import ExceptionPolicy, DEFAULT_EXCEPTION_POLICY
from takler.server.zombie import ChildAction, ZombieDetector


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
    zombie_detector : Optional[ZombieDetector]
        judges every Child_Command against the run instance the server records
        for the target task. ``None`` disables the judgement, which is the
        M1 behaviour and what a directly driven scheduler gets.
    """

    def __init__(
        self,
        bunch: Bunch,
        interval_main_loop: float = DEFAULT_INTERVAL_LOOP_SECONDS,
        exception_policy: Optional[ExceptionPolicy] = None,
        fatal_shutdown: Optional[Callable[[], None]] = None,
        zombie_detector: Optional[ZombieDetector] = None,
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
            exception_policy
            if exception_policy is not None
            else DEFAULT_EXCEPTION_POLICY
        )
        self.fatal_shutdown: Optional[Callable[[], None]] = fatal_shutdown
        # Zombie detection is a server-level feature: ``TaklerServer`` builds the
        # detector from the resolved Auth_Mode and Zombie_Policy and passes it
        # in. ``None`` means "no policy configured", which is the case for a
        # scheduler driven directly -- by the TUI, by a unit test, or by an
        # in-process run -- and leaves every Child_Command with its M1
        # behaviour. See :meth:`_guard_child_command`.
        self.zombie_detector: Optional[ZombieDetector] = zombie_detector

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
                    # ``CommandHandlers._handle_command``.
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
                logger.warning(
                    f"elapse time ({elapsed:.2f}) seconds is larger than main loop interval ({self.interval_main_loop} seconds)"
                )
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
            raise FlowStateError(f"flow is not begun: {flow.name}", flow_name=flow.name)

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

    def _guard_child_command(
        self, node: Task, command: str, task_id: Optional[str] = None
    ) -> ChildAction:
        """Judge a Child_Command against the target task's run instance.

        This is the single zombie decision point of the scheduler. Each of the
        five Child_Commands calls it on one line, **after** the node is located
        and type-checked and **before** anything is written to the node
        (Requirement 9.1), so "no state is changed by a rejected command" holds
        by construction rather than by rollback.

        Five explicit call sites rather than a decorator: ``init`` has to pass
        its ``task_id`` for the ``Z3`` check, and the "judge before writing"
        ordering is worth being able to read off the method body.

        Control_Commands and Query_Commands do not call this at all
        (Requirement 9.10): a zombie is a job instance that disagrees with the
        server's record, and an operator command has no job instance.

        A command that hits no Zombie_Condition is answered with
        :attr:`~takler.server.zombie.ChildAction.PROCEED` and leaves the node --
        including its Job_Password -- untouched (Requirements 10.11, 10.12).

        Parameters
        ----------
        node
            the target task, already located and type-checked. A missing node or
            a non-task node is not a zombie: it keeps the M1 outcome of the
            command in question (Requirement 9.9).
        command
            short name of the Child_Command: ``init``, ``complete``, ``abort``,
            ``event`` or ``meter``.
        task_id
            the job id an ``init`` carries; unused by the other four.

        Returns
        -------
        ChildAction
            ``PROCEED`` to run the command, ``SKIP`` to return success without
            running it (the ``fob`` policy).

        Raises
        ------
        ZombieError
            A Zombie_Condition was hit and the Zombie_Policy is ``fail``.
        """
        if self.zombie_detector is None:
            return ChildAction.PROCEED
        return self.zombie_detector.guard(node, command, task_id)

    def _find_node_or_raise(self, node_path: str) -> Node:
        """Locate ``node_path`` in the bunch or raise ``NodeNotFoundError``."""
        node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(
                f"node is not found: {node_path}", node_path=node_path
            )
        return node

    def _find_task_or_raise(self, node_path: str) -> Task:
        """Locate ``node_path`` and require it to be a ``Task``."""
        node = self._find_node_or_raise(node_path)
        if not isinstance(node, Task):
            raise NodeTypeError(f"node must be Task: {node_path}", node_path=node_path)
        return node

    # Child command -------------------------------------------------

    async def run_command_init(self, command: InitCommand):
        """
        Init the ``Task`` node, call child method ``init``.

        Parameters
        ----------
        command
            The ``init`` DTO: the node path of a task, starting with "/", such
            as /flow1/container1/task1, and the task id to set into parameter
            ``TAKLER_RID``.

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        NodeTypeError
            If node is not a ``Task``.
        ZombieError
            If the command hits a Zombie_Condition and the Zombie_Policy is
            ``fail`` (see :meth:`_guard_child_command`).
        """
        logger.info(f"Init: {command.node_path} with {command.task_id}")
        node = self._find_task_or_raise(command.node_path)

        if self._guard_child_command(node, "init", command.task_id) is ChildAction.SKIP:
            # fob: answer success without touching the node.
            return

        node.init(command.task_id)

    def run_command_complete(self, command: CompleteCommand):
        """
        Set the node to complete status, call child method ``complete``.

        Parameters
        ----------
        command
            The ``complete`` DTO: the node path of a task, starting with "/".

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        NodeTypeError
            If node is not a ``Task``.
        ZombieError
            If the command hits a Zombie_Condition and the Zombie_Policy is
            ``fail`` (see :meth:`_guard_child_command`).
        """
        logger.info(f"Complete: {command.node_path}")
        node = self._find_task_or_raise(command.node_path)

        if self._guard_child_command(node, "complete") is ChildAction.SKIP:
            # fob: answer success without touching the node.
            return

        node.complete()

    def run_command_abort(self, command: AbortCommand):
        """
        Set task to aborted status with aborted reason

        Parameters
        ----------
        command
            The ``abort`` DTO: the node path of a task and the reason it is
            aborted with.

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        NodeTypeError
            If node is not a ``Task``.
        ZombieError
            If the command hits a Zombie_Condition and the Zombie_Policy is
            ``fail`` (see :meth:`_guard_child_command`).
        """
        logger.info(f"Abort: {command.node_path}")
        node = self._find_task_or_raise(command.node_path)

        if self._guard_child_command(node, "abort") is ChildAction.SKIP:
            # fob: answer success without touching the node.
            return

        node.abort(command.reason)

    def run_command_event(self, command: EventCommand):
        """
        Set the event in a node, call child method ``set_event``.

        Parameters
        ----------
        command
            The ``event`` DTO: the node path of the node owning the event and
            the event name.

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        ZombieError
            If the target is a ``Task``, the command hits a Zombie_Condition and
            the Zombie_Policy is ``fail`` (see :meth:`_guard_child_command`).
        """
        logger.info(f"Event set: {command.node_path}:{command.event_name}")
        node = self._find_node_or_raise(command.node_path)

        # A non-task target keeps its M1 behaviour: ``event`` accepts any node
        # which owns the event, and such a node has no job instance to judge
        # (Requirement 9.9).
        if isinstance(node, Task):
            if self._guard_child_command(node, "event") is ChildAction.SKIP:
                # fob: answer success without touching the node.
                return

        node.set_event(command.event_name, True)

    def run_command_meter(self, command: MeterCommand):
        """
        Change meter value, call child method ``meter``.

        Parameters
        ----------
        command
            The ``meter`` DTO: the node path of the node owning the meter, the
            meter name and the value. The value arrives as an ``int`` -- the
            wire carries a string and the DTO owns the conversion.

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        ZombieError
            If the target is a ``Task``, the command hits a Zombie_Condition and
            the Zombie_Policy is ``fail`` (see :meth:`_guard_child_command`).
        """
        logger.info(
            f"Meter set: {command.node_path}:{command.meter_name} {command.meter_value}"
        )
        node = self._find_node_or_raise(command.node_path)

        # Same as ``event``: a non-task target has no job instance, so it keeps
        # its M1 behaviour (Requirement 9.9).
        if isinstance(node, Task):
            if self._guard_child_command(node, "meter") is ChildAction.SKIP:
                # fob: answer success without touching the node.
                return

        node.set_meter(command.meter_name, command.meter_value)

    # Control -------------------------------------------------

    def run_command_requeue(self, command: RequeueCommand):
        """
        Requeue the nodes.

        Parameters
        ----------
        command
            The ``requeue`` DTO: the node paths to requeue, in order.

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        FlowStateError
            If the node belongs to a flow which has not begun (Requirement 8.10).
        """
        for node_path in command.node_paths:
            logger.info(f"Requeue: {node_path}")
            node = self._find_node_or_raise(node_path)
            self._require_begun(node)
            node.requeue()

    def run_command_suspend(self, command: SuspendCommand):
        """
        Suspend the nodes.

        Parameters
        ----------
        command
            The ``suspend`` DTO: the node paths to suspend, in order.

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        """
        for node_path in command.node_paths:
            logger.info(f"Suspend: {node_path}")
            node = self._find_node_or_raise(node_path)
            node.suspend()

    def run_command_resume(self, command: ResumeCommand):
        """
        Resume the nodes from suspended status.

        Parameters
        ----------
        command
            The ``resume`` DTO: the node paths to resume, in order.

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        """
        for node_path in command.node_paths:
            logger.info(f"Resume: {node_path}")
            node = self._find_node_or_raise(node_path)
            node.resume()

    def run_command_run(self, command: RunCommand):
        """
        Run each ``Task`` node of the command.

        A task is run when it is not in submitted or active status; with
        ``force`` set it runs regardless of its status.

        Parameters
        ----------
        command
            The ``run`` DTO: the node paths of the tasks and the force flag.

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        FlowStateError
            If the node belongs to a flow which has not begun (Requirement 8.10).
        """
        for node_path in command.node_paths:
            if self._run_node(node_path, force=command.force):
                logger.info(f"Run: {node_path}")
            else:
                logger.info(f"Run has error: {node_path}")

    def _run_node(self, node_path: str, force: bool = False) -> bool:
        """Run one task node; return True if the task's run method was called."""
        node = self.bunch.find_node(node_path)
        if node is None:
            raise NodeNotFoundError(
                f"node is not found: {node_path}", node_path=node_path
            )

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

    def run_command_force(self, command: ForceCommand):
        """
        Force each target of the command to the command's state.

        Parameters
        ----------
        command
            The ``force`` DTO: the target paths (a node path, or an event path
            of the form ``/flow/task:event``), the state to impose and whether
            a node target's descendants follow.

        Raises
        ------
        NodeNotFoundError
            If a target path is not found.
        UnsupportedValueError
            If state is not a ``NodeStatus`` name for a node, or is not `set`
            or `clear` for an ``Event``.
        FlowStateError
            If the host node belongs to a flow which has not begun
            (Requirement 8.10).
        """
        state = command.state.value
        for variable_path in command.paths:
            if self._force_path(
                variable_path, state=state, recursive=command.recursive
            ):
                logger.info(f"Force: {variable_path} {state}")
            else:
                logger.info(f"Force has error: {variable_path} {state}")

    def _force_path(self, variable_path: str, state: str, recursive: bool) -> bool:
        """Force one node or event path to ``state``.

        For a node: set its status, and its descendants' when ``recursive`` is
        set. For an event: set (``set``) or unset (``clear``) it.
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

    def run_command_free_dep(self, command: FreeDepCommand):
        """
        Free dependencies of the nodes.

        Parameters
        ----------
        command
            The ``free-dep`` DTO: the node paths and the dependency class to
            clear (see ``Node.free_dependencies``).

        Raises
        ------
        NodeNotFoundError
            If node is not found.
        FlowStateError
            If the node belongs to a flow which has not begun (Requirement 8.10).
        """
        dep_type = command.dep_type.value
        for node_path in command.paths:
            node = self._find_node_or_raise(node_path)
            self._require_begun(node)
            node.free_dependencies(dep_type)
            logger.info(f"Free Dep: {dep_type} {node_path}")

    def run_command_load(self, command: LoadCommand):
        """
        Load a new flow into bunch from string bytes.

        Parameters
        ----------
        command
            The ``load`` DTO: the flow type (only ``"json"`` is supported) and
            the serialized flow definition.

        Raises
        ------
        UnsupportedValueError
            If ``flow_type`` is not supported.
        InvalidRequestError
            If the flow definition is not valid json.
        """
        logger.info("Load flow from bytes...")
        if command.flow_type == "json":
            logger.info("load json flow...")
            try:
                flow_dict = json.loads(command.flow_bytes)
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
            logger.warning(
                f"flow type {command.flow_type} is not supported for command load."
            )
            raise UnsupportedValueError(
                f"flow type {command.flow_type} is not supported for command load.",
                value=command.flow_type,
            )

    def run_command_begin(self, command: BeginCommand):
        """
        Begin one flow, or all flows in bunch.

        Beginning a flow starts its calendar with the current time, resets its node
        tree and marks it as begun. Only a begun flow is processed by the main loop
        (see :meth:`_process_flow`).

        Parameters
        ----------
        command
            The ``begin`` DTO: the name of the flow to begin -- an empty string
            means all flows in bunch (Requirement 8.1) -- and the force flag,
            which begins again a flow which has already begun.

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
        logger.info(f"Begin: {command.flow_name} force={command.force}")
        if command.flow_name:
            flow = self.bunch.find_flow(command.flow_name)
            if flow is None:
                raise NodeNotFoundError(
                    f"flow is not found: {command.flow_name}",
                    node_path=f"/{command.flow_name}",
                )
            flows = [flow]
        else:
            flows = list(self.bunch.flows.values())

        if not command.force:
            # Check every flow first so the command either begins all its
            # targets or changes nothing at all.
            for flow in flows:
                if flow.begun:
                    raise FlowStateError(
                        f"flow is already begun: {flow.name}", flow_name=flow.name
                    )

        for flow in flows:
            logger.info(f"begin flow [flow name: {flow.name}, force: {command.force}]")
            flow.begin(force=command.force)

    # Query -------------------------------------------------

    def handle_request_show(self, request: ShowRequest) -> str:
        """Serialize the bunch; the flags select the detail sections."""
        bunch_dict = self.bunch.to_dict()
        bunch_json_str = json.dumps(bunch_dict)

        return bunch_json_str
