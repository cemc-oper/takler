import functools
import inspect
import secrets
from datetime import datetime, timezone
from typing import Optional, Dict, Set
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from .node import Node
from .state import NodeStatus
from takler.exceptions import FlowStateError, UnsupportedValueError
from .limit import Limit
from .parameter import (
    Parameter,
    TASK,
    TAKLER_NAME,
    TAKLER_RID,
    TAKLER_ATTEMPT_ID,
    TAKLER_TRY_NO,
    TAKLER_PASS,
)
from .util import logger, SerializationType


_EXECUTION_HOOKS = ("run", "before_run", "do_run", "after_run")


def _synchronous_result(result):
    """Close an unexpected coroutine so a sync hook cannot appear successful."""
    if inspect.isawaitable(result):
        close = getattr(result, "close", None)
        if callable(close):
            close()
        raise UnsupportedValueError(
            "asynchronous task execution is not supported", value="async_task"
        )
    return result


def ensure_synchronous_task(task_or_type) -> None:
    """Reject coroutine execution hooks before scheduling can change state."""
    for name in _EXECUTION_HOOKS:
        hook = getattr(task_or_type, name, None)
        if inspect.iscoroutinefunction(hook) or inspect.iscoroutinefunction(
            getattr(hook, "__call__", None)
        ):
            raise UnsupportedValueError(
                "asynchronous task execution is not supported", value="async_task"
            )


def ensure_synchronous_tasks(root) -> None:
    """Validate an existing tree before starting scheduler or transport."""
    from .bunch import Bunch

    pending = [root]
    while pending:
        node = pending.pop()
        if isinstance(node, Task):
            ensure_synchronous_task(node)
        pending.extend(node.children)
        if isinstance(node, Bunch):
            pending.extend(node.flows.values())


class Task(Node):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        ensure_synchronous_task(cls)

    def __init__(self, name: str):
        ensure_synchronous_task(self)
        super(Task, self).__init__(name)
        self.task_id: Optional[str] = None

        self.aborted_reason: Optional[str] = None

        self.try_no: int = 0
        self.definition_generation: Optional[str] = None
        self.attempt_id: Optional[str] = None
        self.submitted_at: Optional[str] = None
        self.started_at: Optional[str] = None
        self.ended_at: Optional[str] = None
        self.job_path: Optional[str] = None
        self.jobout_path: Optional[str] = None
        self.orvix_info_path: Optional[str] = None
        self._run_identity_prepared = False

        # One-time job password of the current run instance.
        #
        # It is deliberately **not** part of :meth:`to_dict`: node
        # serialization feeds both the ``show`` response and the checkpoint
        # file, so serializing it here would hand every in-flight password to
        # anyone able to call ``show``.
        #
        # A new attempt gets its own password. Requeue and force-queued clear
        # it; try_no may remain nonzero after force-queued.
        self.job_password: Optional[str] = None

        self.generated_parameters: TaskNodeGeneratedParameters = (
            TaskNodeGeneratedParameters(node=self)
        )

    def __repr__(self):
        return f"Task {self.name}"

    # Serialization ----------------------------------------------

    def to_dict(self) -> Dict:
        result = super().to_dict()
        result.update(
            dict(
                task_id=self.task_id,
                aborted_reason=self.aborted_reason,
                try_no=self.try_no,
            )
        )
        for field in (
            "definition_generation",
            "attempt_id",
            "submitted_at",
            "started_at",
            "ended_at",
            "job_path",
            "jobout_path",
            "orvix_info_path",
        ):
            value = getattr(self, field)
            if value is not None:
                result[field] = value

        return result

    @classmethod
    def fill_from_dict(
        cls, d: Dict, node: "Task", method: SerializationType = SerializationType.Status
    ) -> "Task":
        Node.fill_from_dict(d=d, node=node, method=method)

        # Tree builds a fresh definition: do not even read old run identity.
        # Status requires these fields so incomplete runtime is not silently
        # replaced by definition defaults. Passwords are restored separately
        # by CheckpointManager, never from the node dictionary in either mode.
        if method == SerializationType.Status:
            node.task_id = d["task_id"]
            node.aborted_reason = d["aborted_reason"]
            node.try_no = d["try_no"]
            for field in (
                "definition_generation",
                "attempt_id",
                "submitted_at",
                "started_at",
                "ended_at",
                "job_path",
                "jobout_path",
                "orvix_info_path",
            ):
                setattr(node, field, d.get(field))

        return node

    # State management --------------------------------------------

    def computed_status(self, immediate: bool) -> NodeStatus:
        """
        The status of a task is its own status.
        """
        return self.state.node_status

    # def swim_status_change(self):
    #     """
    #     Task node use its own status, and needn't compute status again.
    #     So swim status change from its parent.
    #     """
    #     return self.parent.swim_status_change()

    def swim_status_change_only(self):
        if self.parent:
            self.parent.swim_status_change_only()

    def sink_status_change(self, node_status: NodeStatus):
        self.set_node_status(node_status)

    def handle_status_change(self):
        self.update_limits()

        self.swim_status_change()

        super(Task, self).handle_status_change()

    # Limit -------------------------------------------------------

    def update_limits(self):
        """
        Update limits according task status, only used when task status is changed.
        """
        status = self.state.node_status
        limit_set: Set[Limit] = set()
        if status == NodeStatus.complete:
            self.decrement_in_limit(limit_set)
        elif status == NodeStatus.aborted:
            self.decrement_in_limit(limit_set)
        elif status in (NodeStatus.submitted, NodeStatus.active):
            self.increment_in_limit(limit_set)
        else:
            self.decrement_in_limit(limit_set)

    # Trigger -----------------------------------------------------
    def check_dependencies(self) -> bool:
        # check node status
        node_status = self.state.node_status
        if node_status in (
            NodeStatus.complete,
            NodeStatus.active,
            NodeStatus.submitted,
            NodeStatus.unknown,
        ):
            return False

        # resolve node dependencies
        if not Node.check_dependencies(self):
            return False

        # don't run when task is aborted. See :github:issue:`27`
        if node_status == NodeStatus.aborted:
            return False

        missing_limits = self.unresolved_limits_up()
        if missing_limits:
            logger.debug("limit dependency blocked: " + "; ".join(missing_limits))
            return False
        if not self.check_in_limit_up():
            return False

        return True

    def resolve_dependencies(self) -> bool:
        """
        Check dependencies and run the task if all dependencies are met.

        Returns
        -------
        bool
            True if all dependencies are met and the task is run, False otherwise.
        """
        ensure_synchronous_task(self)
        if self.check_dependencies():
            # run jobs
            self.run()
            return True
        else:
            return False

    # Parameter ---------------------------------------------------

    def find_generated_parameter(self, name: str) -> Optional[Parameter]:
        param = self.generated_parameters.find_parameter(name)
        if param is not None:
            return param

        return super(Task, self).find_generated_parameter(name)

    def update_generated_parameters(self):
        self.generated_parameters.update_parameters()
        super(Task, self).update_generated_parameters()

    def generated_parameters_only(self) -> Dict[str, Parameter]:
        params = super(Task, self).generated_parameters_only()
        params.update(self.generated_parameters.generated_parameters())
        return params

    # Node Operation ----------------------------------------------
    #   Node operation is used to control the flow.

    def before_run(self):
        """
        Prepare identity when called directly; ``run`` does this before hooks.
        """
        self._require_running()
        if not self._run_identity_prepared:
            Task.increment_try_no(self)

    def _require_running(self) -> None:
        bunch = self.get_bunch()
        if bunch is not None:
            bunch.require_running()

    def do_run(self) -> bool:
        """
        Run the task, actually.

        Subclasses of ``Task`` should reimplement this method to do the real run operation and deal with errors.
        """
        return True

    def after_run(self):
        """
        If task has been successfully run, change node status to ``NodeStatus.submitted`` and handle status change.
        """
        self.submitted_at = datetime.now(timezone.utc).isoformat()
        self.set_node_status(node_status=NodeStatus.submitted)
        logger.info(f"run: {self.node_path} with try no {self.try_no}")
        self.handle_status_change()

    def run(self):
        """
        Run the task, set status to ``NodeStatus.submitted`` and handle status change.
        """
        ensure_synchronous_task(self)
        self._require_running()
        # Allocate before any overridable hook, while preserving the existing
        # before_run -> do_run -> after_run template order for extensions.
        prior_attempt = {
            field: getattr(self, field)
            for field in (
                "try_no",
                "definition_generation",
                "attempt_id",
                "task_id",
                "aborted_reason",
                "job_password",
                "submitted_at",
                "started_at",
                "ended_at",
                "job_path",
                "jobout_path",
                "orvix_info_path",
            )
        }
        Task.increment_try_no(self)
        self._run_identity_prepared = True

        def restore_prior_attempt():
            for field, value in prior_attempt.items():
                setattr(self, field, value)
            self.update_generated_parameters()

        try:
            _synchronous_result(self.before_run())
            # A trusted extension hook may change service state. No task body
            # may start after that transition, and unused identity is restored.
            self._require_running()
        except (FlowStateError, UnsupportedValueError):
            restore_prior_attempt()
            raise
        finally:
            self._run_identity_prepared = False

        try:
            if not _synchronous_result(self.do_run()):
                return False

            _synchronous_result(self.after_run())
            return True
        except UnsupportedValueError:
            restore_prior_attempt()
            raise

    def requeue(self, reset_repeat: bool = True):
        self.try_no = 0
        self.clear_current_attempt()
        super(Task, self).requeue(reset_repeat=reset_repeat)

    def clear_current_attempt(self):
        """Discard the current run association before a future execution."""
        self.task_id = None
        self.aborted_reason = None
        self.job_password = None
        self.attempt_id = None
        self.submitted_at = None
        self.started_at = None
        self.ended_at = None
        self.job_path = None
        self.jobout_path = None
        self.orvix_info_path = None
        self.generated_parameters.takler_rid.value = None
        self.generated_parameters.takler_attempt_id.value = None
        self.generated_parameters.takler_pass.value = None

    # Status update operation ---------------------------------------
    #   Status update operation is used in task's running period,
    #   in order to notify task's status change to takler server.

    def init(self, task_id: str = ""):
        self.task_id = task_id
        if self.attempt_id is not None and self.started_at is None:
            self.started_at = datetime.now(timezone.utc).isoformat()
        self.set_node_status(node_status=NodeStatus.active)
        logger.info(f"init: {self.node_path}")
        self.handle_status_change()

    def complete(self):
        if self.attempt_id is not None:
            self.ended_at = datetime.now(timezone.utc).isoformat()
        self.set_node_status(node_status=NodeStatus.complete)
        logger.info(f"complete: {self.node_path}")
        self.handle_status_change()

    def abort(self, reason: str = ""):
        if self.attempt_id is not None:
            self.ended_at = datetime.now(timezone.utc).isoformat()
        self.set_node_status(node_status=NodeStatus.aborted)
        self.aborted_reason = reason
        logger.info(f"abort: {self.node_path} {reason}")
        self.handle_status_change()

    # Util ------------------------------------
    def increment_try_no(self):
        flow = self.get_flow()
        if flow is not None:
            if flow.definition_generation is None:
                flow.ensure_definition_generation()
            self.definition_generation = flow.definition_generation
        elif self.definition_generation is None:
            self.definition_generation = str(uuid4())
        self.try_no += 1
        self.attempt_id = str(uuid4())
        self.task_id = None
        self.aborted_reason = None
        self.submitted_at = None
        self.started_at = None
        self.ended_at = None
        self.job_path = None
        self.jobout_path = None
        self.orvix_info_path = None
        # ``token_urlsafe(32)`` yields a 43 character URL-safe string from a
        # cryptographically secure source, and contains no shell metacharacter,
        # so it can be exported without quoting.
        self.job_password = secrets.token_urlsafe(32)
        self.update_generated_parameters()


class TaskNodeGeneratedParameters(BaseModel):
    node: Task
    task: Parameter = Parameter(TASK, None)
    takler_name: Parameter = Parameter(TAKLER_NAME, None)
    takler_rid: Parameter = Parameter(TAKLER_RID, None)
    takler_attempt_id: Parameter = Parameter(TAKLER_ATTEMPT_ID, None)
    takler_try_no: Parameter = Parameter(TAKLER_TRY_NO, None)
    # ``TAKLER_PASS`` must stay a *generated* parameter. ``Node.to_dict()``
    # serializes ``user_parameters`` only, so keeping the job password here is
    # what keeps it out of both the ``show`` response and the checkpoint file.
    takler_pass: Parameter = Parameter(TAKLER_PASS, None)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def update_parameters(self):
        """
        Update generated parameters from task node's attrs.
        """
        self.task.value = self.node.name
        self.takler_name.value = self.node.node_path
        self.takler_rid.value = self.node.task_id
        self.takler_attempt_id.value = self.node.attempt_id
        self.takler_try_no.value = self.node.try_no
        self.takler_pass.value = self.node.job_password

    def find_parameter(self, name: str) -> Optional[Parameter]:
        if name == TASK:
            return self.task
        elif name == TAKLER_NAME:
            return self.takler_name
        elif name == TAKLER_RID:
            return self.takler_rid
        elif name == TAKLER_ATTEMPT_ID:
            return self.takler_attempt_id
        elif name == TAKLER_TRY_NO:
            return self.takler_try_no
        elif name == TAKLER_PASS:
            return self.takler_pass
        else:
            return None

    def generated_parameters(self) -> Dict[str, Parameter]:
        return {
            TASK: self.task,
            TAKLER_NAME: self.takler_name,
            TAKLER_RID: self.takler_rid,
            TAKLER_ATTEMPT_ID: self.takler_attempt_id,
            TAKLER_TRY_NO: self.takler_try_no,
            TAKLER_PASS: self.takler_pass,
        }


def task(name: str):
    """Decorator that turns a function into an inline task named ``name``.

    The decorated function runs inside the server process: its body is called
    after the task goes active, and the task is marked complete when the body
    returns. See :doc:`/develop/extending` for the execution semantics and
    limitations.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            class RunTask(Task):
                _takler_inline_run = True

                def __init__(self):
                    super(RunTask, self).__init__(name=name)

                def do_run(self):
                    self._require_running()
                    Task.after_run(self)
                    self.init()
                    kwargs.update(self=self)
                    func(*args, **kwargs)
                    self.complete()
                    return True

                def after_run(self):
                    # The inline body has already reported its final state.
                    pass

            return RunTask()

        return wrapper

    return decorator


def async_task(name: str):
    """Retained public decorator; asynchronous inline tasks are unsupported."""

    def decorator(func):
        raise UnsupportedValueError(
            "async_task is not supported; use task for synchronous functions",
            value="async_task",
        )

    return decorator
