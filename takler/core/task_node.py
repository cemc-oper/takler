import functools
import asyncio
from typing import Optional, Dict, Set

from pydantic import BaseModel, ConfigDict

from .node import Node
from .state import NodeStatus
from .limit import Limit
from .parameter import (
    Parameter,
    TASK, TAKLER_NAME, TAKLER_RID, TAKLER_TRY_NO
)
from .util import logger, SerializationType


class Task(Node):
    def __init__(self, name: str):
        super(Task, self).__init__(name)
        self.task_id: Optional[str] = None

        self.aborted_reason: Optional[str] = None

        self.try_no: int = 0

        self.generated_parameters: TaskNodeGeneratedParameters = TaskNodeGeneratedParameters(node=self)

    def __repr__(self):
        return f"Task {self.name}"

    # Serialization ----------------------------------------------

    def to_dict(self) -> Dict:
        result = super().to_dict()
        result.update(dict(
            task_id=self.task_id,
            aborted_reason=self.aborted_reason,
            try_no=self.try_no,
        ))

        return result

    @classmethod
    def fill_from_dict(cls, d: Dict, node: "Task", method: SerializationType = SerializationType.Status) -> "Task":
        Node.fill_from_dict(d=d, node=node, method=method)

        task_id = d["task_id"]
        aborted_reason = d["aborted_reason"]
        try_no = d["try_no"]
        node.task_id = task_id
        node.aborted_reason = aborted_reason
        node.try_no = try_no

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
        elif status == NodeStatus.submitted:
            self.increment_in_limit(limit_set)
        elif status == NodeStatus.active:
            # TODO: When submitted is absent, active should increment limit
            pass
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
        Increment try no before actually run the task.
        """
        self.increment_try_no()

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
        self.set_node_status(node_status=NodeStatus.submitted)
        logger.info(f"run: {self.node_path} with try no {self.try_no}")
        self.handle_status_change()

    def run(self):
        """
        Run the task, set status to ``NodeStatus.submitted`` and handle status change.
        """
        self.before_run()

        if not self.do_run():
            return

        self.after_run()

    def requeue(self, reset_repeat: bool = True):
        self.task_id = None
        self.aborted_reason = None
        self.try_no = 0
        super(Task, self).requeue(reset_repeat=reset_repeat)

    # Status update operation ---------------------------------------
    #   Status update operation is used in task's running period,
    #   in order to notify task's status change to takler server.

    def init(self, task_id: str = ""):
        self.task_id = task_id
        self.set_node_status(node_status=NodeStatus.active)
        logger.info(f"init: {self.node_path}")
        self.handle_status_change()

    def complete(self):
        self.set_node_status(node_status=NodeStatus.complete)
        logger.info(f"complete: {self.node_path}")
        self.handle_status_change()

    def abort(self, reason: str = ""):
        self.set_node_status(node_status=NodeStatus.aborted)
        self.aborted_reason = reason
        logger.info(f"abort: {self.node_path} {reason}")
        self.handle_status_change()

    # Util ------------------------------------
    def increment_try_no(self):
        self.try_no += 1
        self.task_id = None
        self.aborted_reason = None
        self.update_generated_parameters()


class TaskNodeGeneratedParameters(BaseModel):
    node: Task
    task: Parameter = Parameter(TASK, None)
    takler_name: Parameter = Parameter(TAKLER_NAME, None)
    takler_rid: Parameter = Parameter(TAKLER_RID, None)
    takler_try_no: Parameter = Parameter(TAKLER_TRY_NO, None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

    def update_parameters(self):
        """
        Update generated parameters from task node's attrs.
        """
        self.task.value = self.node.name
        self.takler_name.value = self.node.node_path
        self.takler_rid.value = self.node.task_id
        self.takler_try_no.value = self.node.try_no

    def find_parameter(self, name: str) -> Optional[Parameter]:
        if name == TASK:
            return self.task
        elif name == TAKLER_NAME:
            return self.takler_name
        elif name == TAKLER_RID:
            return self.takler_rid
        elif name == TAKLER_TRY_NO:
            return self.takler_try_no
        else:
            return None

    def generated_parameters(self) -> Dict[str, Parameter]:
        return {
            TASK: self.task,
            TAKLER_NAME: self.takler_name,
            TAKLER_RID: self.takler_rid,
            TAKLER_TRY_NO: self.takler_try_no,
        }


def task(name: str):
    """
    Decorator to create inline task.

    Parameters
    ----------
    name
        task name
    Returns
    -------

    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            class RunTask(Task):
                def __init__(self):
                    super(RunTask, self).__init__(name=name)

                def run(self):
                    Task.run(self)

                    self.init()
                    kwargs.update(self=self)
                    func(*args, **kwargs)
                    self.complete()

            return RunTask()
        return wrapper
    return decorator


def async_task(name: str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            class RunTask(Task):
                def __init__(self):
                    super(RunTask, self).__init__(name=name)

                async def run(self):
                    Task.run(self)

                    async def run_func():
                        kwargs.update(self=self)
                        self.init()
                        await func(*args, **kwargs)
                        self.complete()

                    asyncio.run(run_func())

            return RunTask()
        return wrapper
    return decorator

