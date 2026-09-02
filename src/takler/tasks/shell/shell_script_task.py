import stat
from typing import Union, Optional, Dict
from pathlib import Path

from pydantic import BaseModel

from takler.core import Task, Parameter, Flow, NodeStatus, SerializationType
from takler.core.node import Node
from takler.core.parameter import TAKLER_HOME
from takler.exceptions import JobSubmissionError
from takler.logging import get_logger
from takler.visitor import pre_order_travel, NodeVisitor


from .constant import TAKLER_SCRIPT, TAKLER_JOB, TAKLER_JOBOUT, JOB_SCRIPT_EXTENSION
from .shell_render import ShellRender
from .shell_runner import ShellRunner


logger = get_logger("tasks.shell")


class ShellScriptTask(Task):
    """
    A task to run shell script.

    A shell script task should have a corresponding shell script.
    There are two methods to set the script path:

    * set ``script_path`` attribute, and ``update_generated_parameters()`` method will use it to generate TAKLER_SCRIPT parameter.
    * set ``TAKLER_SCRIPT`` parameter as a user parameter to override generated ``TAKLER_SCRIPT`` parameter.
    """

    def __init__(self, name: str, script_path: Optional[Union[str, Path]] = None):
        super(ShellScriptTask, self).__init__(name)

        self.script_path = script_path

        self.shell_generated_parameters = ShellScriptTaskGeneratedParameters(node=self)

    # Serialization ---------------------------------------------

    def to_dict(self) -> Dict:
        result = super().to_dict()
        result.update(
            dict(
                script_path=None if self.script_path is None else str(self.script_path),
            )
        )

        return result

    @classmethod
    def fill_from_dict(
        cls,
        d: Dict,
        node: "ShellScriptTask",
        method: SerializationType = SerializationType.Status,
    ) -> "ShellScriptTask":
        Task.fill_from_dict(d=d, node=node, method=method)

        # ``script_path`` belongs to the flow definition instead of the runtime status,
        # so it is restored with both ``Tree`` and ``Status`` methods.
        node.script_path = d.get("script_path", None)

        return node

    # Parameter -------------------------------------------------

    def update_generated_parameters(self):
        self.shell_generated_parameters.update_parameters()
        super(ShellScriptTask, self).update_generated_parameters()

    def find_generated_parameter(self, name: str) -> Optional[Parameter]:
        p = self.shell_generated_parameters.find_parameter(name)
        if p is not None:
            return p

        p = super(ShellScriptTask, self).find_generated_parameter(name)
        return p

    def generated_parameters_only(self) -> Dict[str, Parameter]:
        task_params = super(ShellScriptTask, self).generated_parameters_only()
        params = self.shell_generated_parameters.generate_variables()
        for key, p in task_params.items():
            if key not in params:
                params[key] = p
        return params

    # Operation -------------------------------------------------

    def do_run(self) -> bool:
        """
        run shell command

        Job submission failures (script rendering or job spawning) surface as
        ``JobSubmissionError``. They are logged as ERROR with the node path and
        the description text, and the task is set to aborted.
        """
        try:
            self.submit()
        except JobSubmissionError as exc:
            logger.error(f"job submission failed: {self.node_path}: {exc}")
            self.abort(f"JobSubmissionError: {exc}")
            return False
        return True

    # Task specific ------------------------------------------------------------

    def submit(self) -> bool:
        """
        Generate job script and run job command.

        Raises
        ------
        JobSubmissionError
            When the job script cannot be rendered, or the job process cannot
            be spawned.
        """
        run_command = self.create_job_script()

        # run command
        shell_runner = ShellRunner()
        shell_runner.spwan(
            command=run_command,
            node_path=self.node_path,
            on_failure=self.on_job_failure,
        )
        return True

    def on_job_failure(self, exc: BaseException) -> None:
        """
        Set the task to aborted after ``ShellRunner`` has logged the job failure.

        The task is only aborted when it is still ``submitted`` or ``active``:
        a task which has already reported ``complete`` / ``aborted`` with a
        child command must not be overwritten by the job wrapper's exit code.

        Parameters
        ----------
        exc
            The exception the job task ended with.
        """
        if self.state.node_status not in (NodeStatus.submitted, NodeStatus.active):
            logger.info(
                f"skip abort on job failure, task is {self.state.node_status.name}: "
                f"{self.node_path}"
            )
            return

        self.abort(f"{type(exc).__name__}: {exc}")

    def create_job_script(self) -> str:
        """
        Create job script and return run command.

        The generated job script is made executable by its owner. Its read and
        write permission bits are left as created, i.e. decided by the process
        umask, and are never set explicitly by takler.

        Returns
        -------
        str
            run command string.

        Raises
        ------
        JobSubmissionError
            When the job script cannot be rendered. The message contains the
            node path and the reason.
        """
        try:
            self.update_generated_parameters()

            # get script path from TAKLER_SCRIPT
            script_param = self.find_parameter(TAKLER_SCRIPT)
            if script_param is None:
                raise ValueError("script param is empty")
            script_path = script_param.value

            # render job script
            shell_script = ShellRender(self)

            job_script_path = shell_script.render_script(script_path)
            # Only add the owner execute bit, and leave the read/write bits as
            # they were created, i.e. decided by the process umask. A job script
            # may carry ``TAKLER_PASS``, so takler must not widen its read
            # permission by setting an explicit mode such as ``0o755``: who may
            # read the job password is a deployment decision expressed by umask.
            mode = job_script_path.stat().st_mode
            job_script_path.chmod(mode | stat.S_IXUSR)
            logger.info(f"Job generation success: {job_script_path}")

            # get run command
            run_command = shell_script.render_job_command()
            logger.info(f"Render run command success: {run_command}")
        except JobSubmissionError:
            raise
        except Exception as exc:
            raise JobSubmissionError(
                f"render job script failed for {self.node_path}: {exc}"
            ) from exc
        return run_command

    def check_job_creation(self) -> bool:
        _ = self.create_job_script()
        return True


class ShellScriptTaskGeneratedParameters(BaseModel):
    node: ShellScriptTask
    takler_script: Parameter = Parameter(TAKLER_SCRIPT, None)
    takler_job: Parameter = Parameter(TAKLER_JOB, None)
    takler_jobout: Parameter = Parameter(TAKLER_JOBOUT, None)

    class Config:
        arbitrary_types_allowed = True

    def update_parameters(self):
        self.takler_script.value = self.node.script_path

        home_param = self.node.find_parent_parameter(TAKLER_HOME)
        job_path = Path(
            f"{home_param.value}{self.node.node_path}.{JOB_SCRIPT_EXTENSION}{self.node.try_no}"
        )
        self.takler_job.value = job_path.absolute()

        jobout_path = Path(
            f"{home_param.value}{self.node.node_path}.{self.node.try_no}"
        )
        self.takler_jobout.value = jobout_path.absolute()

    def find_parameter(self, name: str) -> Optional[Parameter]:
        if name == TAKLER_SCRIPT:
            return self.takler_script
        elif name == TAKLER_JOB:
            return self.takler_job
        elif name == TAKLER_JOBOUT:
            return self.takler_jobout
        else:
            return None

    def generate_variables(self) -> Dict[str, Parameter]:
        return {
            TAKLER_SCRIPT: self.takler_script,
            TAKLER_JOB: self.takler_job,
            TAKLER_JOBOUT: self.takler_jobout,
        }


class CheckJobCreationVisitor(NodeVisitor):
    """
    A node visitor to check all ``ShellScriptTask``s' job creation.
    """

    def __init__(self):
        super(CheckJobCreationVisitor, self).__init__()
        self.total = 0
        self.success = 0
        self.failed = 0

    def visit(self, node: Node):
        if not isinstance(node, ShellScriptTask):
            return
        self.total += 1
        if node.check_job_creation():
            self.success += 1
        else:
            self.failed += 1


def check_job_creation(flow: Flow):
    """
    Check job creation for ``ShellScriptTask``s

    Parameters
    ----------
    flow
        a flow
    """
    visitor = CheckJobCreationVisitor()
    pre_order_travel(flow, visitor)
    logger.info(
        f"check job creation results: {visitor.total} total, {visitor.success} success, {visitor.failed} failed."
    )
