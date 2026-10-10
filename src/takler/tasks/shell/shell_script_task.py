import stat
import shlex
from typing import Union, Optional, Dict
from pathlib import Path

from pydantic import BaseModel

from takler.core import (
    Task,
    Parameter,
    Flow,
    NodeStatus,
    ServerStatus,
    SerializationType,
)
from takler.core.node import Node
from takler.core.parameter import TAKLER_HOME
from takler.exceptions import JobSubmissionError
from takler.logging import get_logger
from takler.visitor import pre_order_travel, NodeVisitor


from .constant import TAKLER_SCRIPT, TAKLER_JOB, TAKLER_JOBOUT, JOB_SCRIPT_EXTENSION
from .shell_render import ShellRender
from .shell_runner import ShellRunner, redact_job_password


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
        # Process-local identity, never serialized or reused after a submission.
        self._submission_token: Optional[object] = None

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

    def find_parameter(self, name: str) -> Optional[Parameter]:
        # During a run, artifact overrides are resolved to a concrete path.
        # Keep the definition's literal template in user_parameters.
        if name in (TAKLER_JOB, TAKLER_JOBOUT):
            return self.shell_generated_parameters.find_parameter(name)
        return super().find_parameter(name)

    def parameters_only(self) -> Dict[str, Parameter]:
        params = super().parameters_only()
        params[TAKLER_JOB] = self.shell_generated_parameters.takler_job
        params[TAKLER_JOBOUT] = self.shell_generated_parameters.takler_jobout
        return params

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
            reason = redact_job_password(
                f"JobSubmissionError: {exc}", self.job_password
            )
            logger.error(f"job submission failed: {self.node_path}: {reason}")
            self.abort(reason)
            return False
        return True

    def requeue(self, reset_repeat: bool = True):
        self._submission_token = None
        super().requeue(reset_repeat=reset_repeat)

    def clear_current_attempt(self):
        super().clear_current_attempt()
        self._submission_token = None
        self.shell_generated_parameters.takler_job.value = None
        self.shell_generated_parameters.takler_jobout.value = None

    def set_node_status_only(self, node_status: NodeStatus):
        # Includes child completion/abort and forced terminal/queued states.
        if node_status not in (NodeStatus.submitted, NodeStatus.active):
            self._submission_token = None
        super().set_node_status_only(node_status)

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
        self._require_running()
        token = self._submission_token = object()
        flow, bunch, node_path = self.get_flow(), self.get_bunch(), self.node_path
        attempt_id, generation = self.attempt_id, self.definition_generation
        external_submit = self._uses_orvix()

        def is_current_submission() -> bool:
            current = self._submission_token is token
            current = current and self.attempt_id == attempt_id
            current = current and self.definition_generation == generation
            current = current and self.get_flow() is flow and self.get_bunch() is bunch
            if current and flow is not None:
                current = flow.definition_generation == generation
            if current and bunch is not None:
                current = bunch.find_node(node_path) is self
            elif current and flow is not None:
                current = flow.find_node(node_path) is self
            return current

        def before_start() -> bool:
            current = is_current_submission()
            current = current and self.state.node_status is NodeStatus.submitted
            current = current and (flow is None or flow.begun)
            if current and bunch is not None:
                current = (
                    bunch.server_state.status is ServerStatus.running
                    and not bunch._service_stopping
                )
            if not current:
                logger.info(f"skip stale job start: {node_path}")
            return current

        def on_failure(exc: BaseException) -> None:
            # All checks and abort run synchronously on the same event loop.
            # A detached flow can still retain its old bunch back-reference.
            if not is_current_submission():
                logger.info(f"skip stale job failure: {node_path}")
                return
            self.on_job_failure(exc, result_unknown=external_submit)

        def on_skipped() -> None:
            # A valid current submission skipped before run_process has a
            # definite non-start result. Old work must not abort a new attempt.
            if (
                is_current_submission()
                and self.state.node_status is NodeStatus.submitted
            ):
                self.abort("submission skipped before process start")

        run_command = self.create_job_script()
        self._require_running()
        shell_runner = ShellRunner(job_password=self.job_password)
        shell_runner.spawn(
            command=run_command,
            node_path=node_path,
            on_failure=on_failure,
            before_start=before_start,
            on_skipped=on_skipped,
        )
        return True

    def _artifact_paths(self) -> tuple[Path, Path, Optional[Path]]:
        """Resolve this attempt's files and reject collisions before any write."""
        home = self.find_parent_parameter(TAKLER_HOME)
        if home is None or home.value is None:
            raise ValueError("TAKLER_HOME is not set")
        suffix = self.attempt_id or str(self.try_no)
        defaults = (
            f"{home.value}{self.node_path}.{JOB_SCRIPT_EXTENSION}{suffix}",
            f"{home.value}{self.node_path}.{suffix}",
        )

        def resolve(name: str, default: str) -> Path:
            override = self._artifact_override(name)
            if override is None:
                value = default
            else:
                value = override.value
                if not isinstance(value, str) or value.count("{attempt_id}") != 1:
                    raise ValueError(f"{name} requires exactly one {{attempt_id}}")
                value = value.replace("{attempt_id}", self.attempt_id or "unassigned")
            return Path(value).resolve()

        job = resolve(TAKLER_JOB, defaults[0])
        jobout = resolve(TAKLER_JOBOUT, defaults[1])
        info = job.with_name(job.name + ".info.yaml") if self._uses_orvix() else None
        paths = {job, jobout}
        if info is not None:
            paths.add(info)
        if len(paths) != (3 if info is not None else 2):
            raise ValueError("job artifact paths conflict")
        bunch = self.get_bunch()
        if bunch is not None:
            from takler.serialization.builder import walk

            for other in walk(bunch):
                if other is self or not isinstance(other, Task):
                    continue
                for value in (other.job_path, other.jobout_path, other.orvix_info_path):
                    if value is not None and Path(value).resolve() in paths:
                        raise ValueError("job artifact path conflicts with current run")
        return job, jobout, info

    def _artifact_override(self, name: str) -> Optional[Parameter]:
        node = self
        while node is not None:
            found = node.find_user_parameter(name)
            if found is not None:
                return found
            node = node.parent
        bunch = self.get_bunch()
        return None if bunch is None else bunch.find_user_parameter(name)

    def _uses_orvix(self) -> bool:
        param = self.find_parent_parameter("TAKLER_SHELL_JOB_CMD")
        if param is None or not isinstance(param.value, str):
            return False
        try:
            args = shlex.split(param.value)
        except ValueError:
            return False
        return len(args) >= 2 and Path(args[0]).name == "orvix" and args[1] == "submit"

    def on_job_failure(self, exc: BaseException, result_unknown: bool = False) -> None:
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

        reason = redact_job_password(f"{type(exc).__name__}: {exc}", self.job_password)
        if result_unknown:
            reason = (
                f"external submission result unknown; verify before rerun: {reason}"
            )
        self.abort(reason)

    def create_job_script(self) -> str:
        """
        Create job script and return run command.

        The generated job script is made executable by its owner. Its read and
        write permissions follow the directory default ACL and process umask
        for new files. Existing files are overwritten directly; only owner
        execute is added. A failed write may leave incomplete content.

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
        self._require_running()
        try:
            self.update_generated_parameters()

            job, jobout, info = self._artifact_paths()
            self.shell_generated_parameters.takler_job.value = job
            self.shell_generated_parameters.takler_jobout.value = jobout

            # get script path from TAKLER_SCRIPT
            script_param = self.find_parameter(TAKLER_SCRIPT)
            if script_param is None:
                raise ValueError("script param is empty")
            script_path = script_param.value

            # render job script
            shell_script = ShellRender(self)

            job_script_path = shell_script.render_script(script_path)
            if self.attempt_id is not None:
                self.job_path = str(job)
                self.jobout_path = str(jobout)
                self.orvix_info_path = str(info) if info is not None else None
            # Only add the owner execute bit, and leave the read/write bits as
            # they were created by the default ACL/umask or already existed. A job script
            # may carry ``TAKLER_PASS``, so takler must not widen its read
            # permission by setting an explicit mode such as ``0o755``: who may
            # read the job password is a deployment permission decision.
            mode = job_script_path.stat().st_mode
            job_script_path.chmod(mode | stat.S_IXUSR)
            logger.info(f"Job generation success: {job_script_path}")

            # get run command
            run_command = shell_script.render_job_command()
            logger.info(
                redact_job_password(
                    f"Render run command success: {run_command}", self.job_password
                )
            )
        except JobSubmissionError:
            raise
        except Exception as exc:
            raise JobSubmissionError(
                redact_job_password(
                    f"render job script failed for {self.node_path}: {exc}",
                    self.job_password,
                )
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
        suffix = self.node.attempt_id or str(self.node.try_no)
        job_path = Path(
            f"{home_param.value}{self.node.node_path}.{JOB_SCRIPT_EXTENSION}{suffix}"
        )
        self.takler_job.value = job_path.absolute()

        jobout_path = Path(f"{home_param.value}{self.node.node_path}.{suffix}")
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
    A node visitor to check job creation of every ``ShellScriptTask``.
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
    Check job creation for every ``ShellScriptTask`` in a flow

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
