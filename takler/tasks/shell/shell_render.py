import os
from typing import Union, List, Dict, Any, Optional, TYPE_CHECKING
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from .constant import (
    TAKLER_JOB,
    TAKLER_SHELL_JOB_CMD,
    DEFAULT_TAKLER_SHELL_JOB_CMD,
    TAKLER_INCLUDE,
)

if TYPE_CHECKING:
    from .shell_script_task import ShellScriptTask


class ShellRender(object):
    """
    Shell render associated with a ``ShellScriptTask`` for task script and task commands.

    When ``ShellScriptTask`` begins to run, the shell script is rendered into a job script.
    Currently, ``ShellRender`` supports Jinja2 library.
    """

    def __init__(self, node: "ShellScriptTask"):
        self.node: "ShellScriptTask" = node
        self._template_params: Optional[Dict[str, Any]] = None

    def render_script(self, script_path: Union[str, Path]) -> Path:
        """
        render shell script to job script, and write job script to file system.

        Use the following parameters:

        * ``TAKLER_INCLUDE``: template search directory list, split by ``:``
        * ``TAKLER_JOB``: generated job script path
        """
        script_path = Path(script_path)
        loader_paths = [script_path.parent]
        loader_paths.extend(self.get_include_paths())

        template_params = self.template_params()

        file_loader = FileSystemLoader(loader_paths)
        env = Environment(loader=file_loader)

        # TODO: may raise exception
        template = env.get_template(script_path.name)
        job_script_content = template.render(**template_params)

        job_script_path = Path(self.node.find_parameter(TAKLER_JOB).value)
        job_script_path.parent.mkdir(parents=True, exist_ok=True)
        with open(job_script_path, "w") as f:
            f.write(job_script_content)

        return job_script_path

    def render_job_command(self) -> str:
        """
        render job command from ``TAKLER_SHELL_JOB_CMD`` or use ``DEFAULT_TAKLER_SHELL_JOB_CMD`` if not set.
        """
        # get job command, default is ``DEFAULT_TAKLER_SHELL_JOB_CMD``
        job_command_param = self.node.find_parent_parameter(TAKLER_SHELL_JOB_CMD)
        if job_command_param is not None:
            job_command = job_command_param.value
        else:
            job_command = DEFAULT_TAKLER_SHELL_JOB_CMD

        command = self.render_command(job_command)
        return command

    def render_command(self, command: str) -> str:
        """
        render command string using node's parameters.
        """
        template_params = self.template_params()

        env = Environment()
        template = env.from_string(command)

        rendered_command = template.render(**template_params)
        return rendered_command

    def get_include_paths(self) -> List[str]:
        """
        Get template include paths from parameter ``TAKLER_INCLUDE``.

        Paths are split by ``:``.

        If ``TAKLER_INCLUDE`` is not set, return an empty list.
        """
        include_param = self.node.find_parent_parameter(TAKLER_INCLUDE)
        if include_param is None:
            return list()

        include_string = include_param.value
        include_paths = include_string.split(os.pathsep)
        return include_paths

    def template_params(
        self, force: bool = False
    ) -> Dict[str, Union[str, int, float, bool]]:
        """
        Get template params from node only once. Store in ``self._template_params``.

        If ``force`` is set, params will be regenerated.
        """
        if self._template_params is not None and not force:
            return self._template_params

        params = self.node.parameters()
        template_params = {key: p.value for key, p in params.items()}
        self._template_params = template_params
        return self._template_params
