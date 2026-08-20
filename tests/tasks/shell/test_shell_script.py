from pathlib import Path
import sys

import pytest

from takler.core import Bunch, Flow
from takler.tasks import ShellScriptTask
from takler.tasks.shell.shell_render import ShellRender


pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="tests for linux only")


@pytest.fixture
def scripts_directory():
    return Path(Path(__file__).parent, "scripts")


@pytest.fixture
def takler_home_directory():
    return Path(Path(__file__).parent, "takler_home")


@pytest.fixture
def takler_include_directory():
    return Path(Path(__file__).parent, "include")


def test_simple_script_render(scripts_directory, takler_home_directory):
    task1_script_path = str(Path(scripts_directory, "task1.takler"))
    task1 = ShellScriptTask("task1", task1_script_path)
    task1.add_parameter("TAKLER_HOME", str(takler_home_directory))
    task1.add_parameter("SLEEP", 30)
    task1.update_generated_parameters()

    shell_script = ShellRender(node=task1)
    shell_script.render_script(script_path=task1_script_path)


def test_script_with_include_render(
    scripts_directory, takler_home_directory, takler_include_directory
):
    with Bunch("nwpc_op") as bunch:
        with Flow("flow1") as flow1:
            bunch.add_flow(flow1)
            task1_script_path = str(
                Path(scripts_directory, "task1_with_include.takler")
            )
            with flow1.add_task(ShellScriptTask("task1", task1_script_path)) as task1:
                task1.add_parameter("TAKLER_HOME", str(takler_home_directory))
                task1.add_parameter("TAKLER_INCLUDE", str(takler_include_directory))
                task1.add_parameter("SLEEP", 30)
                task1.update_generated_parameters()

    shell_script = ShellRender(node=task1)
    shell_script.render_script(script_path=task1_script_path)
