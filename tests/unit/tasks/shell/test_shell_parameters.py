import sys
import pytest

# from takler.visitor import pre_order_travel, SimplePrintVisitor


pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="tests for linux only")


def test_generated_parameters(shell_task_bunch):
    task1 = shell_task_bunch.task1  # type: ShellScriptTask
    task1.update_generated_parameters()
    params = task1.parameters()
    for key, p in params.items():
        print(key, p.value)
