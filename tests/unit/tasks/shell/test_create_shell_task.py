import sys

import pytest

from takler.tasks import ShellScriptTask


pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="tests for linux only")


def test_create():
    task = ShellScriptTask(
        name="initial", script_path="/home/johndoe/takler/script/initial.takler"
    )
    assert task.name == "initial"
    assert task.script_path == "/home/johndoe/takler/script/initial.takler"
