"""Static guard for the single job spawn path of ``ShellRunner`` (task 14.1).

Requirement 12.6 is a property of the *source file*: ``ShellRunner`` must offer
exactly one public method that derives a job, i.e. ``spwan_v2`` must no longer
exist in ``takler/tasks/shell/shell_runner.py``. The old method forked the
process and, when the first ``os.fork()`` raised ``OSError``, let the parent
fall through to ``os.execl`` and replace the server process with the job. No
behavioural test can assert the absence of a method, so the check is done by
parsing the module with :mod:`ast` (a mention inside a comment or a docstring
must not fail the test) plus an attribute check on the imported class.

Validates: Requirements 12.6
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import List, Set

import takler
from takler.tasks.shell import shell_runner
from takler.tasks.shell.shell_runner import ShellRunner

PACKAGE_ROOT = Path(takler.__file__).parent

SHELL_RUNNER_SOURCE = PACKAGE_ROOT / "tasks" / "shell" / "shell_runner.py"

REMOVED_METHOD = "spwan_v2"

#: The only public method of ``ShellRunner`` allowed to derive a job.
SPAWN_METHOD = "spwan"


def _parse(source_path: Path) -> ast.Module:
    return ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))


def _function_names(tree: ast.AST) -> List[str]:
    """Names of every ``def`` / ``async def`` in ``tree``, at any nesting level."""
    return [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def _class_def(tree: ast.Module, class_name: str) -> ast.ClassDef:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node
    raise AssertionError(f"class not found in {SHELL_RUNNER_SOURCE}: {class_name}")


def _public_method_names(class_node: ast.ClassDef) -> Set[str]:
    """Public methods declared directly in the class body."""
    return {
        node.name
        for node in class_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and not node.name.startswith("_")
    }


def test_source_file_exists():
    assert SHELL_RUNNER_SOURCE.is_file(), (
        f"source file not found: {SHELL_RUNNER_SOURCE}"
    )


def test_no_spwan_v2_function_definition():
    """No ``def spwan_v2`` remains anywhere in the module."""
    names = _function_names(_parse(SHELL_RUNNER_SOURCE))

    assert REMOVED_METHOD not in names, (
        f"{REMOVED_METHOD} must be deleted from {SHELL_RUNNER_SOURCE}, "
        f"found function definitions: {sorted(names)}"
    )


def test_shell_runner_has_single_public_spawn_method():
    """``ShellRunner`` exposes exactly one public job spawn method."""
    class_node = _class_def(_parse(SHELL_RUNNER_SOURCE), "ShellRunner")

    assert _public_method_names(class_node) == {SPAWN_METHOD}


def test_spwan_v2_attribute_is_absent():
    """The removed method is not reachable on the class or the module."""
    assert not hasattr(ShellRunner, REMOVED_METHOD)
    assert not hasattr(shell_runner, REMOVED_METHOD)
    assert callable(getattr(ShellRunner, SPAWN_METHOD))


def test_scanner_detects_spwan_v2(tmp_path: Path):
    """Guard the scanner itself: a real definition must be reported, a
    docstring / comment mention must not."""
    sample = tmp_path / "sample.py"
    sample.write_text(
        "class ShellRunner:\n"
        '    """spwan_v2 is mentioned in this docstring only."""\n'
        "    # spwan_v2 is mentioned in this comment only\n"
        "    def spwan(self, command):\n"
        "        return command\n",
        encoding="utf-8",
    )
    tree = _parse(sample)
    assert _function_names(tree) == [SPAWN_METHOD]
    assert _public_method_names(_class_def(tree, "ShellRunner")) == {SPAWN_METHOD}

    sample.write_text(
        "class ShellRunner:\n"
        "    def spwan(self, command):\n"
        "        return command\n"
        "\n"
        "    def spwan_v2(self, command):\n"
        "        return command\n",
        encoding="utf-8",
    )
    tree = _parse(sample)
    assert sorted(_function_names(tree)) == [SPAWN_METHOD, REMOVED_METHOD]
    assert _public_method_names(_class_def(tree, "ShellRunner")) == {
        SPAWN_METHOD,
        REMOVED_METHOD,
    }
