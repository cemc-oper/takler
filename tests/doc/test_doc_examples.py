"""Tests for the tutorial example scripts under ``doc/examples/``.

``doc/documentation-plan.md`` (D8, batch B / T6) establishes ``doc/examples/``
as the single source of truth for every Python example shown in the tutorial:
the ``.rst`` pages pull the code in with ``literalinclude`` instead of
duplicating it inline, and this test module is what keeps that source
importable, buildable and job-creation-clean as the ``takler`` API evolves.
Each test below imports one example module directly (so a broken import
fails loudly) and re-derives its flow, then runs ``check_job_creation`` (or an
equivalent rendering assertion) against it.

Generated job/output files (``*.job*`` and the ``.<try_no>`` output file) are
already excluded by ``.gitignore``; this module cleans up its own after each
test so repeated local runs don't accumulate stray files under
``doc/examples/``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

# ``tests/doc/test_doc_examples.py`` -> project root.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES_DIR = PROJECT_ROOT / "doc" / "examples" / "getting_started"


def _load_module(path: Path) -> ModuleType:
    """Import a standalone example script by file path.

    The examples are not part of the ``takler`` package (they are
    ``doc/examples/...``, referenced by the docs via ``literalinclude``), so
    they are loaded directly from their file path rather than by dotted name.
    """
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None, path
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def cleanup_generated_files():
    """Remove job/output files the examples create under ``test/``."""
    test_dir = EXAMPLES_DIR / "test"
    before = set(test_dir.iterdir())
    yield
    for path in test_dir.iterdir():
        if path not in before:
            path.unlink()


def test_step1_define_flow_builds_expected_tree():
    """``step1_define_flow.py`` defines a one-task flow ``test`` / ``t1``."""
    module = _load_module(EXAMPLES_DIR / "step1_define_flow.py")

    flow = module.create_flow()

    assert flow.name == "test"
    task1 = flow.find_node("/test/t1")
    assert task1 is not None
    assert task1.name == "t1"
    assert task1.find_parameter("TAKLER_SCRIPT").value == str(
        Path(module.TAKLER_HOME, "test/task1.takler")
    )


def test_step2_check_job_creation_succeeds(cleanup_generated_files):
    """``step2_check_job_creation.py``'s flow renders its job script cleanly.

    This is the regression check literalinclude relies on: if a change to
    ``takler.tasks.shell`` (or to the Jinja2 templates the flow points at)
    breaks job generation, ``check_job_creation`` reports a failure and this
    test catches it before the docs are re-published.
    """
    from takler.tasks.shell import check_job_creation

    module = _load_module(EXAMPLES_DIR / "step2_check_job_creation.py")

    flow = module.create_flow()
    task1 = flow.find_node("/test/t1")
    assert task1.check_job_creation()

    # ``check_job_creation`` walks the whole flow; assert it does not raise
    # and that the one task present renders successfully via the visitor path
    # documented in checking-job-creation.rst.
    check_job_creation(flow)


def test_step3_start_server_flow_matches_step1():
    """``step3_start_server.py`` builds the same one-task flow as step 1."""
    module = _load_module(EXAMPLES_DIR / "step3_start_server.py")

    flow = module.create_flow()

    assert flow.name == "test"
    assert flow.find_node("/test/t1") is not None


def test_head_and_tail_takler_render_with_task1(cleanup_generated_files):
    """The head/tail/task1 templates referenced by understanding-includes.rst
    render together as one job script without a Jinja2 error.

    This is what keeps the three ``literalinclude``-d ``.takler`` files
    (``head.takler``, ``task1.takler``, ``tail.takler``) consistent with each
    other: ``task1.takler`` includes the other two by name, so a rename or a
    syntax change in any of them would otherwise only surface when someone
    manually walks through the tutorial.
    """
    from takler.tasks.shell import ShellScriptTask

    test_dir = EXAMPLES_DIR / "test"
    task1 = ShellScriptTask("t1", str(test_dir / "task1.takler"))
    # ``TAKLER_HOME`` set to ``test_dir`` (not its parent) so the generated
    # job/output files land inside ``test/`` alongside the source templates,
    # where ``cleanup_generated_files`` looks for and removes them.
    task1.add_parameter("TAKLER_HOME", str(test_dir))
    task1.update_generated_parameters()

    assert task1.check_job_creation()
