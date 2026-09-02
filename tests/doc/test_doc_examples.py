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


def test_step4_add_tasks_and_containers_builds_expected_tree():
    """``step4_add_tasks_and_containers.py`` nests two tasks under a container.

    Mirrors the tree printed in add-tasks-and-containers.rst: ``t1`` sits
    directly under the flow, ``group1`` is a :class:`~takler.core.NodeContainer`
    holding ``t2`` and ``t3``.
    """
    module = _load_module(EXAMPLES_DIR / "step4_add_tasks_and_containers.py")

    flow = module.create_flow()

    assert flow.find_node("/test/t1") is not None
    group1 = flow.find_node("/test/group1")
    assert group1 is not None
    assert flow.find_node("/test/group1/t2") is not None
    assert flow.find_node("/test/group1/t3") is not None


def test_step4_container_status_aggregates_to_the_most_significant_child():
    """A container's status is the most significant status among its children.

    Asserts the exact scenario add-tasks-and-containers.rst walks through:
    ``t2`` active + ``t3`` complete aggregates ``group1`` (and the flow) to
    ``active``, because ``active`` outranks ``complete`` in ``NodeStatus``.
    """
    from takler.core import NodeStatus

    module = _load_module(EXAMPLES_DIR / "step4_add_tasks_and_containers.py")
    flow = module.create_flow()

    task2 = flow.find_node("/test/group1/t2")
    task3 = flow.find_node("/test/group1/t3")
    group1 = flow.find_node("/test/group1")

    task2.set_node_status(NodeStatus.active)
    task3.set_node_status(NodeStatus.complete)

    assert group1.state.node_status == NodeStatus.active
    assert flow.state.node_status == NodeStatus.active


def test_step5_variables_resolve_by_nearest_ancestor():
    """``step5_variables.py`` demonstrates parameter shadowing along the tree.

    Mirrors the exact scenario walked through in variables.rst: ``t2``
    shadows both its container and the flow with its own ``GREETING``; ``t3``
    falls back to the container's value; ``t1`` (outside ``group1``) falls
    back all the way to the flow's value.
    """
    module = _load_module(EXAMPLES_DIR / "step5_variables.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    task2 = flow.find_node("/test/group1/t2")
    task3 = flow.find_node("/test/group1/t3")

    assert task1.find_parent_parameter("GREETING").value == "hello from flow"
    assert task2.find_parent_parameter("GREETING").value == "hello from t2"
    assert task3.find_parent_parameter("GREETING").value == "hello from group1"


def test_step5_user_parameter_takes_priority_over_generated_of_same_name():
    """A user-defined ``TAKLER_SCRIPT`` wins over the generated placeholder.

    ``find_parameter`` looks at user parameters first: ``t1`` has both a user
    ``TAKLER_SCRIPT`` (the script path it was configured with) and a
    generated ``TAKLER_SCRIPT`` (still ``None`` before job creation runs), and
    the user value must be what callers see.
    """
    module = _load_module(EXAMPLES_DIR / "step5_variables.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")

    resolved = task1.find_parameter("TAKLER_SCRIPT")

    assert resolved.value == str(Path(module.TAKLER_HOME, "test/task1.takler"))


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
