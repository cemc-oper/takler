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
import shutil
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
            if path.is_dir():
                shutil.rmtree(path)
            else:
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


def test_step6_triggers_block_until_upstream_task_completes():
    """``step6_triggers.py``'s ``t2`` depends on ``t1 == complete``.

    Mirrors the exact scenario walked through in triggers.rst: before ``t1``
    completes, ``t2``'s dependencies do not resolve; once ``t1`` is marked
    complete, they do.
    """
    from takler.core import NodeStatus

    module = _load_module(EXAMPLES_DIR / "step6_triggers.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    task2 = flow.find_node("/test/t2")

    flow.requeue()
    assert task2.resolve_dependencies() is False

    task1.set_node_status(NodeStatus.complete)
    assert task2.resolve_dependencies() is True


def test_step7_event_and_meter_triggers_gate_downstream_tasks():
    """``step7_events_and_meters.py`` gates ``t2``/``t3`` on t1's self-report.

    Mirrors the exact scenario walked through in events-and-meters.rst:
    ``t2`` waits for event ``a`` to be set and ``t3`` waits for meter
    ``step`` to reach 50; neither resolves while ``t1`` has not reported,
    and both resolve once the corresponding attribute value arrives.
    """
    module = _load_module(EXAMPLES_DIR / "step7_events_and_meters.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    task2 = flow.find_node("/test/t2")
    task3 = flow.find_node("/test/t3")

    flow.requeue()
    assert task2.resolve_dependencies() is False
    assert task3.resolve_dependencies() is False

    # Meter below the threshold still blocks t3; reaching it releases t3.
    task1.set_meter("step", 25)
    assert task3.resolve_dependencies() is False
    task1.set_meter("step", 50)
    assert task3.resolve_dependencies() is True

    # Setting the event releases t2 while t1 is still running.
    task1.set_event("a", True)
    assert task2.resolve_dependencies() is True


def test_step7_requeue_resets_events_and_meters():
    """``requeue`` returns t1's event and meter to their initial values.

    events-and-meters.rst promises that a requeued node reports from a clean
    slate: the event falls back to ``initial_value`` (``unset``) and the meter
    falls back to its range minimum, so downstream triggers wait for fresh
    reports on the next run.
    """
    module = _load_module(EXAMPLES_DIR / "step7_events_and_meters.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    task1.set_event("a", True)
    task1.set_meter("step", 50)

    task1.requeue()

    assert task1.find_event("a").value is False
    assert task1.find_meter("step").value == 0


def test_step7_meter_rejects_values_outside_its_range():
    """Meter updates outside ``[min_value, max_value]`` raise ``ValueError``.

    events-and-meters.rst states that out-of-range reports are refused; the
    example's meter ``step`` spans 0~100, so 101 must be rejected.
    """
    module = _load_module(EXAMPLES_DIR / "step7_events_and_meters.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")

    with pytest.raises(ValueError):
        task1.set_meter("step", 101)


def test_step7_task1_with_events_renders_cleanly(cleanup_generated_files):
    """The ``task1_with_events.takler`` script renders without Jinja2 errors."""
    from takler.tasks.shell import ShellScriptTask

    test_dir = EXAMPLES_DIR / "test"
    task1 = ShellScriptTask("t1", str(test_dir / "task1_with_events.takler"))
    task1.add_parameter("TAKLER_HOME", str(test_dir))
    task1.update_generated_parameters()

    assert task1.check_job_creation()


def test_step8_limit_tokens_track_task_status_changes():
    """``step8_limits.py``'s limit counts running tasks and gates the rest.

    Mirrors the exact scenario walked through in limits.rst: a task holds one
    token of ``work`` while it is ``submitted``; once both tokens are taken
    ``t3`` may not start; completing or aborting a running task releases its
    token and lets the next task in.
    """
    from takler.core import NodeStatus

    module = _load_module(EXAMPLES_DIR / "step8_limits.py")
    flow = module.create_flow()

    group1 = flow.find_node("/test/group1")
    limit = group1.find_limit("work")
    assert limit.limit == 2
    assert limit.value == 0

    task1 = flow.find_node("/test/group1/t1")
    task2 = flow.find_node("/test/group1/t2")
    task3 = flow.find_node("/test/group1/t3")

    # A task occupies one token while it is submitted.
    task1.set_node_status(NodeStatus.submitted)
    assert limit.value == 1
    task2.set_node_status(NodeStatus.submitted)
    assert limit.value == 2

    # The pool is exhausted: t3 may not start until a token is released.
    assert task3.check_in_limit_up() is False

    # Completing t1 releases its token, so t3 fits again.
    task1.set_node_status(NodeStatus.complete)
    assert limit.value == 1
    assert task3.check_in_limit_up() is True

    # Aborting a running task releases its token as well.
    task2.set_node_status(NodeStatus.aborted)
    assert limit.value == 0


def test_step8_in_limit_resolves_limit_up_the_node_tree():
    """An in-limit without ``node_path`` finds the nearest limit up the tree.

    limits.rst states the lookup rule mirrors variable lookup: ``t1`` sits
    under ``group1`` which holds ``work``, so the bare ``add_in_limit("work")``
    binds to that limit.
    """
    module = _load_module(EXAMPLES_DIR / "step8_limits.py")
    flow = module.create_flow()

    group1 = flow.find_node("/test/group1")
    task1 = flow.find_node("/test/group1/t1")

    assert task1.find_limit_up("work") is group1.find_limit("work")


def test_step9_repeat_generates_date_variable_for_children():
    """``step9_repeat_and_time.py``'s repeat exposes ``TAKLER_DATE`` to children.

    repeat-and-time.rst promises that a repeat generates a same-named variable
    holding the current loop value (an integer ``YYYYMMDD``) which the whole
    subtree — including task scripts — can reference.
    """
    module = _load_module(EXAMPLES_DIR / "step9_repeat_and_time.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/daily/t1")

    assert task1.parameters()["TAKLER_DATE"].value == 20240101


def test_step9_repeat_advances_and_requeues_when_node_completes():
    """Completing a repeat node advances the date and requeues the subtree.

    Mirrors the exact scenario walked through in repeat-and-time.rst: each
    time ``daily`` completes, the repeat moves to the next day and the
    container (with its task) is requeued; past the end date the container
    stays ``complete``.
    """
    from takler.core import NodeStatus

    module = _load_module(EXAMPLES_DIR / "step9_repeat_and_time.py")
    flow = module.create_flow()

    daily = flow.find_node("/test/daily")
    task1 = flow.find_node("/test/daily/t1")

    task1.set_node_status(NodeStatus.complete)
    assert daily.repeat.value() == 20240102
    assert daily.state.node_status == NodeStatus.queued
    assert task1.state.node_status == NodeStatus.queued

    task1.set_node_status(NodeStatus.complete)
    assert daily.repeat.value() == 20240103
    assert daily.state.node_status == NodeStatus.queued

    # 20240103 is the end date: no next value, so the container stays complete.
    task1.set_node_status(NodeStatus.complete)
    assert daily.repeat.value() == 20240103
    assert daily.state.node_status == NodeStatus.complete


def test_step9_requeue_resets_repeat_to_start():
    """A manual ``requeue`` returns the repeat to its start date.

    repeat-and-time.rst warns about this asymmetry: the scheduler's internal
    requeue during loop advancement keeps the current value, but an explicit
    ``requeue`` resets it.
    """
    from takler.core import NodeStatus

    module = _load_module(EXAMPLES_DIR / "step9_repeat_and_time.py")
    flow = module.create_flow()

    daily = flow.find_node("/test/daily")
    task1 = flow.find_node("/test/daily/t1")

    task1.set_node_status(NodeStatus.complete)
    assert daily.repeat.value() == 20240102

    flow.requeue()
    assert daily.repeat.value() == 20240101


def test_step9_time_dependency_waits_for_flow_calendar():
    """``t2``'s ``12:00`` time attribute follows the flow's logical calendar.

    Mirrors the exact scenario walked through in repeat-and-time.rst: the
    dependency blocks while the flow time is before 12:00; once the calendar
    reaches 12:00 a free latch keeps it satisfied (so later times still pass);
    requeuing the node re-arms the dependency.
    """
    import datetime

    module = _load_module(EXAMPLES_DIR / "step9_repeat_and_time.py")
    flow = module.create_flow()

    task2 = flow.find_node("/test/t2")

    # Start the flow's logical calendar at 11:59: the 12:00 time dependency
    # is not yet satisfied.
    flow.calendar.begin(datetime.datetime(2024, 1, 1, 11, 59))
    assert task2.resolve_time_dependencies() is False

    # One (real) minute later the flow time reaches 12:00 and the dependency
    # is satisfied.
    flow.update_calendar(flow.calendar.last_real_time + datetime.timedelta(minutes=1))
    assert flow.calendar.flow_time.hour == 12
    assert flow.calendar.flow_time.minute == 0
    assert task2.resolve_time_dependencies() is True

    # The free latch keeps it satisfied after the exact minute has passed...
    flow.update_calendar(flow.calendar.last_real_time + datetime.timedelta(minutes=1))
    assert task2.resolve_time_dependencies() is True

    # ...until the node is requeued, which re-arms the time dependency.
    task2.requeue()
    assert task2.resolve_time_dependencies() is False


def test_step9_task1_with_repeat_renders_cleanly(cleanup_generated_files):
    """The ``task1_with_repeat.takler`` script renders the repeat date variable."""
    module = _load_module(EXAMPLES_DIR / "step9_repeat_and_time.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/daily/t1")
    assert task1.check_job_creation()

    jobs = list((EXAMPLES_DIR / "test" / "daily").glob("t1.job*"))
    assert len(jobs) == 1
    assert "processing date 20240101" in jobs[0].read_text()


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
