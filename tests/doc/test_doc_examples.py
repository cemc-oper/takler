"""Tests for the tutorial example scripts under ``doc/examples/`` and for
behavioral claims made by the user-guide pages under ``doc/source/guide/``.

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
import re
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


def test_step7_complete_trigger_completes_task_without_running_it():
    """``step7_complete_trigger.py``'s ``t2`` completes once ``t1`` sets event ``a``.

    Mirrors the exact scenario walked through in complete-triggers.rst: the
    complete trigger is evaluated during dependency resolution and, once
    satisfied, the node is marked ``complete`` directly without a job ever
    being submitted.
    """
    from takler.core import NodeStatus

    module = _load_module(EXAMPLES_DIR / "step7_complete_trigger.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    task2 = flow.find_node("/test/t2")

    assert task2.complete_trigger_expression is not None
    assert task2.complete_trigger_expression.expression_str == "./t1:a == set"

    flow.requeue()
    assert task2.evaluate_complete_trigger() is False

    # Once t1 sets the event, the complete trigger is satisfied...
    task1.set_event("a", True)
    assert task2.evaluate_complete_trigger() is True

    # ...and dependency resolution completes t2 without running its script.
    flow.resolve_dependencies()
    assert task2.is_complete_triggered is True
    assert task2.state.node_status == NodeStatus.complete
    assert task2.try_no == 0


def test_step8_event_trigger_gates_downstream_task():
    """``step8_events.py`` gates ``t2`` on t1's self-reported event ``a``.

    Mirrors the exact scenario walked through in events.rst: ``t2`` waits for
    event ``a`` to be set; the trigger does not resolve while ``t1`` has not
    reported, and resolves as soon as the event arrives — without waiting for
    ``t1`` to complete.
    """
    module = _load_module(EXAMPLES_DIR / "step8_events.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    task2 = flow.find_node("/test/t2")

    flow.requeue()
    assert task2.resolve_dependencies() is False

    # Setting the event releases t2 while t1 is still running.
    task1.set_event("a", True)
    assert task2.resolve_dependencies() is True


def test_step8_requeue_resets_events():
    """``requeue`` returns t1's event to its initial value.

    events.rst promises that a requeued node reports from a clean slate: the
    event falls back to ``initial_value`` (``unset``), so downstream triggers
    wait for a fresh report on the next run.
    """
    module = _load_module(EXAMPLES_DIR / "step8_events.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    task1.set_event("a", True)

    task1.requeue()

    assert task1.find_event("a").value is False


def test_step8_task1_with_event_renders_cleanly(cleanup_generated_files):
    """The ``task1_with_event.takler`` script renders without Jinja2 errors."""
    from takler.tasks.shell import ShellScriptTask

    test_dir = EXAMPLES_DIR / "test"
    task1 = ShellScriptTask("t1", str(test_dir / "task1_with_event.takler"))
    task1.add_parameter("TAKLER_HOME", str(test_dir))
    task1.update_generated_parameters()

    assert task1.check_job_creation()


def test_step9_meter_trigger_gates_downstream_task():
    """``step9_meters.py`` gates ``t2`` on t1's meter ``step`` reaching 50.

    Mirrors the exact scenario walked through in meters.rst: ``t2`` waits for
    meter ``step`` to reach 50; values below the threshold still block it, and
    reaching the threshold releases it while ``t1`` is still running.
    """
    module = _load_module(EXAMPLES_DIR / "step9_meters.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    task2 = flow.find_node("/test/t2")

    flow.requeue()
    assert task2.resolve_dependencies() is False

    # Meter below the threshold still blocks t2; reaching it releases t2.
    task1.set_meter("step", 25)
    assert task2.resolve_dependencies() is False
    task1.set_meter("step", 50)
    assert task2.resolve_dependencies() is True


def test_step9_requeue_resets_meters():
    """``requeue`` returns t1's meter to the bottom of its range.

    meters.rst promises that a requeued node reports from a clean slate: the
    meter falls back to its range minimum, so downstream triggers wait for
    fresh reports on the next run.
    """
    module = _load_module(EXAMPLES_DIR / "step9_meters.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    task1.set_meter("step", 50)

    task1.requeue()

    assert task1.find_meter("step").value == 0


def test_step9_meter_rejects_values_outside_its_range():
    """Meter updates outside ``[min_value, max_value]`` raise ``ValueError``.

    meters.rst states that out-of-range reports are refused; the example's
    meter ``step`` spans 0~100, so 101 must be rejected.
    """
    module = _load_module(EXAMPLES_DIR / "step9_meters.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")

    with pytest.raises(ValueError):
        task1.set_meter("step", 101)


def test_step9_task1_with_meter_renders_cleanly(cleanup_generated_files):
    """The ``task1_with_meter.takler`` script renders without Jinja2 errors."""
    from takler.tasks.shell import ShellScriptTask

    test_dir = EXAMPLES_DIR / "test"
    task1 = ShellScriptTask("t1", str(test_dir / "task1_with_meter.takler"))
    task1.add_parameter("TAKLER_HOME", str(test_dir))
    task1.update_generated_parameters()

    assert task1.check_job_creation()


def test_step10_repeat_generates_date_variable_for_children():
    """``step10_repeat.py``'s repeat exposes ``TAKLER_DATE`` to children.

    repeat.rst promises that a repeat generates a same-named variable holding
    the current loop value (an integer ``YYYYMMDD``) which the whole subtree —
    including task scripts — can reference.
    """
    module = _load_module(EXAMPLES_DIR / "step10_repeat.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/daily/t1")

    assert task1.parameters()["TAKLER_DATE"].value == 20240101


def test_step10_repeat_advances_and_requeues_when_node_completes():
    """Completing a repeat node advances the date and requeues the subtree.

    Mirrors the exact scenario walked through in repeat.rst: each time
    ``daily`` completes, the repeat moves to the next day and the container
    (with its task) is requeued; past the end date the container stays
    ``complete``.
    """
    from takler.core import NodeStatus

    module = _load_module(EXAMPLES_DIR / "step10_repeat.py")
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


def test_step10_requeue_resets_repeat_to_start():
    """A manual ``requeue`` returns the repeat to its start date.

    repeat.rst warns about this asymmetry: the scheduler's internal requeue
    during loop advancement keeps the current value, but an explicit
    ``requeue`` resets it.
    """
    from takler.core import NodeStatus

    module = _load_module(EXAMPLES_DIR / "step10_repeat.py")
    flow = module.create_flow()

    daily = flow.find_node("/test/daily")
    task1 = flow.find_node("/test/daily/t1")

    task1.set_node_status(NodeStatus.complete)
    assert daily.repeat.value() == 20240102

    flow.requeue()
    assert daily.repeat.value() == 20240101


def test_step10_task1_with_repeat_renders_cleanly(cleanup_generated_files):
    """The ``task1_with_repeat.takler`` script renders the repeat date variable."""
    module = _load_module(EXAMPLES_DIR / "step10_repeat.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/daily/t1")
    assert task1.check_job_creation()

    jobs = list((EXAMPLES_DIR / "test" / "daily").glob("t1.job*"))
    assert len(jobs) == 1
    assert "processing date 20240101" in jobs[0].read_text()


def test_step11_time_dependency_waits_for_flow_calendar():
    """``t2``'s ``12:00`` time attribute follows the flow's logical calendar.

    Mirrors the exact scenario walked through in time.rst: the dependency
    blocks while the flow time is before 12:00; once the calendar reaches
    12:00 a free latch keeps it satisfied (so later times still pass);
    requeuing the node re-arms the dependency.
    """
    import datetime

    module = _load_module(EXAMPLES_DIR / "step11_time.py")
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


def test_step12_limit_tokens_track_task_status_changes():
    """``step12_limits.py``'s limit counts running tasks and gates the rest.

    Mirrors the exact scenario walked through in limits.rst: a task holds one
    token of ``work`` while it is ``submitted``; once both tokens are taken
    ``t3`` may not start; completing or aborting a running task releases its
    token and lets the next task in.
    """
    from takler.core import NodeStatus

    module = _load_module(EXAMPLES_DIR / "step12_limits.py")
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


def test_step12_in_limit_resolves_limit_up_the_node_tree():
    """An in-limit without ``node_path`` finds the nearest limit up the tree.

    limits.rst states the lookup rule mirrors variable lookup: ``t1`` sits
    under ``group1`` which holds ``work``, so the bare ``add_in_limit("work")``
    binds to that limit.
    """
    module = _load_module(EXAMPLES_DIR / "step12_limits.py")
    flow = module.create_flow()

    group1 = flow.find_node("/test/group1")
    task1 = flow.find_node("/test/group1/t1")

    assert task1.find_limit_up("work") is group1.find_limit("work")


def test_step13_builds_expected_tree():
    """``step13_control.py`` defines ``test`` with t1 (event), t2 (trigger), t3 (time)."""
    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    task2 = flow.find_node("/test/t2")
    task3 = flow.find_node("/test/t3")
    assert task1.find_event("a") is not None
    assert task2.trigger_expression.expression_str == "./t1 == complete"
    assert len(task3.times) == 1


def test_step13_begin_starts_calendar_and_rejects_second_begin():
    """``begin`` marks the flow begun and requeues the tree; only begun flows run.

    controlling-the-flow.rst walks through this: the first ``begin`` starts the
    calendar and resets the node tree; a second ``begin`` without ``--force``
    is refused; ``--force`` begins it again.
    """
    from takler.core import Bunch, NodeStatus
    from takler.exceptions import FlowStateError
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    flow = scheduler.bunch.add_flow(module.create_flow())

    assert flow.begun is False

    scheduler.run_command_begin("")

    assert flow.begun is True
    assert flow.state.node_status == NodeStatus.queued

    with pytest.raises(FlowStateError):
        scheduler.run_command_begin("test")

    # ``--force`` begins an already begun flow again.
    scheduler.run_command_begin("test", force=True)


def test_step13_control_commands_require_a_begun_flow():
    """``requeue``/``run``/``force``/``free-dep`` on an un-begun flow are refused.

    controlling-the-flow.rst notes that a freshly ``load``-ed flow (or any flow
    before its first ``begin``) rejects these commands with a flow-state error.
    """
    from takler.core import Bunch
    from takler.exceptions import FlowStateError
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    scheduler.bunch.add_flow(module.create_flow())

    with pytest.raises(FlowStateError):
        scheduler.run_command_requeue("/test")
    with pytest.raises(FlowStateError):
        scheduler.run_command_run("/test/t1")
    with pytest.raises(FlowStateError):
        scheduler.run_command_force("/test/t1", "complete")
    with pytest.raises(FlowStateError):
        scheduler.run_command_free_dep("/test/t3", "time")


def test_step13_suspending_a_flow_blocks_its_whole_subtree():
    """A suspended container is never descended into, so its children never run.

    controlling-the-flow.rst states the suspended marker is orthogonal to node
    status: the child itself is not flagged, but the scheduler's dependency
    walk stops at the suspended container.
    """
    from takler.core import Bunch
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    flow = scheduler.bunch.add_flow(module.create_flow())
    scheduler.run_command_begin("")

    scheduler.run_command_suspend("/test")

    assert flow.is_suspended() is True
    # The marker is not pushed down to children...
    task1 = flow.find_node("/test/t1")
    assert task1.state.suspended is False
    # ...but a suspended container's own dependency check fails, so the walk
    # never reaches the children (see NodeContainer.resolve_dependencies).
    assert flow.check_dependencies() is False

    scheduler.run_command_resume("/test")
    assert flow.is_suspended() is False


def test_step13_force_sets_node_status_recursively():
    """``force`` rewrites node status regardless of its current value.

    Mirrors controlling-the-flow.rst: with recursion on (the CLI default) the
    whole subtree takes the status; with ``--no-recursive`` only the target
    node changes and the parents re-aggregate from their children.
    """
    from takler.core import Bunch, NodeStatus
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    flow = scheduler.bunch.add_flow(module.create_flow())
    scheduler.run_command_begin("")

    scheduler.run_command_force("/test", "complete", recursive=True)

    assert flow.state.node_status == NodeStatus.complete
    for child in flow.children:
        assert child.state.node_status == NodeStatus.complete

    # Non-recursive: only t1 changes; the flow re-aggregates to queued.
    scheduler.run_command_force("/test/t1", "queued", recursive=False)

    assert flow.find_node("/test/t1").state.node_status == NodeStatus.queued
    assert flow.state.node_status == NodeStatus.queued


def test_step13_force_sets_and_clears_events():
    """``force set|clear`` on a ``node:event`` path toggles the event.

    controlling-the-flow.rst uses ``force set /test/t1:a`` as the example; an
    unsupported state is rejected and leaves the event untouched.
    """
    from takler.core import Bunch
    from takler.exceptions import UnsupportedValueError
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    flow = scheduler.bunch.add_flow(module.create_flow())
    scheduler.run_command_begin("")

    event_a = flow.find_node("/test/t1").find_event("a")
    assert event_a.value is False

    scheduler.run_command_force("/test/t1:a", "set")
    assert event_a.value is True

    scheduler.run_command_force("/test/t1:a", "clear")
    assert event_a.value is False

    with pytest.raises(UnsupportedValueError):
        scheduler.run_command_force("/test/t1:a", "bogus")
    assert event_a.value is False


def test_step13_free_dep_releases_time_and_trigger():
    """``free-dep`` marks a dependency as satisfied for the current run.

    Mirrors controlling-the-flow.rst: freeing ``t2``'s trigger makes the
    trigger expression evaluate to true without ``t1`` completing; freeing
    ``t3``'s time attribute sets its free latch without the calendar reaching
    12:00. A requeue re-arms both.
    """
    from takler.core import Bunch
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    flow = scheduler.bunch.add_flow(module.create_flow())
    scheduler.run_command_begin("")

    task2 = flow.find_node("/test/t2")
    task3 = flow.find_node("/test/t3")
    assert task2.evaluate_trigger() is False
    assert task3.times[0].free is False

    scheduler.run_command_free_dep("/test/t2", "trigger")
    scheduler.run_command_free_dep("/test/t3", "time")

    assert task2.evaluate_trigger() is True
    assert task3.times[0].free is True

    # Requeuing re-arms the freed dependencies.
    flow.requeue()
    assert task2.evaluate_trigger() is False
    assert task3.times[0].free is False


def test_step13_run_skips_a_submitted_task():
    """``run`` on a submitted/active task is a no-op unless forced.

    controlling-the-flow.rst states the guard exists so one task never runs two
    jobs at once; the forced form is exercised in zombies.rst.
    """
    from takler.core import Bunch, NodeStatus
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    flow = scheduler.bunch.add_flow(module.create_flow())
    scheduler.run_command_begin("")

    task1 = flow.find_node("/test/t1")
    task1.set_node_status(NodeStatus.submitted)

    assert scheduler.run_command_run("/test/t1") is False
    assert task1.state.node_status == NodeStatus.submitted

    # ``run`` only applies to tasks; targeting a container is refused.
    assert scheduler.run_command_run("/test") is False


def test_step13_load_registers_a_flow_without_beginning_it():
    """``load`` registers a JSON flow definition; the flow starts un-begun.

    controlling-the-flow.rst loads ``Flow.to_dict`` output and stresses that an
    explicit ``begin`` is still required before the scheduler touches the flow.
    """
    import json

    from takler.core import Bunch
    from takler.exceptions import InvalidRequestError
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    flow = module.create_flow()

    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    scheduler.run_command_load("json", json.dumps(flow.to_dict()).encode())

    loaded = scheduler.bunch.find_flow("test")
    assert loaded is not None
    assert loaded.begun is False
    assert loaded.find_node("/test/t2") is not None

    with pytest.raises(InvalidRequestError):
        scheduler.run_command_load("json", b"not a json")


def test_step13_try_no_and_job_password_lifecycle():
    """Each run attempt gets a fresh ``try_no`` and job password; requeue clears both.

    zombies.rst builds its zombie story on this invariant: the job file of
    attempt *n* is ``<node>.job<n>`` and its script carries the ``TAKLER_PASS``
    generated for that attempt, so a report from attempt *n-1* can be told
    apart from the current run.
    """
    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    assert task1.try_no == 0
    assert task1.job_password is None

    # First run attempt.
    task1.increment_try_no()
    first_password = task1.job_password
    assert task1.try_no == 1
    assert first_password

    # A resubmission is a new attempt: try_no goes up and the password rotates.
    task1.increment_try_no()
    assert task1.try_no == 2
    assert task1.job_password != first_password

    # Requeue resets the run bookkeeping.
    task1.requeue()
    assert task1.try_no == 0
    assert task1.job_password is None


def test_step13_zombie_rejected_after_requeue():
    """A report arriving after its task was requeued hits zombie condition Z2.

    Mirrors the exact scenario walked through in zombies.rst: the task is
    requeued while its job is still running, so when the old job's ``complete``
    arrives the task is ``queued`` -- a status in which no job should be
    reporting. The default ``fail`` policy raises ``ZombieError``.
    """
    from takler.core import NodeStatus
    from takler.exceptions import ZombieError
    from takler.server.zombie import ChildAction, ZombieCondition, ZombieDetector

    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    flow = module.create_flow()
    task1 = flow.find_node("/test/t1")
    detector = ZombieDetector()

    # A report from the current (submitted) run is not a zombie.
    task1.set_node_status(NodeStatus.submitted)
    assert detector.detect(task1, "complete") is None
    assert detector.guard(task1, "complete") is ChildAction.PROCEED

    # Requeue while the old job is still running; its report is a zombie.
    task1.requeue()
    assert detector.detect(task1, "complete") is ZombieCondition.Z2
    with pytest.raises(ZombieError):
        detector.guard(task1, "complete")

    # The ``fob`` policy drops the report but answers success.
    from takler.server.connect_config import ZombiePolicy

    fob_detector = ZombieDetector(zombie_policy=ZombiePolicy.FOB)
    assert fob_detector.guard(task1, "complete") is ChildAction.SKIP


def test_step13_zombie_conditions_z1_and_z3():
    """Z1 checks the job password (auth enabled only); Z3 catches a second init.

    zombies.rst explains both: Z1 only applies when the server authenticates
    callers, and Z3 fires when an ``init`` names a different job id than the
    one recorded for the active task.
    """
    from takler.core import NodeStatus
    from takler.server.auth import CallCredentials
    from takler.server.connect_config import AuthMode
    from takler.server.zombie import ZombieCondition, ZombieDetector

    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    flow = module.create_flow()
    task1 = flow.find_node("/test/t1")

    task1.increment_try_no()
    task1.init("job-pid-1")
    assert task1.state.node_status == NodeStatus.active

    # Z3: an init from a different job id claims the already active task.
    detector = ZombieDetector()
    assert detector.detect(task1, "init", "job-pid-2") is ZombieCondition.Z3
    assert detector.detect(task1, "init", "job-pid-1") is None

    # Z1 is skipped entirely with the default (disabled) auth mode...
    stale = CallCredentials(job_password="stale-password")
    assert detector.detect(task1, "complete", credentials=stale) is None

    # ...and fires once authentication is enabled.
    auth_detector = ZombieDetector(auth_mode=AuthMode.ENABLED)
    assert (
        auth_detector.detect(task1, "complete", credentials=stale) is ZombieCondition.Z1
    )
    current = CallCredentials(job_password=task1.job_password)
    assert auth_detector.detect(task1, "complete", credentials=current) is None


def test_step13_checkpoint_restore_keeps_in_flight_tasks(tmp_path):
    """A restarted server restores in-flight tasks so their jobs can still report.

    Mirrors the exact scenario walked through in restart.rst: the server is
    killed while ``t1`` is active, the checkpoint holds its status and job
    password, and after a restore the old job's ``complete`` passes the zombie
    guard instead of being rejected.
    """
    import json

    from takler.core import Bunch, NodeStatus
    from takler.server.auth import CallCredentials
    from takler.server.checkpoint import CheckpointManager
    from takler.server.zombie import ZombieDetector

    module = _load_module(EXAMPLES_DIR / "step13_control.py")
    bunch = Bunch(name="bunch")
    flow = bunch.add_flow(module.create_flow())
    flow.begin()

    # t1 is in flight: one run attempt, initialized by its job.
    task1 = flow.find_node("/test/t1")
    task1.increment_try_no()
    task1.init("job-pid-1")
    password = task1.job_password

    checkpoint_file = tmp_path / "takler.check"
    manager = CheckpointManager(bunch, checkpoint_file=checkpoint_file)
    assert manager.write_checkpoint() is True

    # Only in-flight tasks get their password persisted.
    payload = json.loads(checkpoint_file.read_text(encoding="utf-8"))
    assert list(payload["job_passwords"]) == ["/test/t1"]

    # The server restarts: a fresh bunch restores from the snapshot.
    restored_bunch = Bunch(name="bunch")
    restored_manager = CheckpointManager(
        restored_bunch, checkpoint_file=checkpoint_file
    )
    assert restored_manager.restore() is True

    restored_task1 = restored_bunch.find_node("/test/t1")
    assert restored_task1.state.node_status == NodeStatus.active
    assert restored_task1.try_no == 1
    assert restored_task1.job_password == password

    # The still-running job's report passes the zombie guard.
    detector = ZombieDetector()
    assert detector.detect(restored_task1, "complete") is None
    assert (
        detector.detect(
            restored_task1,
            "complete",
            credentials=CallCredentials(job_password=password),
        )
        is None
    )

    # A queued task was never in flight: it restored without a password and a
    # report against it is a zombie (Z2).
    restored_task2 = restored_bunch.find_node("/test/t2")
    from takler.server.zombie import ZombieCondition

    assert detector.detect(restored_task2, "complete") is ZombieCondition.Z2


def test_guide_node_status_is_an_ordered_enum():
    """node-status.rst states the six statuses form an ordered enum.

    The ordering ``unknown < complete < queued < submitted < active <
    aborted`` is what the container aggregation rule below relies on.
    """
    from takler.core import NodeStatus

    assert (
        NodeStatus.unknown
        < NodeStatus.complete
        < NodeStatus.queued
        < NodeStatus.submitted
        < NodeStatus.active
        < NodeStatus.aborted
    )


def test_guide_container_status_is_the_most_significant_child():
    """A container's status is the numerically largest of its children.

    Mirrors the aggregation rule in node-status.rst: ``aborted`` wins over
    everything, ``complete`` only shows when every sibling has completed, and
    a lone ``queued`` child keeps the container ``queued`` even if the rest
    are ``complete``.
    """
    from takler.core import Flow, NodeStatus

    flow = Flow("test")
    task1 = flow.add_task("t1")
    task2 = flow.add_task("t2")

    combos = [
        ((NodeStatus.complete, NodeStatus.complete), NodeStatus.complete),
        ((NodeStatus.complete, NodeStatus.queued), NodeStatus.queued),
        ((NodeStatus.complete, NodeStatus.active), NodeStatus.active),
        ((NodeStatus.complete, NodeStatus.aborted), NodeStatus.aborted),
        ((NodeStatus.queued, NodeStatus.submitted), NodeStatus.submitted),
    ]
    for (status1, status2), expected in combos:
        task1.set_node_status_only(status1)
        task2.set_node_status_only(status2)
        assert flow.computed_status(immediate=True) == expected


def test_guide_default_node_status_only_allows_queued_or_complete():
    """``set_default_node_status`` rejects transient statuses.

    node-status.rst documents that only ``queued`` and ``complete`` are valid
    requeue targets, and that a container whose default is ``complete`` sinks
    ``complete`` over its whole subtree on requeue.
    """
    from takler.core import Flow, NodeStatus
    from takler.exceptions import UnsupportedValueError

    flow = Flow("test")
    task1 = flow.add_task("t1")

    for bad_status in (
        NodeStatus.unknown,
        NodeStatus.submitted,
        NodeStatus.active,
        NodeStatus.aborted,
    ):
        with pytest.raises(UnsupportedValueError):
            task1.set_default_node_status(bad_status)

    task1.set_default_node_status(NodeStatus.complete)
    task1.requeue()
    assert task1.state.node_status == NodeStatus.complete

    # A container with default status complete sinks it over the subtree.
    group1 = flow.add_container("group1")
    task2 = group1.add_task("t2")
    group1.set_default_node_status(NodeStatus.complete)
    group1.requeue()
    assert task2.state.node_status == NodeStatus.complete


def test_guide_suspended_flag_is_orthogonal_to_status():
    """Suspending a node never changes its node status.

    node-status.rst documents ``State = node_status + suspended`` as two
    orthogonal parts: suspend only stops the scheduler from visiting the
    node, the status itself is left untouched.
    """
    from takler.core import Flow, NodeStatus

    flow = Flow("test")
    task1 = flow.add_task("t1")
    task1.set_node_status_only(NodeStatus.queued)

    task1.suspend()
    assert task1.is_suspended()
    assert task1.state.node_status == NodeStatus.queued
    assert not task1.check_dependencies()

    task1.resume()
    assert not task1.is_suspended()
    assert task1.state.node_status == NodeStatus.queued


def test_guide_serialization_tree_restores_definition_status_restores_state():
    """``Tree`` restores the definition only, ``Status`` also the runtime state.

    defining-flows.rst documents the two ``SerializationType`` modes: the
    client ``load`` command uses ``Tree`` (fresh definition, un-begun), the
    server checkpoint uses ``Status`` (status, begun flag and calendar all
    restored).
    """
    from takler.core import Flow, NodeStatus, SerializationType

    flow = Flow("test")
    task1 = flow.add_task("t1")
    flow.begin()
    task1.set_node_status_only(NodeStatus.active)

    d = flow.to_dict()

    tree_copy = Flow.from_dict(d, method=SerializationType.Tree)
    assert tree_copy.begun is False
    assert tree_copy.find_node("/test/t1").state.node_status == NodeStatus.unknown

    status_copy = Flow.from_dict(d, method=SerializationType.Status)
    assert status_copy.begun is True
    assert status_copy.find_node("/test/t1").state.node_status == NodeStatus.active


def test_guide_bunch_holds_multiple_flows_and_server_parameters():
    """A ``Bunch`` indexes flows by name and exposes server parameters.

    defining-flows.rst states that flows live in one ``Bunch`` per server,
    are looked up by name, and that the server-level parameters
    (``TAKLER_HOST``/``TAKLER_PORT``) are visible from any node through the
    parameter inheritance chain.
    """
    from takler.core import Bunch, Flow
    from takler.exceptions import NodeNotFoundError

    bunch = Bunch()
    flow1 = bunch.add_flow(Flow("flow1"))
    flow2 = bunch.add_flow("flow2")
    task1 = flow1.add_task("t1")

    assert bunch.find_flow("flow2") is flow2
    assert bunch.find_node("/flow1/t1") is task1
    assert task1.find_parent_parameter("TAKLER_HOST") is not None
    assert task1.find_parent_parameter("TAKLER_PORT") is not None

    with pytest.raises(NodeNotFoundError):
        bunch.delete_flow("missing")


def test_guide_with_statement_builds_the_same_tree():
    """The ``with``-statement style builds the same tree as plain calls.

    defining-flows.rst presents both styles side by side and claims they are
    equivalent; this test pins that claim.
    """
    from takler.core import Flow
    from takler.tasks.shell import ShellScriptTask

    flow = Flow("test")
    flow.add_task(ShellScriptTask("t1"))
    group1 = flow.add_container("group1")
    group1.add_task(ShellScriptTask("t2"))

    with Flow("test") as flow2:
        with flow2.add_task(ShellScriptTask("t1")):
            pass
        with flow2.add_container("group1") as group2:
            with group2.add_task(ShellScriptTask("t2")):
                pass

    assert [c.name for c in flow2.children] == ["t1", "group1"]
    assert flow2.find_node("/test/t1") is not None
    assert flow2.find_node("/test/group1/t2") is not None


def test_guide_task_decorator_runs_the_function_inline():
    """The ``task`` decorator wraps a function as an inline ``Task``.

    defining-flows.rst shows ``@task("notify")`` producing a task that runs
    the function between ``init`` and ``complete`` inside the server process.
    """
    from takler.core import Flow, NodeStatus, task

    calls = []

    @task("notify")
    def notify(self):
        calls.append(self.node_path)

    flow = Flow("test")
    notify_task = notify()
    flow.add_task(notify_task)

    notify_task.run()

    assert notify_task.state.node_status == NodeStatus.complete
    assert notify_task.try_no == 1
    assert calls == ["/test/notify"]


def test_guide_async_task_decorator_run_is_a_coroutine_function():
    """``async_task`` builds a task whose ``run`` is a coroutine function.

    defining-flows.rst warns that ``async_task`` does not work with the
    current scheduler: ``resolve_dependencies`` calls ``run()``
    synchronously, so the coroutine is never awaited and the function body
    never executes. This test pins that limitation; if ``async_task`` is
    fixed to run properly, this test fails and the doc note must be updated.
    """
    import inspect

    from takler.core import Flow, NodeStatus, async_task

    calls = []

    @async_task("anotify")
    async def anotify(self):
        calls.append(self.node_path)

    flow = Flow("test")
    async_task_node = anotify()
    flow.add_task(async_task_node)

    assert inspect.iscoroutinefunction(async_task_node.run)

    # The scheduler calls run() synchronously: the coroutine is never
    # awaited, the body never runs, and the status stays where it was.
    with pytest.warns(RuntimeWarning, match="never awaited"):
        async_task_node.run()
    assert calls == []
    assert async_task_node.state.node_status == NodeStatus.unknown


def test_guide_check_job_creation_is_a_dry_run(cleanup_generated_files):
    """``check_job_creation`` renders job scripts without submitting anything.

    defining-flows.rst calls it a dry run: job files appear under
    ``TAKLER_HOME`` but the task keeps its initial status and ``try_no``.
    """
    from takler.core import NodeStatus
    from takler.tasks.shell import check_job_creation

    module = _load_module(EXAMPLES_DIR / "step1_define_flow.py")
    flow = module.create_flow()
    task1 = flow.find_node("/test/t1")

    check_job_creation(flow)

    assert (EXAMPLES_DIR / "test" / "t1.job0").exists()
    assert task1.state.node_status == NodeStatus.unknown
    assert task1.try_no == 0


def test_guide_trigger_grammar_accepts_documented_forms():
    """Every expression form listed in trigger-expression.rst must parse.

    Covers absolute/relative node paths, the three status words, ``eq`` as
    an alias of ``==``, case-insensitivity of keywords, event set/unset,
    integer meter comparison, parameter-to-variable comparison and the
    parenthesized ``+`` form.
    """
    from takler.core.expression_parser import parse_trigger

    accepted = [
        "./t1 == complete",
        "/test/t1 eq COMPLETE",
        "./t1 == AbOrTeD",
        "./t1 == complete AND ./t2 == active",
        "./t1 == complete Or ./t2 == complete",
        "(./t1 == aborted or ./t2 == aborted) and ./t3 == complete",
        "../group1/t2 == complete",
        "./t1:event1 == set",
        "./t1:event1 == UnSet",
        "./t1:meter1 >= 4",
        "./t1:THRESHOLD <= ./t2:meter1",
        "(./t1:m1 + ./t2:m2) >= 10",
    ]
    for expression in accepted:
        parse_trigger(expression)


def test_guide_trigger_grammar_rejects_documented_mistakes():
    """Each wrong form in trigger-expression.rst's mistake table must fail."""
    from takler.core.expression_parser import parse_trigger
    from takler.exceptions import ExpressionSyntaxError

    rejected = [
        "t1 == complete",  # bare name: path must start with /, ./ or ../
        "./t1 == queued",  # queued/submitted/unknown are not status words
        "./t1 == submitted",
        "./t1 == unknown",
        "./t1 = complete",  # single = is not a comparison operator
        "./t1:m1 == 4.5",  # number literals are integers only
        "./t1 == ./t2 == complete",  # comparisons do not chain
        "./t1:m1 + ./t2:m2 >= 10",  # + inside a comparison needs parentheses
        "./t1 == complete and",  # trailing operator
    ]
    for expression in rejected:
        with pytest.raises(ExpressionSyntaxError):
            parse_trigger(expression)


def test_guide_trigger_syntax_error_reports_line_and_column():
    """``ExpressionSyntaxError`` carries 1-based line/column, None at EOF.

    trigger-expression.rst documents the position attributes: reported by
    the underlying parser for mid-expression errors, ``None`` when the error
    is at the end of the input.
    """
    from takler.core.expression_parser import parse_trigger
    from takler.exceptions import ExpressionSyntaxError

    with pytest.raises(ExpressionSyntaxError) as exc_info:
        parse_trigger("./t1 = complete")
    assert exc_info.value.expression == "./t1 = complete"
    assert exc_info.value.line == 1
    assert exc_info.value.column == 6

    with pytest.raises(ExpressionSyntaxError) as exc_info:
        parse_trigger("./t1 == queued")
    assert exc_info.value.line == 1
    assert exc_info.value.column == 9

    with pytest.raises(ExpressionSyntaxError) as exc_info:
        parse_trigger("./t1 == complete and")
    assert exc_info.value.line is None
    assert exc_info.value.column is None


def test_guide_trigger_and_binds_tighter_than_or():
    """``a or b and c`` evaluates as ``a or (b and c)``.

    Pins the precedence table in trigger-expression.rst.
    """
    from takler.core import Flow, NodeStatus

    flow = Flow("test")
    task1 = flow.add_task("t1")
    task2 = flow.add_task("t2")
    task1.set_node_status_only(NodeStatus.active)
    task2.set_node_status_only(NodeStatus.aborted)

    probe = flow.add_task("probe")
    probe.add_trigger("./t1 == active or ./t2 == complete and ./t1 == complete")
    # a is True, b and c are both False: True iff and binds tighter than or.
    assert probe.evaluate_trigger()

    probe2 = flow.add_task("probe2")
    probe2.add_trigger("(./t1 == active or ./t2 == complete) and ./t1 == complete")
    assert not probe2.evaluate_trigger()


def test_guide_trigger_compares_status_event_meter_and_parameter():
    """Status words, set/unset, meter integers and parameters all evaluate.

    Mirrors the worked examples in trigger-expression.rst: events compare
    against ``set``/``unset``, meters against integers, and a parameter can
    appear on either side of a comparison.
    """
    from takler.core import Flow, NodeStatus

    flow = Flow("test")
    group1 = flow.add_container("g1")
    task2 = group1.add_task("t2")
    task2.add_meter("m1", 0, 10)
    task2.add_event("e1")
    task2.add_parameter("THRESHOLD", 4)

    probe = group1.add_task("probe")
    probe.add_trigger("./t2:m1 >= 4 and ./t2:e1 == unset")
    assert not probe.evaluate_trigger()  # m1 is 0

    task2.find_meter("m1").value = 5
    assert probe.evaluate_trigger()  # m1 ok, e1 still unset

    task2.set_event("e1", True)
    assert not probe.evaluate_trigger()  # e1 is set now

    probe.add_trigger("./t2:e1 == set and ./t2:m1 >= ./t2:THRESHOLD")
    assert probe.evaluate_trigger()

    task2.set_node_status_only(NodeStatus.active)
    probe.add_trigger("./t2 == active")
    assert probe.evaluate_trigger()


def test_guide_trigger_missing_node_and_variable_fail_at_first_evaluation():
    """Bad paths and bad variables surface as errors on first evaluation.

    trigger-expression.rst documents that parsing is lazy: a missing node
    raises ``NodeNotFoundError`` and a missing variable raises
    ``ExpressionSyntaxError`` when the trigger is first evaluated, not when
    ``add_trigger`` is called.
    """
    from takler.core import Flow
    from takler.exceptions import ExpressionSyntaxError, NodeNotFoundError

    flow = Flow("test")
    flow.add_task("t1")

    bad_path = flow.add_task("bad_path")
    bad_path.add_trigger("./nope == complete")
    with pytest.raises(NodeNotFoundError):
        bad_path.evaluate_trigger()

    bad_variable = flow.add_task("bad_variable")
    bad_variable.add_trigger("./t1:nosuchvar == 1")
    with pytest.raises(ExpressionSyntaxError):
        bad_variable.evaluate_trigger()


def test_guide_variable_resolution_walks_up_and_shadows():
    """Nearest definition along the parent chain wins (shadowing).

    Pins the resolution order in variables.rst: node, then parents up to the
    flow, then the bunch.
    """
    from takler.core import Bunch, Flow

    bunch = Bunch()
    flow = Flow("test")
    bunch.add_flow(flow)
    group1 = flow.add_container("g1")
    task2 = group1.add_task("t2")
    task3 = flow.add_task("t3")

    flow.add_parameter("G", "flow")
    group1.add_parameter("G", "group")
    task2.add_parameter("G", "task")

    assert task2.find_parent_parameter("G").value == "task"
    assert group1.find_parent_parameter("G").value == "group"
    assert task3.find_parent_parameter("G").value == "flow"

    # The bunch's generated server parameters are the last resort.
    assert task2.find_parent_parameter("TAKLER_HOME").value == "."
    flow.add_parameter("TAKLER_HOME", "/data/flow_home")
    assert task2.find_parent_parameter("TAKLER_HOME").value == "/data/flow_home"


def test_guide_user_parameter_shadows_generated_parameter():
    """A user parameter wins over a same-named generated one on a node.

    variables.rst documents this as the reason generated parameters should
    not be redefined via ``add_parameter`` (and, conversely, why a user
    ``TAKLER_SCRIPT`` can override the generated script path).
    """
    from takler.core import Flow

    flow = Flow("test")
    task1 = flow.add_task("t1")
    assert task1.find_generated_parameter("TASK") is not None

    task1.add_parameter("TASK", "user-defined")
    assert task1.find_parameter("TASK").value == "user-defined"


def test_guide_generated_parameters_fill_in_when_the_node_runs():
    """Generated parameters are None until the flow calendar or job starts.

    Pins variables.rst: flow ``DATE``/``TIME`` appear at the first calendar
    tick after ``begin``, task-level parameters at first job creation, and a
    repeat exposes its current value as a parameter named after the repeat.
    """
    import datetime

    from takler.core import Flow, RepeatDate

    flow = Flow("test")
    task1 = flow.add_task("t1")

    assert flow.find_parameter("DATE").value is None
    assert flow.find_parameter("TIME").value is None
    for name in ("TASK", "TAKLER_NAME", "TAKLER_RID", "TAKLER_TRY_NO", "TAKLER_PASS"):
        assert task1.find_parameter(name).value is None

    flow.begin()
    flow.update_calendar(datetime.datetime.now())
    assert flow.find_parameter("DATE").value == datetime.datetime.now().strftime(
        "%Y-%m-%d"
    )

    task1.add_repeat(RepeatDate("REPEAT_DATE", 20260101, 20261231, 1))
    assert task1.find_parameter("REPEAT_DATE").value == 20260101

    # Constants that exist in takler.core.parameter but are NOT generated:
    # FLOW, TAKLER_DATE, TAKLER_TIME and TAKLER_TRIES.
    assert flow.find_parameter("FLOW") is None
    assert flow.find_parameter("TAKLER_DATE") is None
    assert task1.find_parameter("TAKLER_TRIES") is None


def test_guide_shell_task_generated_job_paths(tmp_path):
    """``TAKLER_JOB``/``TAKLER_JOBOUT`` follow the documented patterns.

    variables.rst gives ``{TAKLER_HOME}{node_path}.job{try_no}`` for the job
    file and ``{TAKLER_HOME}{node_path}.{try_no}`` for the output file, both
    resolved to absolute paths.
    """
    from takler.core import Flow
    from takler.tasks.shell import ShellScriptTask

    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", str(tmp_path))
    task1 = flow.add_task(ShellScriptTask("t1", script_path="test/t1.takler"))
    task1.update_generated_parameters()

    assert task1.find_parameter("TAKLER_SCRIPT").value == "test/t1.takler"
    assert task1.find_parameter("TAKLER_JOB").value == tmp_path / "test/t1.job0"
    assert task1.find_parameter("TAKLER_JOBOUT").value == tmp_path / "test/t1.0"


def test_guide_serialization_keeps_user_parameters_only():
    """``to_dict`` writes user parameters, never generated ones.

    variables.rst relies on this for ``TAKLER_PASS``: the job password stays
    out of ``show`` output and checkpoint files because it is generated.
    """
    from takler.core import Flow

    flow = Flow("test")
    task1 = flow.add_task("t1")
    task1.add_parameter("GREETING", "hello")
    task1.increment_try_no()  # fills TASK/TAKLER_NAME/TAKLER_PASS/...

    serialized = task1.to_dict()
    assert [p["name"] for p in serialized["user_parameters"]] == ["GREETING"]
    assert "TAKLER_PASS" not in str(serialized)


def test_guide_undefined_variable_renders_as_empty_string(tmp_path):
    """An undefined Jinja2 variable renders empty instead of failing.

    variables.rst warns about this: neither job rendering nor
    ``check_job_creation`` treats an undefined variable as an error, so the
    mistake only shows up in the rendered job file.
    """
    from takler.core import Flow
    from takler.tasks.shell import ShellScriptTask, check_job_creation

    script = tmp_path / "t1.takler"
    script.write_text('echo "{{ DEFINED }}"{{ UNDEFINED_VAR }}\n')

    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", str(tmp_path))
    flow.add_parameter("DEFINED", "yes")
    flow.add_task(ShellScriptTask("t1", script_path=str(script)))

    check_job_creation(flow)

    assert (tmp_path / "test/t1.job0").read_text().strip() == 'echo "yes"'


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


# ---------------------------------------------------------------------------
# guide/task-script.rst
# ---------------------------------------------------------------------------


def _make_shell_flow(tmp_path: Path, script_text: str = "echo hi\n"):
    """Build a one-task flow with a ShellScriptTask rooted at ``tmp_path``."""
    from takler.core import Flow
    from takler.tasks.shell import ShellScriptTask

    script = tmp_path / "t1.takler"
    script.write_text(script_text)
    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", str(tmp_path))
    task1 = flow.add_task(ShellScriptTask("t1", script_path=str(script)))
    return flow, task1


def test_guide_task_script_include_searches_script_dir_first(tmp_path):
    """task-script.rst: the script's own directory wins over TAKLER_INCLUDE.

    Both directories hold a ``frag.takler``; the one next to the script must
    be the one rendered into the job file.
    """
    include_dir = tmp_path / "includes"
    include_dir.mkdir()
    (include_dir / "frag.takler").write_text("from include dir\n")

    flow, task1 = _make_shell_flow(tmp_path, '{% include "frag.takler" %}\n')
    (tmp_path / "frag.takler").write_text("from script dir\n")
    flow.add_parameter("TAKLER_INCLUDE", str(include_dir))

    task1.create_job_script()
    job_text = (tmp_path / "test/t1.job0").read_text()
    assert "from script dir" in job_text

    # Without a script-dir match, TAKLER_INCLUDE is consulted.
    (tmp_path / "frag.takler").unlink()
    task1.create_job_script()
    job_text = (tmp_path / "test/t1.job0").read_text()
    assert "from include dir" in job_text


def test_guide_task_script_include_dirs_searched_in_order(tmp_path):
    """task-script.rst: TAKLER_INCLUDE directories are tried in listed order."""
    dir1 = tmp_path / "inc1"
    dir2 = tmp_path / "inc2"
    dir1.mkdir()
    dir2.mkdir()
    (dir1 / "frag.takler").write_text("first\n")
    (dir2 / "frag.takler").write_text("second\n")

    flow, task1 = _make_shell_flow(tmp_path, '{% include "frag.takler" %}\n')
    flow.add_parameter("TAKLER_INCLUDE", f"{dir1}:{dir2}")

    task1.create_job_script()
    assert "first" in (tmp_path / "test/t1.job0").read_text()


def test_guide_task_script_render_context_is_the_merged_parameter_view(tmp_path):
    """task-script.rst: scripts see parameters inherited up the parent chain."""
    flow, task1 = _make_shell_flow(tmp_path, "echo {{ GREETING }}\n")
    flow.add_parameter("GREETING", "hello from flow")

    task1.create_job_script()
    assert "hello from flow" in (tmp_path / "test/t1.job0").read_text()


def test_guide_task_script_render_failures_raise_job_submission_error(tmp_path):
    """task-script.rst: missing script, unresolvable include and template
    syntax errors all surface as ``JobSubmissionError`` from job creation.
    """
    from takler.exceptions import JobSubmissionError

    # Script file does not exist.
    flow, task1 = _make_shell_flow(tmp_path)
    task1.script_path = str(tmp_path / "no_such.takler")
    task1.user_parameters.pop("TAKLER_SCRIPT", None)
    with pytest.raises(JobSubmissionError):
        task1.create_job_script()

    # Include cannot be resolved in any search path.
    flow, task1 = _make_shell_flow(tmp_path, '{% include "missing.takler" %}\n')
    with pytest.raises(JobSubmissionError):
        task1.create_job_script()

    # Template syntax error.
    flow, task1 = _make_shell_flow(tmp_path, "echo {{ unclosed\n")
    with pytest.raises(JobSubmissionError):
        task1.create_job_script()


# ---------------------------------------------------------------------------
# guide/job-management.rst
# ---------------------------------------------------------------------------


def test_guide_job_management_submission_failure_aborts_without_submitting(tmp_path):
    """job-management.rst: a submission failure aborts the task directly.

    ``ShellRunner.spawn`` needs a running event loop; calling ``run()`` from
    synchronous code therefore exercises the documented path: the error is
    wrapped in ``JobSubmissionError`` and the task goes straight to
    ``aborted`` without ever being ``submitted`` (``try_no`` was still
    incremented beforehand).
    """
    from takler.core import NodeStatus

    flow, task1 = _make_shell_flow(tmp_path)

    task1.run()

    assert task1.state.node_status == NodeStatus.aborted
    assert task1.try_no == 1
    assert task1.aborted_reason.startswith("JobSubmissionError")


def test_guide_job_management_job_command_template_and_override(tmp_path):
    """job-management.rst: TAKLER_SHELL_JOB_CMD defaults to running the job
    file with output redirection, and a user parameter anywhere up the parent
    chain overrides it; the template renders against the same merged
    parameter view as the script.
    """
    from takler.tasks.shell.constant import (
        DEFAULT_TAKLER_SHELL_JOB_CMD,
        TAKLER_SHELL_JOB_CMD,
    )
    from takler.tasks.shell.shell_render import ShellRender

    flow, task1 = _make_shell_flow(tmp_path)
    task1.update_generated_parameters()
    render = ShellRender(task1)

    expected_default = DEFAULT_TAKLER_SHELL_JOB_CMD.replace(
        "{{TAKLER_JOB}}", str(tmp_path / "test/t1.job0")
    ).replace("{{TAKLER_JOBOUT}}", str(tmp_path / "test/t1.0"))
    assert render.render_job_command() == expected_default

    # A definition on the flow overrides the default for the whole subtree.
    flow.add_parameter(TAKLER_SHELL_JOB_CMD, "sh {{TAKLER_JOB}}")
    assert render.render_job_command() == f"sh {tmp_path / 'test/t1.job0'}"


def test_guide_job_management_job_script_permissions_follow_umask(tmp_path):
    """job-management.rst: takler only adds the owner execute bit; the
    read/write bits are whatever the process umask produced.
    """
    import os
    import stat

    flow, task1 = _make_shell_flow(tmp_path)

    previous_umask = os.umask(0o077)
    try:
        task1.create_job_script()
    finally:
        os.umask(previous_umask)

    mode = (tmp_path / "test/t1.job0").stat().st_mode
    assert mode & stat.S_IXUSR  # owner execute bit added by takler
    assert mode & 0o077 == 0  # group/other bits left as the umask created them


def test_guide_job_management_failure_does_not_overwrite_reported_status(tmp_path):
    """job-management.rst: on_job_failure only aborts a task that is still
    submitted/active; a status already reported by a child command wins over
    the wrapper process's exit code.
    """
    from subprocess import CalledProcessError

    from takler.core import NodeStatus

    flow, task1 = _make_shell_flow(tmp_path)
    exc = CalledProcessError(returncode=137, cmd="killed")

    # Still active: the failure aborts the task.
    task1.set_node_status(NodeStatus.active)
    task1.on_job_failure(exc)
    assert task1.state.node_status == NodeStatus.aborted
    assert "CalledProcessError" in task1.aborted_reason

    # Already complete: the failure is logged and skipped.
    task1.requeue()
    task1.set_node_status(NodeStatus.complete)
    task1.on_job_failure(exc)
    assert task1.state.node_status == NodeStatus.complete


def test_guide_job_management_attempt_files_coexist_per_try_no(tmp_path):
    """job-management.rst: each attempt writes its own job file; earlier
    attempts' files are kept alongside.
    """
    flow, task1 = _make_shell_flow(tmp_path)

    task1.increment_try_no()
    task1.create_job_script()
    task1.increment_try_no()
    task1.create_job_script()

    assert (tmp_path / "test/t1.job1").exists()
    assert (tmp_path / "test/t1.job2").exists()


# ---------------------------------------------------------------------------
# guide/attributes/
# ---------------------------------------------------------------------------


def test_guide_attributes_event_name_uniqueness_and_missing_lookup():
    """Claims from guide/attributes/event.rst: duplicate event names raise RuntimeError
    (unless ``check=False``); ``set_event`` returns False for unknown names;
    requeue resets an event to its ``initial_value`` (not necessarily False).
    """
    from takler.core import Flow

    flow = Flow("test")
    task1 = flow.add_task("t1")
    task1.add_event("a", initial_value=True)

    with pytest.raises(RuntimeError, match="duplicate"):
        task1.add_event("a")
    # check=False skips the duplicate check.
    task1.add_event("a", check=False)

    assert task1.set_event("no_such_event", True) is False

    # requeue resets to initial_value (True here, not False).
    task1.set_event("a", False)
    task1.requeue()
    assert task1.find_event("a").value is True


def test_guide_attributes_meter_rejects_out_of_range_on_both_ends():
    """guide/attributes/meter.rst: assigning a value outside [min_value, max_value]
    raises ValueError; ``set_meter`` returns False for unknown names.
    """
    from takler.core import Flow

    flow = Flow("test")
    task1 = flow.add_task("t1")
    meter = task1.add_meter("step", 10, 20)
    assert meter.value == 10  # initial value is the range minimum

    with pytest.raises(ValueError, match=r"\[10, 20\]"):
        meter.value = 9
    with pytest.raises(ValueError, match=r"\[10, 20\]"):
        meter.value = 21

    assert task1.set_meter("no_such_meter", 15) is False


def test_guide_attributes_limit_and_in_limit_reject_duplicates():
    """guide/attributes/limit.rst: duplicate limit names on one node raise RuntimeError;
    so do duplicate in-limit markers with the same name and node_path.
    """
    from takler.core import Flow

    flow = Flow("test")
    group1 = flow.add_container("g")
    group1.add_limit("work", 2)
    with pytest.raises(RuntimeError, match="duplicate limit"):
        group1.add_limit("work", 3)

    task1 = group1.add_task("t1")
    task1.add_in_limit("work")
    with pytest.raises(RuntimeError, match="duplicate InLimit"):
        task1.add_in_limit("work")
    # A different node_path makes it a different marker.
    task1.add_in_limit("work", node_path="/test/g")


def test_guide_attributes_in_limit_reference_resolution():
    """guide/attributes/limit.rst 引用解析 section: with ``node_path=None`` the limit is
    looked up along the parent chain (nearest wins); with an explicit
    ``node_path`` only that node is searched; an unresolvable in-limit marker
    is silently ignored and does not block the task.
    """
    from takler.core import Flow

    flow = Flow("test")
    flow.add_limit("outer", 5)
    group1 = flow.add_container("g")
    group1.add_limit("outer", 2)

    # node_path=None resolves to the nearest limit up the tree.
    task1 = group1.add_task("t1")
    task1.add_in_limit("outer")
    assert task1.in_limit_manager.in_limit() is True
    assert task1.in_limit_manager.in_limit_list[0].limit is group1.find_limit("outer")

    # Explicit node_path searches only that node: /test/t2 has no limit.
    task2 = group1.add_task("t2")
    task2.add_in_limit("outer", node_path="/test/g/t2")
    assert task2.in_limit_manager.in_limit() is True
    assert task2.in_limit_manager.in_limit_list[0].limit is None

    # A marker naming a limit that exists nowhere is ignored, not blocking.
    task3 = group1.add_task("t3")
    task3.add_in_limit("no_such_limit")
    assert task3.check_in_limit_up() is True


def test_guide_attributes_limit_tokens_lifecycle():
    """guide/attributes/limit.rst 占用与释放 section: tokens are occupied at ``submitted``
    and released at ``complete``/``aborted``; requeue does NOT release;
    the same limit is occupied only once per task run even when several
    in-limit markers point at it; ``tokens`` occupies more than one token.
    """
    from takler.core import Flow, NodeStatus

    flow = Flow("test")
    group1 = flow.add_container("g")
    group1.add_limit("work", 2)

    task1 = group1.add_task("t1")
    task1.add_in_limit("work")
    task1.add_in_limit("work", node_path="/test/g")  # same limit, twice

    task1.set_node_status(NodeStatus.submitted)
    limit = group1.find_limit("work")
    assert limit.value == 1  # occupied once despite two markers

    task1.requeue()
    assert limit.value == 1  # requeue does not release

    task1.set_node_status(NodeStatus.submitted)
    task1.set_node_status(NodeStatus.complete)
    assert limit.value == 0

    # tokens=2 occupies two tokens and blocks a further token.
    task2 = group1.add_task("t2")
    task2.add_in_limit("work", tokens=2)
    task2.set_node_status(NodeStatus.submitted)
    assert limit.value == 2
    assert task1.check_in_limit_up() is False

    task2.set_node_status(NodeStatus.aborted)
    assert limit.value == 0


def test_guide_attributes_repeat_date_change_validates_but_setter_does_not():
    """guide/attributes/repeat.rst: ``RepeatDate.change`` rejects values outside the range
    or off the step grid with ValueError; assigning ``value`` directly does
    no validation.
    """
    from takler.core import RepeatDate

    repeat = RepeatDate("D", 20240101, 20240110, step=2)

    with pytest.raises(ValueError, match="in range"):
        repeat.change("20240111")
    with pytest.raises(ValueError, match="multiply step"):
        repeat.change("20240102")

    repeat.change(20240103)  # on the grid: accepted (int form too)
    assert repeat.value == 20240103

    repeat.value = 20240104  # raw setter: no validation
    assert repeat.value == 20240104
    assert repeat.valid() is True  # valid() checks the range only

    # A second add_repeat replaces the existing repeat.
    from takler.core import Flow

    flow = Flow("test")
    task1 = flow.add_task("t1")
    first = task1.add_repeat(RepeatDate("A", 20240101, 20240102))
    second = task1.add_repeat(RepeatDate("B", 20240101, 20240102))
    assert task1.repeat.r is second
    assert task1.repeat.r is not first


def test_guide_attributes_repeat_generates_same_named_parameter():
    """guide/attributes/repeat.rst: a repeat generates a parameter with the repeat's name,
    holding the current value as a YYYYMMDD integer.
    """
    from takler.core import Flow, RepeatDate

    flow = Flow("test")
    daily = flow.add_container("daily")
    daily.add_repeat(RepeatDate("TAKLER_DATE", "20240101", "20240103"))

    param = daily.find_parameter("TAKLER_DATE")
    assert param is not None
    assert param.value == 20240101

    daily.repeat.increment()
    assert daily.find_parameter("TAKLER_DATE").value == 20240102


def test_guide_attributes_time_latch_holds_until_requeue():
    """guide/attributes/time.rst 判定规则 section: a time dependency is satisfied when
    the flow calendar's HH:MM matches; the free latch keeps it satisfied
    afterwards; requeue re-arms it; multiple time attributes are OR-ed.
    """
    import datetime

    from takler.core import Flow

    flow = Flow("test")
    task1 = flow.add_task("t1")
    task1.add_time("12:00")
    task1.add_time(datetime.time(18, 30))  # datetime.time is accepted too

    flow.calendar.begin(datetime.datetime(2024, 1, 1, 11, 59))
    assert task1.resolve_time_dependencies() is False

    flow.update_calendar(flow.calendar.last_real_time + datetime.timedelta(minutes=1))
    assert task1.resolve_time_dependencies() is True

    # The latch holds after the matching minute has passed.
    flow.update_calendar(flow.calendar.last_real_time + datetime.timedelta(minutes=1))
    assert task1.resolve_time_dependencies() is True

    task1.requeue()
    assert task1.resolve_time_dependencies() is False


def test_guide_attributes_time_missed_minute_waits_for_next_day():
    """guide/attributes/time.rst: a time attribute added after its minute has passed is
    not satisfied retroactively -- it stays unmet until that time of day
    comes around again on the logical calendar.
    """
    import datetime

    from takler.core import Flow

    flow = Flow("test")
    flow.calendar.begin(datetime.datetime(2024, 1, 1, 11, 59))
    # The calendar moves past 12:00 before the attribute exists.
    flow.update_calendar(flow.calendar.last_real_time + datetime.timedelta(minutes=2))

    task1 = flow.add_task("t1")
    task1.add_time("12:00")
    flow.update_calendar(flow.calendar.last_real_time + datetime.timedelta(minutes=1))
    assert task1.resolve_time_dependencies() is False

    # Next day at 12:00 the minute matches and the latch is set
    # (flow time is now 12:02, so +23h58m lands on 12:00).
    flow.update_calendar(
        flow.calendar.last_real_time + datetime.timedelta(hours=23, minutes=58)
    )
    assert task1.resolve_time_dependencies() is True


def test_guide_attributes_time_dependency_requires_a_flow():
    """guide/attributes/time.rst: checking time dependencies on a node outside any flow
    raises RuntimeError; ``free_dependencies("time")`` releases the
    dependency immediately.
    """
    from takler.core import Flow, NodeContainer

    orphan = NodeContainer("orphan")
    orphan.add_time("12:00")
    with pytest.raises(RuntimeError, match="should be in a flow"):
        orphan.resolve_time_dependencies()

    flow = Flow("test")
    task1 = flow.add_task("t1")
    task1.add_time("23:59")
    task1.free_dependencies("time")
    assert task1.resolve_time_dependencies() is True


def test_guide_attributes_calendar_and_requeue():
    """guide/attributes/time.rst 日历 section: the calendar advances by real elapsed
    time; ``Flow.requeue`` resets the node tree but leaves the calendar
    untouched.
    """
    import datetime

    from takler.core import Flow, NodeStatus

    flow = Flow("test")
    task1 = flow.add_task("t1")
    flow.calendar.begin(datetime.datetime(2024, 1, 1, 11, 59))
    flow.update_calendar(flow.calendar.last_real_time + datetime.timedelta(minutes=5))
    assert flow.calendar.flow_time == datetime.datetime(2024, 1, 1, 12, 4)

    task1.set_node_status(NodeStatus.complete)
    flow.requeue()
    # Node tree reset, calendar untouched.
    assert task1.state.node_status == NodeStatus.queued
    assert flow.calendar.flow_time == datetime.datetime(2024, 1, 1, 12, 4)


def test_guide_attributes_serialization_tree_keeps_definition_only():
    """guide/attributes/index.rst intro: with SerializationType.Status the runtime values
    survive a round trip; with SerializationType.Tree only definitions do.
    """
    from takler.core import Event, Meter, RepeatDate, TimeAttribute
    from takler.core.util import SerializationType

    event = Event("a", initial_value=True)
    event.value = False
    d = event.to_dict()
    assert Event.from_dict(d, SerializationType.Status).value is False
    assert Event.from_dict(d, SerializationType.Tree).value is True

    meter = Meter("m", 0, 100)
    meter.value = 42
    d = meter.to_dict()
    assert Meter.from_dict(d, SerializationType.Status).value == 42
    assert Meter.from_dict(d, SerializationType.Tree).value == 0

    repeat = RepeatDate("D", 20240101, 20240103)
    repeat.increment()
    d = repeat.to_dict()
    assert RepeatDate.from_dict(d, SerializationType.Status).value == 20240102
    assert RepeatDate.from_dict(d, SerializationType.Tree).value == 20240101

    time_attr = TimeAttribute("12:00")
    time_attr.set_free()
    d = time_attr.to_dict()
    assert TimeAttribute.from_dict(d, SerializationType.Status).free is True
    assert TimeAttribute.from_dict(d, SerializationType.Tree).free is False


# ---------------------------------------------------------------------------
# guide/cli.rst
# ---------------------------------------------------------------------------

GUIDE_DIR = PROJECT_ROOT / "doc" / "source" / "guide"


def _guide_page(name: str) -> str:
    return (GUIDE_DIR / name).read_text(encoding="utf-8")


def _documented_commands(text: str) -> set[str]:
    """Return every ``name`` literal that appears in a section title.

    The CLI reference gives each command a section whose title is the
    command name (two commands may share one title, as in
    "``suspend`` / ``resume``"), so the command table the page claims to
    cover is readable straight from the markup.
    """
    lines = text.splitlines()
    names: set[str] = set()
    for title, underline in zip(lines, lines[1:]):
        if underline.strip() and re.fullmatch(r"[-~^=]+", underline.strip()):
            names.update(re.findall(r"``([a-z][\w-]*)``", title))
    return names


def test_guide_cli_documents_every_python_subcommand():
    """guide/cli.rst has a section for every ``takler-client-py`` subcommand."""
    from typer.main import get_command

    from takler.client.cli import app

    commands = set(get_command(app).commands)
    documented = _documented_commands(_guide_page("cli.rst"))

    assert commands - documented == set()


def test_guide_cli_documents_every_python_option():
    """Every long option of every subcommand is named in guide/cli.rst."""
    from typer.main import get_command

    from takler.client.cli import app

    text = _guide_page("cli.rst")
    missing: dict[str, list[str]] = {}
    for name, command in get_command(app).commands.items():
        options = {
            opt
            for param in command.params
            for opt in list(param.opts) + list(param.secondary_opts)
            if opt.startswith("--")
        }
        absent = sorted(opt for opt in options if opt not in text)
        if absent:
            missing[name] = absent

    assert missing == {}


def test_guide_cli_exit_codes_match_client_exit_code_module():
    """The exit code table in guide/cli.rst lists the codes the CLI uses."""
    from takler.client import exit_code as ec

    text = _guide_page("cli.rst")
    for code in (
        ec.EXIT_OK,
        ec.EXIT_REQUEST_ERROR,
        ec.EXIT_SERVER_ERROR,
        ec.EXIT_UNREACHABLE,
    ):
        assert f"``{code}``" in text


def test_guide_cli_free_dep_dep_type_must_be_explicit(
    monkeypatch: pytest.MonkeyPatch,
):
    """guide/cli.rst warns that ``free-dep`` without ``--dep-type`` is rejected.

    The option's declared default is the string ``"True"``, which is not
    one of the server's accepted values (all / time / trigger), so omitting
    it surfaces as an unsupported-value failure rather than as "all".
    """
    from typer.testing import CliRunner

    import takler.client.cli as cli

    captured = {}

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def free_dep(self, node_paths, dep_type):
            captured["dep_type"] = dep_type
            return None

    monkeypatch.setattr(cli, "TaklerServiceClient", FakeClient)
    result = CliRunner().invoke(cli.app, ["free-dep", "/f/t1"])

    assert result.exit_code == 0, result.output
    assert captured["dep_type"] == "True"


# ---------------------------------------------------------------------------
# guide/tui.rst
# ---------------------------------------------------------------------------


def test_guide_tui_documents_every_key_binding():
    """guide/tui.rst names every key the TUI binds, in the case it shows."""
    pytest.importorskip("textual", reason="the tui extra is not installed")

    from takler.tui.menu import NODE_ACTIONS

    text = _guide_page("tui.rst").lower()
    missing = [
        action.key
        for action in NODE_ACTIONS
        if action.key is not None and action.key not in text
    ]
    assert missing == []


def test_guide_tui_connect_file_only_resolves_the_address(
    monkeypatch: pytest.MonkeyPatch,
):
    """The TUI passes no connect config to its client: the security section
    of ``connect.yaml`` never reaches TLS / secret resolution there, which is
    why the page tells the reader to use the environment variables instead.
    """
    pytest.importorskip("textual", reason="the tui extra is not installed")

    for name in ("TAKLER_TLS_CA_FILE", "TAKLER_TLS_SERVER_NAME", "TAKLER_SECRET_FILE"):
        monkeypatch.delenv(name, raising=False)

    from takler.tui.service import TaklerTuiService

    service = TaklerTuiService(host="localhost", port="33083")
    assert service._inner.ca_file is None
    assert service._inner.secret_file is None


# ---------------------------------------------------------------------------
# guide/ecflow-differences.rst
# ---------------------------------------------------------------------------


def test_guide_ecflow_differences_repeat_has_only_the_date_variant():
    """``RepeatDate`` is the only concrete repeat type takler implements."""
    from takler.core.repeat import RepeatBase, RepeatDate

    concrete = [
        cls for cls in RepeatBase.__subclasses__() if not cls.__abstractmethods__
    ]
    assert concrete == [RepeatDate]


def test_guide_ecflow_differences_default_node_status_is_limited():
    """``default_node_status`` accepts only ``queued`` and ``complete`` —
    the two defstatus values the differences page lists.
    """
    from takler.core import Bunch
    from takler.core.state import NodeStatus
    from takler.exceptions import UnsupportedValueError

    task = Bunch("b").add_flow("f").add_task("t1")
    task.set_default_node_status(NodeStatus.complete)
    with pytest.raises(UnsupportedValueError):
        task.set_default_node_status(NodeStatus.submitted)


# ---------------------------------------------------------------------------
# operation/deployment.rst and operation/connect-config.rst
# ---------------------------------------------------------------------------

OPERATION_DIR = PROJECT_ROOT / "doc" / "source" / "operation"


def _operation_page(name: str) -> str:
    return (OPERATION_DIR / name).read_text(encoding="utf-8")


def test_operation_deployment_documents_every_server_option():
    """operation/deployment.rst names every ``takler-server`` option."""
    from typer.main import get_command

    from takler.server.cli import app

    command = get_command(app)
    options = {
        opt
        for param in command.params
        for opt in list(param.opts) + list(param.secondary_opts)
        if opt.startswith("--")
    }
    text = _operation_page("deployment.rst")

    assert sorted(opt for opt in options if opt not in text) == []


def test_operation_connect_config_documents_every_field():
    """Every connect.yaml field has a fully qualified literal in the page."""
    from takler.server.connect_config import (
        Address,
        CheckpointSettings,
        SecuritySettings,
    )

    expected = {
        *(f"server.address.{f}" for f in Address.model_fields),
        *(f"checkpoint.{f}" for f in CheckpointSettings.model_fields),
        *(f"security.{f}" for f in SecuritySettings.model_fields),
    }
    text = _operation_page("connect-config.rst")

    assert sorted(f for f in expected if f"``{f}``" not in text) == []


def test_operation_connect_config_precedence_env_beats_file():
    """The documented chain explicit > env > connect.yaml > default holds,
    and a blank env value counts as "not provided".
    """
    from takler.server.connect_config import (
        AuthMode,
        ConnectConfig,
        resolve_auth_mode,
    )

    config = ConnectConfig.model_validate(
        {
            "server": {"address": {"hostname": "h", "ip": "i", "port": "1"}},
            "security": {"auth_mode": "enabled"},
        }
    )

    # Environment beats the config file...
    assert (
        resolve_auth_mode(connect_config=config, env={"TAKLER_AUTH_MODE": "disabled"})
        is AuthMode.DISABLED
    )
    # ...a blank environment value is "not provided", so the file applies...
    assert (
        resolve_auth_mode(connect_config=config, env={"TAKLER_AUTH_MODE": "  "})
        is AuthMode.ENABLED
    )
    # ...and an explicit argument beats both.
    assert (
        resolve_auth_mode(
            explicit="disabled",
            connect_config=config,
            env={"TAKLER_AUTH_MODE": "enabled"},
        )
        is AuthMode.DISABLED
    )


def test_operation_connect_config_invalid_enum_values_degrade_to_defaults():
    """Unrecognized policy names fall back to the built-in defaults with a
    warning rather than raising — the "never less resilient" rule the page
    states.
    """
    from takler.server.connect_config import (
        AuthMode,
        ExceptionPolicy,
        ZombiePolicy,
    )

    assert AuthMode.from_str("on") is AuthMode.DISABLED
    assert ZombiePolicy.from_str("kill") is ZombiePolicy.FAIL
    assert ExceptionPolicy.from_str("boom") is ExceptionPolicy.RESILIENT
    # Case and separator tolerance documented on the page.
    assert ExceptionPolicy.from_str("Fail-Fast") is ExceptionPolicy.FAIL_FAST


def test_operation_connect_config_checkpoint_interval_below_minimum_falls_back():
    """A snapshot period below 10 s is rejected and falls back to 120 s."""
    from takler.core import Bunch
    from takler.server.checkpoint import CheckpointManager

    manager = CheckpointManager(bunch=Bunch("b"), interval=5)
    assert manager.interval == 120.0


def test_operation_deployment_server_address_precedence():
    """Server-side address chain: --host/--port > connect.yaml > defaults."""
    from takler.server.cli import resolve_address
    from takler.server.connect_config import ConnectConfig

    config = ConnectConfig.model_validate(
        {"server": {"address": {"hostname": "cfg-host", "ip": "i", "port": "40000"}}}
    )

    assert resolve_address(None, None, config) == ("cfg-host", 40000)
    assert resolve_address("cli-host", 1234, config) == ("cli-host", 1234)
    assert resolve_address(None, None, None) == ("localhost", 33083)


# ---------------------------------------------------------------------------
# operation/checkpoint.rst and operation/zombie.rst
# ---------------------------------------------------------------------------


def _checkpoint_bunch():
    """One submitted task and one complete task, each holding a password.

    The passwords come from ``run`` / ``increment_try_no``, so both tasks hold
    a non-empty one and only the status decides what a snapshot persists.
    """
    from takler.core import Bunch, Flow

    bunch = Bunch("b")
    flow1 = Flow("flow1")
    with flow1:
        flow1.add_task("submitted_task")
        flow1.add_task("complete_task")
    bunch.add_flow(flow1)
    flow1.begin()

    flow1.find_node("/flow1/submitted_task").run()
    complete_task = flow1.find_node("/flow1/complete_task")
    complete_task.increment_try_no()
    complete_task.complete()
    return bunch


def test_operation_checkpoint_documents_snapshot_keys():
    """The page names every top-level key a snapshot carries."""
    import json

    from takler.core import Bunch
    from takler.server.checkpoint import CheckpointManager

    payload = json.loads(CheckpointManager(bunch=Bunch("b")).build_payload())
    text = _operation_page("checkpoint.rst")

    assert sorted(k for k in payload if f"``{k}``" not in text) == []


def test_operation_checkpoint_write_permissions_and_backup(tmp_path):
    """Snapshot files are created 0600, and the backup appears with the
    second write — the two facts the page states about the files on disk.
    """
    import stat

    from takler.server.checkpoint import CheckpointManager

    manager = CheckpointManager(
        bunch=_checkpoint_bunch(), checkpoint_file=tmp_path / "takler.check"
    )
    assert manager.write_checkpoint()
    assert stat.S_IMODE(manager.checkpoint_file.stat().st_mode) == 0o600
    # The first write has no previous snapshot to copy aside.
    assert not manager.backup_file.exists()

    assert manager.write_checkpoint()
    assert manager.backup_file.exists()
    assert stat.S_IMODE(manager.backup_file.stat().st_mode) == 0o600


def test_operation_checkpoint_persists_only_in_flight_passwords():
    """Only submitted/active tasks have their password persisted — the
    reason a restarted server keeps accepting in-flight job reports.
    """
    import json

    from takler.server.checkpoint import JOB_PASSWORDS_KEY, CheckpointManager

    payload = json.loads(CheckpointManager(bunch=_checkpoint_bunch()).build_payload())
    passwords = payload[JOB_PASSWORDS_KEY]

    assert "/flow1/submitted_task" in passwords
    assert "/flow1/complete_task" not in passwords


def test_operation_checkpoint_restore_falls_back_to_backup(tmp_path):
    """The documented chain: corrupt Checkpoint_File -> backup -> empty."""
    from takler.core import Bunch
    from takler.server.checkpoint import CheckpointManager

    writer = CheckpointManager(
        bunch=_checkpoint_bunch(), checkpoint_file=tmp_path / "takler.check"
    )
    assert writer.write_checkpoint() and writer.write_checkpoint()

    (tmp_path / "takler.check").write_text("not json", encoding="utf-8")
    target = CheckpointManager(
        bunch=Bunch("b"), checkpoint_file=tmp_path / "takler.check"
    )
    assert target.restore()
    assert target.bunch.find_node("/flow1/submitted_task") is not None

    (tmp_path / "takler.check").unlink()
    (tmp_path / "takler.check.b").unlink()
    empty = CheckpointManager(
        bunch=Bunch("b"), checkpoint_file=tmp_path / "takler.check"
    )
    assert not empty.restore()


def _zombie_task(status, job_password, task_id="job-1"):
    """A ``/flow1/task1`` in the state a zombie assertion needs."""
    from takler.core import Flow

    flow1 = Flow("flow1")
    with flow1:
        flow1.add_task("task1")
    flow1.begin()

    task1 = flow1.find_node("/flow1/task1")
    task1.set_node_status(node_status=status)
    task1.task_id = task_id
    task1.job_password = job_password
    return task1


def test_operation_zombie_detection_order_and_auth_mode():
    """The old job reporting after a requeue hits Z1 with authentication
    enabled and Z2 without — the order and the auth-mode relation the page
    states.
    """
    from takler.core.state import NodeStatus
    from takler.server.auth import CallCredentials
    from takler.server.connect_config import AuthMode
    from takler.server.zombie import ZombieCondition, detect_zombie_condition

    task = _zombie_task(status=NodeStatus.queued, job_password=None)
    credentials = CallCredentials(job_password="stale-password")

    assert (
        detect_zombie_condition(
            task, "complete", auth_mode=AuthMode.ENABLED, credentials=credentials
        )
        is ZombieCondition.Z1
    )
    assert (
        detect_zombie_condition(
            task, "complete", auth_mode=AuthMode.DISABLED, credentials=credentials
        )
        is ZombieCondition.Z2
    )


def test_operation_zombie_policies():
    """fail raises and changes nothing, fob answers success and changes
    nothing, adopt runs the command and takes over the password.
    """
    import pytest

    from takler.core.state import NodeStatus
    from takler.exceptions import ZombieError
    from takler.server.auth import CallCredentials
    from takler.server.connect_config import ZombiePolicy
    from takler.server.zombie import (
        ChildAction,
        ZombieCondition,
        dispose_zombie,
    )

    def snapshot(task):
        return (
            task.state.node_status,
            task.task_id,
            task.try_no,
            task.aborted_reason,
            task.job_password,
        )

    task = _zombie_task(status=NodeStatus.queued, job_password=None)
    credentials = CallCredentials(job_password="new-password")

    with pytest.raises(ZombieError):
        dispose_zombie(
            task,
            "complete",
            ZombieCondition.Z2,
            policy=ZombiePolicy.FAIL,
            credentials=credentials,
        )
    assert snapshot(task) == (NodeStatus.queued, "job-1", 0, None, None)

    assert (
        dispose_zombie(
            task,
            "complete",
            ZombieCondition.Z2,
            policy=ZombiePolicy.FOB,
            credentials=credentials,
        )
        is ChildAction.SKIP
    )
    assert snapshot(task) == (NodeStatus.queued, "job-1", 0, None, None)

    assert (
        dispose_zombie(
            task,
            "complete",
            ZombieCondition.Z2,
            policy=ZombiePolicy.ADOPT,
            credentials=credentials,
        )
        is ChildAction.PROCEED
    )
    assert task.job_password == "new-password"

    # A blank takler-pass counts as "not carried" and is not adopted.
    task.job_password = None
    dispose_zombie(
        task,
        "complete",
        ZombieCondition.Z2,
        policy=ZombiePolicy.ADOPT,
        credentials=CallCredentials(job_password="  "),
    )
    assert task.job_password is None


def test_operation_zombie_flag_and_exit_code():
    """fail surfaces as flag=31, which both clients map to exit code 3 —
    the contract the policy table documents.
    """
    from takler.client.exit_code import exit_code_for_error_code
    from takler.exceptions import ZombieError
    from takler.protocol.error_code import error_code_for_exception

    assert error_code_for_exception(ZombieError("x")) == 31
    assert exit_code_for_error_code(31) == 3


# ---------------------------------------------------------------------------
# operation/audit.rst, operation/logging.rst and operation/resilience.rst
# ---------------------------------------------------------------------------


def test_operation_audit_documents_record_shape():
    """The page names the eight keys, the three events, the four outcomes
    and the fixed error_code of a rejection — the contract AuditRecord
    implements.
    """
    from takler.server.audit import (
        DENIED_ERROR_CODE,
        EVENT_CONTROL,
        EVENT_DENIED,
        EVENT_ZOMBIE,
        OUTCOME_DENIED,
        OUTCOME_ERROR,
        OUTCOME_SUCCESS,
        OUTCOME_ZOMBIE,
        AuditRecord,
        audit_timestamp,
    )
    import json

    text = _operation_page("audit.rst")
    for key in (
        "timestamp",
        "event",
        "command",
        "user",
        "peer",
        "target",
        "outcome",
        "error_code",
    ):
        assert f"``{key}``" in text
    for value in (
        EVENT_CONTROL,
        EVENT_DENIED,
        EVENT_ZOMBIE,
        OUTCOME_SUCCESS,
        OUTCOME_ERROR,
        OUTCOME_DENIED,
        OUTCOME_ZOMBIE,
        "unknown",
    ):
        assert f"``{value}``" in text
    assert f"``{DENIED_ERROR_CODE}``" in text

    # Field order in the file is the documented key order.
    line = AuditRecord(
        timestamp=audit_timestamp(),
        event=EVENT_CONTROL,
        command="requeue",
        user="oper",
        peer="ipv4:10.0.0.9:51234",
        target=["/flow1/task1"],
        outcome=OUTCOME_SUCCESS,
        error_code=0,
    ).to_json_line()
    assert list(json.loads(line)) == [
        "timestamp",
        "event",
        "command",
        "user",
        "peer",
        "target",
        "outcome",
        "error_code",
    ]


def test_operation_audit_one_record_one_line():
    """A node path holding a line-boundary character cannot split a record
    across two lines, and non-ASCII paths stay readable — the guarantee the
    page states before showing jq examples.
    """
    import json

    from takler.server.audit import AuditRecord

    record = AuditRecord(
        timestamp="2026-07-15T10:30:00.123456",
        event="control",
        command="requeue",
        user="oper",
        peer="unknown",
        target=["/流程1/任务 1"],
        outcome="success",
        error_code=0,
    )
    line = record.to_json_line()

    assert len(line.splitlines()) == 1
    # The CJK part stays readable; the line-boundary character is escaped.
    assert "/流程1/任务" in line
    assert "\\u2028" in line
    assert json.loads(line)["target"] == ["/流程1/任务 1"]


def test_operation_audit_command_name_derivation():
    """The documented derivation RunCommandFreeDep -> free_dep, applied to
    fully qualified and bare method names.
    """
    from takler.server.audit import audit_command_name

    assert audit_command_name("RunCommandFreeDep") == "free_dep"
    assert (
        audit_command_name("/takler_protocol.TaklerServer/RunCommandRequeue")
        == "requeue"
    )
    assert audit_command_name("RunRequestShow") == "show"


def test_operation_audit_audited_command_set():
    """The eight control commands the page lists are exactly what the
    service audits: operator-level write commands, with the read-only
    show/coroutine opted out.
    """
    from takler.server.network_service import CONTROL_METHOD_NAMES

    assert sorted(CONTROL_METHOD_NAMES) == [
        "RunCommandBegin",
        "RunCommandForce",
        "RunCommandFreeDep",
        "RunCommandLoad",
        "RunCommandRequeue",
        "RunCommandResume",
        "RunCommandRun",
        "RunCommandSuspend",
    ]


def test_operation_audit_file_created_owner_only(tmp_path):
    """The first record pre-creates the audit file 0600 (with its parent
    directory), as the page's permissions section states.
    """
    import stat

    from takler.server.audit import AUDIT_FILE_MODE, AuditLogger, AuditRecord

    audit_file = tmp_path / "sub" / "audit.jsonl"
    AuditLogger(audit_file).record(
        AuditRecord(
            timestamp="2026-07-15T10:30:00.123456",
            event="control",
            command="requeue",
            user="oper",
            peer="unknown",
            target=[],
            outcome="success",
            error_code=0,
        )
    )

    assert audit_file.exists()
    assert stat.S_IMODE(audit_file.stat().st_mode) == AUDIT_FILE_MODE == 0o600


def test_operation_logging_documents_env_vars_and_levels():
    """The page's env-var table covers exactly the three variables the
    logging subsystem consults, and all six canonical level names.
    """
    from takler.logging import config as logging_config
    from takler.logging.levels import LEVEL_ORDER

    text = _operation_page("logging.rst")
    for env in (
        logging_config.ENV_LOG_LEVEL,
        logging_config.ENV_LOG_FILE,
        logging_config.ENV_AUDIT_FILE,
    ):
        assert f"``{env}``" in text
    for level in LEVEL_ORDER:
        assert f"``{level.name}``" in text and f"{level.rank}" in text


def test_operation_logging_invalid_env_level_falls_back_to_info():
    """An unrecognized TAKLER_LOG_LEVEL resolves to INFO and carries the
    offending value for the warning, rather than raising — the graceful
    degradation the level section documents.
    """
    from takler.logging.config import resolve_config
    from takler.logging.levels import LogLevel

    resolved = resolve_config({}, {"TAKLER_LOG_LEVEL": "chatty"})
    assert resolved.level is LogLevel.INFO
    assert resolved.invalid_env_level == "chatty"

    # Blank counts as "not provided"; case is irrelevant.
    assert resolve_config({}, {"TAKLER_LOG_LEVEL": "  "}).level is LogLevel.INFO
    assert resolve_config({}, {"TAKLER_LOG_LEVEL": "debug"}).level is LogLevel.DEBUG


def test_operation_logging_format_layout():
    """The rendered record matches the documented layout:
    RFC 3339 timestamp with millisecond precision and offset, level name,
    component, message — identical for console and file.
    """
    import re
    from datetime import datetime

    from takler.logging.formatter import format_record
    from takler.logging.levels import LogLevel

    line = format_record(
        datetime(2026, 6, 30, 11, 38, 10, 123000),
        LogLevel.INFO,
        "server.scheduler",
        "scheduler shutting down...",
    )
    assert re.fullmatch(
        r"2026-06-30T11:38:10\.123[+-]\d{2}:\d{2} "
        r"INFO server\.scheduler scheduler shutting down\.\.\.",
        line,
    )


def test_operation_logging_rotation_and_retention_parsing():
    """The rotation size/interval strings and retention counts the page
    lists parse as documented (stdlib backend mappings).
    """
    from takler.logging.backends.stdlib_backend import (
        _parse_interval,
        _parse_size,
        _retention_to_backup_count,
    )

    assert _parse_size("10 MB") == 10 * 1024**2
    assert _parse_size("512KB") == 512 * 1024
    assert _parse_size("1048576") == 1048576
    assert _parse_interval("1 day") == ("D", 1)
    assert _parse_interval("30 minutes") == ("M", 30)
    assert _parse_interval("midnight") == ("midnight", 1)
    assert _parse_interval("1 week") == ("D", 7)
    # Retention: a count is kept; anything unparsable keeps everything.
    assert _retention_to_backup_count(5) == 5
    assert _retention_to_backup_count("7 days") == 0


def test_operation_resilience_documents_policy_configuration():
    """The page documents both policy values, the option and the env var,
    and the precedence chain the resolver implements.
    """
    from takler.server.connect_config import (
        ExceptionPolicy,
        resolve_exception_policy,
    )

    text = _operation_page("resilience.rst")
    assert "``--exception-policy``" in text
    assert "``TAKLER_EXCEPTION_POLICY``" in text
    assert "``resilient``" in text and "``fail_fast``" in text

    # CLI option > env > built-in default; unrecognized degrades with a
    # warning rather than raising (already pinned for connect-config, here
    # for the full chain this page states).
    assert (
        resolve_exception_policy("fail_fast", {"TAKLER_EXCEPTION_POLICY": "resilient"})
        is ExceptionPolicy.FAIL_FAST
    )
    assert (
        resolve_exception_policy(None, {"TAKLER_EXCEPTION_POLICY": "fail-fast"})
        is ExceptionPolicy.FAIL_FAST
    )
    assert resolve_exception_policy(None, {}) is ExceptionPolicy.RESILIENT
    assert (
        resolve_exception_policy(None, {"TAKLER_EXCEPTION_POLICY": "boom"})
        is ExceptionPolicy.RESILIENT
    )


def _isolation_bunch():
    """Two begun flows; ``flow1`` raises inside its calendar update.

    Assigning on the instance shadows the method, so the failure originates
    in flow processing exactly where the per-flow boundary wraps it.
    """
    from takler.core import Bunch, Flow

    bunch = Bunch("b")
    flow1, flow2 = Flow("flow1"), Flow("flow2")
    for flow in (flow1, flow2):
        with flow:
            flow.add_task("task1")
        bunch.add_flow(flow)
        flow.begin()

    def boom(time_now):
        raise RuntimeError("calendar exploded")

    flow1.update_calendar = boom
    return bunch


def test_operation_resilience_per_flow_isolation():
    """The failure radius is one flow for one iteration: the failing flow is
    skipped, the healthy flow is still processed in the same and following
    iterations, and the default resilient policy triggers no shutdown.
    """
    import asyncio

    from takler.server.scheduler import Scheduler

    bunch = _isolation_bunch()
    fatal_calls = []
    scheduler = Scheduler(
        bunch,
        interval_main_loop=0.01,
        fatal_shutdown=lambda: fatal_calls.append(1),
    )

    processed = []
    original = scheduler._process_flow

    def spy(name, flow, time_now):
        processed.append(name)
        return original(name, flow, time_now)

    scheduler._process_flow = spy

    async def main():
        task = asyncio.ensure_future(scheduler.main_loop())
        await asyncio.sleep(0.05)
        scheduler.should_stop = True
        await task

    asyncio.run(main())

    # flow1 raised on every iteration, flow2 was reached anyway, and the
    # loop kept iterating until stopped from outside.
    assert processed.count("flow1") > 1
    assert processed.count("flow2") == processed.count("flow1")
    assert fatal_calls == []


def test_operation_resilience_fail_fast_triggers_clean_shutdown():
    """Under fail_fast the same failure fires the fatal-shutdown trigger and
    leaves the main loop on its own — the entry into the shared clean
    shutdown path the page describes.
    """
    import asyncio

    from takler.server.connect_config import ExceptionPolicy
    from takler.server.scheduler import Scheduler

    bunch = _isolation_bunch()
    fatal_calls = []
    scheduler = Scheduler(
        bunch,
        interval_main_loop=0.01,
        exception_policy=ExceptionPolicy.FAIL_FAST,
        fatal_shutdown=lambda: fatal_calls.append(1),
    )

    # Returns on its own after the first failing iteration; no external stop.
    asyncio.run(scheduler.main_loop())

    assert fatal_calls == [1]


# ---------------------------------------------------------------------------
# operation/reference.rst and operation/troubleshooting.rst
# ---------------------------------------------------------------------------


def _reference_page() -> str:
    return _operation_page("reference.rst")


def test_operation_reference_documents_every_env_var():
    """Every environment variable read anywhere in the codebase appears in
    operation/reference.rst."""
    from takler.client import cli as client_cli
    from takler.client.credentials import (
        ENV_JOB_PASSWORD,
        ENV_SECRET_FILE,
        ENV_TLS_CA_FILE,
        ENV_TLS_SERVER_NAME,
        USER_NAME_ENV_NAMES,
    )
    from takler.client.retry import ENV_RETRY_WINDOW
    from takler.logging.config import ENV_LOG_FILE, ENV_LOG_LEVEL
    from takler.server.connect_config import (
        TAKLER_AUDIT_FILE,
        TAKLER_AUTH_MODE,
        TAKLER_CONNECT_FILE,
        TAKLER_EXCEPTION_POLICY,
        TAKLER_ZOMBIE_POLICY,
    )

    env_vars = {
        TAKLER_CONNECT_FILE,
        TAKLER_EXCEPTION_POLICY,
        TAKLER_AUTH_MODE,
        TAKLER_ZOMBIE_POLICY,
        TAKLER_AUDIT_FILE,
        ENV_LOG_LEVEL,
        ENV_LOG_FILE,
        ENV_JOB_PASSWORD,
        ENV_TLS_CA_FILE,
        ENV_TLS_SERVER_NAME,
        ENV_SECRET_FILE,
        ENV_RETRY_WINDOW,
        client_cli.TAKLER_HOST,
        client_cli.TAKLER_PORT,
        client_cli.TAKLER_NAME,
        client_cli.NO_TAKLER,
        *USER_NAME_ENV_NAMES,
    }
    text = _reference_page()

    assert sorted(v for v in env_vars if f"``{v}``" not in text) == []


def test_operation_reference_documents_every_connect_config_field():
    """Every connect.yaml field has a fully qualified literal in the page."""
    from takler.server.connect_config import (
        Address,
        CheckpointSettings,
        SecuritySettings,
    )

    expected = {
        *(f"server.address.{f}" for f in Address.model_fields),
        *(f"checkpoint.{f}" for f in CheckpointSettings.model_fields),
        *(f"security.{f}" for f in SecuritySettings.model_fields),
    }
    text = _reference_page()

    assert sorted(f for f in expected if f"``{f}``" not in text) == []


def test_operation_reference_documents_every_error_code():
    """Every registered Error_Code appears in the page with its name, and the
    page's exit-code column matches EXIT_CODE_BY_ERROR_CODE.
    """
    from takler.client.exit_code import EXIT_CODE_BY_ERROR_CODE
    from takler.protocol.error_code import ERROR_NAME_BY_CODE

    text = _reference_page()

    for code, name in sorted(ERROR_NAME_BY_CODE.items()):
        assert f"``{code}``" in text, code
        assert f"``{name}``" in text, name
        # The page's table column for the client exit code.
        assert f"``{EXIT_CODE_BY_ERROR_CODE[code]}``" in text, code


def test_operation_reference_error_code_classification_fallbacks():
    """The documented mapping rules: exact-type lookup, generic 1 for an
    unregistered TaklerError subclass, 99 for anything else, "unknown" for
    unregistered codes.
    """
    from takler.exceptions import PermissionDeniedError, SecurityConfigError
    from takler.protocol.error_code import (
        error_code_for_exception,
        error_name_for_code,
    )

    assert error_code_for_exception(PermissionDeniedError("x")) == 43
    # SecurityConfigError is a TaklerError without its own code: falls to 1.
    assert error_code_for_exception(SecurityConfigError("x")) == 1
    assert error_code_for_exception(ValueError("x")) == 99
    assert error_name_for_code(12345) == "unknown"


def test_operation_reference_exit_code_mapping():
    """The documented exit-code mapping: request errors -> 1, server-side
    failures -> 3, transport -> 4, unregistered codes conservatively -> 3.
    """
    from takler.client.exit_code import exit_code_for_error_code

    assert exit_code_for_error_code(0) == 0
    assert exit_code_for_error_code(43) == 1
    assert exit_code_for_error_code(31) == 3
    assert exit_code_for_error_code(30) == 3
    assert exit_code_for_error_code(99) == 3
    assert exit_code_for_error_code(41) == 4
    assert exit_code_for_error_code(40) == 4
    assert exit_code_for_error_code(12345) == 3


def test_operation_reference_broken_connect_file_exits_3():
    """An unreadable TAKLER_CONNECT_FILE lands on the failure contract: one
    stderr line naming the exception type, exit code 3 — as both
    reference.rst and connect-config.rst state.
    """
    from typer.testing import CliRunner

    from takler.client.cli import app

    result = CliRunner().invoke(
        app, ["ping"], env={"TAKLER_CONNECT_FILE": "/nonexistent/connect.yaml"}
    )

    assert result.exit_code == 3
    assert "FileNotFoundError" in result.output


def test_operation_reference_retry_window_defaults():
    """The documented retry-window defaults: 86400 s for child commands,
    60 s for control and query.
    """
    from takler.client.retry import DEFAULT_RETRY_WINDOW_BY_KIND, CommandKind

    assert DEFAULT_RETRY_WINDOW_BY_KIND[CommandKind.CHILD] == 86400.0
    assert DEFAULT_RETRY_WINDOW_BY_KIND[CommandKind.CONTROL] == 60.0
    assert DEFAULT_RETRY_WINDOW_BY_KIND[CommandKind.QUERY] == 60.0


def test_operation_troubleshooting_covers_documented_symptoms():
    """The page names every symptom from the plan and links the detail pages
    behind each entry.
    """
    text = _operation_page("troubleshooting.rst")

    for needle in [
        "连不上服务",
        "ping 通但其他命令全被拒绝",
        "任务卡在 queued",
        "任务卡在 submitted",
        "作业在跑但状态不动",
        "zombie 记录刷日志",
        "快照恢复失败",
        "TLS 握手失败",
        "作业脚本渲染失败",
        # Locating commands and handles each entry must hand the operator.
        "TAKLER_TIMEOUT",
        "missing_credential",
        "invalid_credential",
        "not_in_whitelist",
        "--show-all",
        "free_dep",
        "TAKLER_SHELL_JOB_CMD",
        "Z1 / Z2 / Z3",
        "TAKLER_TLS_SERVER_NAME",
        "check_job_creation",
    ]:
        assert needle in text, needle

    for target in [
        "/operation/logging",
        "/operation/audit",
        "/operation/connect-config",
        "/operation/security",
        "/operation/zombie",
        "/operation/checkpoint",
        "/guide/cli",
        "/guide/tui",
        "/guide/trigger-expression",
        "/guide/ecflow-differences",
        "/guide/job-management",
        "/guide/task-script",
    ]:
        assert f":doc:`{target}`" in text, target


# ---------------------------------------------------------------------------
# develop/architecture.rst and develop/core-design.rst
# ---------------------------------------------------------------------------

DEVELOP_DIR = PROJECT_ROOT / "doc" / "source" / "develop"


def _develop_page(name: str) -> str:
    return (DEVELOP_DIR / name).read_text(encoding="utf-8")


def test_develop_index_lists_architecture_and_core_design():
    """develop/index.rst links the two new pages and they exist."""
    text = _develop_page("index.rst")

    assert "architecture" in text
    assert "core-design" in text
    assert (DEVELOP_DIR / "architecture.rst").exists()
    assert (DEVELOP_DIR / "core-design.rst").exists()


def test_develop_architecture_documents_layers_and_components():
    """The architecture page names every package layer and the server-side
    components the assembling paragraph talks about."""
    text = _develop_page("architecture.rst")

    for needle in [
        "``takler.core``",
        "``takler.tasks``",
        "``takler.server``",
        "``takler.client``",
        "``takler.tui``",
        "``takler.logging``",
        "``takler.exceptions``",
        "``takler.visitor``",
        "``TaklerService``",
        "``Scheduler``",
        "``CheckpointManager``",
        "``ZombieDetector``",
        "``AuthInterceptor``",
        "``AuditLogger``",
        "``TaklerService._handle_command``",
        "``resolve_dependencies``",
        "``ShellRunner``",
        "``TaklerServer``",
    ]:
        assert needle in text, needle

    for target in [
        "/guide/cli",
        "/guide/job-management",
        "/guide/node-status",
        "/guide/variables",
        "/operation/security",
        "/operation/checkpoint",
        "/operation/resilience",
        "/operation/audit",
        "/operation/zombie",
        "/operation/reference",
        "/operation/troubleshooting",
    ]:
        assert f":doc:`{target}`" in text, target


def test_develop_architecture_core_never_imports_upper_layers():
    """The layering rule the page states: takler.core imports no server,
    client, tui or tasks code; takler.tasks and the server stay independent
    of each other (ShellScriptTask enters the server via class_type
    reflection, not an import).
    """
    import re

    src = PROJECT_ROOT / "src" / "takler"
    import_re = re.compile(r"^\s*(?:from|import)\s+(takler\.[\w.]+)", re.M)

    def imported_packages(directory: Path) -> set:
        result = set()
        for py_file in directory.rglob("*.py"):
            if "__pycache__" in py_file.parts:
                continue
            for match in import_re.finditer(py_file.read_text(encoding="utf-8")):
                result.add(match.group(1).split(".")[1])
        return result

    core_imports = imported_packages(src / "core")
    assert core_imports <= {"exceptions", "logging", "constant"}

    tasks_imports = imported_packages(src / "tasks")
    assert "server" not in tasks_imports
    assert "client" not in tasks_imports

    server_imports = imported_packages(src / "server")
    assert "tasks" not in server_imports
    assert "client" not in server_imports
    assert "tui" not in server_imports


def test_develop_architecture_main_loop_processes_each_begun_flow():
    """The documented main loop contract: an un-begun flow is skipped
    entirely; a begun flow gets update_calendar + resolve_dependencies.
    """
    import datetime

    from takler.core import Bunch, Flow, NodeStatus
    from takler.server.scheduler import Scheduler

    bunch = Bunch()
    flow = Flow("f1")
    task = flow.add_task("t1")
    bunch.add_flow(flow)
    scheduler = Scheduler(bunch)

    # Un-begun: _process_flow is a no-op, the task stays unknown.
    scheduler._process_flow("f1", flow, datetime.datetime.now())
    assert task.state.node_status == NodeStatus.unknown
    assert flow.calendar.flow_time is None

    # Begun: calendar advances and dependency resolution runs the task.
    flow.begin()
    scheduler._process_flow("f1", flow, datetime.datetime.now())
    assert flow.calendar.flow_time is not None
    assert task.state.node_status == NodeStatus.submitted


def test_develop_core_design_node_hierarchy():
    """The documented class hierarchy of the node tree."""
    from takler.core import Bunch, Flow, NodeContainer, Task
    from takler.core.node import Node
    from takler.tasks.shell import ShellScriptTask

    assert issubclass(NodeContainer, Node)
    assert issubclass(Task, Node)
    assert issubclass(Flow, NodeContainer)
    assert issubclass(Bunch, NodeContainer)
    assert issubclass(ShellScriptTask, Task)


def test_develop_core_design_most_significant_status_precedence():
    """The documented precedence aborted > active > submitted > queued >
    complete, and unknown for an empty list.
    """
    from takler.core import NodeStatus, Task
    from takler.core.node import compute_most_significant_status

    def task_with(status: NodeStatus) -> Task:
        task = Task("t")
        task.set_node_status_only(status)
        return task

    def significant(*statuses: NodeStatus) -> NodeStatus:
        return compute_most_significant_status(
            [task_with(s) for s in statuses], immediate=True
        )

    assert significant(NodeStatus.complete, NodeStatus.aborted) == NodeStatus.aborted
    assert significant(NodeStatus.queued, NodeStatus.active) == NodeStatus.active
    assert significant(NodeStatus.queued, NodeStatus.submitted) == NodeStatus.submitted
    assert significant(NodeStatus.complete, NodeStatus.queued) == NodeStatus.queued
    assert significant(NodeStatus.complete, NodeStatus.complete) == NodeStatus.complete
    assert compute_most_significant_status([], immediate=True) == NodeStatus.unknown


def test_develop_core_design_swim_recomputes_container_status():
    """The documented swim direction: a leaf status change bubbles up and
    the container recomputes from its children.
    """
    from takler.core import Flow, NodeStatus

    flow = Flow("f")
    t1 = flow.add_task("t1")
    t2 = flow.add_task("t2")
    flow.begin()

    t1.set_node_status(NodeStatus.complete)
    assert flow.state.node_status == NodeStatus.queued

    t2.set_node_status(NodeStatus.complete)
    assert flow.state.node_status == NodeStatus.complete


def test_develop_core_design_task_status_gate_and_aborted_no_rerun():
    """The documented Task.check_dependencies gates: complete / active /
    submitted / unknown / aborted all refuse to run; queued passes.
    """
    from takler.core import NodeStatus, Task

    for status in (
        NodeStatus.complete,
        NodeStatus.active,
        NodeStatus.submitted,
        NodeStatus.unknown,
        NodeStatus.aborted,
    ):
        task = Task("t")
        task.set_node_status_only(status)
        assert task.check_dependencies() is False, status

    queued_task = Task("t")
    queued_task.set_node_status_only(NodeStatus.queued)
    assert queued_task.check_dependencies() is True


def test_develop_core_design_expression_lazy_parse_and_free_latch():
    """The documented expression pipeline: add_trigger stores the string
    without parsing; the first evaluation builds the AST; the free latch
    short-circuits evaluation to True.
    """
    from takler.core import Flow
    from takler.core.expression import Expression

    flow = Flow("f")
    task = flow.add_task("t1")
    flow.add_task("t2")

    task.add_trigger("/f/t2 == complete")
    assert task.trigger_expression.ast is None

    assert task.evaluate_trigger() is False
    assert task.trigger_expression.ast is not None

    # The free latch makes the expression always True without an AST.
    expression = Expression("not a valid expression")
    expression.set_free()
    assert expression.evaluate() is True


def test_develop_core_design_trigger_binds_at_first_evaluation():
    """The documented binding behaviour: a trigger referencing a missing
    node fails at first evaluation with NodeNotFoundError, not at
    definition time.
    """
    import pytest

    from takler.core import Flow
    from takler.exceptions import NodeNotFoundError

    flow = Flow("f")
    task = flow.add_task("t1")
    task.add_trigger("/f/missing == complete")  # definition time: no error

    with pytest.raises(NodeNotFoundError):
        task.evaluate_trigger()


def test_develop_core_design_serialization_class_type_and_password():
    """The documented serialization contract: class_type reflection
    restores the concrete class (ShellScriptTask) without the server
    importing takler.tasks, and job_password never enters the dict.
    """
    import json

    from takler.core import Bunch, Flow
    from takler.tasks.shell import ShellScriptTask

    bunch = Bunch()
    flow = Flow("f")
    task = flow.add_task(ShellScriptTask("t1", script_path="t1.takler"))
    # The task must be in a bunch before running: generated parameters such
    # as TAKLER_JOB resolve TAKLER_HOME from the bunch's server state.
    bunch.add_flow(flow)
    task.increment_try_no()
    assert task.job_password is not None

    d = bunch.to_dict()
    blob = json.dumps(d)

    # class_type reflection metadata is present and round-trips.
    task_dict = d["flows"][0]["children"][0]
    assert task_dict["class_type"] == {
        "module": "takler.tasks.shell.shell_script_task",
        "name": "ShellScriptTask",
    }
    restored = Bunch.from_dict(json.loads(blob))
    restored_task = restored.find_node("/f/t1")
    assert isinstance(restored_task, ShellScriptTask)

    # The one-time job password is deliberately not serialized.
    assert "job_password" not in blob
    assert "TAKLER_PASS" not in blob
    assert restored_task.job_password is None


# ---------------------------------------------------------------------------
# develop/protocol.rst, develop/contributing.rst and develop/extending.rst
# ---------------------------------------------------------------------------


def test_develop_index_lists_protocol_contributing_extending():
    """develop/index.rst links the three batch-Q pages and they exist."""
    text = _develop_page("index.rst")

    for name in ("protocol", "contributing", "extending"):
        assert name in text, name
        assert (DEVELOP_DIR / f"{name}.rst").exists(), name


def test_develop_protocol_documents_all_rpc_methods():
    """Every RPC the TaklerServer service declares is named on the page."""
    from takler.server.protocol import takler_pb2

    text = _develop_page("protocol.rst")
    service = takler_pb2.DESCRIPTOR.services_by_name["TaklerServer"]

    methods = [m.name for m in service.methods]
    assert len(methods) == 16
    for name in methods:
        assert f"``{name}``" in text, name

    # The request/response message names an implementer has to look up.
    for message in (
        "ServiceResponse",
        "ChildCommandOptions",
        "InitCommand",
        "ForceCommand",
        "FreeDepCommand",
        "LoadCommand",
        "BeginCommand",
        "ShowRequest",
        "ShowResponse",
    ):
        assert f"``{message}``" in text, message


def test_develop_protocol_error_code_names_documented():
    """Every registered Error_Code name appears on the page (the full table
    itself lives in operation/reference.rst, linked from the page)."""
    from takler.protocol.error_code import ERROR_NAME_BY_CODE

    text = _develop_page("protocol.rst")

    for code, name in ERROR_NAME_BY_CODE.items():
        assert name in text, (code, name)

    assert ":doc:`/operation/reference`" in text
    # The lookup rules the page states.
    assert "``unknown``" in text
    assert "``1``" in text
    assert "``99``" in text


def test_develop_protocol_metadata_keys_and_rejection_contract():
    """The three credential metadata keys and the rejection classification
    strings are documented exactly as the constants spell them."""
    from takler.server.auth import (
        CREDENTIAL_METADATA_KEYS,
        SERVICE_METHOD_PREFIX,
        RejectionReason,
    )

    text = _develop_page("protocol.rst")

    for key in CREDENTIAL_METADATA_KEYS:
        assert key in text, key
        assert f"``{key}``" in text, key

    for reason in RejectionReason:
        assert reason.value in text, reason

    assert "UNAUTHENTICATED" in text
    assert "PERMISSION_DENIED" in text
    # The privilege table is keyed by the fully qualified method name.
    assert "/takler_protocol.TaklerServer/" in text
    assert SERVICE_METHOD_PREFIX == "/takler_protocol.TaklerServer/"


def test_develop_protocol_enums_match_generated_stub():
    """ForceState and DepType values on the page match the generated stub,
    and ForceState numbering deliberately differs from NodeStatus."""
    from takler.core import NodeStatus
    from takler.server.protocol import takler_pb2

    text = _develop_page("protocol.rst")

    for name, value in takler_pb2.ForceCommand.ForceState.items():
        assert f"{name}={value}" in text, (name, value)
    for name, value in takler_pb2.FreeDepCommand.DepType.items():
        assert f"{name}={value}" in text, (name, value)

    # The two numberings must not be conflated: force complete is 1 while
    # NodeStatus.complete is 2.
    assert takler_pb2.ForceCommand.ForceState.Value("complete") == 1
    assert NodeStatus.complete.value == 2


def test_develop_protocol_retry_contract_constants():
    """The retry constants quoted on the page match the client contract."""
    from takler.client.retry import (
        DEFAULT_RETRY_WINDOW_BY_KIND,
        DEFAULT_SINGLE_TIMEOUT,
        ENV_RETRY_WINDOW,
        CommandKind,
    )

    text = _develop_page("protocol.rst")

    assert DEFAULT_SINGLE_TIMEOUT == 10.0
    assert "10" in text
    assert DEFAULT_RETRY_WINDOW_BY_KIND[CommandKind.CHILD] == 86400.0
    assert "86400" in text
    assert DEFAULT_RETRY_WINDOW_BY_KIND[CommandKind.CONTROL] == 60.0
    assert ENV_RETRY_WINDOW == "TAKLER_TIMEOUT"
    assert "TAKLER_TIMEOUT" in text
    assert "min(2**(n-1), 60)" in text
    for status in (
        "UNAVAILABLE",
        "DEADLINE_EXCEEDED",
        "RESOURCE_EXHAUSTED",
        "UNKNOWN",
        "INVALID_ARGUMENT",
        "NOT_FOUND",
    ):
        assert status in text, status


def test_develop_contributing_matches_project_config():
    """The contributing page stays in step with pyproject.toml: ruff rule
    set, pytest import mode, the slow marker and the extras all match."""
    import tomllib

    text = _develop_page("contributing.rst")
    pyproject = tomllib.loads(
        (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )

    for rule in pyproject["tool"]["ruff"]["lint"]["select"]:
        assert f'"{rule}"' in text, rule

    assert (
        "--import-mode=importlib"
        in pyproject["tool"]["pytest"]["ini_options"]["addopts"]
    )
    assert "--import-mode=importlib" in text

    markers = pyproject["tool"]["pytest"]["ini_options"]["markers"]
    assert any(m.startswith("slow:") for m in markers)
    assert "``slow``" in text

    # Each user-facing extra is mentioned on the contributing page (the dev
    # group pulls them in by self-reference, so the page no longer names
    # explicit ``--extra`` flags).
    for extra in pyproject["project"]["optional-dependencies"]:
        assert extra in text, extra

    assert any(
        req.startswith("setuptools_scm")
        for req in pyproject["build-system"]["requires"]
    )
    assert "setuptools_scm" in text


class DocExampleMarkerTask(__import__("takler.core", fromlist=["Task"]).Task):
    """A custom Task subclass defined at module level so that class_type
    reflection can import it back by module path (the constraint the
    extending page documents)."""

    def __init__(self, name: str, marker_path=None):
        from takler.core import Task

        Task.__init__(self, name)
        self.marker_path = marker_path

    def to_dict(self):
        from takler.core import Task

        d = Task.to_dict(self)
        d["marker_path"] = None if self.marker_path is None else str(self.marker_path)
        return d

    @classmethod
    def fill_from_dict(cls, d, node, method=None):
        from takler.core import SerializationType, Task

        if method is None:
            method = SerializationType.Status
        Task.fill_from_dict(d=d, node=node, method=method)
        node.marker_path = d.get("marker_path")
        return node


def test_develop_extending_run_template_order_and_do_run_gate():
    """The documented run() template: before_run -> do_run -> after_run,
    try_no increments and the job password rotates per run, and a do_run
    returning False keeps the task out of submitted."""
    from takler.core import NodeStatus, Task

    calls = []

    class RecordingTask(Task):
        def before_run(self):
            calls.append("before_run")
            super().before_run()

        def do_run(self):
            calls.append("do_run")
            return True

        def after_run(self):
            calls.append("after_run")
            super().after_run()

    task = RecordingTask("t")
    task.run()
    assert calls == ["before_run", "do_run", "after_run"]
    assert task.state.node_status == NodeStatus.submitted
    assert task.try_no == 1
    first_password = task.job_password
    assert first_password

    task.requeue()
    task.run()
    assert task.try_no == 1  # requeue reset it, run incremented again
    assert task.job_password != first_password

    class FailingSubmissionTask(Task):
        def do_run(self):
            return False

    failing = FailingSubmissionTask("t")
    failing.run()
    assert failing.state.node_status == NodeStatus.unknown
    assert failing.try_no == 1  # before_run already ran


def test_develop_extending_custom_task_class_type_roundtrip():
    """The documented class_type reflection constraints: the custom class is
    importable by module path, constructed with name only, and its extra
    field is restored through fill_from_dict."""
    import importlib
    import json

    from takler.core import Bunch, Flow

    module = importlib.import_module(DocExampleMarkerTask.__module__)
    assert module.DocExampleMarkerTask is DocExampleMarkerTask

    bunch = Bunch()
    flow = Flow("f")
    flow.add_task(DocExampleMarkerTask("t1", marker_path="/tmp/marker.txt"))
    bunch.add_flow(flow)

    d = bunch.to_dict()
    task_dict = d["flows"][0]["children"][0]
    assert task_dict["class_type"] == {
        "module": DocExampleMarkerTask.__module__,
        "name": "DocExampleMarkerTask",
    }

    restored = Bunch.from_dict(json.loads(json.dumps(d)))
    restored_task = restored.find_node("/f/t1")
    assert isinstance(restored_task, DocExampleMarkerTask)
    assert restored_task.marker_path == "/tmp/marker.txt"


def test_develop_extending_task_decorator_inline_execution():
    """The documented task decorator semantics: the function body runs
    inline, between init() and complete()."""
    from takler.core import NodeStatus, task

    events = []

    @task("t1")
    def make(self=None):
        events.append(("body", self.state.node_status))

    inline_task = make()
    inline_task.run()

    assert events == [("body", NodeStatus.active)]
    assert inline_task.state.node_status == NodeStatus.complete


def test_develop_extending_logging_backend_contract():
    """The documented backend ABC: LoggingBackend's three abstract methods
    and NamedLogger's single log method; selection probes for loguru."""
    from takler.logging.backends import LoggingBackend, NamedLogger, get_backend

    assert set(LoggingBackend.__abstractmethods__) == {
        "map_level",
        "apply_config",
        "get_named_logger",
    }
    assert set(NamedLogger.__abstractmethods__) == {"log"}

    backend = get_backend()
    assert type(backend).__name__ in ("LoguruBackend", "StdlibBackend")

    text = _develop_page("extending.rst")
    for needle in (
        "``LoggingBackend``",
        "``NamedLogger``",
        "``map_level``",
        "``apply_config``",
        "``get_named_logger``",
        "``select_backend()``",
        "``reset_backend()``",
        "``_BACKEND``",
    ):
        assert needle in text, needle


# ---------------------------------------------------------------------------
# develop/api/*.rst (batch R)
#
# The API pages are built from explicit autosummary lists (core / tasks /
# client / exceptions) and per-module automodule directives (server / logging
# / tui). The tests below pin the listing convention, verify every listed
# object is importable (autosummary entries that name a non-existent object
# would only surface as Sphinx build warnings otherwise), and assert that the
# public surface of ``takler.core`` / ``takler.exceptions`` is fully covered.
# ---------------------------------------------------------------------------

API_DIR = DEVELOP_DIR / "api"


def _api_page(name: str) -> str:
    return (API_DIR / name).read_text(encoding="utf-8")


def _autosummary_entries(text: str) -> list:
    """Return the object names listed in ``.. autosummary::`` blocks."""
    entries = []
    lines = text.splitlines()
    in_block = False
    for line in lines:
        if line.startswith(".. autosummary::"):
            in_block = True
            continue
        if in_block:
            stripped = line.strip()
            if stripped.startswith(":"):
                continue  # directive option
            if not stripped:
                continue  # blank line between options and entries
            if line.startswith("   "):
                entries.append(stripped)
            else:
                in_block = False  # dedent ends the directive
    return entries


def _import_dotted(name: str):
    """Import ``a.b.c`` as a module, falling back to module + attributes."""
    parts = name.split(".")
    last_error = None
    for split in range(len(parts), 0, -1):
        module_name = ".".join(parts[:split])
        try:
            obj = importlib.import_module(module_name)
        except ImportError as exc:
            last_error = exc
            continue
        for attr in parts[split:]:
            obj = getattr(obj, attr)
        return obj
    raise last_error


def test_develop_api_index_lists_all_pages():
    """api/index.rst toctree lists every API page and each file exists."""
    text = _api_page("index.rst")
    for name in (
        "tree",
        "attribute",
        "tasks",
        "client",
        "exceptions",
        "server",
        "logging",
        "tui",
    ):
        assert re.search(rf"^\s+{name}$", text, re.M), name
        assert (API_DIR / f"{name}.rst").exists(), name


def test_develop_api_core_pages_cover_public_exports():
    """Every name in ``takler.core.__all__`` is listed on tree.rst or
    attribute.rst, so the whole public core surface is reachable from the
    API pages."""
    import takler.core

    pages = _api_page("tree.rst") + _api_page("attribute.rst")
    for name in takler.core.__all__:
        assert f"takler.core.{name}" in pages, name


def test_develop_api_autosummary_entries_importable():
    """Every autosummary entry on the four explicit-list pages resolves to a
    real importable object."""
    for page in (
        "tree.rst",
        "attribute.rst",
        "tasks.rst",
        "client.rst",
        "exceptions.rst",
    ):
        entries = _autosummary_entries(_api_page(page))
        assert entries, page
        for entry in entries:
            _import_dotted(entry)  # raises AttributeError/ImportError if stale


def test_develop_api_tasks_page_covers_shell_objects():
    """tasks.rst lists the shell task class, its generated-parameters model,
    the render/runner pair and the job-creation checker."""
    text = _api_page("tasks.rst")
    for entry in (
        "takler.tasks.shell.ShellScriptTask",
        "takler.tasks.shell.ShellScriptTaskGeneratedParameters",
        "takler.tasks.shell.shell_render.ShellRender",
        "takler.tasks.shell.shell_runner.ShellRunner",
        "takler.tasks.shell.check_job_creation",
    ):
        assert entry in text, entry
    # The three names the package re-exports must be the ones the page leads
    # with; ShellRender/ShellRunner stay module-level.
    import takler.tasks.shell

    assert set(takler.tasks.shell.__all__) == {
        "ShellScriptTask",
        "ShellScriptTaskGeneratedParameters",
        "check_job_creation",
    }


def test_develop_api_client_page_covers_client_surface():
    """client.rst lists the service client plus the documented members of the
    credentials, retry and exit_code modules (their public functions/classes,
    not the module constants)."""
    import inspect

    import takler.client.credentials as credentials
    import takler.client.exit_code as exit_code
    import takler.client.retry as retry

    text = _api_page("client.rst")
    assert "takler.client.TaklerServiceClient" in text

    for module in (credentials, retry, exit_code):
        for name in module.__all__:
            obj = getattr(module, name)
            if inspect.isfunction(obj) or inspect.isclass(obj):
                entry = f"{module.__name__}.{name}"
                assert entry in text, entry


def test_develop_api_exceptions_page_covers_hierarchy():
    """exceptions.rst lists every name in ``takler.exceptions.__all__`` and
    its hierarchy prose matches the real base classes."""
    import takler.exceptions as exc_mod

    text = _api_page("exceptions.rst")
    for name in exc_mod.__all__:
        assert f"takler.exceptions.{name}" in text, name

    # Every takler-internal direct base is named on the page as well, so the
    # hierarchy tree cannot silently drift from the code.
    for name in exc_mod.__all__:
        cls = getattr(exc_mod, name)
        for base in cls.__bases__:
            if issubclass(base, exc_mod.TaklerError):
                assert f"takler.exceptions.{base.__name__}" in text, (
                    f"{name} -> {base.__name__}"
                )

    # The ValueError mix-ins were removed in M3 (task 4): the page must not
    # document them, and the types must not regain them.
    assert not issubclass(exc_mod.InvalidRequestError, ValueError)
    assert not issubclass(exc_mod.ExpressionSyntaxError, ValueError)
    assert "``ValueError``" not in text


def test_develop_api_internal_pages_cover_modules():
    """server.rst / logging.rst / tui.rst use per-module automodule, list the
    expected modules, and exclude the generated ``*_pb2*`` stubs."""
    import importlib

    server_modules = (
        "takler.server.scheduler",
        "takler.server.network_service",
        "takler.server.auth",
        "takler.server.zombie",
        "takler.server.audit",
        "takler.server.checkpoint",
        "takler.server.tls",
        "takler.server.connect_config",
        "takler.protocol.error_code",
    )
    logging_modules = (
        "takler.logging",
        "takler.logging.config",
        "takler.logging.levels",
        "takler.logging.errors",
        "takler.logging.formatter",
        "takler.logging.backends",
        "takler.logging.backends.stdlib_backend",
        "takler.logging.backends.loguru_backend",
    )
    tui_modules = (
        "takler.tui.app",
        "takler.tui.service",
        "takler.tui.show_parser",
        "takler.tui.state_style",
        "takler.tui.menu",
        "takler.tui.tabs",
        "takler.tui.widgets",
    )
    for page, modules in (
        ("server.rst", server_modules),
        ("logging.rst", logging_modules),
        ("tui.rst", tui_modules),
    ):
        text = _api_page(page)
        directive_lines = [
            line for line in text.splitlines() if ".. automodule::" in line
        ]
        assert directive_lines, page
        assert all("_pb2" not in line for line in directive_lines), page
        for module in modules:
            assert f".. automodule:: {module}" in text, f"{page}: {module}"
            if module == "takler.logging.backends.loguru_backend":
                # Importable only with the optional ``takler[log]`` extra,
                # exactly as the page says.
                pytest.importorskip("loguru")
            importlib.import_module(module)

        # The stability disclaimer is part of the contract of these pages
        # (whitespace-normalized: the phrase may wrap across source lines).
        assert "内部实现" in re.sub(r"\s+", "", text), page


def test_develop_api_conf_nitpick_allowlist_matches_reality():
    """conf.py no longer ignores the cross-reference targets that batch R
    made resolvable, and the regex allow-list stays limited to the two
    documented patterns (all-caps names and private members)."""
    conf_text = (PROJECT_ROOT / "doc" / "source" / "conf.py").read_text(
        encoding="utf-8"
    )
    for removed in (
        "takler.core.util.SerializationType",
        "takler.core.SerializationType.Status",
        "takler.core.SerializationType.Tree",
        "takler.core.calendar.Calendar",
        '"ExpressionSyntaxError"',
        '"FlowStateError"',
        '"JobSubmissionError"',
    ):
        assert removed not in conf_text, removed

    assert "nitpick_ignore_regex" in conf_text
    # autodoc mock list keeps the optional extras buildable on RTD.
    assert 'autodoc_mock_imports = ["textual", "rich", "loguru"]' in conf_text
