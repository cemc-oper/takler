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


def test_step10_builds_expected_tree():
    """``step10_control.py`` defines ``test`` with t1 (event), t2 (trigger), t3 (time)."""
    module = _load_module(EXAMPLES_DIR / "step10_control.py")
    flow = module.create_flow()

    task1 = flow.find_node("/test/t1")
    task2 = flow.find_node("/test/t2")
    task3 = flow.find_node("/test/t3")
    assert task1.find_event("a") is not None
    assert task2.trigger_expression.expression_str == "./t1 == complete"
    assert len(task3.times) == 1


def test_step10_begin_starts_calendar_and_rejects_second_begin():
    """``begin`` marks the flow begun and requeues the tree; only begun flows run.

    controlling-the-flow.rst walks through this: the first ``begin`` starts the
    calendar and resets the node tree; a second ``begin`` without ``--force``
    is refused; ``--force`` begins it again.
    """
    from takler.core import Bunch, NodeStatus
    from takler.exceptions import FlowStateError
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step10_control.py")
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


def test_step10_control_commands_require_a_begun_flow():
    """``requeue``/``run``/``force``/``free-dep`` on an un-begun flow are refused.

    controlling-the-flow.rst notes that a freshly ``load``-ed flow (or any flow
    before its first ``begin``) rejects these commands with a flow-state error.
    """
    from takler.core import Bunch
    from takler.exceptions import FlowStateError
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step10_control.py")
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


def test_step10_suspending_a_flow_blocks_its_whole_subtree():
    """A suspended container is never descended into, so its children never run.

    controlling-the-flow.rst states the suspended marker is orthogonal to node
    status: the child itself is not flagged, but the scheduler's dependency
    walk stops at the suspended container.
    """
    from takler.core import Bunch
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step10_control.py")
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


def test_step10_force_sets_node_status_recursively():
    """``force`` rewrites node status regardless of its current value.

    Mirrors controlling-the-flow.rst: with recursion on (the CLI default) the
    whole subtree takes the status; with ``--no-recursive`` only the target
    node changes and the parents re-aggregate from their children.
    """
    from takler.core import Bunch, NodeStatus
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step10_control.py")
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


def test_step10_force_sets_and_clears_events():
    """``force set|clear`` on a ``node:event`` path toggles the event.

    controlling-the-flow.rst uses ``force set /test/t1:a`` as the example; an
    unsupported state is rejected and leaves the event untouched.
    """
    from takler.core import Bunch
    from takler.exceptions import UnsupportedValueError
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step10_control.py")
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


def test_step10_free_dep_releases_time_and_trigger():
    """``free-dep`` marks a dependency as satisfied for the current run.

    Mirrors controlling-the-flow.rst: freeing ``t2``'s trigger makes the
    trigger expression evaluate to true without ``t1`` completing; freeing
    ``t3``'s time attribute sets its free latch without the calendar reaching
    12:00. A requeue re-arms both.
    """
    from takler.core import Bunch
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step10_control.py")
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


def test_step10_run_skips_a_submitted_task():
    """``run`` on a submitted/active task is a no-op unless forced.

    controlling-the-flow.rst states the guard exists so one task never runs two
    jobs at once; the forced form is exercised in zombies-and-restart.rst.
    """
    from takler.core import Bunch, NodeStatus
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step10_control.py")
    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    flow = scheduler.bunch.add_flow(module.create_flow())
    scheduler.run_command_begin("")

    task1 = flow.find_node("/test/t1")
    task1.set_node_status(NodeStatus.submitted)

    assert scheduler.run_command_run("/test/t1") is False
    assert task1.state.node_status == NodeStatus.submitted

    # ``run`` only applies to tasks; targeting a container is refused.
    assert scheduler.run_command_run("/test") is False


def test_step10_load_registers_a_flow_without_beginning_it():
    """``load`` registers a JSON flow definition; the flow starts un-begun.

    controlling-the-flow.rst loads ``Flow.to_dict`` output and stresses that an
    explicit ``begin`` is still required before the scheduler touches the flow.
    """
    import json

    from takler.core import Bunch
    from takler.exceptions import InvalidRequestError
    from takler.server.scheduler import Scheduler

    module = _load_module(EXAMPLES_DIR / "step10_control.py")
    flow = module.create_flow()

    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    scheduler.run_command_load("json", json.dumps(flow.to_dict()).encode())

    loaded = scheduler.bunch.find_flow("test")
    assert loaded is not None
    assert loaded.begun is False
    assert loaded.find_node("/test/t2") is not None

    with pytest.raises(InvalidRequestError):
        scheduler.run_command_load("json", b"not a json")


def test_step10_try_no_and_job_password_lifecycle():
    """Each run attempt gets a fresh ``try_no`` and job password; requeue clears both.

    zombies-and-restart.rst builds its zombie story on this invariant: the job
    file of attempt *n* is ``<node>.job<n>`` and its script carries the
    ``TAKLER_PASS`` generated for that attempt, so a report from attempt *n-1*
    can be told apart from the current run.
    """
    module = _load_module(EXAMPLES_DIR / "step10_control.py")
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


def test_step10_zombie_rejected_after_requeue():
    """A report arriving after its task was requeued hits zombie condition Z2.

    Mirrors the exact scenario walked through in zombies-and-restart.rst: the
    task is requeued while its job is still running, so when the old job's
    ``complete`` arrives the task is ``queued`` -- a status in which no job
    should be reporting. The default ``fail`` policy raises ``ZombieError``.
    """
    from takler.core import NodeStatus
    from takler.exceptions import ZombieError
    from takler.server.zombie import ChildAction, ZombieCondition, ZombieDetector

    module = _load_module(EXAMPLES_DIR / "step10_control.py")
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


def test_step10_zombie_conditions_z1_and_z3():
    """Z1 checks the job password (auth enabled only); Z3 catches a second init.

    zombies-and-restart.rst explains both: Z1 only applies when the server
    authenticates callers, and Z3 fires when an ``init`` names a different job
    id than the one recorded for the active task.
    """
    from takler.core import NodeStatus
    from takler.server.auth import CallCredentials
    from takler.server.connect_config import AuthMode
    from takler.server.zombie import ZombieCondition, ZombieDetector

    module = _load_module(EXAMPLES_DIR / "step10_control.py")
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
        auth_detector.detect(task1, "complete", credentials=stale)
        is ZombieCondition.Z1
    )
    current = CallCredentials(job_password=task1.job_password)
    assert auth_detector.detect(task1, "complete", credentials=current) is None


def test_step10_checkpoint_restore_keeps_in_flight_tasks(tmp_path):
    """A restarted server restores in-flight tasks so their jobs can still report.

    Mirrors the exact scenario walked through in zombies-and-restart.rst: the
    server is killed while ``t1`` is active, the checkpoint holds its status
    and job password, and after a restore the old job's ``complete`` passes
    the zombie guard instead of being rejected.
    """
    import json

    from takler.core import Bunch, NodeStatus
    from takler.server.auth import CallCredentials
    from takler.server.checkpoint import CheckpointManager
    from takler.server.zombie import ZombieDetector

    module = _load_module(EXAMPLES_DIR / "step10_control.py")
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
            restored_task1, "complete", credentials=CallCredentials(job_password=password)
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
    assert (
        tree_copy.find_node("/test/t1").state.node_status == NodeStatus.unknown
    )

    status_copy = Flow.from_dict(d, method=SerializationType.Status)
    assert status_copy.begun is True
    assert (
        status_copy.find_node("/test/t1").state.node_status == NodeStatus.active
    )


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
