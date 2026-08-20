"""After a restore, a ``ShellScriptTask`` can create its job again.

This is the third M1 recovery scenario (Requirement 16.15): ``kill -9`` recovery
(task 17.2) proves the *state* comes back, this module proves the recovered tree
is still *operable* -- the restored task can produce a job script again, and that
job script points at the server that is running now.

Two regressions used to make this impossible, and each one has an assertion here:

* ``ShellScriptTask.script_path`` was not serialized, so a restored task had no
  script at all and every job creation failed (fixed by task 6.4,
  Requirement 6.13).
* ``Bunch.from_dict`` built the flow dictionary without going through
  ``add_flow``, so a restored flow had no back reference to its bunch. The
  parameter inheritance chain then stopped at the flow and ``TAKLER_HOST`` /
  ``TAKLER_PORT`` resolved to nothing, which renders as an *empty* value in the
  job script -- a job whose child commands can never reach the server (fixed by
  task 6.5, Requirement 6.12).

Why the restore target is a *different* address
-----------------------------------------------
Requirements 6.5 and 6.22 do not merely ask for a resolvable address, they ask
for the address of the process doing the restoring: the snapshot's
``server_state`` is deliberately dropped. Restoring into a bunch that already
listens somewhere else is the only setup that can tell the two apart, so the
snapshot is always written with one address and restored into another, and every
assertion checks both directions (new address present, snapshot address absent).

Job creation, not job submission
--------------------------------
The scenario stops at ``check_job_creation()`` / ``create_job_script()``, which
is what task 17.4 prescribes: the job script is really rendered to disk and the
run command is really built, but no ``/bin/sh`` is spawned. That is deliberate
rather than a shortcut -- the script used here is the repository's
``task1_with_include.takler``, whose include exports ``TAKLER_HOST`` /
``TAKLER_PORT`` and then calls ``python -m takler.client init`` after sourcing a
developer-specific conda profile. Spawning it would exercise that developer's
machine instead of the restore, while *reading* the rendered file is exactly what
proves the resolved address reaches the job. Everything written lands under
``tmp_path`` through the ``TAKLER_HOME`` parameter, so no test leaves a job file
in the repository.

There is no ``pytest-asyncio`` in this project; the one test that needs a live
server reuses the ``takler_server`` fixture from ``conftest.py``, which owns the
event loop in a background thread.

Validates: Requirements 16.15, 6.13, 6.5, 6.12, 6.22
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Tuple

import pytest

from takler.core import Bunch, Flow, NodeStatus
from takler.core.parameter import TAKLER_HOME, TAKLER_HOST, TAKLER_PORT
from takler.server.checkpoint import CheckpointManager
from takler.tasks import ShellScriptTask
from takler.tasks.shell.constant import TAKLER_INCLUDE, TAKLER_JOB, TAKLER_JOBOUT

# ``ShellScriptTask`` renders a POSIX shell script and its default run command is
# a ``sh`` redirection, mirroring the skip of ``tests/tasks/shell``.
pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="tests for linux only")


#: Address recorded in the snapshot: a host/port pair that is deliberately *not*
#: the one restoring it, and that nothing in the test ever connects to.
SNAPSHOT_HOST = "10.10.10.10"
SNAPSHOT_PORT = "31071"

#: Address of the "current process" in the two-bunch tests. No socket is bound:
#: those tests drive ``CheckpointManager`` directly, so the address only has to
#: be different from the snapshot's.
CURRENT_HOST = "127.0.0.1"
CURRENT_PORT = "33333"

#: Node path of the one ``ShellScriptTask`` the scenario is built around.
TASK_PATH = "/flow1/task1"

#: The try the task was on when the snapshot was written. Carried by the
#: snapshot, so the regenerated job path must use it.
SNAPSHOT_TRY_NO = 1


def _shell_test_data(name: str) -> Path:
    """Return a path inside ``tests/tasks/shell``.

    Task 17.4 asks for a script taken from the repository's own script
    directory, so the templates are reused from where the ``ShellScriptTask``
    unit tests keep them rather than written fresh into ``tmp_path``.
    """
    return Path(__file__).parents[1] / "tasks" / "shell" / name


#: Script with an include that exports ``TAKLER_HOST`` / ``TAKLER_PORT`` /
#: ``TAKLER_NAME``. Chosen over the plain ``task1.takler`` precisely because the
#: rendered output then *shows* which address the restored task resolved.
SCRIPT_PATH = _shell_test_data("scripts/task1_with_include.takler")
INCLUDE_PATH = _shell_test_data("include")


# ---------------------------------------------------------------------------
# The flow that gets snapshotted
# ---------------------------------------------------------------------------


def _build_bunch(takler_home: Path, host: str, port: str) -> Bunch:
    """Build the pre-crash bunch: one begun flow with one aborted shell task.

    ``TAKLER_HOME`` and ``TAKLER_INCLUDE`` are user parameters on the flow, so
    they travel *inside* the snapshot and the restored task keeps writing its
    job under ``tmp_path``. ``TAKLER_HOST`` / ``TAKLER_PORT`` deliberately are
    not: they come from the bunch's server state, which is what the restore must
    replace.

    The task is left aborted on try 1 -- the state a job that died together with
    the server ends up in. It also keeps the live server of the last test from
    submitting it: ``Task.check_dependencies`` refuses to run an aborted task,
    so the tree the assertions read stays exactly as restored.
    """
    bunch = Bunch("ops", host=host, port=port)
    with Flow("flow1") as flow1:
        bunch.add_flow(flow1)
        flow1.add_parameter(TAKLER_HOME, str(takler_home))
        flow1.add_parameter(TAKLER_INCLUDE, str(INCLUDE_PATH))
        with flow1.add_task(ShellScriptTask("task1", str(SCRIPT_PATH))) as task1:
            task1.add_parameter("SLEEP", 1)
    flow1.begin()

    # ``begin`` requeues the tree, so the try counter is set afterwards.
    task1.try_no = SNAPSHOT_TRY_NO
    task1.abort("server was killed while the job was running")

    return bunch


def _write_snapshot(tmp_path: Path) -> Path:
    """Snapshot the pre-crash bunch and return the Checkpoint_File path.

    ``write_checkpoint`` is the synchronous write path, which is all this needs:
    no server is running, and the periodic task would only add a loop between
    the bunch and the file. The path is always under ``tmp_path`` -- the
    built-in default would drop ``takler.check`` into the working directory.
    """
    checkpoint_file = tmp_path / "takler.check"
    bunch = _build_bunch(
        tmp_path / "takler_home", host=SNAPSHOT_HOST, port=SNAPSHOT_PORT
    )
    manager = CheckpointManager(bunch=bunch, checkpoint_file=checkpoint_file)

    assert manager.write_checkpoint() is True
    assert checkpoint_file.exists()

    return checkpoint_file


def _snapshot_task_dict(checkpoint_file: Path) -> dict:
    """Return the serialized dictionary of ``/flow1/task1`` from the file.

    Read from the file rather than from the pre-crash Python object: the file is
    the only thing a restart has, so it is the only legitimate expectation.
    """
    snapshot = json.loads(checkpoint_file.read_text(encoding="utf-8"))
    (flow_dict,) = snapshot["bunch"]["flows"]
    (task_dict,) = flow_dict["children"]
    assert task_dict["name"] == "task1"
    return task_dict


def _restore(checkpoint_file: Path) -> Tuple[Bunch, ShellScriptTask]:
    """Restore the snapshot into a fresh bunch bound to a *different* address.

    Returns:
        ``(bunch, task)`` -- the bunch that now owns the restored flow, and the
        restored ``ShellScriptTask``.
    """
    bunch = Bunch("ops", host=CURRENT_HOST, port=CURRENT_PORT)
    manager = CheckpointManager(bunch=bunch, checkpoint_file=checkpoint_file)

    assert manager.restore() is True

    task = bunch.find_node(TASK_PATH)
    assert isinstance(task, ShellScriptTask), f"{TASK_PATH} came back as {task!r}"
    return bunch, task


@pytest.fixture
def restored(tmp_path: Path) -> Tuple[Path, Bunch, ShellScriptTask]:
    """Write a snapshot on one address and restore it on another.

    Returns:
        ``(checkpoint_file, bunch, task)``.
    """
    checkpoint_file = _write_snapshot(tmp_path)
    bunch, task = _restore(checkpoint_file)
    return checkpoint_file, bunch, task


# ===========================================================================
# Requirement 6.13 -- the script survives the restore
# ===========================================================================


def test_restored_shell_task_keeps_the_script_path_of_the_snapshot(
        restored: Tuple[Path, Bunch, ShellScriptTask],
) -> None:
    """Requirement 6.13: ``script_path`` comes back exactly as written.

    Without it the restored task has nothing to render and every later
    submission fails with ``JobSubmissionError``, which is why this is asserted
    against the snapshot file before anything is rendered.
    """
    checkpoint_file, _, task = restored
    expected = _snapshot_task_dict(checkpoint_file)

    assert expected["script_path"] == str(SCRIPT_PATH), (
        "the snapshot itself lost the script path"
    )
    assert task.script_path == expected["script_path"]

    # The rest of the definition and the runtime state the next submission
    # depends on came back too.
    assert task.try_no == expected["try_no"] == SNAPSHOT_TRY_NO
    assert task.state.node_status == NodeStatus.aborted
    assert task.find_parameter("SLEEP").value == 1


# ===========================================================================
# Requirements 6.5 / 6.12 / 6.22 -- the address is this process's
# ===========================================================================


def test_restored_shell_task_resolves_the_current_server_address(
        restored: Tuple[Path, Bunch, ShellScriptTask],
) -> None:
    """Requirements 6.5, 6.12, 6.22: the chain reaches *this* bunch.

    The restored flow must hold a back reference to the bunch that restored it,
    so ``find_parent_parameter`` walks past the flow into the server state --
    and what it finds there is the current process's host and port, not the
    values the snapshot recorded.
    """
    checkpoint_file, bunch, task = restored

    # Guard the scenario: an assertion that the address is "current" proves
    # nothing if the snapshot happened to record the same one.
    snapshot = json.loads(checkpoint_file.read_text(encoding="utf-8"))
    assert snapshot["bunch"]["server_state"]["host"] == SNAPSHOT_HOST
    assert snapshot["bunch"]["server_state"]["port"] == SNAPSHOT_PORT
    assert (SNAPSHOT_HOST, SNAPSHOT_PORT) != (CURRENT_HOST, CURRENT_PORT)

    assert task.get_bunch() is bunch, "the restored flow lost its bunch"

    host = task.find_parent_parameter(TAKLER_HOST)
    port = task.find_parent_parameter(TAKLER_PORT)
    assert host is not None and port is not None, (
        "the parameter inheritance chain does not reach the server parameters"
    )
    assert host.value == CURRENT_HOST
    assert port.value == CURRENT_PORT

    # And the whole parameter view the job template is rendered from agrees.
    params = task.parameters()
    assert params[TAKLER_HOST].value == CURRENT_HOST
    assert params[TAKLER_PORT].value == CURRENT_PORT
    assert params[TAKLER_HOME].value == str(checkpoint_file.parent / "takler_home")


# ===========================================================================
# Requirement 16.15 -- the restored task can create its job again
# ===========================================================================


def test_restored_shell_task_can_create_its_job_again(
        restored: Tuple[Path, Bunch, ShellScriptTask],
) -> None:
    """Requirement 16.15: job creation succeeds and carries the new address.

    ``check_job_creation`` runs the real rendering path -- template lookup,
    include resolution, job file written and made executable, run command built
    -- and the rendered file is then read back, because "the job was created" is
    only half the requirement: the job also has to be able to talk to the server
    that created it.
    """
    _, _, task = restored

    assert task.check_job_creation() is True

    job_path = Path(task.find_parameter(TAKLER_JOB).value)
    expected_home = task.find_parent_parameter(TAKLER_HOME).value
    assert job_path == Path(f"{expected_home}{TASK_PATH}.job{SNAPSHOT_TRY_NO}")
    assert job_path.is_file()
    assert os.access(job_path, os.X_OK), "the job script was not made executable"

    job_script = job_path.read_text(encoding="utf-8")
    assert f"export {TAKLER_HOST}={CURRENT_HOST}" in job_script
    assert f"export {TAKLER_PORT}={CURRENT_PORT}" in job_script
    assert f"export TAKLER_NAME={TASK_PATH}" in job_script
    # The regression this guards: a chain that stops at the flow renders the
    # parameter as an empty Jinja2 value instead of failing loudly.
    assert f"export {TAKLER_HOST}=\n" not in job_script
    assert SNAPSHOT_HOST not in job_script
    assert SNAPSHOT_PORT not in job_script

    # The run command the submission would hand to the runner names the very
    # job file that was just rendered, and its output file.
    run_command = task.create_job_script()
    assert str(job_path) in run_command
    assert str(task.find_parameter(TAKLER_JOBOUT).value) in run_command


# ===========================================================================
# Requirement 16.15 through a live server
# ===========================================================================


def test_task_restored_by_a_started_server_creates_a_job_for_that_server(
        tmp_path: Path,
        request: pytest.FixtureRequest,
) -> None:
    """The same scenario through ``TaklerServer.start`` and a bound port.

    The two-bunch tests above drive ``CheckpointManager`` directly; this one
    goes through the startup restore of a server that is really listening, so
    the address the restored task resolves is a port some client could actually
    dial rather than a constant.

    The ``takler_server`` fixture is requested from inside the test body on
    purpose: it restores ``tmp_path / "takler.check"`` while starting, so the
    snapshot has to be on disk *before* the fixture runs. The fixture picks its
    own free port, which is what makes the restored address differ from the
    snapshot's.
    """
    checkpoint_file = _write_snapshot(tmp_path)
    assert checkpoint_file == tmp_path / "takler.check", (
        "the snapshot must be where the takler_server fixture restores from"
    )

    runner = request.getfixturevalue("takler_server")

    task = runner.server.bunch.find_node(TASK_PATH)
    assert isinstance(task, ShellScriptTask), f"{TASK_PATH} was not restored"
    assert task.script_path == str(SCRIPT_PATH)

    assert task.find_parent_parameter(TAKLER_HOST).value == runner.host
    assert task.find_parent_parameter(TAKLER_PORT).value == str(runner.port)

    assert task.check_job_creation() is True
    job_script = Path(task.find_parameter(TAKLER_JOB).value).read_text(
        encoding="utf-8"
    )
    assert f"export {TAKLER_HOST}={runner.host}" in job_script
    assert f"export {TAKLER_PORT}={runner.port}" in job_script
    assert SNAPSHOT_PORT not in job_script
