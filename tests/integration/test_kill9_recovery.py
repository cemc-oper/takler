"""``kill -9`` recovery: a restart comes back exactly as the last snapshot.

This is the first half of the M1 roadmap acceptance criterion "after a random
``kill -9``, all flow state and active job ownership recover completely"
(Requirement 16.9).

What "``kill -9``" means for this test
-------------------------------------
``SIGKILL`` gives the process no chance to do anything, so what a restart finds
on disk is whatever the *last periodic* snapshot wrote -- never a final
shutdown snapshot. The kill is therefore simulated the way the design
prescribes: build a bunch, call the synchronous
:meth:`CheckpointManager.write_checkpoint`, then abandon the whole
:class:`TaklerServer` object without any cleanup at all.

Going through ``TaklerServer.stop()`` / ``CheckpointManager.stop()`` instead
would be wrong twice over: those write a *final* snapshot, so the test would
pass because of a code path a killed process never reaches, and it would no
longer prove anything about the periodic snapshot the recovery actually rests
on.

What is asserted
----------------
Everything the restarted server holds in memory is compared against the
snapshot *file*, not against the pre-kill Python objects: the snapshot is the
only thing that survives ``SIGKILL``, so it is the only legitimate source of
truth. Per node that means status, ``suspended`` and -- for every Task_Node --
``task_id`` / ``try_no`` / ``aborted_reason`` (the active-job ownership half of
the criterion), plus each flow's ``begun`` flag and computed status.

Two related requirements ride along:

* The restore must leave submitted / active tasks alone (Requirement 6.4). A
  restore that requeued them would reset ``task_id`` and ``try_no`` and let the
  scheduler submit those jobs a second time, so this gets its own test rather
  than only being implied by the snapshot comparison.
* The restart reuses the *same* host and port, which is the positive path of
  Requirement 6.23: the snapshot holds submitted / active tasks whose job
  scripts already carry that address, so the address verification must take its
  INFO branch (Requirement 6.18) rather than the ERROR one. The three grading
  branches themselves are covered by the address property test; here only the
  matching case is checked, because it is what makes the recovered jobs able to
  report back.

This is the *in-process* layer, the one the design marks as fast and always
executed. The ``@pytest.mark.slow`` real-process layer of task 17.2
(``subprocess.Popen(["takler-server", ...])``, a genuine
``os.kill(pid, SIGKILL)``, and a poll on the snapshot's ``written_at``) is not
in this module yet, for two reasons: the ``takler-server`` console script it
invokes is not declared in ``pyproject.toml`` so far (that comes with the
packaging tasks -- only ``python -m takler.server`` works today), and the
shortest snapshot period the manager accepts is 10 seconds, so that layer needs
a registered ``slow`` marker to stay out of the default run.

There is no ``pytest-asyncio`` in this project, so the async server startup is
driven with :func:`asyncio.run`, mirroring the conventions of
``tests/server/test_server_checkpoint_integration.py``.

Validates: Requirements 16.9, 6.4, 6.18, 6.23, 8.15
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import socket
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

import takler.logging
from takler.core import Bunch, Flow, NodeStatus
from takler.core.task_node import Task
from takler.server import TaklerServer

#: The restart must announce the same address as the killed process
#: (Requirement 6.23), so host and port are resolved once and reused.
HOST = "127.0.0.1"


def _free_port() -> int:
    """Return a currently free TCP port.

    The restarted server really binds a gRPC port, and a hard-coded number
    would collide with a parallel test run or with a server left over from an
    earlier one.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


# ---------------------------------------------------------------------------
# The bunch that gets killed
# ---------------------------------------------------------------------------


def _build_operational_bunch(bunch: Bunch) -> None:
    """Fill ``bunch`` with one begun flow in a mixed state and one idle flow.

    Every status a real server can be killed in is represented, and each one is
    reached through the operation that produces it in production rather than by
    assigning to ``state`` directly -- so ``try_no``, ``task_id`` and
    ``aborted_reason`` end up in exactly the combinations a live server would
    have been holding:

    * ``/ops/pre/prepare``  -- complete
    * ``/ops/pre/fetch``    -- aborted on its second try, with a reason and the
      job id of that try still attached
    * ``/ops/model/forecast`` -- active, i.e. a job that is running right now
      and will report back after the restart
    * ``/ops/model/post``   -- submitted, i.e. a job handed to the runner
    * ``/ops/archive``      -- queued and suspended
    * ``/idle/standby``     -- in a flow that was never begun, so it is
      ``unknown`` and its flow's ``begun`` is ``False``
    """
    flow = Flow("ops")
    with flow.add_container("pre") as pre:
        pre.add_task("prepare")
        pre.add_task("fetch")
    with flow.add_container("model") as model:
        model.add_task("forecast")
        model.add_task("post")
    flow.add_task("archive")
    bunch.add_flow(flow)
    flow.begin()

    flow.find_node("/ops/pre/prepare").complete()

    fetch = flow.find_node("/ops/pre/fetch")
    fetch.run()  # try_no 1
    fetch.run()  # try_no 2, and ``increment_try_no`` cleared the first job id
    fetch.init(task_id="job-fetch-2")
    fetch.abort("exit code 137")

    forecast = flow.find_node("/ops/model/forecast")
    forecast.run()
    forecast.init(task_id="job-forecast-1")

    flow.find_node("/ops/model/post").run()

    flow.find_node("/ops/archive").suspend()

    # A second flow that was loaded but never begun: its ``begun`` flag must
    # stay ``False`` across the restart just as the begun one must stay ``True``
    # (Requirement 8.15).
    idle = Flow("idle")
    idle.add_task("standby")
    bunch.add_flow(idle)


# ---------------------------------------------------------------------------
# Snapshot -> expectations
# ---------------------------------------------------------------------------


def _walk_snapshot(node_dict: dict, path: str) -> Iterator[Tuple[str, dict]]:
    """Yield ``(node_path, node_dict)`` for ``node_dict`` and its descendants."""
    yield path, node_dict
    for child in node_dict.get("children", []):
        yield from _walk_snapshot(child, f"{path}/{child['name']}")


def _snapshot_expectations(
        checkpoint_file: Path,
) -> Tuple[Dict[str, dict], Dict[str, bool]]:
    """Read the snapshot file and return what a correct restore must produce.

    Returns:
        ``(nodes, begun)`` where ``nodes`` maps every node path in the snapshot
        to its serialized dictionary, and ``begun`` maps every flow name to its
        recorded ``begun`` flag.
    """
    snapshot = json.loads(checkpoint_file.read_text(encoding="utf-8"))

    nodes: Dict[str, dict] = {}
    begun: Dict[str, bool] = {}
    for flow_dict in snapshot["bunch"]["flows"]:
        begun[flow_dict["name"]] = flow_dict["begun"]
        for path, node_dict in _walk_snapshot(flow_dict, f"/{flow_dict['name']}"):
            nodes[path] = node_dict
    return nodes, begun


def _restored_paths(bunch: Bunch) -> List[str]:
    """Every node path the restored bunch holds, flows included."""
    paths: List[str] = []

    def walk(node) -> None:
        paths.append(node.node_path)
        for child in node.children:
            walk(child)

    for flow in bunch.flows.values():
        walk(flow)
    return paths


# ---------------------------------------------------------------------------
# The kill and the restart
# ---------------------------------------------------------------------------


def _write_snapshot_then_lose_the_process(tmp_path: Path, port: int) -> Path:
    """Snapshot a mixed-state bunch, then drop the server as ``SIGKILL`` would.

    The server is never started, so no port is bound and no periodic task
    exists; ``write_checkpoint`` stands in for the periodic write that happened
    to be the last one before the kill. Nothing is stopped and nothing is
    flushed afterwards -- that is the whole point.

    Returns:
        The Checkpoint_File path, always under ``tmp_path`` so the suite never
        writes a snapshot into the repository.
    """
    checkpoint_file = tmp_path / "takler.check"
    server = TaklerServer(host=HOST, port=port, checkpoint_file=checkpoint_file)
    _build_operational_bunch(server.bunch)

    assert server.checkpoint_manager.write_checkpoint() is True
    assert checkpoint_file.exists()

    # The process is gone here. No ``stop()``, so no final snapshot: what
    # follows may only rely on the file written above.
    del server

    return checkpoint_file


def _restart(checkpoint_file: Path, port: int) -> Tuple[TaklerServer, str]:
    """Start a fresh server on the same address and return it plus its log.

    ``start()`` is the real thing: it restores the snapshot, binds the gRPC port
    and creates the periodic snapshot task. Both resources are released before
    returning, but *not* through ``TaklerServer.stop()``: ``stop()`` awaits
    ``Scheduler.stop``, which waits for the main loop to acknowledge the stop
    flag, and ``run()`` -- the only thing that starts that loop -- is
    deliberately never called here. Leaving the loop unstarted is what keeps the
    restored tree pristine for the assertions: a single main-loop pass would
    submit the queued tasks and the test could no longer tell a correct restore
    from a scheduler side effect.

    The startup log is captured from the console sink, which
    ``TaklerServer.start`` installs on the current ``sys.stderr`` when it calls
    ``takler.logging.configure()``; the sink is rebound to the real stderr
    afterwards so no later test logs into this buffer.
    """
    server = TaklerServer(host=HOST, port=port, checkpoint_file=checkpoint_file)
    buffer = io.StringIO()

    async def runner() -> None:
        await server.start()
        await server.network_service.stop()
        await server.checkpoint_manager.stop()

    try:
        with contextlib.redirect_stderr(buffer):
            asyncio.run(runner())
    finally:
        takler.logging.configure(console=True)

    return server, buffer.getvalue()


def _kill9_then_restart(
        tmp_path: Path,
) -> Tuple[Dict[str, dict], Dict[str, bool], TaklerServer, str]:
    """Run the whole scenario: snapshot, kill, restart on the same address.

    The expectations are read from the snapshot file *before* the restart,
    because the restarted server writes its own snapshot over that path while
    being shut down again.
    """
    port = _free_port()
    checkpoint_file = _write_snapshot_then_lose_the_process(tmp_path, port)
    expected_nodes, expected_begun = _snapshot_expectations(checkpoint_file)
    server, log = _restart(checkpoint_file, port)
    return expected_nodes, expected_begun, server, log


# ===========================================================================
# Requirement 16.9 -- every node comes back as the snapshot recorded it
# ===========================================================================


def test_every_node_matches_the_snapshot_after_a_kill9(tmp_path: Path) -> None:
    """Status, ``suspended`` and job ownership all survive the kill.

    Requirement 16.9: the restarted server's node tree must equal the snapshot
    node for node -- no node missing, none invented, and for every Task_Node the
    ``task_id`` / ``try_no`` / ``aborted_reason`` that identify its job.
    """
    expected_nodes, _, server, _ = _kill9_then_restart(tmp_path)

    # Spelled out rather than derived, so an empty snapshot -- which would make
    # every loop below vacuous -- cannot pass this test.
    assert sorted(expected_nodes) == [
        "/idle",
        "/idle/standby",
        "/ops",
        "/ops/archive",
        "/ops/model",
        "/ops/model/forecast",
        "/ops/model/post",
        "/ops/pre",
        "/ops/pre/fetch",
        "/ops/pre/prepare",
    ]
    assert sorted(_restored_paths(server.bunch)) == sorted(expected_nodes)

    for path, expected in expected_nodes.items():
        node = server.bunch.find_node(path)
        assert node is not None, f"{path} was not restored"

        assert node.state.node_status == NodeStatus(expected["state"]["status"]), (
            f"{path} came back as {node.state.node_status!r}"
        )
        assert node.state.suspended == expected["state"]["suspended"], (
            f"{path} came back with suspended={node.state.suspended!r}"
        )

        if isinstance(node, Task):
            assert node.task_id == expected["task_id"], (
                f"{path} lost its job id: {node.task_id!r}"
            )
            assert node.try_no == expected["try_no"], (
                f"{path} came back on try {node.try_no}"
            )
            assert node.aborted_reason == expected["aborted_reason"], (
                f"{path} came back with aborted_reason="
                f"{node.aborted_reason!r}"
            )


def test_flow_state_and_begun_survive_the_kill(tmp_path: Path) -> None:
    """Each flow keeps its ``begun`` flag and its aggregate status.

    Requirement 8.15: a flow that had begun is still begun after the restart, so
    the main loop resumes processing it instead of skipping it; a flow that had
    not begun is still waiting. The computed status is checked as well, since
    that is what an operator sees at the top of ``show``: with an aborted task
    below it, ``/ops`` must still read as aborted rather than as anything the
    restore invented.
    """
    _, expected_begun, server, _ = _kill9_then_restart(tmp_path)

    assert sorted(server.bunch.flows) == sorted(expected_begun)
    for name, begun in expected_begun.items():
        assert server.bunch.find_flow(name).begun is begun, (
            f"flow {name!r} came back with begun="
            f"{server.bunch.find_flow(name).begun!r}"
        )

    # Sanity check on the scenario itself: the mixed flow really did record a
    # begun flow and the idle one really did not.
    assert expected_begun == {"ops": True, "idle": False}

    assert server.bunch.find_flow("ops").computed_status(True) == NodeStatus.aborted
    assert server.bunch.find_flow("idle").computed_status(True) == NodeStatus.unknown


# ===========================================================================
# Requirement 6.4 -- in-flight jobs keep their owner
# ===========================================================================


def test_submitted_and_active_tasks_are_not_requeued_by_the_restore(
        tmp_path: Path,
) -> None:
    """Requirement 6.4: the restore does not touch jobs that are in flight.

    A requeue here would be the worst possible outcome of a restart: it clears
    ``task_id`` and ``try_no`` and puts the task back in ``queued``, so the
    scheduler submits a job that is still running a second time, and the child
    commands of the original job then report against a task that no longer
    knows them.
    """
    _, _, server, _ = _kill9_then_restart(tmp_path)

    active = server.bunch.find_node("/ops/model/forecast")
    assert active.state.node_status == NodeStatus.active
    assert active.task_id == "job-forecast-1"
    assert active.try_no == 1

    submitted = server.bunch.find_node("/ops/model/post")
    assert submitted.state.node_status == NodeStatus.submitted
    assert submitted.try_no == 1

    aborted = server.bunch.find_node("/ops/pre/fetch")
    assert aborted.state.node_status == NodeStatus.aborted
    assert aborted.task_id == "job-fetch-2"
    assert aborted.try_no == 2
    assert aborted.aborted_reason == "exit code 137"


# ===========================================================================
# Requirements 6.23 / 6.18 -- restarting on the same address
# ===========================================================================


def test_restarting_on_the_same_address_reports_a_matching_address(
        tmp_path: Path,
) -> None:
    """Requirement 6.23: the snapshot's address is reused, so the check passes.

    The snapshot holds submitted and active tasks whose job scripts already
    carry ``TAKLER_HOST`` / ``TAKLER_PORT`` from before the kill. Coming back on
    the same address is what lets their child commands reach the server again,
    and the address verification must say so at INFO -- the ERROR branch of
    Requirement 6.20 would mean those jobs are talking to nobody.
    """
    _, _, server, log = _kill9_then_restart(tmp_path)

    address_lines = [
        line for line in log.splitlines() if "checkpoint server address" in line
    ]
    assert len(address_lines) == 1, f"expected one address record, got {address_lines}"
    assert "INFO" in address_lines[0]
    assert HOST in address_lines[0]
    assert str(server.bunch.server_state.port) in address_lines[0]

    # Requirement 6.5 / 6.22: the address announced to jobs is this process's,
    # not a value read back out of the snapshot.
    assert server.bunch.server_state.host == HOST
