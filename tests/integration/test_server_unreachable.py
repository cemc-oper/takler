"""A Child_Command outliving a five minute outage (Requirement 16.10).

This is the second half of the M1 acceptance criterion: *a job submitted while
the server is unreachable for five minutes must not be misjudged as aborted*.
The scenario is run for real -- a real ``TaklerServer`` bound to a real port, a
real ``TaklerServiceClient`` speaking gRPC over the loopback interface -- with
one thing faked: **time**.

Faking time is what makes the test both fast and deterministic. ``RetryPolicy``
takes its ``clock`` and its ``sleep`` as constructor arguments and
``TaklerServiceClient`` forwards both, so the shared ``FakeClock`` from
``tests/conftest.py`` can be injected into the client. Every backoff wait then
advances a logical clock instead of blocking, and a 300 second outage costs no
wall-clock time.

The same injection point also makes "unreachable, then reachable" an ordering
rather than a race: the injected sleep function *is* the synchronization point.
It parks the retrying client until the test has restarted the server, so the
server always comes back between two retries, never in the middle of one.

Layout of the scenario:

1. server A starts, ``flow1`` is begun and ``/flow1/task1`` is forced to
   ``submitted`` -- the state of a job that has just been handed to a batch
   system,
2. server A writes its snapshot and stops listening: the port is now refused,
3. the job's ``init`` Child_Command is invoked from a worker thread (the gRPC
   client is blocking) and starts retrying,
4. once the injected sleeps have pushed logical time past 300 seconds, server B
   is started on the same port from the same Checkpoint_File,
5. the ``init`` call is expected to succeed against server B, and ``task1`` must
   be ``active`` -- never ``aborted``.

Both servers are constructed with an explicit ``checkpoint_file`` under
``tmp_path``: the built-in default resolves relative to the current working
directory, and a test that used it would drop a snapshot into the repository.

Neither server's main loop is started, so no scheduler pass can move a node
behind the test's back and ``TaklerServer.stop()`` -- which waits for the main
loop to acknowledge the stop flag -- is not usable here; the services that were
started are stopped individually instead.

Requirements: 16.10, 9.3, 9.4, 9.10.
"""

from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from typing import Any, Dict, Union

import grpc
import pytest

from takler.client.service_client import TaklerServiceClient
from takler.core import Flow, NodeStatus
from takler.server import TaklerServer
from takler.server.connect_config import get_port

#: How long the server stays unreachable, in logical seconds. The M1 acceptance
#: criterion names five minutes.
OUTAGE_SECONDS: float = 300.0

#: Path of the task whose job runs the Child_Command.
NODE_PATH: str = "/flow1/task1"

#: ``task_id`` the job reports through ``init``, checked on the server side to
#: prove the command really landed.
TASK_ID: str = "job-4242"

#: Wall-clock budget for the retry loop itself, in real seconds, i.e. the whole
#: Child_Command minus the one bounded wait for gRPC's channel to reconnect.
#: This is the assertion that would catch a real ``time.sleep`` creeping back
#: into the retry path: 300 logical seconds of backoff must cost nothing.
RETRY_WALL_CLOCK_BUDGET: float = 2.0

#: Wall-clock budget for the whole Child_Command, in real seconds. Above the
#: retry budget because gRPC's reconnect backoff after a refused connection is
#: real time nobody can fake; still two orders of magnitude below the 300
#: logical seconds the command spans.
CALL_WALL_CLOCK_BUDGET: float = 5.0

#: Deadlock guard for the worker thread waiting on the restart, in real seconds.
#: Never reached on a healthy run.
RESTART_TIMEOUT: float = 30.0

#: Deadlock guard for the wait on the reconnected channel, in real seconds.
CHANNEL_READY_TIMEOUT: float = 10.0

#: Snapshot period, in seconds. Large enough that no periodic write happens
#: during the test, so the only snapshots are the explicit ones.
CHECKPOINT_INTERVAL: float = 3600.0


def _make_server(port: Union[int, str], checkpoint_file: Path) -> TaklerServer:
    """Build a server on ``port`` whose snapshot lives under ``tmp_path``."""
    return TaklerServer(
        host="127.0.0.1",
        port=port,
        checkpoint_file=checkpoint_file,
        checkpoint_interval=CHECKPOINT_INTERVAL,
    )


async def _stop_listening(server: TaklerServer) -> None:
    """Stop the services ``start()`` actually started, snapshot included.

    ``TaklerServer.stop()`` also stops the scheduler, which blocks until the
    main loop clears the stop flag; no main loop runs in this test, so the
    network service and the checkpoint manager are stopped directly.
    ``CheckpointManager.stop()`` writes the final snapshot, which is how the
    node states of the stopping server reach the one that replaces it.
    """
    await server.network_service.stop()
    await server.checkpoint_manager.stop()


def test_child_command_survives_a_five_minute_outage(
    tmp_path: Path, fake_clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Requirement 16.10: the command succeeds later, the task is not aborted."""
    # The Retry_Window must be the built-in child-command default (86400
    # seconds, Requirement 9.10); a ``TAKLER_TIMEOUT`` inherited from the
    # developer's environment would silently replace it.
    monkeypatch.delenv("TAKLER_TIMEOUT", raising=False)

    port = get_port()
    checkpoint_file = tmp_path / "takler.check"

    # Set by the client thread once its logical clock has crossed the outage;
    # set by the test thread once the replacement server is listening.
    outage_elapsed = threading.Event()
    server_back = threading.Event()
    timing: Dict[str, float] = {}

    def sleep_hook(seconds: float) -> None:
        """The client's ``sleep``: fake time plus the restart handshake."""
        fake_clock.sleep(seconds)
        if server_back.is_set() or fake_clock.now < OUTAGE_SECONDS:
            return
        # The outage has logically lasted five minutes: ask for the server back
        # and park here until it is listening, so the next attempt is the first
        # one that can possibly succeed.
        outage_elapsed.set()
        assert server_back.wait(RESTART_TIMEOUT), "the server was never restarted"
        # A channel whose peer refused a connection carries gRPC's own reconnect
        # backoff, and the fake sleeps consumed no real time for it to expire,
        # so an immediate attempt would fail fast on a healthy server. Waiting
        # for the channel itself keeps the next attempt the successful one; this
        # is the only real waiting in the test, bounded by gRPC's backoff and
        # unrelated to the length of the outage.
        reconnect_started = time.monotonic()
        grpc.channel_ready_future(client.channel).result(timeout=CHANNEL_READY_TIMEOUT)
        timing["reconnect_seconds"] = time.monotonic() - reconnect_started

    client = TaklerServiceClient(
        host="127.0.0.1",
        port=port,
        single_timeout=1.0,
        clock=fake_clock,
        sleep=sleep_hook,
    )

    def child_command() -> Any:
        """The job's ``init``, exactly as a job script would invoke it."""
        started = time.monotonic()
        try:
            return client.init(node_path=NODE_PATH, task_id=TASK_ID)
        finally:
            timing["call_seconds"] = time.monotonic() - started

    async def scenario():
        # 1. A server with a submitted job.
        first = _make_server(port, checkpoint_file)
        flow = Flow("flow1")
        flow.add_task("task1")
        first.bunch.add_flow(flow)
        await first.start()
        first.scheduler.run_command_begin("flow1")
        first.scheduler.run_command_force(NODE_PATH, state="submitted", recursive=False)

        # 2. The server goes away; the port is refused from here on.
        await _stop_listening(first)

        # 3. The job reports ``init`` into the void and starts retrying.
        loop = asyncio.get_running_loop()
        call = loop.run_in_executor(None, child_command)
        while not (outage_elapsed.is_set() or call.done()):
            await asyncio.sleep(0.01)
        if call.done():
            # Surfaces the client's exception instead of hanging on a restart
            # nobody is waiting for any more.
            await call
            pytest.fail("the child command finished while the server was down")

        # 4. Five logical minutes later, a new server takes over the port and
        #    the snapshot.
        second = _make_server(port, checkpoint_file)
        await second.start()
        server_back.set()
        try:
            response = await call
        finally:
            await _stop_listening(second)
        return second, response

    try:
        server, response = asyncio.run(scenario())
    finally:
        # Never leave the worker thread parked if an assertion above blew up.
        server_back.set()

    # The Child_Command succeeded once the server was back (Requirement 16.10).
    assert response.flag == 0

    # The outage was really five logical minutes of retrying, and the backoff
    # followed ``min(2 ** (n - 1), 60)`` (Requirements 9.3, 9.4). The sequence
    # is exact rather than approximate because the restart is synchronized with
    # the retry loop: 1 + 2 + 4 + 8 + 16 + 32 + 60 * 4 == 303 seconds is the
    # first cumulative backoff crossing the five minute mark.
    assert fake_clock.slept == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0, 60.0]
    assert fake_clock.now >= OUTAGE_SECONDS

    # ... and none of those 303 seconds was really waited out. The reconnect
    # wait is excluded: it is gRPC's own backoff, not takler's retry policy.
    assert timing["call_seconds"] < CALL_WALL_CLOCK_BUDGET
    retry_seconds = timing["call_seconds"] - timing["reconnect_seconds"]
    assert retry_seconds < RETRY_WALL_CLOCK_BUDGET

    # The task is active with the reported task id, and was never aborted: no
    # abort was sent, and nothing else may have written that status either.
    task = server.bunch.find_node(NODE_PATH)
    assert task is not None
    assert task.state.node_status == NodeStatus.active
    assert task.state.node_status != NodeStatus.aborted
    assert task.aborted_reason is None
    assert task.task_id == TASK_ID
