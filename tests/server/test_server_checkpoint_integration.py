"""``TaklerServer`` <-> ``CheckpointManager`` wiring (Requirements 5.9, 6.1).

What is under test here is *ordering*, not snapshot content: the snapshot format,
the atomic write and the restore fallback chain are covered by the
``test_checkpoint_*`` modules. This module asserts the two orderings that
``TaklerServer`` alone is responsible for:

* ``start()``: ``checkpoint_manager.restore()`` completes before the scheduler is
  started -- i.e. before the main loop can resolve a single dependency
  (Requirement 6.1) -- and the periodic snapshot task is created last.
* ``_shutdown()``: ``checkpoint_manager.stop()`` -- which writes the final
  snapshot -- runs after the network service and the scheduler have stopped, so
  the snapshot sees a quiesced bunch (Requirement 5.9).

Collaborators are replaced with mocks that append their name to one shared list,
which makes the assertion the literal expected call sequence. The scheduler and
the network service are mocked out in every test in this module so no port is
bound and no main loop runs; the checkpoint manager is the real thing wherever
the test cares about what ends up on disk, and every such test points it at a
``tmp_path`` so the suite never writes into the repository.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import List
from unittest import mock

from takler.core import Flow
from takler.server import TaklerServer
from takler.server.checkpoint import (
    DEFAULT_CHECKPOINT_FILE,
    DEFAULT_CHECKPOINT_INTERVAL,
)


def _make_server(tmp_path: Path, **kwargs) -> TaklerServer:
    """Build a server whose snapshot lives under ``tmp_path``.

    ``checkpoint_file`` is always passed explicitly: the built-in default
    resolves to ``takler.check`` relative to the current working directory, and
    a test that used it would drop a snapshot next to the sources.
    """
    kwargs.setdefault("checkpoint_file", tmp_path / "takler.check")
    return TaklerServer(host="localhost", port=33999, **kwargs)


def _mock_services(server: TaklerServer, calls: List[str]) -> None:
    """Replace scheduler and network service with order-recording mocks."""
    server.scheduler = mock.AsyncMock()
    server.scheduler.start.side_effect = lambda: calls.append("scheduler.start")
    server.scheduler.stop.side_effect = lambda: calls.append("scheduler.stop")

    server.network_service = mock.AsyncMock()
    server.network_service.start.side_effect = lambda: calls.append("network.start")
    server.network_service.stop.side_effect = lambda: calls.append("network.stop")


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_checkpoint_manager_shares_the_live_bunch(tmp_path: Path) -> None:
    """The manager snapshots the very bunch the other services operate on.

    A copy would make every snapshot lag behind the running state, so identity
    (not equality) is what is asserted.
    """
    server = _make_server(tmp_path)
    assert server.checkpoint_manager.bunch is server.bunch


def test_explicit_checkpoint_arguments_reach_the_manager(tmp_path: Path) -> None:
    """``checkpoint_file`` / ``checkpoint_interval`` are forwarded verbatim."""
    checkpoint_file = tmp_path / "sub" / "custom.check"
    server = _make_server(
        tmp_path, checkpoint_file=checkpoint_file, checkpoint_interval=30.0
    )

    assert server.checkpoint_manager.checkpoint_file == checkpoint_file
    assert server.checkpoint_manager.interval == 30.0


def test_checkpoint_defaults_are_left_to_the_manager() -> None:
    """Omitting both arguments keeps the documented built-in defaults.

    Constructing a server must not write anything, so this is the one test that
    may leave the path at its default: the assertion is on the resolved path
    value, and nothing here starts or stops the server.
    """
    server = TaklerServer(host="localhost", port=33999)

    assert server.checkpoint_manager.checkpoint_file == Path(DEFAULT_CHECKPOINT_FILE)
    assert server.checkpoint_manager.interval == DEFAULT_CHECKPOINT_INTERVAL


# ---------------------------------------------------------------------------
# start(): restore before the main loop (Requirement 6.1)
# ---------------------------------------------------------------------------


def test_start_restores_before_the_scheduler_starts(tmp_path: Path) -> None:
    """Requirement 6.1: the restore completes before the main loop exists."""
    calls: List[str] = []
    server = _make_server(tmp_path)
    _mock_services(server, calls)

    server.checkpoint_manager.restore = mock.MagicMock(
        side_effect=lambda: calls.append("checkpoint.restore") or True
    )
    server.checkpoint_manager.start = mock.AsyncMock(
        side_effect=lambda: calls.append("checkpoint.start")
    )

    asyncio.run(server.start())

    assert calls == [
        "checkpoint.restore",
        "scheduler.start",
        "network.start",
        "checkpoint.start",
    ]


def test_start_sees_the_restored_flows_in_the_bunch(tmp_path: Path) -> None:
    """A snapshot written by one server is loaded by the next one's ``start()``.

    Covers the wiring end to end with a real manager: the restore happens inside
    ``start()`` and mutates the bunch the scheduler was handed, which is what
    Requirement 6.1 is about.
    """
    checkpoint_file = tmp_path / "takler.check"

    first = _make_server(tmp_path, checkpoint_file=checkpoint_file)
    flow = Flow("flow1")
    flow.add_task("task1")
    first.bunch.add_flow(flow)
    assert first.checkpoint_manager.write_checkpoint() is True

    # A brand new server process pointed at the same snapshot.
    second = _make_server(tmp_path, checkpoint_file=checkpoint_file)
    _mock_services(second, [])
    second.checkpoint_manager.start = mock.AsyncMock()
    assert second.bunch.flows == {}

    asyncio.run(second.start())

    assert list(second.bunch.flows) == ["flow1"]
    restored_flow = second.bunch.find_flow("flow1")
    assert [n.name for n in restored_flow.children] == ["task1"]


# ---------------------------------------------------------------------------
# _shutdown(): final snapshot last (Requirement 5.9)
# ---------------------------------------------------------------------------


def test_shutdown_stops_the_checkpoint_manager_last(tmp_path: Path) -> None:
    """Requirement 5.9: the final snapshot is written after everything stops."""
    calls: List[str] = []
    server = _make_server(tmp_path)
    _mock_services(server, calls)

    server.checkpoint_manager.stop = mock.AsyncMock(
        side_effect=lambda: calls.append("checkpoint.stop")
    )

    asyncio.run(server.stop())

    assert calls == ["network.stop", "scheduler.stop", "checkpoint.stop"]


def test_shutdown_writes_a_final_snapshot_of_the_quiesced_bunch(
    tmp_path: Path,
) -> None:
    """A clean shutdown leaves a readable snapshot of the current bunch.

    Requirement 5.9: state added after the last periodic write must still make it
    to disk, so the file is checked for a flow that only ever existed in memory.
    """
    checkpoint_file = tmp_path / "takler.check"
    server = _make_server(tmp_path, checkpoint_file=checkpoint_file)
    _mock_services(server, [])

    flow = Flow("flow1")
    flow.add_task("task1")
    server.bunch.add_flow(flow)
    assert not checkpoint_file.exists()

    asyncio.run(server.stop())

    assert checkpoint_file.exists()
    snapshot = json.loads(checkpoint_file.read_text(encoding="utf-8"))
    assert "flow1" in json.dumps(snapshot["bunch"])


def test_shutdown_runs_at_most_once(tmp_path: Path) -> None:
    """The ``_stopped`` guard still holds now that a third service is stopped."""
    server = _make_server(tmp_path)
    _mock_services(server, [])
    server.checkpoint_manager.stop = mock.AsyncMock()

    async def stop_twice():
        await server.stop()
        await server.stop()

    asyncio.run(stop_twice())

    server.checkpoint_manager.stop.assert_awaited_once()
    server.network_service.stop.assert_awaited_once()
    server.scheduler.stop.assert_awaited_once()
