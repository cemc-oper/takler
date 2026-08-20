"""Unit tests for the Checkpoint_Manager server-address verification.

Task 8.6 of the *m1-operational-baseline* spec adds
``CheckpointManager._verify_server_address``, which compares the host / port
recorded in a snapshot against the ones the current process announces and grades
the report by how many restored Task_Nodes are still submitted or active.

These tests pin the three branches, the "exactly one record per call" shape of
each of them, and the two things the comparison must never do: raise, or write
the snapshot's address back into the bunch.

Snapshots come from the manager's own write path, so a change to the payload
layout cannot make these tests pass against a format nobody writes. Log
assertions go through a captured console sink because the logging backend does
not route records into pytest's handler (same approach as
``test_checkpoint_restore_unit.py``).

Validates: Requirements 6.18, 6.19, 6.20, 6.21, 6.22
"""

from __future__ import annotations

import contextlib
import io
from pathlib import Path

import takler.logging
from takler.core.bunch import Bunch
from takler.core.flow import Flow
from takler.core.parameter import TAKLER_HOST, TAKLER_PORT
from takler.server.checkpoint import CheckpointManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _source_bunch(in_flight: bool) -> Bunch:
    """A bunch announcing ``login01:33083``.

    With ``in_flight`` two tasks are left submitted / active, i.e. jobs whose
    scripts already carry the old address; without it every task stays queued,
    complete or aborted, so a changed address harms nobody.
    """
    bunch = Bunch(host="login01", port="33083")

    flow = Flow("flow1")
    with flow.add_container("container1") as container1:
        container1.add_task("task1")
        container1.add_task("task2")
        container1.add_task("task3")
    bunch.add_flow(flow)
    flow.begin()

    flow.find_node("/flow1/container1/task3").abort("job died")
    if in_flight:
        flow.find_node("/flow1/container1/task1").run()  # -> submitted
        task2 = flow.find_node("/flow1/container1/task2")
        task2.init(task_id="12345")  # -> active

    return bunch


def _restore_into(
    tmp_path: Path,
    in_flight: bool,
    host: str,
    port: str,
) -> CheckpointManager:
    """Snapshot a ``login01:33083`` bunch, then restore it into ``host:port``."""
    checkpoint_file = tmp_path / "takler.check"
    writer = CheckpointManager(
        bunch=_source_bunch(in_flight), checkpoint_file=checkpoint_file
    )
    assert writer.write_checkpoint() is True
    return CheckpointManager(
        bunch=Bunch(host=host, port=port), checkpoint_file=checkpoint_file
    )


def _capturing_stderr(func):
    """Run ``func`` while capturing the console log output."""
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="DEBUG", console=True)
            result = func()
    finally:
        takler.logging.configure(console=True)
    return result, buffer.getvalue()


def _address_lines(captured: str, level: str) -> list:
    """The address-verification records of ``level``, and only those.

    ``restore`` also logs an INFO with the restored counts, so the records of
    this one comparison have to be picked out by their own wording before their
    number can be asserted.
    """
    return [
        line
        for line in captured.splitlines()
        if level in line and "checkpoint server address" in line
    ]


# ---------------------------------------------------------------------------
# Same address (Requirement 6.18)
# ---------------------------------------------------------------------------


def test_matching_address_logs_exactly_one_info_with_host_and_port(tmp_path):
    manager = _restore_into(tmp_path, in_flight=True, host="login01", port="33083")

    result, captured = _capturing_stderr(manager.restore)

    assert result is True
    info = _address_lines(captured, "INFO")
    assert len(info) == 1
    assert "login01" in info[0]
    assert "33083" in info[0]
    assert _address_lines(captured, "WARNING") == []
    assert _address_lines(captured, "ERROR") == []


# ---------------------------------------------------------------------------
# Different address, nothing in flight (Requirements 6.19, 6.21)
# ---------------------------------------------------------------------------


def test_differing_address_without_in_flight_tasks_logs_one_warning(tmp_path):
    manager = _restore_into(tmp_path, in_flight=False, host="login02", port="44084")

    result, captured = _capturing_stderr(manager.restore)

    assert result is True  # requirement 6.21: startup carries on
    warnings = _address_lines(captured, "WARNING")
    assert len(warnings) == 1
    for value in ("login01", "33083", "login02", "44084"):
        assert value in warnings[0]
    assert _address_lines(captured, "ERROR") == []
    assert _address_lines(captured, "INFO") == []


def test_a_differing_port_alone_is_enough_to_warn(tmp_path):
    manager = _restore_into(tmp_path, in_flight=False, host="login01", port="44084")

    _, captured = _capturing_stderr(manager.restore)

    assert len(_address_lines(captured, "WARNING")) == 1


# ---------------------------------------------------------------------------
# Different address with in-flight tasks (Requirements 6.20, 6.21)
# ---------------------------------------------------------------------------


def test_differing_address_with_in_flight_tasks_logs_one_error(tmp_path):
    manager = _restore_into(tmp_path, in_flight=True, host="login02", port="44084")

    result, captured = _capturing_stderr(manager.restore)

    assert result is True  # requirement 6.21: startup carries on
    errors = _address_lines(captured, "ERROR")
    assert len(errors) == 1
    for value in ("login01", "33083", "login02", "44084"):
        assert value in errors[0]
    # The count and every affected path, so the operator can act on the log
    # alone.
    assert "2 task(s)" in errors[0]
    assert "/flow1/container1/task1" in errors[0]
    assert "/flow1/container1/task2" in errors[0]
    # The aborted task is not in flight and must not be reported as affected.
    assert "/flow1/container1/task3" not in errors[0]
    assert _address_lines(captured, "WARNING") == []
    assert _address_lines(captured, "INFO") == []


# ---------------------------------------------------------------------------
# The comparison has no side effects (Requirements 6.21, 6.22)
# ---------------------------------------------------------------------------


def test_verification_never_writes_the_snapshot_address_back(tmp_path):
    manager = _restore_into(tmp_path, in_flight=True, host="login02", port="44084")

    manager.restore()

    assert manager.bunch.server_state.host == "login02"
    assert manager.bunch.server_state.port == "44084"
    flow = manager.bunch.find_flow("flow1")
    assert flow.find_parent_parameter(TAKLER_HOST).value == "login02"
    assert flow.find_parent_parameter(TAKLER_PORT).value == "44084"


def test_verification_does_not_change_any_node_status(tmp_path):
    manager = _restore_into(tmp_path, in_flight=True, host="login02", port="44084")

    manager.restore()
    before = manager.bunch.find_flow("flow1").to_dict()
    manager._verify_server_address(manager._load_snapshot(manager.checkpoint_file))

    assert manager.bunch.find_flow("flow1").to_dict() == before


def test_a_snapshot_without_a_server_state_does_not_raise(tmp_path):
    """Requirement 6.21: a missing address is reported, never a startup failure."""
    manager = _restore_into(tmp_path, in_flight=False, host="login02", port="44084")

    _, captured = _capturing_stderr(
        lambda: manager._verify_server_address({"bunch": {}})
    )

    assert len(_address_lines(captured, "WARNING")) == 1


# ---------------------------------------------------------------------------
# _iter_restored_tasks
# ---------------------------------------------------------------------------


def test_iter_restored_tasks_yields_every_task_and_no_container(tmp_path):
    manager = _restore_into(tmp_path, in_flight=True, host="login01", port="33083")
    manager.restore()

    paths = sorted(node.node_path for node in manager._iter_restored_tasks())

    assert paths == [
        "/flow1/container1/task1",
        "/flow1/container1/task2",
        "/flow1/container1/task3",
    ]
