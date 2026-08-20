"""Unit tests for the Checkpoint_Manager restore path.

Task 8.5 of the *m1-operational-baseline* spec adds ``restore`` /
``_load_snapshot`` / ``_restore_into_bunch``. This file pins what is restored
(status and runtime state of every node, ``begun`` plus the calendar of every
flow), what is deliberately *not* restored (the snapshot's ``server_state``),
the fallback chain Checkpoint_File -> Checkpoint_Backup_File -> empty bunch, the
format-version rules and the "skip one broken flow, keep the rest" behaviour.

Snapshots are produced by the manager's own write path rather than by
hand-written JSON wherever possible, so a change to the payload layout cannot
make these tests pass against a format nobody writes.

Log assertions go through a captured console sink rather than ``caplog``: the
logging backend does not route records into pytest's handler, so the sink has to
be configured inside the redirection block (same approach as
``test_checkpoint_write_unit.py``).

Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.7, 6.8, 6.9, 6.10, 6.11,
6.13, 6.14, 6.15
"""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path

import takler.logging
from takler.core import SerializationType
from takler.core.bunch import Bunch
from takler.core.flow import Flow
from takler.core.parameter import TAKLER_HOST, TAKLER_PORT
from takler.core.state import NodeStatus
from takler.server.checkpoint import (
    CHECKPOINT_FORMAT_VERSION,
    CheckpointManager,
)
from takler.tasks.shell import ShellScriptTask


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _source_bunch() -> Bunch:
    """A bunch carrying every kind of runtime state the requirements name.

    ``flow1`` is begun and holds a container with three tasks in different
    states plus an event, a meter and a limit / in-limit pair. ``flow2`` is left
    un-begun so the ``begun`` flag is exercised in both directions.
    """
    bunch = Bunch(host="login01", port="33083")

    flow1 = Flow("flow1")
    flow1.add_limit("disk", 5)
    with flow1.add_container("container1") as container1:
        container1.add_in_limit("disk", tokens=2)
        with container1.add_task("task1") as task1:
            task1.add_event("done")
            task1.add_meter("progress", 0, 100)
        container1.add_task("task2")
        container1.add_task(
            ShellScriptTask("task3", script_path="/opt/flows/task3.takler")
        )
    bunch.add_flow(flow1)
    flow1.begin()

    task1 = flow1.find_node("/flow1/container1/task1")
    task1.init(task_id="12345")
    task1.find_event("done").value = True
    task1.find_meter("progress").value = 42

    task2 = flow1.find_node("/flow1/container1/task2")
    task2.run()  # -> submitted, try_no 1
    task2.state.suspended = True

    task3 = flow1.find_node("/flow1/container1/task3")
    task3.abort("job died")

    flow2 = Flow("flow2")
    flow2.add_task("task1")
    bunch.add_flow(flow2)

    return bunch


def _target_manager(tmp_path: Path, name: str = "takler.check") -> CheckpointManager:
    """A manager over an empty bunch, i.e. a freshly started server process."""
    return CheckpointManager(
        bunch=Bunch(host="login02", port="44084"),
        checkpoint_file=tmp_path / name,
    )


def _write_source_snapshot(tmp_path: Path, name: str = "takler.check") -> Path:
    """Write one real snapshot of :func:`_source_bunch` and return its path."""
    writer = CheckpointManager(bunch=_source_bunch(), checkpoint_file=tmp_path / name)
    assert writer.write_checkpoint() is True
    return writer.checkpoint_file


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


def _lines(captured: str, level: str) -> list:
    """The records of ``level``, minus the server-address verification.

    ``restore`` ends with ``_verify_server_address``, which grades its own
    record by the restored addresses and the in-flight tasks; the source bunch
    here announces ``login01:33083`` and the target process ``login02:44084``,
    so that comparison legitimately reports. It belongs to the address tests
    (``test_checkpoint_address_unit.py``) and is filtered out here so the
    fallback-chain assertions keep counting only their own records.
    """
    return [
        line
        for line in captured.splitlines()
        if level in line and "checkpoint server address" not in line
    ]


# ---------------------------------------------------------------------------
# What is restored (Requirements 6.1, 6.2, 6.3, 6.11, 6.13)
# ---------------------------------------------------------------------------


def test_restore_brings_back_every_flow_into_the_existing_bunch(tmp_path):
    _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)
    bunch_before = manager.bunch

    assert manager.restore() is True

    # The bunch object itself is never replaced: Scheduler and TaklerService
    # already hold this reference.
    assert manager.bunch is bunch_before
    assert sorted(manager.bunch.flows) == ["flow1", "flow2"]
    assert manager.bunch.find_flow("flow1").bunch is manager.bunch


def test_restore_round_trips_the_status_serialization(tmp_path):
    """Requirement 6.2 / 6.11 in one assertion: nothing written is dropped."""
    source = _source_bunch()
    writer = CheckpointManager(bunch=source, checkpoint_file=tmp_path / "takler.check")
    writer.write_checkpoint()
    manager = _target_manager(tmp_path)

    manager.restore()

    expected = [flow.to_dict() for _, flow in source.flows.items()]
    restored = [flow.to_dict() for _, flow in manager.bunch.flows.items()]
    assert restored == json.loads(json.dumps(expected))


def test_restore_keeps_node_status_and_suspended(tmp_path):
    _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)

    manager.restore()

    task1 = manager.bunch.find_node("/flow1/container1/task1")
    task2 = manager.bunch.find_node("/flow1/container1/task2")
    task3 = manager.bunch.find_node("/flow1/container1/task3")
    assert task1.state.node_status == NodeStatus.active
    assert task2.state.node_status == NodeStatus.submitted
    assert task2.state.suspended is True
    assert task3.state.node_status == NodeStatus.aborted


def test_restore_keeps_event_meter_limit_and_repeat_values(tmp_path):
    _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)

    manager.restore()

    task1 = manager.bunch.find_node("/flow1/container1/task1")
    assert task1.find_event("done").value is True
    assert task1.find_meter("progress").value == 42
    limit = manager.bunch.find_flow("flow1").find_limit("disk")
    assert limit.value == 2
    assert limit.node_paths == {"/flow1/container1/task2"}


def test_restore_keeps_task_id_try_no_and_aborted_reason(tmp_path):
    _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)

    manager.restore()

    task1 = manager.bunch.find_node("/flow1/container1/task1")
    task2 = manager.bunch.find_node("/flow1/container1/task2")
    task3 = manager.bunch.find_node("/flow1/container1/task3")
    assert task1.task_id == "12345"
    assert task2.try_no == 1
    assert task3.aborted_reason == "job died"


def test_restore_keeps_shell_script_task_script_path(tmp_path):
    _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)

    manager.restore()

    task3 = manager.bunch.find_node("/flow1/container1/task3")
    assert isinstance(task3, ShellScriptTask)
    assert task3.script_path == "/opt/flows/task3.takler"


def test_restore_keeps_begun_and_the_calendar(tmp_path):
    source = _source_bunch()
    CheckpointManager(
        bunch=source, checkpoint_file=tmp_path / "takler.check"
    ).write_checkpoint()
    manager = _target_manager(tmp_path)

    manager.restore()

    flow1 = manager.bunch.find_flow("flow1")
    assert flow1.begun is True
    assert (
        flow1.calendar.initial_time == source.find_flow("flow1").calendar.initial_time
    )
    # An un-begun flow stays un-begun, calendar included.
    flow2 = manager.bunch.find_flow("flow2")
    assert flow2.begun is False
    assert flow2.calendar.initial_time is None


def test_restore_does_not_requeue(tmp_path):
    """Requirement 6.4: in-flight tasks must not be reset to queued."""
    _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)

    manager.restore()

    task2 = manager.bunch.find_node("/flow1/container1/task2")
    assert task2.state.node_status == NodeStatus.submitted
    assert task2.try_no == 1
    assert task2.check_dependencies() is False


# ---------------------------------------------------------------------------
# The snapshot's server_state is discarded (Requirements 6.5, 6.22)
# ---------------------------------------------------------------------------


def test_restore_keeps_the_current_process_host_and_port(tmp_path):
    _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)

    manager.restore()

    assert manager.bunch.server_state.host == "login02"
    assert manager.bunch.server_state.port == "44084"
    flow1 = manager.bunch.find_flow("flow1")
    assert flow1.find_parent_parameter(TAKLER_HOST).value == "login02"
    assert flow1.find_parent_parameter(TAKLER_PORT).value == "44084"


# ---------------------------------------------------------------------------
# Reporting (Requirement 6.10)
# ---------------------------------------------------------------------------


def test_restore_logs_the_flow_and_node_counts(tmp_path):
    _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)

    result, captured = _capturing_stderr(manager.restore)

    assert result is True
    # 2 flows; 1 + 1 + 3 nodes in flow1 and 1 + 1 in flow2.
    info = [line for line in _lines(captured, "INFO") if "restored" in line]
    assert len(info) == 1
    assert "2 flow(s)" in info[0]
    assert "7 node(s)" in info[0]
    assert str(manager.checkpoint_file) in info[0]


# ---------------------------------------------------------------------------
# Fallback chain (Requirements 6.7, 6.8, 6.9)
# ---------------------------------------------------------------------------


def test_missing_checkpoint_file_starts_with_an_empty_bunch(tmp_path):
    manager = _target_manager(tmp_path)

    result, captured = _capturing_stderr(manager.restore)

    assert result is False
    assert manager.bunch.flows == {}
    assert "ERROR" not in captured
    assert _lines(captured, "INFO")
    assert str(manager.checkpoint_file) in captured


def test_unparsable_checkpoint_file_falls_back_to_the_backup(tmp_path):
    _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)
    # Rotate the good snapshot into the backup, then corrupt the main file.
    manager.checkpoint_file.replace(manager.backup_file)
    manager.checkpoint_file.write_text("{not json", encoding="utf-8")

    result, captured = _capturing_stderr(manager.restore)

    assert result is True
    assert sorted(manager.bunch.flows) == ["flow1", "flow2"]
    errors = _lines(captured, "ERROR")
    assert len(errors) == 1
    assert str(manager.checkpoint_file) in errors[0]
    assert str(manager.backup_file) in captured


def test_both_files_unparsable_starts_with_an_empty_bunch(tmp_path):
    manager = _target_manager(tmp_path)
    manager.checkpoint_file.write_text("{not json", encoding="utf-8")
    manager.backup_file.write_text("also not json", encoding="utf-8")

    result, captured = _capturing_stderr(manager.restore)

    assert result is False
    assert manager.bunch.flows == {}
    # One ERROR per unusable file plus the summary naming both paths.
    errors = _lines(captured, "ERROR")
    assert len(errors) == 3
    summary = [line for line in errors if "empty bunch" in line]
    assert len(summary) == 1
    assert str(manager.checkpoint_file) in summary[0]
    assert str(manager.backup_file) in summary[0]


def test_a_snapshot_without_a_bunch_key_is_unusable(tmp_path):
    manager = _target_manager(tmp_path)
    manager.checkpoint_file.write_text(
        json.dumps({"format_version": CHECKPOINT_FORMAT_VERSION}),
        encoding="utf-8",
    )

    result, captured = _capturing_stderr(manager.restore)

    assert result is False
    assert manager.bunch.flows == {}
    assert str(manager.checkpoint_file) in "".join(_lines(captured, "ERROR"))


# ---------------------------------------------------------------------------
# Format version (Requirements 6.14, 6.15)
# ---------------------------------------------------------------------------


def test_a_snapshot_without_a_format_version_is_still_restored(tmp_path):
    path = _write_source_snapshot(tmp_path)
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    del snapshot["format_version"]
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    manager = _target_manager(tmp_path)

    result, captured = _capturing_stderr(manager.restore)

    assert result is True
    assert sorted(manager.bunch.flows) == ["flow1", "flow2"]
    assert _lines(captured, "ERROR") == []


def test_a_newer_format_version_is_refused_and_falls_back(tmp_path):
    path = _write_source_snapshot(tmp_path)
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    snapshot["format_version"] = CHECKPOINT_FORMAT_VERSION + 3
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    manager = _target_manager(tmp_path)

    result, captured = _capturing_stderr(manager.restore)

    assert result is False
    assert manager.bunch.flows == {}
    version_errors = [
        line for line in _lines(captured, "ERROR") if "format version" in line
    ]
    assert len(version_errors) == 1
    assert str(manager.checkpoint_file) in version_errors[0]
    assert str(CHECKPOINT_FORMAT_VERSION + 3) in version_errors[0]
    assert str(CHECKPOINT_FORMAT_VERSION) in version_errors[0]


# ---------------------------------------------------------------------------
# One broken flow does not lose the others
# ---------------------------------------------------------------------------


def test_a_broken_flow_is_skipped_and_the_rest_is_restored(tmp_path):
    path = _write_source_snapshot(tmp_path)
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    for flow_dict in snapshot["bunch"]["flows"]:
        if flow_dict["name"] == "flow1":
            flow_dict["class_type"]["name"] = "NoSuchFlowClass"
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    manager = _target_manager(tmp_path)

    result, captured = _capturing_stderr(manager.restore)

    assert result is True
    assert list(manager.bunch.flows) == ["flow2"]
    errors = _lines(captured, "ERROR")
    assert len(errors) == 1
    assert "flow1" in errors[0]
    info = [line for line in _lines(captured, "INFO") if "restored" in line]
    assert "1 flow(s)" in info[0]


def test_restore_never_raises_on_a_directory_in_place_of_the_snapshot(tmp_path):
    manager = _target_manager(tmp_path)
    manager.checkpoint_file.mkdir()

    result, captured = _capturing_stderr(manager.restore)

    assert result is False
    assert _lines(captured, "ERROR")


# ---------------------------------------------------------------------------
# _load_snapshot / _restore_into_bunch directly
# ---------------------------------------------------------------------------


def test_load_snapshot_returns_the_parsed_dictionary(tmp_path):
    path = _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)

    snapshot = manager._load_snapshot(path)

    assert snapshot["format_version"] == CHECKPOINT_FORMAT_VERSION
    assert [f["name"] for f in snapshot["bunch"]["flows"]] == ["flow1", "flow2"]


def test_restore_into_bunch_reports_the_counts(tmp_path):
    path = _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)

    flow_count, node_count = manager._restore_into_bunch(manager._load_snapshot(path))

    assert (flow_count, node_count) == (2, 7)


def test_restore_into_bunch_uses_the_status_serialization(tmp_path, monkeypatch):
    """A Tree restore would drop the status; Status must be used (6.11)."""
    path = _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)
    snapshot = manager._load_snapshot(path)
    used = []
    original = Flow.from_dict

    def recording_from_dict(d, method=SerializationType.Status):
        used.append(method)
        return original(d, method=method)

    monkeypatch.setattr(Flow, "from_dict", recording_from_dict)
    manager._restore_into_bunch(snapshot)

    # ``Node.fill_from_dict`` recurses through ``cls.from_dict``, so the
    # recording function sees the flows and their direct children alike; every
    # one of them must be asked for the Status serialization.
    assert len(used) >= 2
    assert set(used) == {SerializationType.Status}
