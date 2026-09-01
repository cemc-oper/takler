"""Unit tests for the Job_Password scope of the Checkpoint_Manager.

Tasks 5.1 - 5.3 of the *m2-security* spec add ``_collect_job_passwords``,
``_restore_job_passwords`` and the owner-only creation of both snapshot files.
This file pins which tasks a snapshot carries a password for, that a pre-M2
snapshot (no ``job_passwords`` key at all) still restores, that a stale or
non-Task entry is skipped with a WARNING while the remaining entries are still
written back, that both snapshot files are owner read/write only and that adding
the new top level key did not bump the format version.

Snapshots are produced by the manager's own write path rather than by
hand-written JSON wherever possible, so a change to the payload layout cannot
make these tests pass against a format nobody writes. The two fault-tolerance
tests edit one key of a real snapshot instead, which is exactly what a stale
checkpoint file looks like.

No test prints a job password or puts one in a test name or an assertion
message: passwords are only ever read inside an assertion expression.

Log assertions go through a captured console sink rather than ``caplog``: the
logging backend does not route records into pytest's handler, so the sink has to
be configured inside the redirection block (same approach as
``test_checkpoint_restore_unit.py``).

Validates: Requirements 5.1, 5.2, 5.3, 5.6, 5.7, 5.8, 5.10, 12.5, 16.25
"""

from __future__ import annotations

import contextlib
import io
import json
import stat
from pathlib import Path

import takler.logging
from takler.core.bunch import Bunch
from takler.core.flow import Flow
from takler.core.state import NodeStatus
from takler.server.checkpoint import (
    CHECKPOINT_FILE_MODE,
    CHECKPOINT_FORMAT_VERSION,
    JOB_PASSWORDS_KEY,
    CheckpointManager,
)


# Node paths of the source bunch, one task per status the requirements name.
SUBMITTED_PATH = "/flow1/container1/submitted_task"
ACTIVE_PATH = "/flow1/container1/active_task"
QUEUED_PATH = "/flow1/container1/queued_task"
COMPLETE_PATH = "/flow1/container1/complete_task"
ABORTED_PATH = "/flow1/container1/aborted_task"
UNKNOWN_PATH = "/flow1/container1/unknown_task"
CONTAINER_PATH = "/flow1/container1"

#: Every task of the source bunch holds a non-empty password, so these are the
#: only paths that requirement 5.2 keeps out of the mapping by *status*.
NOT_PERSISTED_PATHS = (QUEUED_PATH, COMPLETE_PATH, ABORTED_PATH, UNKNOWN_PATH)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _source_bunch() -> Bunch:
    """A bunch with one task per status, each holding a non-empty password.

    ``increment_try_no`` is what generates the password, so every task is run
    (or incremented) *first* and only then moved to its final status: that is
    what makes the exclusions of requirement 5.2 a statement about status
    rather than about whether a password exists at all.
    """
    bunch = Bunch(host="login01", port="33083")

    flow1 = Flow("flow1")
    with flow1.add_container("container1") as container1:
        container1.add_task("submitted_task")
        container1.add_task("active_task")
        container1.add_task("queued_task")
        container1.add_task("complete_task")
        container1.add_task("aborted_task")
        container1.add_task("unknown_task")
    bunch.add_flow(flow1)

    # ``begin`` requeues the tree, which clears every password, so it has to
    # happen before the passwords are generated.
    flow1.begin()

    flow1.find_node(SUBMITTED_PATH).run()

    active_task = flow1.find_node(ACTIVE_PATH)
    active_task.run()
    active_task.init(task_id="12345")

    # Queued with a password: the try_no was incremented but the job was never
    # submitted, e.g. because ``do_run`` failed.
    flow1.find_node(QUEUED_PATH).increment_try_no()

    complete_task = flow1.find_node(COMPLETE_PATH)
    complete_task.increment_try_no()
    complete_task.complete()

    aborted_task = flow1.find_node(ABORTED_PATH)
    aborted_task.increment_try_no()
    aborted_task.abort("job died")

    unknown_task = flow1.find_node(UNKNOWN_PATH)
    unknown_task.increment_try_no()
    unknown_task.set_node_status_only(NodeStatus.unknown)

    return bunch


def _writer(bunch: Bunch, tmp_path: Path) -> CheckpointManager:
    return CheckpointManager(bunch=bunch, checkpoint_file=tmp_path / "takler.check")


def _target_manager(tmp_path: Path) -> CheckpointManager:
    """A manager over an empty bunch, i.e. a freshly started server process."""
    return CheckpointManager(
        bunch=Bunch(host="login01", port="33083"),
        checkpoint_file=tmp_path / "takler.check",
    )


def _write_source_snapshot(tmp_path: Path) -> Bunch:
    """Write one real snapshot of :func:`_source_bunch`, return the source."""
    bunch = _source_bunch()
    assert _writer(bunch, tmp_path).write_checkpoint() is True
    return bunch


def _snapshot(tmp_path: Path) -> dict:
    return json.loads((tmp_path / "takler.check").read_text(encoding="utf-8"))


def _rewrite_snapshot(tmp_path: Path, snapshot: dict) -> None:
    (tmp_path / "takler.check").write_text(json.dumps(snapshot), encoding="utf-8")


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

    ``restore`` ends with ``_verify_server_address``, whose record is graded by
    the restored addresses and the in-flight tasks and belongs to
    ``test_checkpoint_address_unit.py``; filtering it out keeps the assertions
    here counting only their own records.
    """
    return [
        line
        for line in captured.splitlines()
        if level in line and "checkpoint server address" not in line
    ]


def _password_of(bunch: Bunch, path: str):
    return bunch.find_node(path).job_password


# ---------------------------------------------------------------------------
# What the snapshot carries (Requirements 5.1, 5.2, 5.3, 5.7, 16.25)
# ---------------------------------------------------------------------------


def test_snapshot_holds_the_passwords_of_submitted_and_active_tasks_only():
    """Requirements 5.1, 5.2, 5.3, 16.25."""
    bunch = _source_bunch()
    manager = CheckpointManager(bunch=bunch)

    mapping = json.loads(manager.build_payload())[JOB_PASSWORDS_KEY]

    assert set(mapping) == {SUBMITTED_PATH, ACTIVE_PATH}
    assert mapping[SUBMITTED_PATH] == _password_of(bunch, SUBMITTED_PATH)
    assert mapping[ACTIVE_PATH] == _password_of(bunch, ACTIVE_PATH)


def test_snapshot_text_holds_no_password_of_a_not_persisted_task():
    """Requirement 5.2 as a substring check over the whole file text."""
    bunch = _source_bunch()
    payload = CheckpointManager(bunch=bunch).build_payload()

    for path in NOT_PERSISTED_PATHS:
        # Every one of these tasks does hold a password, so this fails unless
        # the status filter is what keeps it out of the snapshot.
        assert _password_of(bunch, path)
        assert _password_of(bunch, path) not in payload


def test_snapshot_keeps_format_version_one_with_the_new_top_level_key():
    """Requirement 5.7: a new sibling of ``bunch`` is not a format change."""
    snapshot = json.loads(CheckpointManager(bunch=_source_bunch()).build_payload())

    assert snapshot["format_version"] == CHECKPOINT_FORMAT_VERSION == 1
    # A sibling of ``bunch``, never inside the node tree: ``show`` and the
    # snapshot share one ``Bunch.to_dict()``.
    assert JOB_PASSWORDS_KEY in snapshot
    assert JOB_PASSWORDS_KEY not in json.dumps(snapshot["bunch"])


# ---------------------------------------------------------------------------
# Restoring (Requirements 5.4, 5.6, 5.8, 5.10)
# ---------------------------------------------------------------------------


def test_restore_writes_the_passwords_back_onto_the_in_flight_tasks(tmp_path):
    """Requirements 5.4, 5.10."""
    source = _write_source_snapshot(tmp_path)
    manager = _target_manager(tmp_path)

    _, captured = _capturing_stderr(manager.restore)

    restored = manager.bunch
    assert _password_of(restored, SUBMITTED_PATH) == _password_of(
        source, SUBMITTED_PATH
    )
    assert _password_of(restored, ACTIVE_PATH) == _password_of(source, ACTIVE_PATH)
    # Nothing was invented for the statuses the snapshot skipped.
    for path in NOT_PERSISTED_PATHS:
        assert _password_of(restored, path) is None
    assert [line for line in _lines(captured, "INFO") if "job password of 2" in line]


def test_restore_of_a_pre_m2_snapshot_without_the_mapping_key(tmp_path):
    """Requirement 5.6: a snapshot with no password mapping still restores."""
    _write_source_snapshot(tmp_path)
    snapshot = _snapshot(tmp_path)
    del snapshot[JOB_PASSWORDS_KEY]
    _rewrite_snapshot(tmp_path, snapshot)
    manager = _target_manager(tmp_path)

    result, captured = _capturing_stderr(manager.restore)

    assert result is True
    assert sorted(manager.bunch.flows) == ["flow1"]
    assert manager.bunch.find_node(ACTIVE_PATH).state.node_status is NodeStatus.active
    assert manager.bunch.find_node(ACTIVE_PATH).job_password is None
    # An absent mapping is an empty mapping, not a fault: no WARNING, no ERROR.
    assert _lines(captured, "WARNING") == []
    assert _lines(captured, "ERROR") == []


def test_restore_skips_a_stale_path_and_a_non_task_path(tmp_path):
    """Requirement 5.8: two bad entries, two WARNINGs, the rest restored."""
    source = _write_source_snapshot(tmp_path)
    snapshot = _snapshot(tmp_path)
    stale_path = "/flow1/container1/removed_task"
    # Both bad entries carry a value, so a naive implementation would happily
    # write them somewhere; neither value may reach any node.
    snapshot[JOB_PASSWORDS_KEY][stale_path] = "stale-value"
    snapshot[JOB_PASSWORDS_KEY][CONTAINER_PATH] = "container-value"
    _rewrite_snapshot(tmp_path, snapshot)
    manager = _target_manager(tmp_path)

    result, captured = _capturing_stderr(manager.restore)

    assert result is True
    warnings = _lines(captured, "WARNING")
    assert len(warnings) == 2
    assert any(stale_path in line and "does not exist" in line for line in warnings)
    assert any(CONTAINER_PATH in line and "not a task" in line for line in warnings)

    # The two good entries were still written back.
    restored = manager.bunch
    assert _password_of(restored, SUBMITTED_PATH) == _password_of(
        source, SUBMITTED_PATH
    )
    assert _password_of(restored, ACTIVE_PATH) == _password_of(source, ACTIVE_PATH)
    assert [line for line in _lines(captured, "INFO") if "job password of 2" in line]


# ---------------------------------------------------------------------------
# File permissions (Requirement 12.5)
# ---------------------------------------------------------------------------


def test_both_snapshot_files_holding_passwords_are_owner_read_write_only(tmp_path):
    """Requirement 12.5: a snapshot carries Job_Passwords, so it stays 0600."""
    manager = _writer(_source_bunch(), tmp_path)

    # The second write rotates the first snapshot into the backup file.
    assert manager.write_checkpoint() is True
    assert manager.write_checkpoint() is True

    for path in (manager.checkpoint_file, manager.backup_file):
        assert path.exists()
        assert stat.S_IMODE(path.stat().st_mode) == CHECKPOINT_FILE_MODE == 0o600
