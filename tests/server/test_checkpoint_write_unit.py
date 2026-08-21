"""Unit tests for the Checkpoint_Manager snapshot write path.

Task 8.3 of the *m1-operational-baseline* spec adds ``build_payload``,
``write_checkpoint`` / ``write_checkpoint_async`` and the atomic
``_write_payload`` step sequence. This file pins the payload layout, the
temporary-file dance, the backup rotation, the parent directory creation and the
failure contract. The exhaustive "for all failure points" assertions on
atomicity live in the property test of task 9.1.

Log assertions go through a captured console sink rather than ``caplog``: the
logging backend does not route records into pytest's handler, so the sink has to
be configured inside the redirection block (same approach as
``test_checkpoint_config_unit.py``).

Validates: Requirements 5.1, 5.2, 5.3, 5.8, 5.11, 7.7, 12.5
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import stat
import threading
from pathlib import Path

import pytest

import takler.logging
from takler.core.bunch import Bunch
from takler.core.flow import Flow
from takler.server.checkpoint import (
    CHECKPOINT_FILE_MODE,
    CHECKPOINT_FORMAT_VERSION,
    TEMP_SUFFIX,
    CheckpointManager,
    _takler_version,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_bunch() -> Bunch:
    """A bunch holding one flow with one task, enough to be recognizable."""
    bunch = Bunch(host="login01", port="33083")
    flow = Flow(name="flow1")
    flow.add_task("task1")
    bunch.add_flow(flow)
    return bunch


def _make_manager(tmp_path: Path, name: str = "takler.check") -> CheckpointManager:
    return CheckpointManager(bunch=_make_bunch(), checkpoint_file=tmp_path / name)


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


def _temp_names(directory: Path) -> list:
    return sorted(p.name for p in directory.iterdir() if TEMP_SUFFIX in p.name)


# ---------------------------------------------------------------------------
# Payload layout (Requirements 5.11)
# ---------------------------------------------------------------------------


def test_build_payload_returns_a_json_string_with_the_expected_top_level():
    manager = CheckpointManager(bunch=_make_bunch())

    snapshot = json.loads(manager.build_payload())

    assert set(snapshot) == {
        "format_version",
        "takler_version",
        "written_at",
        "bunch",
        # Sibling of ``bunch``, added in M2 without a version bump
        # (requirements 5.1, 5.7).
        "job_passwords",
    }
    assert snapshot["format_version"] == CHECKPOINT_FORMAT_VERSION
    assert snapshot["takler_version"] == _takler_version()
    # ``written_at`` is diagnostic only, so only its parsability is pinned.
    assert isinstance(snapshot["written_at"], str)


def test_build_payload_bunch_subtree_equals_bunch_to_dict():
    """No second snapshot format: the ``bunch`` key is ``Bunch.to_dict()``."""
    bunch = _make_bunch()
    manager = CheckpointManager(bunch=bunch)

    snapshot = json.loads(manager.build_payload())

    assert snapshot["bunch"] == json.loads(json.dumps(bunch.to_dict()))


def test_build_payload_reflects_later_bunch_changes():
    """The payload is built per call, not cached at construction."""
    manager = CheckpointManager(bunch=_make_bunch())

    before = json.loads(manager.build_payload())["bunch"]
    manager.bunch.add_flow(Flow(name="flow2"))
    after = json.loads(manager.build_payload())["bunch"]

    assert len(before["flows"]) == 1
    assert len(after["flows"]) == 2


# ---------------------------------------------------------------------------
# Writing (Requirements 5.1, 5.2)
# ---------------------------------------------------------------------------


def test_write_checkpoint_creates_a_complete_snapshot(tmp_path):
    manager = _make_manager(tmp_path)

    assert manager.write_checkpoint() is True

    snapshot = json.loads(manager.checkpoint_file.read_text(encoding="utf-8"))
    assert snapshot["format_version"] == CHECKPOINT_FORMAT_VERSION
    assert [f["name"] for f in snapshot["bunch"]["flows"]] == ["flow1"]


def test_write_checkpoint_leaves_no_temporary_files(tmp_path):
    manager = _make_manager(tmp_path)

    manager.write_checkpoint()

    assert _temp_names(tmp_path) == []


def test_first_write_creates_no_backup(tmp_path):
    """There is no previous snapshot to preserve on the first write."""
    manager = _make_manager(tmp_path)

    manager.write_checkpoint()

    assert not manager.backup_file.exists()


def test_temp_path_is_a_pid_suffixed_sibling(tmp_path):
    manager = _make_manager(tmp_path)

    main_temp = manager._temp_path(manager.checkpoint_file)
    backup_temp = manager._temp_path(manager.backup_file)

    assert main_temp.name == f"takler.check{TEMP_SUFFIX}.{os.getpid()}"
    assert backup_temp.name == f"takler.check.b{TEMP_SUFFIX}.{os.getpid()}"
    assert main_temp.parent == manager.checkpoint_file.parent


def test_write_steps_are_in_the_documented_order(tmp_path):
    manager = _make_manager(tmp_path)

    names = [
        name
        for name, _ in manager._write_steps(
            "{}", tmp_path / "a.tmp", tmp_path / "b.tmp"
        )
    ]

    assert names == [
        "ensure_parent_directory",
        "write_temp_file",
        "copy_to_backup_temp",
        "replace_backup",
        "replace_checkpoint",
    ]


# ---------------------------------------------------------------------------
# Backup rotation (Requirement 5.3)
# ---------------------------------------------------------------------------


def test_second_write_moves_the_previous_snapshot_to_the_backup(tmp_path):
    manager = _make_manager(tmp_path)
    manager.write_checkpoint()
    first = manager.checkpoint_file.read_text(encoding="utf-8")

    manager.bunch.add_flow(Flow(name="flow2"))
    assert manager.write_checkpoint() is True

    assert manager.backup_file.read_text(encoding="utf-8") == first
    current = json.loads(manager.checkpoint_file.read_text(encoding="utf-8"))
    assert len(current["bunch"]["flows"]) == 2
    assert _temp_names(tmp_path) == []


def test_both_snapshot_files_are_owner_read_write_only(tmp_path):
    """Requirement 12.5: a snapshot carries Job_Passwords, so it stays 0600."""
    manager = _make_manager(tmp_path)
    manager.write_checkpoint()
    # A pre-M2 snapshot with wide permissions must not stay wide once rotated.
    manager.checkpoint_file.chmod(0o644)

    manager.bunch.add_flow(Flow(name="flow2"))
    manager.write_checkpoint()

    assert stat.S_IMODE(manager.checkpoint_file.stat().st_mode) == CHECKPOINT_FILE_MODE
    assert stat.S_IMODE(manager.backup_file.stat().st_mode) == CHECKPOINT_FILE_MODE


def test_temporary_files_are_owner_read_write_only_before_the_rename(tmp_path):
    """The tightening happens at creation, not after the atomic replace."""
    manager = _make_manager(tmp_path)
    manager.write_checkpoint()
    manager.checkpoint_file.chmod(0o644)
    modes = {}

    def record(index, name):
        for temp in (
            manager._temp_path(manager.checkpoint_file),
            manager._temp_path(manager.backup_file),
        ):
            if temp.exists():
                modes.setdefault(temp.name, []).append(
                    stat.S_IMODE(temp.stat().st_mode)
                )

    manager._after_write_step = record
    manager.write_checkpoint()

    assert modes
    assert all(mode == CHECKPOINT_FILE_MODE for seen in modes.values() for mode in seen)


def test_third_write_keeps_only_the_previous_snapshot_as_backup(tmp_path):
    manager = _make_manager(tmp_path)
    manager.write_checkpoint()
    manager.bunch.add_flow(Flow(name="flow2"))
    manager.write_checkpoint()
    second = manager.checkpoint_file.read_text(encoding="utf-8")

    manager.bunch.add_flow(Flow(name="flow3"))
    manager.write_checkpoint()

    assert manager.backup_file.read_text(encoding="utf-8") == second


def test_checkpoint_file_is_never_absent_while_the_backup_is_made(tmp_path):
    """Copy-then-replace: the main snapshot exists at every step boundary."""
    manager = _make_manager(tmp_path)
    manager.write_checkpoint()
    seen = []

    def record(index, name):
        seen.append((name, manager.checkpoint_file.exists()))

    manager._after_write_step = record
    manager.write_checkpoint()

    assert seen and all(exists for _, exists in seen)


# ---------------------------------------------------------------------------
# Parent directory (Requirement 7.7)
# ---------------------------------------------------------------------------


def test_write_creates_a_missing_parent_directory(tmp_path):
    target = tmp_path / "state" / "nested" / "takler.check"
    manager = CheckpointManager(bunch=_make_bunch(), checkpoint_file=target)

    assert manager.write_checkpoint() is True
    assert target.is_file()


def test_write_works_with_a_relative_default_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    manager = CheckpointManager(bunch=_make_bunch())

    assert manager.write_checkpoint() is True
    assert (tmp_path / "takler.check").is_file()


# ---------------------------------------------------------------------------
# Failure handling (Requirement 5.8)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "step",
    [
        "write_temp_file",
        "copy_to_backup_temp",
        "replace_backup",
        "replace_checkpoint",
    ],
)
def test_failure_keeps_existing_files_and_reports_false(tmp_path, step):
    manager = _make_manager(tmp_path)
    manager.write_checkpoint()
    manager.bunch.add_flow(Flow(name="flow2"))
    manager.write_checkpoint()
    main_before = manager.checkpoint_file.read_text(encoding="utf-8")
    backup_before = manager.backup_file.read_text(encoding="utf-8")

    def fail_after(index, name):
        if name == step:
            raise OSError(28, "No space left on device")

    manager._after_write_step = fail_after
    manager.bunch.add_flow(Flow(name="flow3"))
    result, captured = _capturing_stderr(manager.write_checkpoint)

    assert result is False
    errors = [line for line in captured.splitlines() if "ERROR" in line]
    assert len(errors) == 1
    assert str(manager.checkpoint_file) in errors[0]
    assert "No space left on device" in errors[0]
    assert _temp_names(tmp_path) == []
    if step == "replace_checkpoint":
        # The replace itself already happened; only the injected hook failed,
        # so the main file legitimately holds the new snapshot. What matters is
        # that it is a complete snapshot.
        assert json.loads(manager.checkpoint_file.read_text(encoding="utf-8"))
    else:
        assert manager.checkpoint_file.read_text(encoding="utf-8") == main_before
    if step in ("write_temp_file", "copy_to_backup_temp"):
        assert manager.backup_file.read_text(encoding="utf-8") == backup_before


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="root ignores directory permissions",
)
def test_unwritable_parent_directory_is_reported_not_raised(tmp_path):
    target = tmp_path / "state" / "takler.check"
    (tmp_path / "state").mkdir()
    (tmp_path / "state").chmod(0o500)
    manager = CheckpointManager(bunch=_make_bunch(), checkpoint_file=target)
    try:
        result, captured = _capturing_stderr(manager.write_checkpoint)
    finally:
        (tmp_path / "state").chmod(0o700)

    assert result is False
    assert str(target) in captured
    assert not target.exists()


def test_unserializable_bunch_is_reported_not_raised(tmp_path):
    manager = _make_manager(tmp_path)

    def broken():
        raise TypeError("not serializable")

    manager.bunch.to_dict = broken
    result, captured = _capturing_stderr(manager.write_checkpoint)

    assert result is False
    assert "ERROR" in captured
    assert not manager.checkpoint_file.exists()


# ---------------------------------------------------------------------------
# Async variant
# ---------------------------------------------------------------------------


def test_write_checkpoint_async_writes_the_same_snapshot(tmp_path):
    manager = _make_manager(tmp_path)

    assert asyncio.run(manager.write_checkpoint_async()) is True

    snapshot = json.loads(manager.checkpoint_file.read_text(encoding="utf-8"))
    assert snapshot["format_version"] == CHECKPOINT_FORMAT_VERSION
    assert [f["name"] for f in snapshot["bunch"]["flows"]] == ["flow1"]


def test_write_checkpoint_async_builds_the_payload_on_the_loop_thread(tmp_path):
    """The payload must not be built on the worker thread."""
    manager = _make_manager(tmp_path)
    build_threads = []
    write_threads = []
    original_build = manager.build_payload
    original_write = manager._write_payload

    def build_payload():
        build_threads.append(threading.current_thread())
        return original_build()

    def write_payload(payload):
        write_threads.append(threading.current_thread())
        return original_write(payload)

    manager.build_payload = build_payload
    manager._write_payload = write_payload

    async def main():
        loop_thread = threading.current_thread()
        assert await manager.write_checkpoint_async() is True
        return loop_thread

    loop_thread = asyncio.run(main())

    assert build_threads == [loop_thread]
    assert write_threads and write_threads[0] is not loop_thread


def test_write_checkpoint_async_reports_failure_without_raising(tmp_path):
    manager = _make_manager(tmp_path)

    def fail_after(index, name):
        if name == "write_temp_file":
            raise OSError("disk gone")

    manager._after_write_step = fail_after
    result, captured = _capturing_stderr(
        lambda: asyncio.run(manager.write_checkpoint_async())
    )

    assert result is False
    assert "ERROR" in captured
    assert not manager.checkpoint_file.exists()
