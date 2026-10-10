"""Artifact references follow the online tree and current attempts."""

import json
from pathlib import Path

import pytest

from takler.core import Bunch, Flow, Task
from takler.protocol.commands import ReplaceCommand
from takler.serialization import export_definition
from takler.serialization.checkpoint import capture_checkpoint, restore_checkpoint
from takler.server.checkpoint import CheckpointManager
from takler.server.scheduler import Scheduler
from takler.serialization.artifact_path_index import ArtifactPathIndex


def _tree():
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    first = flow.add_task(Task("first"))
    second = flow.add_task(Task("second"))
    return bunch, flow, first, second


def _conflicts(bunch, owner, path):
    index = bunch._artifact_path_index
    if index is None:
        index = bunch._artifact_path_index = ArtifactPathIndex(bunch)
    return index.conflicts(owner, {path.resolve()})


def test_current_and_terminal_references_are_retained(tmp_path):
    bunch, _, first, second = _tree()
    path = tmp_path / "shared.job"
    first.job_path = str(path)
    assert _conflicts(bunch, second, path)
    assert not _conflicts(bunch, first, path)

    # Status alone does not release an artifact reference.
    first.jobout_path = str(tmp_path / "other.out")
    first.state.node_status = first.state.node_status.complete
    assert _conflicts(bunch, second, path)
    first.clear_current_attempt()
    assert not _conflicts(bunch, second, path)


def test_direct_path_changes_and_tree_edits_invalidate_or_update(tmp_path):
    bunch, flow, first, second = _tree()
    old = tmp_path / "old"
    new = tmp_path / "new"
    first.job_path = str(old)
    assert _conflicts(bunch, second, old)
    first.job_path = str(new)
    assert not _conflicts(bunch, second, old)
    assert _conflicts(bunch, second, new)

    replacement = Task("first")
    replacement.jobout_path = str(old)
    flow.update_child(first, replacement)
    assert _conflicts(bunch, second, old)
    assert not _conflicts(bunch, second, new)
    flow.delete_child(replacement)
    assert not _conflicts(bunch, second, old)
    replacement.job_path = str(new)
    assert not _conflicts(bunch, second, new)


def test_path_update_failure_keeps_old_index_entry(tmp_path):
    bunch, _, first, second = _tree()
    old = tmp_path / "old"
    first.job_path = str(old)
    assert _conflicts(bunch, second, old)
    with pytest.raises(ValueError):
        first.job_path = "bad\x00path"
    assert first.job_path == str(old)
    assert _conflicts(bunch, second, old)


def test_flow_replace_and_restored_tree_rebuild(tmp_path):
    bunch, _, first, second = _tree()
    path = tmp_path / "used"
    first.orvix_info_path = str(path)
    assert _conflicts(bunch, second, path)
    bunch.delete_flow("f")
    assert bunch._artifact_path_index is None
    restored = bunch.add_flow(Flow("f"))
    restored_task = restored.add_task(Task("restored"))
    restored_task.job_path = str(path)
    probe = restored.add_task(Task("probe"))
    assert _conflicts(bunch, probe, path)


def test_replace_command_drops_old_flow_references(tmp_path):
    bunch, _, first, second = _tree()
    path = tmp_path / "old"
    first.job_path = str(path)
    assert _conflicts(bunch, second, path)
    new = Flow("f")
    new.add_task(Task("replacement"))
    payload = export_definition(new).model_dump(mode="json")
    result = Scheduler(bunch).run_command_replace(
        ReplaceCommand(target_path="/f", flow_bytes=json.dumps(payload).encode())
    )
    assert result.flag == 0
    assert bunch._artifact_path_index is None
    assert not _conflicts(bunch, bunch.find_node("/f/replacement"), path)
    first.job_path = str(tmp_path / "stale")
    assert not _conflicts(bunch, bunch.find_node("/f/replacement"), tmp_path / "stale")


def test_symlink_target_change_is_rechecked(tmp_path):
    bunch, _, first, second = _tree()
    target_one = tmp_path / "one"
    target_two = tmp_path / "two"
    target_one.mkdir()
    target_two.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target_one, target_is_directory=True)
    first.job_path = str(link / "job")
    assert _conflicts(bunch, second, target_one / "job")
    link.unlink()
    link.symlink_to(target_two, target_is_directory=True)
    assert not _conflicts(bunch, second, target_one / "job")
    assert _conflicts(bunch, second, target_two / "job")


def test_restore_from_dictionary_builds_index_from_retained_paths(tmp_path):
    bunch, _, first, _ = _tree()
    path = tmp_path / "job"
    first.job_path = str(path)
    restored = Bunch.from_dict(bunch.to_dict())
    assert restored._artifact_path_index is None
    owner = restored.find_node("/f/second")
    assert _conflicts(restored, owner, path)


def test_checkpoint_restore_rebuilds_retained_paths(tmp_path):
    bunch, _, first, _ = _tree()
    path = tmp_path / "job"
    first.job_path = str(path)
    document = capture_checkpoint(bunch)
    restored = restore_checkpoint(document)
    assert restored._artifact_path_index is None
    assert _conflicts(restored, restored.find_node("/f/second"), path)

    manager = CheckpointManager(bunch=Bunch(), checkpoint_file=tmp_path / "state")
    manager._restore_into_bunch(document.model_dump())
    assert manager.bunch._artifact_path_index is not None
    assert _conflicts(manager.bunch, manager.bunch.find_node("/f/second"), path)


def test_three_path_kinds_and_normalization(tmp_path):
    bunch, _, first, second = _tree()
    first.job_path = str(tmp_path / "a" / ".." / "job")
    first.jobout_path = str(tmp_path / "out")
    first.orvix_info_path = str(tmp_path / "info")
    for name in ("job", "out", "info"):
        assert _conflicts(bunch, second, Path(tmp_path, name))


def test_duplicate_references_keep_remaining_owner(tmp_path):
    bunch, _, first, second = _tree()
    path = tmp_path / "same"
    first.job_path = str(path)
    first.jobout_path = str(path)
    assert _conflicts(bunch, second, path)
    first.job_path = None
    assert _conflicts(bunch, second, path)
    first.jobout_path = None
    assert not _conflicts(bunch, second, path)
