"""Process-local index of the current artifact references in a bunch.

The scheduler is the single writer. Tree edits invalidate the index; ordinary
attempt changes update just the affected entry. Checkpoints contain only the
task fields, so a restored bunch builds a fresh index on first use.
"""

import os
from pathlib import Path

from takler.core.task_node import Task


ARTIFACT_FIELDS = ("job_path", "jobout_path", "orvix_info_path")


class ArtifactPathIndex:
    def __init__(self, bunch):
        # Most paths have one owner. Allocate a set only for a collision.
        self.by_path: dict[Path, tuple[Task, str] | set[tuple[Task, str]]] = {}
        self.symlink_fields: dict[tuple[Task, str], Path] = {}
        self.entry_count = 0
        pending = [bunch, *bunch.flows.values()]
        while pending:
            node = pending.pop()
            if isinstance(node, Task):
                for field in ARTIFACT_FIELDS:
                    self.update(node, field, getattr(node, field))
            pending.extend(node.children)

    def update(
        self,
        task: Task,
        field: str,
        value: str | None,
        previous: str | None = None,
    ) -> None:
        key = (task, field)
        source = None if value is None else Path(value)
        resolved = None if source is None else source.resolve()
        old_path = self.symlink_fields.pop(key, None)
        if old_path is None and previous is not None:
            old_path = Path(previous).resolve()
        if old_path is not None:
            owners = self.by_path[old_path]
            if isinstance(owners, set):
                owners.remove(key)
                if len(owners) == 1:
                    self.by_path[old_path] = next(iter(owners))
            else:
                assert owners == key
                del self.by_path[old_path]
            self.entry_count -= 1
        if source is None:
            return
        owners = self.by_path.get(resolved)
        if owners is None:
            self.by_path[resolved] = key
        elif isinstance(owners, set):
            owners.add(key)
        else:
            self.by_path[resolved] = {owners, key}
        self.entry_count += 1
        # A changed symlink target is rechecked on the next submission. Normal
        # references stay O(1) per lookup, even for a 100k-task tree.
        if resolved != Path(os.path.abspath(source)):
            self.symlink_fields[key] = resolved

    def conflicts(self, owner: Task, paths: set[Path]) -> bool:
        for (task, field), previous in tuple(self.symlink_fields.items()):
            value = getattr(task, field)
            if Path(value).resolve() != previous:
                self.update(task, field, value, previous=value)
        for path in paths:
            owners = self.by_path.get(path)
            if owners is None:
                continue
            if isinstance(owners, set):
                if any(task is not owner for task, _ in owners):
                    return True
            elif owners[0] is not owner:
                return True
        return False
