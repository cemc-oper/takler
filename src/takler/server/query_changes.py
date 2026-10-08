"""Ephemeral, bounded hints for QueryDocument changes.

The journal holds identities and field groups, never projected values. A reader
older than ``oldest_revision`` must reset; dropping history cannot imply that
the tree was unchanged. All methods run on the scheduler's single writer.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


MAX_CHANGE_BATCHES = 1024
MAX_CHANGES_PER_BATCH = 2048
MAX_RETAINED_CHANGES = 8192
MAX_CHANGE_PATH_BYTES = 4096
FIELD_GROUPS = frozenset(
    {"summary", "parameters", "definition", "runtime", "artifacts", "service"}
)


@dataclass(frozen=True)
class QueryChange:
    path: str
    groups: frozenset[str]
    invalidate: bool = False


@dataclass(frozen=True)
class ChangeBatch:
    revision: int
    changes: tuple[QueryChange, ...]


class QueryChanges:
    """Commit one revision per visible transaction, including partial failures."""

    def __init__(self) -> None:
        self.revision = 1
        self.oldest_revision = 1
        self._batches: deque[ChangeBatch] = deque()
        self._retained_changes = 0
        self._depth = 0
        self._pending: dict[str, QueryChange] = {}

    def begin(self) -> None:
        self._depth += 1

    def end(self) -> None:
        if self._depth == 0:
            raise RuntimeError("query change transaction is not open")
        self._depth -= 1
        if self._depth == 0:
            self._commit()

    def mark(
        self,
        path: str,
        *groups: str,
        invalidate: bool = False,
    ) -> None:
        if not path.startswith("/") or (path != "/" and path.endswith("/")):
            raise ValueError("query change path must be absolute and canonical")
        if not groups and not invalidate:
            raise ValueError("query change needs a field group or invalidation")
        if set(groups) - FIELD_GROUPS:
            raise ValueError("unknown query change field group")
        if len(path.encode("utf-8")) > MAX_CHANGE_PATH_BYTES:
            path, groups, invalidate = "/", (), True
        if len(self._pending) >= MAX_CHANGES_PER_BATCH and path not in self._pending:
            self._pending = {"/": QueryChange("/", frozenset(), True)}
        elif "/" in self._pending and self._pending["/"].invalidate:
            pass
        else:
            old = self._pending.get(path)
            self._pending[path] = QueryChange(
                path,
                frozenset(groups) | (old.groups if old else frozenset()),
                invalidate or (old.invalidate if old else False),
            )
        if self._depth == 0:
            self._commit()

    def _commit(self) -> None:
        if not self._pending:
            return
        self.revision += 1
        batch = ChangeBatch(self.revision, tuple(self._pending.values()))
        self._batches.append(batch)
        self._retained_changes += len(batch.changes)
        self._pending.clear()
        while (
            len(self._batches) > MAX_CHANGE_BATCHES
            or self._retained_changes > MAX_RETAINED_CHANGES
        ):
            dropped = self._batches.popleft()
            self._retained_changes -= len(dropped.changes)
            self.oldest_revision = dropped.revision

    def since(self, revision: int) -> tuple[ChangeBatch, ...] | None:
        """Return consecutive hints, or ``None`` when the waterline expired."""
        if revision < self.oldest_revision or revision > self.revision:
            return None
        return tuple(batch for batch in self._batches if batch.revision > revision)
