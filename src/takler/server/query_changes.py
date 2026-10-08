"""Ephemeral, bounded hints for QueryDocument changes.

The journal holds identities and field groups, never projected values. A reader
older than ``oldest_revision`` must reset; dropping history cannot imply that
the tree was unchanged. All methods run on the scheduler's single writer.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import time


MAX_CHANGE_BATCHES = 1024
MAX_CHANGES_PER_BATCH = 2048
MAX_RETAINED_CHANGES = 8192
MAX_CHANGE_WINDOW_BYTES = 32 * 1024 * 1024
MAX_CHANGE_PATH_BYTES = 4096
MAX_CHANGE_SECONDS = 120
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
    created_at: float
    estimated_bytes: int


class QueryChanges:
    """Commit one revision per visible transaction, including partial failures."""

    def __init__(self) -> None:
        self.revision = 1
        self.oldest_revision = 1
        self._batches: deque[ChangeBatch] = deque()
        self._retained_changes = 0
        self._retained_bytes = 0
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
        changes = tuple(self._pending.values())
        size = sum(len(change.path.encode("utf-8")) + 512 for change in changes)
        batch = ChangeBatch(self.revision, changes, time.monotonic(), size)
        self._batches.append(batch)
        self._retained_changes += len(batch.changes)
        self._retained_bytes += size
        self._pending.clear()
        self._expire()

    def _expire(self) -> None:
        cutoff = time.monotonic() - MAX_CHANGE_SECONDS
        while self._batches and (
            len(self._batches) > MAX_CHANGE_BATCHES
            or self._retained_changes > MAX_RETAINED_CHANGES
            or self._retained_bytes > MAX_CHANGE_WINDOW_BYTES
            or self._batches[0].created_at <= cutoff
        ):
            dropped = self._batches.popleft()
            self._retained_changes -= len(dropped.changes)
            self._retained_bytes -= dropped.estimated_bytes
            self.oldest_revision = dropped.revision

    def since(self, revision: int) -> tuple[ChangeBatch, ...] | None:
        """Return consecutive hints, or ``None`` when the waterline expired."""
        self._expire()
        if revision < self.oldest_revision or revision > self.revision:
            return None
        return tuple(batch for batch in self._batches if batch.revision > revision)

    @property
    def estimated_bytes(self) -> int:
        self._expire()
        return self._retained_bytes
