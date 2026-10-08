"""Bounded, immutable QueryDocument snapshots and incremental batches."""

from __future__ import annotations

import asyncio
import base64
import binascii
import copy
from bisect import bisect_right
import hashlib
import hmac
import json
import re
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from takler.core import Task
from takler.protocol.query_v1 import (
    MAX_PAGE_BYTES,
    MAX_PAGE_NODES,
    CapabilitiesReply,
    ChunkReply,
    ChunkRequest,
    DetailBlobRef,
    DetailGroups,
    DetailReply,
    DetailRequest,
    FailureReply,
    InvalidateScope,
    PageReply,
    PageRequest,
    QueryModel,
    ResetReply,
    SinceReply,
    SinceRequest,
    UpsertSummary,
    encode_query_document,
    decode_query_document,
)
from takler.protocol.wire import WireError
from takler.serialization.query_projection import (
    find_query_node,
    iter_query_summaries,
    project_query_detail,
)
from takler.server.auth import get_call_credentials


SNAPSHOT_SECONDS = 120
MAX_SNAPSHOTS = 8
MAX_OWNER_SNAPSHOTS = 2
MAX_QUERY_BYTES = 128 * 1024 * 1024
# Reserve 32 MiB of the total for page models, encoding buffers, and indexes.
MAX_RETAINED_BYTES = 96 * 1024 * 1024
MAX_BLOB_BYTES = 32 * 1024 * 1024
MAX_FIELD_BYTES = 16 * 1024 * 1024
CHUNK_BYTES = 65_536
# Leave room for page metadata and cursor. Every serialized document is checked.
PAGE_TARGET_BYTES = 230_000
MAX_DELTA_BATCHES = 8
MAX_DELTA_BATCH_BYTES = 8 * 1024 * 1024
MAX_DELTA_RETAINED_BYTES = 32 * 1024 * 1024


def _utc(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _error(code: str, message: str) -> FailureReply:
    return FailureReply(kind="error", schema_version=1, code=code, message=message)


@dataclass(frozen=True)
class _Snapshot:
    owner: str
    query_id: str
    scope: str
    depth: int | None
    base_revision: int
    pages: tuple[bytes, ...]
    expires: float
    size: int


@dataclass(frozen=True)
class _Blob:
    owner: str
    query_id: str
    path: str
    generation: str | None
    attempt: str | None
    pointer: str
    data: bytes
    digest: str
    expires: float


@dataclass(frozen=True)
class _DeltaBatch:
    owner: str
    query_id: str
    from_revision: int
    pages: tuple[bytes, ...]
    expires: float
    size: int


class QueryService:
    """Capture off the event loop, then serve byte-identical replay pages."""

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.session_id = secrets.token_urlsafe(24)
        self._key = secrets.token_bytes(32)
        self._snapshots: dict[str, _Snapshot] = {}
        self._blobs: dict[str, _Blob] = {}
        self._delta_batches: dict[str, _DeltaBatch] = {}
        self._lock = asyncio.Lock()
        self._reserved = 0

    def _owner(self) -> str:
        credentials = get_call_credentials()
        value = json.dumps(
            [
                credentials.user,
                credentials.secret,
                self.scheduler.query_redacted_parameters,
            ],
            separators=(",", ":"),
        ).encode()
        return hmac.new(self._key, value, hashlib.sha256).hexdigest()

    def _query_id(self, request: PageRequest, owner: str) -> str:
        selection = [
            owner,
            self._scope(request),
            request.depth,
            request.field_groups,
        ]
        return hmac.new(
            self._key,
            json.dumps(selection, separators=(",", ":")).encode(),
            hashlib.sha256,
        ).hexdigest()

    def _cursor(self, snapshot_id: str, index: int, owner: str) -> str:
        payload = f"{snapshot_id}.{index}".encode()
        mac = hmac.new(self._key, payload + owner.encode(), hashlib.sha256).digest()
        return base64.urlsafe_b64encode(payload + b"." + mac).decode().rstrip("=")

    def _parse_cursor(self, cursor: str, owner: str) -> tuple[str, int] | None:
        try:
            if not re.fullmatch(r"[A-Za-z0-9_-]+", cursor):
                return None
            raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
            if base64.urlsafe_b64encode(raw).decode().rstrip("=") != cursor:
                return None
            if len(raw) < 34 or raw[-33:-32] != b".":
                return None
            payload, mac = raw[:-33], raw[-32:]
            expected = hmac.new(
                self._key, payload + owner.encode(), hashlib.sha256
            ).digest()
            if not hmac.compare_digest(mac, expected):
                return None
            snapshot_id, index = payload.decode("ascii").split(".")
            if not snapshot_id or not index.isdecimal():
                return None
            return snapshot_id, int(index)
        except (ValueError, UnicodeError, binascii.Error):
            return None

    def _purge(self) -> None:
        now = time.monotonic()
        for key, snapshot in list(self._snapshots.items()):
            if snapshot.expires <= now:
                del self._snapshots[key]
        for key, blob in list(self._blobs.items()):
            if blob.expires <= now:
                del self._blobs[key]
        for key, batch in list(self._delta_batches.items()):
            if batch.expires <= now:
                del self._delta_batches[key]

    def _retained_size(self) -> int:
        return (
            self.scheduler.query_changes.estimated_bytes
            + sum(snapshot.size for snapshot in self._snapshots.values())
            + sum(len(blob.data) for blob in self._blobs.values())
            + sum(batch.size for batch in self._delta_batches.values())
        )

    def _replaceable_snapshot(self, owner: str) -> str | None:
        """The oldest owner view can give way to a complete newer capture."""
        owned = [key for key, value in self._snapshots.items() if value.owner == owner]
        return owned[0] if len(owned) >= MAX_OWNER_SNAPSHOTS else None

    def capabilities(self) -> CapabilitiesReply:
        return CapabilitiesReply(
            kind="capabilities",
            schema_version=1,
            snapshot=True,
            detail=True,
            since=True,
            max_page_nodes=MAX_PAGE_NODES,
            max_decoded_page_bytes=MAX_PAGE_BYTES,
            max_chunk_bytes=65536,
        )

    def _capture(
        self,
        request: PageRequest,
        owner: str,
        query_id: str,
        snapshot_id: str,
        base_revision: int,
        budget: int,
    ):
        scope = request.scope_path or (
            "/" + request.flow_name if request.flow_name else "/"
        )
        observed = datetime.now(timezone.utc)
        expires_at = _utc(observed + timedelta(seconds=SNAPSHOT_SECONDS))
        as_of = _utc(observed)
        pages = []
        nodes = []
        generations = {}
        estimated = 1200
        total_size = 0
        page_index = 0
        root_name = self.scheduler.bunch.name

        def flush(complete: bool):
            nonlocal nodes, generations, estimated, total_size, page_index
            reply = PageReply(
                kind="page",
                schema_version=1,
                session_id=self.session_id,
                query_id=query_id,
                snapshot_id=snapshot_id,
                base_revision=base_revision,
                as_of=as_of,
                expires_at=expires_at,
                page_index=page_index,
                total_nodes=None,
                complete=complete,
                next_cursor=None
                if complete
                else self._cursor(snapshot_id, page_index + 1, owner),
                root_name=root_name,
                flow_generations=generations,
                nodes=nodes,
            )
            raw = encode_query_document(reply)
            total_size += len(raw)
            if total_size > budget:
                raise MemoryError("snapshot budget exceeded")
            pages.append(raw)
            nodes, generations, estimated = [], {}, 1200
            page_index += 1

        for node, generation in iter_query_summaries(
            self.scheduler.bunch, scope_path=scope, depth=request.depth
        ):
            node_size = (
                len(
                    json.dumps(
                        node.model_dump(exclude_unset=True),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode()
                )
                + 2
            )
            flow_path = "/" + node.path.split("/", 2)[1] if node.path != "/" else None
            generation_size = 0
            if flow_path and flow_path not in generations:
                generation_size = (
                    len(flow_path.encode()) + len((generation or "").encode()) + 8
                )
            if nodes and (
                len(nodes) >= MAX_PAGE_NODES
                or estimated + node_size + generation_size > PAGE_TARGET_BYTES
            ):
                flush(False)
            if node_size + generation_size + 1200 > MAX_PAGE_BYTES:
                raise OverflowError("summary node exceeds page budget")
            nodes.append(node)
            estimated += node_size + generation_size
            if flow_path:
                if not generation:
                    raise ValueError("flow has no generation")
                generations[flow_path] = generation
        flush(True)
        return scope, tuple(pages), total_size

    async def page(self, request: PageRequest) -> QueryModel:
        if request.field_groups != ["summary"]:
            return _error(
                "unsupported_capability", "initial pages support summary only"
            )
        owner = self._owner()
        query_id = self._query_id(request, owner)
        async with self._lock:
            self._purge()
            if request.cursor is not None:
                parsed = self._parse_cursor(request.cursor, owner)
                if parsed is None:
                    return _error("invalid_request", "invalid cursor")
                snapshot_id, index = parsed
                snapshot = self._snapshots.get(snapshot_id)
                if snapshot is None:
                    return _error("snapshot_expired", "snapshot unavailable")
                if snapshot.owner != owner or snapshot.query_id != query_id:
                    return _error("permission_denied", "query identity changed")
                if index < 1 or index >= len(snapshot.pages):
                    return _error("invalid_request", "cursor index out of range")
                return decode_query_document(snapshot.pages[index], expected="page")
            replacement = self._replaceable_snapshot(owner)
            if self._reserved or (
                len(self._snapshots) + self._reserved >= MAX_SNAPSHOTS
                and replacement is None
            ):
                return _error("resource_exhausted", "snapshot count limit")
            budget = MAX_RETAINED_BYTES - self._retained_size()
            self._reserved += 1
        snapshot_id = secrets.token_urlsafe(24)
        try:
            captured_at = time.monotonic()
            capture_guard = self.scheduler.query_revision
            base_revision = self.scheduler.query_changes.revision
            if capture_guard % 2:
                return _error("resource_exhausted", "query state is changing")
            try:
                scope, pages, size = await asyncio.to_thread(
                    self._capture,
                    request,
                    owner,
                    query_id,
                    snapshot_id,
                    base_revision,
                    budget,
                )
            except (RuntimeError, KeyError, IndexError):
                return _error(
                    "resource_exhausted", "query state changed during capture"
                )
            if (
                self.scheduler.query_revision != capture_guard
                or self.scheduler.query_changes.revision != base_revision
            ):
                return _error(
                    "resource_exhausted", "query state changed during capture"
                )
        except WireError:
            return _error("field_too_large", "summary exceeds page limit")
        except ValueError:
            return _error("invalid_request", "scope unavailable or invalid")
        except OverflowError:
            return _error("field_too_large", "summary exceeds page limit")
        except MemoryError:
            return _error("resource_exhausted", "snapshot memory limit")
        finally:
            async with self._lock:
                self._reserved -= 1
        async with self._lock:
            self._purge()
            if self.scheduler.query_changes.since(base_revision) is None:
                return _error("resource_exhausted", "query change window expired")
            replacement = self._replaceable_snapshot(owner)
            replaced_size = self._snapshots[replacement].size if replacement else 0
            if (
                len(self._snapshots) - bool(replacement) >= MAX_SNAPSHOTS
                or self._retained_size() + size - replaced_size > MAX_RETAINED_BYTES
            ):
                return _error("resource_exhausted", "snapshot budget exceeded")
            if replacement is not None:
                del self._snapshots[replacement]
            self._snapshots[snapshot_id] = _Snapshot(
                owner,
                query_id,
                scope,
                request.depth,
                base_revision,
                pages,
                captured_at + SNAPSHOT_SECONDS,
                size,
            )
        return decode_query_document(pages[0], expected="page")

    @staticmethod
    def _scope(request: PageRequest) -> str:
        return request.scope_path or (
            "/" + request.flow_name if request.flow_name else "/"
        )

    @staticmethod
    def _visible_path(path: str, scope: str, depth: int | None) -> bool:
        if path != scope and not (scope == "/" or path.startswith(scope + "/")):
            return False
        relative_depth = (0 if path == "/" else path.count("/")) - (
            0 if scope == "/" else scope.count("/")
        )
        return depth is None or relative_depth <= depth

    def _reset(self, request: SinceRequest, reason: str) -> ResetReply:
        return ResetReply(
            kind="reset",
            schema_version=1,
            session_id=request.session_id,
            query_id=request.query_id,
            since_revision=request.since_revision,
            reset_scope=self._scope(request),
            reason=reason,
        )

    def _delta_paths(self, batches, scope: str, depth: int | None):
        paths: set[str] = set()
        invalidations: set[str] = set()
        for batch in batches:
            for change in batch.changes:
                if change.invalidate and (
                    not change.groups or "summary" in change.groups
                ):
                    if change.path == scope or scope.startswith(
                        change.path.rstrip("/") + "/"
                    ):
                        return None
                    if self._visible_path(change.path, scope, depth):
                        invalidations.add(change.path)
                if "summary" in change.groups and self._visible_path(
                    change.path, scope, depth
                ):
                    paths.add(change.path)
        reduced: list[str] = []
        for path in sorted(invalidations):
            if not reduced or not path.startswith(reduced[-1] + "/"):
                reduced.append(path)
        visible_paths = set()
        for path in paths:
            index = bisect_right(reduced, path) - 1
            if index < 0 or not (
                path == reduced[index] or path.startswith(reduced[index] + "/")
            ):
                visible_paths.add(path)
        return visible_paths, set(reduced)

    def _project_delta(
        self, paths: set[str], invalidations: set[str]
    ) -> list[UpsertSummary | InvalidateScope]:
        operations: list[UpsertSummary | InvalidateScope] = [
            InvalidateScope(
                op="invalidate_scope", path=path, reason="structure_changed"
            )
            for path in sorted(invalidations)
        ]
        for path in sorted(paths):
            node, generation = next(
                iter_query_summaries(self.scheduler.bunch, scope_path=path, depth=0)
            )
            operations.append(
                UpsertSummary(
                    op="upsert_summary",
                    path=path,
                    flow_generation=generation,
                    node=node,
                )
            )
        return operations

    def _delta_pages(
        self,
        request: SinceRequest,
        owner: str,
        batch_id: str,
        target_revision: int,
        operations: list[UpsertSummary | InvalidateScope],
    ) -> tuple[tuple[bytes, ...], float]:
        captured_at = time.monotonic()
        observed = datetime.now(timezone.utc)
        as_of = _utc(observed)
        expires_at = _utc(observed + timedelta(seconds=SNAPSHOT_SECONDS))
        pages: list[bytes] = []
        current: list[UpsertSummary | InvalidateScope] = []
        estimated = 1200
        total_size = 0

        def flush(complete: bool) -> None:
            nonlocal current, estimated, total_size
            index = len(pages)
            reply = SinceReply(
                kind="since",
                schema_version=1,
                session_id=self.session_id,
                query_id=request.query_id,
                batch_id=batch_id,
                from_revision=request.since_revision,
                target_revision=target_revision,
                as_of=as_of,
                expires_at=expires_at,
                page_index=index,
                complete=complete,
                next_cursor=None
                if complete
                else self._cursor(batch_id, index + 1, owner),
                unchanged=target_revision == request.since_revision,
                operations=current,
            )
            raw = encode_query_document(reply)
            total_size += len(raw)
            if total_size > MAX_DELTA_BATCH_BYTES:
                raise MemoryError("delta batch budget exceeded")
            pages.append(raw)
            current = []
            estimated = 1200

        for operation in operations:
            size = (
                len(
                    json.dumps(
                        operation.model_dump(exclude_unset=True),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode()
                )
                + 2
            )
            if current and (
                len(current) >= MAX_PAGE_NODES or estimated + size > PAGE_TARGET_BYTES
            ):
                flush(False)
            if size + 1200 > MAX_PAGE_BYTES:
                raise OverflowError("delta operation exceeds page budget")
            current.append(operation)
            estimated += size
        flush(True)
        return tuple(pages), captured_at + SNAPSHOT_SECONDS

    def _build_delta_pages(
        self,
        request: SinceRequest,
        owner: str,
        batch_id: str,
        target: int,
        paths: set[str],
        invalidations: set[str],
    ) -> tuple[tuple[bytes, ...], float]:
        return self._delta_pages(
            request,
            owner,
            batch_id,
            target,
            self._project_delta(paths, invalidations),
        )

    async def since(self, request: SinceRequest) -> QueryModel:
        if request.field_groups != ["summary"]:
            return _error(
                "unsupported_capability", "incremental pages support summary only"
            )
        if request.session_id != self.session_id:
            return self._reset(request, "session_changed")
        owner = self._owner()
        if request.query_id != self._query_id(request, owner):
            return self._reset(request, "scope_changed")
        scope = self._scope(request)
        if request.cursor is not None:
            parsed = self._parse_cursor(request.cursor, owner)
            if parsed is None:
                return _error("invalid_request", "invalid cursor")
            batch_id, index = parsed
            async with self._lock:
                self._purge()
                batch = self._delta_batches.get(batch_id)
                if batch is None:
                    return self._reset(request, "history_expired")
                if (
                    batch.owner != owner
                    or batch.query_id != request.query_id
                    or batch.from_revision != request.since_revision
                ):
                    return self._reset(request, "identity_changed")
                if index < 1 or index >= len(batch.pages):
                    return _error("invalid_request", "cursor index out of range")
                return decode_query_document(batch.pages[index], expected="since")

        guard = self.scheduler.query_revision
        if guard % 2:
            return _error("resource_exhausted", "query state is changing")
        tracker = self.scheduler.query_changes
        target = tracker.revision
        if request.since_revision > target:
            return self._reset(request, "identity_changed")
        changes = tracker.since(request.since_revision)
        if changes is None:
            return self._reset(request, "history_expired")
        selection = self._delta_paths(changes, scope, request.depth)
        if selection is None:
            return self._reset(request, "structure_changed")
        paths, invalidations = selection
        batch_id = secrets.token_urlsafe(24)
        try:
            pages, expires = (
                await asyncio.to_thread(
                    self._build_delta_pages,
                    request,
                    owner,
                    batch_id,
                    target,
                    paths,
                    invalidations,
                )
                if paths or invalidations
                else self._delta_pages(request, owner, batch_id, target, [])
            )
        except (ValueError, KeyError, RuntimeError, StopIteration):
            return self._reset(request, "structure_changed")
        except (WireError, OverflowError, MemoryError):
            return self._reset(request, "budget_exceeded")
        if self.scheduler.query_revision != guard or tracker.revision != target:
            return _error("resource_exhausted", "query state changed during delta")
        if len(pages) == 1:
            return decode_query_document(pages[0], expected="since")
        async with self._lock:
            self._purge()
            size = sum(map(len, pages))
            if (
                len(self._delta_batches) >= MAX_DELTA_BATCHES
                or sum(batch.size for batch in self._delta_batches.values()) + size
                > MAX_DELTA_RETAINED_BYTES
                or self._retained_size() + size > MAX_RETAINED_BYTES
            ):
                return self._reset(request, "budget_exceeded")
            self._delta_batches[batch_id] = _DeltaBatch(
                owner,
                request.query_id,
                request.since_revision,
                pages,
                expires,
                size,
            )
        return decode_query_document(pages[0], expected="since")

    async def detail(self, request: DetailRequest) -> QueryModel:
        owner = self._owner()
        async with self._lock:
            self._purge()
            if request.session_id != self.session_id:
                return _error("snapshot_expired", "session changed")
            matching = [
                s
                for s in self._snapshots.values()
                if s.query_id == request.query_id and s.owner == owner
            ]
            if not matching:
                return _error("snapshot_expired", "query unavailable")
            snapshot = matching[0]
        path = request.path
        if (
            snapshot.scope != "/"
            and path != snapshot.scope
            and not path.startswith(snapshot.scope + "/")
        ):
            return _error("permission_denied", "path outside query scope")
        if (
            snapshot.depth is not None
            and (path.count("/") if path != "/" else 0)
            - (snapshot.scope.count("/") if snapshot.scope != "/" else 0)
            > snapshot.depth
        ):
            return _error("permission_denied", "path outside query depth")
        async with self.scheduler.query_state_lock:
            node = find_query_node(self.scheduler.bunch, path)
            if node is None:
                return _error("stale_identity", "node no longer exists")
            flow = (
                self.scheduler.bunch.find_flow(path.split("/", 2)[1])
                if path != "/"
                else None
            )
            generation = flow.definition_generation if flow else None
            attempt = node.attempt_id if isinstance(node, Task) else None
            if generation != request.flow_generation or attempt != request.attempt_id:
                return _error("stale_identity", "node identity changed")
            try:
                groups = project_query_detail(
                    self.scheduler.bunch,
                    path,
                    request.field_groups,
                    redacted_parameters=self.scheduler.query_redacted_parameters,
                    server_status=(
                        self.scheduler.server_status_response().model_dump(mode="json")
                        if "service" in request.field_groups
                        else None
                    ),
                )
                reply = DetailReply(
                    kind="detail",
                    schema_version=1,
                    session_id=self.session_id,
                    query_id=request.query_id,
                    path=path,
                    flow_generation=generation,
                    attempt_id=attempt,
                    sample_mode="live",
                    as_of=_utc(datetime.now(timezone.utc)),
                    requested_groups=request.field_groups,
                    groups=groups,
                )
                encode_query_document(reply)
                return reply
            except WireError:
                return await self._chunked_detail(reply, owner)
            except ValueError:
                return _error("invalid_request", "detail group unavailable")

    async def _chunked_detail(self, reply: DetailReply, owner: str) -> QueryModel:
        """Replace oversized fields with digest-checked, short-lived blob refs."""
        groups = reply.groups.model_dump(mode="json", exclude_unset=True)
        projected_bytes = 0
        encoder = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"))
        for fragment in encoder.iterencode(groups):
            projected_bytes += len(fragment.encode())
            if projected_bytes > MAX_BLOB_BYTES:
                return _error("resource_exhausted", "detail projection budget exceeded")
        async with self._lock:
            self._purge()
            retained = self._retained_size()
            if retained + 4 * projected_bytes > MAX_QUERY_BYTES:
                return _error("resource_exhausted", "detail workspace budget exceeded")
        refs: list[DetailBlobRef] = []
        pending: list[tuple[str, str, bytes, str]] = []
        while True:
            candidates = []

            def visit(value, path):
                if len(path) >= 2:
                    if len(candidates) >= 10_000:
                        raise MemoryError("too many detail fields")
                    size = len(
                        json.dumps(
                            value, ensure_ascii=False, separators=(",", ":")
                        ).encode()
                    )
                    replacement = (
                        []
                        if isinstance(value, list)
                        else {}
                        if isinstance(value, dict)
                        else None
                    )
                    candidates.append((size, path, replacement))
                if len(path) >= 16:
                    return
                if isinstance(value, dict):
                    for key, child in value.items():
                        visit(child, path + [key])
                elif isinstance(value, list):
                    for index, child in enumerate(value):
                        visit(child, path + [index])

            try:
                visit(groups, [])
            except MemoryError:
                return _error("resource_exhausted", "too many detail fields")
            changed = False
            for size, path, replacement in sorted(
                candidates, key=lambda item: item[0], reverse=True
            ):
                if size < 1024:
                    break
                if size > MAX_FIELD_BYTES:
                    continue
                trial = copy.deepcopy(groups)
                parent = trial
                for part in path[:-1]:
                    parent = parent[part]
                parent[path[-1]] = replacement
                try:
                    validated = DetailGroups.model_validate(trial)
                except ValueError:
                    continue
                original = groups
                for part in path:
                    original = original[part]
                raw = json.dumps(
                    original, ensure_ascii=False, separators=(",", ":")
                ).encode()
                pointer = "/groups/" + "/".join(
                    str(part).replace("~", "~0").replace("/", "~1") for part in path
                )
                blob_id = secrets.token_urlsafe(24)
                ref = DetailBlobRef(
                    pointer=pointer,
                    blob_id=blob_id,
                    total_bytes=size,
                    sha256=hashlib.sha256(raw).hexdigest(),
                )
                test = reply.model_copy(
                    update={"groups": validated, "blob_refs": refs + [ref]}
                )
                groups = trial
                refs.append(ref)
                pending.append((blob_id, pointer, raw, ref.sha256))
                changed = True
                try:
                    encode_query_document(test)
                except WireError:
                    break
                async with self._lock:
                    self._purge()
                    blob_bytes = sum(len(b.data) for b in self._blobs.values())
                    incoming = sum(len(item[2]) for item in pending)
                    if (
                        blob_bytes + incoming > MAX_BLOB_BYTES
                        or self._retained_size() + incoming > MAX_RETAINED_BYTES
                    ):
                        return _error("resource_exhausted", "blob budget exceeded")
                    expires = time.monotonic() + 60
                    for bid, item_pointer, data, digest in pending:
                        self._blobs[bid] = _Blob(
                            owner,
                            reply.query_id,
                            reply.path,
                            reply.flow_generation,
                            reply.attempt_id,
                            item_pointer,
                            data,
                            digest,
                            expires,
                        )
                return test
            if not changed:
                return _error("field_too_large", "detail cannot fit document limit")

    async def chunk(self, request: ChunkRequest) -> QueryModel:
        owner = self._owner()
        async with self.scheduler.query_state_lock:
            async with self._lock:
                self._purge()
                blob = self._blobs.get(request.blob_id)
                if blob is None:
                    return _error("snapshot_expired", "blob unavailable")
                if blob.owner != owner:
                    return _error("permission_denied", "blob owner changed")
                node = find_query_node(self.scheduler.bunch, blob.path)
                flow = (
                    self.scheduler.bunch.find_flow(blob.path.split("/", 2)[1])
                    if blob.path != "/"
                    else None
                )
                generation = flow.definition_generation if flow else None
                attempt = node.attempt_id if isinstance(node, Task) else None
                if (
                    node is None
                    or generation != blob.generation
                    or attempt != blob.attempt
                ):
                    return _error("stale_identity", "blob identity changed")
                if request.offset >= len(blob.data):
                    return _error("invalid_request", "chunk offset out of range")
                data = blob.data[request.offset : request.offset + CHUNK_BYTES]
                return ChunkReply(
                    kind="chunk",
                    schema_version=1,
                    blob_id=request.blob_id,
                    offset=request.offset,
                    total_bytes=len(blob.data),
                    sha256=blob.digest,
                    data_base64=base64.b64encode(data).decode(),
                    complete=request.offset + len(data) == len(blob.data),
                )
