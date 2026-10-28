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
import weakref
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from takler.core import Task
from takler.core.state import NodeStatus
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
    SummaryNode,
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
from takler.serialization.version import (
    FrozenNode,
    VersionUnavailable,
    _SummaryEncoding,
)


SNAPSHOT_SECONDS = 120
QUERY_LEASE_SECONDS = 120
_monotonic = time.monotonic
MAX_SNAPSHOTS = 8
MAX_OWNER_SNAPSHOTS = 2
MAX_QUERY_LEASES = 32
MAX_OWNER_QUERY_LEASES = 8
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
MAX_CAPTURE_WORKERS = 2
MAX_CAPTURE_WAITING = 2


def _utc(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _error(code: str, message: str) -> FailureReply:
    return FailureReply(kind="error", schema_version=1, code=code, message=message)


def _fixed_summary(
    version,
    path: str,
    generations: dict[str, str | None] | None = None,
    *,
    encoded: bool = False,
) -> tuple[SummaryNode | _SummaryEncoding, str | None]:
    row = version.node(path)
    if row.public is None:
        raise VersionUnavailable("unsupported_plugin_detail")
    source = None
    proof = row.summary_encoding if type(row) is FrozenNode else None
    if (
        encoded
        and type(proof) is _SummaryEncoding
        and proof.data is row.summary
        and proof.path == row.path == path
    ):
        summary = proof
    elif row.summary is not None:
        summary = SummaryNode.model_validate_json(row.summary)
    else:
        source = json.loads(row.public)
        fields = dict(
            path=path,
            node_kind=source["node_kind"],
            type_id=source["type_id"],
            status=NodeStatus(source["state"]["status"]).name,
            suspended=source["state"]["suspended"],
        )
        if source["node_kind"] == "task" and source.get("attempt_id") is not None:
            fields["attempt_id"] = source["attempt_id"]
            if source.get("submission_unknown_reason") is not None:
                fields["submission_unknown"] = True
        summary = SummaryNode(**fields)
    generation = None
    if path != "/":
        flow_path = "/" + path.split("/", 2)[1]
        if generations is not None and flow_path in generations:
            generation = generations[flow_path]
        elif flow_path == path:
            if source is None:
                source = json.loads(row.public)
            generation = source.get("definition_generation")
            if generations is not None:
                generations[flow_path] = generation
        else:
            flow = version.node(flow_path)
            if flow.public is None:
                raise VersionUnavailable("unsupported_plugin_detail")
            generation = json.loads(flow.public).get("definition_generation")
            if generations is not None:
                generations[flow_path] = generation
    return summary, generation


def _iter_fixed_summaries(
    version, scope: str, depth: int | None, *, encoded: bool = False
):
    version.node(scope)
    generations: dict[str, str | None] = {}
    pending = [(scope, 0)]
    while pending:
        path, level = pending.pop()
        yield _fixed_summary(version, path, generations, encoded=encoded)
        if depth is None or level < depth:
            pending.extend(
                (child, level + 1) for child in reversed(version.node(path).children)
            )


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
    source: weakref.ReferenceType


@dataclass
class _Capture:
    task: asyncio.Task | None = None
    readers: int = 0


@dataclass(frozen=True)
class _QueryLease:
    owner: str
    scope: str
    depth: int | None
    expires: float


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
        # Small authorization records outlive page bytes while since keeps a
        # scope active. Neither pages nor live node values are retained here.
        self._query_leases: dict[str, _QueryLease] = {}
        self._blobs: dict[str, _Blob] = {}
        self._delta_batches: dict[str, _DeltaBatch] = {}
        self._lock = asyncio.Lock()
        self._reserved = 0
        self._owner_reserved: dict[str, int] = {}
        self._captures: dict[tuple[str, int, int], _Capture] = {}
        self._capture_readers = 0
        self._snapshot_expiry: asyncio.TimerHandle | None = None
        self._closed = False
        self._capture_slots = asyncio.Semaphore(MAX_CAPTURE_WORKERS)
        self._delta_waiting = 0
        from takler.server.full_snapshot import FullSnapshotService

        self.full_snapshots = FullSnapshotService(self)

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
        now = _monotonic()
        for key, lease in list(self._query_leases.items()):
            if lease.expires <= now:
                del self._query_leases[key]
        for key, snapshot in list(self._snapshots.items()):
            if snapshot.expires <= now:
                del self._snapshots[key]
        for key, blob in list(self._blobs.items()):
            if blob.expires <= now:
                del self._blobs[key]
        for key, batch in list(self._delta_batches.items()):
            if batch.expires <= now:
                del self._delta_batches[key]
        if not self._snapshots and self._snapshot_expiry is not None:
            self._snapshot_expiry.cancel()
            self._snapshot_expiry = None

    def _schedule_snapshot_expiry(self) -> None:
        if self._snapshot_expiry is not None:
            self._snapshot_expiry.cancel()
            self._snapshot_expiry = None
        if self._snapshots and not self._closed:
            delay = min(row.expires for row in self._snapshots.values()) - _monotonic()
            self._snapshot_expiry = asyncio.get_running_loop().call_later(
                max(0.001, delay), self._expire_snapshots
            )

    def _expire_snapshots(self) -> None:
        self._snapshot_expiry = None
        self._purge()
        self._schedule_snapshot_expiry()

    def close(self) -> None:
        """Release query data and prevent late captures from retaining pages."""
        self._closed = True
        if self._snapshot_expiry is not None:
            self._snapshot_expiry.cancel()
            self._snapshot_expiry = None
        self._snapshots.clear()
        self._query_leases.clear()
        self._blobs.clear()
        self._delta_batches.clear()
        self.full_snapshots.close()

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

    def _renew_lease(
        self, query_id: str, owner: str, scope: str, depth: int | None
    ) -> None:
        """Keep an active scope without extending retained page or blob bytes."""
        self._query_leases.pop(query_id, None)
        owned = [
            key for key, value in self._query_leases.items() if value.owner == owner
        ]
        if len(owned) >= MAX_OWNER_QUERY_LEASES:
            del self._query_leases[owned[0]]
        if len(self._query_leases) >= MAX_QUERY_LEASES:
            del self._query_leases[next(iter(self._query_leases))]
        self._query_leases[query_id] = _QueryLease(
            owner, scope, depth, _monotonic() + QUERY_LEASE_SECONDS
        )

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
            full_snapshot_v1=True,
        )

    def _capture(
        self,
        request: PageRequest,
        owner: str,
        query_id: str,
        snapshot_id: str,
        base_revision: int,
        budget: int,
        version=None,
    ):
        scope = request.scope_path or (
            "/" + request.flow_name if request.flow_name else "/"
        )
        observed = datetime.now(timezone.utc)
        expires_at = _utc(observed + timedelta(seconds=SNAPSHOT_SECONDS))
        as_of = (
            _utc(datetime.fromisoformat(version.as_of).astimezone(timezone.utc))
            if version is not None
            else _utc(observed)
        )
        pages = []
        nodes = []
        generations = {}
        estimated = 1200
        total_size = 0
        page_index = 0
        root_name = (
            json.loads(version.node("/").public)["name"]
            if version is not None
            else self.scheduler.bunch.name
        )

        def flush(complete: bool):
            nonlocal nodes, generations, estimated, total_size, page_index
            fields = dict(
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
            )
            if version is None:
                raw = encode_query_document(PageReply(**fields, nodes=nodes))
            else:
                # Each row is either canonical, validated version data bound
                # to its exact bytes/path, or encoded from the legacy model.
                # Decode/validate the complete page once, without serializing
                # all its nodes again. No unchecked page is retained.
                header = json.dumps(
                    dict(**fields, nodes=[]),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
                raw = header[:-2] + b",".join(nodes) + b"]}"
                if len(raw) > MAX_PAGE_BYTES:
                    raise WireError("query document too large")
                PageReply.model_validate_json(raw)
            total_size += len(raw)
            if total_size > budget:
                raise MemoryError("snapshot budget exceeded")
            pages.append(raw)
            nodes, generations, estimated = [], {}, 1200
            page_index += 1

        source = (
            _iter_fixed_summaries(version, scope, request.depth, encoded=True)
            if version is not None
            else iter_query_summaries(
                self.scheduler.bunch, scope_path=scope, depth=request.depth
            )
        )
        for index, (node, generation) in enumerate(source):
            if version is not None and index and index % 256 == 0:
                # A 100k capture runs on a worker thread. Release the GIL at
                # predictable intervals so child calls keep making progress.
                time.sleep(0.0005)
            fixed_row = version.node(node.path) if version is not None else None
            node_size = (
                len(fixed_row.summary)
                if fixed_row is not None and fixed_row.summary is not None
                else len(node.model_dump_json(exclude_unset=True).encode())
            ) + 2
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
            if version is None:
                nodes.append(node)
            elif type(node) is _SummaryEncoding:
                nodes.append(node.data)
            else:
                nodes.append(
                    json.dumps(
                        node.model_dump(mode="json", exclude_unset=True),
                        ensure_ascii=False,
                        separators=(",", ":"),
                        allow_nan=False,
                    ).encode("utf-8")
                )
            estimated += node_size + generation_size
            if flow_path:
                if not generation:
                    raise ValueError("flow has no generation")
                generations[flow_path] = generation
        flush(True)
        return scope, tuple(pages), total_size

    async def page(self, request: PageRequest) -> QueryModel:
        reply = await self._page(request)
        return (
            decode_query_document(reply, expected="page")
            if isinstance(reply, bytes)
            else reply
        )

    async def page_encoded(self, request: PageRequest) -> bytes:
        """Return the validated snapshot page without decoding its cached JSON."""
        reply = await self._page(request)
        return reply if isinstance(reply, bytes) else encode_query_document(reply)

    async def _page(self, request: PageRequest) -> bytes | QueryModel:
        if request.field_groups != ["summary"]:
            return _error(
                "unsupported_capability", "initial pages support summary only"
            )
        owner = self._owner()
        query_id = self._query_id(request, owner)
        async with self._lock:
            self._purge()
            if self._closed:
                return _error("snapshot_expired", "query service closed")
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
                return snapshot.pages[index]
            try:
                version = self.scheduler.version_store.pin()
            except VersionUnavailable as exc:
                return _error("resource_exhausted", str(exc))
            base_revision = self.scheduler.query_changes.revision
            # Exact immutable root identity proves reuse; a revision hint alone
            # does not. Weak references never retain an old full state root.
            for snapshot in self._snapshots.values():
                if (
                    snapshot.owner == owner
                    and snapshot.query_id == query_id
                    and snapshot.base_revision == base_revision
                    and snapshot.source() is version
                    # Leave at least half the original lifetime for a complete
                    # new read; do not hand a fresh reader nearly expired pages.
                    and snapshot.expires - _monotonic() >= SNAPSHOT_SECONDS / 2
                ):
                    self._renew_lease(query_id, owner, snapshot.scope, snapshot.depth)
                    self._schedule_snapshot_expiry()
                    return snapshot.pages[0]
            key = (query_id, id(version), base_revision)
            capture = self._captures.get(key)
            if capture is not None:
                if self._reserved + self._capture_readers >= (
                    MAX_CAPTURE_WORKERS + MAX_CAPTURE_WAITING
                ):
                    return _error("resource_exhausted", "snapshot count limit")
                self._capture_readers += 1
                shared = True
            else:
                replacement = self._replaceable_snapshot(owner)
                if (
                    self._owner_reserved.get(owner, 0) >= MAX_OWNER_SNAPSHOTS
                    or self._reserved + self._capture_readers
                    >= MAX_CAPTURE_WORKERS + MAX_CAPTURE_WAITING
                ) or (
                    len(self._snapshots) + self._reserved >= MAX_SNAPSHOTS
                    and replacement is None
                ):
                    return _error("resource_exhausted", "snapshot count limit")
                budget = MAX_RETAINED_BYTES - self._retained_size()
                self._reserved += 1
                self._owner_reserved[owner] = self._owner_reserved.get(owner, 0) + 1
                capture = _Capture()
                self._captures[key] = capture
                capture.task = asyncio.create_task(
                    self._capture_snapshot(
                        request,
                        owner,
                        query_id,
                        version,
                        base_revision,
                        budget,
                        key,
                        capture,
                    )
                )
                capture.task.add_done_callback(
                    lambda task: task.exception() if not task.cancelled() else None
                )
                shared = False
            capture.readers += 1
        cancelled = False
        try:
            reply = await asyncio.shield(capture.task)
            return reply if isinstance(reply, bytes) else reply.model_copy(deep=True)
        except asyncio.CancelledError:
            cancelled = True
            capture.readers -= 1
            # A caller cannot cancel another reader's shared capture. Keep its
            # reservation until the actual worker stops, even with no readers.
            try:
                await asyncio.shield(capture.task)
            except Exception:
                pass
            raise
        finally:
            # These counters belong to the event loop. No await is needed to
            # release a reader, including repeated cancellation during cleanup.
            if not cancelled:
                capture.readers -= 1
            if shared:
                self._capture_readers -= 1

    async def _capture_snapshot(
        self, request, owner, query_id, version, base_revision, budget, key, capture
    ):
        try:
            return await self._build_snapshot(
                request, owner, query_id, version, base_revision, budget, capture
            )
        finally:
            async with self._lock:
                self._captures.pop(key, None)
                self._reserved -= 1
                self._owner_reserved[owner] -= 1
                if not self._owner_reserved[owner]:
                    del self._owner_reserved[owner]

    async def _build_snapshot(
        self, request, owner, query_id, version, base_revision, budget, capture
    ):
        snapshot_id = secrets.token_urlsafe(24)
        captured_at = _monotonic()
        try:
            try:
                async with self._capture_slots:
                    work = asyncio.create_task(
                        asyncio.to_thread(
                            self._capture,
                            request,
                            owner,
                            query_id,
                            snapshot_id,
                            base_revision,
                            budget,
                            version,
                        )
                    )
                    try:
                        scope, pages, size = await asyncio.shield(work)
                    except asyncio.CancelledError:
                        await work
                        raise
            except (RuntimeError, KeyError, IndexError):
                return _error("resource_exhausted", "fixed query version unavailable")
        except WireError:
            return _error("field_too_large", "summary exceeds page limit")
        except ValueError:
            return _error("invalid_request", "scope unavailable or invalid")
        except OverflowError:
            return _error("field_too_large", "summary exceeds page limit")
        except MemoryError:
            return _error("resource_exhausted", "snapshot memory limit")
        async with self._lock:
            self._purge()
            if self._closed or not capture.readers:
                return _error("snapshot_expired", "summary capture abandoned")
            if captured_at + SNAPSHOT_SECONDS <= _monotonic():
                return _error("snapshot_expired", "summary capture expired")
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
                weakref.ref(version),
            )
            self._renew_lease(query_id, owner, scope, request.depth)
            self._schedule_snapshot_expiry()
        return pages[0]

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
                if change.invalidate or change.groups.intersection(
                    {"definition", "parameters"}
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
        self, paths: set[str], invalidations: set[str], version=None
    ) -> list[UpsertSummary | InvalidateScope]:
        operations: list[UpsertSummary | InvalidateScope] = [
            InvalidateScope(
                op="invalidate_scope", path=path, reason="structure_changed"
            )
            for path in sorted(invalidations)
        ]
        for path in sorted(paths):
            node, generation = (
                _fixed_summary(version, path)
                if version is not None
                else next(
                    iter_query_summaries(self.scheduler.bunch, scope_path=path, depth=0)
                )
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
        captured_at = _monotonic()
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
        version=None,
    ) -> tuple[tuple[bytes, ...], float]:
        return self._delta_pages(
            request,
            owner,
            batch_id,
            target,
            self._project_delta(paths, invalidations, version),
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
        async with self._lock:
            self._purge()
            lease = self._query_leases.get(request.query_id)
            if (
                lease is None
                or lease.owner != owner
                or lease.scope != scope
                or lease.depth != request.depth
            ):
                return self._reset(request, "history_expired")
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
                self._renew_lease(request.query_id, owner, scope, request.depth)
                return decode_query_document(batch.pages[index], expected="since")

        try:
            version = self.scheduler.version_store.pin()
        except VersionUnavailable as exc:
            return _error("resource_exhausted", str(exc))
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
            if paths or invalidations:
                async with self._lock:
                    if self._delta_waiting >= MAX_CAPTURE_WORKERS + MAX_CAPTURE_WAITING:
                        return _error("resource_exhausted", "delta worker queue full")
                    self._delta_waiting += 1
                try:
                    async with self._capture_slots:
                        work = asyncio.create_task(
                            asyncio.to_thread(
                                self._build_delta_pages,
                                request,
                                owner,
                                batch_id,
                                target,
                                paths,
                                invalidations,
                                version,
                            )
                        )
                        try:
                            pages, expires = await asyncio.shield(work)
                        except asyncio.CancelledError:
                            await work
                            raise
                finally:
                    async with self._lock:
                        self._delta_waiting -= 1
            else:
                pages, expires = self._delta_pages(request, owner, batch_id, target, [])
        except (ValueError, KeyError, RuntimeError, StopIteration):
            return self._reset(request, "structure_changed")
        except (WireError, OverflowError, MemoryError):
            return self._reset(request, "budget_exceeded")
        if len(pages) == 1:
            async with self._lock:
                self._purge()
                if request.query_id not in self._query_leases:
                    return self._reset(request, "history_expired")
                self._renew_lease(request.query_id, owner, scope, request.depth)
            return decode_query_document(pages[0], expected="since")
        async with self._lock:
            self._purge()
            if request.query_id not in self._query_leases:
                return self._reset(request, "history_expired")
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
            self._renew_lease(request.query_id, owner, scope, request.depth)
        return decode_query_document(pages[0], expected="since")

    async def detail(self, request: DetailRequest) -> QueryModel:
        owner = self._owner()
        async with self._lock:
            self._purge()
            if request.session_id != self.session_id:
                return _error("snapshot_expired", "session changed")
            lease = self._query_leases.get(request.query_id)
            if lease is None or lease.owner != owner:
                return _error("snapshot_expired", "query unavailable")
        path = request.path
        if (
            lease.scope != "/"
            and path != lease.scope
            and not path.startswith(lease.scope + "/")
        ):
            return _error("permission_denied", "path outside query scope")
        if (
            lease.depth is not None
            and (path.count("/") if path != "/" else 0)
            - (lease.scope.count("/") if lease.scope != "/" else 0)
            > lease.depth
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
                async with self._lock:
                    self._purge()
                    if request.query_id not in self._query_leases:
                        return _error("snapshot_expired", "query unavailable")
                    self._renew_lease(request.query_id, owner, lease.scope, lease.depth)
                return reply
            except WireError:
                result = await self._chunked_detail(reply, owner)
                if isinstance(result, DetailReply):
                    async with self._lock:
                        self._purge()
                        if request.query_id not in self._query_leases:
                            return _error("snapshot_expired", "query unavailable")
                        self._renew_lease(
                            request.query_id, owner, lease.scope, lease.depth
                        )
                return result
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
                    expires = _monotonic() + 60
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
