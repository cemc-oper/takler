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
from contextvars import copy_context
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
MAX_QUERY_BYTES = 128 * 1024 * 1024
# Reserve 32 MiB of the total for page models, encoding buffers, and indexes.
MAX_RETAINED_BYTES = 96 * 1024 * 1024
MAX_BLOB_BYTES = 32 * 1024 * 1024
MAX_FIELD_BYTES = 16 * 1024 * 1024
CHUNK_BYTES = 65_536
# Leave room for page metadata and cursor. Every serialized document is checked.
PAGE_TARGET_BYTES = 230_000
MAX_DELTA_REQUEST_SECONDS = 120
MAX_DELTA_BATCH_BYTES = 8 * 1024 * 1024
MAX_DELTA_RETAINED_BYTES = 32 * 1024 * 1024
MAX_CAPTURE_WORKERS = 2
MAX_SUMMARY_CAPTURE_ROOTS = 4
# Per-view allowance for the private ids, expiry and dictionary entry.
SUMMARY_VIEW_BYTES = 512
# Allow for the lease, its two fixed-length identities and the dictionary entry.
# Charge the variable-length scope separately.
QUERY_LEASE_BYTES = 512


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
class _SummarySelection:
    root_id: int
    base_revision: int
    scope: str
    depth: int | None
    redacted_parameters: tuple[str, ...]


@dataclass(frozen=True, eq=False)
class _SummaryContent:
    selection: _SummarySelection
    source: weakref.ReferenceType
    original_snapshot_id: str
    pages: tuple[bytes, ...]
    expires: float
    size: int


@dataclass(frozen=True)
class _Snapshot:
    owner: str
    query_id: str
    content: _SummaryContent
    expires: float


@dataclass
class _Capture:
    content: _ContentCapture
    task: asyncio.Task | None = None
    readers: int = 0


@dataclass
class _ContentCapture:
    snapshot_id: str
    version: object
    task: asyncio.Task | None = None
    readers: int = 0
    reserved_bytes: int = 0


@dataclass
class _QueryLease:
    owner: str
    scope: str
    depth: int | None
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
        # Small authorization records outlive page bytes while since keeps a
        # scope active. Neither pages nor live node values are retained here.
        self._query_leases: dict[str, _QueryLease] = {}
        self._query_lease_bytes = 0
        self._blobs: dict[str, _Blob] = {}
        self._delta_batches: dict[str, _DeltaBatch] = {}
        self._lock = asyncio.Lock()
        self._reserved = 0
        self._captures: dict[tuple[str, int, int], _Capture] = {}
        self._content_captures: dict[_SummarySelection, _ContentCapture] = {}
        self._capture_root_available = asyncio.Event()
        self._capture_root_available.set()
        self._snapshot_expiry: asyncio.TimerHandle | None = None
        self._closed = False
        self._capture_slots = asyncio.Semaphore(MAX_CAPTURE_WORKERS)
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
                self._query_lease_bytes -= lease.size
        for key, snapshot in list(self._snapshots.items()):
            if snapshot.expires <= now:
                del self._snapshots[key]
        for key, blob in list(self._blobs.items()):
            if blob.expires <= now:
                del self._blobs[key]
        for key, batch in list(self._delta_batches.items()):
            if batch.expires <= now:
                del self._delta_batches[key]
        if (
            not (
                self._snapshots
                or self._query_leases
                or self._delta_batches
                or self._blobs
            )
            and self._snapshot_expiry is not None
        ):
            self._snapshot_expiry.cancel()
            self._snapshot_expiry = None

    def _schedule_snapshot_expiry(self, expires: float) -> None:
        if self._closed:
            return
        loop = asyncio.get_running_loop()
        when = loop.time() + max(0.001, expires - _monotonic())
        if self._snapshot_expiry is not None:
            if self._snapshot_expiry.when() <= when:
                return
            self._snapshot_expiry.cancel()
        self._snapshot_expiry = loop.call_at(when, self._expire_snapshots)

    def _expire_snapshots(self) -> None:
        self._snapshot_expiry = None
        self._purge()
        # All retained query data can expire while the server is idle.
        expires = min(
            (
                row.expires
                for table in (
                    self._snapshots,
                    self._query_leases,
                    self._delta_batches,
                    self._blobs,
                )
                for row in table.values()
            ),
            default=None,
        )
        if expires is not None:
            self._schedule_snapshot_expiry(expires)

    def close(self) -> None:
        """Release query data and prevent late captures from retaining pages."""
        self._closed = True
        self._capture_root_available.set()
        if self._snapshot_expiry is not None:
            self._snapshot_expiry.cancel()
            self._snapshot_expiry = None
        self._snapshots.clear()
        self._query_leases.clear()
        self._query_lease_bytes = 0
        self._blobs.clear()
        self._delta_batches.clear()
        self.full_snapshots.close()

    def _retained_size(self, *, adding=None, reserved_view=False) -> int:
        contents = {view.content for view in self._snapshots.values()}
        if adding is not None:
            contents.add(adding)
        occupied = {content.original_snapshot_id for content in contents}
        views = len(self._snapshots) + self._reserved
        if adding is not None and not reserved_view:
            views += 1
        return (
            self.scheduler.query_changes.estimated_bytes
            + self._query_lease_bytes
            + sum(content.size for content in contents)
            + sum(
                build.reserved_bytes
                for build in self._content_captures.values()
                if build.snapshot_id not in occupied
            )
            + views * SUMMARY_VIEW_BYTES
            + sum(len(blob.data) for blob in self._blobs.values())
            + sum(batch.size for batch in self._delta_batches.values())
        )

    def _new_lease_bytes(self, query_id: str, scope: str) -> int:
        if query_id in self._query_leases:
            return 0
        return QUERY_LEASE_BYTES + len(scope.encode("utf-8"))

    def _renew_lease(
        self, query_id: str, owner: str, scope: str, depth: int | None
    ) -> None:
        """Keep an active scope without extending retained page or blob bytes."""
        expires = _monotonic() + QUERY_LEASE_SECONDS
        lease = self._query_leases.get(query_id)
        if lease is None:
            size = self._new_lease_bytes(query_id, scope)
            self._query_leases[query_id] = _QueryLease(
                owner, scope, depth, expires, size
            )
            self._query_lease_bytes += size
        else:
            lease.expires = expires
        self._schedule_snapshot_expiry(expires)

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

    def _page_identity(self, owner, query_id, snapshot_id, next_index):
        return dict(
            query_id=query_id,
            snapshot_id=snapshot_id,
            next_cursor=(
                self._cursor(snapshot_id, next_index, owner)
                if next_index is not None
                else None
            ),
        )

    def _snapshot_page(self, snapshot_id: str, snapshot: _Snapshot, index: int):
        content = snapshot.content
        page = content.pages[index]
        if snapshot_id == content.original_snapshot_id:
            return page
        next_index = index + 1 if index + 1 < len(content.pages) else None
        identity = self._page_identity(
            snapshot.owner, snapshot.query_id, snapshot_id, next_index
        )
        prefix = json.dumps(identity, separators=(",", ":")).encode()[:-1]
        # _capture writes these three server-generated fields first, followed
        # by "kind". Only the requested page gets an owner-specific envelope.
        # The inspected prefix contains no user data and has bounded length.
        body_start = page.index(b',"kind":')
        return b"".join((prefix, memoryview(page)[body_start:]))

    def _capture(
        self,
        request: PageRequest,
        owner: str,
        query_id: str,
        snapshot_id: str,
        base_revision: int,
        budget: int,
        version=None,
        reserve_page=None,
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
                **self._page_identity(
                    owner, query_id, snapshot_id, None if complete else page_index + 1
                ),
                kind="page",
                schema_version=1,
                session_id=self.session_id,
                base_revision=base_revision,
                as_of=as_of,
                expires_at=expires_at,
                page_index=page_index,
                total_nodes=None,
                complete=complete,
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
            if reserve_page is not None:
                reserve_page(len(raw))
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
        while True:
            async with self._lock:
                result = self._begin_page(request, owner, query_id)
            if isinstance(result, _Capture):
                capture = result
                break
            if result is not None:
                return result
            # Only the waiting request survives here. A free version slot
            # lets it retry against the newest root instead of holding an old one.
            await self._capture_root_available.wait()

        try:
            try:
                reply = await asyncio.shield(capture.task)
            finally:
                capture.readers -= 1
        except asyncio.CancelledError:
            # Other readers may still use the capture. Let its worker return
            # the reserved bytes before the caller can shut down its loop.
            try:
                await asyncio.shield(capture.task)
            except Exception:
                pass
            raise
        return reply if isinstance(reply, bytes) else reply.model_copy(deep=True)

    def _begin_page(
        self, request: PageRequest, owner: str, query_id: str
    ) -> bytes | QueryModel | _Capture | None:
        """Return a page or its pending capture, or wait for a root slot on None."""
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
            if index < 1 or index >= len(snapshot.content.pages):
                return _error("invalid_request", "cursor index out of range")
            return self._snapshot_page(snapshot_id, snapshot, index)
        try:
            version = self.scheduler.version_store.pin()
        except VersionUnavailable as exc:
            return _error("resource_exhausted", str(exc))
        base_revision = self.scheduler.query_changes.revision
        selection = _SummarySelection(
            id(version),
            base_revision,
            self._scope(request),
            request.depth,
            tuple(self.scheduler.query_redacted_parameters),
        )
        # Only the public summary field group is admitted above. Compare
        # the weak root reference as well as its id (ids can be recycled).
        reusable = None
        for snapshot_id, snapshot in self._snapshots.items():
            content = snapshot.content
            if (
                content.selection == selection
                and content.source() is version
                and snapshot.expires - _monotonic() >= SNAPSHOT_SECONDS / 2
            ):
                if (
                    snapshot.owner == owner
                    and snapshot.query_id == query_id
                    and query_id in self._query_leases
                ):
                    self._renew_lease(query_id, owner, selection.scope, request.depth)
                    return self._snapshot_page(snapshot_id, snapshot, 0)
                reusable = content

        key = (query_id, id(version), base_revision)
        capture = self._captures.get(key)
        if capture is None:
            snapshot_id = secrets.token_urlsafe(24)
            if reusable is not None:
                return self._publish_snapshot(owner, query_id, snapshot_id, reusable)
            if self._retained_size() + SUMMARY_VIEW_BYTES > MAX_RETAINED_BYTES:
                return _error("resource_exhausted", "snapshot memory limit")
            content_capture = self._content_captures.get(selection)
            if content_capture is None:
                roots = {key.root_id for key in self._content_captures}
                if (
                    selection.root_id not in roots
                    and len(roots) >= MAX_SUMMARY_CAPTURE_ROOTS
                ):
                    self._capture_root_available.clear()
                    return None
                content_capture = _ContentCapture(snapshot_id, version)
                content_capture.task = asyncio.create_task(
                    self._build_content(
                        request,
                        owner,
                        query_id,
                        selection,
                        content_capture,
                    )
                )
                self._content_captures[selection] = content_capture
            content_capture.readers += 1
            self._reserved += 1
            capture = _Capture(content_capture)
            self._captures[key] = capture
            capture.task = asyncio.create_task(
                self._capture_snapshot(
                    owner,
                    query_id,
                    snapshot_id,
                    key,
                    capture,
                    selection,
                )
            )
            capture.task.add_done_callback(
                lambda task: task.exception() if not task.cancelled() else None
            )
        capture.readers += 1
        return capture

    async def _capture_snapshot(
        self, owner, query_id, snapshot_id, key, capture, selection
    ):
        content_capture = capture.content
        try:
            content = await asyncio.shield(content_capture.task)
            async with self._lock:
                self._purge()
                if self._closed or not capture.readers:
                    return _error("snapshot_expired", "summary capture abandoned")
                if isinstance(content, FailureReply):
                    return content
                return self._publish_snapshot(
                    owner, query_id, snapshot_id, content, reserved_view=True
                )
        finally:
            # Return the reservation on the same loop turn as publication.
            content_capture.readers -= 1
            if not content_capture.readers:
                del self._content_captures[selection]
                if not self._capture_root_available.is_set() and all(
                    key.root_id != selection.root_id for key in self._content_captures
                ):
                    self._capture_root_available.set()
            self._captures.pop(key, None)
            self._reserved -= 1

    async def _reserve_summary_page(self, build, size):
        # Accounting runs on the same loop as delta, blob and view publication.
        if self._closed:
            raise RuntimeError("query service closed")
        if self._retained_size() + size > MAX_RETAINED_BYTES:
            raise MemoryError("snapshot budget exceeded")
        build.reserved_bytes += size

    async def _build_content(self, request, owner, query_id, selection, build):
        captured_at = _monotonic()
        loop = asyncio.get_running_loop()

        def reserve_page(size):
            reservation = asyncio.run_coroutine_threadsafe(
                self._reserve_summary_page(build, size), loop
            )
            try:
                reservation.result(timeout=SNAPSHOT_SECONDS)
            except TimeoutError:
                reservation.cancel()
                raise MemoryError("snapshot reservation expired") from None

        try:
            async with self._capture_slots:
                if self._closed or not any(
                    view.content is build and view.readers
                    for view in self._captures.values()
                ):
                    return _error("snapshot_expired", "summary capture abandoned")
                if captured_at + SNAPSHOT_SECONDS <= _monotonic():
                    return _error("snapshot_expired", "summary capture expired")
                work = asyncio.create_task(
                    asyncio.to_thread(
                        self._capture,
                        request,
                        owner,
                        query_id,
                        build.snapshot_id,
                        selection.base_revision,
                        MAX_RETAINED_BYTES - self._retained_size(),
                        build.version,
                        reserve_page,
                    )
                )
                try:
                    _, pages, size = await asyncio.shield(work)
                except asyncio.CancelledError:
                    await work
                    raise
        except WireError:
            return _error("field_too_large", "summary exceeds page limit")
        except (RuntimeError, KeyError, IndexError):
            return _error("resource_exhausted", "fixed query version unavailable")
        except ValueError:
            return _error("invalid_request", "scope unavailable or invalid")
        except OverflowError:
            return _error("field_too_large", "summary exceeds page limit")
        except MemoryError:
            return _error("resource_exhausted", "snapshot memory limit")
        return _SummaryContent(
            selection,
            weakref.ref(build.version),
            build.snapshot_id,
            pages,
            captured_at + SNAPSHOT_SECONDS,
            size,
        )

    def _publish_snapshot(
        self, owner, query_id, snapshot_id, content, *, reserved_view=False
    ):
        if content.expires <= _monotonic():
            return _error("snapshot_expired", "summary capture expired")
        selection = content.selection
        if self.scheduler.query_changes.since(selection.base_revision) is None:
            return _error("resource_exhausted", "query change window expired")
        if (
            self._retained_size(adding=content, reserved_view=reserved_view)
            + self._new_lease_bytes(query_id, selection.scope)
            > MAX_RETAINED_BYTES
        ):
            return _error("resource_exhausted", "snapshot budget exceeded")
        snapshot = _Snapshot(owner, query_id, content, content.expires)
        self._snapshots[snapshot_id] = snapshot
        self._renew_lease(query_id, owner, selection.scope, selection.depth)
        self._schedule_snapshot_expiry(content.expires)
        return self._snapshot_page(snapshot_id, snapshot, 0)

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

    def _delta_worker_done(self, future: asyncio.Future) -> None:
        # A timed-out request's thread still occupies its worker until done.
        self._capture_slots.release()
        if not future.cancelled():
            future.exception()

    def _delta_selection(self, request: SinceRequest):
        tracker = self.scheduler.query_changes
        if request.since_revision > tracker.revision:
            return self._reset(request, "identity_changed")
        changes = tracker.since(request.since_revision)
        if changes is None:
            return self._reset(request, "history_expired")
        selection = self._delta_paths(changes, self._scope(request), request.depth)
        if selection is None:
            return self._reset(request, "structure_changed")
        return selection

    async def _changed_delta(self, request, owner, batch_id, target, selection):
        await self._capture_slots.acquire()
        work = None
        try:
            self._purge()
            if request.query_id not in self._query_leases:
                return self._reset(request, "history_expired")
            try:
                version = self.scheduler.version_store.pin()
            except VersionUnavailable as exc:
                return _error("resource_exhausted", str(exc))
            if selection is None or target != self.scheduler.query_changes.revision:
                target = self.scheduler.query_changes.revision
                selection = self._delta_selection(request)
                if isinstance(selection, ResetReply):
                    return selection
            paths, invalidations = selection
            if not paths and not invalidations:
                return self._delta_pages(request, owner, batch_id, target, [])
            work = asyncio.get_running_loop().run_in_executor(
                None,
                copy_context().run,
                self._build_delta_pages,
                request,
                owner,
                batch_id,
                target,
                paths,
                invalidations,
                version,
            )
            work.add_done_callback(self._delta_worker_done)
            return await asyncio.shield(work)
        finally:
            if work is None:
                self._capture_slots.release()

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

        batch_id = secrets.token_urlsafe(24)
        try:
            self.scheduler.version_store.pin()
        except VersionUnavailable as exc:
            return _error("resource_exhausted", str(exc))
        try:
            target = self.scheduler.query_changes.revision
            selection = self._delta_selection(request)
            if isinstance(selection, ResetReply):
                return selection
            if not any(selection):
                pages, expires = self._delta_pages(request, owner, batch_id, target, [])
            else:
                # Waiting monitors keep only their waterline. Re-read the journal
                # at dispatch instead of keeping a set of changed paths per waiter.
                if self._capture_slots.locked():
                    selection = None
                async with asyncio.timeout(MAX_DELTA_REQUEST_SECONDS):
                    result = await self._changed_delta(
                        request, owner, batch_id, target, selection
                    )
                if isinstance(result, (FailureReply, ResetReply)):
                    return result
                pages, expires = result
        except TimeoutError:
            return _error("resource_exhausted", "delta request deadline exceeded")
        except (WireError, OverflowError, MemoryError):
            return self._reset(request, "budget_exceeded")
        except (ValueError, KeyError, RuntimeError, StopIteration):
            return self._reset(request, "structure_changed")
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
                sum(batch.size for batch in self._delta_batches.values()) + size
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
            self._schedule_snapshot_expiry(expires)
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
                    self._schedule_snapshot_expiry(expires)
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
