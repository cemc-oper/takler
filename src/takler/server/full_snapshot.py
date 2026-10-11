"""Bounded, fixed-version full detail snapshots for QueryDocument v1."""

from __future__ import annotations

import asyncio
import base64
import gc
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from takler.core.state import NodeStatus
from takler.protocol.query_v1 import (
    GROUPS,
    CancelFullSnapshotReply,
    CancelFullSnapshotRequest,
    FailureReply,
    FullSnapshotChunk,
    FullSnapshotChunkRequest,
    FullSnapshotPage,
    FullSnapshotRequest,
    encode_query_document,
)
from takler.protocol.full_snapshot_codec import pack_node
from takler.serialization.version import StateVersion, VersionUnavailable
from takler.server.query_service import _utc

IDLE_SECONDS = 300
ABSOLUTE_SECONDS = 900
MAX_LEASES = 4
MAX_OWNER_LEASES = 2
MAX_WORKERS = 2
MAX_WAITING = 2
MAX_RETAINED_BYTES = 1536 * 1024 * 1024
MAX_ENCODED_BYTES = 512 * 1024 * 1024
MAX_PAGE_BYTES = 262_144
PAGE_TARGET_BYTES = 230_000
COMPACT_PAGE_TARGET_BYTES = 160_000
COMPACT_PUBLIC_MEDIAN_BYTES = 1600
MAX_FIELD_BYTES = 16 * 1024 * 1024
BLOB_THRESHOLD = 32_768
CHUNK_BYTES = 65_536


def _error(code: str, message: str) -> FailureReply:
    return FailureReply(kind="error", schema_version=1, code=code, message=message)


def _canonical(record: dict) -> bytes:
    return json.dumps(
        record,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def _parameter_items(items: list[dict], source: str) -> list[dict]:
    result = []
    for item in items:
        value = item.get("value")
        record = {"name": item["name"], "source_path": source}
        if value == "<redacted>":
            record["redacted"] = True
        else:
            record["value"] = value
            record["redacted"] = False
        result.append(record)
    return result


def _ancestor_context(version: StateVersion, scope: str) -> list[dict]:
    return [
        {
            "source_path": item["source_path"],
            "user": _parameter_items(item["user"], item["source_path"]),
            "generated": _parameter_items(item["generated"], item["source_path"]),
        }
        for item in version.public_node(scope)["ancestor_context"]
    ]


def _service_context(version: StateVersion) -> dict:
    root = version.public_node("/")
    return {
        **root["server_status"],
        "generated_defaults": _parameter_items(root["generated_parameters"], "/"),
    }


def _full_node(version: StateVersion, path: str, groups: tuple[str, ...]) -> dict:
    row = version.node(path)
    if row.definition == b"{}" and row.runtime == b"{}":
        raise VersionUnavailable("unsupported_plugin_detail")
    # Built-in non-shell records already contain every field this projection
    # reads. Walking and decoding ancestors for each of 100k nodes repeats the
    # same work; only shell paths need the inherited TAKLER_HOME derivation.
    own_public = json.loads(row.public) if row.public is not None else None
    if own_public is None:
        raise VersionUnavailable("unsupported_plugin_detail")
    if own_public["type_id"] in {
        "takler.bunch",
        "takler.flow",
        "takler.container",
        "takler.task",
    }:
        public = own_public
        public["generated_parameters"] = [
            value
            for value in public.get("generated_parameters", [])
            if value["name"] not in {"TAKLER_JOB", "TAKLER_JOBOUT"}
        ]
    else:
        public = version.public_node(path)
    if not public["type_id"].startswith("takler."):
        raise VersionUnavailable("unsupported_plugin_detail")
    state = public["state"]
    status = NodeStatus(state["status"]).name
    record = {
        "path": path,
        "parent_path": row.parent,
        "child_paths": list(row.children),
        "summary": {
            "name": public["name"],
            "node_kind": public["node_kind"],
            "type_id": public["type_id"],
            "status": status,
            "suspended": state["suspended"],
            "definition_generation": public.get("definition_generation"),
            "attempt_id": public.get("attempt_id"),
        },
        "groups": {},
    }
    result = record["groups"]
    if "definition" in groups:
        definition = json.loads(row.definition)
        result["definition"] = {
            key: (
                public.get("script_path")
                if key == "script_path"
                else definition.get(key)
            )
            for key in (
                "trigger",
                "complete_trigger",
                "script_path",
                "times",
                "repeat",
                "events",
                "meters",
                "limits",
                "in_limits",
            )
        }
    if "parameters" in groups:
        result["parameters"] = {
            "user": _parameter_items(public["user_parameters"], path),
            "generated": _parameter_items(public["generated_parameters"], path),
        }
    if "runtime" in groups:
        runtime = json.loads(row.runtime)
        result["runtime"] = {
            key: public.get(key)
            for key in (
                "begun",
                "calendar",
                "task_id",
                "try_no",
                "attempt_id",
                "submitted_at",
                "started_at",
                "ended_at",
                "aborted_reason",
                "submission_unknown_reason",
                "events",
                "meters",
                "limits",
                "repeat",
            )
        }
        result["runtime"]["status"] = status
        result["runtime"]["event_values"] = runtime.get("event_values", {})
        result["runtime"]["meter_values"] = runtime.get("meter_values", {})
        result["runtime"]["limit_usage"] = runtime.get("limit_usage", {})
        result["runtime"]["time_free"] = runtime.get("time_free", {})
        result["runtime"]["repeat_value"] = runtime.get("repeat_value")
        result["runtime"]["trigger_free"] = runtime.get("trigger_free")
        result["runtime"]["complete_trigger_free"] = runtime.get(
            "complete_trigger_free"
        )
    if "artifacts" in groups:
        current = public.get("attempt_id") is not None
        result["artifacts"] = {
            "job_ref": {"path": public["job_path"], "origin": "current"}
            if public.get("job_path")
            else None,
            "jobout_ref": {"path": public["jobout_path"], "origin": "current"}
            if public.get("jobout_path")
            else None,
            "orvix_info_path": public.get("orvix_info_path"),
            "not_applicable": public["node_kind"] != "task",
        }
        for name, ref in (("TAKLER_JOB", "job_ref"), ("TAKLER_JOBOUT", "jobout_ref")):
            if result["artifacts"][ref] is None:
                candidate = next(
                    (
                        p["value"]
                        for p in public["generated_parameters"]
                        if p["name"] == name
                    ),
                    None,
                )
                if candidate and candidate != "<redacted>":
                    result["artifacts"][ref] = {
                        "path": candidate,
                        "origin": "current" if current else "anticipated",
                    }
    return record


def _paths(version: StateVersion, scope: str, depth: int | None) -> tuple[str, ...]:
    version.node(scope)
    level = 0 if scope == "/" else scope.count("/")
    prefix = scope.rstrip("/") + "/"
    return tuple(
        row.path
        for row in version.iter_nodes()
        if (row.path == scope or scope == "/" or row.path.startswith(prefix))
        and (
            depth is None
            or (0 if row.path == "/" else row.path.count("/")) - level <= depth
        )
    )


def _page_target(version: StateVersion, paths: tuple[str, ...]) -> int:
    """Use smaller pages when public records are compact enough to stay within
    the 100k standard-tree request budget. Wide records keep full-size pages.
    """
    sizes = sorted(len(version.node(path).public or b"") for path in paths[:64])
    if sizes and sizes[len(sizes) // 2] <= COMPACT_PUBLIC_MEDIAN_BYTES:
        return COMPACT_PAGE_TARGET_BYTES
    return PAGE_TARGET_BYTES


def _pointer_value(value: Any, pointer: str):
    for part in pointer.split("/")[1:]:
        part = part.replace("~1", "/").replace("~0", "~")
        value = value[int(part)] if isinstance(value, list) else value[part]
    return value


@dataclass
class _Lease:
    owner: str
    snapshot_id: str
    selection_hash: str
    version: StateVersion
    scope: str
    depth: int | None
    groups: tuple[str, ...]
    paths: tuple[str, ...]
    created: float
    used: float
    reserved_bytes: int
    page_target_bytes: int = PAGE_TARGET_BYTES
    next_offset: int = 0
    next_page: int = 0
    total_bytes: int = 0
    payload_bytes: int = 0
    digest: Any = field(default_factory=hashlib.sha256)
    last_page: FullSnapshotPage | None = None
    blobs: dict[str, tuple[str, str, int, str]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    expiry_handle: asyncio.TimerHandle | None = None


class FullSnapshotService:
    """Retain one immutable root and emit bounded, digest-checked pages."""

    def __init__(self, query_service):
        self.parent = query_service
        self._leases: dict[str, _Lease] = {}
        self._lock = asyncio.Lock()
        self._workers = asyncio.Semaphore(MAX_WORKERS)
        self._waiting = 0

    def _purge(self):
        now = time.monotonic()
        for key, lease in list(self._leases.items()):
            if (
                now - lease.used >= IDLE_SECONDS
                or now - lease.created >= ABSOLUTE_SECONDS
            ):
                self._drop(key)
            else:
                self._schedule_expiry(lease)

    def _drop(self, key: str) -> None:
        lease = self._leases.pop(key, None)
        if lease is not None and lease.expiry_handle is not None:
            lease.expiry_handle.cancel()

    def _schedule_expiry(self, lease: _Lease) -> None:
        if lease.expiry_handle is not None:
            lease.expiry_handle.cancel()
        deadline = min(lease.used + IDLE_SECONDS, lease.created + ABSOLUTE_SECONDS)
        lease.expiry_handle = asyncio.get_running_loop().call_later(
            max(0.001, deadline - time.monotonic()), self._purge
        )

    def _selection_hash(self, owner: str, request: FullSnapshotRequest) -> str:
        selection = [owner, request.scope_path, request.depth, request.field_groups]
        return hashlib.sha256(
            json.dumps(selection, separators=(",", ":")).encode()
        ).hexdigest()

    def _cursor(self, lease: _Lease, page: int) -> str:
        data = f"{lease.snapshot_id}.{page}".encode()
        mac = hmac.new(
            self.parent._key, data + lease.owner.encode(), hashlib.sha256
        ).digest()
        return base64.urlsafe_b64encode(data + b"." + mac).decode().rstrip("=")

    def _check_cursor(self, lease: _Lease, cursor: str, page: int) -> bool:
        return hmac.compare_digest(cursor, self._cursor(lease, page))

    async def _bounded(self, fn, *args):
        async with self._lock:
            if self._waiting >= MAX_WORKERS + MAX_WAITING:
                raise MemoryError("snapshot worker queue is full")
            self._waiting += 1
        try:
            async with self._workers:
                task = asyncio.create_task(asyncio.to_thread(fn, *args))
                try:
                    return await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task  # worker completion and cleanup precede cancellation
                    raise
        finally:
            async with self._lock:
                self._waiting -= 1

    async def page(self, request: FullSnapshotRequest):
        if set(request.field_groups) - GROUPS:
            return _error("unsupported_capability", "unknown full snapshot field group")
        owner = self.parent._owner()
        selection_hash = self._selection_hash(owner, request)
        if request.full_snapshot_id is None:
            try:
                version = self.parent.scheduler.version_store.pin()
            except VersionUnavailable as exc:
                return _error("resource_exhausted", str(exc))
            reserve = version.retained_bytes + 4 * 1024 * 1024
            async with self._lock:
                self._purge()
                owned = [v for v in self._leases.values() if v.owner == owner]
                if len(owned) >= MAX_OWNER_LEASES or len(self._leases) >= MAX_LEASES:
                    candidates = [v for v in self._leases.values() if v.next_page == 0]
                    if candidates:
                        self._drop(min(candidates, key=lambda v: v.used).snapshot_id)
                if (
                    sum(v.owner == owner for v in self._leases.values())
                    >= MAX_OWNER_LEASES
                    or len(self._leases) >= MAX_LEASES
                    or reserve > 768 * 1024 * 1024
                    or sum(v.reserved_bytes for v in self._leases.values()) + reserve
                    > MAX_RETAINED_BYTES
                ):
                    return _error("resource_exhausted", "full snapshot retention limit")
            try:
                paths = await self._bounded(
                    _paths, version, request.scope_path, request.depth
                )
            except KeyError:
                return _error("invalid_request", "scope unavailable")
            except MemoryError:
                return _error("resource_exhausted", "full snapshot worker limit")
            now = time.monotonic()
            lease = _Lease(
                owner,
                secrets.token_urlsafe(24),
                selection_hash,
                version,
                request.scope_path,
                request.depth,
                tuple(request.field_groups),
                paths,
                now,
                now,
                reserve,
                _page_target(version, paths),
            )
            async with self._lock:
                self._purge()
                if (
                    sum(v.owner == owner for v in self._leases.values())
                    >= MAX_OWNER_LEASES
                    or len(self._leases) >= MAX_LEASES
                    or sum(v.reserved_bytes for v in self._leases.values()) + reserve
                    > MAX_RETAINED_BYTES
                ):
                    return _error("resource_exhausted", "full snapshot retention limit")
                self._leases[lease.snapshot_id] = lease
                self._schedule_expiry(lease)
        else:
            async with self._lock:
                self._purge()
                lease = self._leases.get(request.full_snapshot_id)
                if lease is None:
                    return _error("snapshot_expired", "full snapshot unavailable")
                if lease.owner != owner or lease.selection_hash != selection_hash:
                    return _error(
                        "permission_denied", "full snapshot selection changed"
                    )
        async with lease.lock:
            if self._leases.get(lease.snapshot_id) is not lease:
                return _error("snapshot_expired", "full snapshot unavailable")
            if request.full_snapshot_id is not None:
                index = lease.next_page
                if not self._check_cursor(lease, request.cursor, index):
                    if lease.last_page is not None and self._check_cursor(
                        lease, request.cursor, index - 1
                    ):
                        return lease.last_page
                    return _error("invalid_request", "full snapshot cursor mismatch")
            try:
                page = await self._bounded(self._render_page, lease)
            except asyncio.CancelledError:
                async with self._lock:
                    self._drop(lease.snapshot_id)
                raise
            except VersionUnavailable as exc:
                async with self._lock:
                    self._drop(lease.snapshot_id)
                code = (
                    "unsupported_capability"
                    if str(exc) == "unsupported_plugin_detail"
                    else "resource_exhausted"
                )
                return _error(code, str(exc))
            except (ValueError, KeyError, TypeError, OverflowError) as exc:
                async with self._lock:
                    self._drop(lease.snapshot_id)
                return _error("field_too_large", str(exc))
            except MemoryError:
                async with self._lock:
                    self._drop(lease.snapshot_id)
                return _error("resource_exhausted", "full snapshot budget exceeded")
            except Exception:
                async with self._lock:
                    self._drop(lease.snapshot_id)
                return _error("internal_error", "full snapshot worker failed")
            async with self._lock:
                if self._leases.get(lease.snapshot_id) is not lease:
                    return _error("snapshot_expired", "full snapshot cancelled")
                lease.last_page = page
                lease.used = time.monotonic()
                self._schedule_expiry(lease)
                return page

    def _render_page(self, lease: _Lease) -> FullSnapshotPage:
        version = lease.version
        nodes: list[list] = []
        page_bytes = 0
        digest = lease.digest.copy()
        total_bytes = lease.total_bytes
        payload_bytes = lease.payload_bytes
        position = lease.next_offset
        context = (
            _ancestor_context(version, lease.scope) if lease.next_page == 0 else None
        )
        service = (
            _service_context(version)
            if lease.next_page == 0 and "service" in lease.groups
            else None
        )
        while position < len(lease.paths) and len(nodes) < 2048:
            path = lease.paths[position]
            record = _full_node(version, path, lease.groups)
            if context is not None and position == 0:
                record["ancestor_context"] = context
            if service is not None and position == 0:
                record["service"] = service
            source = version.node(path)
            needs_blob_scan = (
                len(source.public or b"") + len(source.definition) + len(source.runtime)
                > BLOB_THRESHOLD
                or "ancestor_context" in record
                or "service" in record
            )
            # The compact row contains every exported group value. If its
            # entire encoding is below the blob threshold, no group string
            # can require a chunk. Metadata is separate from that row, so
            # the first page still checks it below.
            if not needs_blob_scan:
                packed = pack_node(record, lease.groups)
                raw = _canonical(packed)
                needs_blob_scan = len(raw) > BLOB_THRESHOLD
            if needs_blob_scan:
                record = self._blobify(lease, record, BLOB_THRESHOLD)
                packed = pack_node(record, lease.groups)
                raw = _canonical(packed)
            candidate_context = record.pop("ancestor_context", None)
            candidate_service = record.pop("service", None)
            if len(raw) > lease.page_target_bytes - 4096:
                record = _full_node(version, path, lease.groups)
                if context is not None and position == 0:
                    record["ancestor_context"] = context
                if service is not None and position == 0:
                    record["service"] = service
                record = self._blobify(lease, record, 1024)
                candidate_context = record.pop("ancestor_context", None)
                candidate_service = record.pop("service", None)
                packed = pack_node(record, lease.groups)
                raw = _canonical(packed)
            if len(raw) > lease.page_target_bytes - 4096:
                raise ValueError(f"record exceeds page budget: {path}")
            if nodes and page_bytes + len(raw) > lease.page_target_bytes - 4096:
                break
            if lease.next_page == 0 and position == 0:
                metadata = _canonical(
                    {
                        "ancestor_context": candidate_context,
                        "service": candidate_service,
                    }
                )
                digest.update(metadata)
                digest.update(b"\n")
                total_bytes += len(metadata)
                payload_bytes += len(metadata)
            nodes.append(packed)
            if candidate_context is not None:
                context = candidate_context
            if candidate_service is not None:
                service = candidate_service
            page_bytes += len(raw)
            digest.update(raw)
            digest.update(b"\n")
            total_bytes += len(raw)
            payload_bytes += len(raw) + sum(
                ref["total_bytes"] for ref in record.get("blob_refs", ())
            )
            if payload_bytes > MAX_ENCODED_BYTES:
                raise MemoryError("full snapshot encoded budget")
            position += 1
        complete = position == len(lease.paths)
        observed = datetime.fromisoformat(version.as_of)
        as_of = _utc(observed.astimezone(timezone.utc))
        page = FullSnapshotPage(
            kind="full_snapshot_page",
            schema_version=1,
            session_id=self.parent.session_id,
            full_snapshot_id=lease.snapshot_id,
            state_version=version.state_version,
            as_of=as_of,
            scope_path=lease.scope,
            depth=lease.depth,
            field_groups=list(lease.groups),
            selection_hash=lease.selection_hash,
            encoding="compact_v1",
            page_index=lease.next_page,
            complete=complete,
            next_cursor=None if complete else self._cursor(lease, lease.next_page + 1),
            nodes=nodes,
            ancestor_context=context,
            service=service,
            total_nodes=len(lease.paths) if complete else None,
            total_bytes=total_bytes if complete else None,
            sha256=digest.hexdigest() if complete else None,
        )
        if len(encode_query_document(page)) > MAX_PAGE_BYTES:
            raise ValueError("page exceeds wire budget")
        lease.next_offset = position
        lease.next_page += 1
        lease.total_bytes = total_bytes
        lease.payload_bytes = payload_bytes
        lease.digest = digest
        # Large page streams create many short-lived container objects. A small
        # collection per page avoids rare whole-generation pauses on the event
        # loop while preserving normal GC for the rest of the process.
        if gc.isenabled():
            gc.collect(0)
        return page

    def _blobify(self, lease: _Lease, record: dict, threshold: int) -> dict:
        refs = []

        def visit(value, pointer):
            if isinstance(value, str):
                data = value.encode("utf-8")
                if len(data) > MAX_FIELD_BYTES:
                    raise ValueError(f"field exceeds limit: {record['path']} {pointer}")
                if len(data) <= threshold:
                    return value
                digest = hashlib.sha256(data).hexdigest()
                ident = hmac.new(
                    self.parent._key,
                    f"{lease.snapshot_id}:{record['path']}:{pointer}:{digest}".encode(),
                    hashlib.sha256,
                ).hexdigest()
                lease.blobs[ident] = (record["path"], pointer, len(data), digest)
                refs.append(
                    {
                        "pointer": pointer,
                        "blob_id": ident,
                        "total_bytes": len(data),
                        "sha256": digest,
                    }
                )
                return {"blob_id": ident}
            if isinstance(value, list):
                return [
                    visit(item, f"{pointer}/{index}")
                    for index, item in enumerate(value)
                ]
            if isinstance(value, dict):
                return {
                    key: visit(
                        item, f"{pointer}/{key.replace('~', '~0').replace('/', '~1')}"
                    )
                    for key, item in value.items()
                }
            return value

        record["groups"] = visit(record["groups"], "/groups")
        if "ancestor_context" in record:
            record["ancestor_context"] = visit(
                record["ancestor_context"], "/ancestor_context"
            )
        if "service" in record:
            record["service"] = visit(record["service"], "/service")
        if refs:
            record["blob_refs"] = refs
        return record

    async def chunk(self, request: FullSnapshotChunkRequest):
        owner = self.parent._owner()
        async with self._lock:
            self._purge()
            lease = self._leases.get(request.full_snapshot_id)
            if lease is None or request.session_id != self.parent.session_id:
                return _error("snapshot_expired", "full snapshot unavailable")
            if lease.owner != owner:
                return _error("permission_denied", "full snapshot owner changed")
            ref = lease.blobs.get(request.blob_id)
            if ref is None:
                return _error("invalid_request", "unknown full snapshot blob")
        path, pointer, size, digest = ref
        if request.offset >= size and size:
            return _error("invalid_request", "chunk offset out of range")
        try:
            if pointer.startswith("/ancestor_context/"):
                context = await self._bounded(
                    _ancestor_context, lease.version, lease.scope
                )
                record = {"ancestor_context": context}
            elif pointer.startswith("/service/"):
                service = await self._bounded(_service_context, lease.version)
                record = {"service": service}
            else:
                record = await self._bounded(
                    _full_node, lease.version, path, lease.groups
                )
            data = _pointer_value(record, pointer).encode("utf-8")
        except MemoryError:
            return _error("resource_exhausted", "full snapshot worker limit")
        except VersionUnavailable:
            return _error("snapshot_expired", "fixed blob unavailable")
        except (KeyError, TypeError, ValueError):
            return _error("internal_error", "fixed blob unavailable")
        if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
            return _error("internal_error", "fixed blob changed")
        part = data[request.offset : request.offset + CHUNK_BYTES]
        async with self._lock:
            self._purge()
            if self._leases.get(lease.snapshot_id) is not lease:
                return _error("snapshot_expired", "full snapshot unavailable")
            lease.used = time.monotonic()
            self._schedule_expiry(lease)
        return FullSnapshotChunk(
            kind="full_snapshot_chunk",
            schema_version=1,
            session_id=self.parent.session_id,
            full_snapshot_id=lease.snapshot_id,
            state_version=lease.version.state_version,
            as_of=_utc(
                datetime.fromisoformat(lease.version.as_of).astimezone(timezone.utc)
            ),
            blob_id=request.blob_id,
            offset=request.offset,
            total_bytes=size,
            sha256=digest,
            data_base64=base64.b64encode(part).decode(),
            complete=request.offset + len(part) == size,
        )

    async def cancel(self, request: CancelFullSnapshotRequest):
        owner = self.parent._owner()
        async with self._lock:
            lease = self._leases.get(request.full_snapshot_id)
            cancelled = bool(
                lease
                and lease.owner == owner
                and request.session_id == self.parent.session_id
            )
            if cancelled:
                self._drop(request.full_snapshot_id)
        return CancelFullSnapshotReply(
            kind="cancel_full_snapshot_reply", schema_version=1, cancelled=cancelled
        )
