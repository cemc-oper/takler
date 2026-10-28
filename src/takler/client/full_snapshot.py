"""Validated, bounded full snapshot assembly and streaming."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Callable, Iterator

from takler.exceptions import QueryResponseError, ServerResponseError
from takler.protocol.query_v1 import (
    CancelFullSnapshotRequest,
    CapabilitiesReply,
    CapabilitiesRequest,
    FailureReply,
    FullSnapshotChunk,
    FullSnapshotChunkRequest,
    FullSnapshotPage,
    FullSnapshotRequest,
    FULL_DEFAULT_GROUPS,
    valid_path,
)
from takler.protocol.full_snapshot_codec import unpack_node

MAX_ASSEMBLED_BYTES = 640 * 1024 * 1024
MAX_STREAM_PAGE_BYTES = 262_144


def _canonical(record: dict) -> bytes:
    return json.dumps(
        record,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def _parent(value, pointer: str):
    parts = pointer.split("/")[1:]
    if not parts or parts[0] not in {"groups", "ancestor_context", "service"}:
        raise ServerResponseError("invalid full snapshot blob pointer")
    for part in parts[:-1]:
        key = part.replace("~1", "/").replace("~0", "~")
        value = value[int(key)] if isinstance(value, list) else value[key]
    key = parts[-1].replace("~1", "/").replace("~0", "~")
    return value, int(key) if isinstance(value, list) else key


@dataclass(frozen=True)
class FullSnapshotComplete:
    session_id: str
    full_snapshot_id: str
    state_version: int
    as_of: str
    scope_path: str
    total_nodes: int
    total_bytes: int
    sha256: str
    ancestor_context: list[dict]
    service: dict | None


@dataclass(frozen=True)
class FullSnapshot:
    completion: FullSnapshotComplete
    nodes_by_path: dict[str, dict]


class FullSnapshotClient:
    def __init__(self, transport):
        self.transport = transport

    @staticmethod
    def _reply(value, expected):
        if isinstance(value, FailureReply):
            raise QueryResponseError(value.code, value.message, value.reset_scope)
        if not isinstance(value, expected):
            raise ServerResponseError("invalid full snapshot response")
        return value

    def _blob(self, page: FullSnapshotPage, ref: dict, deadline: float) -> str:
        total = ref["total_bytes"]
        if type(total) is not int or total <= 0 or total > 16 * 1024 * 1024:
            raise ServerResponseError("full snapshot blob exceeds field budget")
        data = bytearray()
        while len(data) < total:
            if time.monotonic() > deadline:
                raise QueryResponseError(
                    "resource_exhausted", "full snapshot deadline exceeded"
                )
            reply = self._reply(
                self.transport.query(
                    FullSnapshotChunkRequest(
                        kind="full_snapshot_chunk_request",
                        schema_version=1,
                        session_id=page.session_id,
                        full_snapshot_id=page.full_snapshot_id,
                        blob_id=ref["blob_id"],
                        offset=len(data),
                    )
                ),
                FullSnapshotChunk,
            )
            if (
                reply.session_id != page.session_id
                or reply.full_snapshot_id != page.full_snapshot_id
                or reply.state_version != page.state_version
                or reply.as_of != page.as_of
                or reply.blob_id != ref["blob_id"]
                or reply.offset != len(data)
                or reply.total_bytes != total
                or reply.sha256 != ref["sha256"]
            ):
                raise ServerResponseError("full snapshot chunk identity changed")
            try:
                part = base64.b64decode(reply.data_base64, validate=True)
            except (ValueError, binascii.Error):
                raise ServerResponseError(
                    "invalid full snapshot chunk encoding"
                ) from None
            if not part or len(part) > 65536 or len(data) + len(part) > total:
                raise ServerResponseError("invalid full snapshot chunk length")
            data.extend(part)
            if reply.complete != (len(data) == total):
                raise ServerResponseError("full snapshot chunk completion mismatch")
        if hashlib.sha256(data).hexdigest() != ref["sha256"]:
            raise ServerResponseError("full snapshot blob digest mismatch")
        try:
            return data.decode("utf-8")
        except UnicodeError:
            raise ServerResponseError("invalid full snapshot UTF-8 blob") from None

    def iter_full_snapshot(
        self,
        *,
        scope_path: str = "/",
        depth: int | None = None,
        field_groups: list[str] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> Iterator[dict | FullSnapshotComplete]:
        valid_path(scope_path)
        groups = field_groups or list(FULL_DEFAULT_GROUPS)
        capabilities = self._reply(
            self.transport.query(
                CapabilitiesRequest(
                    kind="capabilities_request",
                    schema_version=1,
                )
            ),
            CapabilitiesReply,
        )
        if not capabilities.full_snapshot_v1:
            raise QueryResponseError(
                "unsupported_capability", "full snapshot unavailable"
            )
        request = FullSnapshotRequest(
            kind="full_snapshot_request",
            schema_version=1,
            scope_path=scope_path,
            depth=depth,
            field_groups=groups,
        )
        snapshot_id = None
        session_id = None
        identity = None
        digest = hashlib.sha256()
        total_bytes = 0
        payload_bytes = 0
        count = 0
        seen = set()
        context = None
        service = None
        deadline = time.monotonic() + 900
        try:
            for index in range(1_000_000):
                if time.monotonic() > deadline:
                    raise QueryResponseError(
                        "resource_exhausted", "full snapshot deadline exceeded"
                    )
                if cancelled and cancelled():
                    raise QueryResponseError(
                        "resource_exhausted", "full snapshot cancelled"
                    )
                page = self._reply(self.transport.query(request), FullSnapshotPage)
                if page.page_index != index:
                    raise ServerResponseError("full snapshot page order changed")
                current = (
                    page.session_id,
                    page.full_snapshot_id,
                    page.state_version,
                    page.as_of,
                    page.scope_path,
                    page.depth,
                    tuple(page.field_groups),
                    page.selection_hash,
                )
                if identity is None:
                    identity = current
                    session_id, snapshot_id = current[:2]
                    if (
                        page.scope_path != scope_path
                        or page.depth != depth
                        or page.field_groups != groups
                    ):
                        raise ServerResponseError("full snapshot selection changed")
                    context = page.ancestor_context
                    service = page.service
                    if context is None:
                        raise ServerResponseError(
                            "full snapshot ancestor context missing"
                        )
                    metadata = _canonical(
                        {"ancestor_context": context, "service": service}
                    )
                    digest.update(metadata)
                    digest.update(b"\n")
                    total_bytes += len(metadata)
                    payload_bytes += len(metadata)
                    if payload_bytes > 512 * 1024 * 1024:
                        raise QueryResponseError(
                            "resource_exhausted", "full snapshot encoded budget"
                        )
                elif (
                    current != identity
                    or page.ancestor_context is not None
                    or page.service is not None
                ):
                    raise ServerResponseError("full snapshot identity changed")
                for record in page.nodes:
                    if time.monotonic() > deadline:
                        raise QueryResponseError(
                            "resource_exhausted", "full snapshot deadline exceeded"
                        )
                    raw = _canonical(record)
                    total_bytes += len(raw)
                    payload_bytes += len(raw)
                    if payload_bytes > 512 * 1024 * 1024:
                        raise QueryResponseError(
                            "resource_exhausted", "full snapshot encoded budget"
                        )
                    digest.update(raw)
                    digest.update(b"\n")
                    try:
                        record = unpack_node(record, page.field_groups)
                    except (KeyError, IndexError, TypeError, ValueError):
                        raise ServerResponseError("invalid full snapshot row") from None
                    path = record["path"]
                    if path in seen:
                        raise ServerResponseError("duplicate full snapshot path")
                    seen.add(path)
                    if count == 0 and path != scope_path:
                        raise ServerResponseError("full snapshot scope root is missing")
                    if (
                        scope_path != "/"
                        and path != scope_path
                        and not path.startswith(scope_path + "/")
                    ):
                        raise ServerResponseError("full snapshot path outside scope")
                    parent = record.get("parent_path")
                    expected_parent = (
                        None if path == "/" else path.rsplit("/", 1)[0] or "/"
                    )
                    if parent != expected_parent or (
                        path != scope_path and parent not in seen
                    ):
                        raise ServerResponseError("full snapshot parent mismatch")
                    path_level = 0 if path == "/" else path.count("/")
                    scope_level = 0 if scope_path == "/" else scope_path.count("/")
                    if depth is not None and path_level - scope_level > depth:
                        raise ServerResponseError("full snapshot path outside depth")
                    for ref in record.get("blob_refs", []):
                        size = ref.get("total_bytes")
                        if type(size) is not int or size <= 0:
                            raise ServerResponseError(
                                "invalid full snapshot blob length"
                            )
                        payload_bytes += size
                        if payload_bytes > 512 * 1024 * 1024:
                            raise QueryResponseError(
                                "resource_exhausted", "full snapshot encoded budget"
                            )
                        parent, key = _parent(
                            {
                                "groups": record["groups"],
                                "ancestor_context": context,
                                "service": service,
                            },
                            ref["pointer"],
                        )
                        if parent[key] != {"blob_id": ref["blob_id"]}:
                            raise ServerResponseError(
                                "full snapshot blob reference mismatch"
                            )
                        parent[key] = self._blob(page, ref, deadline)
                    count += 1
                    yield record
                if page.complete:
                    if time.monotonic() > deadline:
                        raise QueryResponseError(
                            "resource_exhausted", "full snapshot deadline exceeded"
                        )
                    if (
                        page.total_nodes != count
                        or page.total_bytes != total_bytes
                        or page.sha256 != digest.hexdigest()
                    ):
                        raise ServerResponseError("full snapshot completion mismatch")
                    yield FullSnapshotComplete(
                        page.session_id,
                        page.full_snapshot_id,
                        page.state_version,
                        page.as_of,
                        page.scope_path,
                        count,
                        total_bytes,
                        page.sha256,
                        context,
                        service,
                    )
                    return
                if not page.next_cursor or not page.nodes:
                    raise ServerResponseError("full snapshot has no progress")
                request = request.model_copy(
                    update={
                        "full_snapshot_id": snapshot_id,
                        "cursor": page.next_cursor,
                    }
                )
            raise ServerResponseError("full snapshot page limit exceeded")
        finally:
            if snapshot_id and session_id:
                try:
                    self.transport.query(
                        CancelFullSnapshotRequest(
                            kind="cancel_full_snapshot_request",
                            schema_version=1,
                            session_id=session_id,
                            full_snapshot_id=snapshot_id,
                        )
                    )
                except Exception:
                    pass

    def read_full_snapshot(self, **selection) -> FullSnapshot:
        nodes = {}
        used = 0
        for item in self.iter_full_snapshot(**selection):
            if isinstance(item, FullSnapshotComplete):
                return FullSnapshot(item, nodes)
            size = len(_canonical(item))
            # The retained mapping, decoded strings, and JSON scratch storage
            # cost substantially more than the compact wire representation.
            used += size * 3
            if used > MAX_ASSEMBLED_BYTES:
                raise QueryResponseError(
                    "resource_exhausted",
                    "full snapshot assembly budget; use iter_full_snapshot",
                )
            nodes[item["path"]] = item
        raise ServerResponseError("full snapshot ended without completion")
