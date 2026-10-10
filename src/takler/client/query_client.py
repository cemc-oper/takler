"""Atomic, bounded QueryDocument v1 reads for the Python client."""

from __future__ import annotations

import base64
import binascii
import hashlib
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Callable, Mapping

from takler.exceptions import QueryResponseError, ServerResponseError, TransportError
from takler.protocol.query_v1 import (
    CapabilitiesReply,
    CapabilitiesRequest,
    ChunkReply,
    ChunkRequest,
    DETAIL_GROUPS,
    DetailGroups,
    DetailReply,
    DetailRequest,
    FailureReply,
    InvalidateScope,
    PageReply,
    PageRequest,
    RemoveSubtree,
    ResetReply,
    SinceReply,
    SinceRequest,
    SummaryNode,
    UpsertSummary,
    valid_path,
)
from takler.protocol.wire import WireError, strict_json

MAX_CLIENT_VIEW_BYTES = 128 * 1024 * 1024
MAX_BLOB_FIELD_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class QueryNode:
    path: str
    node_kind: str
    type_id: str
    status: str
    suspended: bool
    attempt_id: str | None
    submission_unknown: bool | None = None

    @classmethod
    def from_wire(cls, node: SummaryNode) -> "QueryNode":
        return cls(
            node.path,
            node.node_kind,
            node.type_id,
            node.status,
            node.suspended,
            node.attempt_id,
            node.submission_unknown,
        )


@dataclass(frozen=True, slots=True)
class QuerySnapshot:
    session_id: str
    query_id: str
    snapshot_id: str
    as_of: str
    expires_at: str
    root_name: str
    scope_path: str
    depth: int | None
    nodes: Mapping[str, QueryNode]
    ordered_paths: tuple[str, ...]
    flow_generations: Mapping[str, str]
    estimated_bytes: int
    revision: int | None = None
    stale_scopes: tuple[str, ...] = ()
    changed_paths: tuple[str, ...] = ()
    removed_paths: tuple[str, ...] = ()

    def get(self, path: str) -> QueryNode | None:
        return self.nodes.get(path)

    def is_stale(self, path: str | None = None) -> bool:
        path = self.scope_path if path is None else path
        return any(
            path == scope or path.startswith(scope.rstrip("/") + "/")
            for scope in self.stale_scopes
        )


class _NodeOverlay(Mapping[str, QueryNode]):
    """Share an initial view while copying only changed summary entries."""

    def __init__(
        self,
        base: Mapping[str, QueryNode],
        changes: Mapping[str, QueryNode],
        base_size: int,
    ):
        self.base = base
        self.changes = MappingProxyType(dict(changes))
        self.base_size = base_size

    def __getitem__(self, key: str) -> QueryNode:
        return self.changes[key] if key in self.changes else self.base[key]

    def __iter__(self):
        return iter(self.base)

    def __len__(self) -> int:
        return len(self.base)


class QueryClient:
    """Read an entire summary before replacing the previously visible view."""

    def __init__(self, transport):
        self.transport = transport
        self.current: QuerySnapshot | None = None

    def capabilities(self) -> CapabilitiesReply:
        reply = self.transport.query(
            CapabilitiesRequest(kind="capabilities_request", schema_version=1)
        )
        self._failure(reply)
        if not isinstance(reply, CapabilitiesReply):
            raise ServerResponseError("invalid query capabilities response")
        return reply

    @staticmethod
    def _failure(reply) -> None:
        if isinstance(reply, FailureReply):
            raise QueryResponseError(reply.code, reply.message, reply.reset_scope)

    def read_snapshot(
        self,
        *,
        scope_path: str | None = None,
        flow_name: str | None = None,
        depth: int | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> QuerySnapshot:
        try:
            request = PageRequest(
                kind="page_request",
                schema_version=1,
                scope_path=scope_path,
                flow_name=flow_name,
                depth=depth,
                field_groups=["summary"],
            )
        except ValueError:
            raise QueryResponseError(
                "invalid_request", "invalid query selection"
            ) from None
        scope = scope_path or ("/" + flow_name if flow_name else "/")
        previous = self.current
        old_size = previous.estimated_bytes if previous else 0
        nodes: dict[str, QueryNode] = {}
        order: list[str] = []
        generations: dict[str, str] = {}
        size = 1024
        identity = None
        expected_total = None
        index = 0
        while True:
            if cancelled is not None and cancelled():
                raise QueryResponseError("cancelled", "superseded query")
            reply = self.transport.query(request)
            self._failure(reply)
            if not isinstance(reply, PageReply):
                raise ServerResponseError("invalid query page response")
            if reply.page_index != index:
                raise ServerResponseError("query page order changed")
            actual = (
                reply.session_id,
                reply.query_id,
                reply.snapshot_id,
                reply.base_revision,
                reply.as_of,
                reply.expires_at,
                reply.root_name,
            )
            if identity is None:
                identity = actual
            elif actual != identity:
                raise ServerResponseError("query snapshot identity changed")
            if reply.total_nodes is not None:
                if expected_total is None:
                    expected_total = reply.total_nodes
                elif reply.total_nodes != expected_total:
                    raise ServerResponseError("query node count changed")
            for flow_path, generation in reply.flow_generations.items():
                earlier = generations.setdefault(flow_path, generation)
                if earlier != generation:
                    raise ServerResponseError("query flow identity changed")
            for wire_node in reply.nodes:
                row = QueryNode.from_wire(wire_node)
                path = row.path
                if path in nodes:
                    raise ServerResponseError("duplicate query node")
                if path != scope and not path.startswith(scope.rstrip("/") + "/"):
                    raise ServerResponseError("query node outside requested scope")
                relative_depth = (path.count("/") if path != "/" else 0) - (
                    scope.count("/") if scope != "/" else 0
                )
                if depth is not None and relative_depth > depth:
                    raise ServerResponseError("query node outside requested depth")
                parent = path.rsplit("/", 1)[0] or "/"
                if path != scope and parent not in nodes:
                    raise ServerResponseError("query parent is missing")
                nodes[path] = row
                order.append(path)
                size += 512 + sum(
                    len(value.encode())
                    for value in (
                        row.path,
                        row.node_kind,
                        row.type_id,
                        row.status,
                        row.attempt_id or "",
                    )
                )
                if old_size + size > MAX_CLIENT_VIEW_BYTES:
                    raise QueryResponseError(
                        "resource_exhausted", "query view memory budget exceeded"
                    )
            if reply.complete:
                break
            request = request.model_copy(update={"cursor": reply.next_cursor})
            index += 1
        if not order or order[0] != scope:
            raise ServerResponseError("query scope root is missing")
        if expected_total is not None and len(order) != expected_total:
            raise ServerResponseError("query node count mismatch")
        if cancelled is not None and cancelled():
            raise QueryResponseError("cancelled", "superseded query")
        if self.current is not previous:
            raise QueryResponseError("cancelled", "superseded query")
        snapshot = QuerySnapshot(
            session_id=identity[0],
            query_id=identity[1],
            snapshot_id=identity[2],
            as_of=identity[4],
            expires_at=identity[5],
            root_name=identity[6],
            scope_path=scope,
            depth=depth,
            nodes=MappingProxyType(nodes),
            ordered_paths=tuple(order),
            flow_generations=MappingProxyType(generations),
            estimated_bytes=size,
            revision=identity[3],
        )
        self.current = snapshot
        return snapshot

    def read_since(
        self,
        snapshot: QuerySnapshot | None = None,
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> QuerySnapshot:
        """Apply a complete delta batch to a private view, then publish it."""
        previous = self.current
        if snapshot is None:
            snapshot = previous
        if snapshot is None or snapshot is not previous:
            raise QueryResponseError("stale_identity", "query view is not current")
        if snapshot.revision is None:
            raise QueryResponseError("unsupported_capability", "query has no revision")
        if snapshot.is_stale(snapshot.scope_path):
            raise QueryResponseError("stale_identity", "query root needs rebuilding")
        try:
            capability = self.capabilities()
        except TransportError:
            raise
        except Exception:
            self._mark_stale(snapshot, snapshot.scope_path)
            raise
        if not capability.since:
            self._mark_stale(snapshot, snapshot.scope_path)
            raise QueryResponseError("unsupported_capability", "server has no since")

        request = SinceRequest(
            kind="since_request",
            schema_version=1,
            scope_path=snapshot.scope_path,
            depth=snapshot.depth,
            field_groups=["summary"],
            session_id=snapshot.session_id,
            query_id=snapshot.query_id,
            since_revision=snapshot.revision,
        )
        identity = None
        changes: dict[str, QueryNode] = {}
        removed: set[str] = set()
        stale = set(snapshot.stale_scopes)
        seen: dict[tuple[str, str], object] = {}
        index = 0
        previous_page: SinceReply | None = None
        repeated_pages = 0
        while True:
            if cancelled is not None and cancelled():
                raise QueryResponseError("cancelled", "superseded query")
            try:
                reply = self.transport.query(request)
                self._failure(reply)
                if isinstance(reply, ResetReply):
                    if (
                        reply.session_id != snapshot.session_id
                        or reply.query_id != snapshot.query_id
                        or reply.since_revision != snapshot.revision
                    ):
                        raise ServerResponseError("query reset identity changed")
                    self._mark_stale(snapshot, reply.reset_scope)
                    raise QueryResponseError(
                        "reset_required", reply.reason, reply.reset_scope
                    )
                if not isinstance(reply, SinceReply):
                    raise ServerResponseError("invalid query since response")
                if previous_page is not None and reply.page_index == index - 1:
                    repeated_pages += 1
                    if reply != previous_page or repeated_pages > 2:
                        raise ServerResponseError("conflicting query delta replay")
                    continue
                repeated_pages = 0
                actual = (
                    reply.session_id,
                    reply.query_id,
                    reply.batch_id,
                    reply.from_revision,
                    reply.target_revision,
                    reply.as_of,
                    reply.expires_at,
                )
                if (
                    reply.session_id != snapshot.session_id
                    or reply.query_id != snapshot.query_id
                    or reply.from_revision != snapshot.revision
                    or reply.page_index != index
                ):
                    raise ServerResponseError(
                        "query delta identity or page order changed"
                    )
                if identity is None:
                    identity = actual
                elif actual != identity:
                    raise ServerResponseError("query delta batch identity changed")
                for operation in reply.operations:
                    key = (operation.op, operation.path)
                    if key in seen:
                        raise ServerResponseError("duplicate query delta operation")
                    seen[key] = operation
                    if isinstance(operation, UpsertSummary):
                        path = operation.path
                        if path not in snapshot.nodes:
                            raise ServerResponseError(
                                "query delta added an unknown node"
                            )
                        if not self._in_view(snapshot, path):
                            raise ServerResponseError("query delta node outside view")
                        flow = "/" + path.split("/", 2)[1] if path != "/" else None
                        if (
                            flow
                            and operation.flow_generation
                            != snapshot.flow_generations.get(flow)
                        ):
                            raise ServerResponseError(
                                "query delta flow identity changed"
                            )
                        changes[path] = QueryNode.from_wire(operation.node)
                    elif isinstance(operation, InvalidateScope):
                        if not self._in_view(snapshot, operation.path):
                            raise ServerResponseError("query invalidation outside view")
                        stale.add(operation.path)
                    elif isinstance(operation, RemoveSubtree):
                        if not self._in_view(snapshot, operation.path):
                            raise ServerResponseError("query removal outside view")
                        flow = "/" + operation.path.split("/", 2)[1]
                        if operation.flow_generation != snapshot.flow_generations.get(
                            flow
                        ):
                            raise ServerResponseError(
                                "query removal flow identity changed"
                            )
                        removed.add(operation.path)
                    else:
                        raise ServerResponseError(
                            "unexpected detail delta in summary view"
                        )
                if reply.complete:
                    break
                previous_page = reply
                request = request.model_copy(update={"cursor": reply.next_cursor})
                index += 1
            except TransportError:
                # The batch was never committed. Retry from the old watermark;
                # a changed session or expired window will explicitly reset it.
                raise
            except Exception:
                if self.current is snapshot:
                    self._mark_stale(snapshot, snapshot.scope_path)
                raise
        if self.current is not snapshot:
            raise QueryResponseError("cancelled", "superseded query")
        if cancelled is not None and cancelled():
            raise QueryResponseError("cancelled", "superseded query")
        if identity[4] == snapshot.revision:
            return snapshot
        base = (
            snapshot.nodes.base
            if isinstance(snapshot.nodes, _NodeOverlay)
            else snapshot.nodes
        )
        base_size = (
            snapshot.nodes.base_size
            if isinstance(snapshot.nodes, _NodeOverlay)
            else snapshot.estimated_bytes
        )
        updates = (
            dict(snapshot.nodes.changes)
            if isinstance(snapshot.nodes, _NodeOverlay)
            else {}
        )
        updates.update(changes)
        order = snapshot.ordered_paths
        generations = snapshot.flow_generations
        if removed:
            if snapshot.estimated_bytes * 2 > MAX_CLIENT_VIEW_BYTES:
                self._mark_stale(snapshot, snapshot.scope_path)
                raise QueryResponseError(
                    "resource_exhausted", "query view memory budget exceeded"
                )
            removed_paths = {
                path
                for path in order
                if any(path == root or path.startswith(root + "/") for root in removed)
            }
            if snapshot.scope_path in removed_paths or any(
                path in removed_paths for path in changes
            ):
                self._mark_stale(snapshot, snapshot.scope_path)
                raise ServerResponseError("query delta conflicts with subtree removal")
            order = tuple(path for path in order if path not in removed_paths)
            base = MappingProxyType({path: snapshot.nodes[path] for path in order})
            base_size = 1024 + sum(512 + len(path.encode()) for path in order)
            updates = changes
            generations = MappingProxyType(
                {
                    path: generation
                    for path, generation in generations.items()
                    if path not in removed_paths
                }
            )
            stale = {path for path in stale if path not in removed_paths}
        extra = sum(512 + len(path.encode()) for path in updates)
        size = base_size + extra
        staging_size = snapshot.estimated_bytes + size if removed else size + extra
        if staging_size > MAX_CLIENT_VIEW_BYTES:
            self._mark_stale(snapshot, snapshot.scope_path)
            raise QueryResponseError(
                "resource_exhausted", "query view memory budget exceeded"
            )
        result = replace(
            snapshot,
            nodes=_NodeOverlay(base, updates, base_size) if updates else base,
            ordered_paths=order,
            flow_generations=generations,
            as_of=identity[5],
            revision=identity[4],
            stale_scopes=tuple(sorted(stale)),
            estimated_bytes=size,
            changed_paths=tuple(changes),
            removed_paths=tuple(removed_paths) if removed else (),
        )
        self.current = result
        return result

    @staticmethod
    def _in_view(snapshot: QuerySnapshot, path: str) -> bool:
        if path != snapshot.scope_path and not path.startswith(
            snapshot.scope_path.rstrip("/") + "/"
        ):
            return False
        relative_depth = path.count("/") - (
            0 if snapshot.scope_path == "/" else snapshot.scope_path.count("/")
        )
        return snapshot.depth is None or relative_depth <= snapshot.depth

    def _mark_stale(self, snapshot: QuerySnapshot, scope: str) -> None:
        if self.current is snapshot:
            self.current = replace(
                snapshot,
                stale_scopes=tuple(sorted(set(snapshot.stale_scopes) | {scope})),
            )

    def read_detail(
        self, snapshot: QuerySnapshot, path: str, field_groups: list[str]
    ) -> DetailReply:
        try:
            valid_path(path)
        except ValueError:
            raise QueryResponseError("invalid_request", "invalid detail path") from None
        row = snapshot.get(path)
        if row is None:
            raise QueryResponseError("invalid_request", "path is not in the query view")
        if snapshot.is_stale(path):
            raise QueryResponseError(
                "stale_identity", "query scope needs rebuilding", path
            )
        if (
            not field_groups
            or len(set(field_groups)) != len(field_groups)
            or set(field_groups) - DETAIL_GROUPS
        ):
            raise QueryResponseError("invalid_request", "invalid detail groups")
        flow_path = "/" + path.split("/", 2)[1] if path != "/" else None
        generation = snapshot.flow_generations.get(flow_path) if flow_path else None
        request = DetailRequest(
            kind="detail_request",
            schema_version=1,
            session_id=snapshot.session_id,
            query_id=snapshot.query_id,
            path=path,
            flow_generation=generation,
            attempt_id=row.attempt_id,
            field_groups=field_groups,
        )
        reply = self.transport.query(request)
        if isinstance(reply, FailureReply) and reply.code == "snapshot_expired":
            self._mark_stale(snapshot, snapshot.scope_path)
        self._failure(reply)
        if not isinstance(reply, DetailReply):
            raise ServerResponseError("invalid query detail response")
        if (
            reply.session_id != snapshot.session_id
            or reply.query_id != snapshot.query_id
            or reply.path != path
            or reply.flow_generation != generation
            or reply.attempt_id != row.attempt_id
            or reply.requested_groups != field_groups
        ):
            raise ServerResponseError("query detail identity changed")
        if not reply.blob_refs:
            return reply
        groups = reply.groups.model_dump(mode="json", exclude_unset=True)
        seen = set()
        for ref in reply.blob_refs:
            if ref.pointer in seen:
                raise ServerResponseError("duplicate query blob pointer")
            seen.add(ref.pointer)
            target = self._pointer_parent(groups, ref.pointer)
            value = self._read_blob(ref)
            target[0][target[1]] = value
        try:
            resolved = DetailGroups.model_validate(groups)
        except ValueError:
            raise ServerResponseError("invalid resolved query detail") from None
        return reply.model_copy(update={"groups": resolved, "blob_refs": []})

    @staticmethod
    def _pointer_parent(groups: dict, pointer: str):
        parts = pointer.split("/")
        if len(parts) < 4 or parts[0] != "" or parts[1] != "groups":
            raise ServerResponseError("invalid query blob pointer")
        current = groups
        for part in parts[2:-1]:
            key = part.replace("~1", "/").replace("~0", "~")
            try:
                if isinstance(current, list):
                    if not key.isdecimal():
                        raise ValueError
                    current = current[int(key)]
                else:
                    current = current[key]
            except (KeyError, IndexError, ValueError, TypeError):
                raise ServerResponseError("invalid query blob pointer") from None
        final = parts[-1].replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            try:
                if not final.isdecimal():
                    raise ValueError
                final = int(final)
                current[final]
            except (ValueError, IndexError):
                raise ServerResponseError("invalid query blob pointer") from None
        elif isinstance(current, dict):
            if final not in current:
                raise ServerResponseError("invalid query blob pointer")
        else:
            raise ServerResponseError("invalid query blob pointer")
        return current, final

    def _read_blob(self, ref):
        if ref.total_bytes > MAX_BLOB_FIELD_BYTES:
            raise ServerResponseError("query blob exceeds field budget")
        data = bytearray()
        while len(data) < ref.total_bytes:
            reply = self.transport.query(
                ChunkRequest(
                    kind="chunk_request",
                    schema_version=1,
                    blob_id=ref.blob_id,
                    offset=len(data),
                )
            )
            self._failure(reply)
            if not isinstance(reply, ChunkReply):
                raise ServerResponseError("invalid query chunk response")
            if (
                reply.blob_id != ref.blob_id
                or reply.offset != len(data)
                or reply.total_bytes != ref.total_bytes
                or reply.sha256 != ref.sha256
            ):
                raise ServerResponseError("query chunk identity changed")
            try:
                raw = base64.b64decode(reply.data_base64, validate=True)
            except (ValueError, binascii.Error):
                raise ServerResponseError("invalid query chunk encoding") from None
            if not raw or len(data) + len(raw) > ref.total_bytes:
                raise ServerResponseError("invalid query chunk length")
            data.extend(raw)
            if reply.complete != (len(data) == ref.total_bytes):
                raise ServerResponseError("query chunk completion mismatch")
        if hashlib.sha256(data).hexdigest() != ref.sha256:
            raise ServerResponseError("query blob digest mismatch")
        try:
            return strict_json(bytes(data))
        except (WireError, ValueError, UnicodeError, RecursionError):
            raise ServerResponseError("invalid query blob JSON") from None


def print_query_show(
    snapshot: QuerySnapshot,
    detail_reader,
    stream,
    *,
    show_parameter: bool = False,
    show_trigger: bool = False,
    show_limit: bool = False,
    show_event: bool = False,
    show_meter: bool = False,
) -> None:
    """Render a complete summary and only explicitly selected live details."""
    requested = []
    if show_parameter:
        requested.append("parameters")
    if show_trigger or show_limit or show_event or show_meter:
        requested.append("definition")
    if show_limit or show_event or show_meter:
        requested.append("runtime")
    targets = len(snapshot.ordered_paths) - (snapshot.scope_path == "/")
    if requested and targets > 4096:
        raise QueryResponseError(
            "resource_exhausted", "select a smaller scope for detailed show"
        )
    stream.write(f"# summary as_of={snapshot.as_of}\n")
    if requested:
        stream.write("# details are sampled live after the summary\n")
    if show_parameter and snapshot.scope_path == "/":
        root = detail_reader(snapshot, "/", ["parameters"])
        for parameter in root.groups.parameters.user:
            value = (
                "<redacted>"
                if parameter.redacted
                else ""
                if parameter.value is None
                else str(parameter.value)
            )
            stream.write(f"param {parameter.name} '{value}'\n")
    base = 1 if snapshot.scope_path == "/" else 0
    for path in snapshot.ordered_paths:
        if path == "/":
            continue
        node = snapshot.nodes[path]
        relative = (path.count("/") if path != "/" else 0) - (
            snapshot.scope_path.count("/") if snapshot.scope_path != "/" else 0
        )
        prefix = "  " * max(0, relative - base)
        state = f"suspend ({node.status})" if node.suspended else node.status
        if node.submission_unknown:
            state += "; submission unknown"
        stream.write(f"{prefix}|- {path.rsplit('/', 1)[-1]} [{state}]\n")
        if not requested:
            continue
        detail = detail_reader(snapshot, path, requested)
        spaces = " " * len(f"{prefix}|- ")

        def line(value):
            stream.write(f"{spaces} {value}\n")

        definition = detail.groups.definition
        runtime = detail.groups.runtime
        if runtime is not None and runtime.submission_unknown_reason:
            line(f"submission unknown: {runtime.submission_unknown_reason}")
        if definition is not None:
            if definition.repeat is not None:
                repeat = definition.repeat
                line(
                    f"repeat {repeat.get('name', '')} [{repeat.get('start_date', '')}, {repeat.get('end_date', '')}]"
                )
            if show_trigger:
                if definition.trigger:
                    line(f"trigger {definition.trigger}")
                for item in definition.times:
                    line(f"time {item.get('time', '')}")
        if show_parameter:
            for parameter in detail.groups.parameters.user:
                value = (
                    "<redacted>"
                    if parameter.redacted
                    else ""
                    if parameter.value is None
                    else str(parameter.value)
                )
                line(f"param {parameter.name} '{value}'")
        if show_limit and runtime is not None:
            definitions = {
                item.get("name"): item.get("limit") for item in definition.limits
            }
            for item in runtime.limits:
                name = item.get("name", "")
                line(
                    f"limit {name} [{item.get('value', 0)}/{definitions.get(name, '')}]"
                )
        if show_event and runtime is not None:
            for item in runtime.events:
                line(
                    f"event {item.get('name', '')} [{'set' if item.get('value') else 'unset'}]"
                )
        if show_meter and runtime is not None:
            definitions = {item.get("name"): item for item in definition.meters}
            for item in runtime.meters:
                name = item.get("name", "")
                meta = definitions.get(name, {})
                line(
                    f"meter {name} {meta.get('min_value', '')} {meta.get('max_value', '')} [{item.get('value', '')}]"
                )
