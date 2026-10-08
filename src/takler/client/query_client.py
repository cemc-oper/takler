"""Atomic, bounded QueryDocument v1 reads for the Python client."""

from __future__ import annotations

import base64
import binascii
import hashlib
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from takler.exceptions import QueryResponseError, ServerResponseError
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
    PageReply,
    PageRequest,
    SummaryNode,
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

    @classmethod
    def from_wire(cls, node: SummaryNode) -> "QueryNode":
        return cls(
            node.path,
            node.node_kind,
            node.type_id,
            node.status,
            node.suspended,
            node.attempt_id,
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

    def get(self, path: str) -> QueryNode | None:
        return self.nodes.get(path)


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
        old_size = self.current.estimated_bytes if self.current else 0
        nodes: dict[str, QueryNode] = {}
        order: list[str] = []
        generations: dict[str, str] = {}
        size = 1024
        identity = None
        expected_total = None
        index = 0
        while True:
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
        snapshot = QuerySnapshot(
            *identity,
            scope,
            depth,
            MappingProxyType(nodes),
            tuple(order),
            MappingProxyType(generations),
            size,
        )
        self.current = snapshot
        return snapshot

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
        stream.write(f"{prefix}|- {path.rsplit('/', 1)[-1]} [{state}]\n")
        if not requested:
            continue
        detail = detail_reader(snapshot, path, requested)
        spaces = " " * len(f"{prefix}|- ")

        def line(value):
            stream.write(f"{spaces} {value}\n")

        definition = detail.groups.definition
        runtime = detail.groups.runtime
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
