"""Immutable, bounded state versions built on the scheduler's single writer.

A version owns only primitive JSON bytes and immutable path/shard tables. Readers
may retain an old root while later transactions replace touched records. This
module neither reads files nor constructs execution nodes from frozen data.
"""

from __future__ import annotations

import json
import os
import pickle
import zlib
from types import SimpleNamespace
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from types import MappingProxyType
from typing import TYPE_CHECKING, Callable, Mapping

from takler.core import Bunch, Task
from takler.core._run_effect import fingerprint as _run_effect_fingerprint
from takler.core.state import NodeStatus
from takler.schema.definition import DefinitionError
from takler.serialization.checkpoint import (
    CheckpointDocument,
    JobSecret,
    NodeRuntime,
    RuntimeDocument,
    SecretsDocument,
    SourceAddress,
    _capture_node,
    _validate_checkpoint_view,
)
from takler.serialization.definition import _project
from takler.serialization.query import project_show, query_hidden_parameters
from takler.serialization.registry import (
    TypeRegistry,
    get_registry,
    require_codec,
    using_registry,
)
from takler.serialization.runtime import RuntimeServerState

if TYPE_CHECKING:
    from takler.server.query_changes import ChangeBatch


SHARD_NODES = 256
MAX_VERSION_BYTES = 768 * 1024 * 1024
MAX_FIELD_BYTES = 16 * 1024 * 1024
MAX_RECORD_BYTES = 64 * 1024 * 1024
MAX_PENDING_BYTES = 32 * 1024 * 1024
_IN_FLIGHT = (NodeStatus.submitted, NodeStatus.active)


class VersionUnavailable(RuntimeError):
    """A complete, supported version could not be fixed for a new reader."""


def _encode(value: object) -> bytes:
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str) and len(item.encode("utf-8")) > MAX_FIELD_BYTES:
            raise VersionUnavailable("field_too_large")
        if isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, (list, tuple)):
            pending.extend(item)
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parent(path: str) -> str | None:
    return None if path == "/" else path.rsplit("/", 1)[0] or "/"


def _children(node, path: str) -> tuple[str, ...]:
    children = node.flows.values() if isinstance(node, Bunch) else node.children
    return tuple(
        "/" + child.name if path == "/" else path + "/" + child.name
        for child in children
    )


def _walk_nodes(root: Bunch):
    pending = [root]
    while pending:
        node = pending.pop()
        yield node
        children = node.flows.values() if isinstance(node, Bunch) else node.children
        pending.extend(reversed(tuple(children)))


def _row_bytes(row: FrozenNode, secret: bytes | None) -> int:
    return (
        len(row.path)
        + sum(map(len, row.children))
        + len(row.definition)
        + len(row.runtime)
        + len(row.public or b"")
        + len(row.summary or b"")
        + len(secret or b"")
        + 160
    )


@dataclass(frozen=True, slots=True)
class FrozenNode:
    path: str
    parent: str | None
    children: tuple[str, ...]
    identity: int
    definition: bytes
    runtime: bytes
    public: bytes | None
    summary: bytes | None = None


@dataclass(frozen=True, slots=True)
class StateVersion:
    state_version: int
    as_of: str
    nodes: tuple[tuple[FrozenNode, ...], ...]
    secrets: tuple[tuple[bytes | None, ...], ...]
    index: Mapping[str, tuple[int, int]]
    server_state: bytes
    source_address: bytes
    cwd: str
    retained_bytes: int

    def _slot(self, path: str) -> tuple[int, int]:
        try:
            return self.index[path]
        except KeyError:
            raise KeyError(f"path absent from fixed version: {path}") from None

    def node(self, path: str) -> FrozenNode:
        shard, offset = self._slot(path)
        return self.nodes[shard][offset]

    def iter_nodes(self):
        for shard in self.nodes:
            yield from shard

    def public_node(self, path: str) -> dict:
        """Return a detached public record and its fixed ancestor context."""
        item = self.node(path)
        if item.public is None:
            raise VersionUnavailable("unsupported_plugin_detail")
        result = json.loads(item.public)
        ancestors = []
        parent = item.parent
        while parent is not None:
            ancestor = self.node(parent)
            if ancestor.public is None:
                raise VersionUnavailable("unsupported_plugin_detail")
            source = json.loads(ancestor.public)
            ancestors.append(
                {
                    "source_path": parent,
                    "user": source["user_parameters"],
                    "generated": source["generated_parameters"],
                }
            )
            parent = ancestor.parent
        result["ancestor_context"] = ancestors
        result["path"] = path
        result["parent_path"] = item.parent
        result["child_paths"] = list(item.children)
        result["state_version"] = self.state_version
        result["as_of"] = self.as_of
        if path != "/":
            root = self.node("/")
            if root.public is None:
                raise VersionUnavailable("unsupported_plugin_detail")
            result["service_context"] = json.loads(root.public)["server_status"]
        # Anticipated shell paths depend on inherited TAKLER_HOME. Compute
        # them from frozen ancestors, never from a live parent or process cwd.
        generated = result.get("generated_parameters", [])
        generated = [
            value
            for value in generated
            if value["name"] not in {"TAKLER_JOB", "TAKLER_JOBOUT"}
        ]
        if result.get("type_id") == "takler.shell":
            hidden = query_hidden_parameters(result.get("redacted_parameters", ()))
            home_item = next(
                (
                    value
                    for source in [
                        {"user": result["user_parameters"]},
                        *ancestors,
                    ]
                    for value in source["user"]
                    if value["name"] == "TAKLER_HOME"
                ),
                None,
            )
            home = None if home_item is None else home_item["value"]
            if home is not None:
                suffix = result.get("attempt_id") or str(result.get("try_no"))
                for name, explicit, extension in (
                    ("TAKLER_JOB", result.get("job_path"), ".job"),
                    ("TAKLER_JOBOUT", result.get("jobout_path"), "."),
                ):
                    value = (
                        "<redacted>"
                        if home == "<redacted>" or name.lower() in hidden
                        else explicit
                        or os.path.abspath(
                            os.path.join(self.cwd, f"{home}{path}{extension}{suffix}")
                        )
                    )
                    generated.append({"name": name, "value": value})
        result["generated_parameters"] = generated
        return result

    def checkpoint_document(
        self, *, saved_at: str | None = None, registry: TypeRegistry | None = None
    ) -> CheckpointDocument:
        """Materialize and validate v3 solely from this fixed version."""
        definitions = {}
        runtime = {}
        passwords = {}
        for row in self.iter_nodes():
            definitions[row.path] = json.loads(row.definition)
            runtime[row.path] = NodeRuntime.model_validate_json(row.runtime)
            shard, offset = self.index[row.path]
            secret = self.secrets[shard][offset]
            if secret is not None:
                passwords[row.path] = JobSecret.model_validate_json(secret)
        for row in self.iter_nodes():
            key = "flows" if row.path == "/" else "children"
            definitions[row.path][key] = [definitions[child] for child in row.children]
        from takler.schema.definition import parse_definition

        registry = registry or get_registry()
        root = definitions["/"]
        del definitions
        definition = parse_definition(
            {
                "kind": "takler.definition",
                "schema_version": 1,
                "root": root,
            },
            model=registry.document_model(),
        )
        del root
        document = CheckpointDocument(
            format_version=3,
            saved_at=saved_at or self.as_of,
            definition=definition.model_dump(mode="json"),
            runtime=RuntimeDocument(
                server_state=RuntimeServerState.model_validate_json(self.server_state),
                nodes=runtime,
            ),
            secrets=SecretsDocument(job_passwords=passwords),
            source_address=SourceAddress.model_validate_json(self.source_address),
        )
        _validate_checkpoint_view(document, definition, registry)
        return document

    def checkpoint_stream(
        self,
        write: Callable[[str], object],
        *,
        saved_at: str | None = None,
        registry: TypeRegistry | None = None,
    ) -> None:
        """Validate and emit v3 incrementally without a full document copy."""
        from takler.schema.definition import parse_definition

        if not self.index:
            indexed = replace(
                self,
                index=MappingProxyType(
                    {
                        row.path: (shard_no, offset)
                        for shard_no, shard in enumerate(self.nodes)
                        for offset, row in enumerate(shard)
                    }
                ),
            )
            return indexed.checkpoint_stream(
                write, saved_at=saved_at, registry=registry
            )
        registry = registry or get_registry()
        saved_at = saved_at or self.as_of
        if datetime.fromisoformat(saved_at).tzinfo is None:
            raise VersionUnavailable("invalid_saved_at")
        RuntimeServerState.model_validate_json(self.server_state)
        SourceAddress.model_validate_json(self.source_address)

        def subtree_data(path: str) -> dict:
            built = {}
            stack = [(path, False)]
            while stack:
                current, complete = stack.pop()
                row = self.node(current)
                if not complete:
                    stack.append((current, True))
                    stack.extend((child, False) for child in reversed(row.children))
                    continue
                item = json.loads(row.definition)
                key = "flows" if current == "/" else "children"
                item[key] = [built.pop(child) for child in row.children]
                built[current] = item
            return built[path]

        def summarize(model, path: str, definitions: dict):
            root = None
            pending = [(model, path, None)]
            while pending:
                item, current, parent = pending.pop()
                summary = SimpleNamespace(
                    name=item.name,
                    type_id=item.type_id,
                    children=[],
                    flows=[],
                    events=getattr(item, "events", ()),
                    meters=getattr(item, "meters", ()),
                    limits=getattr(item, "limits", ()),
                    in_limits=getattr(item, "in_limits", ()),
                    times=getattr(item, "times", ()),
                    repeat=getattr(item, "repeat", None),
                    trigger=getattr(item, "trigger", None),
                    complete_trigger=getattr(item, "complete_trigger", None),
                )
                definitions[current] = summary
                if parent is None:
                    root = summary
                else:
                    getattr(
                        parent, "flows" if current.count("/") == 1 else "children"
                    ).append(summary)
                children = getattr(item, "flows", None)
                if children is None:
                    children = getattr(item, "children", ())
                for child in reversed(children):
                    child_path = (
                        "/" + child.name
                        if current == "/"
                        else current + "/" + child.name
                    )
                    pending.append((child, child_path, summary))
            return root

        definitions = {}
        root_data = json.loads(self.node("/").definition)
        root_data["flows"] = []
        root_model = parse_definition(
            {"kind": "takler.definition", "schema_version": 1, "root": root_data},
            model=registry.document_model(),
        ).root
        root_summary = summarize(root_model, "/", definitions)
        for flow_path in self.node("/").children:
            flow_model = parse_definition(
                {
                    "kind": "takler.definition",
                    "schema_version": 1,
                    "root": subtree_data(flow_path),
                },
                model=registry.document_model(),
            ).root
            flow_summary = summarize(flow_model, flow_path, definitions)
            root_summary.flows.append(flow_summary)

        runtime = {}
        passwords = {}
        checked_entries = {}
        for row in self.iter_nodes():
            item = NodeRuntime.model_validate_json(row.runtime)
            type_id = definitions[row.path].type_id
            if type_id not in checked_entries:
                checked_entries[type_id] = registry.by_id(type_id)
            entry = checked_entries[type_id]
            if entry.type_id.startswith("takler."):
                if item.runtime_data:
                    raise DefinitionError("invalid_runtime")
            else:
                require_codec(entry, "runtime_schema", "restore_runtime")
                if entry.secret_fields & item.runtime_data.keys():
                    raise DefinitionError("invalid_secret")
                entry.runtime_schema.model_validate(item.runtime_data)
            runtime[row.path] = SimpleNamespace(
                kind=item.kind,
                state=item.state,
                root_attributes=item.root_attributes,
                event_values=item.event_values,
                meter_values=item.meter_values,
                limit_usage=item.limit_usage,
                time_free=item.time_free,
                repeat_value=item.repeat_value,
                trigger_free=item.trigger_free,
                complete_trigger_free=item.complete_trigger_free,
                definition_generation=item.definition_generation,
                attempt_id=item.attempt_id,
                try_no=item.try_no,
            )
            shard, offset = self.index[row.path]
            secret = self.secrets[shard][offset]
            if secret is not None:
                passwords[row.path] = JobSecret.model_validate_json(secret)
        view = SimpleNamespace(
            runtime=SimpleNamespace(nodes=runtime),
            secrets=SimpleNamespace(job_passwords=passwords),
        )
        _validate_checkpoint_view(view, SimpleNamespace(root=root_summary), registry)
        del view, runtime, passwords, definitions, root_summary

        def compact(value):
            return json.dumps(
                value, ensure_ascii=False, separators=(",", ":"), allow_nan=False
            )

        write('{"format_version":3,"saved_at":')
        write(compact(saved_at))
        write(',"definition":{"kind":"takler.definition","schema_version":1,"root":')
        stack = [("node", "/")]
        while stack:
            kind, value = stack.pop()
            if kind == "text":
                write(value)
                continue
            row = self.node(value)
            child_key = "flows" if value == "/" else "children"
            # Built-in shallow definitions were encoded and validated when
            # captured. Most task nodes are leaves, so decoding and encoding
            # every field again would repeat millions of JSON operations.
            if row.definition.startswith(b'{"type_id":"takler.'):
                encoded = row.definition.decode("utf-8")
                if not row.children:
                    write(encoded)
                    continue
                marker = f'"{child_key}":[]'
                if encoded.count(marker) == 1:
                    before, _, after = encoded.partition(marker)
                    write(before + f'"{child_key}":[')
                    stack.append(("text", "]" + after))
                    for index in range(len(row.children) - 1, -1, -1):
                        stack.append(("node", row.children[index]))
                        if index:
                            stack.append(("text", ","))
                    continue
            data = json.loads(row.definition)
            write("{")
            fields = list(data.items())
            for position, (name, item) in enumerate(fields):
                if position:
                    write(",")
                write(compact(name))
                write(":")
                if name != child_key:
                    write(compact(item))
                    continue
                write("[")
                trailing = "".join(
                    "," + compact(key) + ":" + compact(content)
                    for key, content in fields[position + 1 :]
                )
                stack.append(("text", "]" + trailing + "}"))
                for index in range(len(row.children) - 1, -1, -1):
                    stack.append(("node", row.children[index]))
                    if index:
                        stack.append(("text", ","))
                break
            else:
                raise VersionUnavailable("invalid_structure")
        write('},"runtime":{"server_state":')
        write(self.server_state.decode("utf-8"))
        write(',"nodes":{')
        for position, row in enumerate(self.iter_nodes()):
            if position:
                write(",")
            write(compact(row.path))
            write(":")
            write(row.runtime.decode("utf-8"))
        write('}},"secrets":{"job_passwords":{')
        first = True
        for row in self.iter_nodes():
            shard, offset = self.index[row.path]
            secret = self.secrets[shard][offset]
            if secret is None:
                continue
            if not first:
                write(",")
            first = False
            write(compact(row.path))
            write(":")
            write(secret.decode("utf-8"))
        write('}},"source_address":')
        write(self.source_address.decode("utf-8"))
        write("}")


def _unpickle_checkpoint_shards(
    state_version, as_of, compressed, server_state, source_address, cwd, retained_bytes
) -> StateVersion:
    nodes = []
    secrets = []
    for shard in compressed:
        records = pickle.loads(zlib.decompress(shard))
        nodes.append(
            tuple(
                FrozenNode(path, parent, children, identity, definition, runtime, None)
                for path, parent, children, identity, definition, runtime, _ in records
            )
        )
        secrets.append(tuple(record[-1] for record in records))
    return StateVersion(
        state_version,
        as_of,
        tuple(nodes),
        tuple(secrets),
        MappingProxyType({}),
        server_state,
        source_address,
        cwd,
        retained_bytes,
    )


@dataclass(frozen=True, slots=True)
class CheckpointTransfer:
    """Pickle only private checkpoint records for an isolated worker."""

    version: StateVersion

    def __reduce__(self):
        version = self.version
        compressed = tuple(
            zlib.compress(
                pickle.dumps(
                    tuple(
                        (
                            row.path,
                            row.parent,
                            row.children,
                            row.identity,
                            row.definition,
                            row.runtime,
                            secret,
                        )
                        for row, secret in zip(shard, private, strict=True)
                    ),
                    protocol=5,
                ),
                level=1,
            )
            for shard, private in zip(version.nodes, version.secrets, strict=True)
        )
        return (
            _unpickle_checkpoint_shards,
            (
                version.state_version,
                version.as_of,
                compressed,
                version.server_state,
                version.source_address,
                version.cwd,
                version.retained_bytes,
            ),
        )


class VersionStore:
    """Publish one COW root after each complete writer transaction."""

    def __init__(
        self,
        bunch: Bunch,
        *,
        status_provider: Callable[[], dict],
        redacted_parameters: tuple[str, ...] = (),
        registry: TypeRegistry | None = None,
        max_bytes: int = MAX_VERSION_BYTES,
        state_version: int = 1,
        transaction_open: Callable[[], bool] = lambda: False,
    ) -> None:
        self.bunch = bunch
        self.registry = registry or get_registry()
        self.status_provider = status_provider
        self.redacted_parameters = redacted_parameters
        self.max_bytes = max_bytes
        self.transaction_open = transaction_open
        self.current: StateVersion | None = None
        self.error: str | None = "not_ready"
        self._pending: dict[str, bool] = {}
        self._pending_bytes = 0
        self._needs_rebuild = False
        self._pending_revision = state_version
        self.rebuild(state_version=state_version)

    @property
    def ready(self) -> bool:
        return self.current is not None and self.error is None

    def note(self, path: str, invalidate: bool) -> None:
        if self._needs_rebuild:
            return
        encoded = len(path.encode("utf-8"))
        if encoded > 4096:
            self._needs_rebuild = True
            self._pending.clear()
            return
        if path not in self._pending:
            self._pending_bytes += encoded + 64
        self._pending[path] = invalidate or self._pending.get(path, False)
        if self._pending_bytes > MAX_PENDING_BYTES:
            self._needs_rebuild = True
            self._pending.clear()

    def enqueue(self, batch: ChangeBatch) -> None:
        """Record the revision after every marked mutation has committed."""
        self._pending_revision = batch.revision

    def _capture(self, node, path: str) -> tuple[FrozenNode, bytes | None]:
        try:
            entry = self.registry.by_type(type(node))
        except DefinitionError as exc:
            if exc.code != "unregistered_type":
                raise
            entry = None
        if entry is None:
            # Public queries have always supported safe base fields on an
            # unregistered subclass. Keep that view fixed while making the
            # private checkpoint view explicitly unusable.
            definition = b"{}"
            runtime = b"{}"
        else:
            definition = _encode(
                _project(node, "$.root", checkpoint_root=True, recurse=False)
            )
            runtime = _encode(
                _capture_node(node, self.registry).model_dump(
                    mode="json", exclude_unset=True
                )
            )
        status = self.status_provider() if path == "/" else None
        value = project_show(
            node,
            self.redacted_parameters,
            server_status=status,
            shallow=True,
        )
        # Shell job paths are rederived from fixed ancestors at read time.
        if entry is not None and entry.type_id == "takler.shell":
            value["generated_parameters"] = [
                item
                for item in value["generated_parameters"]
                if item["name"] not in {"TAKLER_JOB", "TAKLER_JOBOUT"}
            ]
        public = _encode(value)
        summary_fields = {
            "path": path,
            "node_kind": value["node_kind"],
            "type_id": value["type_id"],
            "status": NodeStatus(value["state"]["status"]).name,
            "suspended": value["state"]["suspended"],
        }
        if value["node_kind"] == "task" and value.get("attempt_id") is not None:
            summary_fields["attempt_id"] = value["attempt_id"]
            if value.get("submission_unknown_reason") is not None:
                summary_fields["submission_unknown"] = True
        summary = _encode(summary_fields)
        secret = None
        if isinstance(node, Task) and node.state.node_status in _IN_FLIGHT:
            if not node.attempt_id or not node.job_password:
                raise DefinitionError("incomplete_checkpoint")
            secret = _encode(
                {"attempt_id": node.attempt_id, "password": node.job_password}
            )
        if (
            max(
                len(definition),
                len(runtime),
                len(public or b""),
                len(summary),
                len(secret or b""),
            )
            > MAX_RECORD_BYTES
        ):
            raise VersionUnavailable("field_too_large")
        return (
            FrozenNode(
                path,
                _parent(path),
                _children(node, path),
                node._version_identity,
                definition,
                runtime,
                public,
                summary,
            ),
            secret,
        )

    def _root_metadata(self) -> tuple[bytes, bytes]:
        return (
            _encode(self.bunch.server_state.to_dict()),
            _encode(self.bunch.server_config.to_dict()),
        )

    def _make_version(
        self,
        rows: tuple[tuple[FrozenNode, ...], ...],
        secrets: tuple[tuple[bytes | None, ...], ...],
        index: Mapping[str, tuple[int, int]],
        state_version: int,
        retained_bytes: int | None = None,
    ) -> StateVersion:
        server_state, source_address = self._root_metadata()
        retained = retained_bytes
        if retained is None:
            retained = len(server_state) + len(source_address)
            for shard, private in zip(rows, secrets, strict=True):
                retained += sum(
                    _row_bytes(row, secret)
                    for row, secret in zip(shard, private, strict=True)
                )
        if retained > self.max_bytes:
            raise VersionUnavailable("version_memory_limit")
        return StateVersion(
            state_version,
            _utc_now(),
            rows,
            secrets,
            index
            if isinstance(index, MappingProxyType)
            else MappingProxyType(dict(index)),
            server_state,
            source_address,
            os.getcwd(),
            retained,
        )

    def rebuild(self, *, state_version: int) -> None:
        """Build a candidate off-root; publish only when complete and bounded."""
        self.error = "building"
        try:
            with using_registry(self.registry):
                pairs = []
                for node in _walk_nodes(self.bunch):
                    path = "/" if node is self.bunch else node.node_path
                    pairs.append(self._capture(node, path))
                rows = tuple(
                    tuple(pair[0] for pair in pairs[index : index + SHARD_NODES])
                    for index in range(0, len(pairs), SHARD_NODES)
                )
                secrets = tuple(
                    tuple(pair[1] for pair in pairs[index : index + SHARD_NODES])
                    for index in range(0, len(pairs), SHARD_NODES)
                )
                index = {
                    row.path: (shard_no, offset)
                    for shard_no, shard in enumerate(rows)
                    for offset, row in enumerate(shard)
                }
                if len(index) != len(pairs) or "/" not in index:
                    raise VersionUnavailable("invalid_structure")
                candidate = self._make_version(rows, secrets, index, state_version)
            self.current = candidate
            self.error = None
            self._pending.clear()
            self._pending_bytes = 0
            self._needs_rebuild = False
            self._pending_revision = state_version
        except Exception as exc:
            self.error = (
                str(exc)
                if isinstance(exc, VersionUnavailable)
                else getattr(exc, "code", "capture_failed")
            )

    def flush(self) -> None:
        """Apply coalesced hints after mutation has finished, including failures."""
        if not self._pending and not self._needs_rebuild:
            return
        if self.transaction_open():
            raise VersionUnavailable("state_changing")
        if not self.ready:
            return
        if self._needs_rebuild:
            self.rebuild(state_version=self._pending_revision)
            return
        try:
            with using_registry(self.registry):
                candidate = self._incremental()
            self.current = candidate
            self.error = None
            self._pending.clear()
            self._pending_bytes = 0
        except Exception as exc:
            # The old root remains valid for already pinned readers, but a new
            # reader must never mistake it for the current live state.
            self.error = (
                str(exc)
                if isinstance(exc, VersionUnavailable)
                else getattr(exc, "code", "capture_failed")
            )

    def _incremental(self) -> StateVersion:
        old = self.current
        assert old is not None
        updates: dict[str, tuple[FrozenNode, bytes | None]] = {}
        removed: set[str] = set()
        structural = False

        def add_subtree(node, path):
            stack = [(node, path)]
            while stack:
                current, current_path = stack.pop()
                updates[current_path] = self._capture(current, current_path)
                children = (
                    current.flows.values()
                    if isinstance(current, Bunch)
                    else current.children
                )
                for child in reversed(tuple(children)):
                    child_path = (
                        "/" + child.name
                        if current_path == "/"
                        else current_path + "/" + child.name
                    )
                    stack.append((child, child_path))

        def refresh_parent(path):
            parent_path = _parent(path)
            if parent_path is None:
                return
            parent_node = (
                self.bunch if parent_path == "/" else self.bunch.find_node(parent_path)
            )
            if parent_node is not None:
                updates[parent_path] = self._capture(parent_node, parent_path)

        for path, invalidate in tuple(self._pending.items()):
            node = self.bunch if path == "/" else self.bunch.find_node(path)
            prior = old.node(path) if path in old.index else None
            if node is None:
                if prior is not None:
                    structural = True
                    removed.update(
                        name
                        for name in old.index
                        if name == path or name.startswith(path + "/")
                    )
                    refresh_parent(path)
                continue
            children = _children(node, path)
            if prior is None or prior.identity != node._version_identity:
                structural = True
                removed.update(
                    name
                    for name in old.index
                    if name == path or name.startswith(path + "/")
                )
                add_subtree(node, path)
                if prior is None:
                    refresh_parent(path)
            else:
                updates[path] = self._capture(node, path)
                if children != prior.children:
                    structural = True
                    old_children = set(prior.children)
                    new_children = set(children)
                    for child in old_children - new_children:
                        removed.update(
                            name
                            for name in old.index
                            if name == child or name.startswith(child + "/")
                        )
                    for child in new_children - old_children:
                        target = self.bunch.find_node(child)
                        if target is None:
                            raise VersionUnavailable("invalid_structure")
                        add_subtree(target, child)
                elif invalidate:
                    # update_child can replace an object at the same path.
                    # The parent's child-name list then stays unchanged.
                    for child in children:
                        target = self.bunch.find_node(child)
                        previous = old.node(child) if child in old.index else None
                        if target is None or previous is None:
                            raise VersionUnavailable("invalid_structure")
                        if previous.identity != target._version_identity:
                            structural = True
                            removed.update(
                                name
                                for name in old.index
                                if name == child or name.startswith(child + "/")
                            )
                            add_subtree(target, child)
        if structural:
            kept = {
                row.path: (
                    row,
                    old.secrets[shard_no][offset],
                )
                for shard_no, shard in enumerate(old.nodes)
                for offset, row in enumerate(shard)
                if row.path not in removed
            }
            kept.update(updates)
            # Preorder is stable for readers and checkpoint assembly.
            ordered = []
            stack = ["/"]
            while stack:
                path = stack.pop()
                row = kept.get(path)
                if row is None:
                    raise VersionUnavailable("invalid_structure")
                ordered.append(row)
                stack.extend(reversed(row[0].children))
            if len(ordered) != len(kept):
                raise VersionUnavailable("invalid_structure")
            rows = tuple(
                tuple(item[0] for item in ordered[index : index + SHARD_NODES])
                for index in range(0, len(ordered), SHARD_NODES)
            )
            secrets = tuple(
                tuple(item[1] for item in ordered[index : index + SHARD_NODES])
                for index in range(0, len(ordered), SHARD_NODES)
            )
            index = {
                row.path: (shard_no, offset)
                for shard_no, shard in enumerate(rows)
                for offset, row in enumerate(shard)
            }
        else:
            rows = list(old.nodes)
            secrets = list(old.secrets)
            changed_nodes = {}
            changed_secrets = {}
            for path, (row, secret) in updates.items():
                shard_no, offset = old.index[path]
                changed_nodes.setdefault(shard_no, list(rows[shard_no]))[offset] = row
                changed_secrets.setdefault(shard_no, list(secrets[shard_no]))[
                    offset
                ] = secret
            for shard_no, shard in changed_nodes.items():
                rows[shard_no] = tuple(shard)
                secrets[shard_no] = tuple(changed_secrets[shard_no])
            rows = tuple(rows)
            secrets = tuple(secrets)
            index = old.index
        retained = None
        if not structural:
            server_state, source_address = self._root_metadata()
            retained = (
                old.retained_bytes
                + len(server_state)
                - len(old.server_state)
                + len(source_address)
                - len(old.source_address)
            )
            for path, (row, secret) in updates.items():
                shard_no, offset = old.index[path]
                retained += _row_bytes(row, secret) - _row_bytes(
                    old.nodes[shard_no][offset], old.secrets[shard_no][offset]
                )
        return self._make_version(
            rows, secrets, index, self._pending_revision, retained_bytes=retained
        )

    def pin(self) -> StateVersion:
        if self.transaction_open():
            raise VersionUnavailable("state_changing")
        self.flush()
        if not self.ready:
            raise VersionUnavailable(self.error or "not_ready")
        assert self.current is not None
        return self.current


# Preserve the builtin callbacks even when customized before Scheduler import.
_RUN_EFFECT_BASELINE = _run_effect_fingerprint(VersionStore)
