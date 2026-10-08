"""Transport-neutral R2 query v1 wire models.

Both HTTP and gRPC carry the same strict JSON document. R2-04A defines the
protocol; R2-05/07 add the producer and snapshot storage. These models never
import a scheduler, generated binding, or transport.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from takler.protocol.wire import WireError, strict_json


MAX_PAGE_NODES = 2048
MAX_PAGE_BYTES = 262_144
MAX_CHUNK_BYTES = 65_536
GROUPS = frozenset(
    {"summary", "definition", "parameters", "runtime", "artifacts", "service"}
)
DETAIL_GROUPS = GROUPS - {"summary"}
STATUSES = frozenset(
    {"unknown", "queued", "submitted", "active", "complete", "aborted"}
)
KINDS = frozenset({"bunch", "flow", "container", "task"})
ERROR_CODES = frozenset(
    {
        "invalid_request",
        "unsupported_version",
        "unsupported_capability",
        "permission_denied",
        "stale_identity",
        "snapshot_expired",
        "resource_exhausted",
        "field_too_large",
        "internal_error",
    }
)
_SEGMENT = re.compile(r"^[^/\x00-\x1f]+$")


def valid_path(path: str) -> str:
    if path != "/" and (not path.startswith("/") or path.endswith("/")):
        raise ValueError("invalid path")
    if path != "/" and any(
        segment in {"", ".", ".."} or not _SEGMENT.fullmatch(segment)
        for segment in path[1:].split("/")
    ):
        raise ValueError("invalid path")
    return path


def valid_utc(value: str) -> str:
    if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z", value):
        raise ValueError("invalid UTC timestamp")
    datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


class QueryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class CapabilitiesRequest(QueryModel):
    kind: Literal["capabilities_request"]
    schema_version: Literal[1]


class CapabilitiesReply(QueryModel):
    kind: Literal["capabilities"]
    schema_version: Literal[1]
    snapshot: Literal[True]
    detail: Literal[True]
    since: Literal[False]
    max_page_nodes: Literal[2048]
    max_decoded_page_bytes: Literal[262144]
    max_chunk_bytes: Literal[65536]


class PageRequest(QueryModel):
    kind: Literal["page_request"]
    schema_version: Literal[1]
    scope_path: StrictStr | None = None
    flow_name: StrictStr | None = None
    depth: StrictInt | None = Field(default=None, ge=0)
    field_groups: list[StrictStr]
    cursor: StrictStr | None = None

    @model_validator(mode="after")
    def validate_scope(self):
        if self.scope_path is not None and self.flow_name is not None:
            raise ValueError("scope_path and flow_name are mutually exclusive")
        if self.scope_path is not None:
            valid_path(self.scope_path)
        if self.flow_name is not None and (
            not _SEGMENT.fullmatch(self.flow_name) or self.flow_name in {".", ".."}
        ):
            raise ValueError("invalid flow name")
        if (
            not self.field_groups
            or self.field_groups[0] != "summary"
            or len(set(self.field_groups)) != len(self.field_groups)
            or set(self.field_groups) - GROUPS
        ):
            raise ValueError("invalid field groups")
        if self.cursor == "":
            raise ValueError("empty cursor")
        return self


class SummaryNode(QueryModel):
    path: StrictStr
    node_kind: StrictStr
    type_id: StrictStr
    status: StrictStr
    suspended: StrictBool

    @model_validator(mode="after")
    def validate_node(self):
        valid_path(self.path)
        if (
            self.node_kind not in KINDS
            or self.status not in STATUSES
            or not self.type_id
        ):
            raise ValueError("invalid summary node")
        return self


class PageReply(QueryModel):
    kind: Literal["page"]
    schema_version: Literal[1]
    session_id: StrictStr
    query_id: StrictStr
    snapshot_id: StrictStr
    as_of: StrictStr
    expires_at: StrictStr
    page_index: StrictInt = Field(ge=0)
    total_nodes: StrictInt | None = Field(default=None, ge=0)
    complete: StrictBool
    next_cursor: StrictStr | None
    root_name: StrictStr
    flow_generations: dict[StrictStr, StrictStr]
    nodes: list[SummaryNode] = Field(max_length=MAX_PAGE_NODES)

    @field_validator("as_of", "expires_at")
    @classmethod
    def validate_time(cls, value):
        return valid_utc(value)

    @model_validator(mode="after")
    def validate_page(self):
        if not all(
            (
                self.session_id,
                self.query_id,
                self.snapshot_id,
                self.as_of,
                self.expires_at,
            )
        ):
            raise ValueError("missing page identity")
        if self.complete != (self.next_cursor is None):
            raise ValueError("cursor/complete mismatch")
        if not self.complete and (not self.next_cursor or not self.nodes):
            raise ValueError("nonterminal page must advance")
        paths = [node.path for node in self.nodes]
        if len(paths) != len(set(paths)):
            raise ValueError("duplicate node path")
        for flow_path, generation in self.flow_generations.items():
            valid_path(flow_path)
            if flow_path == "/" or flow_path.count("/") != 1 or not generation:
                raise ValueError("invalid flow generation")
        for path in paths:
            if path != "/" and "/" + path.split("/", 2)[1] not in self.flow_generations:
                raise ValueError("missing flow generation")
        return self


class DetailRequest(QueryModel):
    kind: Literal["detail_request"]
    schema_version: Literal[1]
    session_id: StrictStr
    query_id: StrictStr
    path: StrictStr
    flow_generation: StrictStr | None
    attempt_id: StrictStr | None
    field_groups: list[StrictStr]

    @model_validator(mode="after")
    def validate_detail(self):
        if not self.session_id or not self.query_id:
            raise ValueError("missing query identity")
        valid_path(self.path)
        if self.path == "/" and self.flow_generation is not None:
            raise ValueError("root has no flow generation")
        if self.path != "/" and not self.flow_generation:
            raise ValueError("flow generation required")
        if (
            not self.field_groups
            or len(set(self.field_groups)) != len(self.field_groups)
            or set(self.field_groups) - DETAIL_GROUPS
        ):
            raise ValueError("invalid detail groups")
        return self


Scalar = StrictStr | StrictInt | float | StrictBool | None


class ParameterValue(QueryModel):
    name: StrictStr
    value: Scalar = None
    redacted: Literal[True] | None = None

    @model_validator(mode="after")
    def validate_parameter(self):
        if (
            not self.name
            or ("value" in self.model_fields_set)
            == ("redacted" in self.model_fields_set)
            or ("redacted" in self.model_fields_set and self.redacted is not True)
        ):
            raise ValueError("parameter must have value or redaction")
        return self


class ParameterSource(QueryModel):
    source_path: StrictStr
    user: list[ParameterValue]

    @field_validator("source_path")
    @classmethod
    def validate_source_path(cls, value):
        return valid_path(value)


class ParametersGroup(QueryModel):
    user: list[ParameterValue]
    ancestors: list[ParameterSource] = Field(default_factory=list)


class DefinitionGroup(QueryModel):
    trigger: StrictStr | None = None
    complete_trigger: StrictStr | None = None
    script_path: StrictStr | None = None
    times: list[dict[str, Any]] = Field(default_factory=list)
    repeat: dict[str, Any] | None = None
    events: list[dict[str, Any]] = Field(default_factory=list)
    meters: list[dict[str, Any]] = Field(default_factory=list)
    limits: list[dict[str, Any]] = Field(default_factory=list)
    in_limits: list[dict[str, Any]] = Field(default_factory=list)


class RuntimeGroup(QueryModel):
    begun: StrictBool | None = None
    calendar_flow_time: StrictStr | None = None
    attempt_id: StrictStr | None = None
    try_no: StrictInt | None = Field(default=None, ge=0)
    task_id: StrictStr | None = None
    submitted_at: StrictStr | None = None
    started_at: StrictStr | None = None
    ended_at: StrictStr | None = None
    aborted_reason: StrictStr | None = None
    events: list[dict[str, Any]] = Field(default_factory=list)
    meters: list[dict[str, Any]] = Field(default_factory=list)
    repeat: dict[str, Any] | None = None
    limits: list[dict[str, Any]] = Field(default_factory=list)


class ArtifactRef(QueryModel):
    path: StrictStr
    origin: Literal["current", "anticipated"]


class ArtifactsGroup(QueryModel):
    job_ref: ArtifactRef | None = None
    jobout_ref: ArtifactRef | None = None
    orvix_info_path: StrictStr | None = None


class ServiceGroup(QueryModel):
    status: Literal["running", "halted"]
    status_reason: StrictStr | None = None
    halt_causes: list[StrictStr]
    message: StrictStr | None = None
    last_checkpoint_at: StrictStr | None
    restore_summary: dict[str, Any] | None
    generated_defaults: list[ParameterValue]


class DetailGroups(QueryModel):
    definition: DefinitionGroup | None = None
    parameters: ParametersGroup | None = None
    runtime: RuntimeGroup | None = None
    artifacts: ArtifactsGroup | None = None
    service: ServiceGroup | None = None


class DetailBlobRef(QueryModel):
    pointer: StrictStr
    blob_id: StrictStr
    total_bytes: StrictInt = Field(ge=0, le=16 * 1024 * 1024)
    sha256: StrictStr

    @model_validator(mode="after")
    def validate_ref(self):
        if (
            not self.pointer.startswith("/groups/")
            or not self.blob_id
            or not re.fullmatch(r"[0-9a-f]{64}", self.sha256)
        ):
            raise ValueError("invalid blob reference")
        return self


class DetailReply(QueryModel):
    kind: Literal["detail"]
    schema_version: Literal[1]
    session_id: StrictStr
    query_id: StrictStr
    path: StrictStr
    flow_generation: StrictStr | None
    attempt_id: StrictStr | None
    sample_mode: Literal["live"]
    as_of: StrictStr
    requested_groups: list[StrictStr]
    groups: DetailGroups
    blob_refs: list[DetailBlobRef] = Field(default_factory=list)

    @field_validator("as_of")
    @classmethod
    def validate_time(cls, value):
        return valid_utc(value)

    @model_validator(mode="after")
    def validate_detail(self):
        valid_path(self.path)
        if self.path == "/" and self.flow_generation is not None:
            raise ValueError("root has no flow generation")
        if self.path != "/" and not self.flow_generation:
            raise ValueError("flow generation required")
        if not self.session_id or not self.query_id or not self.as_of:
            raise ValueError("missing detail identity")
        if (
            not self.requested_groups
            or len(set(self.requested_groups)) != len(self.requested_groups)
            or set(self.requested_groups) - DETAIL_GROUPS
            or set(self.requested_groups) != self.groups.model_fields_set
            or any(
                getattr(self.groups, group) is None for group in self.requested_groups
            )
        ):
            raise ValueError("detail group mismatch")
        return self


class ChunkRequest(QueryModel):
    kind: Literal["chunk_request"]
    schema_version: Literal[1]
    blob_id: StrictStr
    offset: StrictInt = Field(ge=0)

    @field_validator("blob_id")
    @classmethod
    def validate_blob_id(cls, value):
        if not value:
            raise ValueError("missing blob id")
        return value


class ChunkReply(QueryModel):
    kind: Literal["chunk"]
    schema_version: Literal[1]
    blob_id: StrictStr
    offset: StrictInt = Field(ge=0)
    total_bytes: StrictInt = Field(ge=0, le=16 * 1024 * 1024)
    sha256: StrictStr
    data_base64: StrictStr
    complete: StrictBool

    @model_validator(mode="after")
    def validate_chunk(self):
        import base64

        try:
            data = base64.b64decode(self.data_base64, validate=True)
        except ValueError as exc:
            raise ValueError("invalid base64") from exc
        if (
            len(data) > MAX_CHUNK_BYTES
            or self.offset + len(data) > self.total_bytes
            or self.complete != (self.offset + len(data) == self.total_bytes)
        ):
            raise ValueError("invalid chunk bounds")
        if not re.fullmatch(r"[0-9a-f]{64}", self.sha256):
            raise ValueError("invalid chunk digest")
        return self


class FailureReply(QueryModel):
    kind: Literal["error"]
    schema_version: Literal[1]
    code: StrictStr
    message: StrictStr
    reset_scope: StrictStr | None = None

    @model_validator(mode="after")
    def validate_error(self):
        if self.code not in ERROR_CODES:
            raise ValueError("unknown query error")
        if self.reset_scope is not None:
            valid_path(self.reset_scope)
        return self


MODEL_BY_KIND = {
    "capabilities_request": CapabilitiesRequest,
    "capabilities": CapabilitiesReply,
    "page_request": PageRequest,
    "page": PageReply,
    "detail_request": DetailRequest,
    "detail": DetailReply,
    "chunk_request": ChunkRequest,
    "chunk": ChunkReply,
    "error": FailureReply,
}


def decode_query_document(
    raw: bytes | str, *, expected: str | None = None
) -> QueryModel:
    try:
        byte_length = len(raw if isinstance(raw, bytes) else raw.encode("utf-8"))
    except UnicodeError:
        raise WireError("invalid query document") from None
    if byte_length > MAX_PAGE_BYTES:
        raise WireError("query document too large")
    value = strict_json(raw)
    if not isinstance(value, dict):
        raise WireError("query document must be an object")
    kind = value.get("kind")
    if not isinstance(kind, str):
        raise WireError("unknown query kind")
    if kind != expected and expected is not None:
        raise WireError("query kind mismatch")
    model = MODEL_BY_KIND.get(kind)
    if model is None:
        raise WireError("unknown query kind")
    try:
        return model.model_validate(value)
    except ValueError:
        raise WireError("invalid query document") from None


def query_failure_for_invalid_request(raw: bytes | str) -> FailureReply:
    """Classify unsupported query versions and A-phase since requests."""
    code = "invalid_request"
    try:
        if len(raw if isinstance(raw, bytes) else raw.encode("utf-8")) > MAX_PAGE_BYTES:
            raise WireError("query document too large")
        value = strict_json(raw)
        if isinstance(value, dict):
            if "schema_version" in value and value["schema_version"] != 1:
                code = "unsupported_version"
            elif value.get("kind") in {"since_request", "since"}:
                code = "unsupported_capability"
    except (WireError, UnicodeError, ValueError):
        pass
    return FailureReply(
        kind="error",
        schema_version=1,
        code=code,
        message="unsupported query version"
        if code == "unsupported_version"
        else "since is unavailable"
        if code == "unsupported_capability"
        else "invalid query document",
    )


def encode_query_document(document: QueryModel) -> bytes:
    raw = json.dumps(
        document.model_dump(mode="json", exclude_unset=True),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    if len(raw) > MAX_PAGE_BYTES:
        raise WireError("query document too large")
    return raw
