"""S2 checkpoint v3: a trusted definition plus path-keyed runtime and secrets.

The definition builder is the only constructor for the restored tree. Applying
validated runtime mutates that detached candidate without running a task,
beginning a flow, or changing a limit through its scheduling hooks.
"""

from __future__ import annotations

from datetime import datetime, timezone
from posixpath import dirname, normpath
from typing import Any, Annotated, Literal
from uuid import UUID

from lark import Lark
from pydantic import Field, field_validator, model_validator

from takler.core import Bunch, Flow, NodeStatus, Task
from takler.core.bunch import ServerState
from takler.core.calendar import Calendar
from takler.core.expression_ast import AstNodePath, AstRoot, AstVariablePath
from takler.core.expression_parser import (
    ExpressionTransformer,
    parse_trigger,
    trigger_parser,
)
from takler.core.state import State
from takler.schema.definition import (
    DefinitionError,
    DefinitionModel,
    EventDefinition,
    InLimitDefinition,
    LimitDefinition,
    MeterDefinition,
    RepeatDateDefinition,
    TimeDefinition,
    _decode_json,
    parse_definition,
    unique,
)
from takler.serialization.builder import (
    build_definition,
    bind_expressions,
    validate_references,
    walk,
)
from takler.serialization.definition import export_definition
from takler.serialization.registry import get_registry, require_codec, using_registry
from takler.serialization.runtime import (
    RuntimeCalendar,
    RuntimeLimit,
    RuntimeMeter,
    RuntimeRepeat,
    RuntimeServerState,
    RuntimeState,
    validate_occupancy,
)

FORMAT_VERSION = 3
_IN_FLIGHT = {NodeStatus.submitted, NodeStatus.active}
_TASK_FIELDS = {
    "task_id",
    "try_no",
    "aborted_reason",
    "definition_generation",
    "attempt_id",
    "submitted_at",
    "started_at",
    "ended_at",
    "job_path",
    "jobout_path",
    "orvix_info_path",
}
_FLOW_FIELDS = {"begun", "calendar", "definition_generation"}
_ROOT_FIELDS = {"root_attributes"}
_CHECKPOINT_TRIGGER_PARSER = Lark(
    trigger_parser.source_grammar, start="expression", parser="lalr"
)


def _path(node) -> str:
    return "/" if isinstance(node, Bunch) else node.node_path


def _parse_checkpoint_expression(text):
    """Parse frequent snapshot expressions quickly, preserving the full parser fallback."""
    try:
        return ExpressionTransformer().transform(_CHECKPOINT_TRIGGER_PARSER.parse(text))
    except Exception:
        try:
            return parse_trigger(text)
        except Exception:
            raise DefinitionError("expression_syntax") from None


_CALENDAR_TIMES = (
    "initial_time",
    "flow_time",
    "initial_real_time",
    "last_real_time",
)


def _capture_calendar(calendar) -> dict:
    data = calendar.to_dict()
    for key in _CALENDAR_TIMES:
        if data[key] is not None:
            data[key] = datetime.fromisoformat(data[key]).astimezone().isoformat()
    return data


def _restore_calendar(runtime: RuntimeCalendar) -> Calendar:
    data = runtime.model_dump()
    for key in _CALENDAR_TIMES:
        if data[key] is not None:
            data[key] = (
                datetime.fromisoformat(data[key])
                .astimezone()
                .replace(tzinfo=None)
                .isoformat()
            )
    return Calendar.from_dict(data)


def _uuid(value: str | None) -> str | None:
    if value is not None and str(UUID(value)) != value:
        raise ValueError("invalid checkpoint UUID")
    return value


class LimitUsage(DefinitionModel):
    value: Annotated[int, Field(ge=0)]
    node_paths: list[str]

    @model_validator(mode="after")
    def unique_paths(self):
        unique(self.node_paths)
        return self


class RootAttributes(DefinitionModel):
    """Root-only scheduling attributes omitted from S1 definitions."""

    default_node_status: Literal["queued", "complete"]
    trigger: str | None
    complete_trigger: str | None
    events: list[EventDefinition]
    meters: list[MeterDefinition]
    limits: list[LimitDefinition]
    in_limits: list[InLimitDefinition]
    repeat: RepeatDateDefinition | None
    times: list[TimeDefinition]

    @model_validator(mode="after")
    def names(self):
        for group in (self.events, self.meters, self.limits):
            unique([item.name for item in group])
        unique([(item.node_path, item.limit_name) for item in self.in_limits])
        unique([item.time for item in self.times])
        return self


class NodeRuntime(DefinitionModel):
    kind: Literal["bunch", "flow", "container", "task"]
    state: RuntimeState
    trigger_free: bool
    complete_trigger_free: bool
    is_complete_triggered: bool
    event_values: dict[str, bool]
    meter_values: dict[str, int]
    limit_usage: dict[str, LimitUsage]
    repeat_value: int | None
    time_free: dict[str, bool]
    runtime_data: dict[str, Any]
    root_attributes: RootAttributes | None = None
    begun: bool | None = None
    calendar: RuntimeCalendar | None = None
    definition_generation: str | None = None
    task_id: str | None = None
    try_no: Annotated[int, Field(ge=0)] | None = None
    aborted_reason: str | None = None
    submission_unknown_reason: str | None = None
    attempt_id: str | None = None
    submitted_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    job_path: str | None = None
    jobout_path: str | None = None
    orvix_info_path: str | None = None

    @field_validator("attempt_id", "definition_generation")
    @classmethod
    def uuid_fields(cls, value):
        return _uuid(value)

    @field_validator("submitted_at", "started_at", "ended_at")
    @classmethod
    def aware_time(cls, value):
        if value is not None and datetime.fromisoformat(value).tzinfo is None:
            raise ValueError("checkpoint time needs a timezone")
        return value

    @field_validator("calendar")
    @classmethod
    def aware_calendar(cls, value):
        if value is not None:
            for key in _CALENDAR_TIMES:
                stamp = getattr(value, key)
                if stamp is not None and datetime.fromisoformat(stamp).tzinfo is None:
                    raise ValueError("checkpoint time needs a timezone")
        return value

    @model_validator(mode="after")
    def conditional_fields(self):
        conditional = (
            _TASK_FIELDS | _FLOW_FIELDS | _ROOT_FIELDS | {"submission_unknown_reason"}
        )
        required = {
            "bunch": _ROOT_FIELDS,
            "flow": _FLOW_FIELDS,
            "container": set(),
            "task": _TASK_FIELDS,
        }[self.kind]
        if self.kind == "task":
            required = _TASK_FIELDS
        allowed = required | (
            {"submission_unknown_reason"} if self.kind == "task" else set()
        )
        if (
            not required <= self.model_fields_set
            or (conditional - allowed) & self.model_fields_set
        ):
            raise ValueError("incomplete or invalid checkpoint runtime")
        if self.kind == "bunch" and self.root_attributes is None:
            raise ValueError("missing root attributes")
        if self.kind == "flow" and (
            self.begun is None
            or self.calendar is None
            or self.definition_generation is None
        ):
            raise ValueError("incomplete flow runtime")
        if self.kind == "task":
            if self.try_no is None or self.definition_generation is None:
                raise ValueError("incomplete task runtime")
            if self.try_no == 0 and self.attempt_id is not None:
                raise ValueError("inconsistent task identity")
            if (
                self.state.status
                in (NodeStatus.submitted.value, NodeStatus.active.value)
                and self.attempt_id is None
            ):
                raise ValueError("in-flight task lacks attempt")
            if self.submission_unknown_reason is not None and (
                not self.submission_unknown_reason
                or self.state.status != NodeStatus.submitted.value
                or self.attempt_id is None
            ):
                raise ValueError("invalid unknown submission")
        return self


class RuntimeDocument(DefinitionModel):
    server_state: RuntimeServerState
    nodes: dict[str, NodeRuntime]


class JobSecret(DefinitionModel):
    attempt_id: str
    password: Annotated[str, Field(min_length=1)]

    @field_validator("attempt_id")
    @classmethod
    def canonical_attempt(cls, value):
        return _uuid(value)


class SecretsDocument(DefinitionModel):
    job_passwords: dict[str, JobSecret]


class SourceAddress(DefinitionModel):
    host: str
    port: str


class CheckpointDocument(DefinitionModel):
    format_version: Literal[3]
    saved_at: str
    definition: dict[str, Any]
    runtime: RuntimeDocument
    secrets: SecretsDocument
    source_address: SourceAddress | None = None

    @field_validator("saved_at")
    @classmethod
    def aware_saved_at(cls, value):
        if datetime.fromisoformat(value).tzinfo is None:
            raise ValueError("checkpoint time needs a timezone")
        return value

    @classmethod
    def model_validate_json(cls, json_data, **kwargs):
        return cls.model_validate(_decode_json(json_data), **kwargs)

    def model_dump(self, **kwargs):
        kwargs.setdefault("exclude_unset", True)
        return super().model_dump(**kwargs)

    def model_dump_json(self, **kwargs):
        kwargs.setdefault("exclude_unset", True)
        return super().model_dump_json(**kwargs)


def _root_attributes(root: Bunch) -> RootAttributes:
    return RootAttributes.model_validate(
        {
            "default_node_status": root.default_node_status.name,
            "trigger": root.trigger_expression.expression_str
            if root.trigger_expression
            else None,
            "complete_trigger": root.complete_trigger_expression.expression_str
            if root.complete_trigger_expression
            else None,
            "events": [
                {"name": item.name, "initial_value": item.initial_value}
                for item in root.events
            ],
            "meters": [
                {
                    "name": item.name,
                    "min_value": item.min_value,
                    "max_value": item.max_value,
                }
                for item in root.meters
            ],
            "limits": [
                {"name": item.name, "limit": item.limit} for item in root.limits
            ],
            "in_limits": [
                {
                    "limit_name": item.limit_name,
                    "node_path": item.node_path,
                    "tokens": item.tokens,
                }
                for item in root.in_limit_manager.in_limit_list
            ],
            "repeat": None
            if root.repeat is None
            else {k: v for k, v in root.repeat.r.to_dict().items() if k != "value"},
            "times": [{"time": item.time.strftime("%H:%M")} for item in root.times],
        }
    )


def _capture_node(node, registry) -> NodeRuntime:
    entry = registry.by_type(type(node))
    if not entry.type_id.startswith("takler."):
        require_codec(entry, "runtime_schema", "export_runtime", "restore_runtime")
        runtime_data = entry.runtime_schema.model_validate(
            entry.export_runtime(node)
        ).model_dump(mode="json")
        if entry.secret_fields & runtime_data.keys():
            raise DefinitionError("invalid_secret")
    else:
        runtime_data = {}
    item = {
        "kind": entry.kind,
        "state": node.state.to_dict(),
        "trigger_free": bool(node.trigger_expression and node.trigger_expression.free),
        "complete_trigger_free": bool(
            node.complete_trigger_expression and node.complete_trigger_expression.free
        ),
        "is_complete_triggered": node.is_complete_triggered,
        "event_values": {event.name: event.value for event in node.events},
        "meter_values": {meter.name: meter.value for meter in node.meters},
        "limit_usage": {
            limit.name: {"value": limit.value, "node_paths": sorted(limit.node_paths)}
            for limit in node.limits
        },
        "repeat_value": None if node.repeat is None else node.repeat.r.value,
        "time_free": {time.time.strftime("%H:%M"): time.free for time in node.times},
        "runtime_data": runtime_data,
    }
    if entry.kind == "bunch":
        item["root_attributes"] = _root_attributes(node).model_dump(mode="json")
    elif entry.kind == "flow":
        item.update(
            begun=node.begun,
            calendar=_capture_calendar(node.calendar),
            definition_generation=node.definition_generation,
        )
    elif entry.kind == "task":
        item.update({key: getattr(node, key) for key in _TASK_FIELDS})
        item["submission_unknown_reason"] = node.submission_unknown_reason
    return NodeRuntime.model_validate(item)


def _validate_checkpoint_view(
    document: CheckpointDocument, definition, registry, *, live_nodes=None
) -> None:
    """Check the path-keyed view without constructing or changing a live tree."""
    definitions = {}

    def visit(item, path):
        if path in definitions:
            raise DefinitionError("invalid_structure")
        definitions[path] = item
        for child in getattr(item, "children", ()):
            visit(child, f"{path.rstrip('/')}/{child.name}")
        for flow in getattr(item, "flows", ()):
            visit(flow, f"/{flow.name}")

    visit(definition.root, "/")
    runtime = document.runtime.nodes
    _same_names(definitions, runtime)
    root = runtime["/"]
    if root.kind != "bunch" or root.root_attributes is None:
        raise DefinitionError("invalid_runtime")

    limits = {}
    in_limits = {}

    def referenced_path(owner_path, reference):
        target_path = normpath(
            reference
            if reference.startswith("/")
            else f"{dirname(owner_path)}/{reference}"
        )
        # Bunch uses "/" as its runtime key and "/<name>" as Node.node_path.
        if target_path == f"/{definition.root.name}" and target_path not in definitions:
            target_path = "/"
        if target_path not in definitions:
            raise DefinitionError("unresolved_reference")
        if live_nodes is not None and not reference.startswith("/"):
            referenced = live_nodes[owner_path].find_node(reference)
            if referenced is None or _path(referenced) != target_path:
                raise DefinitionError("unresolved_reference")
        return target_path

    for path, item in definitions.items():
        state = runtime[path]
        if state.kind != registry.by_id(item.type_id).kind:
            raise DefinitionError("invalid_runtime")
        attrs = root.root_attributes if path == "/" else item
        _same_names((event.name for event in attrs.events), state.event_values)
        _same_names((meter.name for meter in attrs.meters), state.meter_values)
        _same_names((limit.name for limit in attrs.limits), state.limit_usage)
        _same_names((time.time for time in attrs.times), state.time_free)
        if (attrs.repeat is None) != (state.repeat_value is None):
            raise DefinitionError("invalid_runtime")
        if state.trigger_free and attrs.trigger is None:
            raise DefinitionError("invalid_runtime")
        if state.complete_trigger_free and attrs.complete_trigger is None:
            raise DefinitionError("invalid_runtime")

        def check_expression(ast):
            if isinstance(ast, AstRoot):
                check_expression(ast.left)
                check_expression(ast.right)
            elif isinstance(ast, AstVariablePath):
                target_path = check_expression(ast.node)
                if (
                    live_nodes is not None
                    and live_nodes[target_path].find_variable(ast.variable_name) is None
                ):
                    raise DefinitionError("unresolved_reference")
            elif isinstance(ast, AstNodePath):
                return referenced_path(path, ast.node_path)

        for expression_text in (attrs.trigger, attrs.complete_trigger):
            if expression_text is not None:
                check_expression(_parse_checkpoint_expression(expression_text))
        for meter in attrs.meters:
            RuntimeMeter.model_validate(
                {**meter.model_dump(), "value": state.meter_values[meter.name]}
            )
        for limit in attrs.limits:
            usage = state.limit_usage[limit.name]
            RuntimeLimit.model_validate({**limit.model_dump(), **usage.model_dump()})
            limits[path, limit.name] = (limit, usage)
        if attrs.repeat is not None:
            RuntimeRepeat.model_validate(
                {**attrs.repeat.model_dump(), "value": state.repeat_value}
            )
        in_limits[path] = attrs.in_limits

    def resolve(owner_path, item):
        if item.node_path is None:
            target_path = owner_path
            while True:
                if (target_path, item.limit_name) in limits:
                    break
                if target_path == "/":
                    raise DefinitionError("unresolved_reference")
                target_path = dirname(target_path) or "/"
        else:
            reference = item.node_path
            if (
                not reference
                or reference == "/"
                or "//" in reference
                or reference.endswith("/")
                or ":" in reference
                or "\0" in reference
            ):
                raise DefinitionError("invalid_reference")
            target_path = referenced_path(owner_path, reference)
            if (target_path, item.limit_name) not in limits:
                raise DefinitionError("unresolved_reference")
        if item.tokens > limits[target_path, item.limit_name][0].limit:
            raise DefinitionError("invalid_reference")
        return target_path, item.limit_name

    resolved = {
        path: [(resolve(path, item), item.tokens) for item in items]
        for path, items in in_limits.items()
    }
    expected = {}
    in_flight = set()
    for path, state in runtime.items():
        if state.kind != "task":
            continue
        flow_path = f"/{path.split('/')[1]}"
        if state.definition_generation != runtime[flow_path].definition_generation:
            raise DefinitionError("invalid_runtime")
        if state.state.status not in (
            NodeStatus.submitted.value,
            NodeStatus.active.value,
        ):
            continue
        in_flight.add(path)
        if not state.attempt_id or not state.try_no:
            raise DefinitionError("invalid_runtime")
        seen = set()
        current = path
        while True:
            for key, tokens in resolved[current]:
                if key not in seen:
                    expected.setdefault(key, {})[path] = tokens
                    seen.add(key)
            if current == "/":
                break
            current = dirname(current) or "/"

    for key, (_, usage) in limits.items():
        holders = expected.get(key, {})
        if set(usage.node_paths) != set(holders) or usage.value != sum(
            holders.values()
        ):
            raise DefinitionError("invalid_runtime")
    _same_names(in_flight, document.secrets.job_passwords)
    for path in in_flight:
        if document.secrets.job_passwords[path].attempt_id != runtime[path].attempt_id:
            raise DefinitionError("invalid_secret")


def capture_checkpoint(
    bunch: Bunch, *, registry=None, saved_at: str | None = None
) -> CheckpointDocument:
    """Capture one in-memory instant; credentials exist only in `secrets`."""
    registry = registry or get_registry()
    with using_registry(registry):
        try:
            definition = export_definition(
                bunch, registry=registry, checkpoint_root=True
            )
            live_nodes = {_path(node): node for node in walk(bunch)}
            nodes = {
                path: _capture_node(node, registry) for path, node in live_nodes.items()
            }
            passwords = {}
            for node in live_nodes.values():
                if isinstance(node, Task) and node.state.node_status in _IN_FLIGHT:
                    if not node.attempt_id or not node.job_password:
                        raise DefinitionError("incomplete_checkpoint")
                    passwords[node.node_path] = JobSecret(
                        attempt_id=node.attempt_id, password=node.job_password
                    )
            document = CheckpointDocument(
                format_version=FORMAT_VERSION,
                saved_at=saved_at or datetime.now(timezone.utc).isoformat(),
                definition=definition.model_dump(mode="json"),
                runtime=RuntimeDocument(
                    server_state=RuntimeServerState.model_validate(
                        bunch.server_state.to_dict()
                    ),
                    nodes=nodes,
                ),
                secrets=SecretsDocument(job_passwords=passwords),
                source_address=SourceAddress.model_validate(
                    bunch.server_config.to_dict()
                ),
            )
            _validate_checkpoint_view(
                document, definition, registry, live_nodes=live_nodes
            )
            return document
        except DefinitionError:
            raise
        except Exception:
            raise DefinitionError("invalid_checkpoint") from None


def _same_names(actual, saved):
    if set(actual) != set(saved):
        raise DefinitionError("invalid_runtime")


def _apply_root(root: Bunch, runtime: NodeRuntime):
    attrs = runtime.root_attributes
    for item in attrs.meters:
        RuntimeMeter.model_validate(
            {**item.model_dump(), "value": runtime.meter_values[item.name]}
        )
    for item in attrs.limits:
        RuntimeLimit.model_validate(
            {**item.model_dump(), **runtime.limit_usage[item.name].model_dump()}
        )
    data = {
        "name": root.name,
        "state": runtime.state.model_dump(),
        "default_node_status": NodeStatus[attrs.default_node_status].value,
        "user_parameters": [param.to_dict() for param in root.user_parameters.values()],
        "trigger_free": runtime.trigger_free,
        "complete_trigger_free": runtime.complete_trigger_free,
        "is_complete_triggered": runtime.is_complete_triggered,
        "events": [
            {**item.model_dump(), "value": runtime.event_values[item.name]}
            for item in attrs.events
        ],
        "meters": [
            {**item.model_dump(), "value": runtime.meter_values[item.name]}
            for item in attrs.meters
        ],
        "limits": [
            {**item.model_dump(), **runtime.limit_usage[item.name].model_dump()}
            for item in attrs.limits
        ],
        "in_limit_manager": {
            "in_limit_list": [item.model_dump() for item in attrs.in_limits]
        },
        "times": [
            {"time": item.time, "free": runtime.time_free[item.time]}
            for item in attrs.times
        ],
    }
    if attrs.trigger is not None:
        data["trigger"] = attrs.trigger
    if attrs.complete_trigger is not None:
        data["complete_trigger"] = attrs.complete_trigger
    if attrs.repeat is not None:
        RuntimeRepeat.model_validate(
            {**attrs.repeat.model_dump(), "value": runtime.repeat_value}
        )
        data["repeat"] = {
            "r": {**attrs.repeat.model_dump(), "value": runtime.repeat_value}
        }
    root.restore_root_attributes(data)


def _apply_node(node, runtime: NodeRuntime, registry):
    entry = registry.by_type(type(node))
    if entry.kind != runtime.kind:
        raise DefinitionError("invalid_runtime")
    _same_names((item.name for item in node.events), runtime.event_values)
    _same_names((item.name for item in node.meters), runtime.meter_values)
    _same_names((item.name for item in node.limits), runtime.limit_usage)
    _same_names((item.time.strftime("%H:%M") for item in node.times), runtime.time_free)
    if (node.repeat is None) != (runtime.repeat_value is None):
        raise DefinitionError("invalid_runtime")
    if runtime.trigger_free and node.trigger_expression is None:
        raise DefinitionError("invalid_runtime")
    if runtime.complete_trigger_free and node.complete_trigger_expression is None:
        raise DefinitionError("invalid_runtime")
    if not isinstance(node, Bunch):
        node.state = State.from_dict(runtime.state.model_dump())
        if node.trigger_expression is not None:
            node.trigger_expression.free = runtime.trigger_free
        if node.complete_trigger_expression is not None:
            node.complete_trigger_expression.free = runtime.complete_trigger_free
        node.is_complete_triggered = runtime.is_complete_triggered
        for event in node.events:
            event.value = runtime.event_values[event.name]
        for meter in node.meters:
            RuntimeMeter.model_validate(
                {
                    "name": meter.name,
                    "min_value": meter.min_value,
                    "max_value": meter.max_value,
                    "value": runtime.meter_values[meter.name],
                }
            )
            meter.value = runtime.meter_values[meter.name]
        for limit in node.limits:
            saved = runtime.limit_usage[limit.name]
            if saved.value > limit.limit:
                raise DefinitionError("invalid_runtime")
            limit.value = saved.value
            limit.node_paths = set(saved.node_paths)
        if node.repeat is not None:
            RuntimeRepeat.model_validate(
                {**node.repeat.r.to_dict(), "value": runtime.repeat_value}
            )
            node.repeat.r.value = runtime.repeat_value
        for item in node.times:
            item.free = runtime.time_free[item.time.strftime("%H:%M")]
    if isinstance(node, Flow):
        node.begun = runtime.begun
        node.calendar = _restore_calendar(runtime.calendar)
        node.definition_generation = runtime.definition_generation
    if isinstance(node, Task):
        for key in _TASK_FIELDS:
            setattr(node, key, getattr(runtime, key))
        node.submission_unknown_reason = runtime.submission_unknown_reason
    if not entry.type_id.startswith("takler."):
        require_codec(entry, "runtime_schema", "restore_runtime")
        if entry.secret_fields & runtime.runtime_data.keys():
            raise DefinitionError("invalid_secret")
        typed = entry.runtime_schema.model_validate(runtime.runtime_data)
        entry.restore_runtime(node, typed)
    elif runtime.runtime_data:
        raise DefinitionError("invalid_runtime")


def restore_checkpoint(
    document: CheckpointDocument | dict | str | bytes, *, registry=None
) -> Bunch:
    """Decode an entire candidate tree; publish it only after this returns."""
    registry = registry or get_registry()
    with using_registry(registry):
        try:
            if isinstance(document, (str, bytes)):
                document = CheckpointDocument.model_validate_json(document)
            elif not isinstance(document, CheckpointDocument):
                document = CheckpointDocument.model_validate(document)
            root_runtime = document.runtime.nodes.get("/")
            if root_runtime is None or root_runtime.kind != "bunch":
                raise DefinitionError("invalid_runtime")
            root = build_definition(
                document.definition,
                registry=registry,
                initialize=lambda candidate: _apply_root(candidate, root_runtime),
            )
            if not isinstance(root, Bunch):
                raise DefinitionError("invalid_structure")
            definition = parse_definition(
                document.definition, model=registry.document_model()
            )
            _validate_checkpoint_view(document, definition, registry)
            nodes = {_path(node): node for node in walk(root)}
            _same_names(nodes, document.runtime.nodes)
            for path, node in nodes.items():
                _apply_node(node, document.runtime.nodes[path], registry)
            root.server_state = ServerState.from_dict(
                document.runtime.server_state.model_dump()
            )
            validate_references(root)
            bind_expressions(root)
            validate_occupancy(root)
            tasks = {
                path: node for path, node in nodes.items() if isinstance(node, Task)
            }
            in_flight = {
                path
                for path, node in tasks.items()
                if node.state.node_status in _IN_FLIGHT
            }
            _same_names(in_flight, document.secrets.job_passwords)
            for path in in_flight:
                secret = document.secrets.job_passwords[path]
                if secret.attempt_id != tasks[path].attempt_id:
                    raise DefinitionError("invalid_secret")
                tasks[path].job_password = secret.password
            return root
        except DefinitionError:
            raise
        except Exception:
            raise DefinitionError("invalid_checkpoint") from None
