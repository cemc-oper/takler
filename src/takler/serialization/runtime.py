"""Registered mixed-tree codecs and strict checkpoint runtime validation."""

from datetime import datetime
from typing import Annotated

from pydantic import Field, model_validator

from takler.core import Bunch, Flow, Task, NodeStatus, SerializationType
from takler.core.node import Node
from takler.core.bunch import ServerState
from takler.schema.definition import (
    DefinitionModel,
    DefinitionError,
    EventDefinition,
    MeterDefinition,
    LimitDefinition,
    InLimitDefinition,
    TimeDefinition,
    RepeatDateDefinition,
    unique,
)
from .registry import (
    get_registry,
    using_registry,
    require_codec,
    safe_codec,
    validate_definition_data,
    export_definition_data,
)
from .builder import walk, validate_references, bind_expressions


class RuntimeState(DefinitionModel):
    status: Annotated[int, Field(ge=1, le=6)]
    suspended: bool


class RuntimeEvent(EventDefinition):
    value: bool


class RuntimeMeter(MeterDefinition):
    value: int

    @model_validator(mode="after")
    def current(self):
        if not self.min_value <= self.value <= self.max_value:
            raise ValueError("invalid meter runtime")
        return self


class RuntimeLimit(LimitDefinition):
    value: Annotated[int, Field(ge=0)]
    node_paths: list[str]

    @model_validator(mode="after")
    def current(self):
        unique(self.node_paths)
        if self.value > self.limit:
            raise ValueError("invalid limit runtime")
        return self


class RuntimeTime(TimeDefinition):
    free: bool


class RuntimeRepeat(RepeatDateDefinition):
    value: int

    @model_validator(mode="after")
    def current(self):
        start = datetime.strptime(self.start_date, "%Y%m%d")
        end = datetime.strptime(self.end_date, "%Y%m%d")
        value = datetime.strptime(str(self.value), "%Y%m%d")
        if not start <= value <= end or (value - start).days % self.step:
            raise ValueError("invalid repeat runtime")
        return self


class RepeatWrapper(DefinitionModel):
    r: RuntimeRepeat


class InLimitWrapper(DefinitionModel):
    in_limit_list: list[InLimitDefinition]


class RuntimeCalendar(DefinitionModel):
    initial_time: str | None
    flow_time: str | None
    duration: float | None
    increment: float | None
    initial_real_time: str | None
    last_real_time: str | None

    @model_validator(mode="after")
    def dates(self):
        for key in ("initial_time", "flow_time", "initial_real_time", "last_real_time"):
            value = getattr(self, key)
            if value is not None:
                datetime.fromisoformat(value)
        return self


class RuntimeParameter(DefinitionModel):
    name: str
    value: str | int | float | bool | None


class RuntimeServer(DefinitionModel):
    host: str | None
    port: str | None
    parameters: list[RuntimeParameter]


class RuntimeNode(DefinitionModel):
    type_id: str
    name: str
    state: RuntimeState
    default_node_status: Annotated[int, Field(ge=2, le=3)] = 3
    user_parameters: list[RuntimeParameter] = Field(default_factory=list)
    children: list["RuntimeNode"] = Field(default_factory=list)
    events: list[RuntimeEvent] = Field(default_factory=list)
    meters: list[RuntimeMeter] = Field(default_factory=list)
    limits: list[RuntimeLimit] = Field(default_factory=list)
    in_limit_manager: InLimitWrapper = Field(
        default_factory=lambda: InLimitWrapper(in_limit_list=[])
    )
    trigger: str | None = None
    complete_trigger: str | None = None
    trigger_free: bool
    complete_trigger_free: bool
    is_complete_triggered: bool
    repeat: RepeatWrapper | None = None
    times: list[RuntimeTime] = Field(default_factory=list)
    # Conditional fields are required/forbidden by the registered kind below.
    flows: list["RuntimeNode"] = Field(default_factory=list)
    server_state: RuntimeServer | None = None
    begun: bool | None = None
    calendar: RuntimeCalendar | None = None
    task_id: str | None = None
    try_no: Annotated[int, Field(ge=0)] | None = None
    aborted_reason: str | None = None
    script_path: str | None = None
    type_data: dict = Field(default_factory=dict)
    runtime_data: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def invariants(self):
        registry = get_registry()
        entry = registry.by_id(self.type_id)
        if (
            (not self.name and entry.kind != "bunch")
            or self.name in {".", ".."}
            or any(c in self.name for c in "/:\0")
        ):
            raise ValueError("invalid node name")
        for items in (
            self.user_parameters,
            self.children,
            self.flows,
            self.events,
            self.meters,
            self.limits,
        ):
            unique([item.name for item in items])
        unique(
            [
                (item.node_path, item.limit_name)
                for item in self.in_limit_manager.in_limit_list
            ]
        )
        unique([item.time for item in self.times])
        for field in ("trigger", "complete_trigger"):
            expression = getattr(self, field)
            if expression == "" or (
                getattr(self, field + "_free") and expression is None
            ):
                raise ValueError("invalid trigger runtime")
        required = {
            "bunch": {"flows", "server_state"},
            "flow": {"begun", "calendar"},
            "task": {"task_id", "try_no", "aborted_reason"},
            "container": set(),
        }[entry.kind]
        conditional = {
            "flows",
            "server_state",
            "begun",
            "calendar",
            "task_id",
            "try_no",
            "aborted_reason",
            "script_path",
        }
        allowed = required | (
            {"script_path"} if self.type_id == "takler.shell" else set()
        )
        if (
            not required <= self.model_fields_set
            or (conditional - allowed) & self.model_fields_set
        ):
            raise ValueError("incomplete or invalid runtime")
        if entry.kind == "task" and (self.children or self.try_no is None):
            raise ValueError("invalid task runtime")
        if entry.kind == "flow" and (self.begun is None or self.calendar is None):
            raise ValueError("incomplete flow runtime")
        if entry.kind == "bunch" and self.server_state is None:
            raise ValueError("incomplete bunch runtime")
        if any(
            registry.by_id(child.type_id).kind not in {"container", "task"}
            for child in self.children
        ):
            raise ValueError("invalid children")
        if any(registry.by_id(flow.type_id).kind != "flow" for flow in self.flows):
            raise ValueError("invalid flows")
        if self.type_id.startswith("takler.") and (self.type_data or self.runtime_data):
            raise ValueError("unexpected builtin codec data")
        return self


def restore_node(data, method=SerializationType.Status):
    """Decode a registered mixed-tree node, without any data-driven imports.

    Checkpoint callers validate the complete tree before this lower-level codec.
    New definition documents must use build_definition instead.
    """
    registry = get_registry()
    if not isinstance(data, dict) or "class_type" in data:
        raise DefinitionError("invalid_field")
    entry = registry.by_id(data.get("type_id"))
    require_codec(entry, "definition_schema", "construct")
    if entry.type_id.startswith("takler."):
        node = entry.python_type(name=data["name"])
    else:
        typed = entry.definition_schema.model_validate(data.get("type_data", {}))
        validate_definition_data(typed.model_dump(mode="json"), entry.secret_fields)
        node = entry.construct(data["name"], typed)
        if type(node) is not entry.python_type:
            raise DefinitionError("invalid_structure")
    # Always call a trusted builtin base codec; extension runtime is explicit.
    cls = {"bunch": Node, "flow": Flow, "container": Node, "task": Task}[entry.kind]
    cls.fill_from_dict(data, node, method=method)
    if entry.type_id == "takler.shell":
        node.script_path = data.get("script_path")
    if not entry.type_id.startswith("takler.") and method == SerializationType.Status:
        require_codec(entry, "runtime_schema", "restore_runtime")
        runtime = entry.runtime_schema.model_validate(data.get("runtime_data", {}))
        entry.restore_runtime(node, runtime)
    if isinstance(node, Bunch):
        node.server_state = ServerState.from_dict(data["server_state"])
        for flow in data["flows"]:
            restored = restore_node(flow, method)
            if not isinstance(restored, Flow):
                raise DefinitionError("invalid_structure")
            node.add_flow(restored)
    return node


@safe_codec
def export_runtime(root):
    """Write required runtime even when false; keep the existing mixed tree."""

    def project(node):
        entry = get_registry().by_type(type(node))
        require_codec(
            entry,
            "definition_schema",
            "export_definition",
            "runtime_schema",
            "export_runtime",
        )
        item = dict(
            type_id=entry.type_id,
            name=node.name,
            state=node.state.to_dict(),
            trigger_free=bool(node.trigger_expression and node.trigger_expression.free),
            complete_trigger_free=bool(
                node.complete_trigger_expression
                and node.complete_trigger_expression.free
            ),
            is_complete_triggered=node.is_complete_triggered,
        )
        if node.default_node_status != NodeStatus.queued:
            item["default_node_status"] = node.default_node_status.value
        if node.user_parameters:
            item["user_parameters"] = [
                dict(name=p.name, value=p.value) for p in node.user_parameters.values()
            ]
        for field, expression in (
            ("trigger", node.trigger_expression),
            ("complete_trigger", node.complete_trigger_expression),
        ):
            if expression is not None:
                item[field] = expression.expression_str
        for field in ("events", "meters", "limits", "times"):
            values = getattr(node, field)
            if values:
                item[field] = [value.to_dict() for value in values]
        if node.in_limit_manager.in_limit_list:
            item["in_limit_manager"] = node.in_limit_manager.to_dict()
        if node.repeat is not None:
            item["repeat"] = node.repeat.to_dict()
        if node.children:
            item["children"] = [project(child) for child in node.children]
        if entry.kind == "bunch":
            item["flows"] = [project(flow) for flow in node.flows.values()]
            item["server_state"] = node.server_state.to_dict()
        if entry.kind == "flow":
            item.update(begun=node.begun, calendar=node.calendar.to_dict())
        if entry.kind == "task":
            item.update(
                task_id=node.task_id,
                try_no=node.try_no,
                aborted_reason=node.aborted_reason,
            )
        if entry.type_id == "takler.shell":
            item["script_path"] = (
                None if node.script_path is None else str(node.script_path)
            )
        if not entry.type_id.startswith("takler."):
            item["type_data"] = export_definition_data(entry, node)
            item["runtime_data"] = entry.runtime_schema.model_validate(
                entry.export_runtime(node)
            ).model_dump(mode="json")
        return item

    return project(root)


def restore_runtime(data, *, registry=None):
    """Validate and restore a complete snapshot tree without scheduling."""
    registry = registry or get_registry()
    with using_registry(registry):
        try:

            def check_types(item):
                if not isinstance(item, dict):
                    raise DefinitionError("invalid_runtime")
                registry.by_id(item.get("type_id"))
                for child in item.get("children", []) + item.get("flows", []):
                    check_types(child)

            check_types(data)
            RuntimeNode.model_validate(data)
            root = restore_node(data)
            validate_references(root)
            bind_expressions(root)
            validate_occupancy(root)
            return root
        except DefinitionError:
            raise
        except Exception:
            raise DefinitionError("invalid_runtime") from None


def validate_occupancy(root):
    nodes = list(walk(root))
    paths = [node.node_path for node in nodes if not isinstance(node, Bunch)]
    if len(paths) != len(set(paths)):
        raise DefinitionError("invalid_structure")
    expected = {}
    for node in nodes:
        if not isinstance(node, Task) or node.state.node_status not in (
            NodeStatus.active,
            NodeStatus.submitted,
        ):
            continue
        seen = set()
        current = node
        while current is not None:
            for item in current.in_limit_manager.in_limit_list:
                key = id(item.limit)
                if key not in seen:
                    expected.setdefault(key, {})[node.node_path] = item.tokens
                    seen.add(key)
            current = current.parent
    for node in nodes:
        for limit in node.limits:
            holders = expected.get(id(limit), {})
            if set(holders) != limit.node_paths or sum(holders.values()) != limit.value:
                raise DefinitionError("invalid_runtime")
