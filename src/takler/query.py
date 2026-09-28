"""Data-only show views shared by the CLI and TUI; never load execution types."""

from dataclasses import dataclass, field
import json

REDACTED = "<redacted>"
SENSITIVE_PARAMETERS = frozenset(
    {
        "takler_pass",
        "takler_secret",
        "job_password",
        "operator_secret",
        "takler-pass",
        "takler-secret",
    }
)
STATES = {
    1: "unknown",
    2: "complete",
    3: "queued",
    4: "submitted",
    5: "active",
    6: "aborted",
}


def display(value):
    return "" if value is None else str(value)


@dataclass(frozen=True)
class ParameterValue:
    value: str | int | float | bool | None
    redacted: bool = False

    @property
    def text(self):
        return REDACTED if self.redacted else display(self.value)


@dataclass
class NodeInfo:
    name: str
    path: str
    type_id: str
    node_kind: str
    state: str
    suspended: bool
    level: int
    parent_path: str | None
    children: list[str] = field(default_factory=list)
    parameters: dict[str, ParameterValue] = field(default_factory=dict)
    generated: dict[str, ParameterValue] = field(default_factory=dict)
    events: list = field(default_factory=list)
    meters: list = field(default_factory=list)
    limits: list = field(default_factory=list)
    in_limits: list = field(default_factory=list)
    times: list[str] = field(default_factory=list)
    trigger: str | None = None
    complete_trigger: str | None = None
    repeat: str | None = None
    task_id: str | None = None
    try_no: int | None = None
    aborted_reason: str | None = None
    script_path: str | None = None

    @property
    def class_name(self):
        return {
            "takler.flow": "Flow",
            "takler.container": "NodeContainer",
            "takler.task": "Task",
            "takler.shell": "ShellScriptTask",
        }.get(self.type_id, self.type_id)

    @property
    def display_state(self):
        return f"suspend ({self.state})" if self.suspended else self.state

    @property
    def is_root(self):
        return self.level == 0

    @property
    def user_parameters(self):
        return {k: v.text for k, v in self.parameters.items()}

    @property
    def generated_parameters(self):
        return {k: v.text for k, v in self.generated.items()}

    @property
    def all_parameters(self):
        return self.user_parameters


@dataclass
class ShowSnapshot:
    name: str
    nodes: dict[str, NodeInfo]
    roots: list[str]
    root_parameters: dict[str, ParameterValue]
    server_generated: dict[str, ParameterValue]

    def get(self, path):
        return self.nodes.get(path)

    def find_node(self, path):
        return self.get(path)

    def parents_of(self, path):
        result = []
        current = self.get(path)
        while current is not None and current.parent_path is not None:
            current = self.get(current.parent_path)
            if current is not None:
                result.append(current)
        return result

    def resolve_parameter(self, path, name):
        """Return a typed value, preserving missing, null and redacted distinctions."""
        node = self.get(path)
        if node is None:
            return None
        for item in [node, *self.parents_of(path)]:
            for source in (item.parameters, item.generated):
                if name in source:
                    return source[name]
        return self.root_parameters.get(name, self.server_generated.get(name))

    def lookup_parameter(self, path, name):
        value = self.resolve_parameter(path, name)
        # A redacted/null value occupies precedence but must not become a file path.
        return (
            None
            if value is None or value.redacted or value.value is None
            else value.text
        )

    def inherited_parameters(self, path):
        node = self.get(path)
        result = {}
        if node is None:
            return result
        seen = set(node.parameters) | set(node.generated)
        for ancestor in self.parents_of(path):
            for source in (ancestor.parameters, ancestor.generated):
                for name, value in source.items():
                    if name not in seen:
                        result[name] = value.text
                        seen.add(name)
        for source in (self.root_parameters, self.server_generated):
            for name, value in source.items():
                if name not in seen:
                    result[name] = value.text
                    seen.add(name)
        return result

    @property
    def server_parameters(self):
        return {k: v.text for k, v in self.root_parameters.items()}


def _object(value):
    if not isinstance(value, dict):
        raise ValueError("invalid_query")
    return value


def _list(data, key):
    result = data.get(key, [])
    if not isinstance(result, list):
        raise ValueError("invalid_query")
    return result


def _string(value):
    if not isinstance(value, str):
        raise ValueError("invalid_query")
    return value


def _parameters(data, key):
    redacted = {_string(n) for n in _list(data, "redacted_parameters")}
    result = {}
    for item in _list(data, key):
        item = _object(item)
        name = _string(item.get("name"))
        if "value" not in item or name in result:
            raise ValueError("invalid_query")
        value = item["value"]
        if value is not None and type(value) not in (str, int, float, bool):
            raise ValueError("invalid_query")
        hidden = name in redacted or name.lower() in SENSITIVE_PARAMETERS
        result[name] = ParameterValue(REDACTED if hidden else value, hidden)
    return result


def parse_show(payload: str) -> ShowSnapshot:
    """Parse display data without importing or constructing any execution object."""

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("invalid_query")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError("invalid_query")

    data = _object(
        json.loads(payload, object_pairs_hook=pairs, parse_constant=invalid_constant)
    )
    snapshot = ShowSnapshot(
        _string(data.get("name")),
        {},
        [],
        _parameters(data, "user_parameters"),
        _parameters(data, "generated_parameters"),
    )

    def walk(raw, parent=None, level=0):
        raw = _object(raw)
        name = _string(raw.get("name"))
        if not name or "/" in name or name in (".", ".."):
            raise ValueError("invalid_query")
        path = f"{parent or ''}/{name}"
        if path in snapshot.nodes:
            raise ValueError("invalid_query")
        state = _object(raw.get("state"))
        status = state.get("status")
        suspended = state.get("suspended")
        if (
            type(status) is not int
            or status not in STATES
            or type(suspended) is not bool
        ):
            raise ValueError("invalid_query")
        info = NodeInfo(
            name,
            path,
            _string(raw.get("type_id", "unknown")),
            _string(raw.get("node_kind", "unknown")),
            STATES[status],
            suspended,
            level,
            parent,
            parameters=_parameters(raw, "user_parameters"),
            generated=_parameters(raw, "generated_parameters"),
        )
        snapshot.nodes[path] = info
        for key in (
            "trigger",
            "complete_trigger",
            "task_id",
            "aborted_reason",
            "script_path",
        ):
            value = raw.get(key)
            if value is not None:
                _string(value)
            setattr(info, key, value)
        info.try_no = raw.get("try_no")
        if info.try_no is not None and type(info.try_no) is not int:
            raise ValueError("invalid_query")
        # Optional specialized attributes are plain data too. Missing fields
        # on custom types do not prevent the generic tree from being displayed.
        for ev in _list(raw, "events"):
            info.events.append(
                (_string(ev["name"]), "set" if ev.get("value", False) else "unset")
            )
        for meter in _list(raw, "meters"):
            info.meters.append(
                (
                    _string(meter["name"]),
                    display(meter.get("min_value", "")),
                    display(meter.get("max_value", "")),
                    display(meter.get("value", "")),
                )
            )
        for limit in _list(raw, "limits"):
            info.limits.append(
                (
                    _string(limit["name"]),
                    f"{limit.get('value', 0)}/{limit.get('limit', '')}",
                )
            )
        for limit in _list(_object(raw.get("in_limit_manager", {})), "in_limit_list"):
            info.in_limits.append(
                (
                    _string(limit["limit_name"]),
                    limit.get("tokens", 1),
                    limit.get("node_path"),
                )
            )
        for item in _list(raw, "times"):
            info.times.append(_string(item["time"]))
        if raw.get("repeat") is not None:
            repeat = _object(_object(raw["repeat"]).get("r", {}))
            name = _string(repeat.get("name", ""))
            hidden = name.lower() in SENSITIVE_PARAMETERS or name in raw.get(
                "redacted_parameters", []
            )
            value = REDACTED if hidden else repeat.get("value", "")
            info.repeat = f"{name} {value} [{repeat.get('start_date', '')}, {repeat.get('end_date', '')}]"
        for child in _list(raw, "children"):
            info.children.append(walk(child, path, level + 1))
        return path

    try:
        for flow in _list(data, "flows"):
            snapshot.roots.append(walk(flow))
    except (KeyError, TypeError, RecursionError) as exc:
        raise ValueError("invalid_query") from exc
    return snapshot
