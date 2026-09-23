"""Read-only, allowlisted projection of builtins into a pure definition."""

from takler.core import Bunch, Flow, NodeContainer, Task, NodeStatus
from takler.core.repeat import RepeatDate
from takler.tasks.shell.shell_script_task import ShellScriptTask
from takler.schema.definition import (
    DefinitionDocument,
    DefinitionError,
    parse_definition,
)


_BUILTIN_TYPES = {
    Bunch: "takler.bunch",
    Flow: "takler.flow",
    NodeContainer: "takler.container",
    Task: "takler.task",
    ShellScriptTask: "takler.shell",
}


def _project(node, path):
    type_id = _BUILTIN_TYPES.get(type(node))
    if type_id is None:
        raise DefinitionError("unregistered_type", path)
    result = dict(
        type_id=type_id,
        name=node.name,
        user_parameters=[
            dict(name=p.name, value=p.value) for p in node.user_parameters.values()
        ],
    )
    if type(node) is Bunch:
        if (
            node.children
            or node.trigger_expression is not None
            or node.complete_trigger_expression is not None
            or node.events
            or node.meters
            or node.limits
            or node.in_limit_manager.in_limit_list
            or node.repeat is not None
            or node.times
            or node.default_node_status != NodeStatus.queued
        ):
            raise DefinitionError("unsupported_root_attribute", path)
        result["flows"] = [
            _project(flow, f"{path}.flows[{i}]")
            for i, flow in enumerate(node.flows.values())
        ]
        return result
    for time in node.times:
        if time.time.second or time.time.microsecond or time.time.tzinfo is not None:
            raise DefinitionError("invalid_field", f"{path}.times")
    result.update(
        default_node_status=node.default_node_status.name,
        children=[
            _project(child, f"{path}.children[{i}]")
            for i, child in enumerate(node.children)
        ],
        trigger=None
        if node.trigger_expression is None
        else node.trigger_expression.expression_str,
        complete_trigger=None
        if node.complete_trigger_expression is None
        else node.complete_trigger_expression.expression_str,
        events=[dict(name=e.name, initial_value=e.initial_value) for e in node.events],
        meters=[
            dict(name=m.name, min_value=m.min_value, max_value=m.max_value)
            for m in node.meters
        ],
        limits=[dict(name=item.name, limit=item.limit) for item in node.limits],
        in_limits=[
            dict(
                limit_name=item.limit_name, node_path=item.node_path, tokens=item.tokens
            )
            for item in node.in_limit_manager.in_limit_list
        ],
        times=[dict(time=t.time.isoformat(timespec="minutes")) for t in node.times],
    )
    if node.repeat is not None:
        repeat = node.repeat.r
        if type(repeat) is not RepeatDate:
            raise DefinitionError("unregistered_type", f"{path}.repeat")
        result["repeat"] = dict(
            type_id="takler.repeat.date",
            name=repeat.name,
            start_date=f"{repeat.start:08d}",
            end_date=f"{repeat.end:08d}",
            step=repeat.step,
        )
    if type(node) is ShellScriptTask:
        result["script_path"] = (
            None if node.script_path is None else str(node.script_path)
        )
    return result


def export_definition(root: Bunch | Flow) -> DefinitionDocument:
    """Return a validated document without reading files or changing runtime.

    Serialize with ``model_dump(mode="json")`` or ``model_dump_json()``.
    Unknown subclasses are rejected, never downgraded to a builtin task.
    """
    if isinstance(root, (Bunch, Flow)) and type(root) not in _BUILTIN_TYPES:
        raise DefinitionError("unregistered_type")
    if type(root) not in (Bunch, Flow):
        raise DefinitionError("invalid_structure")
    return parse_definition(
        dict(kind="takler.definition", schema_version=1, root=_project(root, "$.root"))
    )
