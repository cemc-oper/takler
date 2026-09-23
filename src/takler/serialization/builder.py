"""Isolated construction and reference binding. No IO or scheduling calls."""

from takler.core import Bunch, Flow, NodeStatus, Parameter, RepeatDate
from takler.schema.definition import (
    DefinitionDocument,
    DefinitionError,
    parse_definition,
    _decode_json,
)
from .registry import (
    get_registry,
    using_registry,
    require_codec,
    safe_codec,
    validate_definition_data,
)


def walk(root):
    yield root
    for child in root.children:
        yield from walk(child)
    if isinstance(root, Bunch):
        for flow in root.flows.values():
            yield from walk(flow)


def validate_references(root, existing_bunch=None):
    """Bind only candidate references; the existing tree is read-only."""
    nodes = {node.node_path: node for node in walk(root)}
    for node in walk(root):
        for item in node.in_limit_manager.in_limit_list:
            target = None
            if item.node_path is None:
                target = node.find_limit_up(item.limit_name)
            else:
                path = item.node_path
                if (
                    not path
                    or path == "/"
                    or "//" in path
                    or path.endswith("/")
                    or ":" in path
                    or "\0" in path
                ):
                    raise DefinitionError("invalid_reference")
                if path.startswith("/"):
                    referenced = nodes.get(path)
                    # A missing path inside the candidate flow must not fall back
                    # to an old instance of the same flow during replace.
                    candidate_names = (
                        set(root.flows)
                        if isinstance(root, Bunch)
                        else ({root.name} if isinstance(root, Flow) else set())
                    )
                    if (
                        referenced is None
                        and existing_bunch is not None
                        and path.split("/")[1] not in candidate_names
                    ):
                        referenced = existing_bunch.find_node(path)
                else:
                    referenced = node.find_node(path)
                if referenced is not None:
                    target = referenced.find_limit(item.limit_name)
            if target is None:
                raise DefinitionError("unresolved_reference")
            if item.tokens > target.limit:
                raise DefinitionError("invalid_reference")
            item.limit = target


def _construct(data, registry):
    entry = registry.by_id(data.type_id)
    require_codec(entry, "definition_schema", "construct")
    if data.type_id.startswith("takler."):
        node = entry.python_type(name=data.name)
    else:
        validate_definition_data(
            data.type_data.model_dump(mode="json"), entry.secret_fields
        )
        node = entry.construct(data.name, data.type_data)
        if type(node) is not entry.python_type or node.name != data.name:
            raise DefinitionError("invalid_structure")
    for param in data.user_parameters:
        node.add_parameter(Parameter(param.name, param.value))
    if entry.kind == "bunch":
        for flow in data.flows:
            node.add_flow(_construct(flow, registry))
        return node
    node.default_node_status = NodeStatus[data.default_node_status]
    for key, add in (
        ("trigger", node.add_trigger),
        ("complete_trigger", node.add_complete_trigger),
    ):
        value = getattr(data, key)
        if value is not None:
            try:
                add(value, parse=False)
            except Exception:
                raise DefinitionError("invalid_field") from None
    for event in data.events:
        node.add_event(event.name, event.initial_value)
    for meter in data.meters:
        node.add_meter(meter.name, meter.min_value, meter.max_value)
    for limit in data.limits:
        node.add_limit(limit.name, limit.limit)
    for item in data.in_limits:
        node.add_in_limit(item.limit_name, item.node_path, item.tokens)
    if data.repeat is not None:
        repeat = data.repeat
        node.add_repeat(
            RepeatDate(repeat.name, repeat.start_date, repeat.end_date, repeat.step)
        )
    for time in data.times:
        node.add_time(time.time)
    if data.type_id == "takler.shell":
        node.script_path = data.script_path
    for child in data.children:
        node.append_child(_construct(child, registry))
    return node


@safe_codec
def build_definition(document, *, registry=None, existing_bunch=None):
    """Build a new unbegun Flow/Bunch; never attach it to an online Bunch."""
    registry = registry or get_registry()
    if isinstance(document, DefinitionDocument):
        document = document.model_dump(mode="json")
    if isinstance(document, (str, bytes)):
        document = _decode_json(document)
    if (
        isinstance(document, dict)
        and document.get("kind") == "takler.definition"
        and type(document.get("schema_version")) is int
        and document["schema_version"] == 1
    ):

        def check_types(item):
            if isinstance(item, dict):
                entry = registry.by_id(item.get("type_id"))
                require_codec(entry, "definition_schema", "construct")
                for field in ("children", "flows"):
                    children = item.get(field, [])
                    if isinstance(children, list):
                        for child in children:
                            check_types(child)

        check_types(document.get("root"))
    with using_registry(registry):
        data = parse_definition(document, model=registry.document_model())
        root = _construct(data.root, registry)
        bind_expressions(root, existing_bunch)
        validate_references(root, existing_bunch)
        return root


def bind_expressions(root, existing_bunch=None):
    """Parse after attachment, binding caches only on the new AST objects."""
    from takler.core.expression_ast import AstRoot, AstNodePath, AstVariablePath

    nodes = {node.node_path: node for node in walk(root)}
    candidate_names = (
        set(root.flows)
        if isinstance(root, Bunch)
        else ({root.name} if isinstance(root, Flow) else set())
    )

    def bind(ast, owner):
        if isinstance(ast, AstRoot):
            bind(ast.left, owner)
            bind(ast.right, owner)
        elif isinstance(ast, AstNodePath):
            path = ast.node_path
            if path.startswith("/"):
                referenced = nodes.get(path)
                if (
                    referenced is None
                    and existing_bunch is not None
                    and path.split("/")[1] not in candidate_names
                ):
                    referenced = existing_bunch.find_node(path)
            else:
                referenced = owner.find_node(path)
            if referenced is None:
                raise DefinitionError("unresolved_reference")
            ast.parent_node = owner
            ast._reference_node = referenced
        elif isinstance(ast, AstVariablePath):
            bind(ast.node, owner)
            if ast.get_variable() is None:
                raise DefinitionError("unresolved_reference")

    for node in walk(root):
        for expression in (node.trigger_expression, node.complete_trigger_expression):
            if expression is not None:
                try:
                    expression.parse_expression()
                    bind(expression.ast, node)
                except DefinitionError:
                    raise
                except Exception:
                    raise DefinitionError("invalid_field") from None
