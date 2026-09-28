"""Prepare a replacement off-tree, then commit on the scheduler's single writer.

No await, checkpoint, job submission or logging belongs in the commit section.
Transport exposure and operator audit are handled separately by R0-14.
"""

from takler.core import Flow, NodeStatus
from takler.core.expression import Expression
from takler.core.expression_ast import AstRoot, AstNodePath, AstVariablePath
from takler.exceptions import (
    ExpressionSyntaxError,
    FlowStateError,
    InvalidNodePathError,
    InvalidRequestError,
    NodeNotFoundError,
    NodeTypeError,
    UnsupportedValueError,
)
from takler.protocol.commands import ServiceResponse
from takler.schema import DefinitionError
from takler.serialization import build_definition
from takler.serialization.builder import walk


def _target(bunch, path):
    if (
        not path.startswith("/")
        or any(part in ("", ".", "..") for part in path[1:].split("/"))
        or ":" in path
        or "\0" in path
    ):
        raise InvalidNodePathError("invalid replace target path", node_path=path)
    node = bunch.find_node(path)
    if node is None:
        raise NodeNotFoundError("replace target not found", node_path=path)
    if not isinstance(node, Flow):
        raise NodeTypeError("replace target must be a Flow", node_path=path)
    return node


def _check_idle(old, bunch):
    for node in walk(old):
        if node.state.node_status in (NodeStatus.active, NodeStatus.submitted):
            raise FlowStateError(
                f"replace blocked by running node: {node.node_path}", flow_name=old.name
            )
        for limit in node.limits:
            if limit.value or limit.node_paths:
                raise FlowStateError(
                    f"replace blocked by occupied limit: {node.node_path}:{limit.name}",
                    flow_name=old.name,
                )
    # A stale reservation owned by this flow on an external limit cannot be
    # silently inherited by a new task at the same path either.
    prefix = old.node_path + "/"
    for node in walk(bunch):
        for limit in node.limits:
            if any(
                p == old.node_path or p.startswith(prefix) for p in limit.node_paths
            ):
                raise FlowStateError(
                    f"replace blocked by reservation: {node.node_path}:{limit.name}",
                    flow_name=old.name,
                )


def _reference_updates(bunch, old, candidate):
    """Return assignments without touching online caches or reservation counts.

    Reparse expressions into isolated ASTs (including variable caches) and
    resolve every in-limit against the prospective tree. Missing existing
    references outside the replaced flow are left alone; this is not a global
    dependency repair operation.
    """
    new_nodes = {node.node_path: node for node in walk(candidate)}
    prefix = old.node_path + "/"
    updates = []

    def replaced(path):
        return path == old.node_path or path.startswith(prefix)

    def resolve(owner, path):
        if path.startswith("/"):
            if replaced(path):
                return new_nodes.get(path), True
            return bunch.find_node(path), False
        found = owner.find_node(path)
        if found is not None and replaced(found.node_path):
            return new_nodes.get(found.node_path), True
        return found, False

    def bind(ast, owner):
        affected = False
        if isinstance(ast, AstRoot):
            left = bind(ast.left, owner)
            right = bind(ast.right, owner)
            return left or right
        if isinstance(ast, AstNodePath):
            target, affected = resolve(owner, ast.node_path)
            if affected and target is None:
                raise InvalidRequestError("replacement removes a referenced node")
            ast.parent_node = owner
            ast._reference_node = target
        elif isinstance(ast, AstVariablePath):
            affected = bind(ast.node, owner)
            target = ast.node._reference_node
            variable = (
                None if target is None else target.find_variable(ast.variable_name)
            )
            if affected and variable is None:
                raise InvalidRequestError("replacement removes a referenced variable")
            ast._node_variable = variable
        return affected

    for node in walk(bunch):
        if node is old or node.get_flow() is old:
            continue
        for item in node.in_limit_manager.in_limit_list:
            if item.node_path is None:
                continue  # ancestor references cannot cross a Flow boundary
            target, affected = resolve(node, item.node_path)
            if affected:
                limit = None if target is None else target.find_limit(item.limit_name)
                if limit is None or item.tokens > limit.limit:
                    raise InvalidRequestError(
                        "replacement invalidates an in-limit reference"
                    )
                updates.append((item, "limit", limit))
        for expression in (node.trigger_expression, node.complete_trigger_expression):
            if expression is None:
                continue
            fresh = Expression(expression.expression_str)
            try:
                fresh.parse_expression()
            except Exception:
                # Existing unrelated invalid expressions must not block replace.
                continue
            if bind(fresh.ast, node):
                updates.append((expression, "ast", fresh.ast))
    return updates


def replace_flow(bunch, command):
    old = _target(bunch, command.target_path)

    def initialize(root):
        if not isinstance(root, Flow):
            raise DefinitionError("invalid_structure")
        if root.name != old.name:
            raise DefinitionError("name_mismatch")
        # Initialization runs before builder binds any online references.
        try:
            root.begin()
        except Exception:
            raise DefinitionError("initialization_failed") from None

    try:
        candidate = build_definition(
            command.flow_bytes, existing_bunch=bunch, initialize=initialize
        )
    except DefinitionError as exc:
        if exc.code in {
            "unknown_type",
            "unregistered_type",
            "unsupported_version",
            "missing_codec",
        }:
            raise UnsupportedValueError(f"invalid replacement: {exc.code}") from None
        if exc.code == "expression_syntax":
            raise ExpressionSyntaxError("invalid replacement expression") from None
        if exc.code in {"construction_failed", "initialization_failed"}:
            raise RuntimeError("replacement preparation failed") from None
        raise InvalidRequestError(f"invalid replacement: {exc.code}") from None

    updates = _reference_updates(bunch, old, candidate)
    name = old.name
    assignments = [
        (candidate.__dict__, "bunch", bunch),
        (bunch.flows, name, candidate),
        (old.__dict__, "bunch", None),
        *((obj.__dict__, attribute, value) for obj, attribute, value in updates),
    ]
    candidate_state = candidate.state.__dict__
    response = ServiceResponse(
        flag=0, message="flow replaced in memory; checkpoint pending"
    )
    # All extension initialization and reference preparation precede the final
    # gate. These checks and backing-dict writes run on the same single writer.
    if bunch.flows.get(name) is not old:
        raise FlowStateError(
            "replace target changed during preparation", flow_name=name
        )
    _check_idle(old, bunch)
    candidate_state["suspended"] = old.state.suspended
    # No extension __setattr__, add_flow, status propagation, user callback,
    # logger or IO runs here. Keys and plain backing dictionaries are prepared.
    for mapping, key, value in assignments:
        mapping[key] = value
    return response
