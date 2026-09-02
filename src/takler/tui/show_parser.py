"""Reconstruct a :class:`takler.core.Bunch` from the show JSON payload.

The server returns ``Bunch.to_dict()`` serialised as JSON. Rather than
parse that dict ourselves, we feed it back through ``Bunch.from_dict``
to obtain a fully-fledged ``Bunch`` tree. The TUI then operates on the
real domain objects (``Bunch`` / ``Flow`` / ``Task`` / ``Parameter`` /
``Event`` / ``Meter`` / ``Limit`` / ``Repeat`` / ``TimeAttribute``),
reusing all of the existing helpers (parameter inheritance, path
lookup, etc.) instead of reimplementing them on top of dicts.

:class:`NodeInfo` is a lightweight, view-only wrapper around a single
``Node`` so the tabs can remain decoupled from the core API.
:class:`ShowSnapshot` wraps the ``Bunch`` and provides path-based
lookups expected by the TUI.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from takler.core import Bunch
from takler.core.node import Node


@dataclass
class NodeInfo:
    """Read-only view over a :class:`Node` for the TUI tabs."""

    node: Node
    level: int
    parent_path: Optional[str]
    children: List[str]

    # Cached, simple-typed projections used by the views. These are
    # filled once at snapshot construction time so tabs don't need to
    # deal with takler core types directly.
    user_parameters: Dict[str, str] = field(default_factory=dict)
    events: List[Tuple[str, str]] = field(default_factory=list)  # (name, set/unset)
    meters: List[Tuple[str, str, str, str]] = field(default_factory=list)
    limits: List[Tuple[str, str]] = field(default_factory=list)
    in_limits: List[Tuple[str, int, Optional[str]]] = field(  # (name, tokens, ref)
        default_factory=list
    )
    times: List[str] = field(default_factory=list)
    trigger: Optional[str] = None
    complete_trigger: Optional[str] = None
    repeat: Optional[str] = None

    @property
    def name(self) -> str:
        return self.node.name

    @property
    def path(self) -> str:
        return self.node.node_path

    @property
    def class_name(self) -> str:
        return type(self.node).__name__

    @property
    def state(self) -> str:
        return self.node.state.node_status.name

    @property
    def suspended(self) -> bool:
        return bool(self.node.state.suspended)

    @property
    def display_state(self) -> str:
        return f"suspend ({self.state})" if self.suspended else self.state

    @property
    def is_root(self) -> bool:
        return self.level == 0

    @property
    def all_parameters(self) -> Dict[str, str]:
        """User parameters defined on this node only.

        Tabs that want inherited parameters should use
        :meth:`ShowSnapshot.lookup_parameter` instead.
        """
        return dict(self.user_parameters)


@dataclass
class ShowSnapshot:
    """Reconstructed bunch state plus a path-indexed view of its nodes."""

    bunch: Bunch
    nodes: Dict[str, NodeInfo]
    roots: List[str]

    def get(self, path: str) -> Optional[NodeInfo]:
        return self.nodes.get(path)

    def find_node(self, path: str) -> Optional[Node]:
        """Return the underlying domain :class:`Node` for ``path``."""
        return self.bunch.find_node(path)

    def parents_of(self, path: str) -> List[NodeInfo]:
        """Ancestor :class:`NodeInfo` records, immediate parent first."""
        info = self.nodes.get(path)
        if info is None:
            return []
        result: List[NodeInfo] = []
        current = info
        while current.parent_path is not None:
            parent = self.nodes.get(current.parent_path)
            if parent is None:
                break
            result.append(parent)
            current = parent
        return result

    def lookup_parameter(self, path: str, name: str) -> Optional[str]:
        """Resolve a parameter using takler's standard lookup chain.

        Walks the node up to the root, then falls back to the bunch's
        server parameters; this matches the semantics of
        :meth:`Node.find_parent_parameter`.
        """
        node = self.bunch.find_node(path)
        if node is None:
            return None
        param = node.find_parent_parameter(name)
        if param is None:
            return None
        return None if param.value is None else str(param.value)

    @property
    def server_parameters(self) -> Dict[str, str]:
        """Bunch-level parameters as a plain dict (for diagnostics)."""
        return _params_from_node(self.bunch)


# ---------------------------------------------------------------------------
# Construction


def _params_from_node(node: Node) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for name, param in node.user_parameters_only().items():
        out[str(name)] = "" if param.value is None else str(param.value)
    return out


def _format_repeat(node: Node) -> Optional[str]:
    repeat = node.repeat
    if repeat is None:
        return None
    inner = repeat.r
    name = getattr(inner, "name", "")
    value = getattr(inner, "value", "")
    start = getattr(inner, "start_date", "")
    end = getattr(inner, "end_date", "")
    return f"{name} {value} [{start}, {end}]"


def _summarise_node(
    node: Node,
    level: int,
    parent_path: Optional[str],
) -> NodeInfo:
    info = NodeInfo(
        node=node,
        level=level,
        parent_path=parent_path,
        children=[child.node_path for child in node.children],
    )

    info.user_parameters = _params_from_node(node)

    if node.trigger_expression is not None:
        info.trigger = node.trigger_expression.expression_str
    if node.complete_trigger_expression is not None:
        info.complete_trigger = node.complete_trigger_expression.expression_str

    info.times = [t.time.strftime("%H:%M") for t in node.times]
    info.repeat = _format_repeat(node)

    for ev in node.events:  # type: Event
        info.events.append((ev.name, "set" if ev.value else "unset"))

    for meter in node.meters:  # type: Meter
        info.meters.append(
            (
                meter.name,
                str(meter.min_value),
                str(meter.max_value),
                str(meter.value),
            )
        )

    for limit in node.limits:
        info.limits.append((limit.name, f"{limit.value}/{limit.limit}"))

    for in_limit in node.in_limit_manager.in_limit_list:
        info.in_limits.append(
            (
                in_limit.limit_name,
                int(in_limit.tokens),
                in_limit.node_path,
            )
        )

    return info


def _walk(
    node: Node,
    level: int,
    parent_path: Optional[str],
    nodes: Dict[str, NodeInfo],
) -> NodeInfo:
    info = _summarise_node(node, level, parent_path)
    nodes[info.path] = info
    for child in node.children:
        _walk(child, level + 1, info.path, nodes)
    return info


def parse_show(payload: str) -> ShowSnapshot:
    """Reconstruct a :class:`Bunch` from the JSON payload.

    Parameters
    ----------
    payload
        JSON string returned by ``RunRequestShow``.

    Returns
    -------
    ShowSnapshot
    """
    data = json.loads(payload)
    bunch = Bunch.from_dict(data)
    # ``Bunch.from_dict`` does not reattach flows to the bunch
    # automatically for the purposes of ``find_node``; ensure linkage.
    for flow in bunch.flows.values():
        flow.bunch = bunch

    nodes: Dict[str, NodeInfo] = {}
    roots: List[str] = []
    for flow in bunch.flows.values():
        info = _walk(flow, level=0, parent_path=None, nodes=nodes)
        roots.append(info.path)

    return ShowSnapshot(bunch=bunch, nodes=nodes, roots=roots)


# Public re-exports kept for callers (e.g. tabs).
__all__ = ["NodeInfo", "ShowSnapshot", "parse_show"]
