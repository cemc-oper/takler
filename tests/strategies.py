"""Shared hypothesis strategies for the m1-operational-baseline property tests.

Import them with the ``tests`` package prefix, which works from any test
directory under the configured ``--import-mode=importlib``::

    from tests.strategies import bunches, flow_operation_sequences

What lives here
---------------

* :func:`bunches` / :func:`flows` build real ``Bunch`` / ``Flow`` object trees
  (depth ``<= 3``) carrying randomised attributes *and* randomised runtime
  state, so a single generator can feed the snapshot round trip, the restore
  and the address verification properties.
* :func:`flow_operation_sequences` builds a ``Flow`` together with a list of
  :class:`FlowOperation` to replay on it. ``begin`` can be excluded, which is
  what separates "calendar is only started by begin" (no begin in the
  sequence) from "begun holds iff the calendar has an initial time" (begin
  included).
* The small input-validation and error-classification generators:
  :func:`invalid_node_paths`, :func:`nonexistent_node_paths`,
  :func:`takler_exception_types`, :func:`foreign_exception_types` and
  :func:`invalid_trigger_expressions`.

Two invariants are baked into the generators on purpose:

* ``flow.begun`` is true **iff** ``flow.calendar.initial_time`` is not ``None``
  (requirement 8.6). ``begun`` and the calendar are always drawn as a pair, so
  no generated value can violate it.
* Everything stays small and cheap: the generators are meant to run under
  ``@settings(max_examples=100, deadline=None)``.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type

from hypothesis import strategies as st

from takler import exceptions as takler_exceptions
from takler.core import (
    Bunch,
    Flow,
    NodeContainer,
    NodeStatus,
    RepeatDate,
    SerializationType,
    Task,
)
from takler.core.expression_parser import parse_trigger
from takler.core.node import Node
from takler.exceptions import ExpressionSyntaxError
from takler.tasks import ShellScriptTask

__all__ = [
    "MAX_TREE_DEPTH",
    "NON_BEGIN_OPERATIONS",
    "FlowOperation",
    "apply_flow_operation",
    "apply_flow_operations",
    "bunches",
    "flows",
    "flow_operation_sequences",
    "invalid_node_paths",
    "nonexistent_node_paths",
    "takler_exception_types",
    "foreign_exception_types",
    "invalid_trigger_expressions",
    "node_paths_of",
]

#: Maximum depth of a generated node tree, counting the ``Flow`` itself as 1.
MAX_TREE_DEPTH = 3

#: ``script_path`` samples for ``ShellScriptTask``: ``None``, plain, with
#: spaces, and non ASCII.
SCRIPT_PATHS: Tuple[Optional[str], ...] = (
    None,
    "scripts/task1.takler",
    "/tmp/takler scripts/task 1.takler",
    "/tmp/takler/脚本 1.takler",
    "脚本/任务.takler",
)

#: Statuses a generated node may carry. ``unknown`` is included because that is
#: what a flow that has never begun looks like.
NODE_STATUSES: Tuple[NodeStatus, ...] = (
    NodeStatus.unknown,
    NodeStatus.complete,
    NodeStatus.queued,
    NodeStatus.submitted,
    NodeStatus.active,
    NodeStatus.aborted,
)

#: Valid trigger expressions used as the seed of
#: :func:`invalid_trigger_expressions`.
VALID_TRIGGER_EXPRESSIONS: Tuple[str, ...] = (
    "/flow1/task1 == complete",
    "/flow1/task1 == aborted",
    "/flow1/container1/task2 == active",
    "/flow1/task1 == complete and /flow1/task2 == complete",
    "/flow1/task1 == complete or /flow1/task2 == aborted",
    "(/flow1/task1 == complete) and /flow1/task2 == complete",
    "/flow1/task1:event1 == set",
    "/flow1/task1:event1 == unset",
    "/flow1/task1:meter1 >= 10",
    "/flow1/task1:meter1 < 20",
)

#: Characters that are not part of the trigger grammar.
_ILLEGAL_TRIGGER_CHARS: Tuple[str, ...] = ("#", "@", "!", "%", "&", "$", "?", "\\", "'", '"')

#: Every takler owned exception type, used by the Error_Code properties.
TAKLER_EXCEPTION_TYPES: Tuple[Type[BaseException], ...] = tuple(
    getattr(takler_exceptions, name) for name in takler_exceptions.__all__
)

#: Exception types that are *not* takler owned, i.e. must map to the internal
#: error code.
FOREIGN_EXCEPTION_TYPES: Tuple[Type[BaseException], ...] = (
    ValueError,
    RuntimeError,
    KeyError,
    OSError,
    TypeError,
    IndexError,
    AttributeError,
    ZeroDivisionError,
    NotImplementedError,
)


# Names ---------------------------------------------------------------------
#
# Node names must match the trigger grammar's ``pure_node_name``
# ((LETTER|DIGIT)("_"|LETTER|DIGIT)*), so they are built from a fixed pattern
# instead of free text. Uniqueness inside a parent is guaranteed by the index.


def _flow_name(index: int) -> str:
    return f"flow{index + 1}"


def _container_name(index: int) -> str:
    return f"container{index + 1}"


def _task_name(index: int) -> str:
    return f"task{index + 1}"


def node_paths_of(node: Node) -> List[str]:
    """Return the node paths of ``node`` and all its descendants, pre-order."""
    paths = [node.node_path]
    for child in node.children:
        paths.extend(node_paths_of(child))
    return paths


def _iter_nodes(node: Node) -> List[Node]:
    """Return ``node`` and all its descendants, pre-order."""
    nodes = [node]
    for child in node.children:
        nodes.extend(_iter_nodes(child))
    return nodes


# Node tree -----------------------------------------------------------------


def _draw_attributes(draw, node: Node, allow_repeat: bool) -> None:
    """Attach randomised definition attributes (no runtime state yet)."""
    for index in range(draw(st.integers(min_value=0, max_value=2))):
        node.add_event(f"event{index + 1}", initial_value=draw(st.booleans()))

    for index in range(draw(st.integers(min_value=0, max_value=1))):
        max_value = draw(st.integers(min_value=1, max_value=10))
        node.add_meter(f"meter{index + 1}", min_value=0, max_value=max_value)

    for index in range(draw(st.integers(min_value=0, max_value=1))):
        node.add_limit(f"limit{index + 1}", draw(st.integers(min_value=1, max_value=3)))

    for hour, minute in draw(
        st.lists(
            st.tuples(
                st.integers(min_value=0, max_value=23),
                st.integers(min_value=0, max_value=59),
            ),
            min_size=0,
            max_size=2,
            unique=True,
        )
    ):
        node.add_time(f"{hour:02d}:{minute:02d}")

    if allow_repeat and draw(st.booleans()):
        step = draw(st.integers(min_value=1, max_value=3))
        node.add_repeat(
            RepeatDate(
                name="YMD",
                start_date=20240101,
                end_date=20240120,
                step=step,
            )
        )


def _draw_subtree(draw, parent: NodeContainer, depth: int, allow_shell_tasks: bool) -> None:
    """Fill ``parent`` with children, keeping the total depth <= MAX_TREE_DEPTH."""
    task_count = draw(st.integers(min_value=1, max_value=2))
    if depth + 1 < MAX_TREE_DEPTH:
        container_count = draw(st.integers(min_value=0, max_value=1))
    else:
        container_count = 0

    for index in range(task_count):
        name = _task_name(index)
        if allow_shell_tasks and draw(st.booleans()):
            task = parent.add_task(ShellScriptTask(name, draw(st.sampled_from(SCRIPT_PATHS))))
        else:
            task = parent.add_task(Task(name))
        _draw_attributes(draw, task, allow_repeat=False)

    for index in range(container_count):
        container = parent.add_container(_container_name(index))
        _draw_attributes(draw, container, allow_repeat=True)
        _draw_subtree(draw, container, depth=depth + 1, allow_shell_tasks=allow_shell_tasks)


def _draw_in_limits(draw, flow: Flow) -> None:
    """Attach ``InLimit``s referring to the limits that exist in ``flow``."""
    limit_refs: List[Tuple[str, Optional[str]]] = []
    for node in _iter_nodes(flow):
        for limit in node.limits:
            limit_refs.append((limit.name, None))
            limit_refs.append((limit.name, node.node_path))

    if not limit_refs:
        return

    for node in _iter_nodes(flow):
        chosen = draw(
            st.lists(st.sampled_from(limit_refs), min_size=0, max_size=2, unique=True)
        )
        for limit_name, node_path in chosen:
            in_limit = node.in_limit_manager
            # ``add_in_limit`` rejects a duplicate (limit_name, node_path) pair.
            if any(
                item.limit_name == limit_name and item.node_path == node_path
                for item in in_limit.in_limit_list
            ):
                continue
            node.add_in_limit(
                limit_name,
                node_path=node_path,
                tokens=draw(st.integers(min_value=1, max_value=2)),
            )


def _draw_runtime_state(draw, flow: Flow) -> None:
    """Draw randomised runtime state for every node of ``flow``."""
    all_paths = node_paths_of(flow)

    for node in _iter_nodes(flow):
        node.set_node_status_only(draw(st.sampled_from(NODE_STATUSES)))
        node.state.suspended = draw(st.booleans())
        node.is_complete_triggered = draw(st.booleans())

        for event in node.events:
            event.value = draw(st.booleans())
        for meter in node.meters:
            meter.value = draw(st.integers(min_value=meter.min_value, max_value=meter.max_value))
        for time_attr in node.times:
            if draw(st.booleans()):
                time_attr.set_free()
        for limit in node.limits:
            limit.node_paths = set(
                draw(
                    st.lists(
                        st.sampled_from(all_paths),
                        min_size=0,
                        max_size=min(3, len(all_paths)),
                        unique=True,
                    )
                )
            )
            limit.value = draw(st.integers(min_value=0, max_value=limit.limit))
        if node.repeat is not None:
            steps = draw(st.integers(min_value=0, max_value=3))
            for _ in range(steps):
                if not node.repeat.increment():
                    break

        if isinstance(node, Task):
            node.try_no = draw(st.integers(min_value=0, max_value=3))
            node.task_id = draw(st.one_of(st.none(), st.sampled_from(["1234", "job-1", "作业 1"])))
            node.aborted_reason = draw(
                st.one_of(st.none(), st.sampled_from(["", "exit 1", "作业 失败"]))
            )


def _draw_begun_and_calendar(draw, flow: Flow) -> None:
    """Draw ``begun`` and the calendar as a pair (requirement 8.6 invariant).

    A flow that has not begun keeps all six calendar fields ``None``. A flow
    that has begun is started through ``Flow.begin()``, the only operation that
    starts a calendar, and its ``flow_time`` is then advanced by an arbitrary
    number of steps through ``Flow.update_calendar()``, exactly the way the
    scheduler's main loop advances it. Every generated calendar is therefore a
    state the running system can really reach.

    Must be called *before* the node runtime state is drawn, because ``begin()``
    requeues the node tree.
    """
    if not draw(st.booleans()):
        # not begun: fresh calendar, all six fields stay None
        return

    flow.begin()

    for _ in range(draw(st.integers(min_value=0, max_value=3))):
        delta = datetime.timedelta(seconds=draw(st.integers(min_value=1, max_value=7200)))
        flow.update_calendar(flow.calendar.last_real_time + delta)


@st.composite
def flows(
    draw,
    name: Optional[str] = None,
    allow_shell_tasks: bool = True,
    with_runtime_state: bool = True,
) -> Flow:
    """Build a single ``Flow`` with a node tree of depth <= ``MAX_TREE_DEPTH``.

    Args:
        name: Flow name. A generated one is used when omitted.
        allow_shell_tasks: Whether ``ShellScriptTask`` (and therefore
            ``script_path``) may appear. Pass ``False`` when the flow is going
            to be *operated on*, because running a shell task really spawns a
            job.
        with_runtime_state: Whether node statuses, attribute values, ``begun``
            and the calendar are randomised. When ``False`` the flow keeps the
            initial values of a freshly built definition.
    """
    flow = Flow(name if name is not None else _flow_name(0))
    _draw_attributes(draw, flow, allow_repeat=True)
    _draw_subtree(draw, flow, depth=1, allow_shell_tasks=allow_shell_tasks)
    _draw_in_limits(draw, flow)
    if with_runtime_state:
        # begun / calendar first: ``begin()`` requeues the node tree, so the
        # node runtime state has to be drawn after it.
        _draw_begun_and_calendar(draw, flow)
        _draw_runtime_state(draw, flow)
    return flow


@st.composite
def bunches(
    draw,
    min_flows: int = 1,
    max_flows: int = 3,
    host: Optional[str] = None,
    port: Optional[str] = None,
    allow_shell_tasks: bool = True,
) -> Bunch:
    """Build a ``Bunch`` holding 1~3 randomised flows.

    Every flow carries randomised attributes and runtime state, and its
    ``begun`` flag is paired with its calendar so that requirement 8.6's
    invariant always holds.

    Args:
        min_flows: Minimum number of flows.
        max_flows: Maximum number of flows.
        host: Server host of the bunch. Drawn when omitted.
        port: Server port of the bunch. Drawn when omitted.
        allow_shell_tasks: See :func:`flows`.
    """
    if host is None:
        host = draw(st.sampled_from(["127.0.0.1", "localhost", "takler-host"]))
    if port is None:
        port = draw(st.sampled_from(["33083", "33084", "50051"]))

    bunch = Bunch(name=draw(st.sampled_from(["", "bunch1"])), host=host, port=port)

    flow_count = draw(st.integers(min_value=min_flows, max_value=max_flows))
    for index in range(flow_count):
        flow = draw(
            flows(
                name=_flow_name(index),
                allow_shell_tasks=allow_shell_tasks,
                with_runtime_state=True,
            )
        )
        bunch.add_flow(flow)

    return bunch


# Flow operation sequences --------------------------------------------------


@dataclass(frozen=True)
class FlowOperation:
    """One operation to replay on a ``Flow``.

    Attributes:
        name: Operation name, see :func:`apply_flow_operation`.
        node_path: Absolute path of the target node. Flow level operations
            (``begin`` and the serialization round trips) use the flow's own
            path.
        args: Keyword arguments of the operation.
    """

    name: str
    node_path: str
    args: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"FlowOperation({self.name}, {self.node_path}, {self.args})"


#: Operations that do not start the calendar. Used by the "calendar is only
#: started by begin" property.
NON_BEGIN_OPERATIONS: Tuple[str, ...] = (
    "requeue",
    "suspend",
    "resume",
    "run",
    "complete",
    "abort",
    "set_event",
    "set_meter",
    "free_dependencies",
    "roundtrip_status",
    "roundtrip_tree",
)


def apply_flow_operation(
        flow: Flow,
        operation: FlowOperation,
        ignore_rejected: bool = True,
) -> Flow:
    """Apply ``operation`` to ``flow`` and return the flow to keep working on.

    The serialization round trip operations replace the flow object, which is
    why the (possibly new) flow is returned instead of being mutated in place.

    Args:
        flow: The flow to operate on.
        operation: The operation to apply.
        ignore_rejected: When set, a ``TaklerError`` raised by the operation is
            swallowed. Rejected operations leave the flow unchanged (for
            example ``begin`` without ``force`` on a flow that has already
            begun), so replaying a sequence does not have to special case them.

    Returns:
        The flow after the operation.
    """
    name = operation.name
    args = operation.args

    if name == "roundtrip_status":
        return Flow.from_dict(flow.to_dict(), method=SerializationType.Status)
    if name == "roundtrip_tree":
        return Flow.from_dict(flow.to_dict(), method=SerializationType.Tree)

    if operation.node_path == flow.node_path:
        node: Optional[Node] = flow
    else:
        node = flow.find_node(operation.node_path)
    if node is None:
        return flow

    try:
        if name == "begin":
            flow.begin(force=args.get("force", False))
        elif name == "requeue":
            node.requeue(reset_repeat=args.get("reset_repeat", True))
        elif name == "suspend":
            node.suspend()
        elif name == "resume":
            node.resume()
        elif name == "run":
            node.run()
        elif name == "complete":
            node.complete()
        elif name == "abort":
            node.abort(args.get("reason", ""))
        elif name == "set_event":
            node.set_event(args["name"], args["value"])
        elif name == "set_meter":
            node.set_meter(args["name"], args["value"])
        elif name == "free_dependencies":
            node.free_dependencies(args.get("dep_type"))
        else:
            raise ValueError(f"unknown flow operation: {name}")
    except takler_exceptions.TaklerError:
        if not ignore_rejected:
            raise

    return flow


def apply_flow_operations(
        flow: Flow,
        operations: Iterable[FlowOperation],
        ignore_rejected: bool = True,
) -> Flow:
    """Apply ``operations`` in order and return the resulting flow."""
    for operation in operations:
        flow = apply_flow_operation(flow, operation, ignore_rejected=ignore_rejected)
    return flow


@st.composite
def flow_operation_sequences(
        draw,
        include_begin: bool = True,
        include_serialization: bool = True,
        min_size: int = 1,
        max_size: int = 6,
        with_runtime_state: bool = True,
) -> Tuple[Flow, List[FlowOperation]]:
    """Build a ``Flow`` together with a sequence of operations to replay on it.

    The generated flow never contains a ``ShellScriptTask``, because ``run``
    would really render and spawn a job.

    Args:
        include_begin: Whether ``begin`` and ``begin(force=True)`` may appear.
            Pass ``False`` to get the begin-free subset used to check that
            nothing but ``begin`` starts the calendar.
        include_serialization: Whether ``Status`` / ``Tree`` round trips may
            appear in the sequence.
        min_size: Minimum number of operations.
        max_size: Maximum number of operations.
        with_runtime_state: Whether the flow starts from randomised runtime
            state (including a randomised ``begun`` / calendar pair) or from a
            freshly built definition.

    Returns:
        A ``(flow, operations)`` tuple. Replay it with
        :func:`apply_flow_operations`, which returns the flow to keep asserting
        on because a round trip replaces the object.
    """
    flow = draw(
        flows(
            allow_shell_tasks=False,
            with_runtime_state=with_runtime_state,
        )
    )

    nodes = _iter_nodes(flow)
    node_paths = [node.node_path for node in nodes]
    task_paths = [node.node_path for node in nodes if isinstance(node, Task)]
    event_refs = [(node.node_path, event.name) for node in nodes for event in node.events]
    meter_refs = [
        (node.node_path, meter.name, meter.min_value, meter.max_value)
        for node in nodes
        for meter in node.meters
    ]

    choices = [
        st.builds(
            lambda path, reset_repeat: FlowOperation("requeue", path, {"reset_repeat": reset_repeat}),
            st.sampled_from(node_paths),
            st.booleans(),
        ),
        st.builds(
            lambda path: FlowOperation("suspend", path),
            st.sampled_from(node_paths),
        ),
        st.builds(
            lambda path: FlowOperation("resume", path),
            st.sampled_from(node_paths),
        ),
        st.builds(
            lambda path, dep_type: FlowOperation("free_dependencies", path, {"dep_type": dep_type}),
            st.sampled_from(node_paths),
            st.sampled_from([None, "all", "time", "trigger"]),
        ),
    ]

    if task_paths:
        choices.extend([
            st.builds(
                lambda path: FlowOperation("run", path),
                st.sampled_from(task_paths),
            ),
            st.builds(
                lambda path: FlowOperation("complete", path),
                st.sampled_from(task_paths),
            ),
            st.builds(
                lambda path, reason: FlowOperation("abort", path, {"reason": reason}),
                st.sampled_from(task_paths),
                st.sampled_from(["", "exit 1", "作业 失败"]),
            ),
        ])

    if event_refs:
        choices.append(
            st.builds(
                lambda ref, value: FlowOperation(
                    "set_event", ref[0], {"name": ref[1], "value": value}
                ),
                st.sampled_from(event_refs),
                st.booleans(),
            )
        )

    if meter_refs:
        choices.append(
            st.builds(
                lambda ref, ratio: FlowOperation(
                    "set_meter",
                    ref[0],
                    {"name": ref[1], "value": ref[2] + int(round(ratio * (ref[3] - ref[2])))},
                ),
                st.sampled_from(meter_refs),
                st.floats(min_value=0.0, max_value=1.0),
            )
        )

    if include_begin:
        choices.append(
            st.builds(
                lambda force: FlowOperation("begin", flow.node_path, {"force": force}),
                st.booleans(),
            )
        )

    if include_serialization:
        choices.append(
            st.builds(
                lambda name: FlowOperation(name, flow.node_path),
                st.sampled_from(["roundtrip_status", "roundtrip_tree"]),
            )
        )

    operations = draw(
        st.lists(st.one_of(*choices), min_size=min_size, max_size=max_size)
    )
    return flow, operations


# Node path generators ------------------------------------------------------

#: A single node path segment: letters and digits, optionally with underscores.
_PATH_SEGMENTS = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_",
    min_size=1,
    max_size=6,
).filter(lambda s: s[0] != "_")


def _is_malformed_absolute_path(path: str) -> bool:
    """Mirror ``Node.check_absolute_node_path`` returning ``False``."""
    return not path.startswith("/") or path == "/"


def invalid_node_paths() -> st.SearchStrategy[str]:
    """Malformed node paths: not absolute, or the bare root.

    These are the paths ``Bunch.find_node`` rejects with
    ``InvalidNodePathError`` instead of returning ``None``: anything that does
    not start with ``/``, plus ``/`` itself. The empty string, whitespace and
    non ASCII text are included.
    """
    fixed = st.sampled_from([
        "",
        "/",
        " ",
        "flow1",
        "flow1/task1",
        "./task1",
        "../flow1/task1",
        "task1",
        "流程/任务",
        " /flow1/task1",
    ])
    generated = st.one_of(
        st.text(min_size=0, max_size=10),
        st.builds(lambda parts: "/".join(parts), st.lists(_PATH_SEGMENTS, min_size=1, max_size=3)),
    ).filter(_is_malformed_absolute_path)
    return st.one_of(fixed, generated)


def nonexistent_node_paths(existing_paths: Iterable[str] = ()) -> st.SearchStrategy[str]:
    """Well formed absolute node paths that are *not* in ``existing_paths``.

    The paths are syntactically valid, so a lookup reaches the "no such node"
    branch instead of the "malformed path" branch.

    Args:
        existing_paths: Paths that do exist, typically
            ``node_paths_of(flow)`` for every flow of a bunch.
    """
    existing = frozenset(existing_paths)
    return st.builds(
        lambda parts: "/" + "/".join(parts),
        st.lists(_PATH_SEGMENTS, min_size=1, max_size=3),
    ).filter(lambda path: path not in existing)


# Exception type generators -------------------------------------------------


def takler_exception_types() -> st.SearchStrategy[Type[BaseException]]:
    """Every takler owned exception type, ``TaklerError`` included."""
    return st.sampled_from(TAKLER_EXCEPTION_TYPES)


def foreign_exception_types() -> st.SearchStrategy[Type[BaseException]]:
    """Exception types that are not takler owned."""
    return st.sampled_from(FOREIGN_EXCEPTION_TYPES)


# Invalid trigger expressions ----------------------------------------------


def _is_invalid_trigger(text: str) -> bool:
    """Whether ``text`` really fails to parse as a trigger expression."""
    try:
        parse_trigger(text)
    except ExpressionSyntaxError:
        return True
    return False


@st.composite
def _mutated_trigger_expressions(draw) -> str:
    """Apply one destructive mutation to a valid trigger expression."""
    base = draw(st.sampled_from(VALID_TRIGGER_EXPRESSIONS))
    mutation = draw(
        st.sampled_from(["drop_char", "truncate", "insert_char", "replace_char", "break_operator"])
    )

    if mutation == "drop_char":
        index = draw(st.integers(min_value=0, max_value=len(base) - 1))
        return base[:index] + base[index + 1:]

    if mutation == "truncate":
        length = draw(st.integers(min_value=0, max_value=len(base) - 1))
        return base[:length]

    if mutation == "insert_char":
        index = draw(st.integers(min_value=0, max_value=len(base)))
        char = draw(st.sampled_from(_ILLEGAL_TRIGGER_CHARS))
        return base[:index] + char + base[index:]

    if mutation == "replace_char":
        index = draw(st.integers(min_value=0, max_value=len(base) - 1))
        char = draw(st.sampled_from(_ILLEGAL_TRIGGER_CHARS))
        return base[:index] + char + base[index + 1:]

    # break_operator: leave a dangling or malformed operator behind
    replacement = draw(st.sampled_from(["=", "=== ==", "and and", "or or", "+ +"]))
    for operator in ("==", ">=", "<=", ">", "<", " and ", " or "):
        if operator in base:
            return base.replace(operator, f" {replacement} ", 1)
    return base + f" {replacement}"


def invalid_trigger_expressions() -> st.SearchStrategy[str]:
    """Trigger expressions that are guaranteed to fail parsing.

    Built by destructively mutating valid expressions (dropping a character,
    truncating, inserting or substituting an illegal character, breaking an
    operator) and then keeping only the candidates the parser really rejects,
    so a test never mistakes an accidentally valid mutation for a bug.
    """
    return _mutated_trigger_expressions().filter(_is_invalid_trigger)
