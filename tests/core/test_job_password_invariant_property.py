"""Property-based test for the job password / ``try_no`` binding invariant.

Covers Property 1 from the ``m2-security`` design: for any ``Task``, the
``job_password`` is empty **if and only if** ``try_no == 0`` (Requirement 4.9).

Why this holds by construction, and why the test is still worth having:
``Task.increment_try_no`` bumps ``try_no`` and generates a password in the same
call, ``Task.requeue`` zeroes and clears both, and those two are the only write
points of either field. The invariant is therefore a property of *those two
methods staying paired*, which is exactly what a later edit can break -- adding
a third write point (say a "retry without a new password" shortcut), or moving
one of the two assignments out of its method, silently produces a task whose
``try_no`` says "has run" while the password says "has not". Under the default
``fail`` Zombie_Policy such a task rejects every Child_Command its own job
sends, so the failure surfaces in production as unexplained zombie rejections.

The generated operation sequence mixes task-level ``increment_try_no`` /
``requeue`` with container-level and flow-level ``requeue``, because
``NodeContainer.requeue`` cascades into ``Task.requeue`` and is thus a second
route into the same write point. The invariant is asserted after *every* step
rather than only at the end of the sequence, so a falsifying example names the
operation that broke it instead of leaving the whole sequence suspect.

Excluded from the property, deliberately: a ``Task`` restored from a
pre-M2 Checkpoint_File, i.e. a snapshot carrying no ``job_passwords`` mapping.
Requirement 4.9 excludes it explicitly, and so does Requirement 5.2, which only
persists the passwords of submitted / active tasks. A restored task can
therefore legitimately have ``try_no > 0`` with an empty password. **Do not
"fix" this property to cover restored nodes** -- the equivalence does not hold
there by design, and a test asserting it would be asserting a wrong spec.

Generated flows come from ``tests/strategies.py`` with
``with_runtime_state=False``. Randomised runtime state is not usable here: it
assigns ``try_no`` directly, bypassing both write points, and so would generate
tasks violating the invariant before a single operation is applied.

No test name, ``print`` or assertion message in this file carries a password
value. The assertions compare ``bool(job_password)`` and report only the node
path, the operation and ``try_no``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from hypothesis import given, settings
from hypothesis import strategies as st

from takler.core import Flow, Task
from takler.core.node import Node
from takler.core.parameter import TAKLER_HOME

from tests.strategies import flows

#: Maximum number of operations replayed on one generated flow.
MAX_OPERATIONS = 8

#: ``TAKLER_HOME`` value given to every generated flow. ``increment_try_no``
#: recomputes the generated parameters, and a ``ShellScriptTask`` derives
#: ``TAKLER_JOB`` / ``TAKLER_JOBOUT`` from an inherited ``TAKLER_HOME``, so a
#: flow without it cannot be operated on at all. Nothing is ever written: the
#: path only takes part in string composition, because no operation in this
#: file renders or submits a job.
TAKLER_HOME_VALUE = "/tmp/takler-job-password-invariant-property"


@dataclass(frozen=True)
class PasswordOperation:
    """One password-affecting operation to replay on a flow.

    Attributes:
        name: ``"increment_try_no"`` or ``"requeue"``.
        node_path: Absolute path of the target node. ``increment_try_no``
            targets a ``Task``; ``requeue`` may target a ``Task``, a
            ``NodeContainer`` or the ``Flow`` itself.
        args: Keyword arguments of the operation.
    """

    name: str
    node_path: str
    args: Dict[str, bool] = field(default_factory=dict)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"PasswordOperation({self.name}, {self.node_path}, {self.args})"


def _iter_nodes(node: Node) -> List[Node]:
    """Return ``node`` and all its descendants, pre-order."""
    nodes = [node]
    for child in node.children:
        nodes.extend(_iter_nodes(child))
    return nodes


def _tasks_of(flow: Flow) -> List[Task]:
    return [node for node in _iter_nodes(flow) if isinstance(node, Task)]


def _find(flow: Flow, node_path: str) -> Optional[Node]:
    """Resolve ``node_path`` inside ``flow``, including the flow's own path."""
    if node_path == flow.node_path:
        return flow
    return flow.find_node(node_path)


def _apply(flow: Flow, operation: PasswordOperation) -> None:
    """Apply one operation to ``flow`` in place."""
    node = _find(flow, operation.node_path)
    assert node is not None, f"generated path does not exist: {operation.node_path}"

    if operation.name == "increment_try_no":
        node.increment_try_no()
    elif operation.name == "requeue":
        node.requeue(reset_repeat=operation.args.get("reset_repeat", True))
    else:  # pragma: no cover - guards the generator, not the production code
        raise ValueError(f"unknown password operation: {operation.name}")


def _assert_invariant(flow: Flow, stage: str) -> None:
    """Assert Property 1 for every ``Task`` of ``flow``.

    The message names the node, the stage and ``try_no``, and reports the
    password only as the boolean "is it empty", never as its value.
    """
    for task in _tasks_of(flow):
        has_password = bool(task.job_password)
        assert has_password == (task.try_no != 0), (
            f"job password / try_no binding broken at {task.node_path} "
            f"{stage}: try_no={task.try_no}, job password present="
            f"{has_password}; the password must be empty if and only if "
            f"try_no == 0 (requirement 4.9)"
        )


@st.composite
def _flows_with_password_operations(
    draw: st.DrawFn,
) -> Tuple[Flow, List[PasswordOperation]]:
    """Draw a freshly defined flow plus operations to replay on it.

    ``with_runtime_state=False`` keeps every generated task at ``try_no == 0``
    with an empty password, so the starting point satisfies the invariant and
    every later state is reached only through the two write points.
    """
    flow = draw(flows(allow_shell_tasks=True, with_runtime_state=False))
    flow.add_parameter(TAKLER_HOME, TAKLER_HOME_VALUE)

    nodes = _iter_nodes(flow)
    task_paths = [node.node_path for node in nodes if isinstance(node, Task)]
    # Containers, the flow itself included: ``NodeContainer.requeue`` cascades
    # into ``Task.requeue``, so it reaches the same write point from above.
    container_paths = [node.node_path for node in nodes if not isinstance(node, Task)]

    choices = [
        st.builds(
            lambda path: PasswordOperation("increment_try_no", path),
            st.sampled_from(task_paths),
        ),
        st.builds(
            lambda path, reset_repeat: PasswordOperation(
                "requeue", path, {"reset_repeat": reset_repeat}
            ),
            st.sampled_from(task_paths),
            st.booleans(),
        ),
        st.builds(
            lambda path, reset_repeat: PasswordOperation(
                "requeue", path, {"reset_repeat": reset_repeat}
            ),
            st.sampled_from(container_paths),
            st.booleans(),
        ),
    ]

    operations = draw(
        st.lists(st.one_of(*choices), min_size=1, max_size=MAX_OPERATIONS)
    )
    return flow, operations


# Feature: m2-security, Property 1: 口令与 try_no 的绑定不变式
# Validates: Requirements 4.9
@settings(max_examples=100, deadline=None)
@given(case=_flows_with_password_operations())
def test_job_password_is_empty_iff_try_no_is_zero(
    case: Tuple[Flow, List[PasswordOperation]],
) -> None:
    """A task's job password is empty iff its ``try_no`` is 0, at every step.

    For any generated node tree and any sequence of ``increment_try_no`` /
    ``requeue`` operations, the equivalence holds for every task of the tree
    before the first operation and after each one (Requirement 4.9).
    """
    flow, operations = case

    _assert_invariant(flow, "before any operation")

    for index, operation in enumerate(operations, start=1):
        _apply(flow, operation)
        _assert_invariant(
            flow,
            f"after step {index} ({operation.name} on {operation.node_path})",
        )
