"""Property-based test for the Job_Password snapshot round trip.

Covers Property 3 from the ``m2-security`` design (Requirements 5.5, 16.6): for
any Bunch built by the Core_Model, let ``M(b)`` be the ``{node path ->
Job_Password}`` mapping whose domain is restricted to the Task nodes in the
submitted or active status (Requirement 5.2), then::

    M(restore(write(b))) == M(b)

That is what makes a server restart survivable for a job that is already in
flight: the job holds a ``TAKLER_PASS`` handed to it before the restart, and the
restarted server must accept the Child_Command it sends afterwards. The two
sides of the equality are computed by the production code itself
(``CheckpointManager._collect_job_passwords``), so the property pins the *pair*
``collect`` / ``restore`` rather than either half against a hand-written
expectation: a path built differently on the two sides, or a status filter
applied on one side only, breaks it.

Why the domain is restricted rather than "every task with a password": a task
outside submitted / active does not get its password persisted at all
(Requirements 5.2, 5.3), so after a restore its password is empty. That is the
designed behaviour, not a round trip failure -- a Child_Command against such a
task is rejected by Zombie_Condition ``Z2`` whether or not its password
matches. **Do not widen the domain of this property**; doing so would assert a
spec that was deliberately not written.

How the generated bunches get their passwords: ``tests/strategies.py`` draws
runtime state including ``try_no`` but leaves ``job_password`` unset, so a
password is assigned here, to exactly the tasks whose ``try_no`` is non-zero.
Pairing the two keeps every generated tree consistent with Property 1's
invariant ("the password is empty iff ``try_no == 0``"), so no example in this
file describes a task the running system could not produce.

Each example writes a real snapshot to its own temporary directory and restores
it into a fresh, empty Bunch through ``CheckpointManager.restore``, i.e. the
same path a restarting server takes. A ``tmp_path`` fixture cannot be used
because a function-scoped fixture is created once for all examples of a
``@given`` test.

No test name, ``print`` or assertion message in this file carries a password
value: the mappings are compared as whole objects and the messages report only
node paths and counts.
"""

from __future__ import annotations

import contextlib
import io
import secrets
import tempfile
from pathlib import Path
from typing import Dict, List

from hypothesis import given, settings
from hypothesis import strategies as st

from takler.core import Bunch, NodeStatus, Task
from takler.core.node import Node
from takler.server.checkpoint import CheckpointManager

from tests.strategies import bunches

#: Byte count handed to ``secrets.token_urlsafe``, the same as
#: ``Task.increment_try_no`` uses, so the generated values are the same shape as
#: the real ones (length, alphabet, and therefore JSON escaping).
PASSWORD_NBYTES = 32

#: The statuses that put a task in the domain of ``M(b)`` (Requirement 5.2).
_IN_FLIGHT_STATUSES = (NodeStatus.submitted, NodeStatus.active)


def _iter_nodes(node: Node) -> List[Node]:
    """Return ``node`` and all its descendants, pre-order."""
    nodes = [node]
    for child in node.children:
        nodes.extend(_iter_nodes(child))
    return nodes


def _tasks_of(bunch: Bunch) -> List[Task]:
    return [
        node
        for flow in bunch.flows.values()
        for node in _iter_nodes(flow)
        if isinstance(node, Task)
    ]


def _mapping_of(bunch: Bunch) -> Dict[str, str]:
    """Return ``M(bunch)``, computed by the production collector.

    ``_collect_job_passwords`` is the definition of the mapping the design
    speaks of -- the status filter of Requirement 5.2 and the path format both
    live there -- so both sides of the equality are read through it instead of
    being rebuilt here.
    """
    return CheckpointManager(bunch=bunch)._collect_job_passwords()


@st.composite
def _bunches_with_job_passwords(draw: st.DrawFn) -> Bunch:
    """Draw a bunch whose in-flight tasks carry a Job_Password.

    A password is given to every task with a non-zero ``try_no`` and to no
    other, which is the pairing ``increment_try_no`` / ``requeue`` maintains.
    Which of those tasks end up in ``M(b)`` is then decided by their status.

    Some of the drawn tasks are additionally moved into an in-flight status.
    Without that nudge the tree generator's six statuses leave two thirds of the
    examples with an empty mapping, and an empty mapping round trips
    trivially -- the interesting examples are the ones with several entries.
    Tasks that stay outside submitted / active are just as necessary, since they
    are what proves the domain restriction is applied on both sides rather than
    ignored on both.
    """
    bunch = draw(bunches())
    for task in _tasks_of(bunch):
        if draw(st.booleans()):
            task.set_node_status_only(draw(st.sampled_from(_IN_FLIGHT_STATUSES)))
            # An in-flight task has run at least once, so keep ``try_no``
            # consistent with the status before the password is assigned.
            task.try_no = max(task.try_no, 1)
        if task.try_no != 0:
            task.job_password = secrets.token_urlsafe(PASSWORD_NBYTES)
    return bunch


# Feature: m2-security, Property 3: 口令快照往返
# Validates: Requirements 5.5, 16.6
@settings(max_examples=100, deadline=None)
@given(bunch=_bunches_with_job_passwords())
def test_job_passwords_survive_a_snapshot_round_trip(bunch: Bunch) -> None:
    """``M(restore(write(b))) == M(b)`` for any generated bunch.

    Writes one real snapshot of ``bunch``, restores it into an empty bunch the
    way a restarting server does, and asserts that the collected
    ``{node path -> Job_Password}`` mapping is unchanged (Requirements 5.5,
    16.6).
    """
    expected = _mapping_of(bunch)

    with tempfile.TemporaryDirectory() as directory:
        checkpoint_file = Path(directory) / "takler.check"

        source = CheckpointManager(bunch=bunch, checkpoint_file=checkpoint_file)
        assert source.write_checkpoint() is True

        target = CheckpointManager(
            bunch=Bunch(host=bunch.server_state.host, port=bunch.server_state.port),
            checkpoint_file=checkpoint_file,
        )
        # The restore logs one INFO per flow plus the password count; swallowing
        # it keeps 100 examples from burying the test output.
        with contextlib.redirect_stderr(io.StringIO()):
            assert target.restore() is True

    restored = _mapping_of(target.bunch)

    assert sorted(restored) == sorted(expected), (
        "the set of tasks holding a job password changed across the snapshot "
        f"round trip: {sorted(set(expected) ^ set(restored))}"
    )
    assert restored == expected, (
        "the job password of "
        f"{sorted(path for path in expected if restored.get(path) != expected[path])} "
        "changed across the snapshot round trip"
    )
