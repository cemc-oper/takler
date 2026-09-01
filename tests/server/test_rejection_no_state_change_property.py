"""Property-based test for the state invariance of every rejection path.

Covers Property 6 from the ``m2-security`` design (Requirements 6.9, 10.2,
10.3): for any Bunch and any RPC the server refuses -- refused by the
Auth_Interceptor, or judged a zombie under the ``fail`` / ``fob`` policy -- the
``state``, ``task_id``, ``try_no``, ``aborted_reason`` and Job_Password of
*every* node of the Bunch are the same after the call as before it.

Why this is worth a property rather than a handful of examples: the guarantee is
"nothing anywhere moved", and the way it breaks is a rejection path that writes
something before deciding to reject -- an ``aborted_reason`` set while locating
the node, a ``try_no`` bumped before the guard, a Job_Password adopted on a path
that then refuses. Such a leak shows up on one status / command / policy
combination and not on the neighbouring ones, which is exactly what generated
trees over the whole state space find and a fixed example does not. The
invariant is asserted over all nodes, not only the target, because a write
through a parent container (a status propagation, a limit release) would
otherwise pass unnoticed.

Both rejection families are driven in process, no gRPC server and no socket:

* the Auth_Interceptor is called directly with a stand-in
  ``handler_call_details`` and a stand-in ``ServicerContext``, the same way
  ``test_auth_rejection_record.py`` does. Its ``continuation`` is a saboteur
  that rewrites the whole tree, so "the refused RPC never reached the handler"
  is not asserted by trusting a flag alone: had the interceptor let the call
  through, the snapshot comparison would fail loudly.
* the zombie path goes through the real :class:`Scheduler` Child_Command
  methods and a real :class:`ZombieDetector`, i.e. the production call sites,
  with the Credential_Metadata published in the ContextVar the way the
  interceptor publishes it.

How a case is guaranteed to be a rejection: with Auth_Mode ``enabled`` the
presented ``takler-pass`` is a value no generated password can equal, so ``Z1``
is hit; with ``disabled`` (where ``Z1`` is not evaluated at all) the target task
is put into a status outside submitted / active, so ``Z2`` is hit. Each example
also asserts that the detector really did report a condition, so a future change
that stops detecting cannot make this test vacuous.

The ``adopt`` policy is deliberately absent: it is not a rejection, it takes the
password over and runs the command, so it changes state by design.

No password value reaches a test name, a ``print`` or an assertion message: the
snapshots are compared as whole mappings and the messages name only node paths.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import io
import secrets
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from takler.core import Bunch, NodeStatus, Task
from takler.core.node import Node
from takler.exceptions import ZombieError
from takler.server.auth import (
    METADATA_KEY_JOB_PASSWORD,
    METADATA_KEY_SECRET,
    METADATA_KEY_USER,
    SERVICE_METHOD_PREFIX,
    AuthInterceptor,
    CallCredentials,
    CredentialStore,
    reset_call_credentials,
    set_call_credentials,
)
from takler.server.connect_config import AuthMode, ZombiePolicy
from takler.server.scheduler import Scheduler
from takler.server.zombie import IN_FLIGHT_STATUSES, ChildAction, ZombieDetector

from tests.strategies import bunches

#: Byte count handed to ``secrets.token_urlsafe``, matching
#: ``Task.increment_try_no`` so the generated passwords have the shape of real
#: ones.
PASSWORD_NBYTES = 32

#: The ``takler-pass`` a zombie presents. Any value that is not the target
#: task's Job_Password hits ``Z1``; a fixed literal keeps the examples
#: reproducible, and it cannot collide with a 43 character random token.
FOREIGN_PASSWORD = "job-password-of-another-try"

#: The one Operator_Secret the store below accepts, and one that it does not.
SECRET = "operator-secret-value"
STALE_SECRET = "retired-secret-value"

#: The one whitelisted user name, and one that is not whitelisted.
USER = "alice"
INTRUDER = "intruder"

PEER = "ipv4:127.0.0.1:54321"

#: The five Child_Command short names, as ``Scheduler`` spells them.
CHILD_COMMANDS: Tuple[str, ...] = ("init", "complete", "abort", "event", "meter")

#: Statuses outside submitted / active, i.e. the ones that hit ``Z2``.
_STALE_STATUSES: Tuple[NodeStatus, ...] = tuple(
    status
    for status in (
        NodeStatus.unknown,
        NodeStatus.complete,
        NodeStatus.queued,
        NodeStatus.aborted,
    )
    if status not in IN_FLIGHT_STATUSES
)

CHILD_METHODS: Tuple[str, ...] = tuple(
    SERVICE_METHOD_PREFIX + name
    for name in ("RunCommandInit", "RunCommandComplete", "RunCommandAbort")
)
OPERATOR_METHODS: Tuple[str, ...] = tuple(
    SERVICE_METHOD_PREFIX + name
    for name in ("RunCommandRequeue", "RunCommandForce", "RunRequestShow")
)


# node tree helpers ---------------------------------------------------------


def _iter_nodes(node: Node) -> List[Node]:
    """Return ``node`` and all its descendants, pre-order."""
    nodes = [node]
    for child in node.children:
        nodes.extend(_iter_nodes(child))
    return nodes


def _all_nodes(bunch: Bunch) -> List[Node]:
    return [node for flow in bunch.flows.values() for node in _iter_nodes(flow)]


def _snapshot(bunch: Bunch) -> Dict[str, Tuple[Any, ...]]:
    """Return the five guarded fields of every node, keyed by node path.

    Non-task nodes have no ``task_id`` / ``try_no`` / ``aborted_reason`` /
    Job_Password, so only their status is recorded; they are still part of the
    snapshot because a status written through a container is as much a state
    change as one written to the target task.
    """
    snapshot: Dict[str, Tuple[Any, ...]] = {}
    for node in _all_nodes(bunch):
        if isinstance(node, Task):
            snapshot[node.node_path] = (
                node.state.node_status,
                node.task_id,
                node.try_no,
                node.aborted_reason,
                node.job_password,
            )
        else:
            snapshot[node.node_path] = (node.state.node_status,)
    return snapshot


def _assert_unchanged(
    bunch: Bunch,
    before: Dict[str, Tuple[Any, ...]],
    what: str,
) -> None:
    """Assert the snapshot of ``bunch`` still equals ``before``.

    The message names the paths whose tuple moved, never the tuples themselves,
    since one of the five fields is a Job_Password.
    """
    after = _snapshot(bunch)
    assert sorted(after) == sorted(before), (
        f"{what} changed the set of nodes: {sorted(set(before) ^ set(after))}"
    )
    changed = [path for path in before if after[path] != before[path]]
    assert not changed, (
        f"{what} changed the state, task_id, try_no, aborted_reason or job "
        f"password of {changed}"
    )


def _sabotage(bunch: Bunch) -> None:
    """Rewrite every guarded field of every node.

    Used as the body of the ``continuation`` the Auth_Interceptor must never
    reach: it makes "the handler did not run" observable through the state
    snapshot instead of only through a flag, which is what Requirement 6.9 is
    really about.
    """
    for node in _all_nodes(bunch):
        node.set_node_status_only(NodeStatus.aborted)
        if isinstance(node, Task):
            node.task_id = "job-of-the-saboteur"
            node.try_no += 1
            node.aborted_reason = "sabotage"
            node.job_password = "password-of-the-saboteur"


# strategies ---------------------------------------------------------------


@st.composite
def _bunches_with_job_passwords(draw: st.DrawFn) -> Bunch:
    """Draw a bunch whose already-run tasks carry a Job_Password.

    A password is given to every task with a non-zero ``try_no`` and to no
    other, which is the pairing ``increment_try_no`` / ``requeue`` maintains
    (Property 1), so every generated tree is one the running system could
    really be in. Shell tasks are excluded because a Child_Command never
    renders a job script, and keeping them out costs nothing here.
    """
    bunch = draw(bunches(allow_shell_tasks=False))
    for node in _all_nodes(bunch):
        if isinstance(node, Task) and node.try_no != 0:
            node.job_password = secrets.token_urlsafe(PASSWORD_NBYTES)
    return bunch


@dataclasses.dataclass
class _RejectedCall:
    """One RPC the Auth_Interceptor is expected to refuse."""

    bunch: Bunch
    method: str
    metadata: Sequence[Tuple[str, str]]


@st.composite
def _refused_rpcs(draw: st.DrawFn) -> _RejectedCall:
    """Draw a Bunch together with an RPC no credential set can authorize.

    The four families cover both status codes and all three rejection
    classifications: a Child_Command without a ``takler-pass``, an
    Operator_Command missing either key, one presenting a secret that is not in
    the Operator_Secret_Set, and one presenting a valid secret under a user name
    that is not whitelisted.
    """
    bunch = draw(_bunches_with_job_passwords())

    family = draw(
        st.sampled_from(
            ["child_without_pass", "operator_missing_key", "stale_secret", "intruder"]
        )
    )

    if family == "child_without_pass":
        method = draw(st.sampled_from(CHILD_METHODS))
        # Anything but ``takler-pass``: an operator's credentials do not stand in
        # for a job's, so these must be refused just the same.
        metadata = draw(
            st.sampled_from(
                [
                    (),
                    ((METADATA_KEY_USER, USER),),
                    ((METADATA_KEY_SECRET, SECRET), (METADATA_KEY_USER, USER)),
                ]
            )
        )
        return _RejectedCall(bunch=bunch, method=method, metadata=metadata)

    method = draw(st.sampled_from(OPERATOR_METHODS))

    if family == "operator_missing_key":
        metadata = draw(
            st.sampled_from(
                [
                    (),
                    ((METADATA_KEY_SECRET, SECRET),),
                    ((METADATA_KEY_USER, USER),),
                    # A job's password is not an operator's credential either.
                    ((METADATA_KEY_JOB_PASSWORD, FOREIGN_PASSWORD),),
                ]
            )
        )
    elif family == "stale_secret":
        user = draw(st.sampled_from([USER, INTRUDER]))
        metadata = ((METADATA_KEY_SECRET, STALE_SECRET), (METADATA_KEY_USER, user))
    else:
        metadata = ((METADATA_KEY_SECRET, SECRET), (METADATA_KEY_USER, INTRUDER))

    return _RejectedCall(bunch=bunch, method=method, metadata=metadata)


@dataclasses.dataclass
class _ZombieCall:
    """One Child_Command that is guaranteed to hit a Zombie_Condition."""

    bunch: Bunch
    node_path: str
    command: str
    args: Dict[str, Any]
    task_id: Optional[str]
    policy: ZombiePolicy
    auth_mode: AuthMode
    credentials: CallCredentials


@st.composite
def _zombie_child_commands(draw: st.DrawFn) -> _ZombieCall:
    """Draw a Bunch and a Child_Command against it that must be rejected.

    The condition that fires is decided by the Auth_Mode, because ``Z1`` only
    exists when authentication is on:

    * ``enabled``: the call presents a ``takler-pass`` that is not the target
      task's Job_Password (or none at all), which is ``Z1`` whatever the task's
      status is.
    * ``disabled``: the target task is moved to a status outside submitted /
      active, which is ``Z2`` -- the requeue case, and the reason zombie
      detection is useful without authentication.

    ``event`` and ``meter`` are only drawn for a target that declares one, since
    a missing attribute is a different rejection (an M1 error) than a zombie.
    """
    bunch = draw(_bunches_with_job_passwords())
    tasks = [node for node in _all_nodes(bunch) if isinstance(node, Task)]
    target: Task = draw(st.sampled_from(tasks))

    auth_mode = draw(st.sampled_from([AuthMode.ENABLED, AuthMode.DISABLED]))
    if auth_mode is AuthMode.ENABLED:
        presented: Optional[str] = draw(st.sampled_from([FOREIGN_PASSWORD, None]))
    else:
        target.set_node_status_only(draw(st.sampled_from(_STALE_STATUSES)))
        presented = draw(st.sampled_from([target.job_password, None]))

    commands: List[str] = ["init", "complete", "abort"]
    if target.events:
        commands.append("event")
    if target.meters:
        commands.append("meter")
    command = draw(st.sampled_from(commands))

    args: Dict[str, Any] = {}
    task_id: Optional[str] = None
    if command == "init":
        task_id = draw(st.sampled_from(["job-1", "job-2"]))
        args["task_id"] = task_id
    elif command == "abort":
        args["reason"] = draw(st.sampled_from(["", "exit 1", "作业 失败"]))
    elif command == "event":
        args["name"] = draw(st.sampled_from([event.name for event in target.events]))
    elif command == "meter":
        meter = draw(st.sampled_from(list(target.meters)))
        args["name"] = meter.name
        args["value"] = str(
            draw(st.integers(min_value=meter.min_value, max_value=meter.max_value))
        )

    return _ZombieCall(
        bunch=bunch,
        node_path=target.node_path,
        command=command,
        args=args,
        task_id=task_id,
        policy=draw(st.sampled_from([ZombiePolicy.FAIL, ZombiePolicy.FOB])),
        auth_mode=auth_mode,
        credentials=CallCredentials(
            job_password=presented,
            secret=None,
            user=draw(st.sampled_from([USER, None])),
            peer=PEER,
        ),
    )


# the interceptor rejection path ------------------------------------------


@dataclasses.dataclass
class _HandlerCallDetails:
    """The two attributes ``AuthInterceptor.intercept_service`` reads."""

    method: str
    invocation_metadata: Sequence[Tuple[str, str]] = ()


class _Context:
    """A stand-in ``ServicerContext`` that records the abort instead of raising."""

    def __init__(self) -> None:
        self.aborted: List[Tuple[Any, str]] = []

    def peer(self) -> Optional[str]:
        return PEER

    async def abort(self, code: Any, details: str) -> None:
        self.aborted.append((code, details))


@pytest.fixture(scope="module")
def store(tmp_path_factory: pytest.TempPathFactory) -> CredentialStore:
    """A store holding one operator secret and one whitelisted user.

    Module scoped: the two files are the same for every example, and a
    function-scoped fixture would be built once for the whole ``@given`` run
    anyway.
    """
    directory: Path = tmp_path_factory.mktemp("credentials")
    secret_file = directory / "secret"
    secret_file.write_text(f"{SECRET}\n")
    whitelist_file = directory / "whitelist"
    whitelist_file.write_text(f"{USER}\n")
    return CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)


# Feature: m2-security, Property 6: 拒绝路径的状态不变性
# Validates: Requirements 6.9, 10.2, 10.3
@settings(max_examples=100, deadline=None)
@given(case=_refused_rpcs())
def test_an_rpc_refused_by_the_interceptor_changes_no_node(
    case: _RejectedCall,
    store: CredentialStore,
) -> None:
    """A refused RPC leaves every node of the Bunch exactly as it was.

    The ``continuation`` rewrites the whole tree, so letting the call through
    fails the snapshot comparison rather than only the "was not called"
    assertion (Requirement 6.9).
    """
    interceptor = AuthInterceptor(auth_mode=AuthMode.ENABLED, credential_store=store)
    before = _snapshot(case.bunch)
    reached: List[str] = []
    context = _Context()

    async def continuation(details: Any) -> Any:
        reached.append(details.method)
        _sabotage(case.bunch)
        raise AssertionError("a refused RPC must not reach the continuation")

    async def run() -> None:
        handler = await interceptor.intercept_service(
            continuation,
            _HandlerCallDetails(method=case.method, invocation_metadata=case.metadata),
        )
        assert handler is not None
        # Running the substitute handler exercises the whole rejection path,
        # including the log record, which is where a stray write would sit.
        await handler.unary_unary(object(), context)

    # The rejection logs one WARNING per example; swallowing it keeps 100
    # examples from burying the test output.
    with contextlib.redirect_stderr(io.StringIO()):
        asyncio.run(run())

    assert reached == [], "the handler of a refused RPC must never run"
    assert len(context.aborted) == 1, "a refusal aborts the call exactly once"
    _assert_unchanged(case.bunch, before, "a refused RPC")


# the zombie rejection path ----------------------------------------------


def _run_child_command(scheduler: Scheduler, case: _ZombieCall) -> None:
    """Issue the drawn Child_Command through the real Scheduler method."""
    path = case.node_path
    if case.command == "init":
        asyncio.run(scheduler.run_command_init(path, case.args["task_id"]))
    elif case.command == "complete":
        scheduler.run_command_complete(path)
    elif case.command == "abort":
        scheduler.run_command_abort(path, case.args["reason"])
    elif case.command == "event":
        scheduler.run_command_event(path, case.args["name"])
    else:
        scheduler.run_command_meter(path, case.args["name"], case.args["value"])


# Feature: m2-security, Property 6: 拒绝路径的状态不变性
# Validates: Requirements 6.9, 10.2, 10.3
@settings(max_examples=100, deadline=None)
@given(case=_zombie_child_commands())
def test_a_zombie_rejected_under_fail_or_fob_changes_no_node(
    case: _ZombieCall,
) -> None:
    """``fail`` and ``fob`` both leave every node of the Bunch as it was.

    ``fail`` reports the rejection to the job (the ``ZombieError`` the handler
    maps to ``flag=31``) and ``fob`` hides it (the job is answered success), but
    neither may write anything: not the target task's status, ``task_id``,
    ``try_no`` or ``aborted_reason``, and not its Job_Password
    (Requirements 10.2, 10.3).
    """
    detector = ZombieDetector(auth_mode=case.auth_mode, zombie_policy=case.policy)
    scheduler = Scheduler(bunch=case.bunch, zombie_detector=detector)
    target: Task = case.bunch.find_node(case.node_path)
    before = _snapshot(case.bunch)

    token = set_call_credentials(case.credentials)
    try:
        # Every generated case is a zombie by construction; asserting it keeps a
        # future change in the detection from making this test vacuous.
        assert (
            detector.detect(
                target, case.command, case.task_id, credentials=case.credentials
            )
            is not None
        )
        # The disposition logs one WARNING per example, hence the redirect.
        with contextlib.redirect_stderr(io.StringIO()):
            if case.policy is ZombiePolicy.FAIL:
                with pytest.raises(ZombieError):
                    _run_child_command(scheduler, case)
            else:
                # ``fob`` answers success: the command returns normally.
                assert _run_child_command(scheduler, case) is None
    finally:
        reset_call_credentials(token)

    _assert_unchanged(case.bunch, before, f"a {case.policy.value}-ed zombie")


# Feature: m2-security, Property 6: 拒绝路径的状态不变性
# Validates: Requirements 6.9, 10.2, 10.3
@settings(max_examples=100, deadline=None)
@given(case=_zombie_child_commands())
def test_the_guard_answers_skip_under_fob_and_raises_under_fail(
    case: _ZombieCall,
) -> None:
    """The disposition itself neither writes nor adopts.

    Same generated cases, one layer lower: the guard is called directly, so a
    write performed by the disposition rather than by the command is attributed
    to the right place. ``adopt`` is not covered here because it takes the
    presented password over on purpose, which is a state change by design.
    """
    detector = ZombieDetector(auth_mode=case.auth_mode, zombie_policy=case.policy)
    target: Task = case.bunch.find_node(case.node_path)
    before = _snapshot(case.bunch)

    with contextlib.redirect_stderr(io.StringIO()):
        if case.policy is ZombiePolicy.FAIL:
            with pytest.raises(ZombieError):
                detector.guard(
                    target, case.command, case.task_id, credentials=case.credentials
                )
        else:
            action = detector.guard(
                target, case.command, case.task_id, credentials=case.credentials
            )
            assert action is ChildAction.SKIP

    _assert_unchanged(case.bunch, before, f"the {case.policy.value} disposition")
