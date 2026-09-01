"""Zombie detection for Child_Commands.

A zombie is a Child_Command that does not belong to the run instance the server
currently records for the target task: the classic case is a job that was still
running when its task got requeued, and that then reports ``complete`` against
the fresh instance. Letting such a report through corrupts the state of the new
run, which is what the second M2 acceptance criterion is about.

This module owns both halves of the feature. Detection: the three
Zombie_Conditions (:class:`ZombieCondition`), the order in which they are
evaluated (:func:`detect_zombie_condition`) and the vocabulary the caller
answers with (:class:`ChildAction`). Disposition: what the configured
Zombie_Policy does about a hit (:func:`dispose_zombie`), which is the only place
that raises, logs or writes to the node.

The two halves stay separate functions on purpose: detection is a pure predicate
that can be tested on a hand-built task, and every state change of the feature
is confined to one function. :meth:`ZombieDetector.guard` joins them and is what
the Scheduler calls.

Both halves are free of any dependency on gRPC and on the Scheduler, and touch
the node tree only through :class:`~takler.core.task_node.Task`, so the whole
feature can be exercised without standing up a server.

No password ever leaves this module: the Job_Password and the ``takler-pass`` of
the call are read for one constant-time comparison and one adoption, and are
never put into a message, a return value or a log record (Requirements 10.9,
12.1).

Requirements: 9.2~9.8, 10.1~10.9.
"""

from __future__ import annotations

import enum
import hmac
from typing import Optional, Tuple

from takler.core.state import NodeStatus
from takler.core.task_node import Task
from takler.exceptions import ZombieError
from takler.logging import get_logger
from takler.server.auth import CallCredentials, get_call_credentials
from takler.server.connect_config import (
    DEFAULT_AUTH_MODE,
    DEFAULT_ZOMBIE_POLICY,
    AuthMode,
    ZombiePolicy,
)

__all__ = [
    "CHILD_COMMAND_INIT",
    "IN_FLIGHT_STATUSES",
    "ChildAction",
    "ZombieCondition",
    "ZombieDetector",
    "describe_zombie",
    "detect_zombie_condition",
    "dispose_zombie",
    "hits_z1",
    "hits_z2",
    "hits_z3",
]

logger = get_logger("server.zombie")


class ChildAction(enum.Enum):
    """What the caller of the zombie guard should do with the Child_Command.

    Two outcomes are enough to express all three policies without adding a
    result type between the Scheduler and the Network_Service, because the M1
    contract of ``TaklerService._handle_command`` already covers the third:
    "whatever ``op()`` returns is the success response, whatever it raises is
    the error response".

    * :attr:`PROCEED`: run the command. This covers both the ordinary
      no-zombie case and the ``adopt`` policy.
    * :attr:`SKIP`: do not run the command, but answer success. This is the
      ``fob`` policy: the caller returns early and the handler builds its usual
      ``flag=0`` response.

    The ``fail`` policy needs no member of its own -- it raises
    :class:`~takler.exceptions.ZombieError`, which the existing handler wrapper
    maps to ``flag=31``.
    """

    PROCEED = "proceed"
    SKIP = "skip"


class ZombieCondition(enum.Enum):
    """The reason a Child_Command was judged not to belong to the current run.

    The three conditions are independent tests, and which one fired has to
    survive the detection: the disposition log record names it (Requirement
    10.8) and so does the Audit_Record, so an operator reading the audit trail
    can tell "a stale job reported against a requeued task" (``Z2``) from "a
    job presented the password of another try" (``Z1``).

    * :attr:`Z1`: the credentials do not match the current run -- either the
      ``takler-pass`` of the call differs from the target task's Job_Password,
      or that task holds no password at all. Only evaluated when Auth_Mode is
      ``enabled`` (Requirements 9.2, 9.3, 9.4).
    * :attr:`Z2`: the target task is neither submitted nor active, so no job of
      it should be reporting anything (Requirement 9.5).
    * :attr:`Z3`: an ``init`` names a different job id than the one recorded for
      the active task, i.e. a second job claims a task that is already running
      (Requirement 9.6).

    The values are the identifiers used in the requirements, in the log records
    and in the Audit_Record, so the identifier never has to be spelled out a
    second time.
    """

    Z1 = "Z1"
    Z2 = "Z2"
    Z3 = "Z3"


#: The two statuses in which a task legitimately has a job that may report.
#:
#: Anything else means the server is not expecting a Child_Command for this
#: task: queued (it was requeued, or never ran), complete / aborted (the run
#: already finished) or unknown. This is the whole of ``Z2``
#: (Requirement 9.5), and it is also why the Checkpoint_File persists the
#: Job_Password of exactly these two statuses -- for any other status the
#: password is irrelevant, since ``Z2`` fires whether or not it matches.
IN_FLIGHT_STATUSES: Tuple[NodeStatus, ...] = (NodeStatus.submitted, NodeStatus.active)

#: Name of the one Child_Command that carries a job id, and therefore the only
#: one ``Z3`` applies to (Requirement 9.6).
#:
#: The command is identified by this short name -- the same one the Scheduler
#: passes to the guard -- rather than by the RPC method name, because the guard
#: sits inside the Scheduler where the RPC name is not known.
CHILD_COMMAND_INIT: str = "init"


def _constant_time_equal(left: Optional[str], right: Optional[str]) -> bool:
    """Compare two secrets without leaking their relationship through timing.

    :func:`hmac.compare_digest` takes an amount of time that does not depend on
    where the first differing byte is, which stops a caller from recovering a
    Job_Password one character at a time by measuring response latency
    (Requirement 9.8). The gain is modest here, since a job password is
    generated per try and lives for one run, but the cost of using the constant
    time comparison is a single call.

    Both operands are encoded to UTF-8 first, because ``compare_digest``
    refuses two :class:`str` arguments unless both are ASCII-only and raises
    :exc:`TypeError` otherwise. A password generated by
    :func:`secrets.token_urlsafe` is ASCII, but the ``takler-pass`` of the call
    comes from the network and may hold anything at all; comparing bytes makes
    the non-ASCII case an ordinary mismatch instead of an exception.

    Nothing raises. An exception on this path would surface as an internal
    server error rather than as a zombie disposition, which is both the wrong
    answer for the caller and a way to bypass the policy: an unpaired
    surrogate in the metadata would take the request down instead of being
    judged a mismatch.

    Args:
        left: One value, or ``None``.
        right: The other value, or ``None``.

    Returns:
        ``True`` only when both values are non-empty and equal. A ``None`` or
        empty operand never compares equal, not even to another empty one:
        "no password" is not a credential, so treating two absent passwords as
        a match would authenticate a caller that presented nothing.
    """
    if not left or not right:
        return False
    try:
        return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))
    except (UnicodeEncodeError, ValueError, TypeError):
        # UnicodeEncodeError: lone surrogates, which arrive from a decoded
        # metadata value. ValueError / TypeError: defensive, for a value that
        # is not the text it is typed as.
        return False


def hits_z1(node: Task, credentials: CallCredentials) -> bool:
    """Whether the credentials of the call fail to match the target task.

    Two situations are the same condition (Requirements 9.2, 9.3):

    * the ``takler-pass`` of the call differs from the task's Job_Password,
      which is what a job of an earlier try presents;
    * the task holds no Job_Password, so there is nothing a legitimate job of
      the current run could be presenting. A task in that state either never
      ran, was requeued, or came back from a checkpoint written before its
      password was persisted.

    A call carrying no ``takler-pass`` at all also lands here: with
    authentication enabled the interceptor already rejects such a
    Child_Command before the handler runs, so this is only reachable for an
    in-process caller, and "no password" must not match a password.

    Args:
        node: The target task.
        credentials: The Credential_Metadata of the call.

    Returns:
        ``True`` when the condition is hit. The caller is responsible for only
        asking when Auth_Mode is ``enabled`` (Requirement 9.4).
    """
    return not _constant_time_equal(credentials.job_password, node.job_password)


def hits_z2(node: Task) -> bool:
    """Whether the target task is in no state to be reported on.

    A Child_Command is only expected while the task is submitted or active. Any
    other status means the server has already moved on -- most often because
    the task was requeued back to queued while its job kept running -- so the
    report belongs to a run instance that no longer exists (Requirement 9.5).

    This condition needs no credentials, which is what makes zombie detection
    useful with authentication turned off: the requeue case is caught here
    whether or not ``Auth_Mode`` is ``enabled`` (Requirement 9.11).

    Args:
        node: The target task.

    Returns:
        ``True`` when the task's status is neither submitted nor active.
    """
    return node.state.node_status not in IN_FLIGHT_STATUSES


def hits_z3(node: Task, command: str, task_id: Optional[str]) -> bool:
    """Whether an ``init`` claims a task another job is already running.

    Applies to ``init`` only, and only while the task is active: ``init`` is
    the one Child_Command that carries a job id, and a mismatch is only
    meaningful once an id has been recorded, which is what the active status
    means here (Requirement 9.6). A second ``init`` with a different id says
    two jobs believe they own this run -- typically the old job re-initializing
    after a requeue-and-resubmit.

    A blank job id and an absent one are treated as the same "no id" value, so
    a task initialized without an id is not reported as a zombie when the next
    ``init`` also carries none. The comparison is a plain one: a job id is not
    a secret, it is an operational identifier that appears in logs and in the
    ``TAKLER_RID`` parameter, so there is nothing for a timing attack to
    recover.

    Args:
        node: The target task.
        command: Short name of the Child_Command being run.
        task_id: The job id the command carries, if any.

    Returns:
        ``True`` when the condition is hit.
    """
    if command != CHILD_COMMAND_INIT:
        return False
    if node.state.node_status is not NodeStatus.active:
        return False
    return (task_id or "") != (node.task_id or "")


def detect_zombie_condition(
    node: Task,
    command: str,
    task_id: Optional[str] = None,
    *,
    auth_mode: AuthMode = DEFAULT_AUTH_MODE,
    credentials: Optional[CallCredentials] = None,
) -> Optional[ZombieCondition]:
    """Return the first Zombie_Condition the Child_Command hits, if any.

    The three conditions are evaluated in the fixed order ``Z1``, ``Z2``,
    ``Z3`` and evaluation stops at the first hit (Requirement 9.7), so a
    command satisfying several of them is reported as the earliest one. The
    order is not arbitrary: it goes from the most specific diagnosis to the
    least. ``Z1`` says *which* run instance the caller belongs to, ``Z2`` only
    says the task is not expecting anyone, and a requeued task hits both --
    reporting ``Z1`` there tells the operator the caller held a stale password,
    which ``Z2`` alone would not.

    ``Z1`` is skipped entirely when Auth_Mode is ``disabled``
    (Requirement 9.4): without authentication no client is expected to send a
    ``takler-pass``, so every Child_Command would hit ``Z1`` and the server
    would reject the whole child protocol. ``Z2`` and ``Z3`` stay in force in
    both modes, which is what makes the requeue case work without
    authentication.

    This function only judges. It does not raise, does not log and does not
    touch the node -- deciding what a hit means, recording it and adopting
    anything belong to the Zombie_Policy layer above.

    Args:
        node: The target task, already located and type-checked by the caller.
            A missing node or a non-task node is not a zombie: it stays the
            ``NodeNotFoundError`` / ``NodeTypeError`` of M1 (Requirement 9.9).
        command: Short name of the Child_Command, one of ``init``,
            ``complete``, ``abort``, ``event``, ``meter``. Only ``init``
            participates in ``Z3``.
        task_id: The job id an ``init`` carries; unused by the other commands.
        auth_mode: The Auth_Mode in force, which decides whether ``Z1`` is
            evaluated. Defaults to the built-in default, ``disabled``, so a
            caller that has no notion of authentication -- a unit test, the
            TUI, an in-process run -- gets the credential-free behaviour rather
            than a spurious ``Z1``.
        credentials: The Credential_Metadata of the call. Defaults to the
            credentials the Auth_Interceptor published for the RPC being
            served, which is what makes the Scheduler able to run the check
            without the password being threaded through its signatures.

    Returns:
        The first :class:`ZombieCondition` hit, or ``None`` when the command
        belongs to the current run instance and may be executed.
    """
    if credentials is None:
        credentials = get_call_credentials()

    if auth_mode is AuthMode.ENABLED and hits_z1(node, credentials):
        return ZombieCondition.Z1

    if hits_z2(node):
        return ZombieCondition.Z2

    if hits_z3(node, command, task_id):
        return ZombieCondition.Z3

    return None


def describe_zombie(
    node: Task,
    command: str,
    condition: ZombieCondition,
    policy: ZombiePolicy,
) -> str:
    """Render the one-line description of a zombie disposition.

    The text carries the five facts Requirement 10.8 asks for -- node path,
    command name, the identifier of the condition that was hit, the policy in
    force and the current status of the target task -- and is used for both the
    WARNING record and the :class:`~takler.exceptions.ZombieError` message, so
    the operator reading the log and the job reading its command output see the
    same account of what happened.

    It deliberately carries no password, neither the task's nor the call's
    (Requirement 10.9). The status is included instead, which is what actually
    explains the hit in the common ``Z2`` case: "the task is queued, so no job
    of it should be reporting".

    Args:
        node: The target task.
        command: Short name of the Child_Command.
        condition: The Zombie_Condition that was hit.
        policy: The Zombie_Policy being applied.

    Returns:
        A single line with no password in it.
    """
    return (
        f"zombie {command} on {node.node_path}: "
        f"hit {condition.value}, status is {node.state.node_status.name}, "
        f"applying policy {policy.value}"
    )


def dispose_zombie(
    node: Task,
    command: str,
    condition: ZombieCondition,
    *,
    policy: ZombiePolicy = DEFAULT_ZOMBIE_POLICY,
    credentials: Optional[CallCredentials] = None,
) -> ChildAction:
    """Apply the Zombie_Policy to a Child_Command that hit a Zombie_Condition.

    Every disposition is logged once as a WARNING before it takes effect
    (Requirements 10.8, 10.9), so the three policies leave the same trace and an
    operator can tell that a ``fob``-ed command was silently accepted -- the
    client sees ``flag=0`` and has no way to know.

    The three policies (Requirements 10.2~10.7):

    * ``fail``: raise :class:`~takler.exceptions.ZombieError`. Nothing is
      written to the node, so its status, ``task_id``, ``try_no``,
      ``aborted_reason`` and Job_Password all stay as they were, and
      ``TaklerService._handle_command`` turns the exception into ``flag=31``
      under the M1 contract.
    * ``fob``: return :attr:`ChildAction.SKIP`. Again nothing is written; the
      caller returns before running the command, and the handler builds its
      usual ``flag=0`` response.
    * ``adopt``: return :attr:`ChildAction.PROCEED` so the caller runs the
      command, after taking over the ``takler-pass`` of the call as the node's
      Job_Password. Nothing else is adopted here: the ``task_id`` of a ``Z3``
      ``init`` is set by ``Task.init()`` itself when the command runs
      (Requirement 10.7), and duplicating that assignment would only create a
      second place for it to drift.

    Args:
        node: The target task, already located and type-checked.
        command: Short name of the Child_Command.
        condition: The Zombie_Condition that was hit, as returned by
            :func:`detect_zombie_condition`.
        policy: The Zombie_Policy in force. Defaults to ``fail``, the built-in
            default and the only policy of the three that neither hides the
            zombie nor lets it write to the current run.
        credentials: The Credential_Metadata of the call, from which ``adopt``
            takes the password. Defaults to the credentials the
            Auth_Interceptor published for the RPC being served.

    Returns:
        :attr:`ChildAction.SKIP` under ``fob``, :attr:`ChildAction.PROCEED`
        under ``adopt``.

    Raises:
        ZombieError: Under ``fail``.
    """
    if credentials is None:
        credentials = get_call_credentials()

    description = describe_zombie(node, command, condition, policy)
    logger.warning(description)

    if policy is ZombiePolicy.FAIL:
        raise ZombieError(description)

    if policy is ZombiePolicy.FOB:
        return ChildAction.SKIP

    # ADOPT. A blank ``takler-pass`` counts as "not carried" (Requirement 10.6)
    # rather than as a password to adopt: storing an empty or whitespace-only
    # value would leave the task with a Job_Password that no job can present
    # meaningfully, and an empty one is itself ``Z1`` for the next command of the
    # very job being adopted. Treating blank as unset is also how the client
    # treats a blank ``TAKLER_PASS`` -- it does not send the key at all -- so the
    # two ends agree on what "carried a password" means.
    if credentials.job_password and credentials.job_password.strip():
        node.job_password = credentials.job_password

    return ChildAction.PROCEED


class ZombieDetector:
    """Detects and disposes of zombie Child_Commands on behalf of the server.

    The detector holds the two server-global settings the feature depends on, so
    the Scheduler does not have to pass them at every call site: Auth_Mode
    decides whether ``Z1`` is evaluated, and Zombie_Policy decides what a hit
    means.

    The instance carries no per-call state, so one detector serves every RPC.
    """

    def __init__(
        self,
        auth_mode: AuthMode = DEFAULT_AUTH_MODE,
        zombie_policy: ZombiePolicy = DEFAULT_ZOMBIE_POLICY,
    ) -> None:
        """Bind the detector to the Auth_Mode and Zombie_Policy in force.

        Args:
            auth_mode: The resolved Auth_Mode. Defaults to ``disabled``, the
                built-in default, which skips ``Z1``.
            zombie_policy: The resolved Zombie_Policy. Defaults to ``fail``, the
                built-in default.
        """
        self._auth_mode = auth_mode
        self._zombie_policy = zombie_policy

    @property
    def auth_mode(self) -> AuthMode:
        """The Auth_Mode this detector applies."""
        return self._auth_mode

    @property
    def zombie_policy(self) -> ZombiePolicy:
        """The Zombie_Policy this detector applies."""
        return self._zombie_policy

    def detect(
        self,
        node: Task,
        command: str,
        task_id: Optional[str] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> Optional[ZombieCondition]:
        """Return the first Zombie_Condition ``command`` hits on ``node``.

        See :func:`detect_zombie_condition`, of which this is the bound form.

        Args:
            node: The target task.
            command: Short name of the Child_Command.
            task_id: The job id an ``init`` carries.
            credentials: The Credential_Metadata of the call, defaulting to the
                credentials published for the RPC being served.

        Returns:
            The first :class:`ZombieCondition` hit, or ``None``.
        """
        return detect_zombie_condition(
            node,
            command,
            task_id,
            auth_mode=self._auth_mode,
            credentials=credentials,
        )

    def guard(
        self,
        node: Task,
        command: str,
        task_id: Optional[str] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> ChildAction:
        """Judge a Child_Command and dispose of it, returning what to do next.

        This is the single entry point the Scheduler calls, once per
        Child_Command, after locating and type-checking the node and before any
        state is written (Requirement 9.1). A command that hits no condition is
        answered with :attr:`ChildAction.PROCEED` without any log record and
        without the node being touched -- in particular its Job_Password is left
        alone (Requirements 10.11, 10.12), since the ordinary path is the
        overwhelming majority of calls and must stay silent.

        The same ``credentials`` serve both halves, so the ``takler-pass`` that
        failed the ``Z1`` comparison is the one ``adopt`` takes over.

        Args:
            node: The target task.
            command: Short name of the Child_Command.
            task_id: The job id an ``init`` carries.
            credentials: The Credential_Metadata of the call, defaulting to the
                credentials published for the RPC being served.

        Returns:
            :attr:`ChildAction.PROCEED` to run the command,
            :attr:`ChildAction.SKIP` to answer success without running it.

        Raises:
            ZombieError: A condition was hit and the Zombie_Policy is ``fail``.
        """
        if credentials is None:
            credentials = get_call_credentials()

        condition = self.detect(node, command, task_id, credentials=credentials)
        if condition is None:
            return ChildAction.PROCEED

        return dispose_zombie(
            node,
            command,
            condition,
            policy=self._zombie_policy,
            credentials=credentials,
        )
