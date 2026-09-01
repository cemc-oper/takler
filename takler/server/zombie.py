"""Zombie detection for Child_Commands.

A zombie is a Child_Command that does not belong to the run instance the server
currently records for the target task: the classic case is a job that was still
running when its task got requeued, and that then reports ``complete`` against
the fresh instance. Letting such a report through corrupts the state of the new
run, which is what the second M2 acceptance criterion is about.

This module owns the *detection* half of the feature: the three
Zombie_Conditions (:class:`ZombieCondition`), the order in which they are
evaluated (:func:`detect_zombie_condition`) and the vocabulary the caller
answers with (:class:`ChildAction`). What to *do* about a detected zombie is the
Zombie_Policy's business and lives on top of this: the detector reports which
condition was hit and never touches the node.

Detection is deliberately free of any dependency on gRPC, on the Scheduler and
on the node tree beyond :class:`~takler.core.task_node.Task` itself, so each
condition can be exercised on a hand-built task without standing up a server.

Nothing here logs, and nothing here returns a password: the Job_Password and the
``takler-pass`` of the call are read for one constant-time comparison and are
never put into a message, a return value or a log record (Requirements 10.9,
12.1).

Requirements: 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8.
"""

from __future__ import annotations

import enum
import hmac
from typing import Optional, Tuple

from takler.core.state import NodeStatus
from takler.core.task_node import Task
from takler.server.auth import CallCredentials, get_call_credentials
from takler.server.connect_config import DEFAULT_AUTH_MODE, AuthMode

__all__ = [
    "CHILD_COMMAND_INIT",
    "IN_FLIGHT_STATUSES",
    "ChildAction",
    "ZombieCondition",
    "ZombieDetector",
    "detect_zombie_condition",
    "hits_z1",
    "hits_z2",
    "hits_z3",
]


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


class ZombieDetector:
    """Applies the Zombie_Conditions on behalf of the server.

    The detector holds the server-global settings the conditions depend on, so
    the Scheduler does not have to pass them at every call site. Only Auth_Mode
    is needed for the detection itself; the Zombie_Policy joins it when the
    disposition is added on top.

    The instance carries no per-call state, so one detector serves every RPC.
    """

    def __init__(self, auth_mode: AuthMode = DEFAULT_AUTH_MODE) -> None:
        """Bind the detector to the Auth_Mode in force.

        Args:
            auth_mode: The resolved Auth_Mode. Defaults to ``disabled``, the
                built-in default, which skips ``Z1``.
        """
        self._auth_mode = auth_mode

    @property
    def auth_mode(self) -> AuthMode:
        """The Auth_Mode this detector applies."""
        return self._auth_mode

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
