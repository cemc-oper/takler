"""Retry policy and time sources for the client side Call_Wrapper.

This module holds everything the client needs to decide *whether* and *how
long* to wait before retrying an RPC, deliberately separated from the RPC
plumbing in :mod:`takler.client.service_client`:

* the command classification (:class:`CommandKind`) that selects the default
  retry window,
* the gRPC status code classification (:data:`RETRYABLE_STATUS_CODES` and
  :data:`NON_RETRYABLE_EXCEPTION_BY_STATUS`),
* the transport-neutral failure classification (:class:`FailureCategory` and
  :class:`FailureVerdict`) every client transport maps its wire failures to,
  and the shared retry loop (:func:`run_with_retry`) driven by the verdicts
  (M3 task 8),
* the backoff schedule (:func:`backoff_seconds`),
* the ``TAKLER_TIMEOUT`` resolution (:func:`resolve_retry_window`),
* and the window bookkeeping (:class:`RetryPolicy`).

:class:`RetryPolicy` takes its time source and its sleep function as fields, so
tests can drive a five minute outage in microseconds by injecting a fake clock
(see ``FakeClock`` in ``tests/conftest.py``) instead of really sleeping.

The gRPC status code tables stay here -- they *are* the gRPC transport's share
of the classification -- but the loop that consumes a classification knows no
gRPC: each transport maps its own failures (gRPC status codes, HTTP status
codes, connection errors) to a :class:`FailureVerdict`, and
:func:`run_with_retry` turns the verdict into "retry", "raise the mapped
exception" or "raise ``TransportError``". That is what makes the Retry_Window,
the backoff and the resulting exception types -- and with them the CLI's exit
codes -- identical whichever transport carries the call.

Requirements: 9.2, 9.3, 9.4, 9.8, 9.9, 9.10, 9.11, 9.12, 9.13.
"""

from __future__ import annotations

import dataclasses
import enum
import os
import re
import time
from typing import Callable, Dict, Mapping, Optional, Type, TypeVar

import grpc

from takler.exceptions import (
    ClientConnectionError,
    InvalidRequestError,
    NodeNotFoundError,
    PermissionDeniedError,
    TaklerError,
    TransportError,
)
from takler.logging import get_logger
from takler.protocol.commands import Command

__all__ = [
    "CommandKind",
    "COMMAND_KIND_BY_COMMAND",
    "ENV_RETRY_WINDOW",
    "DEFAULT_SINGLE_TIMEOUT",
    "MAX_BACKOFF_SECONDS",
    "DEFAULT_RETRY_WINDOW_BY_KIND",
    "RETRYABLE_STATUS_CODES",
    "NON_RETRYABLE_EXCEPTION_BY_STATUS",
    "FailureCategory",
    "FailureVerdict",
    "backoff_seconds",
    "resolve_retry_window",
    "run_with_retry",
    "RetryPolicy",
]

logger = get_logger("client")

T = TypeVar("T")


class CommandKind(enum.Enum):
    """How a client call is classified for retry purposes.

    The classification only selects the default Retry_Window: a child command
    runs inside a job script, where losing a status update desynchronizes the
    server from reality, so it keeps retrying for a day (requirement 9.10). An
    interactive control or query command must not hang an operator's terminal,
    so it gives up after a minute (requirement 9.11).
    """

    CHILD = "child"
    CONTROL = "control"
    QUERY = "query"


#: The CommandKind of every command (requirements 9.10, 9.11).
#:
#: The classification follows the caller, not the wire: the five
#: Child_Commands are reported from inside a job script and retry for a day,
#: the eight Control_Commands and the three Query_Commands are typed by an
#: operator and give up after a minute. A literal table rather than a name
#: pattern, for the same reason as the server's privilege table: a future
#: command must be classified deliberately, not by accident of its name. The
#: module's tests pin the table against the sixteen Commands.
COMMAND_KIND_BY_COMMAND: Dict[Command, CommandKind] = {
    Command.INIT: CommandKind.CHILD,
    Command.COMPLETE: CommandKind.CHILD,
    Command.ABORT: CommandKind.CHILD,
    Command.EVENT: CommandKind.CHILD,
    Command.METER: CommandKind.CHILD,
    Command.REQUEUE: CommandKind.CONTROL,
    Command.SUSPEND: CommandKind.CONTROL,
    Command.RESUME: CommandKind.CONTROL,
    Command.RUN: CommandKind.CONTROL,
    Command.FORCE: CommandKind.CONTROL,
    Command.FREE_DEP: CommandKind.CONTROL,
    Command.LOAD: CommandKind.CONTROL,
    Command.BEGIN: CommandKind.CONTROL,
    Command.SHOW: CommandKind.QUERY,
    Command.PING: CommandKind.QUERY,
    Command.COROUTINE: CommandKind.QUERY,
}


#: Environment variable holding the Retry_Window in seconds (requirement 9.9).
ENV_RETRY_WINDOW: str = "TAKLER_TIMEOUT"

#: Per-call deadline used when none is configured, in seconds (requirement
#: 9.2). Every RPC carries it, so even a wedged TCP connection turns into a
#: ``DEADLINE_EXCEEDED`` and enters the retry loop instead of blocking forever.
DEFAULT_SINGLE_TIMEOUT: float = 10.0

#: Upper bound of the exponential backoff, in seconds (requirement 9.4).
MAX_BACKOFF_SECONDS: float = 60.0

#: Default Retry_Window per command kind, in seconds (requirements 9.10, 9.11).
DEFAULT_RETRY_WINDOW_BY_KIND: Mapping[CommandKind, float] = {
    CommandKind.CHILD: 86400.0,
    CommandKind.CONTROL: 60.0,
    CommandKind.QUERY: 60.0,
}

#: gRPC status codes that mean "transport level failure, worth retrying"
#: (requirement 9.3).
RETRYABLE_STATUS_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.UNKNOWN,
    }
)

#: gRPC status codes that mean "the request itself is wrong, retrying cannot
#: help", mapped to the exception the client raises (requirement 9.8).
NON_RETRYABLE_EXCEPTION_BY_STATUS: Mapping[grpc.StatusCode, Type[TaklerError]] = {
    grpc.StatusCode.INVALID_ARGUMENT: InvalidRequestError,
    grpc.StatusCode.NOT_FOUND: NodeNotFoundError,
    grpc.StatusCode.PERMISSION_DENIED: PermissionDeniedError,
    grpc.StatusCode.UNAUTHENTICATED: PermissionDeniedError,
}

# A non-negative integer, and nothing else. ``[0-9]`` instead of ``\d`` on
# purpose: ``\d`` also matches non-ASCII digits, which ``int()`` happens to
# accept, and silently reading a retry window out of such a string would be
# more surprising than reporting it as unparseable.
_NON_NEGATIVE_INT = re.compile(r"^[0-9]+$")


def backoff_seconds(attempt: int) -> float:
    """Return the wait before retry number ``attempt``, in seconds.

    Implements ``min(2 ** (attempt - 1), 60)`` (requirement 9.4): 1, 2, 4, 8,
    16, 32, then 60 for every later retry.

    Args:
        attempt: 1-based retry number.

    Returns:
        The backoff duration in seconds, never above
        :data:`MAX_BACKOFF_SECONDS`.
    """
    # 2 ** 6 == 64 already exceeds the cap, so short-circuit instead of
    # computing a huge power for a long lived child command.
    if attempt >= 7:
        return MAX_BACKOFF_SECONDS
    return min(2.0 ** (attempt - 1), MAX_BACKOFF_SECONDS)


def resolve_retry_window(
    kind: CommandKind,
    env: Optional[Mapping[str, str]] = None,
) -> float:
    """Resolve the Retry_Window in seconds for ``kind``.

    ``TAKLER_TIMEOUT`` wins when it holds a non-negative integer string
    (requirement 9.9). When it is unset, the default for the command kind
    applies without any log noise (requirements 9.10, 9.11). When it is set but
    empty, whitespace only, or not parseable as a non-negative integer, a single
    WARNING naming the offending value is logged and the same default applies
    (requirement 9.12).

    Args:
        kind: The command classification selecting the default window.
        env: Environment mapping to read; defaults to :data:`os.environ`.

    Returns:
        The Retry_Window in seconds. ``0.0`` is a legitimate result and means
        "one attempt, no retry" (requirement 9.13).
    """
    default_window = DEFAULT_RETRY_WINDOW_BY_KIND[kind]

    if env is None:
        env = os.environ

    raw = env.get(ENV_RETRY_WINDOW)
    if raw is None:
        return default_window

    text = raw.strip()
    if _NON_NEGATIVE_INT.match(text):
        return float(int(text))

    logger.warning(
        f"invalid {ENV_RETRY_WINDOW} value {raw!r}; "
        f"falling back to {default_window:g} seconds "
        f"for {kind.value} commands."
    )
    return default_window


@dataclasses.dataclass
class RetryPolicy:
    """Retry window bookkeeping with injectable time and sleep.

    The policy owns no RPC knowledge: the Call_Wrapper decides which failures
    are retryable and asks :meth:`next_delay` how long to wait, or whether the
    window is over.

    Attributes:
        retry_window: Total time budget for one logical call, counted from the
            first attempt, in seconds.
        single_timeout: Per-attempt deadline handed to gRPC (requirement 9.2).
        clock: Monotonic time source; the injection point that lets tests span
            long windows instantly.
        sleep: Blocking sleep; the second injection point.
    """

    retry_window: float
    single_timeout: float = DEFAULT_SINGLE_TIMEOUT
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep

    def next_delay(self, attempt: int, elapsed: float) -> Optional[float]:
        """Return how long to wait before retry ``attempt``, or ``None``.

        The delay is :func:`backoff_seconds` clipped to the time left in the
        window, so the accumulated wait can never exceed
        :attr:`retry_window` (requirements 9.3, 9.4).

        Args:
            attempt: 1-based number of the retry that is about to happen.
            elapsed: Time spent since the first attempt, in seconds.

        Returns:
            The number of seconds to wait, or ``None`` when the window is
            exhausted and no further retry may happen. With
            ``retry_window == 0`` the first failure already has
            ``elapsed >= 0 == retry_window``, so the answer is ``None`` and
            exactly one attempt happens (requirement 9.13).
        """
        remaining = self.retry_window - elapsed
        if remaining <= 0.0:
            return None
        return min(backoff_seconds(attempt), remaining)


# Transport-neutral failure classification (M3 task 8) --------------------


class FailureCategory(enum.Enum):
    """How one failed attempt is classified, transport-neutrally.

    Every client transport maps its wire failures -- gRPC status codes on one
    side, HTTP status codes and connection errors on the other -- to one of
    three answers (requirement 9.3 and 9.8, generalized):

    * :attr:`RETRYABLE`: a transport-level failure worth spending the
      Retry_Window on (``UNAVAILABLE``, ``HTTP 503``, a refused connection);
    * :attr:`NON_RETRYABLE`: the request itself is wrong, so retrying cannot
      help; the verdict names the takler exception to raise
      (``PERMISSION_DENIED``, ``HTTP 401``);
    * :attr:`FATAL`: any other transport failure; the client raises
      ``TransportError`` without retrying.
    """

    RETRYABLE = "retryable"
    NON_RETRYABLE = "non_retryable"
    FATAL = "fatal"


@dataclasses.dataclass
class FailureVerdict:
    """The classification of one failed attempt.

    Attributes:
        category: What the retry loop should do with the failure.
        failure_name: Printable identity of the failure, used in the exception
            messages both transports produce in the same shape: ``gRPC status
            UNAVAILABLE``, ``HTTP status 503``, ``connection error
            (ConnectError)``.
        log_field: The ``key=value`` fragment closing the retry WARNING line
            (``status=UNAVAILABLE`` on gRPC, ``status=503`` or
            ``error=ConnectError`` on HTTP).
        details: Server-supplied or exception detail text, possibly empty.
        exception_type: The takler exception to raise for
            :attr:`FailureCategory.NON_RETRYABLE`; ``None`` otherwise.
    """

    category: FailureCategory
    failure_name: str
    log_field: str
    details: str = ""
    exception_type: Optional[Type[TaklerError]] = None


def run_with_retry(
    policy: RetryPolicy,
    operation_name: str,
    listen_address: str,
    attempt: Callable[[], T],
    classify: Callable[[Exception], FailureVerdict],
) -> T:
    """Run ``attempt`` under the Retry_Window, classifying each failure.

    This is the transport-neutral spine of the client Call_Wrapper
    (requirement 9.1, generalized to both transports in M3 task 8): the
    per-call policy is already resolved and the wire-specific work is entirely
    inside the two callables, so the Retry_Window bookkeeping, the backoff and
    the resulting exception types -- and with them the CLI's exit codes -- are
    identical whichever transport carries the call.

    Args:
        policy: The resolved policy of this logical call.
        operation_name: The command name used in log and exception messages,
            for example ``complete``.
        listen_address: The server address the messages name.
        attempt: One attempt of the call. Must raise on transport failure; a
            returned value -- including a response whose ``flag`` is non zero
            -- ends the loop (requirement 9.7).
        classify: Maps the exception of a failed attempt to its verdict. An
            exception the transport does not recognize is re-raised from
            inside ``classify`` rather than classified, so a programming error
            can never masquerade as a wire failure.

    Returns:
        Whatever ``attempt`` returned on the successful attempt.

    Raises:
        InvalidRequestError, NodeNotFoundError, PermissionDeniedError:
            A :attr:`FailureCategory.NON_RETRYABLE` verdict (requirement 9.8).
        ClientConnectionError: The Retry_Window is exhausted
            (requirement 9.5).
        TransportError: A :attr:`FailureCategory.FATAL` verdict.
    """
    started = policy.clock()
    attempt_number = 0

    while True:
        attempt_number += 1
        try:
            return attempt()
        except Exception as exc:
            verdict = classify(exc)

            # Non retryable: the request itself is wrong, so spending the
            # retry window on it only delays the error.
            if verdict.category is FailureCategory.NON_RETRYABLE:
                raise verdict.exception_type(
                    f"{operation_name} on server {listen_address} "
                    f"failed with {verdict.failure_name}: {verdict.details}"
                ) from exc

            if verdict.category is FailureCategory.FATAL:
                raise TransportError(
                    f"{operation_name} on server {listen_address} "
                    f"failed with {verdict.failure_name}: {verdict.details}"
                ) from exc

            elapsed = policy.clock() - started
            delay = policy.next_delay(attempt_number, elapsed)
            if delay is None:
                raise ClientConnectionError(
                    f"server {listen_address} is unreachable after "
                    f"{attempt_number} attempts, "
                    f"last {verdict.failure_name}"
                ) from exc

            logger.warning(
                f"retry {operation_name} to {listen_address}: "
                f"elapsed={elapsed:.1f}s, {verdict.log_field}"
            )
            policy.sleep(delay)
