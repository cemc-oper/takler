"""Boundary unit tests for the client retry policy (``takler/client/retry.py``).

The exhaustive "for all inputs" assertions live in the property tests; this file
only pins the boundaries that are easy to get wrong: the backoff cap, the
``TAKLER_TIMEOUT`` fallbacks and the ``Retry_Window == 0`` single attempt case.
"""

from __future__ import annotations

import contextlib
import io

import grpc
import pytest

import takler.logging
from takler.client.retry import (
    DEFAULT_RETRY_WINDOW_BY_KIND,
    DEFAULT_SINGLE_TIMEOUT,
    ENV_RETRY_WINDOW,
    MAX_BACKOFF_SECONDS,
    NON_RETRYABLE_EXCEPTION_BY_STATUS,
    RETRYABLE_STATUS_CODES,
    CommandKind,
    RetryPolicy,
    backoff_seconds,
    resolve_retry_window,
)
from takler.exceptions import (
    InvalidRequestError,
    NodeNotFoundError,
    PermissionDeniedError,
    TaklerError,
)


def test_constants():
    assert DEFAULT_SINGLE_TIMEOUT == 10.0
    assert MAX_BACKOFF_SECONDS == 60.0
    assert set(CommandKind) == {
        CommandKind.CHILD,
        CommandKind.CONTROL,
        CommandKind.QUERY,
    }
    assert DEFAULT_RETRY_WINDOW_BY_KIND[CommandKind.CHILD] == 86400.0
    assert DEFAULT_RETRY_WINDOW_BY_KIND[CommandKind.CONTROL] == 60.0
    assert DEFAULT_RETRY_WINDOW_BY_KIND[CommandKind.QUERY] == 60.0


def test_status_code_classification_is_disjoint():
    assert RETRYABLE_STATUS_CODES == frozenset(
        {
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.UNKNOWN,
        }
    )
    assert NON_RETRYABLE_EXCEPTION_BY_STATUS == {
        grpc.StatusCode.INVALID_ARGUMENT: InvalidRequestError,
        grpc.StatusCode.NOT_FOUND: NodeNotFoundError,
        grpc.StatusCode.PERMISSION_DENIED: PermissionDeniedError,
        grpc.StatusCode.UNAUTHENTICATED: PermissionDeniedError,
    }
    assert not (RETRYABLE_STATUS_CODES & set(NON_RETRYABLE_EXCEPTION_BY_STATUS))
    for exc_type in NON_RETRYABLE_EXCEPTION_BY_STATUS.values():
        assert issubclass(exc_type, TaklerError)


def test_backoff_sequence_and_cap():
    assert [backoff_seconds(n) for n in range(1, 8)] == [
        1.0,
        2.0,
        4.0,
        8.0,
        16.0,
        32.0,
        60.0,
    ]
    # Long lived child commands must not overflow or exceed the cap.
    assert backoff_seconds(10_000) == MAX_BACKOFF_SECONDS


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("0", 0.0),
        ("1", 1.0),
        ("300", 300.0),
        (" 300 ", 300.0),
    ],
)
def test_resolve_retry_window_accepts_non_negative_integers(raw, expected):
    window = resolve_retry_window(CommandKind.CONTROL, env={ENV_RETRY_WINDOW: raw})
    assert window == expected


@pytest.mark.parametrize(
    "kind, expected",
    [
        (CommandKind.CHILD, 86400.0),
        (CommandKind.CONTROL, 60.0),
        (CommandKind.QUERY, 60.0),
    ],
)
def test_resolve_retry_window_unset_uses_kind_default(kind, expected):
    assert resolve_retry_window(kind, env={}) == expected


def _resolve_capturing_stderr(kind, env):
    """Resolve a window while capturing the console log output.

    The active backend binds its console sink to ``sys.stderr`` when the
    configuration is applied, so the configuration has to happen inside the
    redirection block for the records to land in the buffer. This mirrors the
    capturing approach used by the logging test suite.
    """
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="WARNING", console=True)
            window = resolve_retry_window(kind, env=env)
    finally:
        # Rebind the console sink to the real stderr so later tests are not
        # left logging into this example's closed buffer.
        takler.logging.configure(console=True)
    return window, buffer.getvalue()


@pytest.mark.parametrize("raw", ["", "   ", "abc", "-1", "1.5", "1_0", "12x"])
def test_resolve_retry_window_invalid_warns_and_falls_back(raw):
    window, captured = _resolve_capturing_stderr(
        CommandKind.CHILD, {ENV_RETRY_WINDOW: raw}
    )

    assert window == 86400.0
    warnings = [line for line in captured.splitlines() if "WARNING" in line]
    assert len(warnings) == 1
    assert ENV_RETRY_WINDOW in warnings[0]
    assert repr(raw) in warnings[0]


@pytest.mark.parametrize("env", [{}, {ENV_RETRY_WINDOW: "300"}])
def test_resolve_retry_window_valid_or_unset_is_silent(env):
    _, captured = _resolve_capturing_stderr(CommandKind.QUERY, env)
    assert "WARNING" not in captured


def test_next_delay_zero_window_means_single_attempt(fake_clock):
    policy = RetryPolicy(retry_window=0.0, clock=fake_clock, sleep=fake_clock.sleep)
    assert policy.next_delay(attempt=1, elapsed=0.0) is None


def test_next_delay_is_clipped_to_remaining_window(fake_clock):
    policy = RetryPolicy(retry_window=10.0, clock=fake_clock, sleep=fake_clock.sleep)
    assert policy.next_delay(attempt=1, elapsed=0.0) == 1.0
    # 4 seconds of backoff but only 2.5 left in the window.
    assert policy.next_delay(attempt=3, elapsed=7.5) == 2.5
    assert policy.next_delay(attempt=3, elapsed=10.0) is None
    assert policy.next_delay(attempt=3, elapsed=11.0) is None


def test_accumulated_sleep_never_exceeds_window(fake_clock):
    policy = RetryPolicy(retry_window=5.0, clock=fake_clock, sleep=fake_clock.sleep)
    started = policy.clock()
    attempt = 0
    while True:
        attempt += 1
        delay = policy.next_delay(attempt, policy.clock() - started)
        if delay is None:
            break
        policy.sleep(delay)

    assert fake_clock.total_slept == pytest.approx(5.0)
    assert fake_clock.slept == [1.0, 2.0, 2.0]


def test_policy_defaults_to_real_time_sources():
    import time as _time

    policy = RetryPolicy(retry_window=60.0)
    assert policy.single_timeout == DEFAULT_SINGLE_TIMEOUT
    assert policy.clock is _time.monotonic
    assert policy.sleep is _time.sleep
