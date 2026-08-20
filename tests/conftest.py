"""Root-level shared fixtures for the takler test suite.

Currently this module provides the fake clock used to exercise the client-side
retry logic without ever really sleeping.

`RetryPolicy` (``takler/client/retry.py``) takes its time source and its sleep
function as constructor parameters::

    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep

:class:`FakeClock` is designed to be injected into *both* of those points at
once (``clock=fake_clock, sleep=fake_clock.sleep``) so that the policy sees a
consistent logical timeline: every requested sleep is recorded and advances the
logical time instead of blocking the process. That makes it possible to cover
long retry windows (5 minutes for the M1 acceptance scenario, up to 86400
seconds for the child-command default window) in milliseconds of wall time.
"""

from __future__ import annotations

from typing import List

import pytest


class FakeClock:
    """A logical clock that never really sleeps.

    Usage with an injectable retry policy::

        clock = FakeClock()
        policy = RetryPolicy(retry_window=300.0, clock=clock, sleep=clock.sleep)

    Calling the instance returns the current logical time (so the instance
    itself satisfies ``Callable[[], float]``). :meth:`sleep` satisfies
    ``Callable[[float], None]``: it records the requested duration in
    :attr:`slept` and advances :attr:`now` by that amount, without blocking.

    Attributes:
        now: Current logical time, in seconds.
        slept: Every duration passed to :meth:`sleep`, in call order. Tests use
            this to assert on the exact backoff sequence.
    """

    def __init__(self, start: float = 0.0) -> None:
        self.now: float = float(start)
        self.slept: List[float] = []

    def __call__(self) -> float:
        """Return the current logical time (the ``clock`` injection point)."""
        return self.now

    def sleep(self, seconds: float) -> None:
        """Record ``seconds`` and advance logical time; never really sleeps."""
        seconds = float(seconds)
        self.slept.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        """Advance logical time without recording a sleep.

        Useful to simulate time consumed by the operation itself (for example a
        single RPC attempt burning its timeout) as opposed to time spent
        waiting between retries.
        """
        self.now += float(seconds)

    @property
    def total_slept(self) -> float:
        """Total logical time spent in :meth:`sleep`."""
        return sum(self.slept)

    @property
    def sleep_count(self) -> int:
        """How many times :meth:`sleep` was called."""
        return len(self.slept)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"FakeClock(now={self.now!r}, slept={self.slept!r})"


@pytest.fixture
def fake_clock() -> FakeClock:
    """A fresh :class:`FakeClock` starting at logical time 0.0."""
    return FakeClock()
