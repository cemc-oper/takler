"""Unit tests for the shared FakeClock test double (see ``tests/conftest.py``).

FakeClock is injected into ``RetryPolicy``'s ``clock`` / ``sleep`` parameters,
so its own contract is worth pinning down: reading time, recording requested
sleeps, advancing logical time and never blocking.
"""

import time


def test_fake_clock_starts_at_zero_and_records_nothing(fake_clock):
    assert fake_clock() == 0.0
    assert fake_clock.slept == []
    assert fake_clock.sleep_count == 0
    assert fake_clock.total_slept == 0.0


def test_sleep_records_duration_and_advances_logical_time(fake_clock):
    fake_clock.sleep(1.0)
    fake_clock.sleep(2.0)
    fake_clock.sleep(4.0)

    assert fake_clock.slept == [1.0, 2.0, 4.0]
    assert fake_clock() == 7.0
    assert fake_clock.now == 7.0
    assert fake_clock.total_slept == 7.0
    assert fake_clock.sleep_count == 3


def test_sleep_does_not_really_sleep(fake_clock):
    started = time.monotonic()
    fake_clock.sleep(86400.0)
    elapsed = time.monotonic() - started

    assert fake_clock() == 86400.0
    assert elapsed < 1.0


def test_zero_sleep_is_recorded_but_does_not_move_time(fake_clock):
    fake_clock.sleep(0)

    assert fake_clock.slept == [0.0]
    assert fake_clock() == 0.0


def test_advance_moves_time_without_recording_a_sleep(fake_clock):
    fake_clock.advance(10.0)
    fake_clock.sleep(1.0)

    assert fake_clock() == 11.0
    assert fake_clock.slept == [1.0]


def test_can_be_used_as_clock_and_sleep_injection_pair(fake_clock):
    """Mimic how RetryPolicy consumes the two injection points."""
    clock = fake_clock
    sleep = fake_clock.sleep

    started = clock()
    for delay in (1.0, 2.0, 4.0):
        sleep(delay)
    elapsed = clock() - started

    assert elapsed == 7.0
    assert fake_clock.slept == [1.0, 2.0, 4.0]
