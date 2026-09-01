"""Property-based test for job password uniqueness across runs.

Covers Property 2 from the ``m2-security`` design: for any ``n >= 2``, the ``n``
job passwords produced by ``n`` consecutive calls to ``Task.increment_try_no``
are pairwise distinct (Requirement 4.3).

What this property does and does not establish
----------------------------------------------
Pairwise distinctness over a bounded run is a smoke test for "the password
source is neither a constant nor a counter, and it is not reused between runs of
the same task". It says nothing about the *strength* of the source: a weak but
non-repeating generator (a timestamp, a shuffled sequence, a 16-bit PRNG over a
short run) would pass this property just as easily as a CSPRNG.

The real guarantee comes from the implementation choice, not from this test:
``Task.increment_try_no`` uses ``secrets.token_urlsafe(32)``, i.e. 32 bytes
drawn from ``os.urandom``. No test can verify that the underlying source is
cryptographically secure - a collision would be astronomically unlikely to
appear in any feasible number of examples either way. Being explicit about that
boundary here is deliberate, so a later reader does not read a green run of this
file as evidence about randomness quality. Requirement 4.2 (length at least 32)
is asserted by the unit tests in ``test_job_password.py``.

No password value reaches a print, a test name or an assertion message: the
passwords are only ever compared inside assertion expressions, and the failure
messages carry counts, never values.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from takler.core import Task

# Upper bound on ``n``. The property is universally quantified over ``n >= 2``,
# but each increment is an independent draw from the same source, so any
# collision this test could catch (a constant, a counter reset, a reused value)
# shows up at small ``n`` already; larger runs add examples, not coverage. 20 is
# picked to keep 100 examples well under a second and the suite fast.
_MAX_CONSECUTIVE_RUNS = 20


# Feature: m2-security, Property 2: 口令唯一性
# Validates: Requirements 4.3
@settings(max_examples=100, deadline=None)
@given(n=st.integers(min_value=2, max_value=_MAX_CONSECUTIVE_RUNS))
def test_consecutive_increment_try_no_yields_pairwise_distinct_passwords(n):
    """``n`` consecutive ``increment_try_no`` calls yield ``n`` distinct passwords.

    Requirement 4.3 is stated for two consecutive calls; the property
    generalizes it to any ``n >= 2``, which is the form that rules out a
    generator that cycles with a period larger than two.
    """
    task = Task("task1")

    passwords = []
    for _ in range(n):
        task.increment_try_no()
        passwords.append(task.job_password)

    # Every run must actually have produced a password, otherwise distinctness
    # could be satisfied vacuously by a single ``None``.
    assert all(passwords), (
        f"{sum(1 for p in passwords if not p)} of {n} runs produced an empty "
        "job password"
    )

    # Pairwise distinctness (Requirement 4.3). Comparing set cardinality keeps
    # the values out of the failure message; only the counts are reported.
    assert len(set(passwords)) == n, (
        f"{n} consecutive runs produced only {len(set(passwords))} distinct "
        "job passwords"
    )
