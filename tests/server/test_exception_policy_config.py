"""Unit tests for the :class:`ExceptionPolicy` enum and its config resolution.

These example-based tests pin down the concrete behavior added by Task 3.1 of
the *server-exception-resilience* bugfix:

* :meth:`ExceptionPolicy.from_str` parses recognized policy names
  (case-insensitive, ``-``/``_`` tolerant, surrounding whitespace ignored) to
  the correct member, passes an :class:`ExceptionPolicy` through unchanged, and
  degrades an unrecognized / blank / non-string value to the default
  :attr:`ExceptionPolicy.RESILIENT` with a WARNING (Requirement 2.6).
* :func:`resolve_exception_policy` applies the config-source precedence
  ``explicit argument > TAKLER_EXCEPTION_POLICY env var > default RESILIENT``
  (Requirement 2.6).

The WARNING assertions verify a WARNING record is emitted naming the offending
value by spying on the module-level ``server.config`` logger. This keeps the
assertion independent of which logging backend/sink is active and of pytest's
stream capture.

Validates: Requirements 2.6
"""

from __future__ import annotations

from unittest import mock

import pytest

import takler.server.connect_config as connect_config
from takler.server.connect_config import (
    DEFAULT_EXCEPTION_POLICY,
    TAKLER_EXCEPTION_POLICY,
    ExceptionPolicy,
    resolve_exception_policy,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _capture_warning(call):
    """Run ``call`` while spying on the module logger's ``warning`` method.

    Returns ``(result, warning_messages)`` where ``warning_messages`` is the
    list of fully-formatted WARNING strings emitted during the call.
    """
    with mock.patch.object(connect_config.logger, "warning") as warn:
        result = call()
    # The formatted message is the first positional argument to logger.warning.
    messages = [c.args[0] if c.args else "" for c in warn.call_args_list]
    return result, messages


# Each recognized policy name paired with its expected member. Includes the
# canonical value strings plus a couple of spelling variants.
RECOGNIZED_NAMES = [
    ("RESILIENT", ExceptionPolicy.RESILIENT),
    ("resilient", ExceptionPolicy.RESILIENT),
    ("Resilient", ExceptionPolicy.RESILIENT),
    ("FAIL_FAST", ExceptionPolicy.FAIL_FAST),
    ("fail_fast", ExceptionPolicy.FAIL_FAST),
    ("fail-fast", ExceptionPolicy.FAIL_FAST),
    ("Fail-Fast", ExceptionPolicy.FAIL_FAST),
]


# ---------------------------------------------------------------------------
# ExceptionPolicy defaults
# ---------------------------------------------------------------------------


def test_default_policy_is_resilient() -> None:
    """The built-in default policy is RESILIENT (Requirement 2.6)."""
    assert DEFAULT_EXCEPTION_POLICY is ExceptionPolicy.RESILIENT


def test_env_var_constant_name() -> None:
    """The env var constant matches the documented name and TAKLER_* style."""
    assert TAKLER_EXCEPTION_POLICY == "TAKLER_EXCEPTION_POLICY"


# ---------------------------------------------------------------------------
# ExceptionPolicy.from_str
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name, expected", RECOGNIZED_NAMES)
def test_from_str_parses_recognized_names(name: str, expected: ExceptionPolicy) -> None:
    """Recognized names parse to their member regardless of case / separator."""
    assert ExceptionPolicy.from_str(name) is expected


@pytest.mark.parametrize("name, expected", RECOGNIZED_NAMES)
def test_from_str_strips_surrounding_whitespace(
    name: str, expected: ExceptionPolicy
) -> None:
    """Surrounding whitespace is ignored when parsing a recognized name."""
    assert ExceptionPolicy.from_str(f"  {name}\t") is expected


def test_from_str_passes_through_enum_value() -> None:
    """An existing ExceptionPolicy is returned unchanged for convenience."""
    assert ExceptionPolicy.from_str(ExceptionPolicy.FAIL_FAST) is ExceptionPolicy.FAIL_FAST
    assert ExceptionPolicy.from_str(ExceptionPolicy.RESILIENT) is ExceptionPolicy.RESILIENT


@pytest.mark.parametrize("bad", ["", "   ", "resilent", "failfast", "safe", "exit", "0"])
def test_from_str_unknown_value_falls_back_to_resilient(bad: str) -> None:
    """Unrecognized / blank strings degrade to RESILIENT (Requirement 2.6)."""
    result, warnings = _capture_warning(lambda: ExceptionPolicy.from_str(bad))
    assert result is ExceptionPolicy.RESILIENT
    # A WARNING naming the offending value is emitted so the misconfiguration
    # is diagnosable, but resilience is never reduced.
    assert len(warnings) == 1
    assert repr(bad) in warnings[0]


@pytest.mark.parametrize("bad", [None, 123, 4.5, object(), ["fail_fast"]])
def test_from_str_non_string_falls_back_to_resilient(bad) -> None:
    """A non-string, non-enum value degrades to RESILIENT with a WARNING."""
    result, warnings = _capture_warning(lambda: ExceptionPolicy.from_str(bad))
    assert result is ExceptionPolicy.RESILIENT
    assert len(warnings) == 1


# ---------------------------------------------------------------------------
# resolve_exception_policy -- source precedence
# ---------------------------------------------------------------------------


def test_resolve_explicit_argument_wins_over_env() -> None:
    """Explicit argument takes precedence over the env var (Requirement 2.6)."""
    env = {TAKLER_EXCEPTION_POLICY: "resilient"}
    assert (
        resolve_exception_policy(explicit="fail_fast", env=env)
        is ExceptionPolicy.FAIL_FAST
    )
    # ...and an explicit ExceptionPolicy value is honored as well.
    assert (
        resolve_exception_policy(explicit=ExceptionPolicy.FAIL_FAST, env=env)
        is ExceptionPolicy.FAIL_FAST
    )


def test_resolve_env_used_when_no_explicit() -> None:
    """With no explicit argument, the env var value applies (Requirement 2.6)."""
    env = {TAKLER_EXCEPTION_POLICY: "fail-fast"}
    assert resolve_exception_policy(explicit=None, env=env) is ExceptionPolicy.FAIL_FAST


def test_resolve_default_when_nothing_set() -> None:
    """No explicit argument and no env var yields the default RESILIENT."""
    assert resolve_exception_policy(explicit=None, env={}) is ExceptionPolicy.RESILIENT


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
def test_resolve_blank_env_treated_as_absent(blank: str) -> None:
    """A blank/whitespace-only env value falls back to the default RESILIENT."""
    env = {TAKLER_EXCEPTION_POLICY: blank}
    assert resolve_exception_policy(explicit=None, env=env) is ExceptionPolicy.RESILIENT


def test_resolve_unknown_env_falls_back_to_resilient_with_warning() -> None:
    """An unrecognized env value degrades to RESILIENT with a WARNING."""
    env = {TAKLER_EXCEPTION_POLICY: "bogus-policy"}
    result, warnings = _capture_warning(
        lambda: resolve_exception_policy(explicit=None, env=env)
    )
    assert result is ExceptionPolicy.RESILIENT
    assert len(warnings) == 1
    assert repr("bogus-policy") in warnings[0]


def test_resolve_unknown_explicit_falls_back_to_resilient_with_warning() -> None:
    """An unrecognized explicit value degrades to RESILIENT with a WARNING."""
    result, warnings = _capture_warning(
        lambda: resolve_exception_policy(explicit="nonsense", env={})
    )
    assert result is ExceptionPolicy.RESILIENT
    assert len(warnings) == 1
    assert repr("nonsense") in warnings[0]
