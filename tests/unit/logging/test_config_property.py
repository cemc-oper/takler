"""Property-based tests for configuration precedence resolution.

Covers Property 9 from the logging-enhancement design: the per-setting
precedence rule *explicit argument > environment variable > built-in default*
implemented by :func:`takler.logging.config.resolve_config`.
"""

from __future__ import annotations

import os

from hypothesis import given, settings
from hypothesis import strategies as st

from takler.logging.config import (
    DEFAULT_CONSOLE,
    DEFAULT_LEVEL,
    ENV_LOG_FILE,
    ENV_LOG_LEVEL,
    resolve_config,
)
from takler.logging.levels import LogLevel

# The recognized canonical severity names.
LEVEL_NAMES = ["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# Values that must be treated as "not provided" for an environment variable
# (Requirement 7.5).
BLANK_VALUES = ["", " ", "  ", "\t", "\n", " \t ", "\r\n"]


@st.composite
def case_permuted_level_names(draw: st.DrawFn) -> str:
    """Draw a recognized severity name with a random letter-case permutation."""
    name = draw(st.sampled_from(LEVEL_NAMES))
    flips = draw(st.lists(st.booleans(), min_size=len(name), max_size=len(name)))
    return "".join(ch.upper() if flip else ch.lower() for ch, flip in zip(name, flips))


# Non-blank, path-like strings for log-file values.
path_strategy = st.text(
    alphabet="abcdefghijABCDEFGHIJ0123456789/_.-",
    min_size=1,
    max_size=24,
).filter(lambda s: s.strip() != "")


@st.composite
def explicit_args(draw: st.DrawFn) -> dict:
    """Draw a random subset of explicit configuration arguments.

    Each setting is independently either omitted (so the next precedence
    source applies) or present with a value. Only VALID explicit level names
    are generated so the precedence rule is exercised without the
    invalid-explicit raise path (which is covered by Property 2 elsewhere).
    """
    explicit: dict = {}
    if draw(st.booleans()):
        explicit["level"] = draw(case_permuted_level_names())
    if draw(st.booleans()):
        explicit["console"] = draw(st.booleans())
    if draw(st.booleans()):
        explicit["log_file"] = draw(path_strategy)
    if draw(st.booleans()):
        explicit["rotation"] = draw(
            st.one_of(st.integers(min_value=1, max_value=10_000), path_strategy)
        )
    if draw(st.booleans()):
        explicit["retention"] = draw(
            st.one_of(st.integers(min_value=1, max_value=100), path_strategy)
        )
    return explicit


@st.composite
def env_maps(draw: st.DrawFn) -> dict:
    """Draw a random environment mapping for the recognized variables.

    ``TAKLER_LOG_LEVEL`` is either absent, a valid (case-permuted) severity
    name, or a blank/whitespace value. ``TAKLER_LOG_FILE`` is either absent, a
    non-blank path, or a blank/whitespace value. Blank values must be treated
    as not provided (Requirement 7.5).
    """
    env: dict = {}
    level_choice = draw(
        st.one_of(
            st.none(),
            case_permuted_level_names(),
            st.sampled_from(BLANK_VALUES),
        )
    )
    if level_choice is not None:
        env[ENV_LOG_LEVEL] = level_choice

    file_choice = draw(
        st.one_of(
            st.none(),
            path_strategy,
            st.sampled_from(BLANK_VALUES),
        )
    )
    if file_choice is not None:
        env[ENV_LOG_FILE] = file_choice
    return env


def _blank(value):
    return value is None or value.strip() == ""


# Feature: logging-enhancement, Property 9: Configuration precedence is explicit-over-environment-over-default
# Validates: Requirements 7.1, 7.2, 7.4, 7.5
@settings(max_examples=200)
@given(explicit=explicit_args(), env=env_maps())
def test_configuration_precedence_explicit_over_env_over_default(explicit, env):
    """resolve_config picks explicit-if-present else env-if-present else default.

    For each setting, the resolved value must equal the explicit argument when
    present, otherwise the (non-blank) environment value when present,
    otherwise the built-in default. Precedence is applied per-setting, so a
    missing explicit argument still lets the environment value take effect.
    """
    resolved = resolve_config(explicit, env)

    # --- level: explicit > TAKLER_LOG_LEVEL (non-blank) > INFO ---
    if "level" in explicit:
        expected_level = LogLevel.parse(explicit["level"])
    elif not _blank(env.get(ENV_LOG_LEVEL)):
        expected_level = LogLevel.parse(env[ENV_LOG_LEVEL])
    else:
        expected_level = DEFAULT_LEVEL
    assert resolved.level == expected_level
    # Only valid env levels are generated here, so no invalid-env signal.
    assert resolved.invalid_env_level is None

    # --- console: explicit > built-in default True (no env source) ---
    expected_console = (
        DEFAULT_CONSOLE if explicit.get("console") is None else explicit["console"]
    )
    assert resolved.console == expected_console

    # --- log_file: explicit > TAKLER_LOG_FILE (non-blank) > None ---
    if "log_file" in explicit:
        expected_log_file = os.fspath(explicit["log_file"])
    elif not _blank(env.get(ENV_LOG_FILE)):
        expected_log_file = env[ENV_LOG_FILE]
    else:
        expected_log_file = None
    assert resolved.log_file == expected_log_file

    # --- rotation / retention: explicit only > None ---
    assert resolved.rotation == explicit.get("rotation")
    assert resolved.retention == explicit.get("retention")
