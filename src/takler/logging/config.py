"""Configuration resolution for the Takler logging subsystem.

This module defines :class:`ResolvedConfig` -- the single source of truth that
the backend abstraction consumes -- and :func:`resolve_config`, which applies
the per-setting precedence rule *explicit argument > environment variable >
built-in default* (Requirements 1.1, 7.1, 7.2, 7.4, 7.5).

The logic here is pure: it performs no I/O and never mutates global logging
state. The caller (the Logging_Configurator) is responsible for acting on the
resolved configuration and for emitting any warning signalled via
:attr:`ResolvedConfig.invalid_env_level`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Union

from takler.logging.errors import InvalidLogLevelError
from takler.logging.levels import LogLevel

__all__ = [
    "ResolvedConfig",
    "resolve_config",
    "ENV_LOG_LEVEL",
    "ENV_LOG_FILE",
    "ENV_AUDIT_FILE",
    "DEFAULT_LEVEL",
    "DEFAULT_CONSOLE",
    "AUDIT_COMPONENT",
]

# Environment variable names recognized by the logging subsystem.
ENV_LOG_LEVEL = "TAKLER_LOG_LEVEL"
ENV_LOG_FILE = "TAKLER_LOG_FILE"
# Audit sink path. The name is duplicated (not imported) on purpose: the
# logging subsystem must not depend on ``takler.server``, which owns the
# server-side resolution of the same variable.
ENV_AUDIT_FILE = "TAKLER_AUDIT_FILE"

# The component name whose records are routed to the audit sink. Records
# emitted through ``get_logger("audit")`` carry this component name, and the
# backends use it to isolate the audit sink from the regular sinks in both
# directions (Requirement 11.12).
AUDIT_COMPONENT = "audit"

# Built-in defaults applied when neither an explicit argument nor an
# environment variable provides a value.
DEFAULT_LEVEL = LogLevel.INFO
DEFAULT_CONSOLE = True


@dataclass(frozen=True)
class ResolvedConfig:
    """The fully resolved logging configuration.

    Produced by :func:`resolve_config` and consumed by a backend's
    ``apply_config``. It captures the effective value of every setting after
    precedence resolution.

    Attributes:
        level: The effective :class:`LogLevel`. Defaults to ``INFO``.
        console: Whether the console sink is enabled. Defaults to ``True``.
        log_file: Path for an optional file sink, or ``None`` for no file
            sink.
        rotation: Rotation threshold (size or time interval), or ``None`` for
            no rotation. Resolved from an explicit argument only.
        retention: Retention limit (count or age) for rotated files, or
            ``None`` to keep all. Resolved from an explicit argument only.
        invalid_env_level: When the ``TAKLER_LOG_LEVEL`` environment variable
            held a non-empty but unrecognized value (and no explicit level
            argument overrode it), this carries that offending value so the
            caller can emit a WARNING and fall back to ``INFO`` (Requirement
            7.3). It is ``None`` in every other case.
        audit_file: Path for the optional audit sink, or ``None`` for no audit
            sink. When set, the backend installs a third sink that receives
            *only* the records of the :data:`AUDIT_COMPONENT` component, and
            excludes those records from the console and regular file sinks
            (Requirements 11.1, 11.12). When ``None``, no audit sink is
            installed and audit records flow to whatever sinks are configured
            (Requirement 11.13).
    """

    level: LogLevel
    console: bool
    log_file: Optional[str]
    rotation: Optional[Union[str, int]]
    retention: Optional[Union[str, int]]
    invalid_env_level: Optional[str] = None
    audit_file: Optional[str] = None


def _is_blank(value: Optional[str]) -> bool:
    """Return ``True`` when an environment value counts as "not provided".

    Empty strings and whitespace-only strings are treated as absent so that a
    blank environment variable falls back to the built-in default
    (Requirement 7.5).
    """
    return value is None or value.strip() == ""


def _resolve_level(
    explicit_level: Any,
    env: Mapping[str, str],
) -> "tuple[LogLevel, Optional[str]]":
    """Resolve the effective level and any invalid-env-level signal.

    Precedence: an explicit level argument wins and is parsed strictly (an
    unrecognized value raises :class:`InvalidLogLevelError`, per Requirement
    2.3). Otherwise the ``TAKLER_LOG_LEVEL`` environment variable applies when
    non-blank; an unrecognized environment value does not raise but resolves
    to ``INFO`` and is surfaced for a warning (Requirement 7.3). When neither
    is provided, the built-in default ``INFO`` applies.
    """
    if explicit_level is not None:
        # Explicit API argument: a bad value is a programming error and must
        # surface immediately.
        return LogLevel.parse(explicit_level), None

    env_level = env.get(ENV_LOG_LEVEL)
    if _is_blank(env_level):
        return DEFAULT_LEVEL, None

    try:
        return LogLevel.parse(env_level), None
    except InvalidLogLevelError:
        # Environment misconfiguration degrades gracefully: fall back to INFO
        # and signal the offending value so the caller can warn.
        return DEFAULT_LEVEL, env_level


def _resolve_file_path(
    explicit_path: Any,
    env: Mapping[str, str],
    env_key: str,
) -> Optional[str]:
    """Resolve the effective path of a file sink.

    Precedence: an explicit argument wins (coerced to ``str`` so
    ``os.PathLike`` values are accepted), otherwise a non-blank value of the
    ``env_key`` environment variable applies, otherwise no sink.

    Args:
        explicit_path: The explicitly supplied path, or ``None``.
        env: The environment mapping to consult.
        env_key: The environment variable name backing this setting
            (``TAKLER_LOG_FILE`` for the regular file sink,
            ``TAKLER_AUDIT_FILE`` for the audit sink).

    Returns:
        The resolved path, or ``None`` when the sink is not configured.
    """
    if explicit_path is not None:
        return os.fspath(explicit_path)

    env_path = env.get(env_key)
    if _is_blank(env_path):
        return None
    return env_path


def resolve_config(
    explicit: Mapping[str, Any],
    env: Mapping[str, str],
) -> ResolvedConfig:
    """Resolve a :class:`ResolvedConfig` from explicit args and environment.

    Applies per-setting precedence -- explicit argument > environment
    variable > built-in default -- independently for each setting, so a
    missing explicit argument still lets an environment value take effect
    (Requirements 1.1, 7.1, 7.2, 7.4, 7.5).

    Args:
        explicit: A mapping of explicitly supplied settings. Recognized keys
            are ``level``, ``console``, ``log_file``, ``rotation``,
            ``retention`` and ``audit_file``. A missing key, or a key whose
            value is ``None``, is treated as "not provided" so the next
            precedence source applies.
        env: A mapping of environment variables (typically ``os.environ``).
            Only ``TAKLER_LOG_LEVEL``, ``TAKLER_LOG_FILE`` and
            ``TAKLER_AUDIT_FILE`` are consulted.

    Returns:
        The fully resolved configuration.

    Raises:
        InvalidLogLevelError: If an explicit ``level`` argument is supplied
            but is not a recognized severity name.
    """
    level, invalid_env_level = _resolve_level(explicit.get("level"), env)

    explicit_console = explicit.get("console")
    console = DEFAULT_CONSOLE if explicit_console is None else bool(explicit_console)

    log_file = _resolve_file_path(explicit.get("log_file"), env, ENV_LOG_FILE)

    # The audit sink path follows the same per-setting precedence as the
    # regular file sink, backed by its own environment variable
    # (Requirement 11.12).
    audit_file = _resolve_file_path(explicit.get("audit_file"), env, ENV_AUDIT_FILE)

    # Rotation and retention are sourced from explicit arguments only; there
    # is no environment variable for them, and the default is "none".
    rotation = explicit.get("rotation")
    retention = explicit.get("retention")

    return ResolvedConfig(
        level=level,
        console=console,
        log_file=log_file,
        rotation=rotation,
        retention=retention,
        invalid_env_level=invalid_env_level,
        audit_file=audit_file,
    )
