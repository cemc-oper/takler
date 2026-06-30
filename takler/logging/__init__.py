"""Takler logging subsystem.

This package replaces the former single-function ``takler/logging.py`` module
while preserving its public import surface. The existing call sites obtain a
logger via :func:`get_logger`; that behavior is kept intact for backward
compatibility (Requirement 8.1).

A centralized :func:`configure` entry point (the Logging_Configurator) is
exposed here as well. It resolves the effective configuration (explicit
arguments over environment variables over built-in defaults) and applies it to
the active backend, tearing down any previously installed Takler sinks first so
reconfiguration never accumulates duplicate destinations (Requirements 1.1,
1.3, 1.4). When no :func:`configure` call has happened before the first record
is emitted, a default INFO-to-console configuration is applied automatically
(Requirement 1.5).
"""

from __future__ import annotations

import os
import threading
from typing import TYPE_CHECKING, Optional, Union

from takler.logging.backends import get_backend
from takler.logging.config import ENV_LOG_LEVEL, ResolvedConfig, resolve_config
from takler.logging.errors import (
    ApplyResult,
    InvalidLogLevelError,
    SettingFailure,
)

if TYPE_CHECKING:
    from takler.logging.backends import NamedLogger

__all__ = [
    "get_logger",
    "configure",
    "InvalidLogLevelError",
    "SettingFailure",
    "ApplyResult",
]

# The root Takler component name. Absent, ``None``, empty, or whitespace-only
# names normalize to this so every record is attributed to a component
# (Requirement 6.3).
ROOT_COMPONENT = "takler"

# Module-level state tracking whether the Logging_Configurator has run in this
# process. It starts ``False`` so that the first record emitted without an
# explicit :func:`configure` call triggers the default INFO-to-console
# configuration (Requirement 1.5). A successful :func:`configure` call sets it
# so the default is never re-applied over an explicit configuration.
_configured = False

# Guards the "configure once" transition so concurrent first-emitters do not
# race. ``apply_config`` is itself idempotent, so this is belt-and-suspenders.
_configure_lock = threading.Lock()


def get_logger(name: Optional[str] = None) -> "NamedLogger":
    """Return a component-attributed logger for a Takler component.

    The returned object is the active backend's ``NamedLogger`` adapter, whose
    ``trace``/``debug``/``info``/``warning``/``error``/``critical`` methods
    forward to the active backend with the bound component name. This gives a
    uniform method surface regardless of which backend is active and guarantees
    the component name is attributed consistently (Requirements 6.1, 6.2, 8.3).

    Backward compatible: accepts zero or one positional argument, so existing
    no-argument and one-argument call sites keep working unchanged
    (Requirement 8.1). Calling a logging method below the configured level is
    suppressed by the backend and returns control without raising
    (Requirement 8.4).

    Name normalization (Requirement 6.3): a missing argument, ``None``, an
    empty string, or a whitespace-only string all normalize to the root
    component name ``"takler"``. A non-empty name of at most 256 characters is
    used exactly as given (Requirement 6.1); longer names are still accepted.

    Args:
        name: Optional component name (for example ``"server.scheduler"``). A
            non-string, non-``None`` value raises :class:`TypeError`
            (Requirement 6.5).

    Returns:
        A :class:`~takler.logging.backends.NamedLogger` attributed to the
        resolved component name.

    Raises:
        TypeError: If ``name`` is neither ``None`` nor a string. The error
            identifies the offending ``name`` argument and no logger is
            returned (Requirement 6.5).
    """
    if name is not None and not isinstance(name, str):
        raise TypeError(
            f"get_logger() argument 'name' must be a string or None, "
            f"not {type(name).__name__}"
        )

    if name is None or not name.strip():
        component = ROOT_COMPONENT
    else:
        component = name

    # Ensure a default INFO-to-console configuration is in place before the
    # first record can be emitted through the returned adapter, when no
    # explicit ``configure`` call has occurred yet (Requirement 1.5).
    _ensure_configured()

    return get_backend().get_named_logger(component)


def configure(
    level: Optional[str] = None,
    log_file: Optional[Union[str, "os.PathLike[str]"]] = None,
    console: Optional[bool] = None,
    rotation: Optional[Union[str, int]] = None,
    retention: Optional[Union[str, int]] = None,
) -> None:
    """Configure the active logging backend (Logging_Configurator).

    Resolves the effective configuration applying per-setting precedence --
    explicit argument > environment variable > built-in default -- and applies
    it to the active backend. Every previously installed Takler sink is torn
    down before the new set is installed (the backend's ``apply_config`` is
    idempotent), so reconfiguration never accumulates duplicate destinations
    and the most recent invocation governs every record emitted afterward
    (Requirements 1.1, 1.2, 1.3, 1.4).

    Only the arguments the caller actually supplies (non-``None``) are treated
    as explicit; an omitted argument lets an environment variable -- or, in its
    absence, the built-in default -- take effect for that setting
    (Requirement 7.4). Console output can be toggled independently of the file
    sink: disabling the console leaves any configured file sink active
    (Requirement 4.2).

    An invalid explicit ``level`` is a programming error at the API boundary:
    :func:`resolve_config` raises :class:`InvalidLogLevelError` *before* any
    sink is torn down, so the previously active configuration is left unchanged
    (Requirement 2.3). When the ``TAKLER_LOG_LEVEL`` environment variable holds
    an invalid value and no explicit ``level`` overrides it, configuration
    proceeds with a fall-back to INFO and a WARNING naming the offending value
    is emitted through the console sink after it is installed (Requirement 7.3).

    Args:
        level: Target log level name (case-insensitive). An unrecognized value
            raises :class:`InvalidLogLevelError`.
        log_file: Path for an optional rotating file sink.
        console: Whether to emit records to the console sink. ``None`` leaves
            the default (console enabled) in effect.
        rotation: Rotation threshold (size or time interval).
        retention: Retention limit (count or age) for rotated files.

    Raises:
        InvalidLogLevelError: If ``level`` is supplied but is not a recognized
            severity name. The previously active configuration is unchanged.
    """
    global _configured

    # Build the explicit settings map from only the arguments the caller
    # actually provided, so omitted settings fall through to env/defaults
    # (Requirement 7.4).
    explicit: dict = {}
    if level is not None:
        explicit["level"] = level
    if log_file is not None:
        explicit["log_file"] = log_file
    if console is not None:
        explicit["console"] = console
    if rotation is not None:
        explicit["rotation"] = rotation
    if retention is not None:
        explicit["retention"] = retention

    # Resolve first. An invalid explicit level raises here, BEFORE any sink is
    # torn down, leaving the previously active configuration intact
    # (Requirement 2.3).
    resolved = resolve_config(explicit, os.environ)

    # Apply the resolved configuration. ``apply_config`` removes the sinks this
    # subsystem installed previously before installing the new set, so there
    # are never duplicate destinations (Requirements 1.3, 1.4).
    get_backend().apply_config(resolved)

    # Mark the subsystem configured so the lazy default is never applied over
    # this explicit configuration. Set before emitting any warning so the
    # warning path's own ``get_logger`` call does not re-enter default setup.
    _configured = True

    _warn_on_invalid_env_level(resolved)


def _ensure_configured() -> None:
    """Apply the default configuration once if ``configure`` has not run.

    This realizes "apply a default INFO-to-console configuration when no
    ``configure`` call has occurred before the first record" (Requirement 1.5).
    It is invoked from :func:`get_logger` so the default sink is installed
    before the first record can be emitted through the returned adapter. It is
    a no-op once any configuration (default or explicit) has been applied,
    keeping it cheap on the hot path and avoiding duplicate sinks.
    """
    global _configured
    if _configured:
        return

    with _configure_lock:
        if _configured:
            return

        # Default configuration: explicit args empty, so env vars (if any) and
        # built-in defaults (INFO, console on, no file sink) apply.
        resolved = resolve_config({}, os.environ)
        get_backend().apply_config(resolved)
        _configured = True

        _warn_on_invalid_env_level(resolved)


def _warn_on_invalid_env_level(resolved: ResolvedConfig) -> None:
    """Emit a console WARNING when the env log level was invalid.

    When ``TAKLER_LOG_LEVEL`` held a non-empty but unrecognized value (and no
    explicit level overrode it), :func:`resolve_config` falls back to INFO and
    records the offending value on :attr:`ResolvedConfig.invalid_env_level`.
    This emits a WARNING naming that value through the console sink, which by
    now exists because the configuration has already been applied
    (Requirement 7.3). The message is fully formatted before being passed to
    the logger so it renders identically on every backend.
    """
    if resolved.invalid_env_level is None:
        return

    get_logger(ROOT_COMPONENT).warning(
        f"Invalid {ENV_LOG_LEVEL} value {resolved.invalid_env_level!r}; "
        f"falling back to INFO."
    )


def _reset_configured_state() -> None:
    """Reset the module-level "configured" flag.

    Intended for tests that need to re-exercise default-configuration behavior
    within a single process. Production code configures once and leaves the
    flag set for the process lifetime.
    """
    global _configured
    _configured = False
