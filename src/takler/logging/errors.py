"""Shared error and result types for the Takler logging subsystem.

These types are used across the logging package's core logic and backend
abstraction. They are defined here so that both the public API
(``takler.logging``) and the backend implementations can depend on them
without creating import cycles.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Imported lazily / only for type checking to avoid an import cycle with
    # the config module, which depends on this module.
    from takler.logging.config import ResolvedConfig


class InvalidLogLevelError(ValueError):
    """Raised when an unrecognized log level name is supplied.

    The error message identifies the offending value so callers can surface
    a helpful diagnostic. It subclasses :class:`ValueError` because an invalid
    level name is a programming error at the public API boundary.
    """

    def __init__(self, value: object) -> None:
        self.value = value
        super().__init__(f"Invalid log level: {value!r}")


@dataclass(frozen=True)
class SettingFailure:
    """A single setting that could not be applied to the active backend.

    Attributes:
        setting_name: The name of the setting that failed to apply (for
            example ``"log_file"`` or ``"level"``).
        reason: A human-readable description of why the setting failed.
    """

    setting_name: str
    reason: str


@dataclass
class ApplyResult:
    """The outcome of applying a resolved configuration to a backend.

    ``apply_config`` never raises to the caller; instead it reports any
    settings it could not apply via :attr:`failures` while retaining the
    settings it did apply in :attr:`applied`.

    Attributes:
        applied: The configuration that was successfully applied.
        failures: Zero or more :class:`SettingFailure` entries describing
            settings that could not be applied.
    """

    applied: "ResolvedConfig"
    failures: list[SettingFailure] = field(default_factory=list)
