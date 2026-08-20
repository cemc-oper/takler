"""Right-hand tab views for the takler TUI."""

from __future__ import annotations

from .info import InfoTab
from .job import JobTab
from .output import OutputTab
from .parameters import ParametersTab
from .script import ScriptTab


__all__ = ["InfoTab", "JobTab", "OutputTab", "ParametersTab", "ScriptTab"]
