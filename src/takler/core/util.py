from typing import TYPE_CHECKING
from enum import Enum

from takler.logging import get_logger

if TYPE_CHECKING:
    from logging import Logger


logger: "Logger" = get_logger("core")


class SerializationType(Enum):
    """Serialization mode of ``to_dict`` / ``from_dict``.

    The two modes decide whether a serialized node carries its current
    runtime state. See the serialization section of the core design
    document for the full mechanism.
    """

    Tree = "tree"
    """Definition only; runtime state is reset on load, the flow is un-begun."""

    Status = "status"
    """Definition plus current runtime state; used by checkpoint snapshots."""
