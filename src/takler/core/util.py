from typing import TYPE_CHECKING
from enum import Enum

from takler.logging import get_logger

if TYPE_CHECKING:
    from logging import Logger


logger: "Logger" = get_logger("core")


class SerializationType(Enum):
    Tree = "tree"
    Status = "status"
