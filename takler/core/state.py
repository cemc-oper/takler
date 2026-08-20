from __future__ import annotations

from enum import Enum
from typing import Optional, Dict

from .util import SerializationType


class OrderedEnum(Enum):
    """
    An ordered Enum class from Python Documentation.

    See Also
    ---------
    https://docs.python.org/3/library/enum.html#orderedenum
    """

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class NodeStatus(OrderedEnum):
    """
    The running status of a node.

    The status of a family is the largest status of all children nodes' status.

    Current type of status:

    * ``unknown``: if flow is not begin, all nodes is in ``unknown``
    * ``complete``: task is finished
    * ``queued``: task is waiting for its dependencies to be satisfied.
    * ``submitted``: task is submitted to its runner, but not running.
    * ``active``: task is running.
    * ``aborted``: task exists with some error.
    """

    unknown = 1
    complete = 2
    queued = 3
    submitted = 4
    active = 5
    aborted = 6


class State:
    """
    The state of a Node, include node status and other attributes.

    Attributes
    ----------
    node_status : NodeStatus
        status of a node.
    suspended : bool
        whether node is suspended from resolving.
    """

    def __init__(self, node_status: Optional[NodeStatus] = None):
        if node_status is None:
            node_status = NodeStatus.unknown
        self.node_status: NodeStatus = node_status
        self.suspended: bool = False

    # Serialization --------------------------------------------------------------------

    def to_dict(self, method: SerializationType = SerializationType.Status) -> Dict:
        result = dict(status=self.node_status.value, suspended=self.suspended)
        return result

    @classmethod
    def from_dict(
        cls, d: Dict, method: SerializationType = SerializationType.Status
    ) -> "State":
        if method == SerializationType.Status:
            status = d["status"]
            suspended = d["suspended"]
            state = State(node_status=NodeStatus(status))
            state.suspended = suspended
        else:
            state = State()
        return state
