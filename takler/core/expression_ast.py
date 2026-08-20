from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING, TypeVar

from .state import NodeStatus
from .event import Event
from .meter import Meter
from .parameter import Parameter
from takler.exceptions import ExpressionSyntaxError, NodeNotFoundError
from takler.logging import get_logger

if TYPE_CHECKING:
    from .node import Node


logger = get_logger(__name__)
T = TypeVar("T")


@dataclass
class AstBase:
    def set_parent_node(self, node: "Node"):
        ...

    def value(self) -> T:
        raise NotImplementedError("AstBase.value is not implemented")

    def evaluate(self) -> bool:
        raise NotImplementedError("AstBase.evaluate is not implemented")


@dataclass
class AstRoot(AstBase):
    left: Optional[AstBase] = None
    right: Optional[AstBase] = None

    def set_parent_node(self, node: "Node"):
        self.left.set_parent_node(node)
        self.right.set_parent_node(node)


@dataclass
class AstOpEq(AstRoot):
    def evaluate(self) -> bool:
        return self.left.value() == self.right.value()


@dataclass
class AstOpGt(AstRoot):
    def evaluate(self) -> bool:
        return self.left.value() > self.right.value()


@dataclass
class AstOpGe(AstRoot):
    def evaluate(self) -> bool:
        return self.left.value() >= self.right.value()


@dataclass
class AstOpLt(AstRoot):
    def evaluate(self) -> bool:
        return self.left.value() < self.right.value()


@dataclass
class AstOpLe(AstRoot):
    def evaluate(self) -> bool:
        return self.left.value() <= self.right.value()


@dataclass
class AstOpAnd(AstRoot):
    def evaluate(self) -> bool:
        return self.left.evaluate() and self.right.evaluate()


@dataclass
class AstOpOr(AstRoot):
    def evaluate(self) -> bool:
        return self.left.evaluate() or self.right.evaluate()

@dataclass
class AstMathAdd(AstRoot):
    def value(self) -> bool:
        return self.left.value() + self.right.value()

@dataclass
class AstNodePath(AstBase):
    node_path: str
    parent_node: "Optional[Node]" = None
    _reference_node: "Optional[Node]" = None

    def set_parent_node(self, node: "Node"):
        self.parent_node = node
        ref_node = self.get_reference_node()
        if ref_node is None:
            raise NodeNotFoundError(
                f"node path '{self.node_path}' is not found from node '{self.parent_node.node_path}'",
                node_path=self.node_path,
            )

    def value(self) -> NodeStatus:
        ref_node = self.get_reference_node()
        if ref_node is not None:
            return ref_node.state.node_status
        else:
            return NodeStatus.unknown

    def get_reference_node(self) -> "Optional[Node]":
        """
        Find node only once.
        """
        if self._reference_node is not None:
            return self._reference_node

        if self.parent_node is not None:
            self._reference_node = self.parent_node.find_node(self.node_path)
            return self._reference_node
        return None


@dataclass
class AstVariablePath(AstBase):
    node: AstNodePath
    variable_name: str
    _node_variable: Optional = None

    def set_parent_node(self, node: "Node"):
        self.node.set_parent_node(node)

        node_variable = self.get_variable()
        if node_variable is None:
            raise ExpressionSyntaxError(
                f"variable path '{self.node.node_path}:{self.variable_name}' is not found"
            )

    def value(self) -> Optional[int]:
        v = self.get_variable()
        if v is None:
            return None

        if isinstance(v, Event):
            if v.value:
                return 1
            else:
                return 0
        if isinstance(v, Meter):
            return v.value
        elif isinstance(v, Parameter):
            return v.value
        else:
            raise ExpressionSyntaxError(
                f"variable '{self.node.node_path}:{self.variable_name}' has unsupported type: {type(v).__name__}"
            )

    def get_variable(self) -> Optional[Event]:
        # NOTE: delete following codes to retrieve variable when trigger is evaluated,
        # because variable's location and even itself may be changed during workflow running.

        # if self._node_variable is not None:
        #     return self._node_variable

        self._node_variable = self.node.get_reference_node().find_variable(self.variable_name)
        return self._node_variable


@dataclass
class AstInteger(AstBase):
    number: int

    def value(self) -> int:
        return self.number


@dataclass
class AstNodeStatus(AstBase):
    node_status: NodeStatus

    def value(self) -> NodeStatus:
        return self.node_status
