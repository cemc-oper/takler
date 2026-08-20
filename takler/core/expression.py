from typing import Optional, TYPE_CHECKING
from .expression_parser import parse_trigger
from .expression_ast import AstRoot


if TYPE_CHECKING:
    from .node import Node


class Expression:
    """
    An expression for trigger in nodes.

    Attributes
    ----------
    free
        expression with free set is always True.
    ast
        expression AST parsed from expression string.
    expression_str
        original expression string.
    """

    def __init__(self, expression_str: str):
        self.free: bool = False
        self.ast: Optional[AstRoot] = None
        self.expression_str: str = expression_str

    def reset(self):
        self.clear_free()

    def set_free(self):
        self.free = True

    def clear_free(self):
        self.free = False

    def create_ast(self, parent_node: "Node"):
        """
        Create AST from expression string and set parent node for the AST.
        """
        self.parse_expression()
        self.ast.set_parent_node(parent_node)

    def evaluate(self) -> bool:
        """
        Calculate the expression result. If free flag is set, always return True.
        """
        if self.free:
            return True
        return self.ast.evaluate()

    def parse_expression(self):
        """
        Parse expression string and create an AST.
        """
        self.ast = parse_trigger(self.expression_str)
