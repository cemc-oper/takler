import io

from takler.core.node import Node
from takler.visitor import pre_order_travel, PrintVisitor


def get_node_tree_print_string(node: Node) -> str:
    """
    Get output string of PrintVisitor class for node tree.

    Parameters
    ----------
    node
        Any ``Node`` based object.

    Returns
    -------
    str
    """
    node_io = io.StringIO()
    pre_order_travel(
        node,
        PrintVisitor(
            node_io,
            show_trigger=True,
            show_event=True,
            show_limit=True,
            show_meter=True,
            show_repeat=True,
            show_parameter=True,
        ),
    )
    node_text = node_io.getvalue()
    return node_text
