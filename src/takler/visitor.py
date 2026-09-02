from typing import IO, Optional
from abc import ABC, abstractmethod

from takler.core.node import Node


class NodeVisitor(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def visit(self, node: Node):
        pass

    def before_visit_child(self):
        pass

    def after_visit_child(self):
        pass


class SimplePrintVisitor(NodeVisitor):
    """
    Print node tree with state.
    """

    def __init__(self):
        NodeVisitor.__init__(self)
        self.level = 0

    def visit(self, node: Node):
        place_holder = "  " * self.level
        node_name = node.name
        node_state = node.state.node_status.name
        print(f"{place_holder}|- {node_name} [{node_state}]")

    def before_visit_child(self):
        self.level += 1

    def after_visit_child(self):
        self.level -= 1


class PrintVisitor(NodeVisitor):
    """
    Print node tree with attributes. Use ``show_*`` options to choose printed attrs.

    Attributes
    ----------
    show_parameter
        print parameters
    show_trigger
        print trigger expressing
    show_limit
        print limits
    show_event
        print events
    show_meter
        print meters
    show_repeat
        print repeat
    """

    def __init__(
        self,
        stream: IO,
        show_parameter: bool = False,
        show_user_parameter: Optional[bool] = None,
        show_generated_parameter: Optional[bool] = None,
        show_trigger: bool = False,
        show_limit: bool = True,
        show_event: bool = True,
        show_meter: bool = True,
        show_repeat: bool = True,
    ):
        NodeVisitor.__init__(self)
        self.level: int = 0
        self.stream: IO = stream

        self.show_parameter = show_parameter
        if show_user_parameter is None:
            self.show_user_parameter = self.show_parameter
        else:
            self.show_user_parameter = show_user_parameter
        if show_generated_parameter is None:
            self.show_generated_parameter = self.show_parameter
        else:
            self.show_generated_parameter = show_generated_parameter
        self.show_trigger = show_trigger
        self.show_limit = show_limit
        self.show_event = show_event
        self.show_meter = show_meter
        self.show_repeat = show_repeat

    def visit(self, node: Node):
        place_holder = "  " * self.level
        node_name = node.name
        node_state = node.state.node_status.name
        if node.is_suspended():
            node_state = f"suspend ({node_state})"
        self.stream.write(f"{place_holder}|- {node_name} [{node_state}]\n")
        pre_spaces = " " * len(f"{place_holder}|- ")

        if self.show_repeat and node.repeat is not None:
            self.stream.write(
                f"{pre_spaces} repeat {node.repeat.r.name} {node.repeat.value()} [{node.repeat.start()}, {node.repeat.end()}]\n"
            )

        if self.show_trigger and node.trigger_expression is not None:
            self.stream.write(
                f"{pre_spaces} trigger {node.trigger_expression.expression_str}\n"
            )

        if self.show_trigger:
            for time_attr in node.times:
                self.stream.write(
                    f"{pre_spaces} time {time_attr.time.hour:02}:{time_attr.time.minute:02}\n"
                )

        if self.show_user_parameter:
            user_params = node.user_parameters_only()
            for name, param in user_params.items():
                self.stream.write(f"{pre_spaces} param {name} '{param.value}'\n")

        if self.show_generated_parameter:
            generate_params = node.generated_parameters_only()
            for name, param in generate_params.items():
                self.stream.write(f"{pre_spaces} # param {name} '{param.value}'\n")

        if self.show_limit and len(node.limits) > 0:
            for limit in node.limits:
                self.stream.write(
                    f"{pre_spaces} limit {limit.name} [{limit.value}/{limit.limit}]\n"
                )

        if self.show_event and len(node.events) > 0:
            for event in node.events:
                v = "set" if event.value else "unset"
                self.stream.write(f"{pre_spaces} event {event.name} [{v}]\n")

        if self.show_meter and len(node.meters) > 0:
            for meter in node.meters:
                self.stream.write(
                    f"{pre_spaces} meter {meter.name} {meter.min_value} {meter.max_value} [{meter.value}]\n"
                )

    def before_visit_child(self):
        self.level += 1

    def after_visit_child(self):
        self.level -= 1


def pre_order_travel(root_node: Node, visitor: NodeVisitor):
    visitor.visit(root_node)
    for child_node in root_node.children:
        visitor.before_visit_child()
        pre_order_travel(child_node, visitor)
        visitor.after_visit_child()
