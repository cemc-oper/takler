from __future__ import annotations

from typing import Union

from .node import Node, compute_most_significant_status
from .state import NodeStatus
from .task_node import Task
from .calendar import Calendar


class NodeContainer(Node):
    def __init__(self, name: str):
        super(NodeContainer, self).__init__(name)

    # State management ----------------------------------------------------

    def computed_status(self, immediate: bool) -> NodeStatus:
        if len(self.children) == 0:
            return self.state.node_status

        c_state = compute_most_significant_status(self.children, immediate)
        return c_state

    def sink_status_change_only(self, node_status: NodeStatus):
        """
        Apply the node_status change to all its descendants without doing anything.

        Sink status down. This method can only be called in set_state and itself.
        """
        self.set_node_status_only(node_status)
        for a_node in self.children:
            a_node.sink_status_change_only(node_status)

    def sink_status_change(self, node_status: NodeStatus):
        """
        Apply the node_status change to all its descendants with side effects.
        """
        # if self.state.node_status == node_status:
        #     return

        self.sink_status_change_only(node_status)
        self.handle_status_change()

    def handle_status_change(self):
        self.swim_status_change()

        super(NodeContainer, self).handle_status_change()

    # Build flow tree structure -------------------------------------------

    def add_container(self, container: Union[str, NodeContainer]) -> NodeContainer:
        """
        Add a ``NodeContainer``.
        If ``container`` is a string, a new ``NodeContainer`` will be created.

        Parameters
        ----------
        container

        Returns
        -------
        NodeContainer
        """
        if isinstance(container, str):
            container_node = NodeContainer(name=container)
        elif isinstance(container, NodeContainer):
            container_node = container
        else:
            raise TypeError("container must be str or NodeContainer")

        c = self.append_child(container_node)
        return c

    def add_task(self, task: Union[str, Task]) -> Task:
        """
        Add a ``Task``.

        Should use a real ``Task`` instead of base ``Task`` object.

        Parameters
        ----------
        task

        Returns
        -------
        Task
        """
        if isinstance(task, str):
            task_node = Task(name=task)
        elif isinstance(task, Task):
            task_node = task
        else:
            raise TypeError("task must be str or Task")

        self.append_child(task_node)
        return task_node

    # Trigger ------------------------------------------------

    def resolve_dependencies(self) -> bool:
        if not self.check_dependencies():
            return False

        for child in self.children:
            child.resolve_dependencies()

        return True

    # Time Attribute --------------------------------------------

    def calendar_changed(self, calendar: Calendar):
        super(NodeContainer, self).calendar_changed(calendar)
        for node in self.children:
            node.calendar_changed(calendar)

    # Node operation -----------------------------------------

    def requeue(self, reset_repeat: bool = True):
        """
        Requeue the node and all its child nodes.

        Call ``Node.requeue`` first and then requeue its children.

        Returns
        -------

        """
        super(NodeContainer, self).requeue(reset_repeat=reset_repeat)

        for child in self.children:
            child.requeue(reset_repeat=reset_repeat)

        if self.default_node_status == NodeStatus.complete:
            self.sink_status_change_only(NodeStatus.complete)
