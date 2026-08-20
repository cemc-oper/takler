from __future__ import annotations

import datetime
import importlib
from typing import Union, List, Optional, Dict, TYPE_CHECKING, Set, Literal
from pathlib import PurePosixPath
from collections import defaultdict
from abc import ABC

from takler.exceptions import NodeNotFoundError, UnsupportedValueError

from .state import State, NodeStatus
from .parameter import Parameter
from .event import Event
from .meter import Meter
from .limit import Limit, InLimit, InLimitManager
from .expression import Expression
from .repeat import Repeat, RepeatBase
from .time_attr import TimeAttribute

from .util import SerializationType

if TYPE_CHECKING:
    from .bunch import Bunch
    from .calendar import Calendar
    from .flow import Flow


# def compute_node_status(node: Node, immediate: bool) -> NodeStatus:
#     """
#     Compute node_status of a node from its children. Won't change anything in ``node``.
#
#     If ``immediate`` is True, use children's node_status.
#     If ``immediate`` is False, compute each child's node_status
#
#     Parameters
#     ----------
#     node
#     immediate
#
#     Returns
#     -------
#     NodeStatus
#     """
#     if len(node.children) == 0:
#         return node.state.node_status
#
#     state = compute_most_significant_state(node.children, immediate)
#
#     return state


def compute_most_significant_status(nodes: List[Node], immediate: bool) -> NodeStatus:
    """
    Compute the most significant node status from node list. Won't change anything in ``node``.

    If ``immediate`` is True, use children's node_status.
    If ``immediate`` is False, compute each child's node_status

    Parameters
    ----------
    nodes
    immediate

    Returns
    -------
    NodeStatus
    """
    count = defaultdict(int)
    for node in nodes:
        if immediate:
            child_node_state = node.state.node_status
        else:
            child_node_state = node.computed_status(immediate)
        count[child_node_state] += 1

    for status in (
        NodeStatus.aborted,
        NodeStatus.active,
        NodeStatus.submitted,
        NodeStatus.queued,
        NodeStatus.complete,
    ):
        if count[status] > 0:
            return status

    return NodeStatus.unknown


class Node(ABC):
    """
    Node

    Attributes
    ----------
    name
    state
    parent
    children
    user_parameters
    trigger_expression
        触发器表达式，一个节点只有一个触发器表达式。
        触发器在添加时默认不解析。在评估触发器时，会自动解析没有被解析的触发器，触发器只会被解析一次。
    events
    meters
    limits
    in_limit_manager
    repeat
    times
    """
    def __init__(self, name: str):
        self.name: str = name

        # 状态
        self.state: State = State()

        # 默认状态，requeue 时节点会重置为该状态
        self.default_node_status: Literal[NodeStatus.queued, NodeStatus.complete] = NodeStatus.queued

        # 树形结构
        self.parent: Optional["Node"] = None
        self.children: List["Node"] = list()

        # 参数
        self.user_parameters: Dict[str, Parameter] = dict()

        # 触发器
        self.trigger_expression: Optional[Expression] = None
        self.complete_trigger_expression: Optional[Expression] = None
        self.is_complete_triggered: bool = False

        # 事件
        self.events: List[Event] = list()

        # 标尺
        self.meters: List[Meter] = list()

        # 限制
        self.limits: List[Limit] = list()
        self.in_limit_manager: InLimitManager = InLimitManager(self)

        # 重复
        self.repeat: Optional[Repeat] = None

        # Time attributes
        self.times: List[TimeAttribute] = list()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
        return f"{self.__class__.__name__} {self.name}"

    def __str__(self):
        return f"{self.__class__.__name__} {self.name}"

    # Serialization -----------------------------------------------------

    def to_dict(self) -> Dict:
        result = dict(
            name=self.name,
            state=self.state.to_dict(),
            class_type=dict(
                module=self.__module__,
                name=self.__class__.__name__
            )
        )
        if self.default_node_status != NodeStatus.queued:
            result["default_node_status"] = self.default_node_status.value
        if len(self.children) != 0:
            result["children"] = [child.to_dict() for child in self.children]
        if len(self.user_parameters) != 0:
            result["user_parameters"] = [param.to_dict() for key, param in self.user_parameters.items()]
        if self.trigger_expression is not None:
            result["trigger"] = self.trigger_expression.expression_str
        if self.complete_trigger_expression is not None:
            result['complete_trigger'] = self.complete_trigger_expression.expression_str
        if self.is_complete_triggered:
            result["is_complete_triggered"] = self.is_complete_triggered
        if len(self.events) != 0:
            result["events"] = [event.to_dict() for event in self.events]
        if len(self.meters) != 0:
            result["meters"] = [meter.to_dict() for meter in self.meters]
        if len(self.limits) !=0:
            result["limits"] = [limit.to_dict() for limit in self.limits]
        if len(self.in_limit_manager.in_limit_list) != 0:
            result["in_limit_manager"] = self.in_limit_manager.to_dict()
        if self.repeat is not None:
            result["repeat"] = self.repeat.to_dict()
        if len(self.times) != 0:
            result["times"] = [time_attr.to_dict() for time_attr in self.times]

        return result

    @classmethod
    def from_dict(cls, d: Dict,  method: SerializationType = SerializationType.Status) -> "Node":
        """
        Create ``Node`` based object from dictionary. Use ``d["class_type"]`` to determine which class is to be created.

        Parameters
        ----------
        d
        method

        Returns
        -------
        Node
            A ``Node`` based object created from dictionary.
        """
        class_type = d["class_type"]
        class_module = class_type["module"]
        class_name = class_type["name"]
        class_module = importlib.import_module(class_module)
        class_object = getattr(class_module, class_name)
        node = class_object(name=d["name"])
        node = class_object.fill_from_dict(d, node, method=method)
        return node

    @classmethod
    def fill_from_dict(cls, d: Dict, node: "Node", method: SerializationType = SerializationType.Status) -> "Node":
        """
        Fill a ``Node`` based object from dictionary.

        Subclasses of ``Node`` should override this method and call ``Node.file_from_dict`` in the override method.

        Parameters
        ----------
        d
        node
        method

        Returns
        -------
        Node
        """
        name = d["name"]
        node.name = name
        if method == SerializationType.Status:
            state = d["state"]
            node.state = State.from_dict(state, method=method)

        if "default_node_status" in d:
            node.default_node_status = NodeStatus(d["default_node_status"])

        if "user_parameters" in d:
            user_parameters = d["user_parameters"]
            for param in user_parameters:
                node.add_parameter(param["name"], param["value"])

        if "trigger" in d:
            trigger = d["trigger"]
            node.add_trigger(trigger, parse=False)

        if "complete_trigger" in d:
            trigger = d["complete_trigger"]
            node.add_complete_trigger(trigger, parse=False)

        if method == SerializationType.Status:
            # the complete trigger latch is a runtime state, and it is not recomputed after restoring.
            node.is_complete_triggered = d.get("is_complete_triggered", False)

        if "events" in d:
            events = d["events"]
            for event in events:
                node.events.append(Event.from_dict(event, method=method))
        if "meters" in d:
            meters = d["meters"]
            for meter in meters:
                node.meters.append(Meter.from_dict(meter, method=method))

        if "limits" in d:
            limits = d["limits"]
            for limit in limits:
                node.add_limit_object(Limit.from_dict(limit, method=method))

        if "in_limit_manager" in d:
            in_limit_manager = d["in_limit_manager"]
            InLimitManager.fill_from_dict(in_limit_manager, node=node, method=method)

        if "repeat" in d:
            repeat = d["repeat"]
            node.repeat = Repeat.from_dict(repeat, method=method)

        if "times" in d:
            times = d["times"]
            for time_attr in times:
                node.add_time_attribute(TimeAttribute.from_dict(time_attr, method=method))

        if "children" in d:
            for child in d["children"]:
                child_node = cls.from_dict(child, method=method)
                node.append_child(child_node)

        return node

    # Children operation ------------------------------------------------
    #   These methods are for inner usage, and should not be used by Users.

    def append_child(self, child: Union[str, Node]) -> Node:
        if isinstance(child, str):
            child_node = Node(child)
        elif isinstance(child, Node):
            child_node = child
        else:
            raise TypeError("child must be a Node or string")

        child_node.parent = self
        self.children.append(child_node)
        return child_node

    def find_child_index(self, child: Union[str, Node]) -> int:
        if isinstance(child, Node):
            child_name = child.name
        elif isinstance(child, str):
            child_name = child
        else:
            raise TypeError("child must be a Node or a string.")

        for child_index in range(0, len(self.children)):
            if self.children[child_index].name == child_name:
                return child_index
        return -1

    def update_child(self, child: Union[str, Node], new_child_node: Node) -> Node:
        if not isinstance(new_child_node, Node):
            raise TypeError("new_child_node must be a Node")

        child_index = self.find_child_index(child)
        if child_index == -1:
            raise NodeNotFoundError(
                f"child {child} is not found",
                node_path=f"{self.node_path}/{child if isinstance(child, str) else child.name}",
            )

        old_child = self.children[child_index]
        new_child_node.parent = self
        self.children[child_index] = new_child_node
        return old_child

    def delete_child(self, child: Union[str, Node]) -> Node:
        child_node_index = self.find_child_index(child)
        if child_node_index == -1:
            raise NodeNotFoundError(
                f"{child} does not exist",
                node_path=f"{self.node_path}/{child if isinstance(child, str) else child.name}",
            )
        child_node = self.children.pop(child_node_index)
        child_node.delete_children()
        return child_node


    def delete_children(self):
        while len(self.children) > 0:
            node = self.children.pop(0)
            node.delete_children()
            del node
        self.children = list()

    # Node access -----------------------------------------------------

    @property
    def node_path(self) -> str:
        """
        str: full path from root node, starts with "/" and split each level with "/"
        """
        cur_node = self
        node_list = []
        while cur_node is not None:
            node_list.insert(0, cur_node.name)
            cur_node = cur_node.parent
        return str(PurePosixPath("/", *node_list))

    def is_leaf_node(self) -> bool:
        if len(self.children) == 0:
            return True
        else:
            return False

    def get_root(self) -> Node:
        """
        get root node which is usually a ``Flow``

        Returns
        -------
        Node
        """
        root = self
        while root.parent is not None:
            root = root.parent
        return root

    def get_bunch(self) -> "Optional[Bunch]":
        """
        get ``Bunch`` node if node's root is in some bunch.

        Returns
        -------
        Bunch or None
        """
        if self.parent is not None:
            return self.parent.get_bunch()
        else:
            return None

    def get_flow(self) -> "Optional[Flow]":
        """
        get the ``Flow`` which the node belongs to.

        Returns
        -------
        Flow or None
            the root node if it is a ``Flow``, otherwise ``None``.
        """
        from .flow import Flow

        root = self.get_root()
        if isinstance(root, Flow):
            return root
        return None

    def find_node(self, a_path: str) -> Optional[Node]:
        """
        use node path to find a node.

        Parameters
        ----------
        a_path
            type of node path

                1. node3: relative to currently level
                2. /flow1/node1/node2
                3. node1/node2
                4. ./node3
                5. ../node1/node2

        Returns
        -------
        Node or None
            node with node_path or None if not found
        """
        full_node_path = PurePosixPath(self.node_path).parent.joinpath(a_path)
        cur_node = self.get_root()
        parts = full_node_path.parts[1:]
        if parts[0] != cur_node.name:
            return None
        for a_token in parts[1:]:
            if cur_node is None:
                break
            if a_token == ".":
                continue
            if a_token == "..":
                cur_node = cur_node.parent
                continue

            t_node = None
            for a_child in cur_node.children:
                if a_child.name == a_token:
                    t_node = a_child
                    break
            if t_node is None:
                return None
            cur_node = t_node
        return cur_node

    @classmethod
    def check_absolute_node_path(cls, node_path: str) -> bool:
        if not node_path.startswith("/"):
            return False
        if node_path == "/":
            return False
        return True

    @classmethod
    def check_node_path(cls, node_path: str) -> bool:
        if node_path.startswith("/"):
            return cls.check_absolute_node_path(node_path)
        if node_path.startswith("../"):
            if len(node_path) == 3:
                return False
            else:
                return True
        if node_path.startswith("./"):
            if len(node_path) == 2:
                return False
            else:
                return True
        return False

    # State management -----------------------------------------------------

    def is_suspended(self) -> bool:
        return self.state.suspended

    def computed_status(self, immediate: bool) -> NodeStatus:
        """
        Compute node_status of a node from its children. Won't change anything in ``node``.

        If ``immediate`` is True, use children's node_status.
        If ``immediate`` is False, compute each child's node_status

        Parameters
        ----------
        immediate

        Returns
        -------
        NodeStatus
        """
        raise Exception("Not implemented!")

    def set_node_status_only(self, node_status: NodeStatus):
        """
        Set node status to some status without any side effect.

        Parameters
        ----------
        node_status
            Node status, just an enum without any additional data.
        """
        old_state = self.state.node_status
        if old_state == node_status:
            return

        self.state.node_status = node_status

    def set_node_status(self, node_status: NodeStatus):
        """
        Set node status to some status with side effect.

        Parameters
        ----------
        node_status
            Node status, just an enum without any additional data.
        """
        if self.state.node_status == node_status:
            return

        self.set_node_status_only(node_status)
        self.handle_status_change()

    def set_default_node_status(self, node_status: Literal[NodeStatus.queued, NodeStatus.complete]):
        """
        Set the default node status. ``requeue`` will reset the node to this status
        instead of ``NodeStatus.queued``.

        Only ``queued`` and ``complete`` are allowed as a default status,
        because ``submitted``/``active``/``aborted`` are transient runtime statuses and
        make no sense as a requeue target.

        Parameters
        ----------
        node_status
            Default node status used by ``requeue``.
        """
        allowed = (NodeStatus.queued, NodeStatus.complete)
        if node_status not in allowed:
            raise UnsupportedValueError(
                f"default node status {node_status} is not supported. "
                f"Only {', '.join(s.name for s in allowed)} are allowed.",
                value=getattr(node_status, "value", str(node_status)),
            )
        self.default_node_status = node_status

    def sink_status_change_only(self, node_status: NodeStatus):
        """
        Apply the node_status change to all its descendants without doing anything.

        Sink status down. This method can only be called in set_state and itself.
        """
        self.set_node_status_only(node_status)

    def sink_status_change(self, node_status: NodeStatus):
        """
        Apply the node_status change to all its descendants with side effects.
        """
        self.sink_status_change_only(node_status)

    def swim_status_change(self):
        """
        Compute current node's node_status, and swim status change up the tree.

        Swim current status up. This method can only be called in handle_status_change and itself.
        """
        node_status = self.computed_status(immediate=True)

        if node_status == NodeStatus.complete:
            if node_status != self.state.node_status:
                self.set_node_status_only(node_status=node_status)
            if self.repeat is not None:
                if self.repeat.increment():
                    self.requeue(reset_repeat=False)
                    self.swim_status_change_only()
                    return

        if node_status != self.state.node_status:
            self.set_node_status_only(node_status=node_status)

        if self.parent is not None:
            self.parent.swim_status_change()
        return

    def swim_status_change_only(self):
        node_status = self.computed_status(immediate=True)

        if node_status != self.state.node_status:
            self.set_node_status_only(node_status=node_status)

        if self.parent is not None:
            self.parent.swim_status_change()
        return

    def handle_status_change(self):
        pass
        # self.swim_status_change()

    # Trigger --------------------------------------------------------

    def add_trigger(self, trigger: Union[str, Expression], parse: bool = False):
        """
        Add trigger to node.

        Parameters
        ----------
        trigger
        parse
            If set, trigger is parsed to create an AST immediately.
            If not set, trigger is just store as a string in expression and is not parsed.

        Raises
        ------
        TypeError
            If ``trigger`` is neither a ``str`` nor an ``Expression``.
        ExpressionSyntaxError
            If ``parse`` is set and the expression can not be parsed.
            The node's ``trigger_expression`` and ``complete_trigger_expression``
            are left exactly as they were before the call.

        Notes
        -----
        When ``parse`` is set, the expression is validated **before** being assigned
        to the node, so a failing parse never leaves the node partially mutated.

        When an ``Expression`` instance is passed in, that very instance is stored on
        the node (identity is preserved), which keeps the existing behaviour.
        """
        if isinstance(trigger, str):
            expression = Expression(trigger)
        elif isinstance(trigger, Expression):
            expression = trigger
        else:
            raise TypeError("trigger only supports str or Expression.")

        if parse:
            expression.create_ast(self)

        self.trigger_expression = expression

    def evaluate_trigger(self) -> bool:
        """
        Evaluate trigger expression of this node.

        If trigger is not parsed, an AST is created first before the expression is evaluated.

        Returns
        -------
        bool
            true if trigger is satisfied.

        Notes
        -----
        该方法仅检查节点自己的触发器是否满足条件，节点是否满足运行条件还取决于父节点是否满足。

        ``resolve_dependencies`` 方法会自顶向下检查各节点触发器。
        """
        if self.trigger_expression is None:
            return True

        if self.trigger_expression.ast is None:
            self.trigger_expression.create_ast(self)

        return self.trigger_expression.evaluate()

    def add_complete_trigger(self, trigger: Union[str, Expression], parse: bool = False):
        """
        Add complete trigger to node.

        Parameters
        ----------
        trigger
        parse
            If set, trigger is parsed to create an AST immediately.
            If not set, trigger is just store as a string in expression and is not parsed.

        Raises
        ------
        TypeError
            If ``trigger`` is neither a ``str`` nor an ``Expression``.
        ExpressionSyntaxError
            If ``parse`` is set and the expression can not be parsed.
            The node's ``trigger_expression`` and ``complete_trigger_expression``
            are left exactly as they were before the call.

        Notes
        -----
        When ``parse`` is set, the expression is validated **before** being assigned
        to the node, so a failing parse never leaves the node partially mutated.

        When an ``Expression`` instance is passed in, that very instance is stored on
        the node (identity is preserved), which keeps the existing behaviour.
        """
        if isinstance(trigger, str):
            expression = Expression(trigger)
        elif isinstance(trigger, Expression):
            expression = trigger
        else:
            raise TypeError("trigger only supports str or Expression.")

        if parse:
            expression.create_ast(self)

        self.complete_trigger_expression = expression

    def evaluate_complete_trigger(self) -> bool:
        if self.complete_trigger_expression is None:
            return False

        if self.complete_trigger_expression.ast is None:
            self.complete_trigger_expression.create_ast(self)

        return self.complete_trigger_expression.evaluate()

    # Resolve -----------------------------------------------------------
    def check_dependencies(self) -> bool:
        # check suspend
        if self.is_suspended():
            return False

        # check time
        if not self.resolve_time_dependencies():
            return False

        # check complete
        # TODO: check complete before or after check time?
        if self.evaluate_complete_trigger():
            self.is_complete_triggered = True
            self.set_node_status(NodeStatus.complete)
            return False

        # check trigger
        if not self.evaluate_trigger():
            return False

        return True

    def resolve_dependencies(self) -> bool:
        """
        Check all dependencies in the Node, and return True if all dependencies are satisfied.

        Returns
        -------
        bool
            True if node should be run, or False.
        """
        return self.check_dependencies()

    # Variable (Parameter, Event, Meter) -----------------------------

    def find_variable(self, name: str) -> Optional[Union[Event, Parameter, Meter]]:
        """
        Find variable (event, meter, parameter) by name.

        Parameters
        ----------
        name

        Returns
        -------
        Optional[Union[Event, Parameter, Meter]]
            The variable found, or None if not found.
        """
        e = self.find_event(name)
        if e is not None:
            return e

        m = self.find_meter(name)
        if m is not None:
            return m

        p = self.find_parameter(name)
        if p is not None:
            return p

        return None

    # Parameter ------------------------------------------------------

    def add_parameter(
            self,
            param: Union[dict[str, Union[str, float, int, bool]], list[Parameter], str, Parameter],
            value: Optional[Union[str, float, int, bool]] = None
    ) -> Optional[Parameter]:
        """
        Add ``Parameter``(s) to this node.

        If ``param` is a dict or list, ``value`` must be None.
        If ``param`` is a str, ``value`` must not be None.

        TODO: add_parameter([dict])
        """
        if isinstance(param, dict):
            if value is not None:
                raise TypeError("value must be None if param is dict.")
            for k, v in param.items():
                p = Parameter(name=k, value=v)
                self.user_parameters[k] = p
        elif isinstance(param, list):
            if value is not None:
                raise TypeError("value must be None if param is list.")
            for p in param:
                if not isinstance(p, Parameter):
                    raise TypeError("param must be a list of Parameter.")
                self.user_parameters[p.name] = p
        elif isinstance(param, str):
            if value is None or isinstance(value, list) or isinstance(value, dict):
                raise TypeError("value cannot be None/list/dict if param is str.")
            name = param
            p = Parameter(name=name, value=value)
            self.user_parameters[name] = p
            return p
        elif isinstance(param, Parameter):
            if value is not None:
                raise TypeError("value must be None if param is Parameter.")
            self.user_parameters[param.name] = param
            return param
        else:
            raise TypeError(f"{param} is str, list, dict or Parameter.")

    def find_parameter(self, name: str) -> Optional[Parameter]:
        """
        Find a ``parameter`` only in this node, first in user parameters, then in generated parameters.
        """
        p = self.find_user_parameter(name)
        if p is not None:
            return p

        p = self.find_generated_parameter(name)
        return p

    def find_user_parameter(self, name: str) -> Optional[Parameter]:
        """
        Find user ``parameter`` only in this node.
        """
        return self.user_parameters.get(name, None)

    # @abstractmethod
    def find_generated_parameter(self, name: str) -> Optional[Parameter]:
        """
        Find generated ``parameter`` only in this node.
        """
        generated_params = self.generated_parameters_only()
        if name in generated_params:
            return generated_params[name]
        else:
            return None

    def find_parent_parameter(self, name: str) -> Optional[Parameter]:
        """
        Find a ``Parameter`` up along the node tree.
        """
        p = self.find_parameter(name)
        if p is not None:
            return p

        parent_node = self.parent
        while parent_node is not None:
            p = parent_node.find_parameter(name)
            if p is not None:
                return p
            parent_node = parent_node.parent

        bunch = self.get_bunch()
        if bunch is None:
            return None

        return bunch.find_parameter(name)

    def update_generated_parameters(self):
        """
        Update generated parameters for this node.
        """
        # NOTE: repeat generated parameters are updated on time, so this code seem to be no effect.
        if self.repeat is not None:
            self.repeat.generated_parameters()

    def parameters(self) -> dict[str, Parameter]:
        """
        Return all parameters accessible to this Node up the node tree.
        """
        params = self.parameters_only().copy()

        parent_node = self.parent
        while parent_node is not None:
            parent_params = parent_node.parameters_only().copy()
            for key, p in parent_params.items():
                if key not in params:
                    params[key] = p
            parent_node = parent_node.parent

        bunch = self.get_bunch()
        if bunch is not None:
            bunch_params = bunch.parameters_only()
            for key, p in bunch_params.items():
                if key not in params:
                    params[key] = p

        return params

    def parameters_only(self) -> Dict[str, Parameter]:
        """
        Return all parameters only in this Node.

        If generated parameter has same name as user parameter, user parameter will be used.
        """
        user_params = self.user_parameters_only()
        generated_params = self.generated_parameters_only()

        params = {
            **user_params
        }
        for key, p in generated_params.items():
            if key not in params:
                params[key] = p

        return params

    def user_parameters_only(self) -> Dict[str, Parameter]:
        """
        Return user defined parameters in this Node.
        """
        return self.user_parameters

    def generated_parameters_only(self) -> Dict[str, Parameter]:
        """
        Return a dict containerd generated parameters in this Node

        Notes
        -----
        Return value is dict of reference of ``Parameter``s, so it's value will be updated automatically.
        """
        p = dict()
        if self.repeat is not None:
            p.update(self.repeat.generated_parameters())
        return p

    # Event ----------------------------------------------------------

    def add_event(self, name: str, initial_value: bool = False, check: bool = True) -> Event:
        if check:
            if self.find_event(name):
                raise RuntimeError(f"add event failed: event name is duplicate: {name}")

        event = Event(name, initial_value=initial_value)
        self.events.append(event)
        return event

    def set_event(self, name: str, value: bool) -> bool:
        event = self.find_event(name)
        if event is not None:
            event.value = value
            return True

        return False

    def find_event(self, name: str, ) -> Optional[Event]:
        for event in self.events:
            if event.name == name:
                return event
        return None

    def reset_event(self, name: str) -> bool:
        event = self.find_event(name)
        if event is not None:
            event.reset()
            return True

        return False

    # Meter ----------------------------------------------------------

    def add_meter(self, name: str, min_value: int, max_value: int) -> Meter:
        meter = Meter(name, min_value=min_value, max_value=max_value)
        self.meters.append(meter)
        return meter

    def set_meter(self, name: str, value: int) -> bool:
        meter = self.find_meter(name)
        if meter is not None:
            meter.value = value
            return True

        return False

    def find_meter(self, name: str) -> Optional[Meter]:
        for meter in self.meters:
            if meter.name == name:
                return meter
        return None

    def reset_meter(self, name: str) -> bool:
        meter = self.find_meter(name)
        if meter is not None:
            meter.reset()
            return True

        return False

    # Limit ----------------------------------------------------------

    def add_in_limit(self, limit_name: str, node_path: Optional[str] = None, tokens: int = 1) -> InLimit:
        """
        Add InLimit to this node's InLimitManager.

        Parameters
        ----------
        limit_name
            Limit's name
        node_path
            Limit's reference node path, default is None
        tokens
            tokens for this node, default is 1.

        Returns
        -------
        InLimit
        """
        in_limit = InLimit(limit_name, node_path=node_path, tokens=tokens)
        self.in_limit_manager.add_in_limit(in_limit)
        return in_limit

    def add_limit(self, name: str, limit: int) -> Limit:
        """
        Add a Limit to node. Limits in one Node should not have duplicate names.

        Parameters
        ----------
        name
            limit name.
        limit
            total tokens for Limit.
        Returns
        -------
        Limit
        """
        if self.find_limit(name) is not None:
            raise RuntimeError(f"add_limit failed: duplicate limit {name} for node {self.node_path}")
        item = Limit(name, limit)
        item.set_node(self)
        self.limits.append(item)
        return item

    def add_limit_object(self, limit: Limit) -> Limit:
        """
        Add an already created ``Limit`` object to node.

        Unlike ``add_limit``, the ``Limit`` object is attached as it is, so runtime states
        (``value`` and ``node_paths``) carried by the object are kept.
        This is used by deserialization, where a ``Limit`` is created by ``Limit.from_dict``.

        Parameters
        ----------
        limit
            The ``Limit`` object to be attached to this node.

        Returns
        -------
        Limit
            The ``limit`` passed in.

        Raises
        ------
        RuntimeError
            If this node already has a ``Limit`` with the same name.
        """
        if self.find_limit(limit.name) is not None:
            raise RuntimeError(f"add_limit_object failed: duplicate limit {limit.name} for node {self.node_path}")
        limit.set_node(self)
        self.limits.append(limit)
        return limit

    def find_limit(self, name: str) -> Optional[Limit]:
        """
        Find Limit in this node.

        Parameters
        ----------
        name
            Limit's name

        Returns
        -------
        Limit or None
        """
        for item in self.limits:
            if item.name == name:
                return item
        return None

    def find_limit_up(self, name: str) -> Optional[Limit]:
        """
        Find Limit up along the node tree.

        Parameters
        ----------
        name
            Limit's name

        Returns
        -------
        Limit or None
        """
        item = self.find_limit(name)
        if item is not None:
            return item

        the_parent = self.parent
        while the_parent is not None:
            item = the_parent.find_limit(name)
            if item is not None:
                return item
            the_parent = the_parent.parent

        return None

    def check_in_limit_up(self) -> bool:
        """
        Check if all ``InLimit`` have enough tokens up along the tree.

        Returns
        -------
        bool
            If all ``InLimit`` have enough tokens, return True.
        """
        if not self.in_limit_manager.in_limit():
            return False

        the_parent = self.parent
        while the_parent is not None:
            if not the_parent.in_limit_manager.in_limit():
                return False
            the_parent = the_parent.parent

        return True

    def increment_in_limit(self, limit_set: Set[Limit]):
        """
        Occupy all InLimit up the node tree.

        Parameters
        ----------
        limit_set
            A set to save changed ``Limit``, to make sure one Limit is incremented only once.
        """
        node_path = self.node_path
        self.in_limit_manager.increment_in_limit(limit_set, node_path)

        the_parent = self.parent
        while the_parent is not None:
            the_parent.in_limit_manager.increment_in_limit(limit_set, node_path)
            the_parent = the_parent.parent

    def decrement_in_limit(self, limit_set: Set[Limit]):
        """
        Release all InLimit up the node tree.

        Parameters
        ----------
        limit_set
            A set to save changed ``Limit``, to make sure one Limit is decremented only once.
        """
        node_path = self.node_path
        self.in_limit_manager.decrement_in_limit(limit_set, node_path)

        the_parent = self.parent
        while the_parent is not None:
            the_parent.in_limit_manager.decrement_in_limit(limit_set, node_path)
            the_parent = the_parent.parent

    # Repeat ---------------------------------------------------------

    def add_repeat(self, r: RepeatBase) -> RepeatBase:
        self.repeat = Repeat(r)
        return r

    # Time Attribute -----------------------------------------------------------

    def add_time(self, time: Union[datetime.time, str]) -> TimeAttribute:
        """
        Add a ``TimeAttribute`` to Node. Node can have multiply time attributes.

        Parameters
        ----------
        time
            When to "run" the node.

        Returns
        -------
        TimeAttribute
        """
        time_attr = TimeAttribute(time)
        self.times.append(time_attr)
        return time_attr

    def add_time_attribute(self, time_attr: TimeAttribute) -> TimeAttribute:
        """
        Add an already created ``TimeAttribute`` object to Node.

        Unlike ``add_time``, the ``TimeAttribute`` object is attached as it is, so the runtime
        ``free`` latch carried by the object is kept.
        This is used by deserialization, where a ``TimeAttribute`` is created by
        ``TimeAttribute.from_dict``.

        Parameters
        ----------
        time_attr
            The ``TimeAttribute`` object to be attached to this node.

        Returns
        -------
        TimeAttribute
            The ``time_attr`` passed in.
        """
        self.times.append(time_attr)
        return time_attr

    def resolve_time_dependencies(self) -> bool:
        """
        Check if there has one time dependency which is satisfied.

        Returns
        -------
        bool
        """
        if len(self.times) == 0:
            return True

        flow = self.get_root()
        if not hasattr(flow, "calendar"):
            raise RuntimeError("node should be in a flow to check time dependencies.")

        flow_calendar: Calendar = flow.calendar
        for time_attr in self.times:
            if time_attr.is_free(flow_calendar):
                return True

        return False

    def calendar_changed(self, calendar: Calendar):
        """
        When Flow's calendar is changed, call this method to update time attributes.

        Parameters
        ----------
        calendar
            The calendar of a Flow.
        """
        for time_attr in self.times:
            time_attr.calendar_changed(calendar)

    # Node Operations ------------------------------------------------

    def requeue(self, reset_repeat: bool = True):
        """
        Requeue the node itself, don't affect children nodes.

        Requeue operation sets node's status to ``self.default_node_status`` (``NodeStatus.queued`` by default)
        """
        self.set_node_status_only(self.default_node_status)

        # reset complete trigger
        self.is_complete_triggered = False

        # reset time attributes
        for time_attr in self.times:
            time_attr.reset()

        # reset event attributes
        for event in self.events:
            event.reset()

        # reset meter attributes
        for meter in self.meters:
            meter.reset()

        # reset repeat attributes
        # TODO: add requeue args.
        if reset_repeat and self.repeat is not None:
            self.repeat.reset()

        # reset trigger
        if self.trigger_expression is not None:
            self.trigger_expression.reset()

    def suspend(self):
        """
        Suspend the node.

        Suspended nodes does not run automatically, see ``Node.resolve_dependencies`` method.
        """
        self.state.suspended = True

    def resume(self):
        """
        Resume the node.
        """
        self.state.suspended = False

    def free_dependencies(self, dep_type: Optional[Literal["all", "time", "trigger"]] = None):
        """
        Ignore some type of dependencies in Node. Including:

            * time
            * trigger

        Parameters
        ----------
        dep_type
            dependency type:

            * all
            * time
            * trigger
        """
        if dep_type is None:
            dep_type = "all"

        if dep_type not in ("all", "time", "trigger"):
            raise UnsupportedValueError(
                f"dependency type {dep_type} is not supported.",
                value=str(dep_type),
            )

        free_time = False
        free_trigger = False

        if dep_type == "all" or dep_type == "time":
            free_time = True
        if dep_type == "all" or dep_type == "trigger":
            free_trigger = True

        if free_time:
            for time_attr in self.times:
                time_attr.set_free()

        if free_trigger:
            # Freeing the trigger of a node without a trigger is a no-op.
            if self.trigger_expression is not None:
                self.trigger_expression.set_free()

        return
