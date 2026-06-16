import importlib
from typing import Optional, Dict, Union, List

from pydantic import BaseModel, field_validator, ConfigDict

from takler import constant

from .node_container import NodeContainer
from .flow import Flow
from .node import Node
from .state import NodeStatus
from .event import Event
from .meter import Meter
from .parameter import (
    Parameter, TAKLER_HOST, TAKLER_PORT, TAKLER_HOME
)


class Bunch(NodeContainer):
    def __init__(self, name: str = "", host: str = None, port: str = None):
        super(Bunch, self).__init__(name=name)
        self.flows: Dict[str, Flow] = dict()
        self.server_state: ServerState = ServerState(host=host, port=port)
        self.server_state.setup()

    # Serialization ---------------------------------------

    def to_dict(self) -> Dict:
        result = super().to_dict()
        result["flows"] = [flow.to_dict() for key, flow in self.flows.items()]
        result["server_state"] = self.server_state.to_dict()

        return result

    @classmethod
    def from_dict(cls, d: dict) -> "Bunch":
        class_type = d["class_type"]
        class_module = class_type["module"]
        class_name = class_type["name"]
        class_module = importlib.import_module(class_module)
        class_object = getattr(class_module, class_name)
        bunch = class_object(name=d["name"])
        for flow in d["flows"]:
            flow = Flow.from_dict(flow)
            bunch.flows[flow.name] = flow
        bunch.server_state = ServerState.from_dict(d["server_state"])
        return bunch

    # Attr ------------------------------------------------

    def get_node_status(self) -> NodeStatus:
        """
        Calculate node status from all flows in bunch.

        Returns
        -------
        NodeStatus
        """
        if len(self.flows) == 0:
            return NodeStatus.unknown
        status = []
        for _, flow in self.flows.items():
            status.append(flow.computed_status(True))
        return max(status)

    # Flow ------------------------------------------------

    def add_flow(self, flow: Union[Flow, str]) -> Flow:
        if isinstance(flow, str):
            flow = Flow(name=flow)
        self.flows[flow.name] = flow
        flow.bunch = self
        return flow

    def find_flow(self, name: str) -> Optional[Flow]:
        return self.flows.get(name, None)

    def delete_flow(self, flow: Union[str, Flow]) -> Flow:
        if isinstance(flow, Flow):
            flow_name = flow.name
        else:
            flow_name = flow

        if flow_name not in self.flows:
            raise ValueError(f"flow is not in Bunch: {flow_name}")

        flow = self.flows.pop(flow_name)

        return flow

    # Node access -----------------------------------------------

    def find_node(self, a_path: str) -> Optional[Node]:
        """
        Find node from path string.

        Parameters
        ----------
        a_path
            node path starting with "/".
        Returns
        -------
        Optional[Node]
        """
        if not Node.check_absolute_node_path(a_path):
            raise ValueError(f"absolute node path is illegal: {a_path}")
        tokens = a_path.split("/")
        assert len(tokens) > 1
        flow_name = tokens[1]
        a_flow = self.find_flow(flow_name)
        if a_flow is None:
            return None
        return a_flow.find_node(a_path)

    def find_path(self, a_path: str) -> Optional[Union[Node, Meter, Event]]:
        """
        Find node or node's variable from path string.

        Parameters
        ----------
        a_path
            node path (/flow1/task1) or variable path (/flow1/task1:event1)

        Returns
        -------
        Optional[Union[Node, Meter, Event]]
        """
        tokens = a_path.split(":")
        if len(tokens) == 1:
            return self.find_node(a_path)
        elif len(tokens) == 2:
            node_path = tokens[0]
            variable_name = tokens[1]
            node = self.find_node(node_path)
            if node is None:
                return None
            v = node.find_variable(variable_name)
            return v
        else:
            return None

    # Parameter ------------------------------------------------

    def find_generated_parameter(self, name: str) -> Optional[Parameter]:
        p = self.server_state.find_parameter(name)
        return p

    def generated_parameters_only(self) -> Dict[str, Parameter]:
        return self.server_state.generated_parameters()


class ServerState(BaseModel):
    server_parameters: List[Parameter] = []
    host: Optional[str] = constant.DEFAULT_HOST
    port: Optional[str] = constant.DEFAULT_PORT

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment = True,
    )

    @field_validator("host")
    def set_host(cls, h: Optional[str]):
        if h is None:
            h = constant.DEFAULT_HOST
        return h

    @field_validator("port")
    def set_port(cls, p: Optional[str]):
        if p is None:
            p = constant.DEFAULT_PORT
        return p

    def setup(self):
        self.setup_default_server_parameters()

    def setup_default_server_parameters(self):
        self.server_parameters.append(Parameter(TAKLER_HOST, self.host))
        self.server_parameters.append(Parameter(TAKLER_PORT, self.port))
        self.server_parameters.append(Parameter(TAKLER_HOME, "."))

    def find_parameter(self, name: str) -> Optional[Parameter]:
        for p in self.server_parameters:
            if p.name == name:
                return p

        return None

    def generated_parameters(self) -> Dict[str, Parameter]:
        params = dict()
        for p in self.server_parameters:
            params[p.name] = p
        return params

    def to_dict(self) -> Dict:
        params = list()
        for param in self.server_parameters:
            params.append(param.to_dict())
        result = dict(
            parameters=params,
            host=self.host,
            port=self.port,
        )
        return result

    @classmethod
    def from_dict(cls, d: Dict) -> "ServerState":
        params = list()
        for p in d["parameters"]:
            params.append(Parameter.from_dict(p))
        return cls(
            host=d["host"],
            port=d["port"],
            server_parameters=params,
        )
