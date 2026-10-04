from typing import Optional, Dict, Union, List, Literal

from pydantic import BaseModel, field_validator, model_validator, ConfigDict, Field
from pydantic_core import PydanticCustomError

from takler import constant
from takler.exceptions import (
    InvalidNodePathError,
    NodeNotFoundError,
    InvalidRequestError,
)

from .node_container import NodeContainer
from .flow import Flow
from .node import Node
from .state import NodeStatus, ServerStatus, HaltCause
from .util import SerializationType
from .event import Event
from .meter import Meter
from .parameter import Parameter, TAKLER_HOST, TAKLER_PORT, TAKLER_HOME


class Bunch(NodeContainer):
    def __init__(self, name: str = "", host: str = None, port: str = None):
        super(Bunch, self).__init__(name=name)
        self.flows: Dict[str, Flow] = dict()
        self.server_config: ServerConfig = ServerConfig(host=host, port=port)
        self.server_state: ServerState = ServerState()

    # Serialization ---------------------------------------

    def to_dict(self) -> Dict:
        """Serialize the node tree, deployment address, and service state."""
        result = super().to_dict()
        result["flows"] = [flow.to_dict() for key, flow in self.flows.items()]
        result["server_config"] = self.server_config.to_dict()
        result["server_state"] = self.server_state.to_dict()

        return result

    @classmethod
    def from_dict(cls, d: dict) -> "Bunch":
        from takler.serialization.runtime import restore_node

        bunch = restore_node(d)
        if not isinstance(bunch, Bunch):
            raise InvalidRequestError("expected bunch")
        return bunch

    def restore_root_attributes(self, d: Dict) -> None:
        """Restore stored Node attributes without replacing flows or deployment.

        Decode into a fresh node first: missing optional fields reset to their
        defaults, repeated restores do not append attributes, and an invalid
        root leaves the live bunch untouched. This only preserves storage; it
        does not give root scheduling attributes global scheduling semantics.
        """
        root = NodeContainer(name=d["name"])
        Node.fill_from_dict(d, root, method=SerializationType.Status)
        # Only Node state is copied; the temporary container has no service
        # config, service state, or flows. No AST retains the temporary root.
        self.__dict__.update(vars(root))
        self.in_limit_manager.node = self
        for limit in self.limits:
            limit.node = self
        for child in self.children:
            child.parent = self

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
        flow.ensure_definition_generation()
        self.flows[flow.name] = flow
        flow.bunch = self
        return flow

    def validate_limit_references(self) -> List[str]:
        errors = super().validate_limit_references()
        for flow in self.flows.values():
            errors.extend(flow.validate_limit_references())
        return errors

    def find_flow(self, name: str) -> Optional[Flow]:
        return self.flows.get(name, None)

    def delete_flow(self, flow: Union[str, Flow]) -> Flow:
        if isinstance(flow, Flow):
            flow_name = flow.name
        else:
            flow_name = flow

        if flow_name not in self.flows:
            raise NodeNotFoundError(
                f"flow is not in Bunch: {flow_name}",
                node_path=f"/{flow_name}",
            )

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
            raise InvalidNodePathError(
                f"absolute node path is illegal: {a_path}",
                node_path=a_path,
            )
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
        p = self.server_config.find_parameter(name)
        return p

    def generated_parameters_only(self) -> Dict[str, Parameter]:
        return self.server_config.generated_parameters()


class ServerConfig(BaseModel):
    """Deployment address and generated defaults for this server process."""

    host: Optional[str] = constant.DEFAULT_HOST
    port: Optional[str] = constant.DEFAULT_PORT

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
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

    def find_parameter(self, name: str) -> Optional[Parameter]:
        return self.generated_parameters().get(name)

    def generated_parameters(self) -> Dict[str, Parameter]:
        return {
            TAKLER_HOST: Parameter(TAKLER_HOST, self.host),
            TAKLER_PORT: Parameter(TAKLER_PORT, self.port),
            TAKLER_HOME: Parameter(TAKLER_HOME, "."),
        }

    def to_dict(self) -> Dict:
        return {"host": self.host, "port": self.port}

    @classmethod
    def from_dict(cls, d: Dict) -> "ServerConfig":
        if not isinstance(d, dict) or set(d) != {"host", "port"}:
            raise InvalidRequestError("invalid server config")
        return cls.model_validate(d)


class ServerState(BaseModel):
    """Runtime scheduling state; each halt cause is released independently."""

    status: ServerStatus = ServerStatus.running
    status_reason: Optional[
        Literal["manual pause", "recovery pending", "checkpoint failure"]
    ] = None
    halt_causes: List[HaltCause] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @model_validator(mode="after")
    def consistent(self):
        if len(self.halt_causes) != len(set(self.halt_causes)):
            raise PydanticCustomError(
                "invalid_server_state", "duplicate service halt cause"
            )
        if self.status is ServerStatus.running:
            if self.halt_causes or self.status_reason is not None:
                raise PydanticCustomError(
                    "invalid_server_state", "running service cannot have a halt reason"
                )
        else:
            reasons = {
                HaltCause.manual: "manual pause",
                HaltCause.recovery: "recovery pending",
                HaltCause.checkpoint_failure: "checkpoint failure",
            }
            expected = next(
                (
                    reasons[cause]
                    for cause in (
                        HaltCause.checkpoint_failure,
                        HaltCause.recovery,
                        HaltCause.manual,
                    )
                    if cause in self.halt_causes
                ),
                None,
            )
            if self.status_reason != expected:
                raise PydanticCustomError(
                    "invalid_server_state",
                    "halted service reason does not match causes",
                )
        return self

    def to_dict(self) -> Dict:
        return {
            "status": self.status.value,
            "status_reason": self.status_reason,
            "halt_causes": [cause.value for cause in self.halt_causes],
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "ServerState":
        if not isinstance(d, dict) or set(d) != {
            "status",
            "status_reason",
            "halt_causes",
        }:
            raise InvalidRequestError("invalid server state")
        return cls.model_validate(d)
