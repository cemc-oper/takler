"""Takler's own exception hierarchy.

All exceptions raised deliberately by takler derive from :class:`TaklerError`,
so callers can catch takler failures without catching unrelated errors.

This module intentionally imports nothing from the rest of ``takler``: every
layer (``core``, ``server``, ``client``, ``tasks``) depends on it, so keeping it
dependency free rules out import cycles.

Each exception passes the caller supplied description to ``Exception.__init__``,
which makes ``str(exc)`` contain that text. The extra attributes
(``node_path``, ``value``, ``flow_name``, ``expression``, ``line``, ``column``)
are structured supplements for programmatic handling, never a replacement for
the message.

``InvalidRequestError`` and ``ExpressionSyntaxError`` also subclass
:class:`ValueError`. This is a deliberate transitional choice: the places these
types replace used to raise ``ValueError``, and existing callers and tests still
catch ``ValueError``. The compatibility can be dropped after M1.
"""

from __future__ import annotations

from typing import Optional

__all__ = [
    "TaklerError",
    "InvalidRequestError",
    "NodeNotFoundError",
    "InvalidNodePathError",
    "NodeTypeError",
    "UnsupportedValueError",
    "FlowStateError",
    "ExpressionSyntaxError",
    "JobSubmissionError",
    "ZombieError",
    "TransportError",
    "ClientConnectionError",
    "ServerResponseError",
    "PermissionDeniedError",
]


class TaklerError(Exception):
    """Base class of all takler owned exceptions."""


class InvalidRequestError(TaklerError, ValueError):
    """The request content is not acceptable: path, type or value.

    Subclasses ``ValueError`` for backward compatibility with callers that
    still use ``except ValueError``.
    """


class NodeNotFoundError(InvalidRequestError):
    """No node exists at the requested path.

    Attributes:
        node_path: The node path that could not be found.
    """

    def __init__(self, message: str, node_path: str = "") -> None:
        super().__init__(message)
        self.node_path = node_path


class InvalidNodePathError(InvalidRequestError):
    """The node path itself is malformed, for example not absolute.

    Attributes:
        node_path: The offending node path.
    """

    def __init__(self, message: str, node_path: str = "") -> None:
        super().__init__(message)
        self.node_path = node_path


class NodeTypeError(InvalidRequestError):
    """The node exists but its type does not fit the operation.

    For example a child command targeting a node that is not a task.

    Attributes:
        node_path: The path of the node with the unexpected type.
    """

    def __init__(self, message: str, node_path: str = "") -> None:
        super().__init__(message)
        self.node_path = node_path


class UnsupportedValueError(InvalidRequestError):
    """The value is outside of the supported set.

    For example a ``force`` status value, a dependency type or a flow type.

    Attributes:
        value: The unsupported value, as received from the caller.
    """

    def __init__(self, message: str, value: str = "") -> None:
        super().__init__(message)
        self.value = value


class FlowStateError(InvalidRequestError):
    """The flow is in a state that does not allow the operation.

    For example beginning a flow that has already begun.

    Attributes:
        flow_name: The name of the flow in the conflicting state.
    """

    def __init__(self, message: str, flow_name: str = "") -> None:
        super().__init__(message)
        self.flow_name = flow_name


class ExpressionSyntaxError(TaklerError, ValueError):
    """A trigger expression could not be parsed, or references something
    that cannot be evaluated.

    Subclasses ``ValueError`` for backward compatibility, see the module
    docstring.

    Attributes:
        expression: The original expression text, when available.
        line: The 1-based line reported by the underlying parser, or ``None``.
        column: The 1-based column reported by the underlying parser, or
            ``None``.
    """

    def __init__(
            self,
            message: str,
            expression: Optional[str] = None,
            line: Optional[int] = None,
            column: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.expression = expression
        self.line = line
        self.column = column


class JobSubmissionError(TaklerError):
    """Rendering or spawning a job script failed."""


class ZombieError(TaklerError):
    """A job reported an instance the server does not know about.

    M1 only defines the type; the detection logic belongs to M2.
    """


class TransportError(TaklerError):
    """A transport level failure that is not further classified."""


class ClientConnectionError(TransportError):
    """The retry window was exhausted without reaching the server."""


class ServerResponseError(TaklerError):
    """The server response cannot be parsed, or is itself an error text."""


class PermissionDeniedError(TaklerError):
    """The server refused the call.

    M1 does not implement authentication; this only carries the mapping of
    the corresponding gRPC status code.
    """
