"""Error_Code classification carried by ``ServiceResponse.flag``.

``flag == 0`` means success and any non zero value means failure; that existing
semantics is the super concept of the classification codes defined here, so
clients that only test ``flag != 0`` stay compatible.

This module imports only :mod:`takler.exceptions`. It deliberately does **not**
import ``takler_pb2``, so the client side can map codes without pulling in the
generated gRPC code, and so the whole module can be relocated to a protocol
package later without touching its dependencies.
"""

from __future__ import annotations

from typing import Dict, Type

from takler.exceptions import (
    ClientConnectionError,
    ExpressionSyntaxError,
    FlowStateError,
    InvalidNodePathError,
    InvalidRequestError,
    JobSubmissionError,
    NodeNotFoundError,
    NodeTypeError,
    PermissionDeniedError,
    ServerResponseError,
    TaklerError,
    TransportError,
    UnsupportedValueError,
    ZombieError,
)

__all__ = [
    "SUCCESS",
    "GENERIC_TAKLER_ERROR",
    "INTERNAL_SERVER_ERROR",
    "UNKNOWN_ERROR_NAME",
    "ERROR_CODE_BY_TYPE",
    "ERROR_NAME_BY_CODE",
    "error_code_for_exception",
    "error_name_for_code",
]

#: The command succeeded.
SUCCESS: int = 0

#: A takler owned error without a dedicated Error_Code of its own.
GENERIC_TAKLER_ERROR: int = 1

#: Any exception that is not a :class:`~takler.exceptions.TaklerError`.
INTERNAL_SERVER_ERROR: int = 99

#: Placeholder name returned for codes that are not registered.
UNKNOWN_ERROR_NAME: str = "unknown"

#: Exception type -> Error_Code. The mapping is injective and is looked up by
#: exact type, never through the MRO.
ERROR_CODE_BY_TYPE: Dict[Type[BaseException], int] = {
    TaklerError: GENERIC_TAKLER_ERROR,
    NodeNotFoundError: 10,
    InvalidNodePathError: 11,
    NodeTypeError: 12,
    UnsupportedValueError: 13,
    FlowStateError: 14,
    InvalidRequestError: 15,
    ExpressionSyntaxError: 20,
    JobSubmissionError: 30,
    ZombieError: 31,
    TransportError: 40,
    ClientConnectionError: 41,
    ServerResponseError: 42,
    PermissionDeniedError: 43,
}

#: Error_Code -> classification name. The mapping is injective.
ERROR_NAME_BY_CODE: Dict[int, str] = {
    SUCCESS: "success",
    GENERIC_TAKLER_ERROR: "takler_error",
    10: "node_not_found",
    11: "invalid_node_path",
    12: "node_type",
    13: "unsupported_value",
    14: "flow_state",
    15: "invalid_request",
    20: "expression_syntax",
    30: "job_submission",
    31: "zombie",
    40: "transport",
    41: "client_connection",
    42: "server_response",
    43: "permission_denied",
    INTERNAL_SERVER_ERROR: "internal_error",
}


def error_code_for_exception(exc: BaseException) -> int:
    """Return the Error_Code that classifies ``exc``.

    The lookup uses the exact exception type and does not walk the MRO, so a
    ``TaklerError`` subclass without its own code falls back to
    :data:`GENERIC_TAKLER_ERROR` instead of borrowing an ancestor's, more
    specific, code. Anything that is not a ``TaklerError`` maps to
    :data:`INTERNAL_SERVER_ERROR`.

    Args:
        exc: The exception to classify.

    Returns:
        The classification code to put into ``ServiceResponse.flag``.
    """
    code = ERROR_CODE_BY_TYPE.get(type(exc))
    if code is not None:
        return code
    if isinstance(exc, TaklerError):
        return GENERIC_TAKLER_ERROR
    return INTERNAL_SERVER_ERROR


def error_name_for_code(code: int) -> str:
    """Return the classification name of ``code``.

    Args:
        code: An Error_Code, not necessarily a registered one.

    Returns:
        The registered classification name, or :data:`UNKNOWN_ERROR_NAME` when
        the code is not registered. Never raises.
    """
    return ERROR_NAME_BY_CODE.get(code, UNKNOWN_ERROR_NAME)
