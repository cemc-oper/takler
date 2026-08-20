"""Unit tests for the command error response of the RPC boundary.

Covers Requirements 3.4 and 3.5: an Exception_Hierarchy exception maps ``flag``
to its dedicated Error_Code, a ``TaklerError`` without a dedicated code falls
back to the generic code, and a foreign exception maps to the
internal-server-error code. ``message`` keeps carrying the type name and the
description text.
"""

import pytest

from takler.exceptions import (
    NodeNotFoundError,
    NodeTypeError,
    TaklerError,
    UnsupportedValueError,
)
from takler.server.network_service import _command_error_response
from takler.server.protocol import error_code


class _UnregisteredTaklerError(TaklerError):
    """A ``TaklerError`` subclass without a dedicated Error_Code."""


@pytest.mark.parametrize(
    "exc, expected_flag",
    [
        (NodeNotFoundError("no such node", node_path="/flow1/task1"), 10),
        (NodeTypeError("not a task: /flow1"), 12),
        (UnsupportedValueError("bad state", value="nope"), 13),
        (TaklerError("plain"), error_code.GENERIC_TAKLER_ERROR),
        (_UnregisteredTaklerError("derived"), error_code.GENERIC_TAKLER_ERROR),
        (ValueError("foreign"), error_code.INTERNAL_SERVER_ERROR),
        (RuntimeError("foreign"), error_code.INTERNAL_SERVER_ERROR),
    ],
)
def test_command_error_response_flag(exc, expected_flag):
    response = _command_error_response(exc)
    assert response.flag == expected_flag
    assert response.flag != error_code.SUCCESS


def test_command_error_response_message_keeps_type_and_text():
    exc = NodeNotFoundError("no such node: /flow1/task1", node_path="/flow1/task1")
    response = _command_error_response(exc)
    assert response.message == "NodeNotFoundError: no such node: /flow1/task1"
