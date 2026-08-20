"""Process exit codes of the ``Client_CLI`` (requirements 10.1 ~ 10.4).

A client command runs inside a job script that uses ``set -e``, so the exit
code is the only failure signal the script can act on. Four values are used:

* ``0`` success,
* ``1`` the request was wrong (unknown node, malformed path, bad value),
* ``3`` the server, or the client's parsing of its answer, failed,
* ``4`` the server could not be reached within the Retry_Window.

Two entry points cover the two ways a command can fail: a non zero
``ServiceResponse.flag`` coming back from the server
(:func:`exit_code_for_error_code`) and an exception raised locally by the
client, typically by the Call_Wrapper (:func:`exit_code_for_exception`).

**On the duplicated numbers.** The authoritative Error_Code table lives in
``takler/server/protocol/error_code.py``, and that module was written so the
client could import it (it deliberately avoids ``takler_pb2``). Importing it
from here would nevertheless execute ``takler/server/__init__.py``, which pulls
in the scheduler, the network service and the generated stubs, so a child
command would drag the whole server package in just to translate an integer.
Until the mapping moves to a transport neutral package (M3), this module
therefore restates the two columns of the design table it needs and stays
dependent on :mod:`takler.exceptions` only. (``takler/client/__init__.py``
re-exports the service client, so today the server package is loaded anyway
whenever anything in the client package is imported; keeping this module's own
dependencies minimal is what makes that fixable later, in one place.)
The duplication is not left
unguarded: ``tests/client/test_exit_code_unit.py`` cross-checks every entry
against ``error_code.py``, so the tables cannot drift apart silently.
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
    "EXIT_OK",
    "EXIT_REQUEST_ERROR",
    "EXIT_SERVER_ERROR",
    "EXIT_UNREACHABLE",
    "EXIT_CODE_BY_ERROR_CODE",
    "EXIT_CODE_BY_TYPE",
    "exit_code_for_error_code",
    "exit_code_for_exception",
]

#: The command succeeded (requirement 10.1).
EXIT_OK: int = 0

#: The request itself was not acceptable: unknown node, malformed node path,
#: unsupported value, unparseable expression (requirement 10.2).
EXIT_REQUEST_ERROR: int = 1

#: The server failed while executing the command, or answered something the
#: client cannot use (requirement 10.3).
EXIT_SERVER_ERROR: int = 3

#: The Retry_Window was exhausted without reaching the server
#: (requirement 10.4).
EXIT_UNREACHABLE: int = 4

#: Error_Code -> exit code, i.e. the last column of the design's Error_Code
#: table. Keys mirror ``error_code.ERROR_NAME_BY_CODE`` exactly.
EXIT_CODE_BY_ERROR_CODE: Dict[int, int] = {
    0: EXIT_OK,             # success
    1: EXIT_REQUEST_ERROR,  # takler_error
    10: EXIT_REQUEST_ERROR,  # node_not_found
    11: EXIT_REQUEST_ERROR,  # invalid_node_path
    12: EXIT_REQUEST_ERROR,  # node_type
    13: EXIT_REQUEST_ERROR,  # unsupported_value
    14: EXIT_REQUEST_ERROR,  # flow_state
    15: EXIT_REQUEST_ERROR,  # invalid_request
    20: EXIT_REQUEST_ERROR,  # expression_syntax
    30: EXIT_SERVER_ERROR,  # job_submission
    31: EXIT_SERVER_ERROR,  # zombie
    40: EXIT_UNREACHABLE,   # transport
    41: EXIT_UNREACHABLE,   # client_connection
    42: EXIT_SERVER_ERROR,  # server_response
    43: EXIT_REQUEST_ERROR,  # permission_denied
    99: EXIT_SERVER_ERROR,  # internal_error
}

#: Exception type -> exit code, for failures the client raises locally. Keys
#: mirror ``error_code.ERROR_CODE_BY_TYPE`` and each value equals the exit code
#: of that type's Error_Code, so both entry points agree.
EXIT_CODE_BY_TYPE: Dict[Type[BaseException], int] = {
    TaklerError: EXIT_REQUEST_ERROR,
    NodeNotFoundError: EXIT_REQUEST_ERROR,
    InvalidNodePathError: EXIT_REQUEST_ERROR,
    NodeTypeError: EXIT_REQUEST_ERROR,
    UnsupportedValueError: EXIT_REQUEST_ERROR,
    FlowStateError: EXIT_REQUEST_ERROR,
    InvalidRequestError: EXIT_REQUEST_ERROR,
    ExpressionSyntaxError: EXIT_REQUEST_ERROR,
    JobSubmissionError: EXIT_SERVER_ERROR,
    ZombieError: EXIT_SERVER_ERROR,
    TransportError: EXIT_UNREACHABLE,
    ClientConnectionError: EXIT_UNREACHABLE,
    ServerResponseError: EXIT_SERVER_ERROR,
    PermissionDeniedError: EXIT_REQUEST_ERROR,
}


def exit_code_for_error_code(code: int) -> int:
    """Return the process exit code for the Error_Code ``code``.

    An unregistered non zero code is treated as the most conservative failure,
    :data:`EXIT_SERVER_ERROR`: the server reported *some* failure this client
    build does not know about, so the safe reading is "the server side went
    wrong", not "your request was wrong" and not "success".

    Args:
        code: The value of ``ServiceResponse.flag``, not necessarily a
            registered Error_Code.

    Returns:
        One of :data:`EXIT_OK`, :data:`EXIT_REQUEST_ERROR`,
        :data:`EXIT_SERVER_ERROR`, :data:`EXIT_UNREACHABLE`. Never raises.
    """
    return EXIT_CODE_BY_ERROR_CODE.get(code, EXIT_SERVER_ERROR)


def exit_code_for_exception(exc: BaseException) -> int:
    """Return the process exit code for an exception raised on the client side.

    The lookup is by exact type and does not walk the MRO, mirroring
    ``error_code_for_exception``: a ``TaklerError`` subclass without an entry of
    its own is a takler reported failure that this version does not classify
    further, which is exactly what the generic ``TaklerError`` code means, so it
    exits with :data:`EXIT_REQUEST_ERROR`. Anything that is not a
    ``TaklerError`` is an unexpected internal failure and exits with
    :data:`EXIT_SERVER_ERROR`.

    Args:
        exc: The exception that terminated the command.

    Returns:
        The process exit code. Never raises.
    """
    exit_code = EXIT_CODE_BY_TYPE.get(type(exc))
    if exit_code is not None:
        return exit_code
    if isinstance(exc, TaklerError):
        return EXIT_REQUEST_ERROR
    return EXIT_SERVER_ERROR
