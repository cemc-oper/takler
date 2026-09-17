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

The Error_Code table itself is not restated here: since M3 (task 4) it lives
in the transport neutral :mod:`takler.protocol.error_code`, which this module
imports. What stays local is the one column that is genuinely the client's --
the Error_Code -> process exit code mapping -- plus the four exit code
constants. ``exit_code_for_exception`` is derived from the shared
:func:`~takler.protocol.error_code.error_code_for_exception`, so the two entry
points cannot disagree. (``EXIT_CODE_BY_ERROR_CODE``'s key set is pinned
against the shared table by ``tests/client/test_exit_code_unit.py``.)
"""

from __future__ import annotations

from typing import Dict

from takler.protocol.error_code import error_code_for_exception

__all__ = [
    "EXIT_OK",
    "EXIT_REQUEST_ERROR",
    "EXIT_SERVER_ERROR",
    "EXIT_UNREACHABLE",
    "EXIT_CODE_BY_ERROR_CODE",
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
#: table. Keys mirror
#: :data:`~takler.protocol.error_code.ERROR_NAME_BY_CODE` exactly.
EXIT_CODE_BY_ERROR_CODE: Dict[int, int] = {
    0: EXIT_OK,  # success
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
    40: EXIT_UNREACHABLE,  # transport
    41: EXIT_UNREACHABLE,  # client_connection
    42: EXIT_SERVER_ERROR,  # server_response
    43: EXIT_REQUEST_ERROR,  # permission_denied
    99: EXIT_SERVER_ERROR,  # internal_error
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

    Derived from the shared classification: the exception is mapped to its
    Error_Code by :func:`~takler.protocol.error_code.error_code_for_exception`
    (exact type lookup, never through the MRO; an unlisted ``TaklerError``
    subclass falls back to the generic code, anything else to the internal
    error code) and that code is mapped to its exit code. Deriving rather than
    restating is what keeps the two entry points in agreement by construction.

    Args:
        exc: The exception that terminated the command.

    Returns:
        The process exit code. Never raises.
    """
    return exit_code_for_error_code(error_code_for_exception(exc))
