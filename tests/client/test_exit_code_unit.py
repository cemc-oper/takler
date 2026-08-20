"""Unit tests for the client exit code mapping (``takler/client/exit_code.py``).

Besides pinning the four values and the conservative fallback, this file guards
the deliberate duplication documented in ``exit_code.py``: every entry is
cross-checked against the authoritative Error_Code table in
``takler/server/protocol/error_code.py``, so the two tables cannot drift apart.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

import takler.client.exit_code
from takler.client.exit_code import (
    EXIT_CODE_BY_ERROR_CODE,
    EXIT_CODE_BY_TYPE,
    EXIT_OK,
    EXIT_REQUEST_ERROR,
    EXIT_SERVER_ERROR,
    EXIT_UNREACHABLE,
    exit_code_for_error_code,
    exit_code_for_exception,
)
from takler.exceptions import (
    ClientConnectionError,
    ExpressionSyntaxError,
    JobSubmissionError,
    NodeNotFoundError,
    PermissionDeniedError,
    ServerResponseError,
    TaklerError,
    TransportError,
)
from takler.server.protocol.error_code import (
    ERROR_CODE_BY_TYPE,
    ERROR_NAME_BY_CODE,
    GENERIC_TAKLER_ERROR,
    INTERNAL_SERVER_ERROR,
    SUCCESS,
    error_code_for_exception,
)


def test_constants():
    assert (EXIT_OK, EXIT_REQUEST_ERROR, EXIT_SERVER_ERROR, EXIT_UNREACHABLE) == (
        0,
        1,
        3,
        4,
    )


@pytest.mark.parametrize(
    "code, expected",
    [
        (SUCCESS, EXIT_OK),
        (GENERIC_TAKLER_ERROR, EXIT_REQUEST_ERROR),
        (10, EXIT_REQUEST_ERROR),
        (11, EXIT_REQUEST_ERROR),
        (12, EXIT_REQUEST_ERROR),
        (13, EXIT_REQUEST_ERROR),
        (14, EXIT_REQUEST_ERROR),
        (15, EXIT_REQUEST_ERROR),
        (20, EXIT_REQUEST_ERROR),
        (30, EXIT_SERVER_ERROR),
        (31, EXIT_SERVER_ERROR),
        (40, EXIT_UNREACHABLE),
        (41, EXIT_UNREACHABLE),
        (42, EXIT_SERVER_ERROR),
        (43, EXIT_REQUEST_ERROR),
        (INTERNAL_SERVER_ERROR, EXIT_SERVER_ERROR),
    ],
)
def test_exit_code_for_registered_error_code(code, expected):
    assert exit_code_for_error_code(code) == expected


@pytest.mark.parametrize("code", [2, 9, 16, 19, 50, 100, -1, 2**63])
def test_unregistered_non_zero_code_is_treated_as_server_error(code):
    """Requirement 3.9 leaves unregistered codes valid; we take them as 3."""
    assert code not in ERROR_NAME_BY_CODE
    assert exit_code_for_error_code(code) == EXIT_SERVER_ERROR


@pytest.mark.parametrize(
    "exc, expected",
    [
        (NodeNotFoundError("no such node", node_path="/f/t"), EXIT_REQUEST_ERROR),
        (ExpressionSyntaxError("bad expression"), EXIT_REQUEST_ERROR),
        (PermissionDeniedError("refused"), EXIT_REQUEST_ERROR),
        (JobSubmissionError("fork failed"), EXIT_SERVER_ERROR),
        (ServerResponseError("not json"), EXIT_SERVER_ERROR),
        (TransportError("transport broke"), EXIT_UNREACHABLE),
        (ClientConnectionError("window exhausted"), EXIT_UNREACHABLE),
        (TaklerError("generic"), EXIT_REQUEST_ERROR),
    ],
)
def test_exit_code_for_registered_exception(exc, expected):
    assert exit_code_for_exception(exc) == expected


def test_unregistered_takler_subclass_falls_back_to_generic_exit_code():
    class WeirdNodeNotFoundError(NodeNotFoundError):
        pass

    exc = WeirdNodeNotFoundError("still a takler error")
    assert exit_code_for_exception(exc) == EXIT_REQUEST_ERROR
    # Consistent with the generic TaklerError code the server would report.
    assert exit_code_for_exception(exc) == exit_code_for_error_code(
        GENERIC_TAKLER_ERROR
    )


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("plain value error"),
        RuntimeError("plain runtime error"),
        KeyError("missing"),
        OSError("io failed"),
    ],
)
def test_foreign_exception_is_a_server_error(exc):
    assert exit_code_for_exception(exc) == EXIT_SERVER_ERROR
    assert exit_code_for_exception(exc) == exit_code_for_error_code(
        INTERNAL_SERVER_ERROR
    )


def test_error_code_column_covers_the_whole_error_code_table():
    """The duplicated column must have exactly the registered codes as keys."""
    assert set(EXIT_CODE_BY_ERROR_CODE) == set(ERROR_NAME_BY_CODE)
    assert set(EXIT_CODE_BY_ERROR_CODE.values()) <= {
        EXIT_OK,
        EXIT_REQUEST_ERROR,
        EXIT_SERVER_ERROR,
        EXIT_UNREACHABLE,
    }
    # Only success maps to exit code 0.
    assert [
        code
        for code, exit_code in EXIT_CODE_BY_ERROR_CODE.items()
        if exit_code == EXIT_OK
    ] == [SUCCESS]


def test_exception_table_agrees_with_error_code_table():
    """Both entry points must classify the same exception identically."""
    assert set(EXIT_CODE_BY_TYPE) == set(ERROR_CODE_BY_TYPE)
    for exc_type, error_code in ERROR_CODE_BY_TYPE.items():
        exc = exc_type("message")
        assert error_code_for_exception(exc) == error_code
        assert exit_code_for_exception(exc) == exit_code_for_error_code(error_code)


def test_exit_code_module_only_depends_on_takler_exceptions():
    """The reason the two columns are restated here rather than imported.

    Checked on the source, not on ``sys.modules``: ``takler/client/__init__.py``
    re-exports the service client, so any import inside the client package
    currently loads the server package anyway.
    """
    source = pathlib.Path(takler.client.exit_code.__file__).read_text(encoding="utf-8")
    takler_imports = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
            "takler"
        ):
            takler_imports.add(node.module)
        elif isinstance(node, ast.Import):
            takler_imports.update(
                alias.name for alias in node.names if alias.name.startswith("takler")
            )
    assert takler_imports == {"takler.exceptions"}
