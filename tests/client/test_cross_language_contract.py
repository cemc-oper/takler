"""Contract tests pinning the Python constants to the cross-language contract.

This file is one half of a drift guard. Its other half is the Go test
``common/errorcode_test.go`` in the ``takler-client`` repository. Both restate,
by hand, the same fixed tables from the m2-security design's "Cross-Language
Contract" section, so that a change on one side without the matching change on
the other side turns into a test failure rather than a silent protocol
divergence between the Python client and the Go client.

**Every expected value below is therefore a literal.** Nothing here may be
derived from ``ERROR_NAME_BY_CODE``, ``EXIT_CODE_BY_ERROR_CODE`` or the
``retry.py`` constants: a table that reads the mapping it is meant to police can
never detect a wrong edit to that mapping. Do not "simplify" these tables into
loops over the production dictionaries.

The complementary check that the two Python tables agree *with each other*
already lives in ``tests/client/test_exit_code_unit.py``; this file only pins
both of them against the external contract.
"""

from __future__ import annotations

import grpc
import pytest

from takler.client.exit_code import (
    EXIT_CODE_BY_ERROR_CODE,
    exit_code_for_error_code,
)
from takler.client.retry import (
    DEFAULT_RETRY_WINDOW_BY_KIND,
    DEFAULT_SINGLE_TIMEOUT,
    ENV_RETRY_WINDOW,
    MAX_BACKOFF_SECONDS,
    NON_RETRYABLE_EXCEPTION_BY_STATUS,
    RETRYABLE_STATUS_CODES,
    CommandKind,
    backoff_seconds,
)
from takler.server.protocol.error_code import (
    ERROR_NAME_BY_CODE,
    UNKNOWN_ERROR_NAME,
    error_name_for_code,
)

# --------------------------------------------------------------------------
# The contract tables, transcribed from the design document by hand.
# --------------------------------------------------------------------------

#: Error_Code -> classification name, the first two columns of the contract's
#: Error_Code table. Sixteen rows: 0, 1, 10~15, 20, 30, 31, 40~43, 99.
CONTRACT_ERROR_NAME_BY_CODE = {
    0: "success",
    1: "takler_error",
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
    99: "internal_error",
}

#: Error_Code -> client exit code, the last column of the same table. The same
#: sixteen rows, written out again rather than joined against the table above,
#: because the contract lists them as one row per code.
CONTRACT_EXIT_CODE_BY_ERROR_CODE = {
    0: 0,
    1: 1,
    10: 1,
    11: 1,
    12: 1,
    13: 1,
    14: 1,
    15: 1,
    20: 1,
    30: 3,
    31: 3,
    40: 4,
    41: 4,
    42: 3,
    43: 1,
    99: 3,
}

#: Codes the contract deliberately leaves out: the gaps between the allocated
#: ranges, the value just past each range, a negative flag and a huge one. All
#: of them classify as ``unknown`` and exit with the most conservative code 3.
CONTRACT_UNREGISTERED_CODES = [
    2,
    9,
    16,
    19,
    21,
    29,
    32,
    39,
    44,
    98,
    100,
    -1,
    2**31 - 1,
    -(2**31),
]

#: Placeholder name for an unregistered non zero code (requirement 15.7).
CONTRACT_UNKNOWN_ERROR_NAME = "unknown"

#: Exit code for an unregistered non zero code: the contract says "the most
#: conservative 3".
CONTRACT_UNREGISTERED_EXIT_CODE = 3

#: "单次超时默认值" row of the contract's retry constant table, in seconds.
CONTRACT_SINGLE_TIMEOUT_SECONDS = 10.0

#: "退避上限" row, in seconds.
CONTRACT_MAX_BACKOFF_SECONDS = 60.0

#: "Child Retry_Window" and "Control / Query Retry_Window" rows, in seconds.
CONTRACT_RETRY_WINDOW_SECONDS_BY_KIND = {
    CommandKind.CHILD: 86400.0,
    CommandKind.CONTROL: 60.0,
    CommandKind.QUERY: 60.0,
}

#: Environment variable that overrides the Retry_Window. The Go half asserts
#: the same name for ``EnvRetryWindow``.
CONTRACT_ENV_RETRY_WINDOW = "TAKLER_TIMEOUT"

#: "可重试状态码" row.
CONTRACT_RETRYABLE_STATUS_CODES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.UNKNOWN,
}

#: "不可重试状态码" row: the request itself is wrong or refused, so repeating it
#: cannot change the outcome.
CONTRACT_NON_RETRYABLE_STATUS_CODES = {
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.NOT_FOUND,
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.UNAUTHENTICATED,
}

#: "退避公式" row, ``min(2**(n-1), 60)``, written out for the first attempts
#: plus the two points where the cap has taken over.
CONTRACT_BACKOFF_SEQUENCE = [
    (1, 1.0),
    (2, 2.0),
    (3, 4.0),
    (4, 8.0),
    (5, 16.0),
    (6, 32.0),
    (7, 60.0),
    (8, 60.0),
    (20, 60.0),
    (10_000, 60.0),
]


# --------------------------------------------------------------------------
# Error_Code and exit code mappings.
# --------------------------------------------------------------------------


# Feature: m2-security, Property 9: 跨语言常量一致性
# Validates: Requirements 14.14, 15.6, 16.17
@pytest.mark.parametrize("code, name", sorted(CONTRACT_ERROR_NAME_BY_CODE.items()))
def test_error_name_by_code_matches_contract(code, name):
    """Every contract row is present in ``ERROR_NAME_BY_CODE`` with its name."""
    assert code in ERROR_NAME_BY_CODE
    assert ERROR_NAME_BY_CODE[code] == name
    assert error_name_for_code(code) == name


# Feature: m2-security, Property 9: 跨语言常量一致性
# Validates: Requirements 14.14, 15.6, 16.17
def test_error_name_by_code_has_no_key_outside_the_contract():
    """And nothing else: the key sets must be equal in both directions."""
    assert set(ERROR_NAME_BY_CODE) == set(CONTRACT_ERROR_NAME_BY_CODE)
    assert len(ERROR_NAME_BY_CODE) == 16


# Feature: m2-security, Property 9: 跨语言常量一致性
# Validates: Requirements 14.14, 15.6, 16.17
@pytest.mark.parametrize(
    "code, exit_code", sorted(CONTRACT_EXIT_CODE_BY_ERROR_CODE.items())
)
def test_exit_code_by_error_code_matches_contract(code, exit_code):
    """Every contract row is present in ``EXIT_CODE_BY_ERROR_CODE``."""
    assert code in EXIT_CODE_BY_ERROR_CODE
    assert EXIT_CODE_BY_ERROR_CODE[code] == exit_code
    assert exit_code_for_error_code(code) == exit_code


# Feature: m2-security, Property 9: 跨语言常量一致性
# Validates: Requirements 14.14, 15.6, 16.17
def test_exit_code_by_error_code_has_no_key_outside_the_contract():
    assert set(EXIT_CODE_BY_ERROR_CODE) == set(CONTRACT_EXIT_CODE_BY_ERROR_CODE)
    assert len(EXIT_CODE_BY_ERROR_CODE) == 16


# Feature: m2-security, Property 9: 跨语言常量一致性
# Validates: Requirements 14.14, 15.6, 16.17
@pytest.mark.parametrize("code", CONTRACT_UNREGISTERED_CODES)
def test_unregistered_codes_follow_the_contract_fallbacks(code):
    """Unlisted codes read back as ``unknown`` and exit with the safe 3."""
    assert UNKNOWN_ERROR_NAME == CONTRACT_UNKNOWN_ERROR_NAME
    assert error_name_for_code(code) == CONTRACT_UNKNOWN_ERROR_NAME
    assert exit_code_for_error_code(code) == CONTRACT_UNREGISTERED_EXIT_CODE


# --------------------------------------------------------------------------
# Retry constants.
# --------------------------------------------------------------------------


# Feature: m2-security, Property 9: 跨语言常量一致性
# Validates: Requirements 14.14, 15.6, 16.17
def test_single_timeout_and_backoff_cap_match_contract():
    assert DEFAULT_SINGLE_TIMEOUT == CONTRACT_SINGLE_TIMEOUT_SECONDS
    assert MAX_BACKOFF_SECONDS == CONTRACT_MAX_BACKOFF_SECONDS


# Feature: m2-security, Property 9: 跨语言常量一致性
# Validates: Requirements 14.14, 15.6, 16.17
def test_retry_window_defaults_match_contract():
    for kind, seconds in CONTRACT_RETRY_WINDOW_SECONDS_BY_KIND.items():
        assert kind in DEFAULT_RETRY_WINDOW_BY_KIND
        assert DEFAULT_RETRY_WINDOW_BY_KIND[kind] == seconds
    # No kind beyond the three the contract lists, on either side.
    assert set(DEFAULT_RETRY_WINDOW_BY_KIND) == set(
        CONTRACT_RETRY_WINDOW_SECONDS_BY_KIND
    )
    assert {kind.value for kind in CommandKind} == {"child", "control", "query"}


# Feature: m2-security, Property 9: 跨语言常量一致性
# Validates: Requirements 14.14, 15.6, 16.17
def test_retry_window_env_var_name_matches_contract():
    assert ENV_RETRY_WINDOW == CONTRACT_ENV_RETRY_WINDOW


# Feature: m2-security, Property 9: 跨语言常量一致性
# Validates: Requirements 14.14, 15.6, 16.17
@pytest.mark.parametrize("attempt, expected", CONTRACT_BACKOFF_SEQUENCE)
def test_backoff_formula_matches_contract(attempt, expected):
    """``min(2**(n-1), 60)``, cap included, for a long lived child command."""
    assert backoff_seconds(attempt) == expected


# Feature: m2-security, Property 9: 跨语言常量一致性
# Validates: Requirements 14.14, 15.6, 16.17
def test_retryable_status_codes_match_contract():
    for status in CONTRACT_RETRYABLE_STATUS_CODES:
        assert status in RETRYABLE_STATUS_CODES
    # No status the contract does not list may be retried.
    assert set(RETRYABLE_STATUS_CODES) == CONTRACT_RETRYABLE_STATUS_CODES


# Feature: m2-security, Property 9: 跨语言常量一致性
# Validates: Requirements 14.14, 15.6, 16.17
def test_non_retryable_status_codes_match_contract():
    for status in CONTRACT_NON_RETRYABLE_STATUS_CODES:
        assert status in NON_RETRYABLE_EXCEPTION_BY_STATUS
        assert status not in RETRYABLE_STATUS_CODES
    assert set(NON_RETRYABLE_EXCEPTION_BY_STATUS) == (
        CONTRACT_NON_RETRYABLE_STATUS_CODES
    )
