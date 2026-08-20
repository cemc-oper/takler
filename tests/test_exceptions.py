"""Unit tests for the takler exception hierarchy (``takler/exceptions.py``).

These tests pin down the shape of the hierarchy rather than any behaviour:
callers are expected to catch ``TaklerError`` for every deliberate takler
failure, and the transitional ``ValueError`` bases must stay in place while
existing call sites still use ``except ValueError``.
"""

import pytest

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

# The six subclasses Requirement 1.2 names explicitly.
REQUIRED_SUBCLASSES = [
    NodeNotFoundError,
    InvalidNodePathError,
    ExpressionSyntaxError,
    JobSubmissionError,
    ZombieError,
    ClientConnectionError,
]

# Every other type the module exports, so a future addition that forgets to
# derive from TaklerError is caught here too.
OTHER_EXCEPTIONS = [
    InvalidRequestError,
    NodeTypeError,
    UnsupportedValueError,
    FlowStateError,
    TransportError,
    ServerResponseError,
    PermissionDeniedError,
]


class TestTaklerErrorBase:
    """Requirement 1.1: ``TaklerError`` is the base and derives from ``Exception``."""

    def test_takler_error_subclasses_exception(self):
        assert issubclass(TaklerError, Exception)

    def test_takler_error_is_raisable_and_catchable_as_exception(self):
        with pytest.raises(Exception):
            raise TaklerError("boom")


class TestRequiredSubclasses:
    """Requirement 1.2: the six named subclasses all derive from ``TaklerError``."""

    @pytest.mark.parametrize(
        "exc_type", REQUIRED_SUBCLASSES, ids=lambda t: t.__name__
    )
    def test_required_subclass_derives_from_takler_error(self, exc_type):
        assert issubclass(exc_type, TaklerError)

    @pytest.mark.parametrize(
        "exc_type", REQUIRED_SUBCLASSES, ids=lambda t: t.__name__
    )
    def test_required_subclass_is_caught_as_takler_error(self, exc_type):
        with pytest.raises(TaklerError):
            raise exc_type("boom")

    def test_required_subclasses_are_distinct_types(self):
        assert len(set(REQUIRED_SUBCLASSES)) == len(REQUIRED_SUBCLASSES)

    @pytest.mark.parametrize(
        "exc_type", OTHER_EXCEPTIONS, ids=lambda t: t.__name__
    )
    def test_other_exported_exceptions_also_derive_from_takler_error(self, exc_type):
        assert issubclass(exc_type, TaklerError)


class TestValueErrorCompatibility:
    """Requirement 1.2 / transitional compatibility with ``except ValueError``."""

    @pytest.mark.parametrize(
        "exc_type",
        [InvalidRequestError, ExpressionSyntaxError],
        ids=lambda t: t.__name__,
    )
    def test_transitional_types_subclass_value_error(self, exc_type):
        assert issubclass(exc_type, ValueError)

    @pytest.mark.parametrize(
        "exc_type",
        [
            InvalidRequestError,
            NodeNotFoundError,
            InvalidNodePathError,
            NodeTypeError,
            UnsupportedValueError,
            FlowStateError,
            ExpressionSyntaxError,
        ],
        ids=lambda t: t.__name__,
    )
    def test_request_errors_are_caught_by_legacy_value_error_handlers(self, exc_type):
        with pytest.raises(ValueError):
            raise exc_type("boom")

    @pytest.mark.parametrize(
        "exc_type",
        [
            JobSubmissionError,
            ZombieError,
            TransportError,
            ClientConnectionError,
            ServerResponseError,
            PermissionDeniedError,
        ],
        ids=lambda t: t.__name__,
    )
    def test_non_request_errors_are_not_value_errors(self, exc_type):
        assert not issubclass(exc_type, ValueError)


class TestLoggingErrorsStillImportable:
    """Requirement 1.5: ``InvalidLogLevelError`` keeps its import path and base."""

    def test_invalid_log_level_error_import_path_unchanged(self):
        from takler.logging.errors import InvalidLogLevelError

        assert issubclass(InvalidLogLevelError, ValueError)

    def test_invalid_log_level_error_is_caught_as_value_error(self):
        from takler.logging.errors import InvalidLogLevelError

        with pytest.raises(ValueError):
            raise InvalidLogLevelError("nope")
