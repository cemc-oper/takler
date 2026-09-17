"""The shared handler cases driven through the gRPC boundary.

Each case (``tests/server/conftest.py``) is built as a pb2 request and invoked
on :class:`~takler.server.network_service.TaklerService`, proving the gRPC
adapter (pb2 <-> DTO conversion plus dispatch) preserves the semantics the
cases pin at the handler level. The HTTP transport of task 7 reuses the same
cases for its own boundary.
"""


def test_handler_case_over_grpc(
    handler_case, run_via_grpc_fixture, assert_handler_case
):
    response, bunch = run_via_grpc_fixture(handler_case)
    assert_handler_case(handler_case, response, bunch)
