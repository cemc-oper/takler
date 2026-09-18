"""The shared handler cases driven through the HTTP boundary.

Each case (``tests/server/conftest.py``) is posted to the FastAPI app of the
HTTP transport as an envelope body and answered as an envelope, proving the
HTTP adapter -- URL command, envelope validation, payload parse inside the
handlers' exception boundary, response envelope -- preserves the semantics
the cases pin at the handler level, identically to the gRPC boundary.
"""

import pytest

pytest.importorskip(
    "fastapi", reason="the HTTP transport lives behind the takler[http] extra"
)
pytest.importorskip("httpx", reason="the HTTP test driver posts through httpx")


def test_handler_case_over_http(
    handler_case, run_via_http_fixture, assert_handler_case
):
    response, bunch = run_via_http_fixture(handler_case)
    assert_handler_case(handler_case, response, bunch)
