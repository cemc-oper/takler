"""Shared Python/Go QueryDocument v1 vectors and protobuf carrier."""

import json
from pathlib import Path

import pytest

from takler.protocol.query_v1 import (
    MODEL_BY_KIND,
    decode_query_document,
    encode_query_document,
    query_failure_for_invalid_request,
)
from takler.protocol.wire import WireError
from takler.server.auth import (
    QUERY_PRIVILEGE_BY_METHOD,
    PrivilegeLevel,
    privilege_for_method,
)
from takler.server.protocol import takler_pb2
from takler.server.protocol.query_adapter import query_from_pb2, query_to_pb2


VECTORS = json.loads(
    (Path(__file__).parent / "fixtures" / "query_v1_vectors.json").read_text()
)["cases"]


@pytest.mark.parametrize("case", VECTORS, ids=lambda case: case["id"])
def test_query_v1_shared_vector(case):
    raw = case["raw"].encode()
    if not case["valid"]:
        with pytest.raises(WireError):
            decode_query_document(raw, expected=case["kind"])
        return
    document = decode_query_document(raw, expected=case["kind"])
    assert decode_query_document(encode_query_document(document)) == document
    assert query_from_pb2(query_to_pb2(document), expected=case["kind"]) == document


def test_query_v1_wrong_grpc_kind_rejected():
    document = decode_query_document(VECTORS[0]["raw"])
    with pytest.raises(WireError):
        query_from_pb2(query_to_pb2(document), expected="page_request")


def test_unsupported_version_and_since_are_typed():
    assert (
        query_failure_for_invalid_request(
            b'{"kind":"page_request","schema_version":2}'
        ).code
        == "unsupported_version"
    )
    assert (
        query_failure_for_invalid_request(
            b'{"kind":"since_request","schema_version":1}'
        ).code
        == "unsupported_capability"
    )


def test_query_service_methods_are_explicitly_operator_only():
    service = takler_pb2.DESCRIPTOR.services_by_name["TaklerQuery"]
    declared = {f"/{service.full_name}/{method.name}" for method in service.methods}
    assert declared == set(QUERY_PRIVILEGE_BY_METHOD)
    assert all(
        privilege_for_method(method) is PrivilegeLevel.OPERATOR for method in declared
    )
    assert {method.input_type.name for method in service.methods} == {
        "QueryDocumentPayload"
    }
    assert {method.output_type.name for method in service.methods} == {
        "QueryDocumentPayload"
    }


def test_published_query_schema_matches_models():
    path = (
        Path(__file__).parents[2]
        / "src"
        / "takler"
        / "protocol"
        / "query_v1_schema.json"
    )
    schema = json.loads(path.read_text())
    assert schema["kinds"] == {
        name: model.model_json_schema() for name, model in MODEL_BY_KIND.items()
    }
