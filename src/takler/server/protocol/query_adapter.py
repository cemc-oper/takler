"""The gRPC encoding of the shared R2 QueryDocument v1 JSON contract."""

from takler.protocol.query_v1 import (
    QueryModel,
    decode_query_document,
    encode_query_document,
)
from takler.server.protocol import takler_pb2


def query_to_pb2(document: QueryModel) -> takler_pb2.QueryDocumentPayload:
    return takler_pb2.QueryDocumentPayload(json=encode_query_document(document))


def query_from_pb2(
    message: takler_pb2.QueryDocumentPayload, *, expected: str | None = None
) -> QueryModel:
    return decode_query_document(message.json, expected=expected)
