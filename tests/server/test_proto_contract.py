"""Contract tests for ``ServiceResponse`` in ``takler.proto``.

M1 carries the Error_Code on the existing ``ServiceResponse.flag`` field
instead of adding a new protobuf field. That decision only stays wire
compatible as long as the message keeps exactly its two original fields with
their original field numbers, so a client built against a pre-M1 ``.proto``
(the legacy Go client, which only tests ``flag != 0``) keeps working.

The trade-off of reusing a field whose name does not describe its new meaning
is paid back by a comment in the ``.proto`` pointing at the Error_Code table.
Both halves are asserted here: the descriptor pins the shape of the message,
and the ``.proto`` text pins the comment.

Validates: Requirements 3.2, 3.10, 3.11
"""

from __future__ import annotations

from pathlib import Path

import takler
from takler.server.protocol import takler_pb2


PROTO_PATH = Path(takler.__file__).parent / "server" / "protocol" / "takler.proto"


def _proto_text() -> str:
    return PROTO_PATH.read_text(encoding="utf-8")


def _service_response_block(text: str) -> str:
    """Return the body of ``message ServiceResponse`` including its comments."""
    start = text.index("message ServiceResponse")
    end = text.index("}", start)
    return text[start : end + 1]


# ---------------------------------------------------------------------------
# Message shape (Requirements 3.2, 3.10)
# ---------------------------------------------------------------------------


def test_service_response_has_exactly_two_fields():
    """No field was added to or removed from ``ServiceResponse``."""
    fields = takler_pb2.ServiceResponse.DESCRIPTOR.fields

    assert len(fields) == 2
    assert [f.name for f in fields] == ["flag", "message"]


def test_service_response_field_numbers_are_frozen():
    """``flag`` stays 1 and ``message`` stays 2, keeping the wire format."""
    descriptor = takler_pb2.ServiceResponse.DESCRIPTOR

    assert descriptor.fields_by_name["flag"].number == 1
    assert descriptor.fields_by_name["message"].number == 2


def test_service_response_field_types_are_unchanged():
    """``flag`` is still an int32 and ``message`` still a string."""
    descriptor = takler_pb2.ServiceResponse.DESCRIPTOR
    flag = descriptor.fields_by_name["flag"]
    message = descriptor.fields_by_name["message"]

    assert flag.type == flag.TYPE_INT32
    assert message.type == message.TYPE_STRING


def test_service_response_default_flag_is_success():
    """An unset ``flag`` reads back as 0, i.e. success."""
    response = takler_pb2.ServiceResponse()

    assert response.flag == 0
    assert response.message == ""


# ---------------------------------------------------------------------------
# Proto text and comment (Requirements 3.2, 3.11)
# ---------------------------------------------------------------------------


def test_proto_file_is_shipped_with_the_package():
    """The ``.proto`` lives inside the installed package, next to the stubs."""
    assert PROTO_PATH.is_file()


def test_flag_field_carries_a_comment():
    """The ``flag`` declaration is preceded by at least one comment line."""
    block = _service_response_block(_proto_text())
    lines = [line.strip() for line in block.splitlines()]
    flag_index = next(
        i for i, line in enumerate(lines) if line.startswith("int32 flag = 1;")
    )

    comment_lines = []
    for line in reversed(lines[:flag_index]):
        if line.startswith("//"):
            comment_lines.append(line)
        else:
            break

    assert comment_lines, "no comment found above the flag field"


def test_flag_comment_points_at_the_error_code_table():
    """The comment explains the error classification and where to find it."""
    block = _service_response_block(_proto_text())

    assert "Error_Code" in block
    assert "takler/server/protocol/error_code.py" in block
    assert "ERROR_NAME_BY_CODE" in block


def test_service_response_declares_only_the_two_known_fields_in_text():
    """The ``.proto`` text itself declares no extra ``ServiceResponse`` field."""
    block = _service_response_block(_proto_text())
    declarations = [
        line.strip()
        for line in block.splitlines()
        if line.strip().endswith(";") and not line.strip().startswith("//")
    ]

    assert declarations == ["int32 flag = 1;", "string message = 2;"]
