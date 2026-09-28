import copy
import json
from pathlib import Path

import pytest

from takler.exceptions import ServerResponseError
from takler.protocol.commands import BATCH_COMMANDS, Command
from takler.protocol.envelope import Envelope
from takler.protocol.batch import validate_batch_response
from takler.protocol.wire import (
    SCHEMA,
    WireError,
    decode_envelope,
    is_json_content_type,
)

VECTORS = json.loads(
    (Path(__file__).parent / "fixtures/http_vectors.json").read_text()
)["cases"]


@pytest.mark.parametrize("case", VECTORS, ids=lambda c: c["id"])
def test_shared_wire_vectors(case):
    valid = False
    try:
        request = case["request"]
        decode_envelope(
            case["raw_body"],
            direction=case["direction"],
            command=request["command"] if case["direction"] == "response" else None,
            trace_id=request["trace_id"] if case["direction"] == "response" else None,
        )
        if (
            case["direction"] == "response"
            and Command(request["command"]) in BATCH_COMMANDS
        ):
            envelope = Envelope.model_validate(json.loads(case["raw_body"]))
            validate_batch_response(
                envelope.command, request["payload"], envelope.parse_response()
            )
        valid = is_json_content_type(case["content_type"])
        if case["direction"] == "request":
            valid = valid and json.loads(case["raw_body"])["command"] == case["command"]
    except (WireError, ServerResponseError):
        pass
    assert valid == case["expected_wire_valid"]


@pytest.mark.parametrize("direction", ["request", "response"])
def test_each_field_rejects_missing_null_and_unknown(direction):
    seeds = {
        c["command"]: c
        for c in VECTORS
        if c["direction"] == direction and c["expected_wire_valid"]
    }
    if direction == "response":
        for command in SCHEMA[direction]:
            if command not in seeds:
                template = seeds[
                    "suspend" if "results" in SCHEMA[direction][command] else "complete"
                ]
                seeds[command] = template
    assert set(seeds) == set(SCHEMA[direction])
    for command, case in seeds.items():
        original = json.loads(case["raw_body"])
        original["command"] = command
        for key in original["payload"]:
            for missing in (True, False):
                value = copy.deepcopy(original)
                if missing:
                    del value["payload"][key]
                else:
                    value["payload"][key] = None
                with pytest.raises(WireError):
                    decode_envelope(json.dumps(value), direction=direction)
        original["payload"]["UNKNOWN"] = 1
        with pytest.raises(WireError):
            decode_envelope(json.dumps(original), direction=direction)


@pytest.mark.parametrize(
    "value", ["0", "-1", "9223372036854775807", "-9223372036854775808"]
)
def test_grpc_meter_accepts_canonical_int64(value):
    from takler.server.protocol import adapter, takler_pb2

    assert adapter.request_from_pb2(
        Command.METER, takler_pb2.MeterCommand(meter_value=value)
    ).meter_value == int(value)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "01",
        "-0",
        "+1",
        " 1",
        "1 ",
        "1.0",
        "1e0",
        "１",
        "9223372036854775808",
        "-9223372036854775809",
    ],
)
def test_grpc_meter_rejects_noncanonical_int64(value):
    from takler.server.protocol import adapter, takler_pb2

    with pytest.raises(WireError):
        adapter.request_from_pb2(
            Command.METER, takler_pb2.MeterCommand(meter_value=value)
        )
