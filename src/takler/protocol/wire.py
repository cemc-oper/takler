"""Strict HTTP wire decoding, separate from convenient local DTO defaults."""

import base64
import json
import re
from importlib.resources import files

from .error_code import ERROR_NAME_BY_CODE

SCHEMA = json.loads(files("takler.protocol").joinpath("wire_schema.json").read_text())
READ_ONLY = frozenset({"ping", "show", "coroutine"})


class WireError(ValueError):
    """Safe diagnostics never include input values."""


def is_json_content_type(value):
    return (
        re.fullmatch(
            r'application/json(?:[ \t]*;[ \t]*charset[ \t]*=[ \t]*(?:utf-8|"utf-8"))?',
            value.strip(" \t"),
            re.IGNORECASE,
        )
        is not None
    )


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise WireError("duplicate JSON key")
            result[key] = value
        return result

    def invalid(_):
        raise WireError("invalid JSON constant")

    def strings(value):
        if isinstance(value, str):
            value.encode("utf-8")
        elif isinstance(value, dict):
            for k, v in value.items():
                strings(k)
                strings(v)
        elif isinstance(value, list):
            for v in value:
                strings(v)

    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        result = json.loads(text, object_pairs_hook=pairs, parse_constant=invalid)
        strings(result)
        return result
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise WireError("invalid JSON document") from None


def decimal(value):
    if type(value) is not str or not re.fullmatch(r"0|-?[1-9][0-9]*", value):
        raise WireError("invalid decimal integer")
    if len(value) > 20 or not -(2**63) <= int(value) < 2**63:
        raise WireError("decimal integer out of range")
    return value


def validate_object(value, schema):
    if type(value) is not dict or value.keys() != schema.keys():
        raise WireError("missing or unknown fields")
    for key, kind in schema.items():
        v = value[key]
        if kind == "b":
            valid = type(v) is bool
        elif kind == "as":
            valid = type(v) is list and all(type(i) is str for i in v)
        elif kind in {"flag", "uint32"}:
            valid = type(v) is int and (
                v in ERROR_NAME_BY_CODE if kind == "flag" else 0 <= v < 2**32
            )
        elif kind in {"items", "coroutines"}:
            valid = type(v) is list
            if valid:
                for item in v:
                    validate_object(
                        item, SCHEMA["item" if kind == "items" else "coroutine"]
                    )
        else:
            valid = type(v) is str
            if valid and kind == "decimal":
                decimal(v)
            elif valid and kind == "base64":
                try:
                    valid = (
                        base64.b64encode(base64.b64decode(v, validate=True)).decode(
                            "ascii"
                        )
                        == v
                    )
                except (ValueError, UnicodeError):
                    valid = False
            elif valid and kind in {"state", "dep", "effect"}:
                valid = (
                    v
                    in {
                        "state": {
                            "unknown",
                            "complete",
                            "queued",
                            "submitted",
                            "active",
                            "aborted",
                            "clear",
                            "set",
                        },
                        "dep": {"all", "trigger", "time"},
                        "effect": {"none", "applied", "partial", "unknown"},
                    }[kind]
                )
        if not valid:
            raise WireError("invalid field type or value")


def decode_envelope(raw, *, direction, command=None, trace_id=None):
    value = strict_json(raw)
    if type(value) is not dict:
        raise WireError("envelope must be an object")
    required = {"version", "command", "trace_id", "payload"}
    if not required <= value.keys() or value.keys() - required - {"auth", "target"}:
        raise WireError("missing or unknown envelope fields")
    if value.get("auth") is not None or value.get("target") is not None:
        raise WireError("reserved envelope field must be null")
    if (
        value["version"] != "1"
        or type(value["command"]) is not str
        or value["command"] not in SCHEMA[direction]
    ):
        raise WireError("unsupported envelope version or command")
    if type(value["trace_id"]) is not str or not re.fullmatch(
        "[0-9a-f]{32}", value["trace_id"]
    ):
        raise WireError("invalid trace id")
    if command is not None and value["command"] != command:
        raise WireError("response command mismatch")
    if trace_id is not None and value["trace_id"] != trace_id:
        raise WireError("response trace id mismatch")
    validate_object(value["payload"], SCHEMA[direction][value["command"]])
    return value
