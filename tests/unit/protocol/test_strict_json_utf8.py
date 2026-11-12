"""UTF-8 proof keeps strict JSON rejection and independent decoded values."""

import json

from hypothesis import given, settings, strategies as st
import pytest

from takler.protocol.wire import WireError, strict_json


SCALARS = st.text(st.characters(blacklist_categories=("Cs",)), max_size=30)
VALUES = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False, allow_infinity=False)
    | SCALARS,
    lambda children: (
        st.lists(children, max_size=5) | st.dictionaries(SCALARS, children, max_size=5)
    ),
    max_leaves=20,
)


@settings(max_examples=120, deadline=None)
@given(value=VALUES, ascii_only=st.booleans(), binary=st.booleans())
def test_unicode_values_round_trip_with_both_json_escape_styles(
    value, ascii_only, binary
):
    raw = json.dumps(value, ensure_ascii=ascii_only, allow_nan=False)
    assert strict_json(raw.encode("utf-8") if binary else raw) == value


@pytest.mark.parametrize("binary", [False, True])
@pytest.mark.parametrize(
    "raw,expected",
    [
        (
            '{"值":"α😀","text":"line\\nquote\\"slash\\\\"}',
            {"值": "α😀", "text": 'line\nquote"slash\\'},
        ),
        (r'["\u0041","\u0000","\ud83d\ude00"]', ["A", "\0", "😀"]),
        (r'{"text":"\\uD800"}', {"text": r"\uD800"}),
        (r'{"\u006b":"valid"}', {"k": "valid"}),
    ],
)
def test_valid_utf8_unicode_escapes_and_literal_backslash_u(raw, expected, binary):
    assert strict_json(raw.encode("utf-8") if binary else raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        r'"\ud800"',
        r'"\udfff"',
        r'"\ud800x\udc00"',
        r'"\udc00\ud800"',
        r'{"\ud800":1}',
        r'{"nested":[{"value":"\ud800"}]}',
        r'"\\\ud800"',
        r'"\UD800"',
        '{"secret":"never-echo-sensitive\ud800"}',
        '{"\udfff":"never-echo-sensitive"}',
    ],
)
@pytest.mark.parametrize("binary", [False, True])
def test_invalid_surrogates_still_rejected_in_keys_values_and_nested_lists(raw, binary):
    data = raw.encode("utf-8", errors="surrogatepass") if binary else raw
    with pytest.raises(WireError) as exc:
        strict_json(data)
    assert str(exc.value) == "invalid JSON document"
    assert "never-echo-sensitive" not in str(exc.value)


@pytest.mark.parametrize(
    "raw",
    [
        b"\xff",
        b'{"value":"\xed\xa0\x80"}',
        b'"\xc0\x80"',
        b'{"x":1,"x":2}',
        b'{"outer":{"x":1,"x":2}}',
        rb'{"x":1,"\u0078":2}',
        '{"é":1,"\\u00e9":2}'.encode(),
        b"[NaN]",
        b'{"x":Infinity}',
        b'{"x":-Infinity}',
        b'{"x":1} trailing',
    ],
)
def test_invalid_utf8_duplicates_constants_and_syntax_remain_strict(raw):
    with pytest.raises(WireError):
        strict_json(raw)


def test_custom_input_types_keep_legacy_validation():
    class Text(str):
        def encode(self, *args, **kwargs):
            return b"not a Unicode proof"

    class Binary(bytes):
        def decode(self, *args, **kwargs):
            return '"\ud800"'

    class SpoofedType(type):
        def __eq__(cls, other):
            return True

    class SpoofedBinary(Binary, metaclass=SpoofedType):
        pass

    for raw in [Text('"\ud800"'), Binary(b"{}"), SpoofedBinary(b"{}")]:
        with pytest.raises(WireError):
            strict_json(raw)
    for raw in [
        bytearray(b'{"text":"ok"}'),
        bytearray('{"text":"值"}'.encode("utf-16")),
    ]:
        assert strict_json(raw)["text"] in {"ok", "值"}
    with pytest.raises(WireError):
        strict_json(bytearray(rb'{"text":"\ud800"}'))


@pytest.mark.parametrize("raw", [None, 1, {}, [], memoryview(b"{}")])
def test_unsupported_input_types_still_raise_wire_error(raw):
    with pytest.raises(WireError):
        strict_json(raw)


def test_separate_reads_keep_independent_decoded_containers():
    raw = '{"nodes":[{"path":"/值","status":"active"}]}'.encode()
    first, second = strict_json(raw), strict_json(raw)
    first["nodes"][0]["status"] = "changed"
    assert second["nodes"][0]["status"] == "active"


@pytest.mark.parametrize("depth", [5, 250, 750])
def test_deep_ascii_containers_remain_readable(depth):
    result = strict_json("[" * depth + "0" + "]" * depth)
    for _ in range(depth):
        result = result[0]
    assert result == 0


def test_excessive_depth_remains_rejected():
    with pytest.raises(WireError):
        strict_json("[" * 2000 + "0" + "]" * 2000)
