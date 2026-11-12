"""Property-based test for the Audit_Record JSON Lines round trip.

Covers Property 5 from the m2-security design: for any Audit_Record field
values -- node paths and user names holding quotes, backslashes, line breaks
and non-ASCII text -- ``AuditRecord.to_json_line()`` returns text that holds no
line boundary at all, that ``json.loads`` parses, and whose result carries the
eight keys of Requirement 11.5 with the values that went in (Requirements
11.10, 11.5).

What the generators have to contain, and why
--------------------------------------------

``json.dumps`` escapes every character below U+0020, so a ``\\n``, ``\\r``,
``\\v`` or ``\\f`` inside a node path is already safe and a test built only from
those would pass without exercising anything. The interesting characters are
the three that ``ensure_ascii=False`` leaves raw even though Python's
``str.splitlines`` treats them as line boundaries: NEXT LINE (U+0085), LINE
SEPARATOR (U+2028) and PARAGRAPH SEPARATOR (U+2029). ``to_json_line`` escapes
those explicitly, and they are what makes the "exactly one line" assertion a
real assertion. The generators sample them directly, and the ``@example`` cases
pin them down deterministically rather than leaving them to chance.

That is also why the single-line check is ``len(line.splitlines()) == 1`` and
not ``"\\n" not in line``: the latter is blind to exactly the three characters
this property exists to protect against.

The last assertion is the flip side of the same choice: a non-ASCII value must
come out of ``to_json_line`` as readable text, not as a ``\\uXXXX`` escape.
Node paths in this project are routinely Chinese, and an audit file full of
escapes would be unreadable for the operator it exists to serve.
"""

from __future__ import annotations

import json
import string
from typing import List

from hypothesis import example, given, settings
from hypothesis import strategies as st

from takler.server.audit import (
    DENIED_ERROR_CODE,
    EVENT_CONTROL,
    EVENT_DENIED,
    EVENT_ZOMBIE,
    OUTCOME_DENIED,
    OUTCOME_ERROR,
    OUTCOME_SUCCESS,
    OUTCOME_ZOMBIE,
    UNKNOWN_USER,
    AuditRecord,
)

# The eight keys Requirement 11.5 lists. Spelled out here rather than derived
# from the dataclass fields, so that dropping or renaming a field fails this
# test instead of silently redefining what it checks.
_AUDIT_RECORD_KEYS = frozenset(
    {
        "timestamp",
        "event",
        "command",
        "user",
        "peer",
        "target",
        "outcome",
        "error_code",
    }
)

#: The three characters that ``json.dumps(..., ensure_ascii=False)`` emits raw
#: while ``str.splitlines`` counts them as line boundaries. ``to_json_line`` has
#: to escape them; the test asserts both halves of that (they are absent from
#: the line, and their escape is present).
_ESCAPED_LINE_BOUNDARIES = ("\u0085", "\u2028", "\u2029")

#: Characters ``str.splitlines`` treats as line boundaries. The ones below
#: U+0020 are escaped by ``json.dumps`` itself; the last three are not, and are
#: the reason this property exists.
_LINE_BOUNDARY_CHARS = (
    "\n",
    "\r",
    "\r\n",
    "\v",
    "\f",
    "\x1c",
    "\x1d",
    "\x1e",
    *_ESCAPED_LINE_BOUNDARIES,
)

#: Characters that carry meaning inside JSON text and must therefore survive a
#: round trip unchanged.
_STRUCTURAL_CHARS = ('"', "\\", "'", "{", "}", "[", "]", ":", ",", "/")

#: Non-ASCII fragments. Chinese is the realistic case for this project's node
#: paths and user names; the rest widen the alphabet without widening the intent.
_NON_ASCII_FRAGMENTS = (
    "流程",
    "任务",
    "节点路径",
    "作业失败",
    "王大牛",
    "気温",
    "naïve",
    "Ω",
)

#: Ordinary path/name characters, so a generated value looks like something a
#: real deployment could produce and is not pure punctuation.
_PLAIN_CHARS = string.ascii_letters + string.digits + "_-."

# One piece of a hostile string. ``st.characters`` widens the alphabet to the
# whole of Unicode; surrogates are excluded because they cannot be encoded to
# UTF-8 and an Audit_File is a UTF-8 file, so they are not a state the system
# can reach.
_hostile_fragments = st.one_of(
    st.sampled_from(_LINE_BOUNDARY_CHARS),
    st.sampled_from(_STRUCTURAL_CHARS),
    st.sampled_from(_NON_ASCII_FRAGMENTS),
    st.text(alphabet=_PLAIN_CHARS, min_size=1, max_size=8),
    st.characters(exclude_categories=("Cs",)),
)


def _hostile_text(min_size: int = 0, max_size: int = 5) -> st.SearchStrategy[str]:
    """Text built from hostile fragments: quotes, line breaks, non-ASCII."""
    return st.lists(_hostile_fragments, min_size=min_size, max_size=max_size).map(
        "".join
    )


def _hostile_node_paths() -> st.SearchStrategy[str]:
    """Absolute node paths whose segments hold hostile characters."""
    return st.lists(_hostile_text(min_size=1, max_size=3), min_size=1, max_size=3).map(
        lambda segments: "/" + "/".join(segments)
    )


@st.composite
def _audit_records(draw: st.DrawFn) -> AuditRecord:
    """Build an ``AuditRecord`` with hostile ``user`` and ``target`` values.

    ``event`` and ``outcome`` are drawn from their fixed vocabularies
    (Requirements 11.6, 11.7) because those are the only values a record point
    can produce. ``timestamp``, ``command`` and ``peer`` mix realistic values
    with hostile text: nothing in the schema constrains them, and a command
    name or peer address is echoed from the wire.
    """
    return AuditRecord(
        timestamp=draw(
            st.one_of(
                st.sampled_from(["2026-07-15T10:30:00", "2026-07-15T10:30:00.123456"]),
                _hostile_text(),
            )
        ),
        event=draw(st.sampled_from([EVENT_CONTROL, EVENT_DENIED, EVENT_ZOMBIE])),
        command=draw(
            st.one_of(
                st.sampled_from(["requeue", "suspend", "run", "complete", "init"]),
                _hostile_text(),
            )
        ),
        user=draw(st.one_of(st.just(UNKNOWN_USER), _hostile_text())),
        peer=draw(
            st.one_of(
                st.sampled_from(["ipv4:10.0.0.9:51234", "unix:/tmp/takler.sock", ""]),
                _hostile_text(),
            )
        ),
        target=draw(st.lists(_hostile_node_paths(), min_size=0, max_size=3)),
        outcome=draw(
            st.sampled_from(
                [OUTCOME_SUCCESS, OUTCOME_ERROR, OUTCOME_DENIED, OUTCOME_ZOMBIE]
            )
        ),
        error_code=draw(
            st.one_of(
                st.sampled_from([0, 31, DENIED_ERROR_CODE, 99]),
                st.integers(min_value=-1000, max_value=1000),
            )
        ),
    )


def _text_values(record: AuditRecord) -> List[str]:
    """Every string value of ``record``, ``target`` entries included."""
    return [
        record.timestamp,
        record.event,
        record.command,
        record.user,
        record.peer,
        record.outcome,
        *record.target,
    ]


# Feature: m2-security, Property 5: Audit_Record 的 JSON Lines 往返
# Validates: Requirements 11.10, 11.5
@settings(max_examples=100, deadline=None)
@given(record=_audit_records())
# The three characters that survive ``ensure_ascii=False`` raw and that
# ``str.splitlines`` nonetheless breaks on, each in a place a real deployment
# could put it: a node path, a user name, a command name.
@example(
    record=AuditRecord(
        timestamp="2026-07-15T10:30:00",
        event=EVENT_CONTROL,
        command="requeue\u0085",
        user="王大牛\u2028root",
        peer="ipv4:10.0.0.9:51234",
        target=['/流程1/家族\u2029/任务 "1"', "/flow1\\task\u0085"],
        outcome=OUTCOME_SUCCESS,
        error_code=0,
    )
)
# A quote, a backslash and a bare newline in a node path: the ASCII half of the
# same failure mode.
@example(
    record=AuditRecord(
        timestamp="2026-07-15T10:30:00.123456",
        event=EVENT_ZOMBIE,
        command="complete",
        user=UNKNOWN_USER,
        peer="",
        target=['/flow1/"task"\n/sub', "/flow1/task\\1"],
        outcome=OUTCOME_ZOMBIE,
        error_code=31,
    )
)
# A purely Chinese record: the readability half of the property.
@example(
    record=AuditRecord(
        timestamp="2026-07-15T10:30:00",
        event=EVENT_DENIED,
        command="suspend",
        user="王大牛",
        peer="ipv4:10.0.0.9:51234",
        target=["/流程1/家族1/任务1"],
        outcome=OUTCOME_DENIED,
        error_code=DENIED_ERROR_CODE,
    )
)
def test_audit_record_json_line_round_trip(record: AuditRecord) -> None:
    """``to_json_line()`` is one line, and parses back to the same eight keys.

    Three things are asserted for every generated record:

    1. the output holds no line boundary of any kind, so appending it plus one
       newline adds exactly one line to the Audit_File;
    2. ``json.loads`` succeeds and returns an object whose key set is exactly
       the eight keys of Requirement 11.5, each field equal to what went in
       (the round trip of Requirement 11.10);
    3. non-ASCII characters appear as themselves rather than as ``\\uXXXX``
       escapes, so a Chinese node path stays readable in the audit file.
    """
    line = record.to_json_line()

    # 1. Exactly one line. ``splitlines`` rather than a ``"\n" not in line``
    # check, because U+0085 / U+2028 / U+2029 break a line for Python and for
    # line-oriented readers while passing a newline check unnoticed.
    assert len(line.splitlines()) == 1

    for char in _ESCAPED_LINE_BOUNDARIES:
        assert char not in line
        if any(char in value for value in _text_values(record)):
            # Escaped, not dropped: the value has to come back intact below.
            assert f"\\u{ord(char):04x}" in line

    # 2. Parses back to the eight keys, with every field preserved.
    parsed = json.loads(line)

    assert isinstance(parsed, dict)
    assert set(parsed) == set(_AUDIT_RECORD_KEYS)

    assert parsed["timestamp"] == record.timestamp
    assert parsed["event"] == record.event
    assert parsed["command"] == record.command
    assert parsed["user"] == record.user
    assert parsed["peer"] == record.peer
    assert parsed["target"] == record.target
    assert parsed["outcome"] == record.outcome
    assert parsed["error_code"] == record.error_code

    # 3. Non-ASCII text stays readable. Every non-ASCII character of every
    # field appears literally in the line, which is what ``ensure_ascii=False``
    # buys and what a ``\uXXXX`` escape would take away. The three characters
    # checked above are the deliberate exception.
    for value in _text_values(record):
        for char in value:
            if ord(char) < 0x80 or char in _ESCAPED_LINE_BOUNDARIES:
                continue
            assert char in line
