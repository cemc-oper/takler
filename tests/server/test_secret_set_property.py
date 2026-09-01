"""Property-based test for the hit-independence of Operator_Secret verification.

Covers Property 10 from the m2-security design: for any Operator_Secret_Set
``S`` and any candidate value ``c``,

1. ``CredentialStore.verify_secret(c)`` answers exactly ``c in S``
   (Requirements 7.8, 7.12), and
2. one such call performs exactly ``len(S)`` comparisons -- whatever ``c`` is,
   whether it matches at all, and whichever line of the Operator_Secret_File it
   matches (Requirement 7.9).

The second half is the one that needs a property test. A ``break`` after the
match would leave every functional assertion passing while making the response
time of a verification depend on the matching line, which during a secret
rotation reveals which clients still present the old value. Counting the
comparisons through the injected ``compare`` seam is what makes that
observable, so the missing ``break`` cannot be "optimized" back in later.

What the generators have to respect
-----------------------------------

``S`` is not the list of lines written to the file, it is what
``parse_credential_lines`` makes of them: blank lines and ``#`` comments are
dropped, every line is stripped, and duplicates collapse into a set. For
``c in S`` to be a meaningful expectation, a generated secret must therefore
survive that parsing verbatim -- no leading or trailing whitespace, no line
boundary anywhere inside it, and no leading ``#``. The alphabet below holds no
whitespace at all except the interior spaces the explicit ``@example`` cases
add, which keeps the round trip exact while still covering the punctuation and
non-ASCII text a real secret file can hold.

Candidates are drawn so that hits are frequent rather than accidental: a
uniformly random string would practically never match, and the whole point is
comparing a hit at the first line, a hit at the last line and a miss against
each other. Near-misses (a prefix of a secret, a secret with one character
appended) are sampled explicitly, since those are what a short-circuiting
comparison would answer faster.

No assertion message and no test id carries a secret or a candidate value: the
assertions compare booleans and counts only, so a failure report names no
credential.
"""

from __future__ import annotations

import hmac
import string
import tempfile
from collections import Counter
from pathlib import Path
from typing import List, Optional, Sequence

from hypothesis import example, given, settings
from hypothesis import strategies as st

from takler.server.auth import CredentialStore, parse_credential_lines

#: Characters a generated secret is built from. Deliberately without any
#: whitespace: a value with leading or trailing whitespace would be stripped by
#: the parsing, and the assertion ``verify_secret(c) == (c in S)`` compares
#: against the parsed set. ``#`` is present because it is only special at the
#: start of a line, which the composite strategy below rules out separately.
_SECRET_CHARS = string.ascii_letters + string.digits + "_-.+/=@:#!$%*"

#: Non-ASCII secrets are legal -- the file is read as text and the values are
#: encoded to UTF-8 before comparison -- so the alphabet includes some.
_NON_ASCII_SECRETS = (
    "密钥轮换第一轮",
    "運用パスワード",
    "clé-secrète",
    "Ω-secret",
)


@st.composite
def _secret_values(draw: st.DrawFn) -> str:
    """One value that survives ``parse_credential_lines`` unchanged."""
    value = draw(
        st.one_of(
            st.text(alphabet=_SECRET_CHARS, min_size=1, max_size=24),
            st.sampled_from(_NON_ASCII_SECRETS),
        )
    )
    # A line whose first non-blank character is ``#`` is a comment and would
    # never enter the set, so it cannot be used as a member of ``S``.
    return value if not value.startswith("#") else f"s{value}"


@st.composite
def _secret_sets(draw: st.DrawFn) -> List[str]:
    """A list of distinct secrets, in the order they are written to the file.

    Empty sets are included: an Operator_Secret_File holding nothing but
    comments verifies no candidate and performs no comparison at all, which is
    the fail-closed edge of the same property.
    """
    return draw(
        st.lists(_secret_values(), min_size=0, max_size=6, unique=True),
    )


@st.composite
def _candidates(draw: st.DrawFn, secrets: Sequence[str]) -> str:
    """A candidate value, biased towards hits and near-misses.

    When ``secrets`` is non-empty the candidate is a member with high
    probability, and otherwise something close to one: a prefix, a member with
    a character appended, or a member with its case swapped. Those are exactly
    the inputs whose verification cost a short-circuiting comparison would let
    differ.
    """
    if not secrets:
        return draw(st.text(alphabet=_SECRET_CHARS, min_size=0, max_size=8))

    member = draw(st.sampled_from(list(secrets)))
    return draw(
        st.one_of(
            # A hit, at whichever line of the file this member sits on.
            st.just(member),
            st.sampled_from(list(secrets)),
            # Near-misses.
            st.just(member[:-1]),
            st.just(member + "x"),
            st.just(member.swapcase()),
            st.just(f" {member}"),
            st.just(""),
            # And a plain unrelated value.
            st.text(alphabet=_SECRET_CHARS, min_size=0, max_size=12),
        )
    )


class _CountingComparison:
    """A stand-in for ``compare_secret_values`` that records every call.

    It keeps the real :func:`hmac.compare_digest` verdict, so substituting it
    changes only what the test can observe, not what ``verify_secret`` answers.
    The recorded operands are the encoded Operator_Secrets, used to assert that
    each one was compared exactly once; they never reach an assertion message.
    """

    def __init__(self) -> None:
        self.compared: List[bytes] = []

    def __call__(self, candidate: bytes, secret: bytes) -> bool:
        self.compared.append(secret)
        return hmac.compare_digest(candidate, secret)


def _write_secret_file(directory: Path, secrets: Sequence[str]) -> Path:
    """Write ``secrets`` to a secret file, interleaved with noise lines.

    Blank lines and comments surround the values so that the set the store
    parses is exercised through the real file format rather than through a
    hand-built set, and so that a member's line number bears no relation to its
    index.
    """
    lines: List[str] = ["# operator secret file", ""]
    for index, secret in enumerate(secrets):
        lines.append(secret)
        if index % 2 == 0:
            lines.append("")
        else:
            lines.append(f"# rotation round {index}")
    path = directory / "operator_secret"
    path.write_text("\n".join(lines) + "\n")
    return path


@st.composite
def _secret_set_and_candidate(draw: st.DrawFn) -> tuple:
    """Draw an Operator_Secret_Set together with a candidate for it."""
    secrets = draw(_secret_sets())
    candidate = draw(_candidates(secrets))
    return secrets, candidate


def _check_hit_independence(secrets: Sequence[str], candidate: Optional[str]) -> None:
    """Assert both halves of Property 10 for one set and one candidate."""
    with tempfile.TemporaryDirectory() as raw_directory:
        directory = Path(raw_directory)
        secret_file = _write_secret_file(directory, secrets)
        store = CredentialStore(secret_file=secret_file)

        # The expectation is the *parsed* set, which is what the store compares
        # against. It equals ``set(secrets)`` for every value the generators
        # produce; deriving it through the parser rather than asserting that
        # equality keeps the property about verification instead of parsing.
        parsed = parse_credential_lines(secret_file.read_text())
        expected = candidate is not None and candidate in parsed

        comparison = _CountingComparison()
        result = store.verify_secret(candidate, compare=comparison)

        # 1. The verdict is set membership, nothing more and nothing less.
        assert result is expected

        # 2. Exactly ``|S|`` comparisons, each Operator_Secret compared once,
        # independent of whether and where the candidate matched. Only counts
        # are asserted, so a failure report holds no credential value.
        counts = Counter(comparison.compared)
        assert len(comparison.compared) == len(parsed)
        assert len(counts) == len(parsed)
        assert all(count == 1 for count in counts.values())


# Feature: m2-security, Property 10: 密钥集合校验的命中无关性
# Validates: Requirements 7.8, 7.9, 7.12
@settings(max_examples=100, deadline=None)
@given(case=_secret_set_and_candidate())
# A hit on the first line and a hit on the last line of the same three-value
# file: the two must cost the same three comparisons. This is the rotation case
# of Requirement 7.12 -- old and new secret valid at once -- and the pair a
# ``break`` would separate.
@example(case=(["old-secret", "current-secret", "new-secret"], "old-secret"))
@example(case=(["old-secret", "current-secret", "new-secret"], "new-secret"))
# A miss against the same file: same cost again.
@example(case=(["old-secret", "current-secret", "new-secret"], "guessed-secret"))
# A candidate sharing a long prefix with a member: what a byte-wise
# short-circuit inside one comparison would answer faster.
@example(case=(["current-secret"], "current-secre"))
# Interior whitespace survives the parsing, surrounding whitespace does not.
@example(case=(["secret with spaces"], "secret with spaces"))
@example(case=(["secret with spaces"], " secret with spaces "))
# An empty Operator_Secret_Set accepts nothing and compares nothing.
@example(case=([], "any-candidate"))
@example(case=([], ""))
# Non-ASCII values, which are encoded before being compared.
@example(case=(["密钥轮换第一轮", "運用パスワード"], "運用パスワード"))
def test_verify_secret_is_membership_at_constant_comparison_count(case: tuple) -> None:
    """``verify_secret`` answers set membership in ``|S|`` comparisons.

    Two assertions per example, both from Property 10:

    1. the verdict equals ``candidate in S`` (Requirements 7.8, 7.12);
    2. the number of comparisons equals ``|S|`` and every Operator_Secret is
       compared exactly once, regardless of whether the candidate matched and
       of which line it matched on (Requirement 7.9).
    """
    secrets, candidate = case
    _check_hit_independence(secrets, candidate)
