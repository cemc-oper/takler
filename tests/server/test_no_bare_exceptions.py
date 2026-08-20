"""Static scan guarding the Exception_Hierarchy migration of tasks 3.1-3.3.

Requirements 2.7 and 2.8 are stated as properties of the *source files* rather
than of any single call: once the migration is done, ``scheduler.py`` must no
longer raise bare ``ValueError`` / ``RuntimeError``, and ``node.py`` /
``bunch.py`` must no longer raise bare ``ValueError``. Behavioural tests can
only cover the paths they happen to exercise, so the check is done by parsing
the modules with :mod:`ast` and walking every ``raise`` statement.

Note the deliberate asymmetry: ``node.py`` keeps a few ``RuntimeError`` raises
(duplicate ``add_event`` / ``add_limit``, ``resolve_time_dependencies`` on a
node outside a flow). The design records those as known items outside the scope
of requirement 2.8, which only speaks about ``ValueError``, so this test does
not assert against them.

Validates: Requirements 2.7, 2.8
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterator, List, Tuple

import pytest

import takler

PACKAGE_ROOT = Path(takler.__file__).parent

SCHEDULER = "server/scheduler.py"
NODE = "core/node.py"
BUNCH = "core/bunch.py"

#: ``relative source path -> exception names that must not be raised directly``.
FORBIDDEN_RAISES = {
    SCHEDULER: ("ValueError", "RuntimeError"),
    NODE: ("ValueError",),
    BUNCH: ("ValueError",),
}


def _raised_exception_name(node: ast.Raise) -> str | None:
    """Return the name of the exception type raised by ``node``.

    Handles ``raise Foo(...)``, ``raise Foo`` and ``raise mod.Foo(...)``. A bare
    ``raise`` (re-raise inside an ``except`` block) has no type to report and
    yields ``None``.
    """
    exc = node.exc
    if exc is None:  # bare ``raise`` inside an except block
        return None
    if isinstance(exc, ast.Call):
        exc = exc.func
    if isinstance(exc, ast.Name):
        return exc.id
    if isinstance(exc, ast.Attribute):
        return exc.attr
    return None


def _iter_raises(source_path: Path) -> Iterator[Tuple[str, int]]:
    """Yield ``(exception name, line number)`` for every ``raise`` in the file."""
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Raise):
            name = _raised_exception_name(node)
            if name is not None:
                yield name, node.lineno


@pytest.mark.parametrize(
    ("relative_path", "forbidden"),
    sorted(FORBIDDEN_RAISES.items()),
)
def test_no_bare_exception_raises(relative_path: str, forbidden: Tuple[str, ...]):
    """No ``raise ValueError(...)`` / ``raise RuntimeError(...)`` remains."""
    source_path = PACKAGE_ROOT / relative_path
    assert source_path.is_file(), f"source file not found: {source_path}"

    offenders: List[str] = [
        f"{relative_path}:{lineno}: raise {name}"
        for name, lineno in _iter_raises(source_path)
        if name in forbidden
    ]

    assert not offenders, (
        "these raise statements must use the Exception_Hierarchy types instead:\n"
        + "\n".join(offenders)
    )


def test_scanner_detects_forbidden_raises(tmp_path: Path):
    """Guard the scanner itself: it must flag the patterns it is looking for."""
    sample = tmp_path / "sample.py"
    sample.write_text(
        "def f(x):\n"
        "    if x:\n"
        "        raise ValueError('bad')\n"
        "    try:\n"
        "        pass\n"
        "    except Exception:\n"
        "        raise\n"
        "    raise RuntimeError\n",
        encoding="utf-8",
    )

    # ``ast.walk`` does not visit in source order, so compare by line number.
    found = sorted(_iter_raises(sample), key=lambda item: item[1])
    assert found == [("ValueError", 3), ("RuntimeError", 8)]
