"""Strict Sphinx build test for ``doc/source``.

Runs ``sphinx-build -b html -W`` against the documentation source tree and
asserts it exits without error. ``-W`` turns every warning (a broken
cross-reference, an orphaned page, a missing ``:py:class:`` target, etc.) into
a build failure, so this test is what keeps a batch's "must build with zero
warnings" requirement (see ``doc/documentation-plan.md`` D8 and the batch
Demo sections) enforced in CI rather than only checked by hand.

This test is marked ``slow``: it spawns a real Sphinx build process and reads
every ``.rst`` source file plus the full ``takler`` package via autodoc.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

# ``tests/doc/test_doc_build.py`` -> project root.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOC_SOURCE = PROJECT_ROOT / "doc" / "source"


@pytest.mark.slow
def test_sphinx_build_html_strict(tmp_path: Path):
    """``sphinx-build -b html -W`` succeeds with no warnings treated as errors."""
    build_dir = tmp_path / "html"
    doctree_dir = tmp_path / "doctrees"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "sphinx",
            "-b",
            "html",
            "-W",
            "-d",
            str(doctree_dir),
            str(DOC_SOURCE),
            str(build_dir),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, (
        "sphinx-build failed or reported warnings:\n"
        f"--- stdout ---\n{result.stdout}\n"
        f"--- stderr ---\n{result.stderr}"
    )
