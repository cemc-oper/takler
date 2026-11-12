"""Check the server module's help entry point in a separate interpreter."""

import os
from pathlib import Path
import subprocess
import sys

from tests.unit.server.test_server_cli_unit import EXPECTED_OPTIONS, WIDE

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_python_dash_m_help_matches():
    """``python -m takler.server --help`` is the same CLI (requirement 15.3)."""
    completed = subprocess.run(
        [sys.executable, "-m", "takler.server", "--help"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, **WIDE},
    )

    assert completed.returncode == 0
    for option in EXPECTED_OPTIONS:
        assert option in completed.stdout
