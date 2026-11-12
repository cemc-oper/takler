"""Verify the schema import boundary in a fresh Python interpreter."""

import subprocess
import sys


def test_schema_does_not_import_execution_objects():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import takler.schema; "
            "assert not any(x.startswith(('takler.core', 'takler.server', 'takler.client', 'takler.tasks')) for x in sys.modules)",
        ],
        check=True,
    )
