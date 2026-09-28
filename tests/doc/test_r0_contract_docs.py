"""Executable definition example and documentation boundaries for R0."""

import json
from pathlib import Path
import subprocess
import sys

from takler.core import NodeStatus
from takler.schema import parse_definition
from takler.serialization import build_definition

ROOT = Path(__file__).resolve().parents[2]


def test_definition_export_example(tmp_path):
    output = tmp_path / "forecast.json"
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "doc/examples/definition/export_flow.py"),
            str(output),
        ],
        check=True,
    )
    data = json.loads(output.read_text())
    assert data["kind"] == "takler.definition"
    assert data["schema_version"] == 1
    flow = build_definition(parse_definition(output.read_bytes()))
    assert flow.name == "forecast"
    assert not flow.begun
    assert flow.children[0].default_node_status is NodeStatus.complete
    assert "job_password" not in output.read_text()


def test_documented_commands_and_hpc_boundary():
    docs = ROOT / "doc/source"
    protocol = (docs / "develop/protocol.rst").read_text()
    assert "十七个" in protocol
    assert "九个控制命令" in protocol
    definition = (docs / "develop/definition.rst").read_text()
    assert "当前不能通过客户端调用 replace" not in definition
    assert "exactly-once" in definition
    assert "schema_version=1" in definition and "format_version=2" in definition
    for path in (
        "guide/job-management.rst",
        "guide/ecflow-differences.rst",
        "tutorial/hpc-appendix.rst",
    ):
        content = (docs / path).read_text()
        assert "orvix" in content
        assert "只有本地 shell" not in content
