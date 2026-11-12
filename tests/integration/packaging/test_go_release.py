"""Check release parity with the adjacent Go client checkout."""

import pytest
import yaml

from tests.unit.packaging.test_release_gate import ROOT, SCRIPT


def test_go_release_workflow_gates_binaries_when_peer_is_present():
    peer = ROOT.parent / "takler-client/.github/workflows/release.yml"
    if not peer.exists():
        pytest.skip("Go peer is checked separately in paired CI")
    workflow = yaml.safe_load(peer.read_text())
    build = workflow["jobs"]["build"]
    publish = workflow["jobs"]["publish"]
    commands = "\n".join(step.get("run", "") for step in build["steps"])
    assert "release_gate.py source" in commands
    assert "ci (Go)" in commands and "paired protocol and CLI contracts" in commands
    assert '--expected-sha "$GITHUB_SHA"' in commands
    assert 'GOARCH="$arch"' in commands
    assert "sha256sum -c SHA256SUMS" in commands
    assert "release_gate.py manifest" in commands
    assert "release_gate.py verify" in commands
    assert publish["needs"] == "build" and "dry_run" in publish["if"]
    publish_commands = "\n".join(step.get("run", "") for step in publish["steps"])
    assert "release_gate.py verify" in publish_commands
    assert "gh release create" in publish_commands


def test_go_release_gate_uses_same_validation_when_peer_is_present():
    peer = ROOT.parent / "takler-client/scripts/release_gate.py"
    if not peer.exists():
        pytest.skip("Go peer is checked separately in paired CI")
    assert peer.read_bytes() == SCRIPT.read_bytes()
