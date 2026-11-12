"""Release checks fail closed for a wrong tag, CI run, job, or checksum."""

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "release_gate.py"
SPEC = importlib.util.spec_from_file_location("release_gate", SCRIPT)
assert SPEC and SPEC.loader
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)
SHA = "a" * 40


def test_source_rejects_wrong_tag_and_checkout(monkeypatch, tmp_path):
    monkeypatch.setattr(
        gate, "git", lambda _checkout, *args: SHA if args[-1] == "HEAD" else "b" * 40
    )
    with pytest.raises(gate.GateError, match="does not match"):
        gate.source_commit(tmp_path, "v1.2.3")
    with pytest.raises(gate.GateError, match="simple v-prefixed"):
        gate.source_commit(tmp_path, "main")
    monkeypatch.setattr(gate, "git", lambda _checkout, *args: SHA)
    with pytest.raises(gate.GateError, match="event commit"):
        gate.source_commit(tmp_path, "v1.2.3", "b" * 40)
    assert gate.source_commit(tmp_path, "v1.2.3", SHA) == SHA


def test_remote_tag_must_still_point_to_checked_commit(monkeypatch, tmp_path):
    monkeypatch.setattr(
        gate,
        "git",
        lambda _checkout, *_args: (
            f"{'b' * 40}\trefs/tags/v1.2.3\n{SHA}\trefs/tags/v1.2.3^{{}}"
        ),
    )
    gate.check_remote_tag(tmp_path, "v1.2.3", SHA)
    monkeypatch.setattr(
        gate,
        "git",
        lambda _checkout, *_args: f"{'b' * 40}\trefs/tags/v1.2.3^{{}}",
    )
    with pytest.raises(gate.GateError, match="remote tag"):
        gate.check_remote_tag(tmp_path, "v1.2.3", SHA)


def test_latest_main_ci_run_must_succeed():
    older = dict(
        id=1,
        head_sha=SHA,
        event="push",
        head_branch="main",
        status="completed",
        conclusion="success",
    )
    newer = dict(older, id=2, conclusion="failure")
    with pytest.raises(gate.GateError, match="latest CI run"):
        gate.select_ci_run(dict(total_count=2, workflow_runs=[older, newer]), SHA)
    newer["conclusion"] = "success"
    assert (
        gate.select_ci_run(dict(total_count=2, workflow_runs=[older, newer]), SHA)["id"]
        == 2
    )
    with pytest.raises(gate.GateError, match="no main push"):
        gate.select_ci_run(
            dict(total_count=1, workflow_runs=[dict(newer, head_sha="b" * 40)]), SHA
        )


def test_every_required_job_must_match_commit_and_succeed():
    good = dict(name="build", status="completed", conclusion="success", head_sha=SHA)
    gate.check_jobs(dict(total_count=1, jobs=[good]), ["build"], SHA)
    for jobs in (
        [],
        [dict(good, conclusion="failure")],
        [dict(good, head_sha="b" * 40)],
    ):
        with pytest.raises(gate.GateError):
            gate.check_jobs(dict(total_count=len(jobs), jobs=jobs), ["build"], SHA)


def test_release_can_require_the_latest_manual_run_for_the_same_main_commit():
    push = dict(
        id=1,
        head_sha=SHA,
        event="push",
        head_branch="main",
        status="completed",
        conclusion="success",
    )
    manual = dict(push, id=2, event="workflow_dispatch")
    branch_run = dict(manual, id=3, head_branch="candidate")
    wrong_commit = dict(manual, id=4, head_sha="b" * 40)
    runs = dict(total_count=4, workflow_runs=[push, manual, branch_run, wrong_commit])
    assert gate.select_ci_run(runs, SHA, event="workflow_dispatch")["id"] == 2
    assert gate.select_ci_run(runs, SHA)["id"] == 1
    manual["conclusion"] = "failure"
    with pytest.raises(gate.GateError, match="latest CI run"):
        gate.select_ci_run(runs, SHA, event="workflow_dispatch")


def test_artifact_manifest_rejects_changed_or_extra_files(monkeypatch, tmp_path):
    monkeypatch.setattr(gate, "source_commit", lambda *_args: SHA)
    proof = tmp_path / "source.json"
    proof.write_text(
        json.dumps(dict(repository="owner/takler", tag="v1.2.3", commit=SHA))
    )
    dist = tmp_path / "dist"
    dist.mkdir()
    wheel = dist / "takler.whl"
    wheel.write_bytes(b"original")
    manifest = dist / "release-manifest.json"
    gate.manifest(SimpleNamespace(proof_file=proof, dist=dist, manifest=manifest))
    args = SimpleNamespace(
        manifest=manifest,
        dist=dist,
        tag="v1.2.3",
        repository="owner/takler",
        checkout=tmp_path,
        expected_sha=SHA,
        check_remote=False,
    )
    gate.verify(args)
    wheel.write_bytes(b"changed")
    with pytest.raises(gate.GateError, match="checksums"):
        gate.verify(args)
    wheel.write_bytes(b"original")
    (dist / "extra").write_bytes(b"extra")
    with pytest.raises(gate.GateError, match="names"):
        gate.verify(args)
    args.tag = "v2.0.0"
    with pytest.raises(gate.GateError, match="another repository or tag"):
        gate.verify(args)


def test_release_workflow_checks_before_publish():
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
    build = workflow["jobs"]["build"]
    publish = workflow["jobs"]["publish"]
    build_commands = "\n".join(step.get("run", "") for step in build["steps"])
    publish_commands = "\n".join(step.get("run", "") for step in publish["steps"])
    assert "release_gate.py source" in build_commands
    assert "--ci-event workflow_dispatch" in build_commands
    for job in (
        "ci (Python 3.11)",
        "ci (Python 3.12)",
        "install smoke (Python 3.11)",
        "install smoke (Python 3.12)",
        "paired protocol and CLI contracts",
    ):
        assert job in build_commands
    assert '--expected-sha "$GITHUB_SHA"' in build_commands
    assert "release_gate.py manifest" in build_commands
    assert "release_gate.py verify" in build_commands
    assert publish["needs"] == "build"
    assert "dry_run" in publish["if"]
    assert "release_gate.py verify" in publish_commands
    assert next(
        i
        for i, step in enumerate(publish["steps"])
        if "release_gate.py verify" in step.get("run", "")
    ) < next(
        i
        for i, step in enumerate(publish["steps"])
        if "gh-action-pypi-publish" in step.get("uses", "")
    )
