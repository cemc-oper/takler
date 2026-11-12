"""Check the workflow's peer selection shell step with local inputs."""

import os
import subprocess

import pytest

from tests.unit.packaging.test_ci_workflow import workflow as workflow


@pytest.mark.parametrize(
    "requested,success",
    [
        ("", True),
        ("b" * 40, True),
        ("main", False),
        ("v1", False),
        ("abc", False),
        ("a" * 40 + "\ninjected=value", False),
    ],
)
def test_pair_selector_validates_sha_before_checkout(
    workflow, tmp_path, requested, success
):
    output = tmp_path / "output"
    selector = next(
        step
        for step in workflow["jobs"]["paired-contract"]["steps"]
        if step.get("id") == "peer"
    )
    result = subprocess.run(
        ["bash", "-e", "-c", selector["run"]],
        cwd=tmp_path,
        env=dict(os.environ, REQUESTED_SHA=requested, GITHUB_OUTPUT=str(output)),
        capture_output=True,
    )
    assert (result.returncode == 0) == success
    if success:
        assert output.read_text() == f"ref={requested or 'main'}\nsha={requested}\n"
    else:
        assert not output.exists()
