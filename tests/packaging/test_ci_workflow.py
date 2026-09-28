"""Structural tests for the CI workflow ``.github/workflows/test.yml``.

The workflow itself cannot run here, so what is pinned down is its *intent*: the
Python versions the test job fans out over, the fact that one failing version
does not cancel the others, the extras the install step pulls in, the two ruff
gates, and the coverage collection. Every assertion reads the parsed YAML and
looks for commands or values, never for exact whitespace or step ordering, so
reformatting the workflow (or reordering unrelated steps) does not break these
tests.

One invariant here is not cosmetic: the environment comes from ``uv.lock`` via
``uv sync --locked``, and every tool runs through ``uv run``. Letting a resolver
pick versions at CI time is what produced a green local ``ruff check`` next to a
red CI one (``ruff>=0.5`` resolved to 0.16, which enables 413 rules by default
instead of 59), so ``--locked`` and the ``uv run`` prefix are asserted rather
than left to convention.

Two YAML details are worth naming, because both are easy to trip over:

* under YAML 1.1 the key ``on:`` parses as the boolean ``True``, not as the
  string ``"on"``. ``_triggers`` normalises both spellings;
* ``python-version: [ 3.11, 3.12 ]`` without quotes parses as floats, and the
  day someone adds ``3.10`` that silently becomes ``3.1``. The matrix test
  therefore also asserts the entries are strings.

Only the properties above are asserted. The workflow legitimately grows other
jobs, steps and permissions, so nothing here claims a set is exhaustive apart
from the matrix entries that requirement 16.1 names.

Validates: Requirements 16.1, 16.2, 16.3, 16.4, 16.5, 16.6
"""

from __future__ import annotations

import re
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml


# ``tests/packaging/test_ci_workflow.py`` -> project root.
WORKFLOW_PATH = (
    Path(__file__).resolve().parents[2] / ".github" / "workflows" / "test.yml"
)

EXPECTED_PYTHON_VERSIONS = {"3.11", "3.12"}

#: Actions that may be responsible for providing the interpreter.
PYTHON_SETUP_ACTIONS = ("setup-uv", "setup-python")

#: Tools that must not be invoked bare, i.e. from whatever is on ``PATH``.
#: Each of them has to come from the environment ``uv sync --locked`` built.
LOCKED_TOOLS = frozenset({"ruff", "pytest", "coverage"})

#: The prefix that runs a command inside the locked environment.
RUNNER_PREFIX = ("uv", "run")

#: ``on:`` is read back as the boolean ``True`` by a YAML 1.1 loader.
ON_KEYS = (True, "on")


@pytest.fixture(scope="module")
def workflow() -> dict[str, Any]:
    with WORKFLOW_PATH.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def test_job(workflow: dict[str, Any]) -> dict[str, Any]:
    """Return the single job that installs dependencies and runs pytest."""
    jobs = workflow["jobs"]
    candidates = [
        job
        for job in jobs.values()
        if any("pytest" in command for command in _run_commands(job))
    ]

    assert len(candidates) == 1, sorted(jobs)
    return candidates[0]


def _triggers(workflow: dict[str, Any]) -> dict[str, Any]:
    """Return the ``on:`` section regardless of how the loader spelled the key."""
    for key in ON_KEYS:
        if key in workflow:
            return workflow[key]
    raise AssertionError(f"no trigger section among keys {sorted(map(str, workflow))}")


def _run_commands(job: dict[str, Any]) -> list[str]:
    """Return the ``run:`` script of every step of a job."""
    return [step["run"] for step in job["steps"] if "run" in step]


def _command_lines(job: dict[str, Any]) -> list[str]:
    """Return every non-empty line of every ``run:`` script, stripped."""
    return [
        line.strip()
        for command in _run_commands(job)
        for line in command.splitlines()
        if line.strip()
    ]


def _invoked_tokens(line: str) -> list[str]:
    """Return the tokens of ``line`` with a leading ``uv run`` removed.

    ``uv run ruff check .`` is an invocation *of ruff*, so the runner prefix (and
    any options it carries, e.g. ``uv run --frozen``) is stripped before the
    tokens are matched. ``uv sync ...`` is left alone: there ``uv`` is the
    command being invoked.
    """
    tokens = line.split()
    if tokens[: len(RUNNER_PREFIX)] != list(RUNNER_PREFIX):
        return tokens
    rest = tokens[len(RUNNER_PREFIX) :]
    while rest and rest[0].startswith("-"):
        rest = rest[1:]
    return rest


def _steps_running(job: dict[str, Any], executable: str, *args: str) -> list[str]:
    """Return the command lines that invoke ``executable`` with all of ``args``.

    Matching is token based so that ``ruff check`` cannot be satisfied by
    ``ruff format --check``, and argument order is irrelevant. A leading
    ``uv run`` does not hide the executable behind it.
    """
    matches = []
    for line in _command_lines(job):
        tokens = _invoked_tokens(line)
        if not tokens or tokens[0] != executable:
            continue
        if all(arg in tokens[1:] for arg in args):
            matches.append(line)
    return matches


# ---------------------------------------------------------------------------
# Triggers (guards the YAML 1.1 ``on:`` pitfall)
# ---------------------------------------------------------------------------


def test_workflow_declares_push_and_pull_request_triggers(workflow: dict[str, Any]):
    """The workflow runs on both pushes and pull requests."""
    triggers = _triggers(workflow)

    assert {"push", "pull_request"} <= set(triggers)


# ---------------------------------------------------------------------------
# Action references
# ---------------------------------------------------------------------------


def test_astral_actions_are_not_pinned_to_a_bare_major_tag(workflow: dict[str, Any]):
    """``astral-sh/setup-uv`` must be referenced by an exact version or a SHA.

    Unlike ``actions/*``, astral-sh stopped publishing floating major tags after
    ``v7``: ``v8``, ``v9`` and ``v10`` exist only as ``v8.3.2``, ``v9.0.0``,
    ``v10.0.1`` and so on. Writing ``@v10`` therefore fails the run before any
    step executes, with "unable to resolve action ... unable to find version".
    """
    bare_major = []
    for job in workflow["jobs"].values():
        for step in job["steps"]:
            uses = str(step.get("uses", ""))
            if not uses.startswith("astral-sh/"):
                continue
            _, _, ref = uses.partition("@")
            if re.fullmatch(r"v\d+", ref):
                bare_major.append(uses)

    assert bare_major == []


# ---------------------------------------------------------------------------
# Python version matrix (Requirements 16.1, 16.2)
# ---------------------------------------------------------------------------


def test_matrix_covers_python_3_11_and_3_12(test_job: dict[str, Any]):
    """The test job fans out over exactly the two supported versions."""
    versions = test_job["strategy"]["matrix"]["python-version"]

    assert all(isinstance(version, str) for version in versions), versions
    assert set(versions) == EXPECTED_PYTHON_VERSIONS


def test_matrix_does_not_cancel_sibling_versions_on_failure(test_job: dict[str, Any]):
    """``fail-fast: false`` keeps the other versions running after a failure."""
    assert test_job["strategy"]["fail-fast"] is False


def test_job_name_identifies_the_python_version(test_job: dict[str, Any]):
    """The summary must tell the versions apart, so the name carries it."""
    assert "matrix.python-version" in test_job["name"]


def test_the_interpreter_comes_from_the_matrix_version(test_job: dict[str, Any]):
    """The interpreter actually installed is the matrix one, not a fixed one.

    Either ``astral-sh/setup-uv`` or ``actions/setup-python`` may provide it;
    what matters is that the version it is given is the matrix value. The
    ``setup-uv`` ``python-version`` input overrides the project's
    ``requires-python`` / ``.python-version`` pin, which is what makes the two
    matrix branches actually differ.
    """
    setup_steps = [
        step
        for step in test_job["steps"]
        if any(action in str(step.get("uses", "")) for action in PYTHON_SETUP_ACTIONS)
    ]

    assert setup_steps, test_job["steps"]
    from_matrix = [
        step
        for step in setup_steps
        if "matrix.python-version"
        in str(step.get("with", {}).get("python-version", ""))
    ]
    assert from_matrix, setup_steps


# ---------------------------------------------------------------------------
# Dependency installation (Requirement 16.3)
# ---------------------------------------------------------------------------


def test_install_step_pulls_all_dependency_groups(test_job: dict[str, Any]):
    """The sync installs every group, so the suite sees test and docs deps.

    ``--all-groups`` covers ``dev`` (which itself includes ``test`` and, via
    the project's self-reference, the ``tui``/``log`` extras) plus ``docs``,
    which ``tests/doc/test_doc_build.py`` needs for its ``sphinx-build -W``.
    """
    syncs = _steps_running(test_job, "uv", "sync")

    assert syncs, _command_lines(test_job)
    assert any("--all-groups" in line.split() for line in syncs), syncs


def test_install_step_restores_the_locked_environment(test_job: dict[str, Any]):
    """``uv sync --locked`` installs exactly what ``uv.lock`` pins.

    Without ``--locked`` a stale lockfile is silently re-resolved, so CI would
    again be free to pick versions a developer never ran against.
    """
    assert _steps_running(test_job, "uv", "sync", "--locked")


def test_no_step_installs_dependencies_with_pip(test_job: dict[str, Any]):
    """A stray ``pip install`` would resolve outside the lockfile."""
    pip_installs = [line for line in _command_lines(test_job) if "pip install" in line]

    assert pip_installs == []


def test_every_tool_runs_inside_the_locked_environment(test_job: dict[str, Any]):
    """ruff / pytest / coverage are invoked via ``uv run``, not bare from PATH."""
    bare = [
        line
        for line in _command_lines(test_job)
        if line.split() and line.split()[0] in LOCKED_TOOLS
    ]

    assert bare == []


# ---------------------------------------------------------------------------
# Lint gates (Requirements 16.4, 16.5)
# ---------------------------------------------------------------------------


def test_a_step_runs_ruff_check(test_job: dict[str, Any]):
    """``ruff check`` runs as its own command, so its exit code fails the job."""
    assert _steps_running(test_job, "ruff", "check")


def test_a_step_runs_ruff_format_check(test_job: dict[str, Any]):
    """``ruff format --check`` runs in check mode, i.e. it does not rewrite."""
    assert _steps_running(test_job, "ruff", "format", "--check")


# ---------------------------------------------------------------------------
# Test execution and coverage (Requirement 16.6)
# ---------------------------------------------------------------------------


def test_pytest_step_collects_coverage_and_reports_it(test_job: dict[str, Any]):
    """Coverage is measured on the ``takler`` package and a report is emitted."""
    pytest_lines = _steps_running(test_job, "pytest")

    assert pytest_lines
    measured = [line for line in pytest_lines if "--cov=takler" in line.split()]
    assert measured, pytest_lines
    for line in measured:
        reports = [token for token in line.split() if token.startswith("--cov-report")]
        assert reports, line


def test_pair_defaults_to_main_and_contract_runs_in_ci(workflow):
    assert any(
        'coverage report --include="src/takler/serialization/*" --fail-under=85'
        in command
        for command in _run_commands(workflow["jobs"]["build"])
    )
    for scope in ("src/takler/schema/*", "src/takler/server/flow_replace.py"):
        assert any(
            f'coverage report --include="{scope}" --fail-under=85' in command
            for command in _run_commands(workflow["jobs"]["build"])
        )
    assert "peer_sha" in _triggers(workflow)["workflow_dispatch"]["inputs"]
    job = workflow["jobs"]["paired-contract"]
    steps = job["steps"]
    peer_checkout = next(
        step for step in steps if step.get("with", {}).get("repository")
    )
    assert peer_checkout["with"]["ref"] == "${{ steps.peer.outputs.ref }}"
    commands = "\n".join(_run_commands(job))
    assert "^[0-9a-f]{40}$" in commands
    assert 'git -C "$repo" rev-parse HEAD' in commands
    assert '"$GITHUB_STEP_SUMMARY"' in commands
    assert "uv sync --locked --all-groups --extra http" in commands
    assert "bash scripts/check_proto.sh" in commands
    assert "make proto-check wire-check" in commands
    assert "make http-contract" in commands
    assert "make load-contract replace-contract show-contract" in commands
    assert "paired_check.py" not in commands


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
