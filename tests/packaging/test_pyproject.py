"""Structural tests for the packaging metadata in ``pyproject.toml``.

``pip install`` is not executed here: a real install is validated by the CI
install step itself. What these tests pin down is the *shape* of the metadata,
which is what makes a deployment reproducible and keeps the console entry
points from drifting back to names that clash with the Go client:

* the three user-facing extras (``tui`` / ``log`` / ``test``) exist and carry
  the members the design prescribes,
* every requirement, runtime or optional, declares a minimum version lower
  bound so a resolver cannot pick an arbitrarily old release,
* no requirement is guarded by a ``python_version`` marker (``requires-python``
  is already ``>=3.11``, so such a marker is either dead or a hidden branch),
* ``[project.scripts]`` declares exactly ``takler-server``,
  ``takler-client-py`` and ``takler-tui`` -- notably neither ``takler`` nor
  ``takler_client``, the latter being the artifact name of the Go client,
* dev tooling (ruff) stays in ``[dependency-groups] dev`` rather than in an
  extra, and ``[tool.ruff.lint] select`` names the rule set instead of inheriting
  whatever the installed ruff happens to default to.

Only these properties are asserted. The file legitimately grows other tables
(coverage configuration, for one), so nothing here claims a table set is
exhaustive.

Validates: Requirements 14.1, 14.2, 14.3, 14.4, 14.5, 14.6, 14.7, 15.1, 15.7, 16.18
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest


# ``tests/packaging/test_pyproject.py`` -> project root.
PYPROJECT_PATH = Path(__file__).resolve().parents[2] / "pyproject.toml"

EXPECTED_EXTRAS = {"tui", "log", "test"}
EXPECTED_SCRIPTS = {"takler-server", "takler-client-py", "takler-tui"}
FORBIDDEN_SCRIPTS = {"takler", "takler_client"}

# A lower bound can be spelled in several ways; each of these pins a floor.
LOWER_BOUND_OPERATORS = (">=", "==", "~=", ">")

_NAME_TERMINATORS = "<>=!~;[ ("


@pytest.fixture(scope="module")
def pyproject() -> dict:
    with PYPROJECT_PATH.open("rb") as f:
        return tomllib.load(f)


@pytest.fixture(scope="module")
def project(pyproject: dict) -> dict:
    return pyproject["project"]


@pytest.fixture(scope="module")
def optional_dependencies(project: dict) -> dict[str, list[str]]:
    return project["optional-dependencies"]


def _distribution_name(requirement: str) -> str:
    """Return the distribution name of a PEP 508 requirement string."""
    text = requirement.strip()
    for index, char in enumerate(text):
        if char in _NAME_TERMINATORS:
            return text[:index]
    return text


def _specifier_part(requirement: str) -> str:
    """Return the version specifier part, i.e. the text before any marker."""
    return requirement.split(";", 1)[0]


def _has_lower_bound(requirement: str) -> bool:
    specifier = _specifier_part(requirement)
    # Strip the name (and any extras) so a name such as ``zope.interface``
    # cannot be mistaken for an operator.
    name = _distribution_name(requirement)
    specifier = specifier[len(name) :]
    return any(operator in specifier for operator in LOWER_BOUND_OPERATORS)


def _all_requirements(
    project: dict, optional_dependencies: dict[str, list[str]]
) -> list[tuple[str, str]]:
    """Return ``(source, requirement)`` pairs for runtime and optional deps."""
    pairs = [("project.dependencies", req) for req in project["dependencies"]]
    for group, requirements in optional_dependencies.items():
        pairs.extend(
            (f"project.optional-dependencies.{group}", req) for req in requirements
        )
    return pairs


# ---------------------------------------------------------------------------
# Optional dependency groups (Requirements 14.1, 14.2, 14.3, 14.7)
# ---------------------------------------------------------------------------


def test_optional_dependency_groups_are_exactly_tui_log_test(
    optional_dependencies: dict[str, list[str]],
):
    """The three user-facing extras exist, and no other extra is declared."""
    assert set(optional_dependencies) == EXPECTED_EXTRAS


def test_tui_extra_installs_textual_and_rich(
    optional_dependencies: dict[str, list[str]],
):
    """``pip install takler[tui]`` pulls in both TUI libraries."""
    names = {_distribution_name(req) for req in optional_dependencies["tui"]}

    assert {"textual", "rich"} <= names


def test_log_extra_installs_loguru(optional_dependencies: dict[str, list[str]]):
    """``pip install takler[log]`` pulls in the loguru backend."""
    names = {_distribution_name(req) for req in optional_dependencies["log"]}

    assert "loguru" in names


def test_dependency_groups_keeps_only_dev_tooling(pyproject: dict):
    """The extras moved out of ``[dependency-groups]``, which keeps ``dev``."""
    groups = pyproject["dependency-groups"]

    assert "dev" in groups
    assert EXPECTED_EXTRAS.isdisjoint(groups)


def test_ruff_is_dev_tooling_not_a_user_facing_extra(pyproject: dict):
    """The linter belongs to ``[dependency-groups] dev``, not to any extra.

    Two reasons, and the second one is the one that bit us: a user installing
    ``takler[test]`` has no use for a linter, and dependency groups are locked by
    ``uv.lock``, so CI (``uv sync --locked``) and a developer machine
    (``uv run ruff``) get the byte-identical ruff. While ruff sat in the ``test``
    extra with a ``>=0.5`` floor, CI resolved it freely and 0.16 changed the
    default rule set under us.
    """
    dev_names = {
        _distribution_name(req) for req in pyproject["dependency-groups"]["dev"]
    }
    extra_names = {
        _distribution_name(req)
        for requirements in pyproject["project"]["optional-dependencies"].values()
        for req in requirements
    }

    assert "ruff" in dev_names
    assert "ruff" not in extra_names


def test_every_dev_group_requirement_declares_a_lower_bound_or_is_bare(
    pyproject: dict,
):
    """Dev tooling may be unpinned, but a declared bound must be a lower one.

    ``uv.lock`` is what actually pins these, so a bare name (``build``,
    ``ipython``) is fine here; what must not appear is an upper bound only,
    which would let a resolver walk backwards.
    """
    upper_only = [
        req
        for req in pyproject["dependency-groups"]["dev"]
        if _specifier_part(req) != _distribution_name(req) and not _has_lower_bound(req)
    ]

    assert upper_only == []


# ---------------------------------------------------------------------------
# Ruff configuration (guards the 0.16 default-rule-set change)
# ---------------------------------------------------------------------------


def test_ruff_lint_rule_set_is_declared_explicitly(pyproject: dict):
    """``[tool.ruff.lint] select`` freezes the rule set across ruff versions.

    Without ``select``, the enabled rules are whatever the installed ruff
    defaults to, and 0.16.0 raised that from 59 rules to 413. The value is not
    asserted -- adopting more rules is a legitimate decision -- only that a
    decision was written down.
    """
    select = pyproject["tool"]["ruff"]["lint"]["select"]

    assert select, select


# ---------------------------------------------------------------------------
# Version lower bounds (Requirements 14.4, 14.5)
# ---------------------------------------------------------------------------


def test_every_runtime_dependency_declares_a_lower_bound(project: dict):
    """No runtime requirement is left unbounded below."""
    unbounded = [req for req in project["dependencies"] if not _has_lower_bound(req)]

    assert unbounded == []


def test_every_optional_dependency_declares_a_lower_bound(
    optional_dependencies: dict[str, list[str]],
):
    """No requirement in any extra is left unbounded below."""
    unbounded = {
        group: [req for req in requirements if not _has_lower_bound(req)]
        for group, requirements in optional_dependencies.items()
    }
    unbounded = {group: reqs for group, reqs in unbounded.items() if reqs}

    assert unbounded == {}


# ---------------------------------------------------------------------------
# Environment markers (Requirement 14.6)
# ---------------------------------------------------------------------------


def test_no_requirement_carries_a_python_version_marker(
    project: dict, optional_dependencies: dict[str, list[str]]
):
    """``requires-python >= 3.11`` makes such markers dead conditions."""
    marked = [
        f"{source}: {req}"
        for source, req in _all_requirements(project, optional_dependencies)
        if "python_version" in req
    ]

    assert marked == []


# ---------------------------------------------------------------------------
# Console entry points (Requirements 15.1, 15.7, 16.18)
# ---------------------------------------------------------------------------


def test_scripts_are_exactly_the_three_expected_entries(project: dict):
    """The entry point set is frozen so a rename cannot be reverted silently."""
    assert set(project["scripts"]) == EXPECTED_SCRIPTS


def test_scripts_avoid_takler_and_takler_client_names(project: dict):
    """Neither the bare name nor the Go client artifact name is taken."""
    names = set(project["scripts"])

    assert FORBIDDEN_SCRIPTS.isdisjoint(names)


def test_each_script_points_at_a_dotted_module_target(project: dict):
    """Every entry point value is a ``module:attribute`` target in ``takler``."""
    for name, target in project["scripts"].items():
        module, _, attribute = target.partition(":")

        assert module.startswith("takler."), (name, target)
        assert attribute, (name, target)
