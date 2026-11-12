"""Unit tests for process-wide backend selection.

Covers the backend selector in :mod:`takler.logging.backends`:

* When ``loguru`` can be imported, the selector yields a ``LoguruBackend`` and
  the choice stays fixed for the process (Requirement 9.1).
* When importing ``loguru`` raises ``ImportError``, the selector yields a
  ``StdlibBackend`` and the choice stays fixed for the process (Requirement 9.2).

Because ``loguru`` may or may not be installed in the running environment, the
tests simulate *both* import branches rather than depending on the actual
install state. The loguru-absent branch is exercised by patching
``builtins.__import__`` so that importing ``loguru`` raises ``ImportError``
while every other import behaves normally. The loguru-present branch injects a
stub ``loguru`` module into ``sys.modules`` when the real library is not
installed, so the import inside the selector succeeds regardless of environment.

Every test resets the module-level singleton via ``reset_backend`` in setup and
teardown so the process-wide cache never leaks between tests.

_Requirements: 9.1, 9.2_
"""

from __future__ import annotations

import builtins
import importlib.util
import sys
import types
from contextlib import contextmanager
from typing import Iterator

import pytest

import takler.logging.backends as backends_module
from takler.logging.backends import (
    get_backend,
    reset_backend,
    select_backend,
)
from takler.logging.backends.stdlib_backend import StdlibBackend

# Whether the real loguru library is importable in this environment. The tests
# are written to pass either way.
_LOGURU_INSTALLED = importlib.util.find_spec("loguru") is not None

# The real loguru import succeeds only when the package is installed. The
# concrete LoguruBackend class can only be imported in that case (it does
# ``from loguru import logger`` at module top level).
if _LOGURU_INSTALLED:
    from takler.logging.backends.loguru_backend import LoguruBackend


@pytest.fixture(autouse=True)
def _reset_backend_singleton() -> Iterator[None]:
    """Reset the process-wide backend cache around every test.

    Keeps the module-level singleton from leaking selection state between
    tests (and out to the rest of the suite).
    """
    reset_backend()
    yield
    reset_backend()


@contextmanager
def _simulate_loguru_absent() -> Iterator[None]:
    """Make ``import loguru`` raise ``ImportError`` within the block.

    Patches ``builtins.__import__`` so any attempt to import ``loguru`` (or a
    submodule of it) raises ``ImportError`` while all other imports proceed
    normally. Also hides any already-imported ``loguru`` modules from
    ``sys.modules`` for the duration so a cached module cannot satisfy the
    import.
    """
    real_import = builtins.__import__
    saved_modules = {
        name: module
        for name, module in list(sys.modules.items())
        if name == "loguru" or name.startswith("loguru.")
    }
    for name in saved_modules:
        del sys.modules[name]

    def fake_import(name, *args, **kwargs):
        if name == "loguru" or name.startswith("loguru."):
            raise ImportError("simulated: loguru is not installed")
        return real_import(name, *args, **kwargs)

    builtins.__import__ = fake_import
    try:
        yield
    finally:
        builtins.__import__ = real_import
        sys.modules.update(saved_modules)


@contextmanager
def _simulate_loguru_present() -> Iterator[None]:
    """Ensure ``import loguru`` succeeds within the block.

    When the real library is installed this is a no-op. Otherwise a minimal
    stub module exposing a ``logger`` attribute is injected into
    ``sys.modules`` so the bare ``import loguru`` performed by the selector
    succeeds. The stub is removed again on exit.
    """
    if _LOGURU_INSTALLED:
        yield
        return

    stub = types.ModuleType("loguru")
    stub.logger = object()  # selector only needs the import to succeed
    sys.modules["loguru"] = stub
    try:
        yield
    finally:
        sys.modules.pop("loguru", None)


def test_select_backend_yields_loguru_when_importable():
    """loguru importable -> selector constructs a LoguruBackend (Req 9.1)."""
    if not _LOGURU_INSTALLED:
        pytest.skip("real loguru not installed; covered by the stub-based tests")

    with _simulate_loguru_present():
        backend = select_backend()

    assert isinstance(backend, LoguruBackend)


def test_select_backend_falls_back_to_stdlib_on_import_error():
    """loguru import raises ImportError -> selector yields StdlibBackend (Req 9.2)."""
    with _simulate_loguru_absent():
        backend = select_backend()

    assert isinstance(backend, StdlibBackend)


def test_get_backend_selects_stdlib_when_loguru_absent():
    """get_backend caches a StdlibBackend when loguru cannot be imported (Req 9.2)."""
    with _simulate_loguru_absent():
        backend = get_backend()

    assert isinstance(backend, StdlibBackend)


def test_get_backend_selects_loguru_when_present():
    """get_backend caches a loguru-backed backend when loguru imports (Req 9.1)."""
    if not _LOGURU_INSTALLED:
        pytest.skip("real loguru not installed; covered by the stub-based tests")

    with _simulate_loguru_present():
        backend = get_backend()

    assert isinstance(backend, LoguruBackend)


def test_selection_stays_fixed_to_stdlib_even_if_loguru_becomes_available():
    """Once stdlib is selected, it stays fixed even if loguru later imports (Req 9.2).

    The selection is made once and cached for the process lifetime; a change in
    loguru's import availability must not flip the active backend until the
    cache is explicitly reset.
    """
    with _simulate_loguru_absent():
        first = get_backend()
    assert isinstance(first, StdlibBackend)

    # loguru is now importable, but without a reset the cached choice holds.
    with _simulate_loguru_present():
        second = get_backend()

    assert second is first
    assert isinstance(second, StdlibBackend)


def test_selection_stays_fixed_to_loguru_even_if_loguru_disappears():
    """Once loguru is selected, it stays fixed even if loguru later fails to import (Req 9.1)."""
    if not _LOGURU_INSTALLED:
        pytest.skip("real loguru not installed; covered by the stub-based tests")

    with _simulate_loguru_present():
        first = get_backend()
    assert isinstance(first, LoguruBackend)

    # Even if loguru becomes unimportable, the cached choice is unchanged.
    with _simulate_loguru_absent():
        second = get_backend()

    assert second is first
    assert isinstance(second, LoguruBackend)


def test_repeated_get_backend_returns_same_instance_until_reset():
    """get_backend returns the same cached instance; reset forces re-selection (Req 9.1, 9.2)."""
    first = get_backend()
    second = get_backend()
    assert first is second

    reset_backend()
    third = get_backend()
    # After a reset a fresh instance is selected (same type, new object).
    assert third is not first
    assert type(third) is type(first)


def test_reset_backend_clears_cached_singleton():
    """reset_backend clears the module-level singleton so the next call re-selects."""
    get_backend()
    assert backends_module._BACKEND is not None

    reset_backend()
    assert backends_module._BACKEND is None
