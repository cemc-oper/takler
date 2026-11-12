"""Selection for the on-demand test suites."""

import pytest


def pytest_itemcollected(item: pytest.Item) -> None:
    """Keep the extended marker available for selecting this directory."""
    item.add_marker(pytest.mark.extended)
