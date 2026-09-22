"""Pilot tests for the standalone widgets: Toolbar and StatusBar."""

from __future__ import annotations

from datetime import datetime

import pytest
from textual.app import App, ComposeResult
from textual.widgets import Static

from takler.tui.widgets import StatusBar, Toolbar

from .conftest import text_of


class ToolbarHost(App[None]):
    """Mounts a toolbar and records its outgoing messages."""

    def __init__(self, auto_refresh_seconds: int = 3600) -> None:
        super().__init__()
        self.toolbar = Toolbar(
            host="fake-host:33083", auto_refresh_seconds=auto_refresh_seconds
        )
        self.refresh_requests = 0
        self.toggles: list[bool] = []

    def compose(self) -> ComposeResult:
        yield self.toolbar

    def on_toolbar_refresh_requested(self, event: Toolbar.RefreshRequested) -> None:
        self.refresh_requests += 1

    def on_toolbar_auto_refresh_toggled(
        self, event: Toolbar.AutoRefreshToggled
    ) -> None:
        self.toggles.append(event.enabled)


@pytest.mark.anyio
async def test_toolbar_initial_labels() -> None:
    app = ToolbarHost()
    async with app.run_test(size=(100, 30)):
        toolbar = app.toolbar
        assert text_of(toolbar.query_one("#toolbar-host", Static)) == "fake-host:33083"
        assert text_of(toolbar.query_one("#toolbar-countdown-label", Static)) == (
            "3600s"
        )
        assert text_of(toolbar.query_one("#toolbar-bunch", Static)) == ""
        assert text_of(toolbar.query_one("#toolbar-refreshed", Static)) == ""
        assert toolbar.auto_refresh_enabled is True


@pytest.mark.anyio
async def test_toolbar_refresh_button_requests_refresh() -> None:
    app = ToolbarHost()
    async with app.run_test(size=(100, 30)) as pilot:
        await pilot.click("#toolbar-refresh")
        await pilot.pause()
        assert app.refresh_requests == 1


@pytest.mark.anyio
async def test_toolbar_switch_toggles_auto_refresh() -> None:
    app = ToolbarHost()
    async with app.run_test(size=(100, 30)) as pilot:
        await pilot.click("#toolbar-auto-switch")
        await pilot.pause()
        assert app.toggles == [False]
        assert app.toolbar.auto_refresh_enabled is False

        await pilot.click("#toolbar-auto-switch")
        await pilot.pause()
        assert app.toggles == [False, True]
        assert app.toolbar.auto_refresh_enabled is True


@pytest.mark.anyio
async def test_toolbar_countdown_requests_refresh_when_full() -> None:
    app = ToolbarHost(auto_refresh_seconds=1)
    async with app.run_test(size=(100, 30)) as pilot:
        # Drive the tick directly instead of waiting for the 1s timer.
        app.toolbar._tick_countdown()
        await pilot.pause()
        assert app.refresh_requests == 1


@pytest.mark.anyio
async def test_toolbar_reset_countdown_empties_progress() -> None:
    app = ToolbarHost()
    async with app.run_test(size=(100, 30)):
        toolbar = app.toolbar
        toolbar._tick_countdown()
        assert toolbar._countdown_elapsed == 1
        toolbar.reset_countdown()
        assert toolbar._countdown_elapsed == 0


@pytest.mark.anyio
async def test_toolbar_bunch_and_refresh_labels() -> None:
    app = ToolbarHost()
    async with app.run_test(size=(100, 30)):
        toolbar = app.toolbar
        toolbar.set_bunch("test_bunch")
        assert "test_bunch" in text_of(toolbar.query_one("#toolbar-bunch", Static))
        toolbar.set_bunch(None)
        assert text_of(toolbar.query_one("#toolbar-bunch", Static)) == ""

        toolbar.set_refreshed(datetime(2026, 9, 22, 13, 14, 15))
        assert "13:14:15" in text_of(toolbar.query_one("#toolbar-refreshed", Static))
        toolbar.set_refreshed(None)
        assert text_of(toolbar.query_one("#toolbar-refreshed", Static)) == ""


class StatusBarHost(App[None]):
    def __init__(self) -> None:
        super().__init__()
        self.status_bar = StatusBar()

    def compose(self) -> ComposeResult:
        yield self.status_bar


@pytest.mark.anyio
async def test_status_bar_empty_selection_shows_placeholder() -> None:
    app = StatusBarHost()
    async with app.run_test(size=(100, 30)):
        left = app.status_bar.query_one("#status-left", Static)
        assert text_of(left) == "—"


@pytest.mark.anyio
async def test_status_bar_selection_shows_badge_and_path(snapshot) -> None:
    app = StatusBarHost()
    async with app.run_test(size=(100, 30)):
        node = snapshot.get("/flow1/family1/task1")
        app.status_bar.set_selection(node)
        text = text_of(app.status_bar.query_one("#status-left", Static))
        assert text.startswith("■■")  # suspended two-tone badge
        assert text.endswith("/flow1/family1/task1")

        app.status_bar.set_selection(None)
        assert text_of(app.status_bar.query_one("#status-left", Static)) == "—"


@pytest.mark.anyio
async def test_status_bar_message() -> None:
    app = StatusBarHost()
    async with app.run_test(size=(100, 30)):
        app.status_bar.set_message("requeue: /flow1", "green")
        assert (
            text_of(app.status_bar.query_one("#status-right", Static))
            == "requeue: /flow1"
        )
        # Unknown styles fall back to unstyled text instead of raising.
        app.status_bar.set_message("plain", "not-a-style")
        assert text_of(app.status_bar.query_one("#status-right", Static)) == "plain"
