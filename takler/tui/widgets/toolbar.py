"""Top toolbar for the takler TUI.

:class:`Toolbar` groups the manual refresh button, the connection host
label, the auto-refresh switch + countdown bar, and the bunch /
last-refresh status labels.

It owns the once-per-second countdown timer and emits
:class:`Toolbar.RefreshRequested` both when the user presses the refresh
button and when the countdown fills. The app listens for that message,
performs the gRPC refresh, and calls back into :meth:`set_bunch`,
:meth:`set_refreshed`, and :meth:`reset_countdown`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.message import Message
from textual.widgets import Button, ProgressBar, Static, Switch


class Toolbar(Horizontal):
    """Toolbar row: refresh / host / auto-refresh / bunch / last-refresh."""

    DEFAULT_CSS = """
    Toolbar {
        height: 1;
        background: $boost;
        padding: 0 1;
    }

    Toolbar Button {
        min-width: 11;
        margin-right: 1;
    }

    Toolbar #toolbar-host {
        width: auto;
        padding-right: 2;
        color: $text-muted;
    }

    Toolbar #toolbar-countdown {
        width: auto;
        padding-right: 2;
    }

    Toolbar #toolbar-countdown-label {
        width: auto;
        padding-right: 1;
        color: $text-muted;
    }

    Toolbar #toolbar-countdown-bar {
        width: 16;
    }

    /* Track (total length) in an obvious colour so the full span is
       always visible; the filled portion advances over it. */
    #toolbar-countdown-bar Bar > .bar--bar {
        color: $warning;
        background: $primary-darken-2;
    }

    #toolbar-countdown-bar Bar > .bar--complete {
        color: $warning;
        background: $primary-darken-2;
    }

    Toolbar #toolbar-auto {
        width: auto;
        height: 1;
        padding-right: 1;
    }

    Toolbar #toolbar-auto-label {
        width: auto;
        height: 1;
        padding-right: 1;
        color: $text-muted;
    }

    Toolbar #toolbar-auto-switch {
        width: auto;
        height: 1;
        border: none;
        padding: 0;
        background: $boost;
    }

    Toolbar #toolbar-auto-switch:focus {
        border: none;
        background-tint: $foreground 5%;
    }

    Toolbar #toolbar-bunch {
        width: auto;
        padding-right: 2;
        text-style: bold;
    }

    Toolbar #toolbar-spacer {
        width: 1fr;
    }

    Toolbar #toolbar-refreshed {
        width: auto;
        color: $text-muted;
    }
    """

    class RefreshRequested(Message):
        """Posted when a refresh should run (button press or countdown fill)."""

    class AutoRefreshToggled(Message):
        """Posted when the auto-refresh switch changes.

        Attributes
        ----------
        enabled
            New on/off state of auto-refresh.
        """

        def __init__(self, enabled: bool) -> None:
            super().__init__()
            self.enabled = enabled

    def __init__(self, host: str, auto_refresh_seconds: int) -> None:
        super().__init__(id="toolbar")
        self._host = host
        self._auto_refresh_seconds = auto_refresh_seconds

        self._refresh_button: Button = Button(
            "↻",
            id="toolbar-refresh",
            variant="primary",
            compact=True,
            tooltip="Refresh (r)",
        )
        self._host_label: Static = Static(host, id="toolbar-host")
        # Auto-refresh countdown: a progress bar that fills over
        # ``auto_refresh_seconds`` and triggers a refresh when full.
        self._countdown_label: Static = Static("", id="toolbar-countdown-label")
        self._countdown_bar: ProgressBar = ProgressBar(
            total=auto_refresh_seconds,
            show_percentage=False,
            show_eta=False,
            id="toolbar-countdown-bar",
        )
        self._countdown_elapsed: int = 0
        self._countdown_timer = None
        # Auto-refresh on/off switch.
        self._auto_refresh_enabled: bool = True
        self._auto_switch: Switch = Switch(
            value=True,
            id="toolbar-auto-switch",
            animate=False,
            tooltip="Toggle auto-refresh",
        )
        self._bunch_label: Static = Static("", id="toolbar-bunch")
        self._refreshed_label: Static = Static("", id="toolbar-refreshed")

    def compose(self) -> ComposeResult:
        yield self._refresh_button
        yield self._host_label
        with Horizontal(id="toolbar-auto"):
            yield Static("auto", id="toolbar-auto-label")
            yield self._auto_switch
        with Horizontal(id="toolbar-countdown"):
            yield self._countdown_label
            yield self._countdown_bar
        yield self._bunch_label
        yield Static("", id="toolbar-spacer")
        yield self._refreshed_label

    def on_mount(self) -> None:
        self._countdown_label.update(f"{self._auto_refresh_seconds}s")
        self._countdown_bar.tooltip = (
            f"Auto-refresh every {self._auto_refresh_seconds}s"
        )
        self._countdown_bar.update(total=self._auto_refresh_seconds, progress=0)
        self._countdown_timer = self.set_interval(1.0, self._tick_countdown)

    # -- Toolbar interactions ---------------------------------------

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button is self._refresh_button:
            event.stop()
            self.post_message(self.RefreshRequested())

    def on_switch_changed(self, event: Switch.Changed) -> None:
        if event.switch is self._auto_switch:
            event.stop()
            self._set_auto_refresh(event.value)

    @property
    def auto_refresh_enabled(self) -> bool:
        return self._auto_refresh_enabled

    def _set_auto_refresh(self, enabled: bool) -> None:
        self._auto_refresh_enabled = enabled
        if enabled:
            # Start a fresh countdown so the bar reflects the new cycle.
            self.reset_countdown()
        else:
            # Empty the bar so it's clear nothing is counting down.
            self._countdown_elapsed = 0
            self._countdown_bar.update(total=self._auto_refresh_seconds, progress=0)
        self.post_message(self.AutoRefreshToggled(enabled))

    # -- Auto-refresh countdown -------------------------------------

    def _tick_countdown(self) -> None:
        """Advance the countdown once per second; request refresh when full.

        Skipped while auto-refresh is off, or while a refresh worker is
        already running so the bar doesn't race ahead of an in-flight
        request.
        """
        if not self._auto_refresh_enabled:
            return
        if self._refresh_in_progress():
            return
        self._countdown_elapsed += 1
        self._countdown_bar.update(
            progress=min(self._countdown_elapsed, self._auto_refresh_seconds)
        )
        if self._countdown_elapsed >= self._auto_refresh_seconds:
            self.post_message(self.RefreshRequested())

    def reset_countdown(self) -> None:
        """Restart the countdown from zero (empty bar)."""
        self._countdown_elapsed = 0
        self._countdown_bar.update(total=self._auto_refresh_seconds, progress=0)

    def _refresh_in_progress(self) -> bool:
        for worker in self.app.workers:
            if worker.group == "refresh" and worker.is_running:
                return True
        return False

    # -- Labels -----------------------------------------------------

    def set_bunch(self, name: Optional[str]) -> None:
        if name:
            self._bunch_label.update(Text.from_markup(f"[b]bunch:[/b] {name}"))
        else:
            self._bunch_label.update("")

    def set_refreshed(self, when: Optional[datetime]) -> None:
        if when is None:
            self._refreshed_label.update("")
        else:
            self._refreshed_label.update(
                Text.from_markup(f"last refresh: {when.strftime('%H:%M:%S')}")
            )
