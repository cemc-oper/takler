"""Pilot tests for :class:`takler.tui.app.TaklerTuiApp`.

The app is driven end to end against :class:`FakeTuiService`: refresh,
selection, tab visibility, key bindings, the right-click/``m`` menu and
the confirm-modified destructive actions. The fake records every
command, so each test asserts what *would* have been sent to the server.
"""

from __future__ import annotations

import pytest
from textual.widgets import Static, TabbedContent

from takler.tui.app import TaklerTuiApp
from takler.tui.menu import (
    NODE_ACTIONS,
    ConfirmModal,
    ForceStateMenu,
    NodeActionMenu,
)

from .conftest import FakeTuiService, text_of, wait_until


# ---------------------------------------------------------------------------
# Bindings (pure)
# ---------------------------------------------------------------------------


def test_bindings_cover_every_keyed_action_plus_extras() -> None:
    by_key = {binding.key: binding.action for binding in TaklerTuiApp.BINDINGS}
    for action in NODE_ACTIONS:
        if action.key is None:
            continue
        assert by_key.get(action.key) == action.action, (
            f"binding for {action.id} missing or mismatched"
        )
    # Keyless actions (right-click only) must not claim a binding.
    for action in NODE_ACTIONS:
        if action.key is None:
            assert action.action not in by_key.values()
    # Navigation extras.
    assert by_key["m"] == "open_menu"
    assert by_key["space"] == "toggle_node"
    assert by_key["q"] == "quit"


# ---------------------------------------------------------------------------
# App interactions (pilot)
# ---------------------------------------------------------------------------


def make_app(fake_service: FakeTuiService) -> TaklerTuiApp:
    return TaklerTuiApp(service=fake_service)


async def started_app(pilot, app: TaklerTuiApp, service: FakeTuiService) -> None:
    """Wait until the initial mount-time refresh has populated the tree."""
    await wait_until(
        lambda: "/flow1/family1/task1" in app._tree._path_nodes,
        pilot,
        message="initial refresh never populated the tree",
    )
    assert service.show_calls, "refresh did not call show"


async def select(pilot, app: TaklerTuiApp, path: str) -> None:
    """Move the cursor to ``path`` and select it (Enter)."""
    app._tree.focus()
    app._tree.select_path(path)
    await pilot.press("enter")
    await wait_until(
        lambda: app._selected_path == path,
        pilot,
        message=f"selection never landed on {path}",
    )


@pytest.mark.anyio
async def test_initial_refresh_populates_tree_and_toolbar(
    fake_service: FakeTuiService,
) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        assert "test_bunch" in text_of(app._toolbar.query_one("#toolbar-bunch", Static))
        assert "last refresh:" in text_of(
            app._toolbar.query_one("#toolbar-refreshed", Static)
        )
        # The TUI asks for every detail section.
        assert fake_service.show_calls[0] == {
            "show_parameter": True,
            "show_trigger": True,
            "show_limit": True,
            "show_event": True,
            "show_meter": True,
        }
        # Sub-title shows where we're connected.
        assert app.sub_title == "fake-host:33083"
    # Leaving the app closes the service.
    assert fake_service.closed is True


@pytest.mark.anyio
async def test_selection_renders_info_tab_and_status_bar(
    fake_service: FakeTuiService,
) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        await select(pilot, app, "/flow1/family1/task1")
        await wait_until(
            lambda: (
                "/flow1/family1/task1"
                in text_of(app._status_bar.query_one("#status-left", Static))
            ),
            pilot,
        )
        assert "/flow1/family1/task1" in text_of(app._info_tab._body)
        assert "Trigger" in text_of(app._info_tab._body)


@pytest.mark.anyio
async def test_task_only_tabs_follow_the_selection(
    fake_service: FakeTuiService,
) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        tabs = app.query_one(TabbedContent)

        # Initially nothing selected: the three task tabs are hidden.
        for tab_id in ("tab-script-pane", "tab-job-pane", "tab-output-pane"):
            assert tabs.get_tab(tab_id).display is False

        await select(pilot, app, "/flow1/family1/task1")
        for tab_id in ("tab-script-pane", "tab-job-pane", "tab-output-pane"):
            assert tabs.get_tab(tab_id).display is True

        # Selecting a container hides them again, and an active task tab
        # falls back to info first.
        tabs.active = "tab-script-pane"
        await select(pilot, app, "/flow1")
        assert tabs.active == "tab-info-pane"
        assert tabs.get_tab("tab-script-pane").display is False


@pytest.mark.anyio
async def test_refresh_key_pulls_again(fake_service: FakeTuiService) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        before = len(fake_service.show_calls)
        await pilot.press("r")
        await wait_until(
            lambda: len(fake_service.show_calls) > before,
            pilot,
            message="r did not trigger a refresh",
        )


@pytest.mark.anyio
async def test_ping_key_reports_to_the_status_bar(
    fake_service: FakeTuiService,
) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        await pilot.press("p")
        await wait_until(lambda: "ping" in fake_service.names(), pilot)
        await wait_until(
            lambda: (
                "pong in" in text_of(app._status_bar.query_one("#status-right", Static))
            ),
            pilot,
            message="pong never reached the status bar",
        )


@pytest.mark.anyio
async def test_menu_dispatch_sends_the_control_command(
    fake_service: FakeTuiService,
) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        await select(pilot, app, "/flow1/family1/task1")

        await pilot.press("m")
        await pilot.pause()
        assert isinstance(app.screen, NodeActionMenu)

        # per-node order: run, requeue, ... -> one down lands on Requeue.
        before = len(fake_service.show_calls)
        await pilot.press("down")
        await pilot.press("enter")
        await wait_until(lambda: "requeue" in fake_service.names(), pilot)
        assert ("requeue", {"paths": ["/flow1/family1/task1"]}) in fake_service.calls
        # requeue refreshes afterwards.
        await wait_until(
            lambda: len(fake_service.show_calls) > before,
            pilot,
            message="requeue did not trigger the post-refresh",
        )


@pytest.mark.anyio
async def test_control_keys_dispatch_to_the_service(
    fake_service: FakeTuiService,
) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        await select(pilot, app, "/flow1/family1/task1")

        await pilot.press("ctrl+s")
        await wait_until(lambda: "suspend" in fake_service.names(), pilot)
        await pilot.press("ctrl+u")
        await wait_until(lambda: "resume" in fake_service.names(), pilot)
        await pilot.press("ctrl+d")
        await wait_until(lambda: "free_dep" in fake_service.names(), pilot)

        assert ("suspend", {"paths": ["/flow1/family1/task1"]}) in fake_service.calls
        assert ("resume", {"paths": ["/flow1/family1/task1"]}) in fake_service.calls
        assert (
            "free_dep",
            {"paths": ["/flow1/family1/task1"], "dep_type": "all"},
        ) in fake_service.calls


@pytest.mark.anyio
async def test_run_on_a_task(fake_service: FakeTuiService) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        await select(pilot, app, "/flow1/family1/task1")
        await pilot.press("ctrl+r")
        await wait_until(lambda: "run" in fake_service.names(), pilot)
        assert (
            "run",
            {"paths": ["/flow1/family1/task1"], "force": False},
        ) in fake_service.calls


@pytest.mark.anyio
async def test_run_on_a_container_is_refused_locally(
    fake_service: FakeTuiService,
) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        await select(pilot, app, "/flow1")
        await pilot.press("ctrl+r")
        await pilot.pause()
        await pilot.pause()
        assert "run" not in fake_service.names()
        assert "Run is only available on Task nodes" in text_of(
            app._status_bar.query_one("#status-right", Static)
        )


@pytest.mark.anyio
async def test_force_complete_requires_confirmation(
    fake_service: FakeTuiService,
) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        await select(pilot, app, "/flow1/family1/task1")

        await pilot.press("ctrl+f")
        await pilot.pause()
        assert isinstance(app.screen, ConfirmModal)
        # Declining sends nothing.
        await pilot.press("n")
        await pilot.pause()
        await pilot.pause()
        assert "force_state" not in fake_service.names()

        await pilot.press("ctrl+f")
        await pilot.pause()
        await pilot.press("y")
        await wait_until(lambda: "force_state" in fake_service.names(), pilot)
        assert (
            "force_state",
            {
                "paths": ["/flow1/family1/task1"],
                "state": "complete",
                "recursive": False,
            },
        ) in fake_service.calls


@pytest.mark.anyio
async def test_force_submenu_flow(fake_service: FakeTuiService) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        await select(pilot, app, "/flow1/family1/task1")

        # Open the node menu and pick "Force…" (last-but-one entry:
        # run, requeue, suspend, resume, force_complete, force, free_dep).
        await pilot.press("m")
        await pilot.pause()
        for _ in range(5):
            await pilot.press("down")
        await pilot.press("enter")
        await pilot.pause()
        assert isinstance(app.screen, ForceStateMenu)

        # task1's state is unknown, so the "unknown" option is disabled;
        # one down from "complete" lands on "queued".
        await pilot.press("down")
        await pilot.press("enter")
        await pilot.pause()
        assert isinstance(app.screen, ConfirmModal)

        await pilot.press("y")
        await wait_until(lambda: "force_state" in fake_service.names(), pilot)
        assert (
            "force_state",
            {"paths": ["/flow1/family1/task1"], "state": "queued", "recursive": False},
        ) in fake_service.calls


@pytest.mark.anyio
async def test_right_click_opens_the_node_menu(fake_service: FakeTuiService) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        # Hover the first tree row (the flow), then right-click it.
        await pilot.hover(app._tree, offset=(2, 0))
        await pilot.click(app._tree, offset=(2, 0), button=3)
        await wait_until(
            lambda: isinstance(app.screen, NodeActionMenu),
            pilot,
            message="right click did not open the node menu",
        )
        assert app.screen._node_path == "/flow1"
        # The click also selected the node.
        await wait_until(lambda: app._selected_path == "/flow1", pilot)
        await pilot.press("escape")


@pytest.mark.anyio
async def test_double_click_toggles_expand(fake_service: FakeTuiService) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        family = app._tree._path_nodes["/flow1/family1"]
        assert family.is_expanded is True
        # The toggle is driven off the hover line, so move the mouse first.
        await pilot.hover(app._tree, offset=(2, 1))
        await pilot.click(app._tree, offset=(2, 1), times=2)
        await wait_until(
            lambda: not app._tree._path_nodes["/flow1/family1"].is_expanded,
            pilot,
            message="double click did not collapse family1",
        )


@pytest.mark.anyio
async def test_space_toggles_the_node_under_the_cursor(
    fake_service: FakeTuiService,
) -> None:
    app = make_app(fake_service)
    async with app.run_test(size=(120, 40)) as pilot:
        await started_app(pilot, app, fake_service)
        app._tree.focus()
        app._tree.select_path("/flow1/family1")
        await pilot.pause()
        await pilot.press("space")
        await wait_until(
            lambda: not app._tree._path_nodes["/flow1/family1"].is_expanded,
            pilot,
        )
        await pilot.press("space")
        await wait_until(
            lambda: app._tree._path_nodes["/flow1/family1"].is_expanded,
            pilot,
        )


@pytest.mark.anyio
async def test_menu_key_without_selection_warns(
    fake_service: FakeTuiService,
) -> None:
    # Nothing selected yet (cursor never moved): m must not open a menu.
    payload = FakeTuiService(_payload_without_cursor_bunch())
    app = make_app(payload)
    async with app.run_test(size=(120, 40)) as pilot:
        await wait_until(
            lambda: "/flow1" in app._tree._path_nodes,
            pilot,
            message="initial refresh never populated the tree",
        )
        app._selected_path = None
        await pilot.press("m")
        await pilot.pause()
        assert not isinstance(app.screen, NodeActionMenu)
        assert "no node selected" in text_of(
            app._status_bar.query_one("#status-right", Static)
        )


def _payload_without_cursor_bunch() -> str:
    from takler.core import Bunch, Flow

    from .conftest import show_payload

    bunch = Bunch(name="b")
    bunch.add_flow(Flow("flow1"))
    return show_payload(bunch)
