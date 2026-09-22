"""Tests for :mod:`takler.tui.menu`: the action table and the three modals."""

from __future__ import annotations

import pytest
from textual.app import App, ComposeResult
from textual.geometry import Offset
from textual.widgets import OptionList, Static

from takler.core.node_container import NodeContainer
from takler.core.task_node import Task
from takler.tui.menu import (
    FORCE_STATES,
    NODE_ACTIONS,
    ConfirmModal,
    ForceStateMenu,
    NodeActionMenu,
    applicable_actions,
    confirm,
    find_action,
    per_node_actions,
)

from .conftest import text_of


# ---------------------------------------------------------------------------
# Action table (pure)
# ---------------------------------------------------------------------------


def test_action_ids_are_unique() -> None:
    ids = [action.id for action in NODE_ACTIONS]
    assert len(ids) == len(set(ids))


def test_action_keys_are_unique() -> None:
    keys = [action.key for action in NODE_ACTIONS if action.key is not None]
    assert len(keys) == len(set(keys))


def test_every_action_targets_an_action_method_name() -> None:
    for action in NODE_ACTIONS:
        assert action.action
        assert action.action.isidentifier()


def test_per_node_actions_exclude_nodeless_queries() -> None:
    ids = {action.id for action in per_node_actions()}
    assert "refresh" not in ids
    assert "ping" not in ids
    assert "run" in ids
    # Everything else needs a node, so the two sets only differ by those.
    nodeless = {action.id for action in NODE_ACTIONS if not action.needs_node}
    assert ids == {action.id for action in NODE_ACTIONS} - nodeless


def test_applicable_actions_fall_back_to_full_list_without_node() -> None:
    assert applicable_actions(None) == per_node_actions()


def test_run_is_only_offered_for_tasks() -> None:
    task_ids = {a.id for a in applicable_actions(Task("t"))}
    container_ids = {a.id for a in applicable_actions(NodeContainer("f"))}
    assert "run" in task_ids
    assert "run" not in container_ids
    # Non-filtered actions stay available on containers.
    assert "requeue" in container_ids


def test_find_action_round_trip() -> None:
    for action in NODE_ACTIONS:
        assert find_action(action.id) is action
    assert find_action("no-such-action") is None


def test_destructive_actions_are_marked_for_confirmation() -> None:
    force_complete = find_action("force_complete")
    assert force_complete is not None and force_complete.confirm is True
    refresh = find_action("refresh")
    assert refresh is not None and refresh.confirm is False


# ---------------------------------------------------------------------------
# Modals (pilot)
# ---------------------------------------------------------------------------


class MenuHost(App[None]):
    """Minimal host the modals are pushed onto."""

    def __init__(self) -> None:
        super().__init__()
        self.result = None
        self.dismissed = False

    def compose(self) -> ComposeResult:
        yield Static("host")

    def _capture(self, value) -> None:
        self.result = value
        self.dismissed = True


@pytest.mark.anyio
async def test_node_action_menu_lists_actions_and_returns_the_pick() -> None:
    app = MenuHost()
    async with app.run_test(size=(80, 30)) as pilot:
        actions = per_node_actions()
        app.push_screen(
            NodeActionMenu("/flow1/task1", actions, Offset(5, 5)),
            app._capture,
        )
        await pilot.pause()
        menu = app.screen
        assert isinstance(menu, NodeActionMenu)
        options = menu.query_one("#menu-options", OptionList)
        assert [o.id for o in options.options] == [a.id for a in actions]
        assert text_of(menu.query_one("#menu-title", Static)) == "/flow1/task1"

        # Highlight the second entry, then accept it with Enter.
        await pilot.press("down")
        await pilot.press("enter")
        await pilot.pause()
        assert app.dismissed is True
        assert app.result == actions[1].id


@pytest.mark.anyio
async def test_node_action_menu_escape_cancels_with_none() -> None:
    app = MenuHost()
    async with app.run_test(size=(80, 30)) as pilot:
        app.push_screen(
            NodeActionMenu("/flow1/task1", per_node_actions(), Offset(5, 5)),
            app._capture,
        )
        await pilot.pause()
        await pilot.press("escape")
        await pilot.pause()
        assert app.dismissed is True
        assert app.result is None


@pytest.mark.anyio
async def test_node_action_menu_click_outside_cancels() -> None:
    app = MenuHost()
    async with app.run_test(size=(80, 30)) as pilot:
        app.push_screen(
            NodeActionMenu("/flow1/task1", per_node_actions(), Offset(40, 20)),
            app._capture,
        )
        await pilot.pause()
        # (0, 0) is far from the anchor at (40, 20): outside the card.
        await pilot.click(None, offset=(0, 0))
        await pilot.pause()
        assert app.dismissed is True
        assert app.result is None


@pytest.mark.anyio
async def test_force_state_menu_disables_the_current_state() -> None:
    app = MenuHost()
    async with app.run_test(size=(80, 30)) as pilot:
        app.push_screen(
            ForceStateMenu("/flow1/task1", current_state="queued", anchor=Offset(5, 5)),
            app._capture,
        )
        await pilot.pause()
        menu = app.screen
        assert isinstance(menu, ForceStateMenu)
        assert "Force on /flow1/task1" in text_of(menu.query_one("#menu-title", Static))

        options = menu.query_one("#menu-options", OptionList)
        by_id = {o.id: o for o in options.options}
        assert [o.id for o in options.options] == [state for state, _ in FORCE_STATES]
        assert by_id["queued"].disabled is True
        assert "(current)" in by_id["queued"].prompt
        assert by_id["complete"].disabled is False

        # The highlight skips the disabled entry; accepting lands on the
        # first enabled state.
        await pilot.press("enter")
        await pilot.pause()
        assert app.dismissed is True
        assert app.result == "complete"


@pytest.mark.anyio
async def test_confirm_modal_keys() -> None:
    for key, expected in (("y", True), ("Y", True), ("n", False), ("escape", False)):
        app = MenuHost()
        async with app.run_test(size=(80, 30)) as pilot:
            app.push_screen(ConfirmModal("really?"), app._capture)
            await pilot.pause()
            await pilot.press(key)
            await pilot.pause()
            assert app.dismissed is True, f"key {key!r} did not dismiss"
            assert app.result is expected, f"key {key!r} -> {app.result!r}"


@pytest.mark.anyio
async def test_confirm_modal_enter_defaults_to_cancel() -> None:
    # The cancel button holds the initial focus, so a bare Enter is a No.
    app = MenuHost()
    async with app.run_test(size=(80, 30)) as pilot:
        app.push_screen(ConfirmModal("really?"), app._capture)
        await pilot.pause()
        from textual.widgets import Button

        assert app.screen.focused is app.screen.query_one("#confirm-no", Button)
        await pilot.press("enter")
        await pilot.pause()
        assert app.result is False


@pytest.mark.anyio
async def test_confirm_modal_button_click_confirms() -> None:
    app = MenuHost()
    async with app.run_test(size=(80, 30)) as pilot:
        app.push_screen(ConfirmModal("really?", title="Sure?"), app._capture)
        await pilot.pause()
        await pilot.click("#confirm-yes")
        await pilot.pause()
        assert app.result is True


@pytest.mark.anyio
async def test_confirm_helper_runs_callback_only_on_yes() -> None:
    app = MenuHost()
    calls = []
    async with app.run_test(size=(80, 30)) as pilot:
        confirm(app, "really?", lambda: calls.append("yes"))
        await pilot.pause()
        await pilot.press("n")
        await pilot.pause()
        assert calls == []

        confirm(app, "really?", lambda: calls.append("yes"))
        await pilot.pause()
        await pilot.press("y")
        await pilot.pause()
        assert calls == ["yes"]
