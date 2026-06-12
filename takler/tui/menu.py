"""Floating menus for tree-node actions and confirmations.

The menu is keyboard- and mouse-friendly: it opens via the ``m`` binding
on the focused tree node, and via right-click on the tree pane. Both
paths push :class:`NodeActionMenu`, a small ``ModalScreen`` whose only
content is an :class:`OptionList`.

A single :data:`NODE_ACTIONS` table feeds both the menu and the global
bindings, so adding a new action is one entry.

:class:`ForceStateMenu` is a second-level menu opened from
``Force…`` — it lists the per-state targets (``complete`` / ``queued``
/ etc.) and disables the option matching the node's current state.

:class:`ConfirmModal` is the third modal in the module — a yes/no
dialog used by destructive actions (e.g. forcing a state).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.geometry import Offset
from textual.screen import ModalScreen
from textual.widgets import Button, OptionList, Static
from textual.widgets.option_list import Option

from takler.core.node import Node
from takler.core.task_node import Task


# Sentinel returned by :class:`NodeActionMenu` when the user picks
# ``Force…`` — the app should open :class:`ForceStateMenu` next.
FORCE_MENU_SENTINEL: str = "__force__"


# Force-state submenu entries. Order roughly mirrors the proto enum and
# keeps the most common targets near the top. ``clear`` / ``set`` are
# event-only and not exposed here.
FORCE_STATES: List[tuple[str, str]] = [
    ("complete", "complete"),
    ("queued", "queued"),
    ("submitted", "submitted"),
    ("active", "active"),
    ("aborted", "aborted"),
    ("unknown", "unknown"),
]


@dataclass(frozen=True)
class NodeAction:
    """One entry in the right-click / ``m`` menu.

    Attributes
    ----------
    id
        Stable identifier; also used as the ``OptionList`` option id.
    label
        Text shown in menu and footer.
    key
        Keyboard binding registered on the main app. ``None`` means
        the action is only reachable from the right-click menu.
    action
        Name of the ``action_*`` method on the app (without the
        ``action_`` prefix), as expected by Textual's binding system.
    needs_node
        Whether the action requires a node to be selected. Actions
        without a target node (e.g. ``Refresh``, ``Ping``) are still
        listed in the footer but excluded from the per-node menu.
    confirm
        Pop a yes/no dialog before running. Use for destructive ops.
    refresh_after
        Trigger an automatic ``action_refresh()`` after a successful
        call. Control operations want this; queries (``refresh``
        itself, ``ping``) do not.
    applies_to
        Predicate over the selected :class:`Node` deciding whether the
        action is offered for that node. Defaults to "applies to every
        node". Use this to hide e.g. ``Run`` on containers, since the
        scheduler only runs ``Task`` nodes.
    """

    id: str
    label: str
    key: Optional[str]
    action: str
    needs_node: bool = True
    confirm: bool = False
    refresh_after: bool = False
    applies_to: Callable[[Node], bool] = field(
        default=lambda node: True, repr=False
    )


def _task_only(node: Node) -> bool:
    """Predicate: only ``Task`` nodes accept the action."""
    return isinstance(node, Task)


NODE_ACTIONS: List[NodeAction] = [
    NodeAction(
        "refresh", "Refresh", "r", "refresh", needs_node=False
    ),
    NodeAction(
        "run",
        "Run",
        "ctrl+r",
        "run_now",
        refresh_after=True,
        applies_to=_task_only,
    ),
    NodeAction("requeue", "Requeue", "ctrl+q", "requeue", refresh_after=True),
    NodeAction("suspend", "Suspend", "ctrl+s", "suspend", refresh_after=True),
    NodeAction("resume", "Resume", "ctrl+u", "resume", refresh_after=True),
    NodeAction(
        "force_complete",
        "Force complete",
        "ctrl+f",
        "force_complete",
        confirm=True,
        refresh_after=True,
    ),
    NodeAction(
        "force",
        "Force…",
        None,
        "force_menu",
        refresh_after=False,
    ),
    NodeAction(
        "free_dep", "Free dependencies", "ctrl+d", "free_dep", refresh_after=True
    ),
    NodeAction("ping", "Ping", "p", "ping", needs_node=False),
]


def per_node_actions() -> List[NodeAction]:
    """Subset of :data:`NODE_ACTIONS` that require a selected node."""
    return [a for a in NODE_ACTIONS if a.needs_node]


def applicable_actions(node: Optional[Node]) -> List[NodeAction]:
    """Per-node actions that apply to ``node``'s concrete type.

    When ``node`` is ``None`` we fall back to :func:`per_node_actions`
    so callers without snapshot context still get the full list.
    """
    actions = per_node_actions()
    if node is None:
        return actions
    return [a for a in actions if a.applies_to(node)]


def find_action(action_id: str) -> Optional[NodeAction]:
    for action in NODE_ACTIONS:
        if action.id == action_id:
            return action
    return None


class _AnchoredOptionMenu(ModalScreen[Optional[str]]):
    """Base class for the small anchored popup menus.

    Subclasses provide the title and the list of :class:`Option`
    entries; this base handles positioning (clamped to the screen),
    focus, selection, escape / click-outside dismissal, and the shared
    CSS. ``dismiss`` returns the selected option's id, or ``None`` when
    cancelled.
    """

    DEFAULT_CSS = """
    _AnchoredOptionMenu {
        align: left top;
        background: transparent;
    }

    _AnchoredOptionMenu > #menu-card {
        width: auto;
        max-width: 40;
        height: auto;
        max-height: 80%;
        background: $panel;
        border: tall $accent;
        padding: 0;
    }

    _AnchoredOptionMenu #menu-title {
        background: $accent;
        color: $text;
        padding: 0 1;
        text-style: bold;
    }

    _AnchoredOptionMenu OptionList {
        height: auto;
        background: $panel;
        padding: 0 1;
    }
    """

    BINDINGS = [
        Binding("escape", "dismiss_none", "Cancel"),
    ]

    # Approximate width used to clamp the menu inside the screen.
    _APPROX_WIDTH: int = 24

    def __init__(self, anchor: Offset) -> None:
        super().__init__()
        self._anchor = anchor

    # -- Subclass hooks ---------------------------------------------

    def _menu_title(self) -> str:
        raise NotImplementedError

    def _menu_options(self) -> List[Option]:
        raise NotImplementedError

    # -- Shared behaviour -------------------------------------------

    def compose(self) -> ComposeResult:
        title = Static(self._menu_title(), id="menu-title")
        option_list = OptionList(*self._menu_options(), id="menu-options")
        with Vertical(id="menu-card"):
            yield title
            yield option_list

    def on_mount(self) -> None:
        # Position the menu near the click. Clamp inside the screen so a
        # click on the bottom row still produces a fully visible menu.
        card = self.query_one("#menu-card")
        screen_size = self.size
        approx_h = min(len(self._menu_options()) + 3, screen_size.height)
        x = max(0, min(self._anchor.x, screen_size.width - self._APPROX_WIDTH))
        y = max(0, min(self._anchor.y, max(0, screen_size.height - approx_h)))
        card.styles.offset = (x, y)
        self.query_one(OptionList).focus()

    def on_option_list_option_selected(
        self, event: OptionList.OptionSelected
    ) -> None:
        event.stop()
        self.dismiss(event.option.id)

    def action_dismiss_none(self) -> None:
        self.dismiss(None)

    def on_click(self, event) -> None:
        # Click outside the menu card cancels.
        card = self.query_one("#menu-card")
        if event.widget is None or not card.region.contains(
            event.screen_x, event.screen_y
        ):
            event.stop()
            self.dismiss(None)


class NodeActionMenu(_AnchoredOptionMenu):
    """A small popup of node actions; dismisses with the chosen action id."""

    _APPROX_WIDTH = 24

    def __init__(
        self,
        node_path: str,
        actions: List[NodeAction],
        anchor: Offset,
    ) -> None:
        super().__init__(anchor)
        self._node_path = node_path
        self._actions = actions

    def _menu_title(self) -> str:
        return self._node_path

    def _menu_options(self) -> List[Option]:
        return [Option(action.label, id=action.id) for action in self._actions]


class ForceStateMenu(_AnchoredOptionMenu):
    """Submenu for choosing which state to force on a node.

    Dismisses with the chosen state name (one of :data:`FORCE_STATES`)
    or ``None`` if the user cancels. The option matching ``current_state``
    is disabled so users can't redundantly force the state the node is
    already in.
    """

    _APPROX_WIDTH = 28

    def __init__(
        self,
        node_path: str,
        current_state: Optional[str],
        anchor: Offset,
    ) -> None:
        super().__init__(anchor)
        self._node_path = node_path
        self._current_state = current_state

    def _menu_title(self) -> str:
        return f"Force on {self._node_path}"

    def _menu_options(self) -> List[Option]:
        items: List[Option] = []
        for state, label in FORCE_STATES:
            disabled = state == self._current_state
            display = f"{label}  (current)" if disabled else label
            items.append(Option(display, id=state, disabled=disabled))
        return items


class ConfirmModal(ModalScreen[bool]):
    """A small yes/no dialog. Dismisses with the chosen boolean."""

    DEFAULT_CSS = """
    ConfirmModal {
        align: center middle;
        background: rgba(0, 0, 0, 0.4);
    }

    ConfirmModal > #confirm-card {
        width: 60;
        height: auto;
        background: $panel;
        border: tall $error;
        padding: 1 2;
    }

    ConfirmModal #confirm-title {
        text-style: bold;
        padding-bottom: 1;
    }

    ConfirmModal #confirm-prompt {
        padding-bottom: 1;
    }

    ConfirmModal #confirm-buttons {
        height: 3;
        align-horizontal: right;
    }

    ConfirmModal Button {
        margin-left: 1;
    }
    """

    BINDINGS = [
        Binding("y", "yes", "Yes", show=False),
        Binding("Y", "yes", "Yes", show=False),
        Binding("enter", "yes", "Yes"),
        Binding("n", "no", "No", show=False),
        Binding("escape", "no", "Cancel"),
    ]

    def __init__(
        self,
        prompt: str,
        title: str = "Confirm",
        confirm_label: str = "Yes",
        cancel_label: str = "No",
        danger: bool = True,
    ) -> None:
        super().__init__()
        self._prompt = prompt
        self._title = title
        self._confirm_label = confirm_label
        self._cancel_label = cancel_label
        self._danger = danger

    def compose(self) -> ComposeResult:
        confirm_button = Button(
            self._confirm_label,
            id="confirm-yes",
            variant="error" if self._danger else "primary",
        )
        cancel_button = Button(self._cancel_label, id="confirm-no")
        with Vertical(id="confirm-card"):
            yield Static(self._title, id="confirm-title")
            yield Static(self._prompt, id="confirm-prompt")
            with Horizontal(id="confirm-buttons"):
                yield cancel_button
                yield confirm_button

    def on_mount(self) -> None:
        # Focus the cancel button by default; users have to either press
        # 'y' or move to confirm explicitly. Avoids accidental confirms
        # via Enter on a freshly-opened dialog.
        self.query_one("#confirm-no", Button).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "confirm-yes")

    def action_yes(self) -> None:
        self.dismiss(True)

    def action_no(self) -> None:
        self.dismiss(False)


def confirm(
    app,
    prompt: str,
    on_yes: Callable[[], None],
    *,
    title: str = "Confirm",
    danger: bool = True,
) -> None:
    """Convenience helper: push a confirm modal, run callback only on yes."""

    def _on_close(answer: Optional[bool]) -> None:
        if answer:
            on_yes()

    app.push_screen(
        ConfirmModal(prompt=prompt, title=title, danger=danger),
        _on_close,
    )
