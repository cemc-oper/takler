"""Main Textual application for the takler TUI.

The app is the orchestrator: it owns the gRPC :class:`TaklerTuiService`,
runs the background refresh worker, routes a fetched
:class:`~takler.tui.show_parser.ShowSnapshot` into the tree / tabs /
toolbar, wires the per-node menus, and dispatches server control
actions.

The visible pieces live in dedicated widgets:

* :class:`~takler.tui.widgets.Toolbar` — refresh button + auto-refresh
  countdown + bunch / last-refresh labels.
* :class:`~takler.tui.widgets.NodeTree` — bunch hierarchy + inline
  attribute rows.
* :class:`~takler.tui.widgets.StatusBar` — selected node + status line.
* :mod:`takler.tui.tabs` — the right-hand info / parameters / script /
  job / output panes.
"""
from __future__ import annotations

from datetime import datetime
from typing import Callable, Dict, Optional

from textual import events, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.geometry import Offset
from textual.widgets import (
    Footer,
    Header,
    Static,
    TabbedContent,
    TabPane,
)

from takler.core.task_node import Task

from .menu import (
    NODE_ACTIONS,
    NodeAction,
    NodeActionMenu,
    ForceStateMenu,
    applicable_actions,
    confirm,
    find_action,
)
from .service import TaklerTuiService
from .show_parser import NodeInfo, ShowSnapshot, parse_show
from .tabs import InfoTab, JobTab, OutputTab, ParametersTab, ScriptTab
from .widgets import NodeTree, StatusBar, Toolbar


def _build_bindings() -> list[Binding]:
    """Bindings derived from :data:`NODE_ACTIONS` plus a few extras.

    Actions whose ``key`` is ``None`` (right-click only) are skipped so
    they don't show up in the footer or claim a global hotkey.
    """
    bindings = [
        Binding(action.key, action.action, action.label)
        for action in NODE_ACTIONS
        if action.key is not None
    ]
    bindings.append(Binding("m", "open_menu", "Menu"))
    bindings.append(Binding("space", "toggle_node", "Expand/Collapse"))
    bindings.append(Binding("q", "quit", "Quit"))
    return bindings


class TaklerTuiApp(App):
    """A read-only-by-default TUI client for a takler server.

    On first mount the app pulls the bunch state once. Afterwards refresh
    is **manual**: press ``r`` or trigger a control action (which also
    refreshes after success). Right-click a tree node (or press ``m``)
    for the per-node menu.

    Bindings
    --------
    Query / navigation: ``r`` refresh, ``p`` ping, ``m`` menu, ``q`` quit.
    Control (require a selected node): ``Ctrl+R`` run, ``Ctrl+Q`` requeue,
    ``Ctrl+S`` suspend, ``Ctrl+U`` resume, ``Ctrl+F`` force complete
    (with confirm), ``Ctrl+D`` free dependencies.

    Notes
    -----
    On terminals where XON/XOFF flow control is enabled, ``Ctrl+S`` /
    ``Ctrl+Q`` may be intercepted by the TTY before reaching the app.
    Run ``stty -ixon`` (or use a terminal that disables flow control by
    default) if those keys appear unresponsive.
    """

    CSS = """
    Screen { layout: vertical; }

    #main {
        height: 1fr;
    }

    #tree-pane {
        width: 40%;
        min-width: 36;
        border-right: tall $primary;
    }

    #tabs-pane {
        width: 1fr;
    }
    """

    BINDINGS = _build_bindings()

    # Seconds between automatic refreshes; the toolbar countdown bar
    # fills over this interval and triggers a refresh when full.
    AUTO_REFRESH_SECONDS: int = 60

    def __init__(self, service: TaklerTuiService) -> None:
        super().__init__()
        self.service = service
        self._snapshot: Optional[ShowSnapshot] = None
        self._tree = NodeTree()
        self._info_tab = InfoTab()
        self._params_tab = ParametersTab()
        self._script_tab = ScriptTab()
        self._job_tab = JobTab()
        self._output_tab = OutputTab()
        # Lazy-tab rendering state: the node currently shown in the tab
        # pane and the set of panes not yet rendered for it.
        self._current_node: Optional[NodeInfo] = None
        self._dirty_panes: set[str] = set()
        self._tab_by_pane = {
            "tab-info-pane": self._info_tab,
            "tab-params-pane": self._params_tab,
            "tab-script-pane": self._script_tab,
            "tab-job-pane": self._job_tab,
            "tab-output-pane": self._output_tab,
        }
        self._toolbar = Toolbar(
            host=self.service.listen_address,
            auto_refresh_seconds=self.AUTO_REFRESH_SECONDS,
        )
        self._status_bar = StatusBar()
        self._selected_path: Optional[str] = None

    # -- Layout ------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield self._toolbar
        with Horizontal(id="main"):
            with Vertical(id="tree-pane"):
                yield self._tree
            with Vertical(id="tabs-pane"):
                with TabbedContent(initial="tab-info-pane"):
                    with TabPane("info", id="tab-info-pane"):
                        yield self._info_tab
                    with TabPane("parameters", id="tab-params-pane"):
                        yield self._params_tab
                    with TabPane("script", id="tab-script-pane"):
                        yield self._script_tab
                    with TabPane("job", id="tab-job-pane"):
                        yield self._job_tab
                    with TabPane("output", id="tab-output-pane"):
                        yield self._output_tab
        yield self._status_bar
        yield Footer()

    def on_mount(self) -> None:
        # Task-only tabs start hidden until a Task is selected.
        self._update_tab_visibility(None)
        self.title = "takler tui"
        self.sub_title = self.service.listen_address
        # Defer the first refresh so the screen finishes mounting before
        # the gRPC call. Errors will be reflected in toasts and toolbar.
        self.call_after_refresh(self.action_refresh)

    # -- Status helpers ---------------------------------------------

    _STATUS_SEVERITY: Dict[str, str] = {
        "green": "information",
        "yellow": "warning",
        "bold red": "error",
        "red": "error",
    }

    def _set_status(self, message: str, style: str = "") -> None:
        """Surface short status updates as both a toast and a status-bar line.

        ``style`` follows the legacy palette (``green`` / ``yellow`` /
        ``bold red``); we map it to Textual's severity levels for the
        toast and hand the raw style to the status bar for the
        persistent right-side label.
        """
        severity = self._STATUS_SEVERITY.get(style, "information")
        self.notify(message, severity=severity, markup=False)
        self._status_bar.set_message(message, style)

    # -- Toolbar messages -------------------------------------------

    def on_toolbar_refresh_requested(
        self, event: Toolbar.RefreshRequested
    ) -> None:
        event.stop()
        self.action_refresh()

    def on_toolbar_auto_refresh_toggled(
        self, event: Toolbar.AutoRefreshToggled
    ) -> None:
        event.stop()
        if event.enabled:
            self._set_status("auto-refresh on", style="green")
        else:
            self._set_status("auto-refresh off", style="yellow")

    # -- Refresh / tree ---------------------------------------------

    def action_refresh(self) -> None:
        """Kick off a background refresh.

        The gRPC ``show`` call and the (potentially expensive)
        ``parse_show`` reconstruction run on a worker thread so the UI
        stays responsive. Results are applied back on the main thread
        via :meth:`_apply_snapshot`.

        Resets the auto-refresh countdown so a manual refresh also
        restarts the timer.
        """
        self._toolbar.reset_countdown()
        self._refresh_worker()

    @work(thread=True, exclusive=True, group="refresh")
    def _refresh_worker(self) -> None:
        try:
            payload = self.service.show(
                show_parameter=True,
                show_trigger=True,
                show_limit=True,
                show_event=True,
                show_meter=True,
            )
            snapshot = parse_show(payload)
        except Exception as exc:  # pragma: no cover - network errors
            self.call_from_thread(
                self._set_status, f"refresh failed: {exc}", "bold red"
            )
            return
        self.call_from_thread(self._apply_snapshot, snapshot)

    def _apply_snapshot(self, snapshot: ShowSnapshot) -> None:
        """Apply a freshly-fetched snapshot to the UI (main thread)."""
        self._snapshot = snapshot
        self._tree.rebuild(snapshot)
        node = (
            snapshot.get(self._selected_path)
            if self._selected_path
            else None
        )
        self._render_tabs(node)
        self._toolbar.set_bunch(snapshot.bunch.name)
        self._toolbar.set_refreshed(datetime.now())

    # -- Selection --------------------------------------------------

    def on_tree_node_selected(self, event: NodeTree.NodeSelected) -> None:
        path = event.node.data
        if not isinstance(path, str):
            return
        self._selected_path = path
        if self._snapshot is None:
            return
        node = self._snapshot.get(path)
        self._render_tabs(node)

    def on_tree_node_highlighted(self, event: NodeTree.NodeHighlighted) -> None:
        path = event.node.data
        if isinstance(path, str):
            self._selected_path = path

    _TASK_ONLY_TAB_IDS = ("tab-script-pane", "tab-job-pane", "tab-output-pane")

    def _render_tabs(self, node: Optional[NodeInfo]) -> None:
        """Render the visible tab now; defer the rest until activated.

        Several tabs (``script`` / ``job`` / ``output``) touch the
        filesystem in ``show_node``; rendering all five on every
        selection wastes work on panes the user can't see. We remember
        the current node, render only the active pane, and mark the
        others dirty so :meth:`on_tabbed_content_tab_activated` can
        render them on demand.
        """
        self._current_node = node
        # Every pane other than the active one becomes stale.
        self._dirty_panes = set(self._tab_by_pane)
        self._update_tab_visibility(node)
        self._render_active_tab()
        self._status_bar.set_selection(node)

    def _render_active_tab(self) -> None:
        """Render whichever tab is currently active, if it's stale."""
        try:
            tabs = self.query_one(TabbedContent)
        except Exception:
            return  # before mount
        active = tabs.active
        if not active or active not in self._dirty_panes:
            return
        tab = self._tab_by_pane.get(active)
        if tab is None:
            return
        tab.show_node(self._current_node, self._snapshot)
        self._dirty_panes.discard(active)

    def on_tabbed_content_tab_activated(
        self, event: TabbedContent.TabActivated
    ) -> None:
        # Render the newly-activated pane lazily if it hasn't been
        # rendered for the current node yet.
        self._render_active_tab()

    def _update_tab_visibility(self, node: Optional[NodeInfo]) -> None:
        """Show ``script`` / ``output`` only when the selected node is a task."""
        try:
            tabs = self.query_one(TabbedContent)
        except Exception:
            return  # called before mount completes

        is_task = node is not None and isinstance(node.node, Task)

        # If we're about to hide the active tab, switch to ``info`` first
        # so Textual doesn't pick an arbitrary sibling on hide.
        if not is_task and tabs.active in self._TASK_ONLY_TAB_IDS:
            tabs.active = "tab-info-pane"

        for tab_id in self._TASK_ONLY_TAB_IDS:
            try:
                if is_task:
                    tabs.show_tab(tab_id)
                else:
                    tabs.hide_tab(tab_id)
            except Exception:
                # Older / newer Textual may raise if tab is unknown.
                continue

    # -- Right-click / menu -----------------------------------------

    def on_click(self, event: events.Click) -> None:
        if not self._is_inside_tree(event):
            return

        # Double-click on a non-leaf node toggles expand / collapse.
        if event.button == 1 and event.chain == 2:
            tree_node = self._tree.node_at_hover()
            if tree_node is None or tree_node.allow_expand is False:
                return
            # Avoid acting through a synthetic attribute leaf (its data
            # is the owning node's path but it isn't the node itself).
            if not self._tree.is_real_node(tree_node):
                return
            event.stop()
            tree_node.toggle()
            return

        # Right-click opens the per-node action menu.
        if event.button != 3:
            return

        path = self._tree.path_at_hover()
        if path is None:
            return

        self._tree.select_path(path)
        event.stop()
        anchor = Offset(event.screen_x, event.screen_y)
        self.push_screen(
            NodeActionMenu(
                node_path=path,
                actions=self._actions_for_path(path),
                anchor=anchor,
            ),
            self._on_menu_dismissed,
        )

    def action_open_menu(self) -> None:
        path = self._selected_or_warn()
        if path is None:
            return
        anchor = self._tree.anchor_for_cursor()
        self.push_screen(
            NodeActionMenu(
                node_path=path,
                actions=self._actions_for_path(path),
                anchor=anchor,
            ),
            self._on_menu_dismissed,
        )

    def action_toggle_node(self) -> None:
        """Expand / collapse the node under the tree cursor."""
        if self._snapshot is None:
            return
        # The cursor may be sitting on a synthetic attribute leaf; only
        # toggle if it's a real node row.
        tree_node = self._tree.node_at_line(self._tree.cursor_line)
        if tree_node is None or tree_node.allow_expand is False:
            return
        if not self._tree.is_real_node(tree_node):
            return
        tree_node.toggle()

    def _on_menu_dismissed(self, action_id: Optional[str]) -> None:
        if action_id is None:
            return
        action = find_action(action_id)
        if action is None:
            return
        method = getattr(self, f"action_{action.action}", None)
        if callable(method):
            method()

    # -- Click → node lookup helpers --------------------------------

    def _is_inside_tree(self, event: events.Click) -> bool:
        widget = event.widget
        while widget is not None:
            if widget is self._tree:
                return True
            widget = getattr(widget, "parent", None)
        return False

    def _node_for_path(self, path: str) -> Optional[NodeInfo]:
        if self._snapshot is None:
            return None
        return self._snapshot.get(path)

    def _actions_for_path(self, path: str) -> list[NodeAction]:
        """Per-node actions filtered by the concrete node type."""
        info = self._node_for_path(path)
        node = info.node if info is not None else None
        return applicable_actions(node)

    # -- Server actions ---------------------------------------------

    def _selected_or_warn(self) -> Optional[str]:
        if not self._selected_path:
            self._set_status("no node selected", style="yellow")
            return None
        return self._selected_path

    def _run_control(
        self,
        action_id: str,
        op: Callable[[str], None],
        *,
        success_style: str = "green",
        success_message: Optional[str] = None,
    ) -> None:
        """Invoke a control op, handling confirm + post-refresh policy.

        The actual ``op`` (a blocking gRPC call) runs on a worker thread
        so the UI stays responsive; status updates and the optional
        post-refresh are marshalled back to the main thread.
        """
        path = self._selected_or_warn()
        if path is None:
            return
        action = find_action(action_id)

        def _do() -> None:
            self._control_worker(
                action,
                action_id,
                path,
                op,
                success_style,
                success_message,
            )

        if action and action.confirm:
            confirm(
                self,
                prompt=(
                    f"{action.label} on [b]{path}[/b]?\n\n"
                    f"This is a destructive operation."
                ),
                on_yes=_do,
                title=f"{action.label}?",
            )
            return

        _do()

    @work(thread=True, group="control")
    def _control_worker(
        self,
        action: Optional[NodeAction],
        action_id: str,
        path: str,
        op: Callable[[str], None],
        success_style: str,
        success_message: Optional[str],
    ) -> None:
        try:
            op(path)
        except Exception as exc:  # pragma: no cover - network errors
            self.call_from_thread(
                self._set_status,
                f"{action.label if action else action_id} failed: {exc}",
                "bold red",
            )
            return
        self.call_from_thread(
            self._set_status,
            success_message or f"{action.label if action else action_id}: {path}",
            success_style,
        )
        if action and action.refresh_after:
            # Re-pull so the tree reflects the new state.
            self.call_from_thread(self.action_refresh)

    @work(thread=True, group="control")
    def action_ping(self) -> None:
        ok, msg = self.service.ping()
        self.call_from_thread(
            self._set_status, msg, "green" if ok else "bold red"
        )

    def action_requeue(self) -> None:
        self._run_control("requeue", lambda p: self.service.requeue([p]))

    def action_suspend(self) -> None:
        self._run_control("suspend", lambda p: self.service.suspend([p]))

    def action_resume(self) -> None:
        self._run_control("resume", lambda p: self.service.resume([p]))

    def action_run_now(self) -> None:
        path = self._selected_or_warn()
        if path is None:
            return
        info = self._node_for_path(path)
        if info is not None and not isinstance(info.node, Task):
            self._set_status(
                f"Run is only available on Task nodes ({info.class_name})",
                style="yellow",
            )
            return
        self._run_control(
            "run", lambda p: self.service.run([p], force=False)
        )

    def action_force_menu(self) -> None:
        """Open the per-state submenu under the selected node.

        The ``Force…`` entry in the right-click menu lands here.
        ``Ctrl+F`` is reserved for the more common ``Force complete``
        path (``action_force_complete``). The submenu disables the
        option matching the node's current state so users can't
        redundantly force the same state.
        """
        path = self._selected_or_warn()
        if path is None:
            return
        info = self._node_for_path(path)
        current_state = info.state if info is not None else None
        anchor = self._tree.anchor_for_cursor()
        self.push_screen(
            ForceStateMenu(
                node_path=path,
                current_state=current_state,
                anchor=anchor,
            ),
            self._on_force_menu_dismissed,
        )

    def action_force_complete(self) -> None:
        """Shortcut for the most common ``Force complete`` operation.

        Bound to ``Ctrl+F`` and exposed at the top level of the menu so
        users can mark a stuck task as done without going through the
        ``Force…`` submenu.
        """
        self._run_control(
            "force_complete",
            lambda p: self.service.force_state(
                [p], state="complete", recursive=False
            ),
        )

    def _on_force_menu_dismissed(self, state: Optional[str]) -> None:
        if state is None:
            return
        path = self._selected_path
        if path is None:
            return

        def _do() -> None:
            self._force_state_worker(path, state)

        confirm(
            self,
            prompt=(
                f"Force [b]{state}[/b] on [b]{path}[/b]?\n\n"
                f"This is a destructive operation."
            ),
            on_yes=_do,
            title=f"Force {state}?",
        )

    @work(thread=True, group="control")
    def _force_state_worker(self, path: str, state: str) -> None:
        try:
            self.service.force_state([path], state=state, recursive=False)
        except Exception as exc:  # pragma: no cover - network errors
            self.call_from_thread(
                self._set_status, f"Force {state} failed: {exc}", "bold red"
            )
            return
        self.call_from_thread(
            self._set_status, f"Force {state}: {path}", "green"
        )
        self.call_from_thread(self.action_refresh)

    def action_free_dep(self) -> None:
        self._run_control(
            "free_dep",
            lambda p: self.service.free_dep([p], dep_type="all"),
        )

    # -- Lifecycle --------------------------------------------------

    def on_unmount(self) -> None:
        self.service.close()
