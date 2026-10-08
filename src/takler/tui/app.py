"""Main Textual application for the takler TUI.

The app owns :class:`~takler.tui.service.TaklerTuiService`, runs background
summary refreshes, deltas, and selected-node detail reads, and routes the
resulting compact view into the tree, tabs, and toolbar. It also dispatches
server control actions.

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
    TabbedContent,
    TabPane,
)
from takler.exceptions import QueryResponseError

from .menu import (
    NODE_ACTIONS,
    NodeAction,
    NodeActionMenu,
    ForceStateMenu,
    applicable_actions,
    confirm,
    find_action,
)
from .query_view import QueryTuiView
from .service import TaklerTuiService
from .show_parser import NodeInfo, ShowSnapshot
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
    bindings.append(Binding("s", "scope_selected", "Open subtree"))
    bindings.append(Binding("b", "scope_parent", "Parent scope"))
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

    def __init__(self, service: TaklerTuiService, scope_path: str = "/") -> None:
        super().__init__()
        self.service = service
        self._scope_path = scope_path
        self._snapshot: Optional[ShowSnapshot | QueryTuiView] = None
        self._refresh_seq = 0
        self._detail_seq = 0
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

    def on_toolbar_refresh_requested(self, event: Toolbar.RefreshRequested) -> None:
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

        QueryDocument pagination and compact-view construction run on a worker
        thread so the UI stays responsive. Results are applied on the main
        thread via :meth:`_apply_snapshot`.

        Resets the auto-refresh countdown so a manual refresh also
        restarts the timer.
        """
        self._toolbar.reset_countdown()
        self._refresh_seq += 1
        self._refresh_worker(self._refresh_seq, self._scope_path)

    @work(thread=True, exclusive=True, group="refresh")
    def _refresh_worker(self, sequence: int, scope_path: str) -> None:
        try:
            current = self._snapshot
            if (
                isinstance(current, QueryTuiView)
                and current.query.scope_path == scope_path
                and current.query.revision is not None
            ):
                try:
                    updated = self.service.read_since(
                        current.query,
                        cancelled=lambda: sequence != self._refresh_seq,
                    )
                except QueryResponseError as exc:
                    if exc.code not in {
                        "reset_required",
                        "stale_identity",
                        "unsupported_capability",
                    }:
                        raise
                else:
                    if updated is current.query:
                        self.call_from_thread(self._apply_unchanged, current, sequence)
                        return
                    if not updated.stale_scopes:
                        root_reply = (
                            self.service.read_detail(
                                updated,
                                "/",
                                ["service", "parameters"],
                                current=lambda: sequence == self._refresh_seq,
                            )
                            if scope_path == "/" and "/" in updated.changed_paths
                            else None
                        )
                        self.call_from_thread(
                            self._apply_delta, current, updated, root_reply, sequence
                        )
                        return
            compact = self.service.read_snapshot(
                scope_path=None if scope_path == "/" else scope_path,
                cancelled=lambda: sequence != self._refresh_seq,
            )
            if isinstance(compact, ShowSnapshot):
                # Older widget test doubles return an already parsed view.
                snapshot = compact
            else:
                snapshot = QueryTuiView(compact)
                root = (
                    self.service.read_detail(
                        compact,
                        "/",
                        ["service", "parameters"],
                        current=lambda: sequence == self._refresh_seq,
                    )
                    if scope_path == "/"
                    else self.service.read_root_detail(
                        cancelled=lambda: sequence != self._refresh_seq
                    )
                )
                if root is None:
                    return
                snapshot.apply_root_detail(root)
        except Exception as exc:  # pragma: no cover - network errors
            if isinstance(exc, QueryResponseError) and exc.code == "cancelled":
                return
            self.call_from_thread(self._refresh_failed, sequence, str(exc))
            return
        self.call_from_thread(self._apply_snapshot, snapshot, sequence)

    def _apply_unchanged(self, view: QueryTuiView, sequence: int) -> None:
        if sequence != self._refresh_seq or view is not self._snapshot:
            return
        self._status_bar.set_stale(False)
        self._toolbar.set_refreshed(datetime.now())

    def _apply_delta(
        self, view: QueryTuiView, updated, root_reply, sequence: int
    ) -> None:
        if sequence != self._refresh_seq or view is not self._snapshot:
            return
        changed = updated.changed_paths
        removed = updated.removed_paths
        selected_changed = (
            self._selected_path in changed or self._selected_path in removed
        )
        if selected_changed or root_reply is not None:
            self._detail_seq += 1
        view.apply_delta(updated)
        self._tree.apply_query_delta(view, changed, removed)
        if root_reply is not None:
            view.apply_root_detail(root_reply)
            self._status_bar.set_service(view.service)
        node = view.get(self._selected_path) if self._selected_path else None
        if node is None and self._selected_path is not None:
            self._selected_path = None
            self._render_tabs(None)
        elif node is not None and (selected_changed or root_reply is not None):
            self._render_tabs(node)
            self._request_detail(node.path)
        self._status_bar.set_stale(False)
        self._toolbar.set_refreshed(datetime.now())

    def _refresh_failed(self, sequence: int, message: str) -> None:
        if sequence == self._refresh_seq:
            self._status_bar.set_stale(True)
            self._set_status(f"refresh failed: {message}", "bold red")

    def _apply_snapshot(
        self, snapshot: ShowSnapshot | QueryTuiView, sequence: int | None = None
    ) -> None:
        """Apply a freshly-fetched snapshot to the UI (main thread)."""
        if sequence is not None and sequence != self._refresh_seq:
            return
        self._detail_seq += 1
        self._snapshot = snapshot
        self._status_bar.set_stale(False)
        self._status_bar.set_service(snapshot.service)
        if isinstance(snapshot, QueryTuiView):
            self._tree.rebuild_query(snapshot)
        else:
            self._tree.rebuild(snapshot)
        node = snapshot.get(self._selected_path) if self._selected_path else None
        if node is None:
            self._selected_path = None
        elif isinstance(snapshot, QueryTuiView):
            self._tree.reveal_query_path(node.path)
        self._render_tabs(node)
        if node is not None and isinstance(snapshot, QueryTuiView):
            self._request_detail(node.path)
        self._toolbar.set_bunch(snapshot.name)
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
        if isinstance(self._snapshot, QueryTuiView):
            self._request_detail(path)

    def _request_detail(self, path: str) -> None:
        view = self._snapshot
        if not isinstance(view, QueryTuiView):
            return
        self._detail_seq += 1
        self._detail_worker(view, path, self._detail_seq)

    @work(thread=True, exclusive=True, group="detail")
    def _detail_worker(self, view: QueryTuiView, path: str, sequence: int) -> None:
        try:
            reply = self.service.read_detail(
                view.query,
                path,
                ["parameters", "definition", "runtime", "artifacts"],
                current=lambda: (
                    view is self._snapshot
                    and path == self._selected_path
                    and sequence == self._detail_seq
                ),
            )
            if reply is None:
                return
        except Exception as exc:  # pragma: no cover - network errors
            self.call_from_thread(self._detail_failed, view, path, sequence, str(exc))
            return
        self.call_from_thread(self._apply_detail, view, path, sequence, reply)

    def _detail_failed(
        self, view: QueryTuiView, path: str, sequence: int, message: str
    ) -> None:
        if (
            view is self._snapshot
            and path == self._selected_path
            and sequence == self._detail_seq
        ):
            self._set_status(f"detail failed: {message}", "bold red")

    def _apply_detail(
        self, view: QueryTuiView, path: str, sequence: int, reply
    ) -> None:
        if (
            view is not self._snapshot
            or path != self._selected_path
            or sequence != self._detail_seq
        ):
            return
        node = view.apply_detail(path, reply)
        self._tree.update_query_detail(path)
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

        is_task = node is not None and node.node_kind == "task"

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

    def action_scope_selected(self) -> None:
        """Replace the current view with the selected subtree."""
        if not isinstance(self._snapshot, QueryTuiView):
            return
        path = self._selected_or_warn()
        if path is not None and path in self._snapshot.query.nodes:
            self._scope_path = path
            self.action_refresh()

    def action_scope_parent(self) -> None:
        if self._scope_path == "/":
            return
        previous = self._scope_path
        self._scope_path = previous.rsplit("/", 1)[0] or "/"
        self._selected_path = previous
        self.action_refresh()

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
        return applicable_actions(info)

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
        self.call_from_thread(self._set_status, msg, "green" if ok else "bold red")

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
        if info is not None and info.node_kind != "task":
            self._set_status(
                f"Run is only available on Task nodes ({info.class_name})",
                style="yellow",
            )
            return
        self._run_control("run", lambda p: self.service.run([p], force=False))

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
            lambda p: self.service.force_state([p], state="complete", recursive=False),
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
        self.call_from_thread(self._set_status, f"Force {state}: {path}", "green")
        self.call_from_thread(self.action_refresh)

    def action_free_dep(self) -> None:
        self._run_control(
            "free_dep",
            lambda p: self.service.free_dep([p], dep_type="all"),
        )

    # -- Lifecycle --------------------------------------------------

    def on_unmount(self) -> None:
        self.service.close()
