"""Script tab: dump the contents of the ``TAKLER_SCRIPT`` template file."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..show_parser import NodeInfo, ShowSnapshot
from ._fileview import _FileViewTab


class ScriptTab(_FileViewTab):
    """Render the source ``.takler`` template for the selected task."""

    _TAB_ID = "tab-script"
    _BODY_ID = "script-body"

    DEFAULT_CSS = """
    ScriptTab { padding: 1 2; }
    ScriptTab Static.title { padding-bottom: 1; text-style: bold; }
    ScriptTab #script-body { height: 1fr; }
    """

    def show_node(
        self,
        node: Optional[NodeInfo],
        snapshot: Optional[ShowSnapshot] = None,
    ) -> None:
        if node is None:
            self._show_empty()
            return

        script_path = self._resolve_script(node, snapshot)
        if not script_path:
            self._show_message(
                f"{node.path}: no TAKLER_SCRIPT",
                "This node has no TAKLER_SCRIPT parameter "
                "(it may be a container or a non-shell task).",
            )
            return

        self._render_file(node.path, Path(script_path))

    @staticmethod
    def _resolve_script(
        node: NodeInfo, snapshot: Optional[ShowSnapshot]
    ) -> Optional[str]:
        # Direct override on the node first.
        value = node.user_parameters.get("TAKLER_SCRIPT")
        if value:
            return value
        # Then walk parents — TAKLER_SCRIPT can in principle be set on
        # a container.
        if snapshot is not None:
            value = snapshot.lookup_parameter(node.path, "TAKLER_SCRIPT")
            if value:
                return value
        return None
