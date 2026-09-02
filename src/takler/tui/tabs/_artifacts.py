"""Shared helpers for locating a task's on-disk job artifacts.

Both the ``job`` and ``output`` tabs need to derive where a task's
generated files live. The rule mirrors
``ShellScriptTaskGeneratedParameters`` on the server side::

    <TAKLER_HOME><node_path>.<suffix>

where ``node_path`` already starts with ``/`` so it concatenates
directly onto ``TAKLER_HOME``. Centralising the derivation here keeps
the two tabs from drifting apart.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..show_parser import NodeInfo, ShowSnapshot


def artifact_prefix(
    node: "NodeInfo", snapshot: Optional["ShowSnapshot"]
) -> Optional[Path]:
    """Return ``<TAKLER_HOME><node_path>`` for ``node``, or ``None``.

    ``None`` is returned when there is no snapshot or the node has no
    resolvable ``TAKLER_HOME`` parameter (e.g. a container, or a task
    on a server whose home is not visible to the TUI host).
    """
    if snapshot is None:
        return None
    home = snapshot.lookup_parameter(node.path, "TAKLER_HOME")
    if not home:
        return None
    # ``node.path`` already starts with '/', so direct concatenation
    # mirrors ``ShellScriptTaskGeneratedParameters``.
    return Path(f"{home}{node.path}")
