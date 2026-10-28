"""Original builtin identities captured before the core package is exposed.

Pure in-memory metadata; no serialization, query revisions or I/O. Keep original
function/code objects alive so later hook replacement (including before server
import) can only revoke RUN eligibility.
"""

from takler._run_effect import fingerprint, stamp

from takler.logging.backends import (
    _RUN_EFFECT_CLASSES as _LOG_CLASSES,
    _RUN_EFFECT_HELPERS as _LOG_HELPERS,
)

from . import task_node as _task_node

from . import Bunch, Flow, NodeContainer, Task
from .node import Node
from .state import State
from .limit import InLimitManager
from .parameter import Parameter
from .meter import Meter
from .event import Event
from .task_node import TaskNodeGeneratedParameters
from .bunch import ServerState, ServerConfig
from .calendar import Calendar


CORE_CLASSES = {
    cls: fingerprint(cls)
    for cls in (
        Node,
        Bunch,
        Flow,
        NodeContainer,
        Task,
        State,
        InLimitManager,
        Parameter,
        Meter,
        Event,
        TaskNodeGeneratedParameters,
        ServerState,
        ServerConfig,
        Calendar,
    )
}


CORE_HELPERS = tuple(
    (module, name, stamp(getattr(module, name)))
    for module, name in (
        (_task_node, "ensure_synchronous_task"),
        (_task_node, "_synchronous_result"),
        (_task_node, "uuid4"),
        (_task_node, "datetime"),
        (_task_node, "timezone"),
        (_task_node.secrets, "token_urlsafe"),
    )
)


# Each logging module freezes its own definitions before exporting classes.
CORE_CLASSES.update(_LOG_CLASSES)
CORE_HELPERS += _LOG_HELPERS
try:
    from takler.logging.backends import loguru_backend as _loguru_backend
except ImportError:
    pass
else:
    CORE_CLASSES.update(_loguru_backend._RUN_EFFECT_CLASSES)
    CORE_HELPERS += _loguru_backend._RUN_EFFECT_HELPERS
