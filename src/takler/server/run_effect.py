"""Complete, bounded RUN write-set evidence for conservatively checked builtins.

Qualification deliberately walks the live tree on each target. Extensions can
replace hooks/serializers without notifying the query journal; a cached query
revision or a Task type check cannot certify this operation.
"""

from types import MethodType
import sys

from takler.core import Bunch, Flow, NodeContainer, Task
from takler.core._run_effect import CORE_CLASSES, CORE_HELPERS
from takler._run_effect import class_members as _methods, stamp as _stamp
from takler.core.bunch import ServerState, ServerConfig
from takler.core.calendar import Calendar
from takler.core.state import State, NodeStatus
from takler.core.limit import InLimitManager
from takler.core.parameter import Parameter
from takler.core.meter import Meter
from takler.core.event import Event
from takler.core.task_node import TaskNodeGeneratedParameters
from takler.core import task_node
from takler.serialization.version import VersionStore, _RUN_EFFECT_BASELINE
from takler.serialization import registry as registry_module
from takler.server.query_changes import QueryChanges

try:
    from takler.logging.backends.loguru_backend import LoguruNamedLogger, LoguruBackend
    from loguru._logger import Logger as LoguruLogger, Core as LoguruCore
except ImportError:
    LoguruNamedLogger = LoguruBackend = LoguruLogger = LoguruCore = None


_RUNTIME = (
    "task_id",
    "aborted_reason",
    "try_no",
    "definition_generation",
    "attempt_id",
    "submission_unknown_reason",
    "submitted_at",
    "started_at",
    "ended_at",
    "job_path",
    "jobout_path",
    "orvix_info_path",
)
_NODE_TYPES = (Bunch, Flow, NodeContainer, Task)
_CLASSES = {**CORE_CLASSES, **registry_module._RUN_EFFECT_CLASSES}
_OVERRIDES = {cls: frozenset(members) for cls, members in _CLASSES.items()}
_SCHEDULER = None
_HELPERS = ()


def initialize(scheduler_type):
    """Capture shipped implementations at scheduler module load, before hooks."""
    global _SCHEDULER, _HELPERS
    _SCHEDULER = scheduler_type
    classes = (QueryChanges, scheduler_type)
    _CLASSES[VersionStore] = _RUN_EFFECT_BASELINE
    _OVERRIDES[VersionStore] = frozenset(_RUN_EFFECT_BASELINE)
    for cls in classes:
        methods = _methods(cls)
        _CLASSES[cls] = {name: _stamp(value) for name, value in methods.items()}
        _OVERRIDES[cls] = frozenset(methods)
    module = sys.modules[scheduler_type.__module__]
    _HELPERS = (
        CORE_HELPERS
        + registry_module._RUN_EFFECT_HELPERS
        + ((module, "ensure_synchronous_task", _stamp(module.ensure_synchronous_task)),)
    )


def _plain(value, cls):
    return type(value) is cls and vars(value).keys().isdisjoint(_OVERRIDES[cls])


def _quiet_logger(value):
    # An enabled user sink/filter/patcher can change arbitrary nodes. Only the
    # builtin adapter's suppressed INFO path is certified here.
    if LoguruNamedLogger is None or not _plain(value, LoguruNamedLogger):
        return False
    backend, logger = value._backend, value._logger
    return (
        _plain(backend, LoguruBackend)
        and _plain(logger, LoguruLogger)
        and _plain(logger._core, LoguruCore)
        and type(logger._core.min_level) is int
        and logger._core.min_level > 20
        and type(logger._core.levels_lookup) is dict
        and type(logger._core.levels_lookup.get("INFO")) is tuple
        and type(logger._core.levels_lookup["INFO"][2]) is int
        and logger._core.levels_lookup["INFO"][2] == 20
    )


def _builtin_registry(registry):
    """Builtin classes can still have extension registrations; reject those."""
    if not _plain(registry, registry_module.TypeRegistry):
        return False
    if type(registry._ids) is not dict or type(registry._types) is not dict:
        return False
    expected = {
        "takler.bunch",
        "takler.flow",
        "takler.container",
        "takler.task",
        "takler.shell",
    }
    if (
        any(type(key) is not str for key in registry._ids)
        or set(registry._ids) != expected
    ):
        return False
    if len(registry._types) != 5:
        return False
    from takler.tasks.shell import ShellScriptTask

    fields = {
        "type_id",
        "python_type",
        "kind",
        "definition_schema",
        "export_definition",
        "construct",
        "runtime_schema",
        "export_runtime",
        "restore_runtime",
        "query",
        "secret_fields",
    }
    # Exact default registrations have no user code or mutable type-data aliases.
    for cls, type_id, kind in (
        (Bunch, "takler.bunch", "bunch"),
        (Flow, "takler.flow", "flow"),
        (NodeContainer, "takler.container", "container"),
        (Task, "takler.task", "task"),
        (ShellScriptTask, "takler.shell", "task"),
    ):
        entries = [entry for key, entry in registry._types.items() if key is cls]
        if (
            len(entries) != 1
            or type(entries[0]) is not registry_module.NodeRegistration
        ):
            return False
        entry = entries[0]
        data = vars(entry)
        if any(type(name) is not str for name in data) or set(data) != fields:
            return False
        if (
            type(data["type_id"]) is not str
            or data["type_id"] != type_id
            or type(data["kind"]) is not str
            or data["kind"] != kind
            or data["python_type"] is not cls
            or registry._ids[type_id] is not entry
        ):
            return False
        if any(
            data[name] is not None
            for name in fields - {"type_id", "python_type", "kind", "secret_fields"}
        ):
            return False
        if type(data["secret_fields"]) is not frozenset or data["secret_fields"]:
            return False
    return True


class RunEffect:
    def __init__(self, target, ancestors):
        self.target = target
        self.ancestors = ancestors
        self.parameters = tuple(
            value
            for name, value in vars(target.generated_parameters).items()
            if name != "node"
        )
        self.before = self.current()

    def current(self):
        # Immutable values of the ENTIRE serialized write set. No query hints,
        # serializers, mutable aliases or revision counters are used as evidence.
        return (
            tuple(vars(self.target)[name] for name in _RUNTIME),
            tuple(node.state.node_status for node in (self.target, *self.ancestors)),
            # Generated parameters can also be aliased from public parameters
            # on unrelated nodes. Track their values even across those aliases.
            tuple(parameter.value for parameter in self.parameters),
        )

    def effect(self):
        return "none" if self.current() == self.before else "partial"


def controlled_run(scheduler, target):
    """Return complete evidence, or None to retain the original full snapshots.

    No extension callable is invoked during qualification. A prepared generation
    and absent limits/repeats bound the write set to target runtime and ancestor
    statuses. Builtin tracking changes only ephemeral query/version metadata.
    """
    if (
        _SCHEDULER is None
        or type(scheduler) is not _SCHEDULER
        or type(target) is not str
    ):
        return None
    for cls, expected in _CLASSES.items():
        if {name: _stamp(value) for name, value in _methods(cls).items()} != expected:
            return None
    if not _plain(scheduler, _SCHEDULER):
        return None
    if any(_stamp(getattr(module, name)) != stamp for module, name, stamp in _HELPERS):
        return None
    from takler.server import scheduler as scheduler_module

    if not _quiet_logger(task_node.logger) or not _quiet_logger(
        scheduler_module.logger
    ):
        return None
    bunch = scheduler.bunch
    if not _plain(bunch, Bunch) or type(bunch.flows) is not dict:
        return None
    if (
        not _plain(bunch.server_state, ServerState)
        or not _plain(bunch.server_config, ServerConfig)
        or type(bunch._service_stopping) is not bool
    ):
        return None
    tracker, store = scheduler.query_changes, scheduler.version_store
    if not _plain(tracker, QueryChanges) or not _plain(store, VersionStore):
        return None
    if bunch._query_tracker is not tracker or store.bunch is not bunch:
        return None
    # Inspect the current context without calling a potentially lazy factory.
    # An unbuilt/default or extension registry retains legacy serialization.
    current_registry = registry_module._current.get()
    if current_registry is None:
        current_registry = registry_module._default
    if not _builtin_registry(current_registry) or not _builtin_registry(store.registry):
        return None
    for callback, function in (
        (tracker.on_mark, VersionStore.note),
        (tracker.on_commit, VersionStore.enqueue),
    ):
        if (
            type(callback) is not MethodType
            or callback.__self__ is not store
            or callback.__func__ is not function
        ):
            return None
    if any(
        type(name) is not str or type(flow) is not Flow or flow.parent is not None
        for name, flow in bunch.flows.items()
    ):
        return None
    pending = [bunch, *bunch.flows.values()]
    seen = set()
    while pending:
        node = pending.pop()
        if id(node) in seen:
            return None
        seen.add(id(node))
        cls = type(node)
        if cls not in _NODE_TYPES or not _plain(node, cls):
            return None
        if (
            type(node.name) is not str
            or not _plain(node.state, State)
            or type(node.state.node_status) is not NodeStatus
            or type(node.children) is not list
            or not _plain(node.in_limit_manager, InLimitManager)
            or node.in_limit_manager.node is not node
            or type(node.in_limit_manager.in_limit_list) is not list
            or node.in_limit_manager.in_limit_list
            or type(node.limits) is not list
            or node.limits
            or node.repeat is not None
            or type(node.times) is not list
            or node.times
            or node.trigger_expression is not None
            or node.complete_trigger_expression is not None
            or type(node.user_parameters) is not dict
            or type(node.events) is not list
            or type(node.meters) is not list
        ):
            return None
        # Most tasks have no values here. Avoid allocating three generator
        # objects per node merely to certify empty builtin collections.
        if node.user_parameters and any(
            not _plain(value, Parameter) for value in node.user_parameters.values()
        ):
            return None
        if node.events and any(not _plain(value, Event) for value in node.events):
            return None
        if node.meters and any(not _plain(value, Meter) for value in node.meters):
            return None
        if cls in (Bunch, Flow, NodeContainer):
            counts = node._status_counts
            if counts is not None and (
                type(counts) is not dict
                or any(type(key) is not NodeStatus for key in counts)
                or len(counts) != len(NodeStatus)
                or any(type(value) is not int for value in counts.values())
            ):
                return None
            if type(node._status_child_count) is not int:
                return None
        if cls is Task:
            if node.children:
                return None
        elif cls is Flow and (
            not _plain(node.calendar, Calendar)
            or type(node.begun) is not bool
            or node.bunch is not bunch
            or type(node.definition_generation) is not str
        ):
            return None
        if any(
            type(child) not in _NODE_TYPES or child.parent is not node
            for child in node.children
        ):
            return None
        pending.extend(node.children)
        # Comparing the ordinary builtin path is safe only after its ancestors
        # have been checked; locate the target after the complete qualification.
    found = bunch.find_node(target)
    if type(found) is not Task:
        return None
    ancestors = []
    node = found.parent
    while node is not None:
        ancestors.append(node)
        node = node.parent
    if not ancestors or type(ancestors[-1]) is not Flow:
        return None
    if any(type(vars(found)[name]) not in (str, int, type(None)) for name in _RUNTIME):
        return None
    generated = found.generated_parameters
    if (
        not _plain(generated, TaskNodeGeneratedParameters)
        or generated.node is not found
    ):
        return None
    if any(
        not _plain(value, Parameter) or type(value.value) not in (str, int, type(None))
        for name, value in vars(generated).items()
        if name != "node"
    ):
        return None
    return RunEffect(found, ancestors)
