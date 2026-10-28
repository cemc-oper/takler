"""Full evidence snapshots reuse only unchanged, unadorned builtin Task hooks."""

from contextvars import Context
from functools import partial, wraps
import inspect
from unittest.mock import Mock

import pytest

from takler.core import Bunch, Task
from takler.core import task_node
from takler.schema.definition import DefinitionError, DefinitionModel
from takler.serialization.registry import (
    NodeRegistration,
    _snapshot_type_checks,
    builtin_registry,
    using_registry,
)
from takler.server import batch
from takler.server.scheduler import Scheduler


async def async_hook(self=None):
    raise AssertionError("async body must never execute")


@pytest.fixture
def tree():
    bunch = Bunch()
    flow = bunch.add_flow("f")
    first = flow.add_task("a")
    second = flow.add_task("b")
    flow.begin()
    return Scheduler(bunch), first, second


def test_full_snapshot_reuses_one_check_and_keeps_deepcopy(tree, monkeypatch):
    scheduler, first, _ = tree
    expected = scheduler.bunch.to_dict()
    ensure = Mock(wraps=task_node.ensure_synchronous_task)
    copy = Mock(wraps=batch.deepcopy)
    monkeypatch.setattr(task_node, "ensure_synchronous_task", ensure)
    monkeypatch.setattr(batch, "deepcopy", copy)
    before = batch._snapshot(scheduler)
    assert before == expected
    assert ensure.call_count == copy.call_count == 1
    assert before["flows"][0]["children"][0] is not expected["flows"][0]["children"][0]
    first.state.suspended = True
    assert not before["flows"][0]["children"][0]["state"]["suspended"]
    after = batch._snapshot(scheduler)
    assert after != before and ensure.call_count == copy.call_count == 2
    # The ordinary registry boundary still validates after the scope has ended.
    builtin_registry().by_type(Task)
    assert ensure.call_count >= 3


@pytest.mark.parametrize(
    "change", ["replace", "code", "call", "partial", "callable", "marker"]
)
@pytest.mark.parametrize("hook_name", ["run", "before_run", "do_run", "after_run"])
def test_hook_changes_between_nodes_are_not_hidden(
    tree, monkeypatch, change, hook_name
):
    scheduler, first, _ = tree
    original = first.to_dict

    def plain(self):
        return True

    monkeypatch.setattr(Task, hook_name, plain)

    class AsyncCallable:
        async def __call__(self):
            raise AssertionError("never execute")

    def serializer():
        value = original()
        if change == "replace":
            monkeypatch.setattr(Task, hook_name, async_hook)
        elif change == "code":
            monkeypatch.setattr(plain, "__code__", async_hook.__code__)
        elif change == "call":
            monkeypatch.setattr(plain, "__call__", async_hook, raising=False)
        elif change == "partial":
            monkeypatch.setattr(Task, hook_name, partial(async_hook))
        elif change == "callable":
            monkeypatch.setattr(Task, hook_name, AsyncCallable())
        else:
            if not hasattr(inspect, "markcoroutinefunction"):
                pytest.skip("coroutine markers require Python 3.12+")
            inspect.markcoroutinefunction(plain)
        return value

    monkeypatch.setattr(first, "to_dict", serializer)
    assert batch._snapshot(scheduler) is None
    with pytest.raises(DefinitionError, match="async_task_unsupported"):
        builtin_registry().by_type(Task)


def test_sync_replacement_is_revalidated_and_snapshot_remains_complete(
    tree, monkeypatch
):
    scheduler, first, _ = tree
    original = first.to_dict
    ensure = Mock(wraps=task_node.ensure_synchronous_task)
    monkeypatch.setattr(task_node, "ensure_synchronous_task", ensure)

    def serializer():
        value = original()
        monkeypatch.setattr(Task, "do_run", lambda self: True)
        return value

    monkeypatch.setattr(first, "to_dict", serializer)
    assert batch._snapshot(scheduler) is not None
    assert ensure.call_count == 2


def test_descriptor_hooks_keep_each_lookup_and_validation(tree, monkeypatch):
    scheduler, _, _ = tree
    lookups = []

    class Hook:
        def __get__(self, obj, cls):
            lookups.append(obj)
            return lambda: True

    monkeypatch.setattr(Task, "do_run", Hook())
    ensure = Mock(wraps=task_node.ensure_synchronous_task)
    monkeypatch.setattr(task_node, "ensure_synchronous_task", ensure)
    assert batch._snapshot(scheduler) is not None
    assert ensure.call_count == 2 and len(lookups) == 2


def test_extensions_validate_each_time_and_keep_alias_detachment(monkeypatch):
    class PluginTask(Task):
        pass

    class PluginDefinition(DefinitionModel):
        pass

    registry = builtin_registry()
    registry.register(
        NodeRegistration(
            "example.snapshot",
            PluginTask,
            "task",
            definition_schema=PluginDefinition,
            export_definition=lambda n: {},
            construct=lambda name, data: PluginTask(name),
        )
    )
    bunch = Bunch()
    flow = bunch.add_flow("f")
    first = flow.add_task(PluginTask("a"))
    flow.add_task(PluginTask("b"))
    bag = {"value": 0}
    original = first.to_dict
    monkeypatch.setattr(first, "to_dict", lambda: {**original(), "alias": bag})
    with using_registry(registry):
        scheduler = Scheduler(bunch)
    ensure = Mock(wraps=task_node.ensure_synchronous_task)
    monkeypatch.setattr(task_node, "ensure_synchronous_task", ensure)
    with using_registry(registry):
        before = batch._snapshot(scheduler)
    assert ensure.call_count == 2 and before is not None
    bag["value"] = 1
    assert before["flows"][0]["children"][0]["alias"]["value"] == 0


def test_extension_serializer_can_invalidate_later_builtin_hook(monkeypatch):
    class PluginTask(Task):
        pass

    class PluginDefinition(DefinitionModel):
        pass

    registry = builtin_registry()
    registry.register(
        NodeRegistration(
            "example.mutator",
            PluginTask,
            "task",
            definition_schema=PluginDefinition,
            export_definition=lambda n: {},
            construct=lambda name, data: PluginTask(name),
        )
    )
    bunch = Bunch()
    flow = bunch.add_flow("f")
    flow.add_task("a")
    mutator = flow.add_task(PluginTask("b"))
    flow.add_task("c")
    original = mutator.to_dict

    def serializer():
        result = original()
        monkeypatch.setattr(Task, "do_run", async_hook)
        return result

    monkeypatch.setattr(mutator, "to_dict", serializer)
    with using_registry(registry):
        assert batch._snapshot(Scheduler(bunch)) is None


def test_scopes_are_nested_isolated_and_reset_after_failure(monkeypatch):
    registry = builtin_registry()
    ensure = Mock(wraps=task_node.ensure_synchronous_task)
    monkeypatch.setattr(task_node, "ensure_synchronous_task", ensure)
    with _snapshot_type_checks():
        registry.by_type(Task)
        registry.by_type(Task)
        assert ensure.call_count == 1
        with pytest.raises(RuntimeError):
            with _snapshot_type_checks():
                registry.by_type(Task)
                raise RuntimeError("scope cleanup")
        registry.by_type(Task)
        assert ensure.call_count == 2
        Context().run(registry.by_type, Task)
        assert ensure.call_count == 3
    registry.by_type(Task)
    registry.by_type(Task)
    assert ensure.call_count == 5


def test_registry_contexts_still_resolve_their_own_entries(tree):
    scheduler, _, _ = tree
    first, second = builtin_registry(), builtin_registry()
    with _snapshot_type_checks():
        with using_registry(first):
            assert batch._snapshot(scheduler) is not None
        del second._types[Task]
        with using_registry(second):
            assert batch._snapshot(scheduler) is None


def test_synchronous_timing_wrappers_reuse_checks(tree, monkeypatch):
    scheduler, _, _ = tree
    original = Task.run

    @wraps(original)
    def observed(*args, **kwargs):
        return original(*args, **kwargs)

    monkeypatch.setattr(Task, "run", observed)
    ensure = Mock(wraps=task_node.ensure_synchronous_task)
    monkeypatch.setattr(task_node, "ensure_synchronous_task", ensure)
    assert batch._snapshot(scheduler) is not None
    assert ensure.call_count == 1
