"""Unsupported async task entry points fail before allocating or running work."""

import asyncio
import json
import warnings

import pytest

from takler.core import Bunch, Flow, NodeStatus, Task, async_task, task
from takler.exceptions import UnsupportedValueError
from takler.protocol.commands import RunCommand
from takler.schema import DefinitionError
from takler.schema.definition import DefinitionModel
from takler.serialization import (
    NodeRegistration,
    build_definition,
    builtin_registry,
    export_definition,
)
from takler.serialization.registry import using_registry
from takler.server import TaklerServer
from takler.server.scheduler import Scheduler


async def _async_hook(self):
    raise AssertionError("async body must not start")


@pytest.mark.parametrize("hook", ["run", "before_run", "do_run", "after_run"])
def test_async_task_subclass_is_rejected_when_declared(hook):
    with pytest.raises(UnsupportedValueError, match="asynchronous task execution"):
        type("UnsupportedTask", (Task,), {hook: _async_hook})


def test_async_decorator_rejects_without_running_or_creating_a_task():
    calls = []

    async def body(self):
        calls.append(self)

    with pytest.raises(UnsupportedValueError, match="async_task is not supported"):
        async_task("work")(body)
    assert calls == []


def test_direct_and_scheduler_entries_reject_modified_async_hooks(monkeypatch):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    node = flow.add_task("t")
    flow.begin()
    before = (node.state.node_status, node.try_no, node.attempt_id)
    monkeypatch.setattr(node, "do_run", _async_hook.__get__(node))

    with pytest.raises(UnsupportedValueError, match="asynchronous task execution"):
        node.run()
    with pytest.raises(UnsupportedValueError, match="asynchronous task execution"):
        node.resolve_dependencies()
    with pytest.raises(UnsupportedValueError, match="asynchronous task execution"):
        Scheduler(bunch)._run_node("/f/t")
    response = Scheduler(bunch).run_command_run(RunCommand(node_paths=["/f/t"]))
    assert response.flag == 16
    assert response.results[0].flag == 13
    assert response.results[0].effect == "none"

    assert before == (node.state.node_status, node.try_no, node.attempt_id)
    assert node.job_password is None


def test_sync_hook_returning_coroutine_is_closed_and_rejected(monkeypatch):
    node = Task("t")
    monkeypatch.setattr(node, "do_run", lambda: _async_hook(node))
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        with pytest.raises(UnsupportedValueError, match="asynchronous task execution"):
            node.run()
    assert node.try_no == 0 and node.attempt_id is None
    assert node.state.node_status is NodeStatus.unknown


def test_async_callable_hook_is_rejected_before_identity(monkeypatch):
    class AsyncCallable:
        async def __call__(self):
            raise AssertionError("async body must not start")

    node = Task("t")
    monkeypatch.setattr(node, "do_run", AsyncCallable())
    with pytest.raises(UnsupportedValueError, match="asynchronous task execution"):
        node.run()
    assert node.try_no == 0 and node.attempt_id is None


def test_server_start_rejects_async_hook_before_restore_or_network(
    monkeypatch, tmp_path
):
    server = TaklerServer(checkpoint_file=tmp_path / "state.check")
    node = server.bunch.add_flow(Flow("f")).add_task("t")
    monkeypatch.setattr(node, "run", _async_hook.__get__(node))
    restore_calls = []
    monkeypatch.setattr(
        server.checkpoint_manager,
        "restore",
        lambda: restore_calls.append("restore"),
    )

    with pytest.raises(UnsupportedValueError, match="asynchronous task execution"):
        asyncio.run(server.start())

    assert restore_calls == []
    assert node.try_no == 0 and node.state.node_status is NodeStatus.unknown
    assert not (tmp_path / "state.check").exists()


def test_registry_and_builder_reject_a_late_async_override(monkeypatch):
    class PluginTask(Task):
        pass

    class PluginDefinition(DefinitionModel):
        pass

    constructed = []

    def construct(name, data):
        constructed.append(name)
        return PluginTask(name)

    entry = NodeRegistration(
        "example.async_candidate",
        PluginTask,
        "task",
        definition_schema=PluginDefinition,
        export_definition=lambda node: {},
        construct=construct,
    )
    registry = builtin_registry()
    registry.register(entry)
    flow = Flow("f")
    flow.add_task(PluginTask("t"))
    with using_registry(registry):
        document = export_definition(flow).model_dump(mode="json")

    monkeypatch.setattr(PluginTask, "do_run", _async_hook)
    with pytest.raises(DefinitionError, match="async_task_unsupported"):
        registry.by_id("example.async_candidate")
    with pytest.raises(DefinitionError, match="async_task_unsupported"):
        build_definition(json.dumps(document), registry=registry)
    with pytest.raises(DefinitionError, match="async_task_unsupported"):
        builtin_registry().register(entry)
    assert constructed == []


def test_sync_inline_task_still_executes():
    calls = []
    node = task("sync")(lambda self: calls.append(self.name))()
    assert node.run() is True
    assert node.state.node_status is NodeStatus.complete
    assert calls == ["sync"]
