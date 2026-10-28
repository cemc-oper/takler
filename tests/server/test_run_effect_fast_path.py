"""Controlled RUN evidence must agree with legacy snapshots, or fall back."""

from unittest.mock import Mock

import pytest

from takler.core import Bunch, NodeStatus, Task
from takler.exceptions import FlowStateError
from takler.logging import configure
from takler.protocol.commands import RunCommand
from takler.server import batch
from takler.server.run_effect import controlled_run
from takler.server.scheduler import Scheduler


@pytest.fixture(autouse=True)
def controlled_logging(monkeypatch):
    # Other logging tests deliberately reset global backends/remove sinks.
    # Own isolated builtin cores so eligibility tests do not depend on order.
    import sys
    from loguru._logger import Core
    from takler.logging.backends.loguru_backend import LoguruBackend
    from takler.core import task_node
    from takler.server import scheduler as scheduler_module

    backend = LoguruBackend()
    loggers = []
    for module, component in (
        (task_node, "core"),
        (scheduler_module, "server.scheduler"),
    ):
        logger = backend.get_named_logger(component)
        logger._logger._core = Core()
        logger._logger.add(sys.stderr, level="WARNING")
        monkeypatch.setattr(module, "logger", logger)
        loggers.append(logger)
    yield
    for logger in loggers:
        logger._logger.remove()


@pytest.fixture
def builtin(monkeypatch):
    # Enabled arbitrary log sinks belong to the compatibility path.
    configure(level="WARNING")
    bunch = Bunch()
    flow = bunch.add_flow("f")
    source = flow.add_task("a")
    other = flow.add_task("b")
    flow.begin()
    scheduler = Scheduler(bunch)
    snapshot = Mock(wraps=batch._snapshot)
    monkeypatch.setattr(batch, "_snapshot", snapshot)
    yield scheduler, source, other, snapshot
    configure(level="INFO")


def test_controlled_success_has_zero_full_snapshots(builtin):
    scheduler, source, _, snapshot = builtin
    assert controlled_run(scheduler, source.node_path) is not None
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.flag == 0 and result.results[0].effect == "applied"
    assert source.attempt_id and source.job_password and source.try_no == 1
    assert source.state.node_status is NodeStatus.submitted
    assert snapshot.call_count == 0


def test_controlled_rejection_and_force_preserve_order_and_identity(builtin):
    scheduler, source, other, snapshot = builtin
    scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    attempt, password = source.attempt_id, source.job_password
    result = scheduler.run_command_run(
        RunCommand(node_paths=[source.node_path, other.node_path])
    )
    assert [item.effect for item in result.results] == ["none", "applied"]
    assert [item.flag for item in result.results] == [14, 0]
    assert (source.attempt_id, source.job_password) == (attempt, password)
    forced = scheduler.run_command_run(
        RunCommand(node_paths=[source.node_path], force=True)
    )
    assert forced.results[0].effect == "applied" and source.try_no == 2
    assert source.attempt_id != attempt and source.job_password != password
    assert snapshot.call_count == 0


@pytest.mark.parametrize(
    "where",
    [
        "target",
        "ancestor",
        "class",
        "tracker",
        "serializer",
        "off_target_serializer",
        "code",
    ],
)
def test_unannounced_hooks_keep_complete_legacy_evidence(builtin, monkeypatch, where):
    scheduler, source, other, snapshot = builtin
    if where == "target":

        def fail():
            other.state.suspended = True
            raise FlowStateError("secret")

        monkeypatch.setattr(source, "before_run", fail)
    elif where == "ancestor":
        original = source.parent.swim_status_change
        monkeypatch.setattr(source.parent, "swim_status_change", lambda: original())
    elif where == "class":
        original = Task.before_run
        monkeypatch.setattr(Task, "before_run", lambda self: original(self))
    elif where == "code":

        def alternate(self):
            return None

        monkeypatch.setattr(Task.before_run, "__code__", alternate.__code__)
    elif where == "tracker":
        scheduler.query_changes.on_mark = lambda *args: None
    else:
        node = source if where == "serializer" else other
        original = node.to_dict
        bag = {"value": 0}
        monkeypatch.setattr(node, "to_dict", lambda: {**original(), "bag": bag})
    assert controlled_run(scheduler, source.node_path) is None
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert snapshot.call_count >= 1
    assert result.results[0].effect == ("partial" if where == "target" else "applied")


def test_qualification_rechecks_between_batch_items(builtin, monkeypatch):
    scheduler, source, other, snapshot = builtin
    original = source.before_run

    def install_next_hook():
        other.before_run = lambda: setattr(source, "aborted_reason", "cross-node")
        original()

    monkeypatch.setattr(source, "before_run", install_next_hook)
    result = scheduler.run_command_run(
        RunCommand(node_paths=[source.node_path, other.node_path])
    )
    assert [r.effect for r in result.results] == ["applied", "applied"]
    assert source.aborted_reason == "cross-node"
    assert snapshot.call_count == 2


def test_limit_retain_compatibility(builtin):
    scheduler, source, _, snapshot = builtin
    source.parent.add_limit("capacity", 1)
    source.add_in_limit("capacity", source.parent.node_path)
    assert controlled_run(scheduler, source.node_path) is None
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.results[0].effect == "applied"
    assert source.parent.find_limit("capacity").value == 1
    assert snapshot.call_count == 1


def test_enabled_logging_retains_compatibility(builtin):
    scheduler, source, _, snapshot = builtin
    import sys
    from takler.core.task_node import logger

    logger._logger.add(sys.stderr, level="INFO")
    assert controlled_run(scheduler, source.node_path) is None
    assert (
        scheduler.run_command_run(RunCommand(node_paths=[source.node_path])).flag == 0
    )
    assert snapshot.call_count == 1


def test_write_set_evidence_agrees_with_serialized_change(builtin):
    scheduler, source, _, _ = builtin
    evidence = controlled_run(scheduler, source.node_path)
    before = scheduler.bunch.to_dict()
    assert evidence.effect() == "none"
    source.run()
    assert scheduler.bunch.to_dict() != before
    assert evidence.effect() == "partial"


def test_missing_generation_retain_compatibility(builtin):
    scheduler, source, other, snapshot = builtin
    source.parent.definition_generation = None
    source.definition_generation = other.definition_generation = None
    assert controlled_run(scheduler, source.node_path) is None
    assert (
        scheduler.run_command_run(RunCommand(node_paths=[source.node_path])).flag == 0
    )
    assert other.definition_generation == source.definition_generation
    assert snapshot.call_count == 1


def test_controlled_handler_commits_fixed_version_and_restores_identity(builtin):
    import asyncio
    from takler.protocol.commands import Command, InitCommand
    from takler.server.handlers import CommandHandlers
    from takler.server.auth import (
        CallCredentials,
        set_call_credentials,
        reset_call_credentials,
    )
    from takler.serialization.checkpoint import restore_checkpoint
    from takler.serialization.version import VersionStore

    scheduler, source, _, snapshot = builtin
    old = scheduler.version_store.pin()

    async def apply():
        handler = CommandHandlers(scheduler)
        run = await handler.handle(
            Command.RUN, RunCommand(node_paths=[source.node_path])
        )
        assert run.results[0].effect == "applied"
        token = set_call_credentials(CallCredentials(job_password=source.job_password))
        try:
            init = await handler.handle(
                Command.INIT,
                InitCommand(
                    node_path=source.node_path,
                    attempt_id=source.attempt_id,
                    task_id="controlled-child",
                ),
            )
        finally:
            reset_call_credentials(token)
        assert init.flag == 0

    asyncio.run(apply())
    fixed = scheduler.version_store.pin()
    assert fixed.state_version > old.state_version
    assert (
        old.public_node(source.node_path)["state"]["status"] == NodeStatus.queued.value
    )
    assert (
        fixed.public_node(source.node_path)["state"]["status"]
        == NodeStatus.active.value
    )
    restored = restore_checkpoint(fixed.checkpoint_document())
    task = restored.find_node(source.node_path)
    assert (task.attempt_id, task.job_password, task.task_id, task.try_no) == (
        source.attempt_id,
        source.job_password,
        "controlled-child",
        1,
    )
    actual = VersionStore(
        restored,
        status_provider=lambda: scheduler.server_status_response().model_dump(),
    ).pin()
    assert actual.node(source.node_path).runtime == fixed.node(source.node_path).runtime
    assert snapshot.call_count == 0


def test_250_normal_fixture_run_init_pairs_take_zero_full_snapshots(monkeypatch):
    import asyncio
    import sys
    import time
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
    from mix100k01 import NS, fixture, prepare

    configure(level="WARNING")
    try:
        bunch, paths = fixture(1000)
        scheduler = Scheduler(bunch)
        snapshot = Mock(wraps=batch._snapshot)
        monkeypatch.setattr(batch, "_snapshot", snapshot)
        credentials = asyncio.run(
            prepare(
                scheduler,
                paths,
                lambda *args, **kwargs: None,
                time.monotonic_ns() + 30 * NS,
            )
        )
        assert len(credentials) == 250
        assert snapshot.call_count == 0
    finally:
        configure(level="INFO")


@pytest.mark.parametrize("where", ["class_hook", "tracker_callback"])
def test_qualification_does_not_invoke_extension_equality(builtin, monkeypatch, where):
    scheduler, source, other, snapshot = builtin
    events = []

    class Extension:
        def __eq__(self, value):
            events.append("equality called")
            other.state.suspended = True
            return False

        def __call__(self, *args):
            other.state.suspended = True
            raise FlowStateError("extension rejected after off-target mutation")

    if where == "class_hook":
        monkeypatch.setattr(Task, "before_run", Extension())
    else:
        scheduler.query_changes.on_mark = Extension()
    assert controlled_run(scheduler, source.node_path) is None
    assert not events and not other.state.suspended
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.results[0].effect == "partial"
    assert other.state.suspended and snapshot.call_count == 2


def test_core_hook_replacement_before_scheduler_import_keeps_compatibility():
    import os
    from pathlib import Path
    import subprocess
    import sys

    code = """
from takler.core import Bunch, Task
from takler.exceptions import FlowStateError
from takler.logging import configure
configure(level="WARNING")
other = None
def early_hook(self):
    other.state.suspended = True
    raise FlowStateError("changed another task")
Task.before_run = early_hook
from takler.server.scheduler import Scheduler
from takler.server.run_effect import controlled_run
from takler.protocol.commands import RunCommand
b = Bunch()
f = b.add_flow("f")
a = f.add_task("a")
other = f.add_task("b")
f.begin()
s = Scheduler(b)
assert controlled_run(s, a.node_path) is None
reply = s.run_command_run(RunCommand(node_paths=[a.node_path]))
assert reply.results[0].effect == "partial"
assert other.state.suspended
"""
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2] / "src"))
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_custom_legality_truth_value_keeps_compatibility(builtin):
    scheduler, source, other, snapshot = builtin

    class CustomBegun:
        def __bool__(self):
            other.state.suspended = True
            return False

    source.parent.begun = CustomBegun()
    assert controlled_run(scheduler, source.node_path) is None
    assert not other.state.suspended
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.results[0].flag == 14
    assert result.results[0].effect == "partial"
    assert other.state.suspended and snapshot.call_count == 2


def test_generated_parameter_alias_is_part_of_complete_write_set(builtin):
    scheduler, source, other, _ = builtin
    # Generated values are normally private, but trusted extensions can retain
    # these ordinary Parameter objects in another node's serialized parameters.
    alias = source.generated_parameters.takler_try_no
    other.user_parameters[alias.name] = alias
    evidence = controlled_run(scheduler, source.node_path)
    before = scheduler.bunch.to_dict()
    source.run()
    assert other.user_parameters[alias.name].value == 1
    assert scheduler.bunch.to_dict() != before and evidence.effect() == "partial"


def test_logging_hook_before_core_import_is_not_blessed_as_builtin():
    import os
    from pathlib import Path
    import subprocess
    import sys

    code = """
from takler.logging.backends import NamedLogger
from takler.exceptions import UnsupportedValueError
from takler.logging import configure
configure(level="WARNING")
armed = False
other = None
def log_hook(self, *args, **kwargs):
    if armed:
        other.state.suspended = True
        raise UnsupportedValueError("changed another task")
NamedLogger.info = log_hook
from takler.core import Bunch
from takler.server.scheduler import Scheduler
from takler.server.run_effect import controlled_run
from takler.protocol.commands import RunCommand
b = Bunch()
f = b.add_flow("f")
a = f.add_task("a")
other = f.add_task("b")
f.begin()
a.run()
prior = a.attempt_id
s = Scheduler(b)
armed = True
assert controlled_run(s, a.node_path) is None
assert not other.state.suspended
reply = s.run_command_run(RunCommand(node_paths=[a.node_path], force=True))
assert reply.results[0].effect == "partial", reply
assert other.state.suspended and a.attempt_id == prior
"""
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2] / "src"))
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_in_place_coroutine_marker_revokes_eligibility(builtin, monkeypatch):
    import inspect

    mark = getattr(inspect, "markcoroutinefunction", None)
    if mark is None:
        pytest.skip("interpreter has no coroutine function marker")
    marked = mark(lambda: None)
    for name, value in vars(marked).items():
        monkeypatch.setattr(Task.before_run, name, value, raising=False)
    scheduler, source, _, snapshot = builtin
    assert controlled_run(scheduler, source.node_path) is None
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.results[0].flag == 13
    assert result.results[0].effect == "unknown"
    assert snapshot.call_count == 1


def test_wrapped_run_operation_code_change_keeps_off_target_evidence(
    builtin, monkeypatch
):
    scheduler, source, other, snapshot = builtin

    def replacement(self, command):
        self.bunch.find_node("/f/b").state.suspended = True
        raise FlowStateError("operation changed another task")

    monkeypatch.setattr(
        Scheduler.run_command_run.__wrapped__, "__code__", replacement.__code__
    )
    assert controlled_run(scheduler, source.node_path) is None
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.results[0].effect == "partial" and other.state.suspended
    assert snapshot.call_count == 2


def test_builtin_python_types_with_extension_registrations_keep_compatibility(builtin):
    from takler.core import Flow, NodeContainer
    from takler.schema.definition import DefinitionModel
    from takler.serialization.registry import (
        NodeRegistration,
        TypeRegistry,
        using_registry,
    )

    _, source, other, snapshot = builtin
    bunch = source.parent.bunch
    source.parent.begun = False
    calls = []

    class Data(DefinitionModel):
        pass

    def export(node):
        if node is source:
            calls.append(node.name)
            other.state.suspended = not other.state.suspended
        return {}

    registry = TypeRegistry()
    for cls, kind in (
        (Bunch, "bunch"),
        (Flow, "flow"),
        (NodeContainer, "container"),
        (Task, "task"),
    ):
        registry.register(
            NodeRegistration(
                "example." + kind,
                cls,
                kind,
                definition_schema=Data,
                export_definition=export,
                construct=lambda name, data: None,
            )
        )
    with using_registry(registry):
        scheduler = Scheduler(bunch)
        assert controlled_run(scheduler, source.node_path) is None
        assert not calls
        result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.results[0].flag == 14 and result.results[0].effect == "partial"
    assert calls == ["a", "a"] and snapshot.call_count == 2


def test_qualification_checks_scheduler_code_before_instance_access(
    builtin, monkeypatch
):
    scheduler, source, _, _ = builtin
    calls = []

    def access(self, name):
        calls.append(name)
        return object.__getattribute__(self, name)

    monkeypatch.setattr(Scheduler, "__getattribute__", access)
    assert controlled_run(scheduler, source.node_path) is None
    assert not calls


def test_instance_scheduler_hook_keeps_complete_compatibility(builtin, monkeypatch):
    scheduler, source, other, snapshot = builtin

    def replacement(*args, **kwargs):
        other.state.suspended = True
        raise FlowStateError("scheduler hook changed another node")

    monkeypatch.setattr(scheduler, "_run_node", replacement)
    assert controlled_run(scheduler, source.node_path) is None
    result = scheduler.run_command_run(RunCommand(node_paths=[source.node_path]))
    assert result.results[0].effect == "partial" and snapshot.call_count == 2
