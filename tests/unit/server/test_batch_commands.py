import asyncio
from unittest.mock import Mock

import pytest

from takler.core import Bunch
from takler.core.state import NodeStatus
from takler.protocol.commands import (
    Command,
    RequeueCommand,
    SuspendCommand,
    ResumeCommand,
    RunCommand,
    ForceCommand,
    FreeDepCommand,
    BeginCommand,
)
from takler.server.scheduler import Scheduler
from takler.server.handlers import CommandHandlers
from takler.server.connect_config import ExceptionPolicy


@pytest.fixture
def scheduler():
    bunch = Bunch()
    flow = bunch.add_flow("f")
    for name in ("a", "b", "c"):
        flow.add_task(name)
    flow.begin()
    return Scheduler(bunch)


CASES = [
    ("requeue", RequeueCommand, "node_paths", {}),
    ("suspend", SuspendCommand, "node_paths", {}),
    ("resume", ResumeCommand, "node_paths", {}),
    ("run", RunCommand, "node_paths", {}),
    ("force", ForceCommand, "paths", {"state": "complete"}),
    ("free_dep", FreeDepCommand, "paths", {}),
]


@pytest.mark.parametrize("name,model,field,extra", CASES)
@pytest.mark.parametrize(
    "targets",
    [
        ["/f/a", "/missing", "/f/b"],
        ["/missing", "/f/a"],
        ["/f/a", "/missing"],
        ["/missing", "bad"],
        ["/f/a", "/f/b"],
    ],
)
def test_ordered_best_effort(scheduler, name, model, field, extra, targets):
    response = getattr(scheduler, "run_command_" + name)(
        model(**{field: targets}, **extra)
    )
    expected = [10 if t == "/missing" else 11 if t == "bad" else 0 for t in targets]
    assert [r.flag for r in response.results] == expected
    assert [r.target for r in response.results] == targets
    assert [r.index for r in response.results] == list(range(len(targets)))
    assert response.flag == (16 if any(expected) else 0)
    if name == "run":
        for t, flag in zip(targets, expected):
            if not flag:
                assert (
                    scheduler.bunch.find_node(t).state.node_status
                    == NodeStatus.submitted
                )


@pytest.mark.parametrize("name,model,field,extra", CASES)
def test_empty_list_and_malformed_structure(scheduler, name, model, field, extra):
    op = getattr(scheduler, "run_command_" + name)
    assert op(model(**{field: []}, **extra)).flag == 15
    response = asyncio.run(
        CommandHandlers(scheduler).dispatch(
            Command(name.replace("_", "-")),
            lambda: model(**{field: ["/f/a", None]}, **extra),
        )
    )
    assert response.flag == 15 and response.results == []
    assert scheduler.bunch.find_node("/f/a").state.node_status == NodeStatus.queued


def test_duplicate_run_and_force(scheduler):
    response = scheduler.run_command_run(
        RunCommand(node_paths=["/f/a", "/f/a", "/f/b"])
    )
    assert [r.flag for r in response.results] == [0, 14, 0]
    response = scheduler.run_command_run(
        RunCommand(node_paths=["/f/a", "/f/a"], force=True)
    )
    assert [r.flag for r in response.results] == [0, 0]
    assert scheduler.bunch.find_node("/f/a").try_no == 3


def test_execution_failure_continues_and_delays_fail_fast(scheduler, monkeypatch):
    events = []
    a = scheduler.bunch.find_node("/f/a")
    b = scheduler.bunch.find_node("/f/b")

    def fail():
        a.state.suspended = True
        events.append("failed")
        raise RuntimeError("SECRET-MUST-NOT-LEAK")

    monkeypatch.setattr(a, "suspend", fail)
    original = b.suspend

    def succeed():
        events.append("continued")
        original()

    monkeypatch.setattr(b, "suspend", succeed)
    audit = Mock()
    handlers = CommandHandlers(
        scheduler,
        exception_policy=ExceptionPolicy.FAIL_FAST,
        fatal_shutdown=lambda: events.append("shutdown"),
        audit_logger=audit,
    )
    response = asyncio.run(
        handlers.handle(Command.SUSPEND, SuspendCommand(node_paths=["/f/a", "/f/b"]))
    )
    assert events == ["failed", "continued", "shutdown"]
    assert [r.effect for r in response.results] == ["unknown", "applied"]
    assert a.state.suspended and b.state.suspended
    record = audit.record.call_args.args[0]
    assert record.results == [r.model_dump() for r in response.results]
    assert record.error_code == 16
    assert "SECRET" not in record.to_json_line() + response.model_dump_json()


def test_run_sync_failure_and_wrong_types(scheduler, monkeypatch):
    a = scheduler.bunch.find_node("/f/a")
    monkeypatch.setattr(a, "do_run", lambda: False)
    result = scheduler.run_command_run(RunCommand(node_paths=["/f", "/f/a", "/f/b"]))
    assert [r.flag for r in result.results] == [12, 30, 0]
    assert result.results[1].effect == "partial"
    a.add_meter("m", 0, 10)
    a.add_event("e")
    result = scheduler.run_command_force(
        ForceCommand(paths=["/f/a:m", "/f/a:e", "/f/b"], state="queued")
    )
    assert [r.flag for r in result.results] == [12, 13, 0]


def test_begin_snapshot_and_empty():
    scheduler = Scheduler(Bunch())
    assert scheduler.run_command_begin(BeginCommand()).results == []
    scheduler.bunch.add_flow("first").begin()
    scheduler.bunch.add_flow("second")
    result = scheduler.run_command_begin(BeginCommand())
    assert [r.flag for r in result.results] == [14, 0]
    assert scheduler.bunch.find_flow("second").begun
    assert scheduler.run_command_begin(BeginCommand(force=True)).flag == 0


@pytest.mark.parametrize("targets", [["/f", "/f/a"], ["/f/a", "/f"]])
def test_overlapping_targets_execute_in_order(scheduler, monkeypatch, targets):
    seen = []
    for path in targets:
        node = scheduler.bunch.find_node(path)
        original = node.requeue

        def record(*args, _path=path, _op=original, **kwargs):
            seen.append(_path)
            return _op(*args, **kwargs)

        monkeypatch.setattr(node, "requeue", record)
    result = scheduler.run_command_requeue(RequeueCommand(node_paths=targets))
    assert [r.target for r in result.results] == targets
    assert result.flag == 0
    # Requeue of the parent recursively visits a, and the explicit child
    # occurrence still executes independently, in the original order.
    assert seen == (
        ["/f", "/f/a", "/f/a"] if targets[0] == "/f" else ["/f/a", "/f", "/f/a"]
    )


def test_begin_all_does_not_include_new_flows(scheduler, monkeypatch):
    scheduler.bunch.add_flow("second")
    second = scheduler.bunch.find_flow("second")
    original = second.begin

    def begin(*args, **kwargs):
        scheduler.bunch.add_flow("late")
        original(*args, **kwargs)

    monkeypatch.setattr(second, "begin", begin)
    result = scheduler.run_command_begin(BeginCommand())
    assert [r.target for r in result.results] == ["/f", "/second"]
    assert [r.flag for r in result.results] == [14, 0]
    assert not scheduler.bunch.find_flow("late").begun


def test_audit_failure_does_not_change_batch_result(scheduler):
    audit = Mock()
    audit.record.side_effect = OSError("unavailable")
    result = asyncio.run(
        CommandHandlers(scheduler, audit_logger=audit).handle(
            Command.SUSPEND, SuspendCommand(node_paths=["/f/a", "/missing", "/f/b"])
        )
    )
    assert [r.flag for r in result.results] == [0, 10, 0]
    assert scheduler.bunch.find_node("/f/a").state.suspended
    assert scheduler.bunch.find_node("/f/b").state.suspended
    audit.record.assert_called_once()


def test_extension_serialization_failure_cannot_block_other_targets(
    scheduler, monkeypatch
):
    node = scheduler.bunch.find_node("/f/a")
    monkeypatch.setattr(
        node, "to_dict", Mock(side_effect=RuntimeError("cannot serialize"))
    )
    result = scheduler.run_command_suspend(
        SuspendCommand(node_paths=["/f/b", "/missing", "/f/c"])
    )
    assert [r.flag for r in result.results] == [0, 10, 0]
    assert result.results[1].effect == "unknown"
    assert scheduler.bunch.find_node("/f/b").state.suspended
    assert scheduler.bunch.find_node("/f/c").state.suspended
