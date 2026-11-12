"""R1 child reports are bound to one current Task attempt."""

import asyncio

import pytest

from takler.core import Bunch, Flow, NodeStatus
from takler.exceptions import AttemptMismatchError, InvalidRequestError, ZombieError
from takler.protocol.commands import (
    AbortCommand,
    CompleteCommand,
    EventCommand,
    InitCommand,
    MeterCommand,
)
from takler.server.connect_config import ZombiePolicy
from takler.server.connect_config import AuthMode
from takler.server.auth import (
    CallCredentials,
    set_call_credentials,
    reset_call_credentials,
)
from takler.server.scheduler import Scheduler
from takler.server.zombie import ZombieDetector

OLD = "123e4567-e89b-42d3-a456-426614174000"
CURRENT = "123e4567-e89b-42d3-a456-426614174001"


def scheduler(policy=ZombiePolicy.FAIL):
    bunch = Bunch(name="bunch")
    flow = Flow("f")
    container = flow.add_container("c")
    container.add_event("ready")
    container.add_meter("progress", 0, 100)
    task = container.add_task("t")
    bunch.add_flow(flow)
    task.attempt_id = CURRENT
    task.set_node_status(NodeStatus.submitted)
    return (
        Scheduler(bunch, zombie_detector=ZombieDetector(zombie_policy=policy)),
        task,
        container,
    )


def report(scheduler, command):
    if isinstance(command, InitCommand):
        return asyncio.run(scheduler.run_command_init(command))
    return getattr(
        scheduler,
        f"run_command_{command.__class__.__name__.removesuffix('Command').lower()}",
    )(command)


@pytest.mark.parametrize("policy", list(ZombiePolicy))
@pytest.mark.parametrize(
    "build",
    [
        lambda attempt: InitCommand(
            node_path="/f/c/t", task_id="job", attempt_id=attempt
        ),
        lambda attempt: CompleteCommand(node_path="/f/c/t", attempt_id=attempt),
        lambda attempt: AbortCommand(
            node_path="/f/c/t", reason="failed", attempt_id=attempt
        ),
        lambda attempt: EventCommand(
            node_path="/f/c",
            source_task_path="/f/c/t",
            event_name="ready",
            attempt_id=attempt,
        ),
        lambda attempt: MeterCommand(
            node_path="/f/c",
            source_task_path="/f/c/t",
            meter_name="progress",
            meter_value=50,
            attempt_id=attempt,
        ),
    ],
)
def test_old_and_missing_attempt_never_mutate_under_any_zombie_policy(policy, build):
    service, task, container = scheduler(policy)
    for attempt in (OLD, ""):
        before = (
            task.state.node_status,
            task.task_id,
            task.aborted_reason,
            container.find_event("ready").value,
            container.find_meter("progress").value,
        )
        with pytest.raises(AttemptMismatchError):
            report(service, build(attempt))
        assert (
            task.state.node_status,
            task.task_id,
            task.aborted_reason,
            container.find_event("ready").value,
            container.find_meter("progress").value,
        ) == before


def test_init_duplicate_is_noop_and_conflicting_rid_is_zombie():
    service, task, _ = scheduler()
    command = InitCommand(node_path="/f/c/t", task_id="job", attempt_id=CURRENT)
    report(service, command)
    started_at = task.started_at
    report(service, command)
    assert task.started_at == started_at
    with pytest.raises(ZombieError):
        report(
            service,
            InitCommand(node_path="/f/c/t", task_id="other", attempt_id=CURRENT),
        )
    assert task.task_id == "job"


def test_duplicate_init_cannot_adopt_another_password():
    service, task, _ = scheduler()
    command = InitCommand(node_path="/f/c/t", task_id="job", attempt_id=CURRENT)
    report(service, command)
    task.job_password = "current-password"
    service.zombie_detector = ZombieDetector(
        auth_mode=AuthMode.ENABLED, zombie_policy=ZombiePolicy.ADOPT
    )
    token = set_call_credentials(CallCredentials(job_password="foreign-password"))
    try:
        with pytest.raises(ZombieError):
            report(service, command)
    finally:
        reset_call_credentials(token)
    assert task.job_password == "current-password"


@pytest.mark.parametrize("command", ["complete", "abort"])
def test_terminal_report_cannot_be_repeated_or_conflicted(command):
    service, task, _ = scheduler()
    report(service, InitCommand(node_path="/f/c/t", task_id="job", attempt_id=CURRENT))
    terminal = (
        CompleteCommand(node_path="/f/c/t", attempt_id=CURRENT)
        if command == "complete"
        else AbortCommand(node_path="/f/c/t", attempt_id=CURRENT)
    )
    report(service, terminal)
    ended_at = task.ended_at
    with pytest.raises(AttemptMismatchError):
        report(service, terminal)
    with pytest.raises(AttemptMismatchError):
        report(
            service,
            AbortCommand(node_path="/f/c/t", attempt_id=CURRENT)
            if command == "complete"
            else CompleteCommand(node_path="/f/c/t", attempt_id=CURRENT),
        )
    assert task.ended_at == ended_at


def test_event_source_must_be_current_task_in_target_flow():
    service, task, container = scheduler()
    other = Flow("other")
    other.add_event("ready")
    service.bunch.add_flow(other)
    with pytest.raises(InvalidRequestError):
        report(
            service,
            EventCommand(
                node_path="/other",
                source_task_path="/f/c/t",
                event_name="ready",
                attempt_id=CURRENT,
            ),
        )
    with pytest.raises(InvalidRequestError):
        report(
            service,
            EventCommand(
                node_path="/f/c",
                source_task_path="/f/c",
                event_name="ready",
                attempt_id=CURRENT,
            ),
        )
    assert container.find_event("ready").value is False


def test_invalid_uuid_is_invalid_request():
    service, _, _ = scheduler()
    with pytest.raises(InvalidRequestError):
        report(service, CompleteCommand(node_path="/f/c/t", attempt_id="not-a-uuid"))


def test_missing_identity_precedes_target_lookup():
    service, _, _ = scheduler()
    with pytest.raises(AttemptMismatchError):
        report(service, CompleteCommand(node_path="/missing"))
