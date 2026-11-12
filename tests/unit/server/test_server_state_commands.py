"""Service scheduling commands remain independent of node controls."""

import asyncio

import pytest

from takler.core.bunch import Bunch, HaltCause, ServerState
from takler.protocol.commands import (
    Command,
    ServerHaltCommand,
    ServerResumeCommand,
    ServerStatusRequest,
)
from takler.server.handlers import CommandHandlers
from takler.server.scheduler import Scheduler


def run(handlers, command, request):
    return asyncio.run(handlers.handle(command, request))


def test_status_halt_resume_are_idempotent_and_only_change_service_state():
    bunch = Bunch(name="bunch")
    handlers = CommandHandlers(Scheduler(bunch=bunch))
    assert (
        run(handlers, Command.SERVER_STATUS, ServerStatusRequest()).status == "running"
    )
    first = run(handlers, Command.SERVER_HALT, ServerHaltCommand())
    assert first.flag == 0
    assert first.status == "halted"
    assert first.halt_causes == ["manual"]
    assert first.status_reason == "manual pause"
    assert first.last_checkpoint_at is None
    assert first.restore_summary is None
    assert run(handlers, Command.SERVER_HALT, ServerHaltCommand()).halt_causes == [
        "manual"
    ]
    assert (
        run(handlers, Command.SERVER_STATUS, ServerStatusRequest()).status == "halted"
    )
    assert (
        run(handlers, Command.SERVER_RESUME, ServerResumeCommand()).status == "running"
    )
    assert run(handlers, Command.SERVER_RESUME, ServerResumeCommand()).halt_causes == []


def test_checkpoint_failure_blocks_resume_and_reports_actual_state():
    bunch = Bunch(name="bunch")
    bunch.server_state = ServerState.from_causes(
        [HaltCause.manual, HaltCause.recovery, HaltCause.checkpoint_failure]
    )
    handlers = CommandHandlers(Scheduler(bunch=bunch))
    response = run(handlers, Command.SERVER_RESUME, ServerResumeCommand())
    assert response.flag == 33
    assert response.status == "halted"
    assert response.status_reason == "checkpoint failure"
    assert response.halt_causes == ["checkpoint_failure"]
    assert run(handlers, Command.SERVER_STATUS, ServerStatusRequest()).halt_causes == [
        "checkpoint_failure"
    ]


def test_invalid_state_cannot_be_installed():
    with pytest.raises(ValueError):
        ServerState(status="paused")


def test_state_writes_audit_success_and_blocked_resume(tmp_path):
    import json
    from pathlib import Path

    import takler.logging
    from takler.server.audit import AuditLogger

    audit_path = tmp_path / "audit.jsonl"
    bunch = Bunch(name="bunch")
    handlers = CommandHandlers(
        Scheduler(bunch=bunch), audit_logger=AuditLogger(str(audit_path))
    )
    takler.logging.configure(console=False, audit_file=str(audit_path))
    try:
        run(handlers, Command.SERVER_STATUS, ServerStatusRequest())
        run(handlers, Command.SERVER_HALT, ServerHaltCommand())
        bunch.server_state = ServerState.from_causes(
            [HaltCause.manual, HaltCause.checkpoint_failure]
        )
        response = run(handlers, Command.SERVER_RESUME, ServerResumeCommand())
        assert response.flag == 33
    finally:
        takler.logging.configure(console=False)
    records = [json.loads(line) for line in Path(audit_path).read_text().splitlines()]
    assert [(r["command"], r["outcome"], r["error_code"]) for r in records] == [
        ("server_halt", "success", 0),
        ("server_resume", "error", 33),
    ]


def test_blocked_resume_is_business_failure_under_fail_fast_policy():
    from takler.server.connect_config import ExceptionPolicy

    bunch = Bunch(name="bunch")
    bunch.server_state = ServerState.from_causes([HaltCause.checkpoint_failure])
    shutdowns = []
    handlers = CommandHandlers(
        Scheduler(bunch=bunch),
        exception_policy=ExceptionPolicy.FAIL_FAST,
        fatal_shutdown=lambda: shutdowns.append(True),
    )
    response = run(handlers, Command.SERVER_RESUME, ServerResumeCommand())
    assert response.flag == 33
    assert response.status == "halted"
    assert shutdowns == []
