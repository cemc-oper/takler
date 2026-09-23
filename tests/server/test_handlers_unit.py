"""The shared handler cases driven directly against ``CommandHandlers``.

Covers the M3 task 5 handler layer: every command answers its DTO, errors are
classified inside the boundary (including the parse of the request itself),
and the control commands write exactly one Audit_Record. The same cases run
through the gRPC boundary in ``test_handlers_grpc_boundary.py``; the cases,
drivers and assertions live in ``tests/server/conftest.py``.
"""

import asyncio
import json
import pathlib

import pytest

from takler.protocol.commands import (
    Command,
    PingRequest,
    PingResponse,
    RequeueCommand,
    BatchResponse,
    ShowRequest,
)
from takler.server import handlers
from takler.server.audit import AuditLogger
from takler.server.handlers import (
    CONTROL_METHOD_NAMES,
    METHOD_NAME_BY_COMMAND,
    CommandHandlers,
)
from takler.server.protocol import takler_pb2


def test_handler_case(handler_case, run_via_handlers_fixture, assert_handler_case):
    response, bunch = run_via_handlers_fixture(handler_case)
    assert_handler_case(handler_case, response, bunch)


def test_case_list_covers_every_command(handler_cases):
    """The shared suite exercises all sixteen commands at least once."""
    assert {case.command for case in handler_cases} == set(Command)


def test_method_names_match_the_grpc_service_descriptor():
    """Every command's canonical operation name is a method of the proto service.

    The names are how the privilege table and the audit trail refer to the
    commands, so a typo here would silently drop auditing rather than fail.
    """
    service = takler_pb2.DESCRIPTOR.services_by_name["TaklerServer"]
    descriptor_names = {method.name for method in service.methods}
    assert set(METHOD_NAME_BY_COMMAND) == set(Command)
    assert set(METHOD_NAME_BY_COMMAND.values()) == descriptor_names


def test_control_method_names_match_the_control_commands():
    control = {
        Command.REQUEUE,
        Command.SUSPEND,
        Command.RESUME,
        Command.RUN,
        Command.FORCE,
        Command.FREE_DEP,
        Command.LOAD,
        Command.BEGIN,
    }
    assert CONTROL_METHOD_NAMES == {
        METHOD_NAME_BY_COMMAND[command] for command in control
    }


def test_every_command_has_request_info():
    """The dispatch's request-info table covers exactly the sixteen commands."""
    assert set(handlers._REQUEST_INFO_BY_COMMAND) == set(Command)


def test_handle_accepts_an_already_parsed_request(build_scenario_scheduler):
    """``handle`` is the in-process form of ``dispatch``."""
    command_handlers = CommandHandlers(build_scenario_scheduler())

    response = asyncio.run(
        command_handlers.handle(Command.REQUEUE, RequeueCommand(node_paths=["/flow1"]))
    )

    assert isinstance(response, BatchResponse)
    assert response.flag == 0


def test_dispatch_accepts_a_command_name_string(build_scenario_scheduler):
    """An envelope-style string command name resolves to the enum."""
    command_handlers = CommandHandlers(build_scenario_scheduler())

    response = asyncio.run(command_handlers.dispatch("ping", lambda: PingRequest()))

    assert isinstance(response, PingResponse)


def test_an_unknown_command_name_fails_fast(build_scenario_scheduler):
    command_handlers = CommandHandlers(build_scenario_scheduler())

    with pytest.raises(ValueError):
        asyncio.run(command_handlers.dispatch("no_such_command", lambda: ShowRequest()))


def _run_with_audit(action, audit_file):
    """Run ``action`` with the audit sink bound to ``audit_file``.

    Audit records travel through the logging subsystem, so the file only sees
    them while ``takler.logging.configure(audit_file=...)`` is active; the
    teardown re-configures without the sink, which flushes and closes it.
    """
    import contextlib
    import io

    import takler.logging

    takler.logging._reset_configured_state()
    with contextlib.redirect_stderr(io.StringIO()):
        takler.logging.configure(level="DEBUG", console=False, audit_file=audit_file)
        try:
            return action()
        finally:
            takler.logging.configure(level="DEBUG", console=False)


def test_control_command_writes_exactly_one_audit_record(
    tmp_path, build_scenario_scheduler
):
    """The boundary audits a control command on the handler layer (Req 11.2)."""
    audit_file = str(tmp_path / "audit.jsonl")
    command_handlers = CommandHandlers(
        build_scenario_scheduler(), audit_logger=AuditLogger(audit_file)
    )

    def action():
        response = asyncio.run(
            command_handlers.handle(
                Command.REQUEUE, RequeueCommand(node_paths=["/flow1"])
            )
        )
        assert response.flag == 0
        # ``show`` is operator-level but read-only: no record.
        asyncio.run(command_handlers.handle(Command.SHOW, ShowRequest()))

    _run_with_audit(action, audit_file)

    lines = pathlib.Path(audit_file).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["event"] == "control"
    assert record["command"] == "requeue"
    assert record["target"] == ["/flow1"]
    assert record["outcome"] == "success"
    assert record["error_code"] == 0


def test_failed_control_command_is_audited_with_its_error(
    tmp_path, build_scenario_scheduler
):
    audit_file = str(tmp_path / "audit.jsonl")
    command_handlers = CommandHandlers(
        build_scenario_scheduler(), audit_logger=AuditLogger(audit_file)
    )

    def action():
        response = asyncio.run(
            command_handlers.handle(
                Command.REQUEUE, RequeueCommand(node_paths=["/flow1/no_such"])
            )
        )
        assert response.flag == 16
        assert response.results[0].flag == 10

    _run_with_audit(action, audit_file)

    lines = pathlib.Path(audit_file).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["command"] == "requeue"
    assert record["outcome"] == "error"
    assert record["error_code"] == 16
    assert record["results"][0]["flag"] == 10
