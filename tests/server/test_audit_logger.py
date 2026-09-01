"""Unit tests for the Audit_Logger and its three record points.

Task 9.4 of the *m2-security* spec. The module under test is
``takler/server/audit.py``, but an audit record is only worth anything if it is
actually written where the requirements say it is, so this file exercises the
three record points that produce records -- the Control_Command handler
(Requirement 11.2), the Auth_Interceptor's refusal path (Requirement 11.3) and
the Zombie_Detector's disposition (Requirement 11.4) -- and reads the records
back out of the Audit_File the logging subsystem wrote them to.

Five things are pinned:

* **Exactly one record per audited event.** One per Control_Command, whether it
  succeeded or failed, none for a read-only rpc, one per refusal, one per zombie
  disposition. "Exactly one" is the part that rots quietly: a second record
  point added later, or a retry loop around the write, doubles the audit trail
  and nothing else notices.
* **The field values.** ``event`` / ``outcome`` / ``error_code`` against the
  three-value, four-value and flag-derived conventions of Requirements 11.6 and
  11.7, ``user`` falling back to ``unknown`` (Requirement 11.8) and ``target``
  naming every path the command acted on (Requirement 11.9).
* **The routing.** With an Audit_File configured the records reach it and *only*
  it; without one they fall back to the ordinary sinks (Requirements 11.12,
  11.13).
* **The file itself.** Owner-only permissions and an auto-created parent
  directory (Requirements 11.14, 11.16).
* **The failure mode.** An Audit_File that cannot be written produces a WARNING
  on the regular log and leaves the audited RPC's response exactly as it was
  (Requirement 11.15) -- auditing is observability, not availability.

Every behavioral test runs on both logging backends. The ``logging_backend``
fixture pins the process-wide backend singleton rather than constructing a
backend directly (as ``tests/logging/conftest.py`` does), because the code under
test reaches its sink through ``get_logger("audit")``, i.e. through that
singleton.

No test writes a credential value into a test name or an assertion message; the
stand-in secret below is only ever read inside an assertion expression.

Validates: Requirements 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9,
11.10, 11.11, 11.12, 11.13, 11.14, 11.15, 11.16
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import importlib.util
import io
import json
import os
import stat
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pytest

import takler.logging
from takler.core import Bunch, Flow
from takler.core.state import NodeStatus
from takler.exceptions import ZombieError
from takler.logging import backends as logging_backends
from takler.server.audit import (
    AUDIT_FILE_MODE,
    DENIED_ERROR_CODE,
    EVENT_CONTROL,
    EVENT_DENIED,
    EVENT_ZOMBIE,
    OUTCOME_DENIED,
    OUTCOME_ERROR,
    OUTCOME_SUCCESS,
    OUTCOME_ZOMBIE,
    UNKNOWN_USER,
    AuditLogger,
)
from takler.server.auth import (
    METADATA_KEY_SECRET,
    METADATA_KEY_USER,
    SERVICE_METHOD_PREFIX,
    AuthInterceptor,
    CallCredentials,
    CredentialStore,
    reset_call_credentials,
    set_call_credentials,
)
from takler.server.connect_config import AuthMode, ZombiePolicy
from takler.server.network_service import CONTROL_METHOD_NAMES, TaklerService
from takler.server.protocol import takler_pb2
from takler.server.scheduler import Scheduler
from takler.server.zombie import ZombieDetector

# ---------------------------------------------------------------------------
# fixtures and helpers
# ---------------------------------------------------------------------------

#: A caller address, as gRPC would report it.
PEER = "ipv4:127.0.0.1:54321"

#: The operator the whitelist accepts, used for the ``user`` field.
USER = "alice"

#: A stand-in Operator_Secret. Long enough that "the record does not contain the
#: secret" cannot pass by accident.
SECRET = "operator-secret-0123456789abcdef"

TASK3 = "/flow1/task3"
MISSING = "/flow1/no-such-node"

# Whether the optional loguru library is importable here; the loguru backend
# module imports it at module import time, so the backend can only be
# parametrized over when it is present.
LOGURU_INSTALLED = importlib.util.find_spec("loguru") is not None

BACKENDS = ["stdlib"] + (["loguru"] if LOGURU_INSTALLED else [])

# Whether the process can be blocked by directory permissions at all. root
# ignores them, so the "unwritable Audit_File" test cannot be provoked.
RUNNING_AS_ROOT = getattr(os, "geteuid", lambda: 1)() == 0


def _remove_leftover_sinks() -> None:
    """Drop every sink either backend may have installed."""
    from takler.logging.backends.stdlib_backend import (
        ROOT_LOGGER_NAME,
        _MANAGED_HANDLER_FLAG,
    )
    import logging as stdlib_logging

    logger = stdlib_logging.getLogger(ROOT_LOGGER_NAME)
    for handler in list(logger.handlers):
        if getattr(handler, _MANAGED_HANDLER_FLAG, False):
            logger.removeHandler(handler)
            with contextlib.suppress(Exception):
                handler.close()

    if LOGURU_INSTALLED:
        from loguru import logger as loguru_logger

        loguru_logger.remove()


@pytest.fixture(params=BACKENDS)
def logging_backend(request: pytest.FixtureRequest):
    """Pin the process-wide logging backend to one of the two backends.

    :class:`~takler.server.audit.AuditLogger` writes through
    ``get_logger("audit")``, which resolves the backend singleton, so forcing
    that singleton is what makes the same test body exercise both backends.
    """
    from takler.logging.backends.stdlib_backend import StdlibBackend

    original = logging_backends._BACKEND
    _remove_leftover_sinks()

    if request.param == "stdlib":
        logging_backends._BACKEND = StdlibBackend()
    else:
        from takler.logging.backends.loguru_backend import LoguruBackend

        logging_backends._BACKEND = LoguruBackend()
    takler.logging._reset_configured_state()

    try:
        yield request.param
    finally:
        _remove_leftover_sinks()
        logging_backends._BACKEND = original
        # Let the next test's first record re-apply the default configuration
        # on the restored backend.
        takler.logging._reset_configured_state()


@dataclasses.dataclass
class Captured:
    """What one audited action produced, per destination."""

    result: Any
    audit_lines: List[str]
    log_lines: List[str]
    console: str

    @property
    def records(self) -> List[Dict[str, Any]]:
        """The Audit_File contents, one parsed record per line (Req 11.10)."""
        return [json.loads(line) for line in self.audit_lines]

    @property
    def record(self) -> Dict[str, Any]:
        """The single record the action was expected to produce."""
        assert len(self.audit_lines) == 1, (
            f"expected exactly one audit record, got {len(self.audit_lines)}"
        )
        return self.records[0]


def _read_lines(path: Optional[str]) -> List[str]:
    """Lines of ``path``, or ``[]`` when it is unset or was never created."""
    if path is None or not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read().splitlines()


def capture(
    action: Callable[[], Any],
    audit_file: Optional[str] = None,
    log_file: Optional[str] = None,
) -> Captured:
    """Run ``action`` with logging configured, and collect every destination.

    The configuration is applied inside the ``redirect_stderr`` block so both
    backends bind their console sink to the buffer, and the sinks are torn down
    before the files are read back so their handlers are flushed and closed.
    """
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    with contextlib.redirect_stderr(buffer):
        takler.logging.configure(
            level="DEBUG", console=True, log_file=log_file, audit_file=audit_file
        )
        try:
            result = action()
        finally:
            takler.logging.configure(level="DEBUG", console=False)

    return Captured(
        result=result,
        audit_lines=_read_lines(audit_file),
        log_lines=_read_lines(log_file),
        console=buffer.getvalue(),
    )


def raising(action: Callable[[], Any], expected: type) -> Callable[[], Any]:
    """Wrap ``action`` so an expected exception is captured, not propagated."""

    def run() -> Any:
        with pytest.raises(expected) as excinfo:
            action()
        return excinfo.value

    return run


class _Context:
    """The one method ``_audit_control`` reads off a ``ServicerContext``."""

    def __init__(self, peer: Optional[str] = PEER) -> None:
        self._peer = peer

    def peer(self) -> Optional[str]:
        return self._peer


def build_flow(name: str = "flow1") -> Flow:
    """A small begun flow: one container with a task, plus ``/flow1/task3``."""
    flow = Flow(name)
    container1 = flow.add_container("container1")
    container1.add_task("task1")
    flow.add_task("task3")
    return flow


def make_scheduler(zombie_detector=None, begin: bool = True) -> Scheduler:
    """A scheduler over a one-flow bunch, begun unless asked otherwise."""
    bunch = Bunch(name="bunch")
    flow = build_flow()
    bunch.add_flow(flow)
    if begin:
        flow.begin()
    return Scheduler(bunch=bunch, zombie_detector=zombie_detector)


def make_service(
    audit_file: Optional[str] = None,
    scheduler: Optional[Scheduler] = None,
) -> TaklerService:
    """A service wired to an :class:`AuditLogger` for ``audit_file``."""
    return TaklerService(
        scheduler=scheduler if scheduler is not None else make_scheduler(),
        audit_logger=AuditLogger(audit_file),
    )


def call(service: TaklerService, method: str, request: Any) -> Any:
    """Invoke one rpc handler of ``service`` and return its response."""
    return asyncio.run(getattr(service, method)(request, _Context()))


# The eight Control_Commands, each with a request that succeeds, a request that
# fails, and the ``target`` the successful one is expected to record. The table
# is keyed by method name so :func:`test_every_control_command_is_covered` can
# hold it against the service's own classification -- a ninth Control_Command
# added later arrives together with a failing test.
_LOADED_FLOW = json.dumps(Flow("flow2").to_dict()).encode("utf-8")

CONTROL_CASES: "Dict[str, Tuple[Callable[[], Any], Callable[[], Any], List[str]]]" = {
    "RunCommandRequeue": (
        lambda: takler_pb2.RequeueCommand(node_path=[TASK3]),
        lambda: takler_pb2.RequeueCommand(node_path=[MISSING]),
        [TASK3],
    ),
    "RunCommandSuspend": (
        lambda: takler_pb2.SuspendCommand(node_path=[TASK3]),
        lambda: takler_pb2.SuspendCommand(node_path=[MISSING]),
        [TASK3],
    ),
    "RunCommandResume": (
        lambda: takler_pb2.SuspendCommand(node_path=[TASK3]),
        lambda: takler_pb2.SuspendCommand(node_path=[MISSING]),
        [TASK3],
    ),
    "RunCommandRun": (
        lambda: takler_pb2.RunCommand(node_path=[TASK3], force=True),
        lambda: takler_pb2.RunCommand(node_path=[MISSING], force=True),
        [TASK3],
    ),
    "RunCommandForce": (
        lambda: takler_pb2.ForceCommand(
            path=[TASK3], state=takler_pb2.ForceCommand.ForceState.complete
        ),
        lambda: takler_pb2.ForceCommand(
            path=[MISSING], state=takler_pb2.ForceCommand.ForceState.complete
        ),
        [TASK3],
    ),
    "RunCommandFreeDep": (
        lambda: takler_pb2.FreeDepCommand(path=[TASK3]),
        lambda: takler_pb2.FreeDepCommand(path=[MISSING]),
        [TASK3],
    ),
    "RunCommandBegin": (
        lambda: takler_pb2.BeginCommand(flow_name="flow1", force=True),
        lambda: takler_pb2.BeginCommand(flow_name="no-such-flow"),
        ["flow1"],
    ),
    "RunCommandLoad": (
        lambda: takler_pb2.LoadCommand(flow_type="json", flow=_LOADED_FLOW),
        lambda: takler_pb2.LoadCommand(flow_type="yaml", flow=b""),
        # A ``load`` carries a serialized flow rather than a path, so there is
        # nothing to name (see the handler).
        [],
    ),
}


def test_every_control_command_is_covered() -> None:
    """The case table above covers exactly the audited Control_Commands.

    Validates: Requirement 11.2
    """
    assert set(CONTROL_CASES) == set(CONTROL_METHOD_NAMES)


# ---------------------------------------------------------------------------
# one record per Control_Command, success and failure (Requirements 11.2, 11.7)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("method", sorted(CONTROL_CASES))
def test_control_command_success_writes_exactly_one_record(
    logging_backend, tmp_path: Path, method: str
) -> None:
    """A successful Control_Command leaves one ``success`` record behind.

    Validates: Requirements 11.2, 11.5, 11.6, 11.7, 11.9
    """
    audit_file = str(tmp_path / "audit.jsonl")
    success_request, _, expected_target = CONTROL_CASES[method]
    service = make_service(audit_file)

    captured = capture(
        lambda: call(service, method, success_request()), audit_file=audit_file
    )

    assert captured.result.flag == 0
    record = captured.record
    assert record["event"] == EVENT_CONTROL
    assert record["outcome"] == OUTCOME_SUCCESS
    assert record["error_code"] == 0
    assert record["target"] == expected_target
    assert record["peer"] == PEER
    assert set(record) == {
        "timestamp",
        "event",
        "command",
        "user",
        "peer",
        "target",
        "outcome",
        "error_code",
    }


@pytest.mark.parametrize("method", sorted(CONTROL_CASES))
def test_control_command_failure_writes_exactly_one_record(
    logging_backend, tmp_path: Path, method: str
) -> None:
    """A failing Control_Command leaves one ``error`` record behind.

    ``error_code`` is asserted against the ``flag`` the client actually
    receives, which is the invariant Requirement 11.7 is after: the audit trail
    and the response must not be able to disagree.

    Validates: Requirements 11.2, 11.7
    """
    audit_file = str(tmp_path / "audit.jsonl")
    _, failure_request, _ = CONTROL_CASES[method]
    service = make_service(audit_file)

    captured = capture(
        lambda: call(service, method, failure_request()), audit_file=audit_file
    )

    assert captured.result.flag != 0
    record = captured.record
    assert record["event"] == EVENT_CONTROL
    assert record["outcome"] == OUTCOME_ERROR
    assert record["error_code"] == captured.result.flag


def test_read_only_requests_write_no_record(logging_backend, tmp_path: Path) -> None:
    """``show`` and ``ping`` are not Control_Commands and are not recorded.

    The TUI polls ``show`` continuously; recording it would bury the records
    that matter.

    Validates: Requirement 11.2
    """
    audit_file = str(tmp_path / "audit.jsonl")
    service = make_service(audit_file)

    def action() -> None:
        call(service, "RunRequestShow", takler_pb2.ShowRequest())
        call(service, "RunRequestPing", takler_pb2.PingRequest())

    captured = capture(action, audit_file=audit_file)

    assert captured.audit_lines == []


def test_target_names_every_affected_path(logging_backend, tmp_path: Path) -> None:
    """A multi-path command records all of its paths, in order.

    Validates: Requirement 11.9
    """
    audit_file = str(tmp_path / "audit.jsonl")
    service = make_service(audit_file)
    paths = [TASK3, "/flow1/container1/task1", "/flow1/container1"]

    captured = capture(
        lambda: call(
            service, "RunCommandSuspend", takler_pb2.SuspendCommand(node_path=paths)
        ),
        audit_file=audit_file,
    )

    assert captured.result.flag == 0
    assert captured.record["target"] == paths


def test_command_field_is_the_short_command_name(
    logging_backend, tmp_path: Path
) -> None:
    """``command`` reads like the sub-command the operator typed.

    Validates: Requirement 11.5
    """
    audit_file = str(tmp_path / "audit.jsonl")
    service = make_service(audit_file)

    captured = capture(
        lambda: call(
            service, "RunCommandFreeDep", takler_pb2.FreeDepCommand(path=[TASK3])
        ),
        audit_file=audit_file,
    )

    assert captured.record["command"] == "free_dep"


# ---------------------------------------------------------------------------
# user (Requirement 11.8)
# ---------------------------------------------------------------------------


def test_user_comes_from_the_call_credentials(logging_backend, tmp_path: Path) -> None:
    """``user`` is the ``takler-user`` the Auth_Interceptor published.

    Validates: Requirement 11.8
    """
    audit_file = str(tmp_path / "audit.jsonl")
    service = make_service(audit_file)
    token = set_call_credentials(CallCredentials(user=USER, secret=SECRET))

    try:
        captured = capture(
            lambda: call(
                service,
                "RunCommandRequeue",
                takler_pb2.RequeueCommand(node_path=[TASK3]),
            ),
            audit_file=audit_file,
        )
    finally:
        reset_call_credentials(token)

    record = captured.record
    assert record["user"] == USER
    # The record carries no credential value (Requirement 11.11).
    assert SECRET not in captured.audit_lines[0]


def test_missing_user_is_recorded_as_unknown(logging_backend, tmp_path: Path) -> None:
    """A call carrying no ``takler-user`` records the fixed placeholder.

    Validates: Requirement 11.8
    """
    audit_file = str(tmp_path / "audit.jsonl")
    service = make_service(audit_file)

    captured = capture(
        lambda: call(
            service, "RunCommandRequeue", takler_pb2.RequeueCommand(node_path=[TASK3])
        ),
        audit_file=audit_file,
    )

    assert captured.record["user"] == UNKNOWN_USER


# ---------------------------------------------------------------------------
# the rejection record point (Requirement 11.3)
# ---------------------------------------------------------------------------


class _AbortContext:
    """A ``ServicerContext`` that records the abort instead of raising."""

    def __init__(self, peer: Optional[str] = PEER) -> None:
        self._peer = peer
        self.aborted: List[Tuple[Any, str]] = []

    def peer(self) -> Optional[str]:
        return self._peer

    async def abort(self, code: Any, details: str) -> None:
        self.aborted.append((code, details))


@dataclasses.dataclass
class _HandlerCallDetails:
    """The two attributes ``intercept_service`` reads."""

    method: str
    invocation_metadata: Sequence[Tuple[str, str]] = ()


async def _never_called(handler_call_details: Any) -> Any:
    raise AssertionError("a refused RPC must not reach the continuation")


def refuse(
    interceptor: AuthInterceptor,
    method: str,
    metadata: Sequence[Tuple[str, str]],
) -> _AbortContext:
    """Run one RPC that is expected to be refused."""
    context = _AbortContext()

    async def run() -> None:
        handler = await interceptor.intercept_service(
            _never_called,
            _HandlerCallDetails(method=method, invocation_metadata=metadata),
        )
        assert handler is not None
        await handler.unary_unary(object(), context)

    asyncio.run(run())
    assert len(context.aborted) == 1
    return context


@pytest.fixture
def credential_store(tmp_path: Path) -> CredentialStore:
    """A store holding one Operator_Secret and one whitelisted user."""
    secret_file = tmp_path / "secret"
    secret_file.write_text(f"{SECRET}\n")
    whitelist_file = tmp_path / "whitelist"
    whitelist_file.write_text(f"{USER}\n")
    return CredentialStore(secret_file=secret_file, whitelist_file=whitelist_file)


@pytest.mark.parametrize(
    "metadata, expected_user",
    [
        pytest.param([], UNKNOWN_USER, id="no-credentials"),
        pytest.param(
            [(METADATA_KEY_SECRET, "wrong-secret-value"), (METADATA_KEY_USER, USER)],
            USER,
            id="wrong-secret",
        ),
    ],
)
def test_rejection_writes_exactly_one_denied_record(
    logging_backend,
    tmp_path: Path,
    credential_store: CredentialStore,
    metadata: Sequence[Tuple[str, str]],
    expected_user: str,
) -> None:
    """A refused RPC leaves one ``denied`` record with the fixed error code.

    Validates: Requirements 11.3, 11.6, 11.7, 11.8, 11.11
    """
    audit_file = str(tmp_path / "audit.jsonl")
    interceptor = AuthInterceptor(
        auth_mode=AuthMode.ENABLED,
        credential_store=credential_store,
        audit_logger=AuditLogger(audit_file),
    )
    method = SERVICE_METHOD_PREFIX + "RunCommandRequeue"

    captured = capture(
        lambda: refuse(interceptor, method, metadata), audit_file=audit_file
    )

    record = captured.record
    assert record["event"] == EVENT_DENIED
    assert record["outcome"] == OUTCOME_DENIED
    assert record["error_code"] == DENIED_ERROR_CODE
    assert record["command"] == "requeue"
    assert record["user"] == expected_user
    assert record["peer"] == PEER
    # The request body is never parsed on the refusal path, so nothing is known
    # about which nodes the caller meant to act on.
    assert record["target"] == []
    assert SECRET not in captured.audit_lines[0]


# ---------------------------------------------------------------------------
# the zombie record point (Requirement 11.4)
# ---------------------------------------------------------------------------


def make_zombie_scheduler(audit_file: str, policy: ZombiePolicy) -> Scheduler:
    """A scheduler whose ``/flow1/task3`` is queued, so ``complete`` hits Z2."""
    detector = ZombieDetector(
        auth_mode=AuthMode.DISABLED,
        zombie_policy=policy,
        audit_logger=AuditLogger(audit_file),
    )
    return make_scheduler(zombie_detector=detector)


@pytest.mark.parametrize(
    "policy, expected_error_code, raises",
    [
        pytest.param(ZombiePolicy.FAIL, 31, True, id="fail"),
        pytest.param(ZombiePolicy.FOB, 0, False, id="fob"),
        pytest.param(ZombiePolicy.ADOPT, 0, False, id="adopt"),
    ],
)
def test_zombie_disposition_writes_exactly_one_record(
    logging_backend,
    tmp_path: Path,
    policy: ZombiePolicy,
    expected_error_code: int,
    raises: bool,
) -> None:
    """Every disposition leaves one ``zombie`` record, whichever policy applied.

    ``fob`` is why this matters: the client is answered ``flag=0`` and has no
    way to learn its report was dropped, so the record is the only place the
    disposition is visible.

    Validates: Requirements 11.4, 11.6, 11.7, 11.9
    """
    audit_file = str(tmp_path / "audit.jsonl")
    scheduler = make_zombie_scheduler(audit_file, policy)

    def action() -> None:
        scheduler.run_command_complete(TASK3)

    captured = capture(
        raising(action, ZombieError) if raises else action, audit_file=audit_file
    )

    record = captured.record
    assert record["event"] == EVENT_ZOMBIE
    assert record["outcome"] == OUTCOME_ZOMBIE
    assert record["error_code"] == expected_error_code
    assert record["command"] == "complete"
    assert record["target"] == [TASK3]
    assert record["user"] == UNKNOWN_USER


def test_a_clean_child_command_writes_no_record(
    logging_backend, tmp_path: Path
) -> None:
    """A Child_Command that hits no condition is not a zombie and not recorded.

    Validates: Requirement 11.4
    """
    audit_file = str(tmp_path / "audit.jsonl")
    scheduler = make_zombie_scheduler(audit_file, ZombiePolicy.FAIL)
    task3 = scheduler.bunch.find_node(TASK3)
    task3.run()  # -> submitted, so ``complete`` hits nothing

    captured = capture(
        lambda: scheduler.run_command_complete(TASK3), audit_file=audit_file
    )

    assert captured.audit_lines == []


def test_the_zombie_record_carries_no_job_password(
    logging_backend, tmp_path: Path
) -> None:
    """A disposition record names the node, never the Job_Password.

    Validates: Requirements 11.11, 12.1
    """
    audit_file = str(tmp_path / "audit.jsonl")
    scheduler = make_zombie_scheduler(audit_file, ZombiePolicy.ADOPT)
    task3 = scheduler.bunch.find_node(TASK3)
    task3.run()
    node_password = task3.job_password
    assert node_password  # the run generated one; only read inside assertions
    call_password = "call-password-0123456789abcdef"
    # Back to complete, so the stale ``complete`` below hits Z2.
    task3.set_node_status(NodeStatus.complete)
    token = set_call_credentials(CallCredentials(job_password=call_password))

    try:
        captured = capture(
            lambda: scheduler.run_command_complete(TASK3), audit_file=audit_file
        )
    finally:
        reset_call_credentials(token)

    line = captured.audit_lines[0]
    assert call_password not in line
    assert node_password not in line


# ---------------------------------------------------------------------------
# routing (Requirements 11.12, 11.13)
# ---------------------------------------------------------------------------


def test_records_reach_only_the_audit_file(logging_backend, tmp_path: Path) -> None:
    """With an Audit_File configured, records go there and nowhere else.

    Validates: Requirements 11.1, 11.12
    """
    audit_file = str(tmp_path / "audit.jsonl")
    log_file = str(tmp_path / "takler.log")
    service = make_service(audit_file)
    marker = "a regular record of some other component"

    def action() -> Any:
        response = call(
            service, "RunCommandRequeue", takler_pb2.RequeueCommand(node_path=[TASK3])
        )
        takler.logging.get_logger("server.scheduler").info(marker)
        return response

    captured = capture(action, audit_file=audit_file, log_file=log_file)

    line = captured.audit_lines[0]
    assert line not in captured.console
    assert all(line not in log_line for log_line in captured.log_lines)
    # The other direction: a regular record reaches the ordinary destinations
    # and stays out of the Audit_File, whose every line must be valid JSON.
    assert marker in captured.console
    assert any(marker in log_line for log_line in captured.log_lines)
    assert all(marker not in audit_line for audit_line in captured.audit_lines)


def test_records_fall_back_to_the_regular_targets(
    logging_backend, tmp_path: Path
) -> None:
    """Without an Audit_File, records go to the configured sinks.

    Requirement 11.13's point is that they must not vanish.

    Validates: Requirement 11.13
    """
    log_file = str(tmp_path / "takler.log")
    service = make_service(audit_file=None)

    captured = capture(
        lambda: call(
            service, "RunCommandRequeue", takler_pb2.RequeueCommand(node_path=[TASK3])
        ),
        log_file=log_file,
    )

    audit_lines = [line for line in captured.log_lines if f'"{EVENT_CONTROL}"' in line]
    assert len(audit_lines) == 1
    assert '"outcome": "success"' in audit_lines[0]
    assert f'"{EVENT_CONTROL}"' in captured.console


# ---------------------------------------------------------------------------
# the file itself (Requirements 11.14, 11.16)
# ---------------------------------------------------------------------------


def test_audit_file_is_created_owner_only_under_a_new_directory(
    logging_backend, tmp_path: Path
) -> None:
    """The parent directory is created and the file is owner-only.

    Validates: Requirements 11.14, 11.16
    """
    audit_file = tmp_path / "missing" / "nested" / "audit.jsonl"
    service = make_service(str(audit_file))

    captured = capture(
        lambda: call(
            service, "RunCommandRequeue", takler_pb2.RequeueCommand(node_path=[TASK3])
        ),
        audit_file=str(audit_file),
    )

    assert audit_file.exists()
    assert len(captured.audit_lines) == 1
    mode = stat.S_IMODE(audit_file.stat().st_mode)
    assert mode == AUDIT_FILE_MODE


def test_an_existing_audit_file_keeps_its_mode(logging_backend, tmp_path: Path) -> None:
    """Requirement 11.14 constrains the file *this* process creates.

    An Audit_File an operator placed deliberately is appended to, not silently
    re-permissioned.

    Validates: Requirement 11.14
    """
    audit_file = tmp_path / "audit.jsonl"
    audit_file.write_text("")
    audit_file.chmod(0o640)
    service = make_service(str(audit_file))

    captured = capture(
        lambda: call(
            service, "RunCommandRequeue", takler_pb2.RequeueCommand(node_path=[TASK3])
        ),
        audit_file=str(audit_file),
    )

    assert len(captured.audit_lines) == 1
    assert stat.S_IMODE(audit_file.stat().st_mode) == 0o640


# ---------------------------------------------------------------------------
# write failures (Requirement 11.15)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(RUNNING_AS_ROOT, reason="root ignores directory permissions")
def test_an_unwritable_audit_file_warns_and_leaves_the_response_unchanged(
    logging_backend, tmp_path: Path
) -> None:
    """A failed audit write costs a WARNING, not the command.

    The same request is issued twice -- once against a writable Audit_File and
    once against an unwritable one -- and the two responses are compared, which
    is what "the same Service_Response as when the write succeeds" means.

    Validates: Requirement 11.15
    """
    writable = tmp_path / "ok" / "audit.jsonl"
    good_service = make_service(str(writable))
    good = capture(
        lambda: call(
            good_service,
            "RunCommandRequeue",
            takler_pb2.RequeueCommand(node_path=[TASK3]),
        ),
        audit_file=str(writable),
    )

    read_only_dir = tmp_path / "read-only"
    read_only_dir.mkdir()
    read_only_dir.chmod(0o500)
    blocked = read_only_dir / "audit.jsonl"
    log_file = str(tmp_path / "takler.log")
    bad_service = make_service(str(blocked))

    try:
        bad = capture(
            lambda: call(
                bad_service,
                "RunCommandRequeue",
                takler_pb2.RequeueCommand(node_path=[TASK3]),
            ),
            audit_file=str(blocked),
            log_file=log_file,
        )
    finally:
        read_only_dir.chmod(0o700)

    assert not blocked.exists()
    assert bad.result.flag == good.result.flag
    assert bad.result.message == good.result.message

    warnings = [
        line
        for line in bad.log_lines
        if "WARNING" in line and "failed to write audit record" in line
    ]
    assert len(warnings) == 1
    assert str(blocked) in warnings[0]


@pytest.mark.skipif(RUNNING_AS_ROOT, reason="root ignores directory permissions")
def test_repeated_failures_keep_serving_commands(
    logging_backend, tmp_path: Path
) -> None:
    """A permanently unwritable Audit_File never fails a command.

    Three commands against a blocked Audit_File all succeed. How many warnings
    that produces is deliberately not asserted -- the two backends differ on
    whether a failing sink raises to the writer -- only that the commands keep
    working and that the failure is reported at least once.

    Validates: Requirement 11.15
    """
    read_only_dir = tmp_path / "read-only"
    read_only_dir.mkdir()
    read_only_dir.chmod(0o500)
    blocked = read_only_dir / "audit.jsonl"
    log_file = str(tmp_path / "takler.log")
    service = make_service(str(blocked))

    def action() -> List[int]:
        return [
            call(
                service,
                "RunCommandRequeue",
                takler_pb2.RequeueCommand(node_path=[TASK3]),
            ).flag
            for _ in range(3)
        ]

    try:
        captured = capture(action, audit_file=str(blocked), log_file=log_file)
    finally:
        read_only_dir.chmod(0o700)

    assert captured.result == [0, 0, 0]
    warnings = [
        line for line in captured.log_lines if "failed to write audit record" in line
    ]
    assert warnings
