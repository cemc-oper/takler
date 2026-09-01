"""The Auth_Interceptor in front of a real ``grpc.aio`` server.

This is the M2 acceptance test of "a client holding no or wrong credentials
cannot run any Control_Command". It is deliberately not a unit test of
:class:`~takler.server.auth.AuthInterceptor`: a check that only holds when the
interceptor is called by hand is worth little, because the thing that can break
in production is the *wiring* -- an interceptor that is not registered, an
abort handler that does not abort, a status code that degrades to ``UNKNOWN``
somewhere between the interceptor and the wire. So a real ``TaklerServer`` is
started on a real port here and every RPC is sent through a real gRPC channel.

What is pinned, one claim per group of tests:

* ``Auth_Mode=disabled`` lets every RPC through with no credentials at all,
  which is what keeps an M1 deployment working unchanged (Requirement 6.3);
* ``ping`` passes without credentials even when authentication is on, so a
  health check keeps working (Requirement 6.8);
* a Child_Command with no ``takler-pass`` is refused with ``UNAUTHENTICATED``,
  and one *with* a ``takler-pass`` is let through whatever the value is -- the
  comparison is the Zombie_Detector's business (Requirements 6.4, 6.13);
* every Operator_Command -- all eight Control_Commands plus ``show`` and
  ``coroutine``, not one representative -- is refused: ``UNAUTHENTICATED`` when
  a credential key is missing, ``PERMISSION_DENIED`` when the secret is wrong or
  the user name is not whitelisted (Requirements 6.5, 6.6, 6.7);
* a refused RPC never reaches its handler and leaves every node of the Bunch
  exactly as it was (Requirement 6.9). The handler is watched with a counting
  spy wrapped around the servicer methods *before* the service registers them,
  so "the handler did not run" is observed rather than inferred from the state;
* neither the abort details nor the log record echoes a credential value
  (Requirements 6.10, 6.12);
* the Client_CLI turns a refusal into exit code 1 plus one stderr line naming
  ``PermissionDeniedError`` and the server's description (Requirement 6.14).

Two clients are used on purpose. The raw ``TaklerServerStub`` is what the status
code assertions need: the Call_Wrapper maps both ``UNAUTHENTICATED`` and
``PERMISSION_DENIED`` onto the same ``PermissionDeniedError``, so the
distinction Requirements 6.4 ~ 6.7 draw is only observable below it. The
Client_CLI is what Requirement 6.14 is about, and it runs against the same live
server.

``tests/server/test_auth_rejection_record.py`` covers the *text* of a refusal
against a stand-in context; only the end-to-end half of that -- what actually
crosses the wire and what reaches a log while a real server serves -- is
repeated here.

No credential value is printed, put into a test name or into an assertion
message: every secret is generated in a fixture and only ever read inside an
assertion expression or handed to the code under test.

Validates: Requirements 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9, 6.10, 6.12, 6.14,
16.2, 16.3
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import getpass
import io
import json
import socket
import threading
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import grpc
import pytest
from typer.testing import CliRunner

import takler.logging
from takler.client import cli
from takler.core import Flow, NodeStatus
from takler.core.task_node import Task
from takler.server import TaklerServer
from takler.server.auth import (
    METADATA_KEY_JOB_PASSWORD,
    METADATA_KEY_SECRET,
    METADATA_KEY_USER,
    PRIVILEGE_BY_METHOD,
    SERVICE_METHOD_PREFIX,
    PrivilegeLevel,
    RejectionReason,
)
from takler.server.connect_config import (
    TAKLER_AUTH_MODE,
    TAKLER_ZOMBIE_POLICY,
    AuthMode,
    ZombiePolicy,
    generate_connect_config,
)
from takler.server.protocol import takler_pb2
from takler.server.protocol.takler_pb2_grpc import TaklerServerStub

#: Loopback only: no name resolution, no traffic leaving the machine.
LOCALHOST = "127.0.0.1"

FLOW_NAME = "flow1"
TASK_PATH = "/flow1/task1"
NESTED_TASK_PATH = "/flow1/container1/task2"

#: The Operator_Secret the server accepts, and two values it must not.
SECRET = "operator-secret-value"
WRONG_SECRET = "retired-secret-value"

#: A whitelisted user name, and one that is not on the list.
USER = "alice"
INTRUDER = "intruder"

#: What a job presents on a Child_Command. Its value is irrelevant to the
#: interceptor -- only its presence is checked (Requirement 6.13) -- which is
#: exactly what one of the tests below asserts.
JOB_PASSWORD = "job-password-value"

#: Per-attempt deadline of the raw stub calls. Every call here is answered by an
#: in-process server, so a call that takes seconds is a hang, not slowness.
RPC_TIMEOUT = 15.0

#: Main-loop interval of the in-process server: the scheduler notices
#: ``should_stop`` only between two iterations, so a long interval would make the
#: fixture teardown wait that long.
TEST_MAIN_LOOP_INTERVAL = 0.05

runner = CliRunner()


# ---------------------------------------------------------------------------
# The RPC table: every method of the service, with a well formed request
# ---------------------------------------------------------------------------


def _child_options(node_path: str = TASK_PATH) -> takler_pb2.ChildCommandOptions:
    return takler_pb2.ChildCommandOptions(node_path=node_path)


def build_requests(flow_bytes: bytes) -> Dict[str, Any]:
    """A well formed request for every RPC of the ``TaklerServer`` service.

    Well formed on purpose: a request the handler would reject anyway could not
    tell a refusal apart from a bad request, and the authorized control case
    below has to be able to reach the handler and succeed.

    Args:
        flow_bytes: A serialized flow definition, for ``RunCommandLoad``.

    Returns:
        A mapping from method name to request message. Its key set is asserted
        against the privilege table, so a new rpc cannot be silently left out of
        this file.
    """
    return {
        # Child_Commands.
        "RunCommandInit": takler_pb2.InitCommand(
            child_options=_child_options(), task_id="job-1"
        ),
        "RunCommandComplete": takler_pb2.CompleteCommand(
            child_options=_child_options()
        ),
        "RunCommandAbort": takler_pb2.AbortCommand(
            child_options=_child_options(), reason="job failed"
        ),
        "RunCommandEvent": takler_pb2.EventCommand(
            child_options=_child_options(), event_name="event1"
        ),
        "RunCommandMeter": takler_pb2.MeterCommand(
            child_options=_child_options(), meter_name="meter1", meter_value="1"
        ),
        # Control_Commands.
        "RunCommandRequeue": takler_pb2.RequeueCommand(node_path=[TASK_PATH]),
        "RunCommandSuspend": takler_pb2.SuspendCommand(node_path=[NESTED_TASK_PATH]),
        "RunCommandResume": takler_pb2.SuspendCommand(node_path=[NESTED_TASK_PATH]),
        "RunCommandRun": takler_pb2.RunCommand(force=True, node_path=[TASK_PATH]),
        "RunCommandForce": takler_pb2.ForceCommand(
            state=takler_pb2.ForceCommand.ForceState.Value("complete"),
            recursive=False,
            path=[NESTED_TASK_PATH],
        ),
        "RunCommandFreeDep": takler_pb2.FreeDepCommand(
            dep_type=takler_pb2.FreeDepCommand.DepType.Value("all"),
            path=[NESTED_TASK_PATH],
        ),
        "RunCommandLoad": takler_pb2.LoadCommand(flow_type="json", flow=flow_bytes),
        "RunCommandBegin": takler_pb2.BeginCommand(flow_name=FLOW_NAME, force=True),
        # Query_Commands at Operator level: both return the whole flow
        # definition.
        "RunRequestShow": takler_pb2.ShowRequest(
            show_trigger=True,
            show_parameter=True,
            show_limit=True,
            show_event=True,
            show_meter=True,
        ),
        "QueryCoroutine": takler_pb2.CoroutineRequest(),
        # The one PUBLIC rpc.
        "RunRequestPing": takler_pb2.PingRequest(),
    }


def method_names_at(level: PrivilegeLevel) -> List[str]:
    """The short method names registered at ``level``, in table order."""
    return [
        method[len(SERVICE_METHOD_PREFIX) :]
        for method, registered in PRIVILEGE_BY_METHOD.items()
        if registered is level
    ]


CHILD_METHODS: List[str] = method_names_at(PrivilegeLevel.CHILD)
OPERATOR_METHODS: List[str] = method_names_at(PrivilegeLevel.OPERATOR)
PUBLIC_METHODS: List[str] = method_names_at(PrivilegeLevel.PUBLIC)
ALL_METHODS: List[str] = CHILD_METHODS + OPERATOR_METHODS + PUBLIC_METHODS


def test_the_rpc_table_of_this_file_covers_the_whole_service() -> None:
    """Every classified rpc is exercised below; a new one fails here first.

    Without this the loops over ``OPERATOR_METHODS`` would keep passing while
    quietly not covering a newly added Control_Command, which is precisely the
    hole Requirement 16.2 asks the suite to close.
    """
    assert set(build_requests(b"{}")) == set(ALL_METHODS)
    assert len(CHILD_METHODS) == 5
    assert len(OPERATOR_METHODS) == 10
    assert PUBLIC_METHODS == ["RunRequestPing"]


# ---------------------------------------------------------------------------
# The flow the assertions are about
# ---------------------------------------------------------------------------


def build_flow() -> Flow:
    """One begun, suspended flow with a nested container and two tasks.

    Suspended so the scheduler main loop -- which really runs in these tests --
    cannot submit the queued tasks and move the state the assertions compare
    against. ``check_dependencies`` stops at a suspended node, so suspending the
    root freezes the whole tree.
    """
    flow = Flow(FLOW_NAME)
    flow.add_task("task1")
    container = flow.add_container("container1")
    container.add_task("task2")
    flow.begin()
    flow.suspend()
    return flow


def start_a_job(task: Task) -> str:
    """Drive ``task`` into the state a running job reports from.

    A task holding a Job_Password and sitting in ``active`` is what makes the
    "nothing changed" assertions meaningful: the snapshot then contains a
    non-trivial state, a task id, a try number and a password, and a refused RPC
    has something it could plausibly damage.

    Returns:
        The Job_Password of this try.
    """
    task.run()
    task.init(task_id="job-1")
    assert task.state.node_status is NodeStatus.active
    password = task.job_password
    assert password
    return password


def flow_definition_bytes() -> bytes:
    """A serialized flow definition for ``load``, under a second name."""
    flow = Flow("flow2")
    flow.add_task("task1")
    return json.dumps(flow.to_dict()).encode("utf-8")


NodeSnapshot = Tuple[Any, Any, Any, Any, Any]


def bunch_snapshot(server: TaklerServer) -> Dict[str, NodeSnapshot]:
    """The five mutable attributes of every node of the Bunch.

    The same five the zombie disposition tests compare, for the same reason:
    they are what a Control_Command can move, so "all node states unchanged"
    (Requirement 6.9) is checkable as one equality.
    """
    snapshot: Dict[str, NodeSnapshot] = {}

    def walk(node: Any, prefix: str) -> None:
        path = f"{prefix}/{node.name}"
        snapshot[path] = (
            node.state.node_status,
            getattr(node, "task_id", None),
            getattr(node, "try_no", None),
            getattr(node, "aborted_reason", None),
            getattr(node, "job_password", None),
        )
        for child in node.children:
            walk(child, path)

    for flow in server.bunch.flows.values():
        walk(flow, "")
    return snapshot


# ---------------------------------------------------------------------------
# The handler spy
# ---------------------------------------------------------------------------


class HandlerSpy:
    """Counts how often each servicer method is entered.

    Requirement 6.9 says a refused RPC's handler is not called, which is a
    stronger claim than "the state did not change": a handler that runs and
    happens to be a no-op for this request would satisfy the second and not the
    first. Counting entries is the only way to observe the difference.

    The wrapping has to happen before :meth:`TaklerService.start`, because
    ``add_TaklerServerServicer_to_server`` binds the servicer methods once at
    registration time; a later ``setattr`` would not be seen by the registered
    handlers.
    """

    def __init__(self, service: Any) -> None:
        self.calls: "collections.Counter[str]" = collections.Counter()
        for name in ALL_METHODS:
            self._wrap(service, name)

    def _wrap(self, service: Any, name: str) -> None:
        original = getattr(service, name)

        async def spy(request: Any, context: Any) -> Any:
            self.calls[name] += 1
            return await original(request, context)

        setattr(service, name, spy)

    @property
    def total(self) -> int:
        return sum(self.calls.values())


# ---------------------------------------------------------------------------
# Server construction and lifetime
# ---------------------------------------------------------------------------


def free_port() -> int:
    """Return a port that is free right now.

    A hard-coded one would collide with a parallel test run or with a
    developer's own server.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def make_server(monkeypatch: Any, auth_mode: AuthMode, tmp_path: Path) -> TaklerServer:
    """Build a server with the given Auth_Mode, without starting it.

    The Auth_Mode arrives through the environment because that is the only way
    in: ``TaklerServer.__init__`` resolves it itself. The credential files are
    written owner-readable only, so their permissions do not add a warning of
    their own to the log assertions.

    The whitelist holds the invoking OS user as well as :data:`USER`, so that a
    Client_CLI refusal below is classified by the *missing secret* rather than
    by the user name -- which is what Requirement 16.2 is about.
    """
    monkeypatch.setenv(TAKLER_AUTH_MODE, auth_mode.value)
    # Explicit rather than inherited: the child-command tests below present a
    # password that matches nothing, and ``fail`` keeps that visible as an error
    # response instead of a silently skipped command.
    monkeypatch.setenv(TAKLER_ZOMBIE_POLICY, ZombiePolicy.FAIL.value)

    secret_file = tmp_path / "operator.secret"
    secret_file.write_text(f"# rotation round 1\n{SECRET}\n")
    secret_file.chmod(0o600)

    whitelist_file = tmp_path / "operator.whitelist"
    whitelist_file.write_text(f"{USER}\n{getpass.getuser()}\n")
    whitelist_file.chmod(0o600)

    connect_config = generate_connect_config()
    connect_config.security.operator_secret_file = str(secret_file)
    connect_config.security.operator_whitelist_file = str(whitelist_file)

    return TaklerServer(
        host=LOCALHOST,
        port=free_port(),
        connect_config=connect_config,
        checkpoint_file=tmp_path / "takler.check",
    )


class ServedServer:
    """A :class:`TaklerServer` running in a background event-loop thread.

    ``TaklerServer`` is asyncio and both the raw stub and the Client_CLI are
    blocking, so answering them from the loop that issues them would deadlock on
    the first call. Owning the loop in a thread keeps every test body plain
    synchronous code.
    """

    def __init__(self, server: TaklerServer) -> None:
        self.server = server
        self.spy = HandlerSpy(server.network_service)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._error: Optional[BaseException] = None

    @property
    def port(self) -> Any:
        return self.server.network_service.port

    @property
    def address(self) -> str:
        return f"{LOCALHOST}:{self.port}"

    def start(self, timeout: float = 15.0) -> "ServedServer":
        self._thread = threading.Thread(
            target=self._thread_main, name="takler-auth-test-server", daemon=True
        )
        self._thread.start()
        if not self._ready.wait(timeout):
            raise TimeoutError(f"takler server did not start within {timeout} seconds")
        if self._error is not None:
            raise self._error
        return self

    def stop(self, timeout: float = 20.0) -> None:
        if self._thread is None:
            return
        if self._loop is not None and self._thread.is_alive():
            future = asyncio.run_coroutine_threadsafe(self.server.stop(), self._loop)
            try:
                future.result(timeout=timeout)
            except Exception:  # noqa: BLE001 - teardown must not mask a failure
                pass
        self._thread.join(timeout=timeout)
        self._thread = None

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._serve())
        except BaseException as exc:  # noqa: BLE001 - reported from start()
            self._error = exc
        finally:
            # Unblock ``start()`` even when startup failed, so the test fails
            # with the real error instead of a timeout.
            self._ready.set()

    async def _serve(self) -> None:
        self.server.scheduler.interval_main_loop = TEST_MAIN_LOOP_INTERVAL
        self._loop = asyncio.get_running_loop()
        await self.server.start()
        run_task = self._loop.create_task(self.server.run(), name="takler.test.server")
        self._ready.set()
        await run_task

    # -- calling it ----------------------------------------------------

    @contextlib.contextmanager
    def stub(self) -> Iterator[TaklerServerStub]:
        """A raw stub on a plaintext channel to this server."""
        channel = grpc.insecure_channel(self.address)
        try:
            yield TaklerServerStub(channel)
        finally:
            channel.close()


def _serve(monkeypatch: Any, tmp_path: Path, auth_mode: AuthMode) -> ServedServer:
    """Start a server holding the flow the assertions are about."""
    server = make_server(monkeypatch, auth_mode, tmp_path)
    server.bunch.add_flow(build_flow())
    start_a_job(server.bunch.find_node(TASK_PATH))
    return ServedServer(server).start()


@pytest.fixture
def enabled(monkeypatch: Any, tmp_path: Path) -> Iterator[ServedServer]:
    """A started server with ``Auth_Mode=enabled``."""
    running = _serve(monkeypatch, tmp_path, AuthMode.ENABLED)
    try:
        yield running
    finally:
        running.stop()


@pytest.fixture
def disabled(monkeypatch: Any, tmp_path: Path) -> Iterator[ServedServer]:
    """A started server with ``Auth_Mode=disabled``, the M1 posture."""
    running = _serve(monkeypatch, tmp_path, AuthMode.DISABLED)
    try:
        yield running
    finally:
        running.stop()


# ---------------------------------------------------------------------------
# Calling helpers
# ---------------------------------------------------------------------------

Metadata = Sequence[Tuple[str, str]]

NO_CREDENTIALS: Metadata = ()
JOB_PASSWORD_ONLY: Metadata = ((METADATA_KEY_JOB_PASSWORD, JOB_PASSWORD),)
SECRET_ONLY: Metadata = ((METADATA_KEY_SECRET, SECRET),)
USER_ONLY: Metadata = ((METADATA_KEY_USER, USER),)
VALID_OPERATOR: Metadata = (
    (METADATA_KEY_SECRET, SECRET),
    (METADATA_KEY_USER, USER),
)
WRONG_SECRET_METADATA: Metadata = (
    (METADATA_KEY_SECRET, WRONG_SECRET),
    (METADATA_KEY_USER, USER),
)
NOT_WHITELISTED_METADATA: Metadata = (
    (METADATA_KEY_SECRET, SECRET),
    (METADATA_KEY_USER, INTRUDER),
)


def call(
    stub: TaklerServerStub,
    method: str,
    metadata: Metadata,
    flow_bytes: bytes = b"{}",
) -> Any:
    """Send one RPC, returning either the response or the ``grpc.RpcError``."""
    request = build_requests(flow_bytes)[method]
    try:
        return getattr(stub, method)(
            request, timeout=RPC_TIMEOUT, metadata=list(metadata)
        )
    except grpc.RpcError as exc:
        return exc


def refuse_every(
    served: ServedServer,
    methods: Sequence[str],
    metadata: Metadata,
) -> Dict[str, grpc.RpcError]:
    """Send ``metadata`` to every method of ``methods``, expecting a refusal.

    Returns:
        The error of each call, keyed by method name.
    """
    errors: Dict[str, grpc.RpcError] = {}
    with served.stub() as stub:
        for method in methods:
            outcome = call(stub, method, metadata)
            assert isinstance(outcome, grpc.RpcError), (
                f"{method} was answered instead of refused: {outcome!r}"
            )
            errors[method] = outcome
    return errors


@contextlib.contextmanager
def captured_log() -> Iterator[io.StringIO]:
    """Capture takler's console log output for the duration of the block.

    The logging backend does not route records into pytest's handler, so the
    console sink is pointed at a buffer instead -- the same approach as
    ``test_credential_store_fail_closed.py``. The records asserted on are
    written by the server thread, which shares the process-wide ``sys.stderr``
    this replaces.
    """
    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="DEBUG", console=True)
            yield buffer
    finally:
        takler.logging._reset_configured_state()
        takler.logging.configure(console=True)


# ---------------------------------------------------------------------------
# Auth_Mode=disabled: everything passes (Requirement 6.3)
# ---------------------------------------------------------------------------


def test_disabled_lets_every_rpc_through_without_credentials(
    disabled: ServedServer,
) -> None:
    """An M1 client keeps working against a server that has not enabled auth.

    Every rpc of the service is sent with no metadata at all and none of them is
    refused; the handler of each one runs.

    Validates: Requirement 6.3
    """
    with disabled.stub() as stub:
        for method in ALL_METHODS:
            outcome = call(stub, method, NO_CREDENTIALS, flow_definition_bytes())
            assert not isinstance(outcome, grpc.RpcError), (
                f"{method} was refused with authentication disabled: {outcome!r}"
            )

    assert disabled.spy.calls == collections.Counter(dict.fromkeys(ALL_METHODS, 1))


# ---------------------------------------------------------------------------
# ping (Requirement 6.8)
# ---------------------------------------------------------------------------


def test_ping_needs_no_credentials_when_authentication_is_enabled(
    enabled: ServedServer,
) -> None:
    """A health check keeps working on an authenticated server.

    Validates: Requirement 6.8
    """
    with enabled.stub() as stub:
        outcome = call(stub, "RunRequestPing", NO_CREDENTIALS)

    assert isinstance(outcome, takler_pb2.PingResponse)
    assert enabled.spy.calls["RunRequestPing"] == 1


def test_ping_passes_even_with_a_wrong_secret(enabled: ServedServer) -> None:
    """``PUBLIC`` means the metadata is not looked at, not "must be empty".

    Validates: Requirement 6.8
    """
    with enabled.stub() as stub:
        outcome = call(stub, "RunRequestPing", WRONG_SECRET_METADATA)

    assert isinstance(outcome, takler_pb2.PingResponse)


# ---------------------------------------------------------------------------
# Child_Commands (Requirements 6.4, 6.13)
# ---------------------------------------------------------------------------


def test_child_command_without_job_password_is_unauthenticated(
    enabled: ServedServer,
) -> None:
    """All five Child_Commands are refused when ``takler-pass`` is absent.

    Validates: Requirements 6.4, 6.9
    """
    before = bunch_snapshot(enabled.server)

    errors = refuse_every(enabled, CHILD_METHODS, NO_CREDENTIALS)

    for method, error in errors.items():
        assert error.code() is grpc.StatusCode.UNAUTHENTICATED, method
        assert RejectionReason.MISSING_CREDENTIAL.value in error.details()

    assert enabled.spy.total == 0
    assert bunch_snapshot(enabled.server) == before


def test_child_command_with_a_job_password_reaches_the_handler(
    enabled: ServedServer,
) -> None:
    """Presence is all the interceptor checks; the value is judged later.

    :data:`JOB_PASSWORD` matches no task's Job_Password, so the handler answers
    with a zombie error -- which is the point: the RPC got *past* the
    interceptor and was judged by the Zombie_Detector, not refused for lack of
    credentials.

    Validates: Requirement 6.13
    """
    with enabled.stub() as stub:
        for method in CHILD_METHODS:
            outcome = call(stub, method, JOB_PASSWORD_ONLY)
            assert not isinstance(outcome, grpc.RpcError), (
                f"{method} was refused although it carried a job password"
            )

    assert enabled.spy.calls == collections.Counter(dict.fromkeys(CHILD_METHODS, 1))


# ---------------------------------------------------------------------------
# Operator_Commands: missing credentials (Requirements 6.5, 16.2)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "metadata",
    [
        pytest.param(NO_CREDENTIALS, id="neither-key"),
        pytest.param(SECRET_ONLY, id="no-user"),
        pytest.param(USER_ONLY, id="no-secret"),
    ],
)
def test_every_operator_command_missing_a_key_is_unauthenticated(
    enabled: ServedServer,
    metadata: Metadata,
) -> None:
    """Each of the ten Operator_Commands is refused, none of them runs.

    This is the M2 acceptance criterion: not one Control_Command gets through
    without credentials, the Bunch is untouched and no handler was entered.

    Validates: Requirements 6.5, 6.9, 16.2
    """
    before = bunch_snapshot(enabled.server)

    errors = refuse_every(enabled, OPERATOR_METHODS, metadata)

    assert set(errors) == set(OPERATOR_METHODS)
    for method, error in errors.items():
        assert error.code() is grpc.StatusCode.UNAUTHENTICATED, method
        assert RejectionReason.MISSING_CREDENTIAL.value in error.details(), method

    assert enabled.spy.total == 0
    assert bunch_snapshot(enabled.server) == before


# ---------------------------------------------------------------------------
# Operator_Commands: wrong credentials (Requirements 6.6, 6.7, 16.3)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "metadata, expected_reason",
    [
        pytest.param(
            WRONG_SECRET_METADATA,
            RejectionReason.INVALID_CREDENTIAL,
            id="wrong-secret",
        ),
        pytest.param(
            NOT_WHITELISTED_METADATA,
            RejectionReason.NOT_IN_WHITELIST,
            id="not-whitelisted",
        ),
    ],
)
def test_every_operator_command_with_bad_credentials_is_permission_denied(
    enabled: ServedServer,
    metadata: Metadata,
    expected_reason: RejectionReason,
) -> None:
    """A stale secret and an unauthorized user name are told apart from absence.

    ``PERMISSION_DENIED`` rather than ``UNAUTHENTICATED`` is what lets a client
    distinguish "I am not configured for an authenticated server" from "my
    secret is stale", without parsing message text.

    Validates: Requirements 6.6, 6.7, 6.9, 16.3
    """
    before = bunch_snapshot(enabled.server)

    errors = refuse_every(enabled, OPERATOR_METHODS, metadata)

    for method, error in errors.items():
        assert error.code() is grpc.StatusCode.PERMISSION_DENIED, method
        assert expected_reason.value in error.details(), method

    assert enabled.spy.total == 0
    assert bunch_snapshot(enabled.server) == before


def test_an_unregistered_method_is_refused_too(enabled: ServedServer) -> None:
    """A method nobody classified demands operator credentials, not none.

    The refusal arrives as ``UNAUTHENTICATED`` -- the interceptor runs before
    the method is resolved -- rather than as ``UNIMPLEMENTED``, which is what
    proves the fail-closed default is in force on the wire.

    Validates: Requirements 6.5, 6.9
    """
    channel = grpc.insecure_channel(enabled.address)
    try:
        rpc = channel.unary_unary(SERVICE_METHOD_PREFIX + "RunCommandDelete")
        with pytest.raises(grpc.RpcError) as excinfo:
            rpc(b"", timeout=RPC_TIMEOUT, metadata=[])
    finally:
        channel.close()

    assert excinfo.value.code() is grpc.StatusCode.UNAUTHENTICATED
    assert enabled.spy.total == 0


# ---------------------------------------------------------------------------
# The control case: valid credentials do reach the handlers
# ---------------------------------------------------------------------------


def test_valid_operator_credentials_reach_every_handler(
    enabled: ServedServer,
) -> None:
    """Without this the refusal tests would pass on a server that refuses all.

    The commands really run here, so the Bunch does change -- that is the whole
    difference being asserted. What each command does to the state is other
    tests' subject; this one only claims that authentication is not what stopped
    it.

    Validates: Requirements 6.5, 6.6, 6.7
    """
    with enabled.stub() as stub:
        for method in OPERATOR_METHODS:
            outcome = call(stub, method, VALID_OPERATOR, flow_definition_bytes())
            assert not isinstance(outcome, grpc.RpcError), (
                f"{method} was refused although its credentials were valid: {outcome!r}"
            )

    assert enabled.spy.calls == collections.Counter(dict.fromkeys(OPERATOR_METHODS, 1))


# ---------------------------------------------------------------------------
# What a refusal is allowed to say (Requirements 6.10, 6.12)
# ---------------------------------------------------------------------------


def test_neither_the_details_nor_the_log_carry_a_credential(
    enabled: ServedServer,
) -> None:
    """A refusal crossing the wire leaks no credential value, in either channel.

    The record itself is pinned by ``test_auth_rejection_record.py``; what is
    added here is that a *real* server, logging from its own thread through the
    configured sink, produces the same containment -- and that neither the
    accepted secret nor the presented one reaches the client.

    Validates: Requirements 6.10, 6.12
    """
    with captured_log() as buffer:
        errors = refuse_every(enabled, OPERATOR_METHODS, WRONG_SECRET_METADATA)
        log = buffer.getvalue()

    for method, error in errors.items():
        details = error.details()
        assert WRONG_SECRET not in details, method
        assert SECRET not in details, method
        assert method in details
        assert RejectionReason.INVALID_CREDENTIAL.value in details

    assert WRONG_SECRET not in log
    assert SECRET not in log
    # Requirement 6.10: one actionable record per refusal, naming the user, the
    # caller's address and the classification.
    assert log.count("refused ") == len(OPERATOR_METHODS)
    assert f"user={USER}" in log
    assert LOCALHOST in log


# ---------------------------------------------------------------------------
# The Client_CLI contract (Requirement 6.14)
# ---------------------------------------------------------------------------


def cli_invocations(port: Any, flow_file: Path) -> Dict[str, List[str]]:
    """The command line of every Operator_Command the CLI exposes.

    Keyed by CLI subcommand name rather than by rpc name, since that is the
    granularity a job script or an operator types.
    """
    address = ["--host", LOCALHOST, "--port", str(port)]
    return {
        "requeue": ["requeue", *address, TASK_PATH],
        "suspend": ["suspend", *address, NESTED_TASK_PATH],
        "resume": ["resume", *address, NESTED_TASK_PATH],
        "run": ["run", *address, TASK_PATH],
        "force": ["force", *address, "complete", NESTED_TASK_PATH],
        "free-dep": ["free-dep", *address, "--dep-type", "all", NESTED_TASK_PATH],
        "load": ["load", *address, str(flow_file)],
        "begin": ["begin", *address, FLOW_NAME],
        "show": ["show", *address],
        "coroutine": ["coroutine", *address],
    }


def stderr_lines(result: Any) -> List[str]:
    return [line for line in result.stderr.splitlines() if line.strip()]


def test_client_cli_exits_one_for_every_refused_control_command(
    enabled: ServedServer,
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    """The whole path, from the typed command to the exit code and stderr line.

    The CLI is run with no secret file configured, which is what an un-upgraded
    or unconfigured client looks like. Every Operator_Command it offers ends with
    exit code 1 and one stderr line naming ``PermissionDeniedError`` plus the
    server's description; no handler runs and the Bunch does not move.

    Validates: Requirements 6.14, 16.2
    """
    # An inherited secret file or connect file would give the CLI credentials and
    # turn this into a test of nothing.
    for name in ("TAKLER_SECRET_FILE", "TAKLER_CONNECT_FILE", "TAKLER_PASS"):
        monkeypatch.delenv(name, raising=False)

    flow_file = tmp_path / "flow2.json"
    flow_file.write_bytes(flow_definition_bytes())
    before = bunch_snapshot(enabled.server)

    for name, argv in cli_invocations(enabled.port, flow_file).items():
        result = runner.invoke(cli.app, argv)

        assert result.exit_code == 1, f"{name}: {result.stderr}"
        lines = stderr_lines(result)
        assert len(lines) == 1, f"{name}: {lines}"
        assert "PermissionDeniedError" in lines[0], name
        # The server's own description, which is what tells the operator that the
        # refusal came from authentication rather than from the request.
        assert RejectionReason.MISSING_CREDENTIAL.value in lines[0], name
        assert "Traceback" not in result.stderr, name

    assert enabled.spy.total == 0
    assert bunch_snapshot(enabled.server) == before


def test_client_cli_ping_still_succeeds_without_credentials(
    enabled: ServedServer,
    monkeypatch: Any,
) -> None:
    """The counterpart of the refusals: monitoring is unaffected by auth.

    "``ping`` works but everything else fails" is the documented symptom of a
    server that has enabled authentication in front of clients that are not
    configured for it, so both halves of it are worth pinning.

    Validates: Requirement 6.8
    """
    for name in ("TAKLER_SECRET_FILE", "TAKLER_CONNECT_FILE"):
        monkeypatch.delenv(name, raising=False)

    result = runner.invoke(
        cli.app, ["ping", "--host", LOCALHOST, "--port", str(enabled.port)]
    )

    assert result.exit_code == 0, result.stderr
