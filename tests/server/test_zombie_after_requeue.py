"""The requeue-then-report zombie, which is the M2 acceptance case.

A task is requeued while its job is still running; the job then finishes and
reports ``complete``. Without zombie detection that report lands on the fresh
instance of the task and the flow silently continues from a state no job ever
produced. This file pins that scenario down end to end, in both Auth_Modes and
all the way out to the exit code a job script sees.

Three things are asserted, and each of them is a separate claim:

* the server really has a Zombie_Detector. ``TaklerServer`` builds one from the
  resolved Auth_Mode and Zombie_Policy and hands it to the ``Scheduler``;
  without that wiring ``Scheduler.zombie_detector`` stays ``None`` -- the "no
  policy configured" case meant for a directly driven scheduler -- and the whole
  feature is dead code in production;
* the report of the old job changes nothing on the target task, under
  ``Auth_Mode=disabled`` as well as under ``enabled``. The ``disabled`` case is
  the interesting one: it is carried by ``Z2`` (the task is queued, so no job of
  it should be reporting) and therefore proves zombie detection does not depend
  on authentication being turned on (Requirement 9.11);
* the Client_CLI turns the rejection into exit code 3 and one stderr line naming
  the Error_Code classification ``zombie`` (Requirement 10.10), which is the
  only signal the ``set -e`` job script wrapper can act on.

The Client_CLI half runs against a real ``TaklerServer`` bound to a real port in
this test process, so the whole path -- CLI -> client -> gRPC -> interceptor ->
handler -> Scheduler -> Zombie_Detector -- is exercised. The server runs in a
background event-loop thread because the client is blocking; the same split as
in ``tests/integration/conftest.py``.

No password is printed, put into a test name or into an assertion message: the
Job_Password of the old job is only ever read inside an assertion expression or
handed to the code under test.

Validates: Requirements 9.11, 10.2, 10.10, 16.4
"""

from __future__ import annotations

import asyncio
import socket
import threading
from pathlib import Path
from typing import Iterator, Optional, Tuple

import pytest
from typer.testing import CliRunner

from takler.client import cli
from takler.core import Flow, NodeStatus
from takler.core.task_node import Task
from takler.exceptions import ZombieError
from takler.server import TaklerServer
from takler.server.auth import (
    CallCredentials,
    reset_call_credentials,
    set_call_credentials,
)
from takler.server.connect_config import (
    TAKLER_AUTH_MODE,
    TAKLER_ZOMBIE_POLICY,
    AuthMode,
    ZombiePolicy,
    generate_connect_config,
)
from takler.server.zombie import ZombieCondition


#: Loopback keeps the CLI half hermetic: no name resolution, no traffic leaving
#: the machine.
LOCALHOST = "127.0.0.1"

FLOW_NAME = "flow1"
TASK_PATH = "/flow1/task1"

#: Main-loop interval of the in-process server. The scheduler notices
#: ``should_stop`` only between two iterations, so a long interval would make the
#: fixture teardown wait that long.
TEST_MAIN_LOOP_INTERVAL = 0.05

runner = CliRunner()


# ---------------------------------------------------------------------------
# The scenario, built through the operations that produce it
# ---------------------------------------------------------------------------


def build_flow() -> Flow:
    """One begun, suspended flow holding a single task.

    Suspended on purpose: the scheduler main loop really runs in the server
    tests below and would submit the dependency-free queued task again right
    after the requeue, which is precisely the state the assertions are about.
    ``check_dependencies`` stops at a suspended node, so suspending the flow
    root freezes the tree and leaves the requeue observable.
    """
    flow = Flow(FLOW_NAME)
    flow.add_task("task1")
    flow.begin()
    flow.suspend()
    return flow


def start_a_job(task: Task) -> str:
    """Drive ``task`` into the state a running job reports from.

    Both steps are the real ones: ``run`` submits the task and, through
    ``increment_try_no``, generates the Job_Password of this try; ``init`` is the
    first thing the job itself does, and makes the task active.

    Returns:
        The Job_Password of this try -- what the job exports as ``TAKLER_PASS``
        and presents on every later Child_Command.
    """
    task.run()
    task.init(task_id="job-1")
    assert task.state.node_status is NodeStatus.active
    password = task.job_password
    assert password  # the run generated one, so the scenario is meaningful
    return password


def requeue_under_the_running_job(server: TaklerServer) -> None:
    """Requeue the task while its job is still running.

    Through the Scheduler, i.e. the same code path a ``takler-client-py requeue``
    reaches, but without the wire: with ``Auth_Mode=enabled`` a requeue over gRPC
    would need operator credentials, which is a different requirement's subject.
    """
    server.scheduler.run_command_requeue(TASK_PATH)


def snapshot(task: Task) -> Tuple:
    """The five attributes a ``fail`` disposition must leave untouched."""
    return (
        task.state.node_status,
        task.task_id,
        task.try_no,
        task.aborted_reason,
        task.job_password,
    )


# ---------------------------------------------------------------------------
# Server construction
# ---------------------------------------------------------------------------


def make_server(
    monkeypatch,
    auth_mode: AuthMode,
    tmp_path: Path,
    zombie_policy: ZombiePolicy = ZombiePolicy.FAIL,
    port: Optional[int] = None,
) -> TaklerServer:
    """Build a server with the given security settings, without starting it.

    The settings arrive through the environment variables rather than through
    constructor arguments because that is the only way in: ``Auth_Mode`` and
    ``Zombie_Policy`` are resolved by ``TaklerServer.__init__`` itself.

    With ``Auth_Mode=enabled`` an Operator_Secret_File has to exist, otherwise
    ``start()`` refuses to bring the server up (Requirement 7.4). Its content is
    irrelevant here -- no Operator_Command crosses the wire in this file -- but
    the file has to be there and owner-readable only, so that its permissions do
    not add a warning of their own.
    """
    monkeypatch.setenv(TAKLER_AUTH_MODE, auth_mode.value)
    monkeypatch.setenv(TAKLER_ZOMBIE_POLICY, zombie_policy.value)

    connect_config = generate_connect_config()
    secret_file = tmp_path / "operator.secret"
    secret_file.write_text("s3cret\n")
    secret_file.chmod(0o600)
    connect_config.security.operator_secret_file = str(secret_file)

    return TaklerServer(
        host=LOCALHOST,
        port=free_port() if port is None else port,
        connect_config=connect_config,
        checkpoint_file=tmp_path / "takler.check",
    )


def free_port() -> int:
    """Return a currently free TCP port.

    Binding port 0 and reading the assigned port back is the portable way to get
    a port that is free right now; a hard-coded one would collide with a parallel
    test run or with a developer's own server.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


class ServedServer:
    """Runs a :class:`TaklerServer` in a background event-loop thread.

    ``TaklerServer`` is an ``asyncio`` server and ``TaklerServiceClient`` -- and
    with it the Client_CLI -- is fully blocking, so calling the client from the
    loop that has to answer it would deadlock on the first command. Owning the
    loop in a thread keeps the test body plain synchronous code.
    """

    def __init__(self, server: TaklerServer) -> None:
        self.server = server
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._error: Optional[BaseException] = None

    def start(self, timeout: float = 15.0) -> "ServedServer":
        self._thread = threading.Thread(
            target=self._thread_main, name="takler-zombie-test-server", daemon=True
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
        # The port is bound once ``start()`` returned, so the client may dial.
        self._ready.set()
        await run_task


@pytest.fixture
def served(monkeypatch, tmp_path, request) -> Iterator[ServedServer]:
    """A started server whose Auth_Mode comes from the test's parameter."""
    auth_mode = getattr(request, "param", AuthMode.DISABLED)
    running = ServedServer(make_server(monkeypatch, auth_mode, tmp_path))
    running.start()
    try:
        yield running
    finally:
        running.stop()


# ---------------------------------------------------------------------------
# The detector is actually wired into the server
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("auth_mode", [AuthMode.DISABLED, AuthMode.ENABLED])
@pytest.mark.parametrize("zombie_policy", [ZombiePolicy.FAIL, ZombiePolicy.FOB])
def test_server_gives_its_scheduler_a_detector(
    monkeypatch, tmp_path, auth_mode, zombie_policy
):
    """A real server judges Child_Commands; ``None`` would disable the feature."""
    server = make_server(monkeypatch, auth_mode, tmp_path, zombie_policy=zombie_policy)

    detector = server.scheduler.zombie_detector
    assert detector is not None
    assert detector is server.zombie_detector
    # Built from the settings the server resolved, not from the defaults.
    assert detector.auth_mode is auth_mode
    assert detector.zombie_policy is zombie_policy


# ---------------------------------------------------------------------------
# The old job's report changes nothing (Requirements 9.11, 10.2, 16.4)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "auth_mode, expected_condition",
    [
        # Without authentication ``Z1`` is not evaluated at all, so the case is
        # carried by ``Z2``: the task is queued, so no job of it should report.
        (AuthMode.DISABLED, ZombieCondition.Z2),
        # With authentication ``Z1`` comes first and fires as well, because the
        # requeue cleared the Job_Password the old job is still presenting.
        (AuthMode.ENABLED, ZombieCondition.Z1),
    ],
)
def test_old_job_complete_after_requeue_is_rejected(
    monkeypatch, tmp_path, auth_mode, expected_condition
):
    server = make_server(monkeypatch, auth_mode, tmp_path)
    server.bunch.add_flow(build_flow())
    task: Task = server.bunch.find_node(TASK_PATH)
    old_password = start_a_job(task)

    requeue_under_the_running_job(server)
    assert task.state.node_status is NodeStatus.queued
    # The requeue is what makes the old job a zombie in either mode: the task is
    # no longer in flight, and it no longer holds a password.
    assert task.job_password is None
    before = snapshot(task)

    # What the old job presents: the Job_Password of the try that was requeued.
    token = set_call_credentials(CallCredentials(job_password=old_password))
    try:
        assert server.zombie_detector.detect(task, "complete") is expected_condition
        with pytest.raises(ZombieError) as excinfo:
            server.scheduler.run_command_complete(TASK_PATH)
    finally:
        reset_call_credentials(token)

    # Requirement 10.2: none of the five attributes moved, so the fresh instance
    # of the task is exactly where the requeue left it.
    assert snapshot(task) == before
    assert task.state.node_status is NodeStatus.queued

    message = str(excinfo.value)
    assert expected_condition.value in message
    assert TASK_PATH in message
    assert old_password not in message


def test_a_report_of_the_current_job_is_not_a_zombie(monkeypatch, tmp_path):
    """The control case: the same command from the *new* job goes through.

    Without this the tests above would also pass on a server that rejects every
    Child_Command.
    """
    server = make_server(monkeypatch, AuthMode.ENABLED, tmp_path)
    server.bunch.add_flow(build_flow())
    task: Task = server.bunch.find_node(TASK_PATH)
    start_a_job(task)
    requeue_under_the_running_job(server)

    # The task is submitted again and its new job initializes: this is the run
    # instance the server records from now on.
    new_password = start_a_job(task)

    token = set_call_credentials(CallCredentials(job_password=new_password))
    try:
        server.scheduler.run_command_complete(TASK_PATH)
    finally:
        reset_call_credentials(token)

    assert task.state.node_status is NodeStatus.complete
    assert task.job_password == new_password


# ---------------------------------------------------------------------------
# The Client_CLI contract (Requirement 10.10)
# ---------------------------------------------------------------------------


def stderr_lines(result) -> list:
    return [line for line in result.stderr.splitlines() if line.strip()]


@pytest.mark.parametrize("served", [AuthMode.DISABLED, AuthMode.ENABLED], indirect=True)
def test_client_cli_exits_three_and_names_the_zombie_classification(served):
    """The whole path, from the job script's command to its exit code.

    Requirements 10.2, 10.10: the ``fail`` policy answers the old job with
    ``flag=31``, the CLI turns that into exit code 3 plus one stderr line naming
    the Error_Code classification ``zombie``, and the task keeps the status the
    requeue gave it.
    """
    server = served.server
    server.bunch.add_flow(build_flow())
    task: Task = server.bunch.find_node(TASK_PATH)
    old_password = start_a_job(task)

    requeue_under_the_running_job(server)
    before = snapshot(task)

    result = runner.invoke(
        cli.app,
        [
            "complete",
            "--node-path",
            TASK_PATH,
            "--host",
            LOCALHOST,
            "--port",
            str(server.network_service.port),
        ],
        # What ``head.takler`` exports for the job: the password of the try that
        # has since been requeued. Under ``Auth_Mode=enabled`` the
        # Auth_Interceptor also requires the key to be present at all.
        env={"TAKLER_PASS": old_password},
    )

    assert result.exit_code == 3
    lines = stderr_lines(result)
    assert len(lines) == 1
    assert "zombie" in lines[0]
    assert old_password not in result.stderr
    assert "Traceback" not in result.stderr

    assert snapshot(task) == before
    assert task.state.node_status is NodeStatus.queued
