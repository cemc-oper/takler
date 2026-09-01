"""A server restart must not turn a running job into a zombie.

This is the M1 acceptance criterion "an in-flight job is not misjudged after a
server restart", now guarded against the machinery M2 adds. M2 gives every try of
a task a Job_Password and has the Zombie_Detector compare it against the
``takler-pass`` of every Child_Command (condition ``Z1``). A password that lives
only in the server process would therefore be lost on restart, and the very
first report of every still-running job would be rejected -- the feature meant to
catch stray jobs would instead kill the healthy ones. Requirement 5.4 avoids that
by persisting the passwords of the submitted / active tasks into the snapshot and
writing them back on restore; this file is the end-to-end proof that the whole
chain holds.

The scenario is a restart, spelled out: a snapshot is written while a task is
active, a *new* ``TaklerServer`` -- a new process, as far as the code under test
is concerned -- restores it, and the job that was already running reports over
real gRPC with the password of its own try.

``Auth_Mode`` is ``enabled`` throughout, and that is the point of the whole file.
Under ``disabled`` the detector never evaluates ``Z1``, so the report would be
accepted even if the snapshot carried no password at all and the test would pass
against the bug it exists to catch.

Two claims, and the second is what gives the first its teeth:

* the report of the restored job's own password comes back with ``flag=0`` and
  the task is not aborted (Requirements 5.4, 5.5, 16.7);
* a report carrying *another* password against the same restored server is still
  rejected as a zombie. Without this, a server that ignored ``Z1`` -- or accepted
  everything after a restore -- would satisfy the first claim.

The server runs in a background event-loop thread because
``TaklerServiceClient`` is fully blocking; the same split as in
``tests/server/test_zombie_after_requeue.py``.

No password is printed, put into a test name or into an assertion message: every
password is only ever read inside an assertion expression or handed to the code
under test.

Validates: Requirements 5.4, 5.5, 16.7
"""

from __future__ import annotations

import asyncio
import socket
import threading
from pathlib import Path
from typing import Iterator, Optional, Tuple

import pytest

from takler.client.credentials import ENV_JOB_PASSWORD, ENV_TLS_CA_FILE
from takler.client.service_client import TaklerServiceClient
from takler.core import Flow, NodeStatus
from takler.core.bunch import Bunch
from takler.core.task_node import Task
from takler.exceptions import ZombieError
from takler.server import TaklerServer
from takler.server.checkpoint import CheckpointManager
from takler.server.connect_config import (
    TAKLER_AUTH_MODE,
    TAKLER_ZOMBIE_POLICY,
    AuthMode,
    ZombiePolicy,
    generate_connect_config,
)
from takler.server.protocol.error_code import SUCCESS, error_code_for_exception


#: Loopback keeps the test hermetic: no name resolution, no traffic leaving the
#: machine.
LOCALHOST = "127.0.0.1"

FLOW_NAME = "flow1"
TASK_NAME = "task1"
TASK_PATH = "/flow1/task1"

#: The ``task_id`` the running job reported at ``init``, i.e. its scheduler job
#: id. Kept identical across the restart because the job did not change.
JOB_TASK_ID = "job-1"

#: Main-loop interval of the in-process server. The scheduler notices
#: ``should_stop`` only between two iterations, so a long interval would make the
#: fixture teardown wait that long.
TEST_MAIN_LOOP_INTERVAL = 0.05

#: Retry_Window of the test client, in seconds. The default for a Child_Command
#: is 86400 -- a job script is meant to sit out a whole server outage -- which in
#: a test would turn "the server is not there" into a day-long hang instead of a
#: failure.
TEST_RETRY_WINDOW = 5.0

#: ``ServiceResponse.flag`` of a rejected Child_Command. Derived from the
#: exception the handler maps rather than written as a literal, so this test
#: cannot drift from the Error_Code table.
ZOMBIE_FLAG = error_code_for_exception(ZombieError("zombie"))


# ---------------------------------------------------------------------------
# The state a restart has to survive
# ---------------------------------------------------------------------------


def in_flight_bunch(port: int) -> Tuple[Bunch, str]:
    """A bunch holding one active task, as the old server process left it.

    Built through the real operations, not by setting attributes: ``run``
    submits the task and, through ``increment_try_no``, generates the
    Job_Password of this try, and ``init`` is the first thing the job itself
    does, which makes the task active. That is what makes the snapshot below a
    snapshot of a genuine in-flight state.

    The flow is suspended so that the restored tree stays put: the new server
    really runs its main loop, and a suspended root stops
    ``check_dependencies`` from submitting anything.

    ``port`` goes into the bunch's server state so that the restored address
    matches the new server's and the restore's address verification stays a
    quiet INFO -- a mismatch is a separate requirement's subject
    (Requirement 6.20) and would only add noise here.

    Returns:
        The bunch and the Job_Password of the running job, i.e. what the job
        exports as ``TAKLER_PASS``.
    """
    bunch = Bunch(host=LOCALHOST, port=str(port))

    flow = Flow(FLOW_NAME)
    flow.add_task(TASK_NAME)
    bunch.add_flow(flow)
    # ``begin`` requeues the tree, which clears the passwords, so it has to come
    # before the job is started.
    flow.begin()
    flow.suspend()

    task: Task = bunch.find_node(TASK_PATH)
    task.run()
    task.init(task_id=JOB_TASK_ID)
    assert task.state.node_status is NodeStatus.active

    password = task.job_password
    assert password  # the run generated one, so the scenario is meaningful
    return bunch, password


def write_snapshot(bunch: Bunch, checkpoint_file: Path) -> None:
    """Write one real snapshot of ``bunch``, the way the old server would.

    Through ``CheckpointManager.write_checkpoint`` rather than hand-written
    JSON: a snapshot nobody writes would make this test pass against a format
    that never occurs.
    """
    manager = CheckpointManager(bunch=bunch, checkpoint_file=checkpoint_file)
    assert manager.write_checkpoint() is True


# ---------------------------------------------------------------------------
# The restarted server
# ---------------------------------------------------------------------------


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


def make_server(monkeypatch, tmp_path: Path, port: int) -> TaklerServer:
    """Build the restarted server: ``Auth_Mode=enabled``, ``fail`` policy.

    The settings arrive through the environment variables rather than through
    constructor arguments because that is the only way in: ``Auth_Mode`` and
    ``Zombie_Policy`` are resolved by ``TaklerServer.__init__`` itself.

    With ``Auth_Mode=enabled`` an Operator_Secret_File has to exist, otherwise
    ``start()`` refuses to bring the server up (Requirement 7.4). Its content is
    irrelevant here -- no Operator_Command crosses the wire in this file -- but
    the file has to be there and owner-readable only, so that its permissions do
    not add a warning of their own.

    The server is *not* started here: ``start()`` is what restores the snapshot,
    so the snapshot has to be on disk first.
    """
    monkeypatch.setenv(TAKLER_AUTH_MODE, AuthMode.ENABLED.value)
    monkeypatch.setenv(TAKLER_ZOMBIE_POLICY, ZombiePolicy.FAIL.value)

    connect_config = generate_connect_config()
    secret_file = tmp_path / "operator.secret"
    secret_file.write_text("s3cret\n")
    secret_file.chmod(0o600)
    connect_config.security.operator_secret_file = str(secret_file)

    return TaklerServer(
        host=LOCALHOST,
        port=port,
        connect_config=connect_config,
        checkpoint_file=tmp_path / "takler.check",
    )


class ServedServer:
    """Runs a :class:`TaklerServer` in a background event-loop thread.

    ``TaklerServer`` is an ``asyncio`` server and ``TaklerServiceClient`` is
    fully blocking, so calling the client from the loop that has to answer it
    would deadlock on the first command. Owning the loop in a thread keeps the
    test body plain synchronous code.
    """

    def __init__(self, server: TaklerServer) -> None:
        self.server = server
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._error: Optional[BaseException] = None

    def start(self, timeout: float = 15.0) -> "ServedServer":
        self._thread = threading.Thread(
            target=self._thread_main, name="takler-restart-test-server", daemon=True
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
        # ``start()`` is the restart: it restores the snapshot -- node states and
        # Job_Passwords -- before the port is bound.
        await self.server.start()
        run_task = self._loop.create_task(self.server.run(), name="takler.test.server")
        self._ready.set()
        await run_task


class Restarted:
    """The restored server plus what the still-running job knows about itself."""

    def __init__(self, running: ServedServer, password: str, port: int) -> None:
        self.running = running
        self.server = running.server
        #: The Job_Password of the try that was in flight across the restart.
        self.password = password
        self.port = port

    @property
    def task(self) -> Task:
        return self.server.bunch.find_node(TASK_PATH)


@pytest.fixture
def restarted(monkeypatch, tmp_path) -> Iterator[Restarted]:
    """A server that restored a snapshot taken while a task was active.

    The whole restart happens in here: the old process's snapshot is written,
    then a brand new ``TaklerServer`` over the same Checkpoint_File is started
    and restores it. The test bodies only see the state afterwards.
    """
    port = free_port()
    server = make_server(monkeypatch, tmp_path, port)

    source_bunch, password = in_flight_bunch(port)
    write_snapshot(source_bunch, server.checkpoint_manager.checkpoint_file)
    # Nothing of the old process is shared with the new server: the password has
    # to come back out of the file.
    assert server.bunch.flows == {}

    running = ServedServer(server).start()
    try:
        yield Restarted(running, password, port)
    finally:
        running.stop()


# ---------------------------------------------------------------------------
# The client half
# ---------------------------------------------------------------------------


def report_complete(monkeypatch, restarted: Restarted, password: str):
    """Send one ``complete`` Child_Command carrying ``password``.

    ``complete`` is what the job runs last, and the client reads ``TAKLER_PASS``
    out of its own environment exactly as it would inside a job script, so the
    password travels as the ``takler-pass`` metadata key over real gRPC and
    through the Auth_Interceptor before any handler sees it.

    ``TAKLER_TLS_CA_FILE`` is cleared so that a developer's own environment
    cannot turn this into a TLS connection against a plaintext server.

    Returns:
        The ``ServiceResponse``, including a non-zero ``flag``: a rejected
        command is a response, not an exception.
    """
    monkeypatch.setenv(ENV_JOB_PASSWORD, password)
    monkeypatch.delenv(ENV_TLS_CA_FILE, raising=False)

    client = TaklerServiceClient(
        host=LOCALHOST,
        port=restarted.port,
        retry_window=TEST_RETRY_WINDOW,
    )
    return client.complete(node_path=TASK_PATH)


def snapshot_of(task: Task) -> Tuple:
    """The five attributes a rejected Child_Command must leave untouched."""
    return (
        task.state.node_status,
        task.task_id,
        task.try_no,
        task.aborted_reason,
        task.job_password,
    )


# ---------------------------------------------------------------------------
# The in-flight job survives the restart (Requirements 5.4, 5.5, 16.7)
# ---------------------------------------------------------------------------


def test_restore_brings_the_in_flight_state_and_password_back(restarted: Restarted):
    """The precondition of the acceptance case, asserted on its own.

    If the restore dropped the password, the command test below would fail with
    a zombie rejection and the cause would be ambiguous; here it is not.
    """
    task = restarted.task

    assert task.state.node_status is NodeStatus.active
    assert task.task_id == JOB_TASK_ID
    # Requirement 5.4: the password of the running job came back out of the
    # snapshot, so the detector has something to match ``takler-pass`` against.
    assert task.job_password == restarted.password


def test_report_of_the_in_flight_job_after_a_restart_is_accepted(
    monkeypatch, restarted: Restarted
):
    """Requirements 5.4, 5.5, 16.7 under ``Auth_Mode=enabled``.

    The job that was running before the restart reports with the password of its
    own try and is served: ``flag=0``, and the task ends up complete rather than
    aborted.
    """
    response = report_complete(monkeypatch, restarted, restarted.password)

    assert response.flag == SUCCESS

    task = restarted.task
    # The report was applied, and the task was not judged a zombie: neither the
    # status nor the aborted reason of a rejection is here.
    assert task.state.node_status is NodeStatus.complete
    assert task.state.node_status is not NodeStatus.aborted
    assert not task.aborted_reason


def test_report_carrying_another_password_after_a_restart_is_still_rejected(
    monkeypatch, restarted: Restarted
):
    """The control case: ``Z1`` is still in force on the restored server.

    Without this, a server that skipped the password comparison after a restore
    -- or one that accepted every Child_Command -- would pass the test above.
    The ``fail`` policy answers with the zombie Error_Code and leaves all five
    task attributes untouched (Requirement 10.2).
    """
    task = restarted.task
    before = snapshot_of(task)
    # A password of the same shape as a real one, belonging to no try of this
    # task: what a stray job from an earlier try would present.
    foreign_password = "not-the-password-of-this-try"
    assert foreign_password != restarted.password

    response = report_complete(monkeypatch, restarted, foreign_password)

    assert response.flag == ZOMBIE_FLAG
    assert snapshot_of(task) == before
    assert task.state.node_status is NodeStatus.active
    assert restarted.password not in response.message
