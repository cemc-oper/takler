"""The Job_Password must not leak out of a real server, end to end.

Task 12.1 of the *m2-security* spec. ``tests/server/test_password_not_in_show.py``
pins the same property at unit level, on a directly driven ``Scheduler``; this
file pins it on the whole running system, because every surface Requirement 12
names is produced by a different layer and only a live server produces all of
them at once:

* the **log output** of the server process -- console sink and Audit_File-free
  log file, at ``DEBUG``, so no level is exempt (Requirement 12.1);
* the **Audit_Records** the Audit_Logger appended (Requirement 12.4);
* the **``ServiceResponse.message``** of each command the client issued
  (Requirement 12.2), including a refused one, whose message is built from an
  exception the server raised while holding both the presented and the stored
  password (Requirement 12.3);
* the **``show`` response text** an operator receives (Requirement 4.11 /
  16.8), and the tree the client prints from it.

The scenario is one full Child_Command sequence over gRPC -- the server submits
the task itself through its main loop (that is what generates the password), the
client then reports ``init`` and ``complete`` presenting it as ``TAKLER_PASS`` --
plus one Operator_Command and one ``show``, so that the Audit_File is not empty
and the "no password in the audit trail" assertion is not vacuous. ``Auth_Mode``
is ``enabled``: with authentication off the password is never compared, and the
paths that handle it most are never taken.

The server runs in a background event-loop thread because ``TaklerServer`` is
``asyncio`` and ``TaklerServiceClient`` is blocking; the same split as in
``tests/integration/conftest.py`` and ``tests/server/test_zombie_after_requeue.py``.

Every assertion is paired with a non-vacuity assertion on the same text: a
surface that turned out empty would otherwise pass the "does not contain"
check while proving nothing.

No password is printed, put into a test name or into an assertion message:
passwords are only ever read inside an assertion expression or handed to the
code under test.

Validates: Requirements 12.1, 12.2, 12.3, 12.4, 16.8
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import socket
import threading
import time
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import pytest

import takler.logging
from takler.client.credentials import ENV_JOB_PASSWORD
from takler.client.service_client import TaklerServiceClient
from takler.core import Flow, NodeStatus
from takler.core.task_node import Task
from takler.logging.config import ENV_AUDIT_FILE, ENV_LOG_FILE, ENV_LOG_LEVEL
from takler.server import TaklerServer
from takler.server.connect_config import (
    TAKLER_AUTH_MODE,
    TAKLER_ZOMBIE_POLICY,
    AuthMode,
    ZombiePolicy,
    generate_connect_config,
)

LOCALHOST = "127.0.0.1"

FLOW_NAME = "flow1"
TASK_PATH = "/flow1/task1"
TASK_ID = "job-4711"

#: Main-loop interval of the in-process server. Short on purpose: the scheduler
#: submits the queued task and notices ``should_stop`` only between two
#: iterations, so a long interval would slow both the test and its teardown.
TEST_MAIN_LOOP_INTERVAL = 0.05

#: Per-attempt deadline and Retry_Window of the test client. The server is up
#: and local, so a command either answers at once or the test found a real
#: problem; the default child-command window is 86400 seconds.
TEST_SINGLE_TIMEOUT = 10.0
TEST_RETRY_WINDOW = 10.0

#: A stand-in Operator_Secret, so ``show`` and the Control_Command below get
#: past the Auth_Interceptor. Only ever read inside an assertion expression.
OPERATOR_SECRET = "operator-secret-0123456789abcdef"

#: What a stale job would present: a syntactically plausible password that is
#: not the one the server stored.
STALE_PASSWORD = "stale-job-password-0123456789abcdef"

SHOW_KWARGS = dict(
    show_parameter=True,
    show_trigger=True,
    show_limit=True,
    show_event=True,
    show_meter=True,
)


# ---------------------------------------------------------------------------
# the surfaces a leak could appear on
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Surfaces:
    """Every text one run of the scenario produced, per destination."""

    log_file: str
    audit_file: str
    console: str
    messages: List[str]
    show_output: str

    def texts(self) -> Dict[str, str]:
        """The surfaces keyed by a name an assertion failure can name."""
        return {
            "log file": self.log_file,
            "audit file": self.audit_file,
            "console": self.console,
            "response messages": "\n".join(self.messages),
            "show response": self.show_output,
        }

    @property
    def audit_records(self) -> List[dict]:
        """The Audit_File, one parsed record per line."""
        return [
            json.loads(line) for line in self.audit_file.splitlines() if line.strip()
        ]


def assert_absent(surfaces: Surfaces, password: str) -> None:
    """Assert ``password`` is a substring of none of the surfaces."""
    for name, text in surfaces.texts().items():
        assert password not in text, f"job password leaked into the {name}"


# ---------------------------------------------------------------------------
# a real server in a background event-loop thread
# ---------------------------------------------------------------------------


def free_port() -> int:
    """Return a port that is free right now."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


class ServedServer:
    """Runs a :class:`TaklerServer` in a background event-loop thread."""

    def __init__(self, server: TaklerServer) -> None:
        self.server = server
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._error: Optional[BaseException] = None

    def start(self, timeout: float = 15.0) -> "ServedServer":
        self._thread = threading.Thread(
            target=self._thread_main, name="takler-no-leak-test-server", daemon=True
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


class Harness:
    """A started server, a connected client, and the surfaces they write to."""

    def __init__(
        self,
        served: ServedServer,
        client: TaklerServiceClient,
        log_file: Path,
        audit_file: Path,
        capfd,
    ) -> None:
        self.served = served
        self.client = client
        self.log_file = log_file
        self.audit_file = audit_file
        self._capfd = capfd
        self.messages: List[str] = []
        self._console = ""

    # the scenario ---------------------------------------------------------

    @property
    def server(self) -> TaklerServer:
        return self.served.server

    @property
    def task(self) -> Task:
        return self.server.bunch.find_node(TASK_PATH)

    def wait_for_submitted(self, timeout: float = 10.0) -> str:
        """Wait until the server's own main loop submitted the task.

        The submission is what generates the Job_Password of this try, and it is
        done by the server rather than by the test, so the password under test is
        one the production path produced.

        Returns:
            The Job_Password the server stored for the submitted try.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            task = self.task
            if task.state.node_status is NodeStatus.submitted and task.job_password:
                return task.job_password
            time.sleep(0.02)
        raise TimeoutError(f"{TASK_PATH} was not submitted within {timeout} seconds")

    def record(self, response) -> object:
        """Keep a command's ``message`` as one of the surfaces, and return it."""
        self.messages.append(response.message)
        return response

    def collect(self, show_output: str = "") -> Surfaces:
        """Read every destination back.

        Both file sinks flush per record, so they can be read while the server
        keeps running; the console is drained through pytest's capture, which
        also holds what the client itself printed.
        """
        captured = self._capfd.readouterr()
        self._console += captured.out + captured.err
        return Surfaces(
            log_file=_read(self.log_file),
            audit_file=_read(self.audit_file),
            console=self._console,
            messages=list(self.messages),
            show_output=show_output,
        )


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def build_flow() -> Flow:
    """One begun flow holding a single dependency-free task."""
    flow = Flow(FLOW_NAME)
    task1 = flow.add_task("task1")
    # A recognizable user parameter keeps the ``show`` assertions non-vacuous:
    # it proves the response really serializes parameters, so the password's
    # absence is a property of the password rather than of an empty tree.
    task1.add_parameter("SENTINEL_USER_PARAM", "sentinel-value")
    flow.begin()
    return flow


@pytest.fixture
def harness(monkeypatch, tmp_path: Path, capfd) -> Iterator[Harness]:
    """A live server with authentication on and every sink pointed at tmp_path.

    The logging destinations arrive through the environment because
    ``TaklerServer.start()`` configures logging itself; ``DEBUG`` is what makes
    "no level is exempt" testable.
    """
    log_file = tmp_path / "takler.log"
    audit_file = tmp_path / "audit" / "audit.jsonl"

    monkeypatch.setenv(TAKLER_AUTH_MODE, AuthMode.ENABLED.value)
    monkeypatch.setenv(TAKLER_ZOMBIE_POLICY, ZombiePolicy.FAIL.value)
    monkeypatch.setenv(ENV_LOG_LEVEL, "DEBUG")
    monkeypatch.setenv(ENV_LOG_FILE, str(log_file))
    monkeypatch.setenv(ENV_AUDIT_FILE, str(audit_file))

    secret_file = tmp_path / "operator.secret"
    secret_file.write_text(f"{OPERATOR_SECRET}\n")
    secret_file.chmod(0o600)

    connect_config = generate_connect_config()
    connect_config.security.operator_secret_file = str(secret_file)

    server = TaklerServer(
        host=LOCALHOST,
        port=free_port(),
        connect_config=connect_config,
        checkpoint_file=tmp_path / "takler.check",
    )
    server.bunch.add_flow(build_flow())

    served = ServedServer(server).start()
    client = TaklerServiceClient(
        host=LOCALHOST,
        port=server.network_service.port,
        single_timeout=TEST_SINGLE_TIMEOUT,
        retry_window=TEST_RETRY_WINDOW,
        secret_file=str(secret_file),
    )
    client.start()
    try:
        yield Harness(served, client, log_file, audit_file, capfd)
    finally:
        client.close_channel()
        served.stop()
        # The server installed process-wide sinks on this tmp_path; drop them so
        # the next test starts from the default configuration.
        takler.logging.configure(level="INFO", console=False)
        takler.logging._reset_configured_state()


# ---------------------------------------------------------------------------
# the happy path: submit -> init -> complete
# ---------------------------------------------------------------------------


def test_a_full_child_command_sequence_leaks_no_password(monkeypatch, harness) -> None:
    """The password the job presents appears on none of the surfaces.

    Validates: Requirements 12.1, 12.2, 12.4, 16.8
    """
    password = harness.wait_for_submitted()
    # What ``head.takler`` exports for the job; the client reads it per call.
    monkeypatch.setenv(ENV_JOB_PASSWORD, password)

    init = harness.record(
        harness.client.run_command_init(node_path=TASK_PATH, task_id=TASK_ID)
    )
    complete = harness.record(harness.client.run_command_complete(node_path=TASK_PATH))
    assert init.flag == 0
    assert complete.flag == 0
    assert harness.task.state.node_status is NodeStatus.complete

    # One Operator_Command, so the Audit_File holds a record at all, and one
    # ``show``, which is the response an operator reads.
    suspend = harness.record(harness.client.run_command_suspend(node_path=[TASK_PATH]))
    assert suspend.flag == 0
    show = harness.client.run_request_show(**SHOW_KWARGS)

    surfaces = harness.collect(show_output=show.output)

    # Non-vacuity: every surface really carries this run.
    assert TASK_PATH in surfaces.log_file
    assert [record["command"] for record in surfaces.audit_records] == ["suspend"]
    assert "sentinel-value" in surfaces.show_output
    assert "received: success" in surfaces.console

    assert_absent(surfaces, password)


def test_the_serialized_task_carries_no_password(monkeypatch, harness) -> None:
    """``Task.to_dict()`` on the live, active task holds no password.

    The same serialization feeds the ``show`` response and the Checkpoint_File's
    ``bunch`` section, so this is where a password added to the node's own
    fields would first become visible.

    Validates: Requirements 4.10, 12.4
    """
    password = harness.wait_for_submitted()
    monkeypatch.setenv(ENV_JOB_PASSWORD, password)

    harness.client.run_command_init(node_path=TASK_PATH, task_id=TASK_ID)
    task = harness.task
    assert task.state.node_status is NodeStatus.active

    serialized = json.dumps(task.to_dict())

    # Non-vacuity: the serialization does describe this try.
    assert TASK_ID in serialized
    assert password not in serialized


# ---------------------------------------------------------------------------
# the refusal path: the message is built from an exception (Requirement 12.3)
# ---------------------------------------------------------------------------


def test_a_refused_child_command_leaks_neither_password(monkeypatch, harness) -> None:
    """A stale job's report names the condition, not the two passwords.

    The ``fail`` policy answers a mismatching Job_Password with ``flag=31`` and a
    ``message`` built from the ``ZombieError`` the server raised while holding
    both the presented and the stored value -- the one place where a careless
    f-string would put a password on the wire.

    Validates: Requirements 12.1, 12.2, 12.3, 12.4
    """
    password = harness.wait_for_submitted()
    monkeypatch.setenv(ENV_JOB_PASSWORD, STALE_PASSWORD)

    refused = harness.record(harness.client.run_command_complete(node_path=TASK_PATH))

    assert refused.flag == 31
    # The refusal changed nothing: the current try is still the one in flight.
    assert harness.task.state.node_status is NodeStatus.submitted
    assert harness.task.job_password == password

    surfaces = harness.collect()

    # Non-vacuity: the surfaces describe this refusal.
    assert TASK_PATH in surfaces.messages[0]
    assert [record["outcome"] for record in surfaces.audit_records] == ["zombie"]

    assert_absent(surfaces, password)
    assert_absent(surfaces, STALE_PASSWORD)
