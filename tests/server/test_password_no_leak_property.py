"""Property-based test: a Job_Password never appears as a substring anywhere.

Property 4 of the *m2-security* design (Requirements 12.4, 16.8, 4.10, 4.11):
for any Job_Password ``p``, none of the texts the server produces while handling
one full Child_Command sequence contains ``p`` as a substring -- the log at any
level, the Audit_Records, the ``ServiceResponse.message`` of every command, the
``show`` response an operator reads, and the serialization ``Task.to_dict()``
feeds both of the latter (Requirements 4.10, 4.11).

``tests/server/test_password_no_leak.py`` pins the same property end to end, on
a real gRPC server, with the password ``secrets.token_urlsafe`` actually
produced. This file trades that fidelity for two things the integration test
cannot have:

* **shape coverage.** The password under test is *injected* into the task rather
  than being the generated one, so the examples include the shapes a real
  ``token_urlsafe`` value never has and that a leak-prevention mechanism is most
  likely to mishandle: quotes, backslashes, bare newlines, non-ASCII text, regex
  metacharacters, and fragments of the node paths and parameter names the
  surfaces legitimately carry. Injection is safe because nothing in the server
  constrains what a Job_Password may hold -- it is compared as bytes
  (:func:`takler.server.zombie._constant_time_equal`) and interpolated nowhere.
* **speed.** 100 examples must not mean 100 gRPC servers, so the scenario is
  driven in process: :class:`~takler.server.network_service.TaklerService`
  handlers are awaited directly, with the Credential_Metadata published into the
  context variable the Auth_Interceptor would have published it into. Everything
  below the RPC transport -- the exception boundary, the audit record points, the
  Zombie_Detector, the Scheduler, the node tree -- is the production code, and
  the log and the Audit_File are real files.

The scenario per example is one complete Child_Command sequence (``init`` then
``complete``, both presenting the password), then a stale ``complete`` which the
``fail`` policy refuses -- the refusal path is where the server holds the
presented and the stored password at the same time, so it is where a careless
f-string would put one into a message -- then one Control_Command, so the
Audit_File is not empty, and one ``show``.

No password is printed, put into a test name or into an assertion message:
generated values are only ever read inside an assertion expression or handed to
the code under test.

**Validates: Requirements 12.4, 16.8, 4.10, 4.11**
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import string
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

from hypothesis import assume, given, settings
from hypothesis import strategies as st

import takler.logging
from takler.core import Bunch, Flow, NodeStatus
from takler.core.task_node import Task
from takler.server.audit import AuditLogger
from takler.server.auth import (
    CallCredentials,
    reset_call_credentials,
    set_call_credentials,
)
from takler.server.connect_config import AuthMode, ZombiePolicy
from takler.server.network_service import TaklerService
from takler.server.protocol import takler_pb2
from takler.server.scheduler import Scheduler
from takler.server.zombie import ZombieDetector

FLOW_NAME = "flow1"
TASK_NAME = "task1"
TASK_PATH = "/flow1/task1"
TASK_ID = "job-4711"
USER_PARAMETER = "SENTINEL_USER_PARAM"
USER_PARAMETER_VALUE = "sentinel-value"

#: The caller identity the records are written under. Not a secret.
JOB_USER = "job-user"
OPERATOR_USER = "operator-user"
PEER = "ipv4:127.0.0.1:51234"

#: A stand-in Operator_Secret for the Control_Command. Never compared here --
#: the Auth_Interceptor is not in the in-process path -- but carried so the
#: credentials of the audited command look like a real operator's.
OPERATOR_SECRET = "operator-secret-0123456789abcdef"

#: What the stale job presents: a plausible password that is not the stored one.
STALE_PASSWORD = "stale-job-password-0123456789abcdef"

#: The ``flag`` of a refused Child_Command under the ``fail`` policy.
ZOMBIE_FLAG = 31

SHOW_KWARGS = dict(
    show_parameter=True,
    show_trigger=True,
    show_limit=True,
    show_event=True,
    show_meter=True,
)


# ---------------------------------------------------------------------------
# the strategy: adversarial password shapes
# ---------------------------------------------------------------------------

#: Characters a leak-prevention mechanism is most likely to mishandle, next to
#: the ordinary ones so that plain values stay in the example space.
#:
#: * ``"`` ``'`` ``\`` -- JSON and repr escaping: a password that survives as
#:   ``\"...\"`` in a JSON surface is a leak this test's raw substring check
#:   would miss, so quotes belong in the alphabet to make the escaping happen.
#: * ``\n`` ``\r`` -- line-oriented surfaces: a password holding a bare newline
#:   splits a log line or an Audit_File line, and "the second half of the
#:   password is now its own line" is still a leak.
#: * ``$`` ``*`` ``.`` ``^`` ``[`` ``]`` ``|`` ``(`` ``)`` -- regex
#:   metacharacters, in case a redaction is ever implemented with ``re.sub``
#:   rather than ``str.replace``.
#: * ``中`` ``é`` ``\u2028`` -- non-ASCII: ``ensure_ascii=False`` keeps these raw
#:   in the Audit_File, and ``U+2028`` is a line boundary to Python but not to
#:   JSON.
_HOSTILE_CHARACTERS = "\"'\\\n\r\t $*.^[]|(){}?+中é\u2028"
_PASSWORD_ALPHABET = string.ascii_letters + string.digits + "-_" + _HOSTILE_CHARACTERS

#: Fragments of the text the surfaces legitimately carry. A password *containing*
#: one of them is the interesting adversarial case: a leak check implemented by
#: subtracting the known-public text would pass such a password by mistake.
_PUBLIC_FRAGMENTS: Tuple[str, ...] = (
    TASK_PATH,
    FLOW_NAME,
    TASK_NAME,
    TASK_ID,
    USER_PARAMETER,
    USER_PARAMETER_VALUE,
    "complete",
    "suspend",
    "zombie",
    "TAKLER_PASS",
)

#: Minimum length of a generated password, which is also the minimum length
#: Requirement 4.2 gives a real one. It is what keeps the property free of
#: false positives: a value this long cannot appear in a surface by
#: coincidence, so "the password is a substring of a surface" means a leak and
#: not an accident of the surface's own vocabulary. Do not lower it -- a short
#: value such as ``"complete"`` is a substring of every log line of the run and
#: would fail the property while proving nothing.
MIN_PASSWORD_LENGTH = 32

#: The known-public text a generated password must not itself be a substring of.
#: The length bound above makes this near-redundant; it is kept because it is
#: the one coincidence that is cheap to rule out outright, and it states which
#: values are excluded and why.
_PUBLIC_TEXT = " ".join(_PUBLIC_FRAGMENTS)


@st.composite
def job_passwords(draw: st.DrawFn) -> str:
    """Draw a Job_Password of an adversarial shape.

    Built as a sequence of chunks, each either free text over
    :data:`_PASSWORD_ALPHABET` or one of :data:`_PUBLIC_FRAGMENTS`, so the
    examples range from plain ``token_urlsafe``-looking values to values holding
    quotes, newlines, non-ASCII characters and pieces of the node paths the
    surfaces print anyway. Padding at the end enforces
    :data:`MIN_PASSWORD_LENGTH` without capping the interesting part.

    Returns:
        A password of at least :data:`MIN_PASSWORD_LENGTH` characters which is
        not a substring of the run's known-public text.
    """
    chunks = draw(
        st.lists(
            st.one_of(
                st.text(alphabet=_PASSWORD_ALPHABET, min_size=1, max_size=12),
                st.sampled_from(_PUBLIC_FRAGMENTS),
            ),
            min_size=1,
            max_size=6,
        )
    )
    password = "".join(chunks)
    if len(password) < MIN_PASSWORD_LENGTH:
        padding = draw(
            st.text(
                alphabet=string.ascii_letters + string.digits,
                min_size=MIN_PASSWORD_LENGTH - len(password),
                max_size=MIN_PASSWORD_LENGTH - len(password),
            )
        )
        password += padding
    # The one coincidence worth ruling out outright: a value the run prints
    # anyway is not a password whose absence could be asserted.
    assume(password not in _PUBLIC_TEXT)
    return password


# ---------------------------------------------------------------------------
# the surfaces one run produced
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Surfaces:
    """Every text one run of the scenario produced, per destination."""

    log_file: str
    audit_file: str
    messages: List[str]
    show_output: str
    serialized_task: str

    def texts(self) -> Dict[str, str]:
        """The surfaces keyed by a name an assertion failure can name."""
        return {
            "log file": self.log_file,
            "audit file": self.audit_file,
            "response messages": "\n".join(self.messages),
            "show response": self.show_output,
            "serialized task": self.serialized_task,
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
# the scenario, driven in process
# ---------------------------------------------------------------------------


class FakeContext:
    """The little of a gRPC ``ServicerContext`` the audited handlers touch."""

    def peer(self) -> str:
        return PEER


def build_service(bunch: Bunch, audit_logger: AuditLogger) -> TaklerService:
    """Wire a service the way ``TaklerServer`` does, minus the gRPC server.

    Auth_Mode is ``enabled`` and Zombie_Policy is ``fail``: with authentication
    off the Job_Password is never compared, so the code paths that handle it most
    are never taken, and ``fail`` is the policy that produces a refusal message
    from an exception raised while both passwords are in hand.
    """
    detector = ZombieDetector(
        auth_mode=AuthMode.ENABLED,
        zombie_policy=ZombiePolicy.FAIL,
        audit_logger=audit_logger,
    )
    scheduler = Scheduler(bunch=bunch, zombie_detector=detector)
    return TaklerService(scheduler=scheduler, audit_logger=audit_logger)


def build_bunch(password: str) -> Tuple[Bunch, Task]:
    """A begun flow whose single task is submitted and holds ``password``.

    The task goes through the real ``run`` path, so ``increment_try_no`` produced
    a password as it does in production; that value is then replaced by the
    generated one, which is the whole point of this file -- the server places no
    constraint on what a Job_Password holds, so the property has to be checked
    for shapes ``secrets.token_urlsafe`` cannot produce.
    """
    flow = Flow(FLOW_NAME)
    task = flow.add_task(TASK_NAME)
    # A recognizable user parameter keeps the ``show`` assertion non-vacuous: it
    # proves the response really serializes parameters, so the password's
    # absence is a property of the password rather than of an empty tree.
    task.add_parameter(USER_PARAMETER, USER_PARAMETER_VALUE)

    bunch = Bunch(name="bunch")
    bunch.add_flow(flow)
    flow.begin()

    task.run()
    assert task.state.node_status is NodeStatus.submitted
    task.job_password = password
    return bunch, task


@contextlib.contextmanager
def credentials_of(password: str = None, secret: str = None, user: str = JOB_USER):
    """Publish Credential_Metadata for the duration of one call.

    This is the context variable the Auth_Interceptor writes, and the
    Zombie_Detector's ``Z1`` check and both audit record points read; setting it
    directly is what lets the scenario run without a gRPC stack.
    """
    token = set_call_credentials(
        CallCredentials(job_password=password, secret=secret, user=user, peer=PEER)
    )
    try:
        yield
    finally:
        reset_call_credentials(token)


async def drive(service: TaklerService, password: str) -> Tuple[List[str], str]:
    """Run the scenario against ``service``, returning the texts it answered.

    One complete Child_Command sequence presenting ``password``, one stale
    ``complete`` presenting another value, one Control_Command and one ``show``.

    Returns:
        The ``message`` of every command issued, and the ``show`` output.
    """
    messages: List[str] = []
    options = takler_pb2.ChildCommandOptions(node_path=TASK_PATH)
    context = FakeContext()

    with credentials_of(password=password):
        init = await service.RunCommandInit(
            takler_pb2.InitCommand(child_options=options, task_id=TASK_ID), context
        )
        complete = await service.RunCommandComplete(
            takler_pb2.CompleteCommand(child_options=options), context
        )
    messages.extend([init.message, complete.message])
    assert init.flag == 0
    assert complete.flag == 0
    assert service.scheduler.bunch.find_node(TASK_PATH).state.node_status is (
        NodeStatus.complete
    )

    # The refusal path: the task is complete, so a further report hits ``Z2``
    # and the ``fail`` policy answers with a message built from the exception.
    with credentials_of(password=STALE_PASSWORD):
        refused = await service.RunCommandComplete(
            takler_pb2.CompleteCommand(child_options=options), context
        )
    messages.append(refused.message)
    assert refused.flag == ZOMBIE_FLAG

    with credentials_of(secret=OPERATOR_SECRET, user=OPERATOR_USER):
        suspend = await service.RunCommandSuspend(
            takler_pb2.SuspendCommand(node_path=[TASK_PATH]), context
        )
        show = await service.RunRequestShow(
            takler_pb2.ShowRequest(**SHOW_KWARGS), context
        )
    messages.append(suspend.message)
    assert suspend.flag == 0

    return messages, show.output


def run_scenario(password: str) -> Surfaces:
    """Handle one full Child_Command sequence and collect every surface.

    Logging is configured per example, at ``DEBUG`` so no level is exempt
    (Requirement 12.1) and with the console off so 100 examples do not bury the
    test output, into a temporary directory of this example's own. A ``tmp_path``
    fixture cannot be used: a function-scoped fixture is created once for all
    examples of a ``@given`` test.
    """
    with tempfile.TemporaryDirectory() as directory:
        log_file = Path(directory) / "takler.log"
        audit_file = Path(directory) / "audit" / "audit.jsonl"
        takler.logging.configure(
            level="DEBUG", console=False, log_file=log_file, audit_file=audit_file
        )

        bunch, task = build_bunch(password)
        service = build_service(bunch, AuditLogger(audit_file))
        messages, show_output = asyncio.run(drive(service, password))
        # Read while the sinks are still installed, and serialize the task in
        # the state the run left it: ``to_dict`` is what feeds both the ``show``
        # response and the Checkpoint_File's ``bunch`` section, so it is where a
        # password added to the node's own fields would first become visible
        # (Requirement 4.10).
        serialized_task = json.dumps(task.to_dict())
        surfaces = Surfaces(
            log_file=_read(log_file),
            audit_file=_read(audit_file),
            messages=messages,
            show_output=show_output,
            serialized_task=serialized_task,
        )

    # Drop the sinks pointed at the (now deleted) directory so the next example
    # -- and the next test -- starts from a configuration of its own.
    takler.logging.configure(level="INFO", console=False)
    takler.logging._reset_configured_state()
    return surfaces


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


# Feature: m2-security, Property 4: 口令不作为子串泄露
# Validates: Requirements 12.4, 16.8, 4.10, 4.11
@settings(max_examples=100, deadline=None)
@given(password=job_passwords())
def test_no_surface_contains_the_job_password(password: str) -> None:
    """No text one Child_Command sequence produces contains the password.

    Handles ``init`` / ``complete`` / a refused ``complete`` / ``suspend`` /
    ``show`` with ``password`` as the task's Job_Password, then asserts the
    password is a substring of neither the DEBUG log file, the Audit_File, any
    ``ServiceResponse.message``, the ``show`` response, nor ``Task.to_dict()``
    (Requirements 12.4, 16.8, 4.10, 4.11).

    Every assertion is paired with a non-vacuity check on the same text: a
    surface that came out empty would otherwise pass the "does not contain"
    check while proving nothing.
    """
    surfaces = run_scenario(password)

    # Non-vacuity: every surface really carries this run.
    assert TASK_PATH in surfaces.log_file
    assert [record["outcome"] for record in surfaces.audit_records] == [
        "zombie",
        "success",
    ]
    assert USER_PARAMETER_VALUE in surfaces.show_output
    assert TASK_ID in surfaces.serialized_task
    assert TASK_PATH in surfaces.messages[2]

    assert_absent(surfaces, password)
    # The value the stale job presented is not a password of the current run,
    # but it is a credential of the caller and must not be echoed either.
    assert_absent(surfaces, STALE_PASSWORD)
