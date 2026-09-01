"""
The job password must not reach a ``show`` response.

Covers requirements 4.11 and 16.8 of the ``m2-security`` spec.

``Scheduler.handle_request_show`` returns the JSON of ``Bunch.to_dict()``, and
the checkpoint file is built from the same method, so requirement 4.11 rests
entirely on the password being neither a serialized node field nor a user
parameter. ``tests/core/test_job_password.py`` pins the key set of
``Task.to_dict()``; this module pins the other end of the pipeline: the response
text an operator actually receives, and what a client rebuilds from it.

The task under test is put into the active state, which is exactly the state
whose password is live and persisted.

No test here prints a password or puts one into a test name or an assertion
message: passwords are only ever read inside an assertion expression.
"""

import json

import pytest

from takler.core import Bunch, Flow, NodeStatus
from takler.core.parameter import TAKLER_PASS
from takler.server.scheduler import Scheduler

SHOW_KWARGS = dict(
    show_parameter=True,
    show_trigger=True,
    show_limit=True,
    show_event=True,
    show_meter=True,
)


@pytest.fixture
def scheduler() -> Scheduler:
    """
    A bunch whose ``/flow1/task1`` is active, i.e. holds a live job password.

    The task goes through the real ``run`` / ``init`` path rather than having
    its status assigned, so ``increment_try_no`` is what generates the
    password, as it does in production.
    """
    flow1 = Flow("flow1")
    with flow1:
        task1 = flow1.add_task("task1")
        flow1.add_task("task2")

    # A user parameter carrying a recognizable value keeps the assertions
    # below non-vacuous: it proves the response really does serialize
    # parameters, so the password's absence is a property of the password
    # rather than of the serialization being empty.
    task1.add_parameter("SENTINEL_USER_PARAM", "sentinel-value")

    bunch = Bunch(name="bunch")
    bunch.add_flow(flow1)
    flow1.begin()

    task1.run()
    task1.init("job-4711")
    assert task1.state.node_status is NodeStatus.active
    assert task1.job_password

    return Scheduler(bunch=bunch)


def test_show_response_does_not_contain_job_password(scheduler):
    """Requirements 4.11, 16.8: the password is absent from the show response."""
    task1 = scheduler.bunch.find_node("/flow1/task1")

    output = scheduler.handle_request_show(**SHOW_KWARGS)

    # The response does describe the active task and its user parameter, so
    # the absence of the password below is meaningful.
    assert "sentinel-value" in output
    assert task1.job_password not in output


def test_bunch_to_dict_does_not_contain_job_password(scheduler):
    """Requirement 4.10 at bunch level: the same serialization feeds checkpoints."""
    task1 = scheduler.bunch.find_node("/flow1/task1")

    assert task1.job_password not in json.dumps(scheduler.bunch.to_dict())


def test_client_round_trip_yields_empty_takler_pass(scheduler):
    """
    Requirement 4.11 on the client side.

    A client parses the show response through ``Bunch.from_dict``. The
    reconstructed task must carry the server's status but an empty
    ``TAKLER_PASS``, which is what proves the password does not survive the
    round trip even indirectly.
    """
    task1 = scheduler.bunch.find_node("/flow1/task1")

    output = scheduler.handle_request_show(**SHOW_KWARGS)
    restored_bunch = Bunch.from_dict(json.loads(output))
    restored_task1 = restored_bunch.find_node("/flow1/task1")

    # The status side of the response did survive, so the empty password is
    # not an artifact of the round trip having lost everything.
    assert restored_task1.state.node_status is NodeStatus.active
    assert restored_task1.try_no == task1.try_no
    assert restored_task1.task_id == task1.task_id

    assert restored_task1.job_password is None
    assert not restored_task1.find_parameter(TAKLER_PASS).value

    # Recomputing the generated parameters, as a client does before rendering
    # or printing, must not conjure a password either.
    restored_task1.update_generated_parameters()
    assert not restored_task1.generated_parameters_only()[TAKLER_PASS].value
    assert TAKLER_PASS not in restored_task1.user_parameters_only()
