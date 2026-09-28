"""
The job password must not reach a ``show`` response.

Covers requirements 4.11 and 16.8 of the ``m2-security`` spec.

``Scheduler.handle_request_show`` builds a dedicated safe projection. This
module pins the response received by an operator and its data-only client view;
checkpoint's separate password map is never part of this query document.

The task under test is put into the active state, which is exactly the state
whose password is live and persisted.

No test here prints a password or puts one into a test name or an assertion
message: passwords are only ever read inside an assertion expression.
"""

import json

import pytest

from takler.core import Bunch, Flow, NodeStatus
from takler.core.parameter import TAKLER_PASS
from takler.protocol.commands import ShowRequest
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

    output = scheduler.handle_request_show(ShowRequest(**SHOW_KWARGS))

    # The response does describe the active task and its user parameter, so
    # the absence of the password below is meaningful.
    assert "sentinel-value" in output
    assert task1.job_password not in output


def test_bunch_to_dict_does_not_contain_job_password(scheduler):
    """Requirement 4.10 at bunch level: the same serialization feeds checkpoints."""
    task1 = scheduler.bunch.find_node("/flow1/task1")

    assert task1.job_password not in json.dumps(scheduler.bunch.to_dict())


def test_client_query_view_has_no_password_or_execution_methods(scheduler):
    from takler.query import parse_show

    task = scheduler.bunch.find_node("/flow1/task1")
    snapshot = parse_show(scheduler.handle_request_show(ShowRequest(**SHOW_KWARGS)))
    view = snapshot.find_node("/flow1/task1")
    assert view.state == "active"
    assert view.try_no == task.try_no
    assert view.task_id == task.task_id
    assert not hasattr(view, "job_password")
    assert not hasattr(view, "update_generated_parameters")
    assert snapshot.resolve_parameter(view.path, TAKLER_PASS) is None
