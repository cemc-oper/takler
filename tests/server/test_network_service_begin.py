"""Unit tests for the ``RunCommandBegin`` RPC handler.

Covers Requirements 8.1, 3.3 and 3.4: the handler runs behind the existing
``_handle_command`` boundary, returns ``flag=0`` on success (an empty
``flow_name`` meaning all flows), and maps a failure to the Error_Code of the
raised exception through ``_command_error_response``.

There is no ``pytest-asyncio`` in this project (see ``pyproject.toml``), so the
async handlers are driven with :func:`asyncio.run`, mirroring the convention of
``tests/server/test_exception_resilience_bug_condition.py``.
"""
import asyncio
from unittest import mock

import pytest

from takler.core import Bunch, Flow
from takler.exceptions import FlowStateError, NodeNotFoundError
from takler.server.network_service import TaklerService
from takler.server.protocol import takler_pb2
from takler.server.protocol.error_code import (
    ERROR_CODE_BY_TYPE,
    SUCCESS,
)
from takler.server.scheduler import Scheduler


def build_flow(name: str) -> Flow:
    flow = Flow(name)
    with flow:
        flow.add_task("task1")
    return flow


@pytest.fixture
def service() -> TaklerService:
    scheduler = Scheduler(bunch=Bunch(name="bunch"))
    return TaklerService(scheduler=scheduler)


def run_begin(service: TaklerService, flow_name: str, force: bool = False):
    request = takler_pb2.BeginCommand(flow_name=flow_name, force=force)
    return asyncio.run(service.RunCommandBegin(request, mock.MagicMock()))


def test_begin_named_flow_returns_success(service):
    """A named flow is begun and the handler returns ``flag=0``."""
    flow = service.scheduler.bunch.add_flow(build_flow("flow1"))

    response = run_begin(service, "flow1")

    assert isinstance(response, takler_pb2.ServiceResponse)
    assert response.flag == SUCCESS
    assert response.message == ""
    assert flow.begun is True


def test_begin_empty_flow_name_begins_all_flows(service):
    """An empty ``flow_name`` is passed through and means all flows."""
    flows = [service.scheduler.bunch.add_flow(build_flow(f"flow{i}")) for i in range(2)]

    response = run_begin(service, "")

    assert response.flag == SUCCESS
    assert all(flow.begun for flow in flows)


def test_begin_force_flag_is_forwarded(service):
    """``force`` lets an already begun flow be begun again."""
    flow = service.scheduler.bunch.add_flow(build_flow("flow1"))
    run_begin(service, "flow1")
    initial_time = flow.calendar.initial_time

    response = run_begin(service, "flow1", force=True)

    assert response.flag == SUCCESS
    assert flow.begun is True
    assert flow.calendar.initial_time >= initial_time


def test_begin_unknown_flow_maps_node_not_found_code(service):
    """An unknown flow name fails with the node-not-found Error_Code."""
    response = run_begin(service, "no_such_flow")

    assert response.flag == ERROR_CODE_BY_TYPE[NodeNotFoundError]
    assert response.flag != SUCCESS
    assert response.message == "NodeNotFoundError: flow is not found: no_such_flow"


def test_begin_already_begun_flow_maps_flow_state_code(service):
    """Beginning an already begun flow fails with the flow-state Error_Code."""
    flow = service.scheduler.bunch.add_flow(build_flow("flow1"))
    run_begin(service, "flow1")

    response = run_begin(service, "flow1")

    assert response.flag == ERROR_CODE_BY_TYPE[FlowStateError]
    assert response.flag != SUCCESS
    assert "flow1" in response.message
    assert flow.begun is True
