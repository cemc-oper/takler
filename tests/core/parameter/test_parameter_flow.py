import datetime

import pytest

from takler.core import Flow, Parameter
from takler.core.flow import FlowGeneratedParameters
from takler.core.parameter import DATE, TIME


TEST_TIME = datetime.datetime(2025, 5, 19, 13, 30, 40)


@pytest.fixture
def patch_datetime_now(monkeypatch):
    """
    set ``datetime.datetime.now`` to a fixed time, 2022-09-12 10:00:01
    """

    class TestDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return TEST_TIME

    monkeypatch.setattr(datetime, "datetime", TestDateTime)


# -----------------------------
# FlowGeneratedParameters
# -----------------------------


def test_flow_generated_parameters_create():
    flow1 = Flow("flow1")
    gen_params = FlowGeneratedParameters(flow=flow1)
    assert gen_params.flow == flow1
    assert gen_params.date == Parameter(DATE, None)
    assert gen_params.time == Parameter(TIME, None)


def test_flow_generated_parameters_update_parameters(
    flow_with_parameter, patch_datetime_now
):
    flow1 = flow_with_parameter.flow1
    flow1.requeue_calendar()
    gen_params = FlowGeneratedParameters(flow=flow1)

    gen_params.update_parameters()
    assert gen_params.date == Parameter(DATE, "2025-05-19")
    assert gen_params.time == Parameter(TIME, "13:30")


def test_flow_generated_parameters_find_parameter(
    flow_with_parameter, patch_datetime_now
):
    flow1 = flow_with_parameter.flow1
    flow1.requeue_calendar()
    gen_params = FlowGeneratedParameters(flow=flow1)
    gen_params.update_parameters()

    assert gen_params.find_parameter(DATE) == Parameter(DATE, "2025-05-19")
    assert gen_params.find_parameter(TIME) == Parameter(TIME, "13:30")
    assert gen_params.find_parameter("NON_EXIST") is None


def test_flow_generated_parameters_generated_parameters(
    flow_with_parameter, patch_datetime_now
):
    flow1 = flow_with_parameter.flow1
    flow1.requeue_calendar()
    gen_params = FlowGeneratedParameters(flow=flow1)
    gen_params.update_parameters()

    assert gen_params.generated_parameters() == {
        DATE: Parameter(DATE, "2025-05-19"),
        TIME: Parameter(TIME, "13:30"),
    }


# --------------------
# Flow
# --------------------


def test_flow_update_generated_parameters(flow_with_parameter, patch_datetime_now):
    flow1 = flow_with_parameter.flow1
    flow1.requeue_calendar()
    flow1.update_generated_parameters()
    gen_params = flow1.generated_parameters

    assert gen_params.date == Parameter(DATE, "2025-05-19")
    assert gen_params.time == Parameter(TIME, "13:30")


def test_flow_generated_parameters_only(flow_with_parameter, patch_datetime_now):
    flow1 = flow_with_parameter.flow1
    flow1.requeue_calendar()
    flow1.update_generated_parameters()
    gen_params = flow1.generated_parameters_only()

    assert gen_params == {
        DATE: Parameter(DATE, "2025-05-19"),
        TIME: Parameter(TIME, "13:30"),
    }
