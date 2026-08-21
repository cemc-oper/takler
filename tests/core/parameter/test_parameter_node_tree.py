from takler.core import Parameter
from takler.core.task_node import (
    TASK,
    TAKLER_NAME,
    TAKLER_RID,
    TAKLER_TRY_NO,
    TAKLER_PASS,
)
from takler.core.parameter import DATE, TIME


# ------------------------
# find_user_parameter
# ------------------------


def test_find_user_parameter_with_task(flow_with_parameter):
    task1 = flow_with_parameter.task1
    assert task1.find_user_parameter("DATA_SOURCE") == Parameter("DATA_SOURCE", "local")
    assert task1.find_user_parameter("FORECAST_DAYS") is None
    assert task1.find_user_parameter("NO_EXIST") is None

    task7 = flow_with_parameter.task7
    assert task7.find_user_parameter("PARTITION") == Parameter("PARTITION", "operation")
    assert task7.find_user_parameter("FORECAST_DAYS") is None
    assert task7.find_user_parameter("NO_EXIST") is None


def test_find_user_parameter_with_flow(flow_with_parameter):
    flow1 = flow_with_parameter.flow1
    assert flow1.find_user_parameter("FORECAST_DAYS") == Parameter("FORECAST_DAYS", 3.5)
    assert flow1.find_user_parameter("PARTITION") == Parameter("PARTITION", "serial")
    assert flow1.find_user_parameter("NO_EXIST") is None


# ----------------------------
# find_generated_parameter
# ----------------------------


def test_find_generated_parameter_with_task(flow_with_parameter):
    task1 = flow_with_parameter.task1
    task1.init(task_id="1001")
    task1.update_generated_parameters()
    assert task1.find_generated_parameter(TASK) == Parameter(TASK, "task1")
    assert task1.find_generated_parameter(TAKLER_NAME) == Parameter(
        TAKLER_NAME, "/flow1/task1"
    )
    assert task1.find_generated_parameter(TAKLER_RID) == Parameter(TAKLER_RID, "1001")

    assert task1.find_generated_parameter("DATA_SOURCE") is None
    assert task1.find_generated_parameter("NO_EXIST") is None


# -----------------
# find_parameter
# -----------------


def test_find_parameter_with_container(flow_with_parameter):
    container1 = flow_with_parameter.container1
    assert container1.find_parameter("TIME_INTERVAL") == Parameter("TIME_INTERVAL", 10)

    # parent
    assert container1.find_parameter("FORECAST_DAYS") is None
    # child
    assert container1.find_parameter("AN_OPTION") is None
    # no exist
    assert container1.find_parameter("NO_EXIST") is None


def test_find_parameter_with_task(flow_with_parameter):
    task1 = flow_with_parameter.task1
    task1.init(task_id="1001")
    task1.update_generated_parameters()
    assert task1.find_parameter(TASK) == Parameter(TASK, "task1")
    assert task1.find_parameter("DATA_SOURCE") == Parameter("DATA_SOURCE", "local")

    assert task1.find_parameter("PARTITION") is None
    assert task1.find_parameter("NO_EXIST") is None


# -----------------------
# find_parent_parameter
# -----------------------


def test_find_parent_parameter_with_task(flow_with_parameter):
    task2 = flow_with_parameter.task2
    task2.init(task_id="1001")
    task2.update_generated_parameters()
    assert task2.find_parent_parameter("AN_OPTION") == Parameter("AN_OPTION", 2)
    assert task2.find_parent_parameter(TAKLER_NAME) == Parameter(
        TAKLER_NAME, "/flow1/container1/task2"
    )
    assert task2.find_parent_parameter("NO_EXIST") is None

    assert task2.find_parent_parameter("TIME_INTERVAL") == Parameter(
        "TIME_INTERVAL", 10
    )
    assert task2.find_parent_parameter("PARTITION") == Parameter("PARTITION", "serial")


def test_find_parent_parameter_with_container(flow_with_parameter):
    container1 = flow_with_parameter.container2
    assert container1.find_parent_parameter("TIME_INTERVAL") == Parameter(
        "TIME_INTERVAL", 10
    )
    assert container1.find_parent_parameter("NO_EXIST") is None

    assert container1.find_parent_parameter("PARTITION") == Parameter(
        "PARTITION", "serial"
    )
    assert container1.find_parent_parameter("AN_OPTION") is None


def test_find_parent_parameter_with_flow(flow_with_parameter):
    flow1 = flow_with_parameter.flow1

    assert flow1.find_parent_parameter("PARTITION") == Parameter("PARTITION", "serial")

    assert flow1.find_parent_parameter("TIME_INTERVAL") is None
    assert flow1.find_parent_parameter("NO_EXIST") is None


# ----------------
# parameters
# ----------------


def test_parameters_with_task(flow_with_parameter):
    task2 = flow_with_parameter.task2
    task2.init(task_id="1001")
    task2.update_generated_parameters()
    assert task2.parameters() == {
        "AN_OPTION": Parameter("AN_OPTION", 2),
        TASK: Parameter(TASK, "task2"),
        TAKLER_NAME: Parameter(TAKLER_NAME, "/flow1/container1/task2"),
        TAKLER_RID: Parameter(TAKLER_RID, "1001"),
        TAKLER_TRY_NO: Parameter(TAKLER_TRY_NO, 0),
        TAKLER_PASS: Parameter(TAKLER_PASS, None),
        "TIME_INTERVAL": Parameter("TIME_INTERVAL", 10),
        "FORECAST_DAYS": Parameter("FORECAST_DAYS", 3.5),
        "NODES": Parameter("NODES", 4),
        "DATA_PREFIX": Parameter("DATA_PREFIX", "global"),
        "FLAG_UPLOAD": Parameter("FLAG_UPLOAD", True),
        "PARTITION": Parameter("PARTITION", "serial"),
        DATE: Parameter("DATE", None),
        TIME: Parameter("TIME", None),
    }


# ------------------
# parameters_only
# ------------------


def test_parameters_only_with_task(flow_with_parameter):
    task2 = flow_with_parameter.task2
    task2.init(task_id="1001")
    task2.update_generated_parameters()
    assert task2.parameters_only() == {
        "AN_OPTION": Parameter("AN_OPTION", 2),
        TASK: Parameter(TASK, "task2"),
        TAKLER_NAME: Parameter(TAKLER_NAME, "/flow1/container1/task2"),
        TAKLER_RID: Parameter(TAKLER_RID, "1001"),
        TAKLER_TRY_NO: Parameter(TAKLER_TRY_NO, 0),
        TAKLER_PASS: Parameter(TAKLER_PASS, None),
    }


# ------------------
# user_parameters_only
# ------------------


def test_user_parameters_only_with_task(flow_with_parameter):
    task2 = flow_with_parameter.task2
    task2.init(task_id="1001")
    task2.update_generated_parameters()
    assert task2.user_parameters_only() == {
        "AN_OPTION": Parameter("AN_OPTION", 2),
    }


# -----------------------------
# generated_parameters_only
# -----------------------------


def test_generated_parameters_only_with_task(flow_with_parameter):
    task2 = flow_with_parameter.task2
    task2.init(task_id="1001")
    task2.update_generated_parameters()
    assert task2.generated_parameters_only() == {
        TASK: Parameter(TASK, "task2"),
        TAKLER_NAME: Parameter(TAKLER_NAME, "/flow1/container1/task2"),
        TAKLER_RID: Parameter(TAKLER_RID, "1001"),
        TAKLER_TRY_NO: Parameter(TAKLER_TRY_NO, 0),
        TAKLER_PASS: Parameter(TAKLER_PASS, None),
    }
