from takler.core import Parameter, Task, Flow
from takler.core.task_node import TaskNodeGeneratedParameters
from takler.core.parameter import (
    TASK,
    TAKLER_NAME,
    TAKLER_RID,
    TAKLER_TRY_NO,
    TAKLER_PASS,
)


def test_task_node_generated_parameters_create():
    task_node = Task("task1")

    gen_params = TaskNodeGeneratedParameters(node=task_node)
    assert gen_params.node == task_node
    assert gen_params.task == Parameter(TASK, None)
    assert gen_params.takler_name == Parameter(TAKLER_NAME, None)
    assert gen_params.takler_rid == Parameter(TAKLER_RID, None)
    assert gen_params.takler_try_no == Parameter(TAKLER_TRY_NO, None)
    assert gen_params.takler_pass == Parameter(TAKLER_PASS, None)


def test_task_node_generated_parameters_update_parameters():
    flow1 = Flow("flow1")
    task_node = flow1.add_task("task1")

    gen_params = task_node.generated_parameters

    assert task_node.node_path == "/flow1/task1"
    assert task_node.task_id is None
    assert task_node.try_no == 0

    gen_params.update_parameters()
    assert gen_params.task == Parameter(TASK, "task1")
    assert gen_params.takler_name == Parameter(TAKLER_NAME, "/flow1/task1")
    assert gen_params.takler_rid == Parameter(TAKLER_RID, None)
    assert gen_params.takler_try_no == Parameter(TAKLER_TRY_NO, 0)

    task_node.init(task_id="1001")
    assert task_node.task_id == "1001"
    assert task_node.try_no == 0

    gen_params.update_parameters()
    assert gen_params.task == Parameter(TASK, "task1")
    assert gen_params.takler_name == Parameter(TAKLER_NAME, "/flow1/task1")
    assert gen_params.takler_rid == Parameter(TAKLER_RID, "1001")
    assert gen_params.takler_try_no == Parameter(TAKLER_TRY_NO, 0)


def test_task_node_generated_parameters_find_parameter():
    flow1 = Flow("flow1")
    task_node = flow1.add_task("task1")
    task_node.init(task_id="1001")

    gen_params = task_node.generated_parameters
    gen_params.update_parameters()

    assert gen_params.find_parameter(TASK) == Parameter(TASK, "task1")
    assert gen_params.find_parameter(TAKLER_NAME) == Parameter(
        TAKLER_NAME, "/flow1/task1"
    )
    assert gen_params.find_parameter(TAKLER_RID) == Parameter(TAKLER_RID, "1001")
    assert gen_params.find_parameter(TAKLER_TRY_NO) == Parameter(TAKLER_TRY_NO, 0)
    assert gen_params.find_parameter(TAKLER_PASS) == Parameter(TAKLER_PASS, None)
    assert gen_params.find_parameter("NO_EXIST") is None


def test_task_node_generated_parameters_generated_parameters():
    flow1 = Flow("flow1")
    task_node = flow1.add_task("task1")
    task_node.init(task_id="1001")
    gen_params = task_node.generated_parameters
    gen_params.update_parameters()

    assert gen_params.generated_parameters() == {
        TASK: Parameter(TASK, "task1"),
        TAKLER_NAME: Parameter(TAKLER_NAME, "/flow1/task1"),
        TAKLER_RID: Parameter(TAKLER_RID, "1001"),
        TAKLER_TRY_NO: Parameter(TAKLER_TRY_NO, 0),
        TAKLER_PASS: Parameter(TAKLER_PASS, None),
    }
