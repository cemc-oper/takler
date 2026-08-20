import pytest

from takler.core import Parameter, Task
from takler.core.parameter import TASK, TAKLER_NAME, TAKLER_RID, TAKLER_TRY_NO


def test_task_add_parameter_single():
    task1 = Task("task1")

    int_param = task1.add_parameter("int_param", 1)
    assert int_param is not None
    assert int_param.value == 1
    assert task1.user_parameters["int_param"] == int_param
    float_param = task1.add_parameter("float_param", -1.5)
    assert float_param is not None
    assert float_param.value == -1.5
    assert task1.user_parameters["float_param"] == float_param
    string_param = task1.add_parameter("string_param", "this is a param")
    assert string_param is not None
    assert string_param.value == "this is a param"
    assert task1.user_parameters["string_param"] == string_param
    bool_param = task1.add_parameter("bool_param", True)
    assert bool_param is not None
    assert bool_param.value
    assert task1.user_parameters["bool_param"] == bool_param


def test_task_add_parameter_error_type_value():
    task1 = Task("task1")
    with pytest.raises(TypeError):
        task1.add_parameter("int_param", [1, 2, 3])

    with pytest.raises(TypeError):
        task1.add_parameter("int_param", {"a": 1, "b": 2})

    with pytest.raises(TypeError):
        task1.add_parameter("int_param", None)


def test_task_add_parameter_parameter():
    task1 = Task("task1")
    int_param = task1.add_parameter(Parameter("int_param", 1))
    assert int_param is not None
    assert int_param.value == 1
    assert task1.user_parameters["int_param"] == int_param


def test_task_add_parameter_parameter_with_value():
    task1 = Task("task1")
    with pytest.raises(TypeError):
        task1.add_parameter(Parameter("int_param", 1), value=100)


def test_task_add_parameters_list():
    task1 = Task("task1")

    task1.add_parameter(
        [
            Parameter("int_param", 1),
            Parameter("float_param", -1.5),
            Parameter("string_param", "this is a param"),
            Parameter("bool_param", True),
        ]
    )

    assert len(task1.user_parameters) == 4


def test_task_add_parameter_list_with_value():
    task1 = Task("task1")
    with pytest.raises(TypeError):
        task1.add_parameter(
            [
                Parameter("int_param", 1),
                Parameter("float_param", -1.5),
                Parameter("string_param", "this is a param"),
                Parameter("bool_param", True),
            ],
            value="some value",
        )


def test_task_add_parameter_list_with_other_type():
    task1 = Task("task1")
    with pytest.raises(TypeError):
        task1.add_parameter(
            [
                Parameter("int_param", 1),
                ("float_param", -1.5),
            ]
        )


def test_task_add_parameter_dict():
    task1 = Task("task1")
    task1.add_parameter(
        {
            "int_param": 1,
            "float_param": -1.5,
            "string_param": "this is a param",
            "bool_param": True,
        }
    )
    assert len(task1.user_parameters) == 4


def test_task_add_parameter_dict_with_value():
    task1 = Task("task1")
    with pytest.raises(TypeError):
        task1.add_parameter(
            {
                "int_param": 1,
                "float_param": -1.5,
                "string_param": "this is a param",
                "bool_param": True,
            },
            value="some value",
        )


def test_task_add_parameter_error_type_param():
    task1 = Task("task1")
    with pytest.raises(TypeError):
        task1.add_parameter(("param1", 1))

    with pytest.raises(TypeError):
        task1.add_parameter(1, 1)

    with pytest.raises(TypeError):
        task1.add_parameter(None, 1)


# -----------------------------
# Task generated parameters
# -----------------------------


def test_task_update_and_find_generated_parameter(flow_with_parameter):
    task1 = flow_with_parameter.task1
    task1.init(task_id="1001")
    task1.update_generated_parameters()

    assert task1.find_generated_parameter(TASK) == Parameter(TASK, "task1")
    assert task1.find_generated_parameter(TAKLER_NAME) == Parameter(
        TAKLER_NAME, "/flow1/task1"
    )
    assert task1.find_generated_parameter(TAKLER_RID) == Parameter(TAKLER_RID, "1001")

    assert task1.find_generated_parameter("NO_EXIST") is None


def test_task_generated_parameters_only(flow_with_parameter):
    """
    generated_parameters_only() return a dict including reference of Parameters.
    """
    task1 = flow_with_parameter.task1

    gen_params = task1.generated_parameters_only()
    assert gen_params == {
        TASK: Parameter(TASK, None),
        TAKLER_NAME: Parameter(TAKLER_NAME, None),
        TAKLER_RID: Parameter(TAKLER_RID, None),
        TAKLER_TRY_NO: Parameter(TAKLER_TRY_NO, None),
    }

    task1.update_generated_parameters()
    assert gen_params == {
        TASK: Parameter(TASK, "task1"),
        TAKLER_NAME: Parameter(TAKLER_NAME, "/flow1/task1"),
        TAKLER_RID: Parameter(TAKLER_RID, None),
        TAKLER_TRY_NO: Parameter(TAKLER_TRY_NO, 0),
    }

    task1.init(task_id="1001")
    task1.update_generated_parameters()
    assert gen_params == {
        TASK: Parameter(TASK, "task1"),
        TAKLER_NAME: Parameter(TAKLER_NAME, "/flow1/task1"),
        TAKLER_RID: Parameter(TAKLER_RID, "1001"),
        TAKLER_TRY_NO: Parameter(TAKLER_TRY_NO, 0),
    }
