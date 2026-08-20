import pytest

from takler.core.expression import Expression


def test_task_add_trigger_expression(trigger_simple_flow):
    task2 = trigger_simple_flow.task2

    task2.add_trigger(Expression("./task1 == complete"))

    assert task2.trigger_expression.expression_str == "./task1 == complete"


def test_task_add_trigger_error_type(trigger_simple_flow):
    task4 = trigger_simple_flow.task4

    with pytest.raises(TypeError):
        task4.add_trigger(111)


def test_task_add_trigger_parse(trigger_simple_flow):
    task2 = trigger_simple_flow.task2
    task3 = trigger_simple_flow.task3
    task4 = trigger_simple_flow.task4

    task2.add_trigger("./task1 == complete")
    assert task2.trigger_expression.ast is None

    task3.add_trigger("../task2 == complete", parse=True)
    assert task3.trigger_expression.ast is not None

    task4.add_trigger("../task3 == complete", parse=False)
    assert task4.trigger_expression.ast is None
