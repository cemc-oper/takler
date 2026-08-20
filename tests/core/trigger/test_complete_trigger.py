import pytest

from takler.core import NodeStatus
from takler.core.expression import Expression


def test_task_add_complete_trigger(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    task2 = trigger_simple_flow.task2

    task1.add_event("event_a")
    task2.add_trigger("./task1 == complete")
    task2.add_complete_trigger("./task1:event_a == set")

    flow1.requeue()

    assert not task2.evaluate_trigger()
    assert not task2.evaluate_complete_trigger()

    task1.set_event("event_a", True)
    assert not task2.evaluate_trigger()
    assert task2.evaluate_complete_trigger()


def test_container_add_complete_trigger(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    container1 = trigger_simple_flow.container1

    container1.add_complete_trigger("./task1 == complete")
    container1.add_trigger("./task1 == aborted")

    flow1.requeue()
    assert not container1.evaluate_complete_trigger()
    assert not container1.evaluate_trigger()

    task1.complete()
    assert container1.evaluate_complete_trigger()
    assert not container1.evaluate_trigger()


def test_task_add_complete_trigger_run(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    task2 = trigger_simple_flow.task2

    task1.add_event("event_a")
    task2.add_trigger("./task1 == complete")
    task2.add_complete_trigger("./task1:event_a == set")

    flow1.requeue()

    assert not task2.is_complete_triggered
    assert task2.state.node_status == NodeStatus.queued

    task1.set_event("event_a", True)
    flow1.resolve_dependencies()

    assert task2.is_complete_triggered
    assert task2.state.node_status == NodeStatus.complete


def test_task_add_complete_trigger_and_trigger_run(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    task2 = trigger_simple_flow.task2

    task1.add_event("event_a")
    task2.add_trigger("./task1 == complete")
    task2.add_complete_trigger("./task1:event_a == set")

    flow1.requeue()
    assert not task2.is_complete_triggered
    assert task2.state.node_status == NodeStatus.queued

    task1.set_event("event_a", True)
    task1.complete()
    flow1.resolve_dependencies()

    assert task2.is_complete_triggered
    assert task2.state.node_status == NodeStatus.complete

    # complete trigger is evaluated before normal trigger.
    flow1.requeue()
    assert not task2.is_complete_triggered
    assert task2.state.node_status == NodeStatus.queued

    task1.complete()
    flow1.resolve_dependencies()

    assert not task2.is_complete_triggered
    assert not task2.state.node_status == NodeStatus.complete


def test_task_add_complete_trigger_expression(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    task2 = trigger_simple_flow.task2

    task1.add_event("event_a")
    task2.add_trigger(Expression("./task1 == complete"))
    task2.add_complete_trigger(Expression("./task1:event_a == set"))

    flow1.requeue()

    assert not task2.evaluate_trigger()
    assert not task2.evaluate_complete_trigger()

    task1.set_event("event_a", True)
    assert not task2.evaluate_trigger()
    assert task2.evaluate_complete_trigger()


def test_task_add_complete_trigger_error_type(trigger_simple_flow):
    task2 = trigger_simple_flow.task2
    with pytest.raises(TypeError):
        task2.add_complete_trigger(111)


def test_task_add_complete_trigger_parse(trigger_simple_flow):
    task2 = trigger_simple_flow.task2
    task3 = trigger_simple_flow.task3
    task4 = trigger_simple_flow.task4

    task2.add_complete_trigger("./task1 == complete")
    assert task2.complete_trigger_expression.ast is None
    task3.add_complete_trigger("../task2 == complete", parse=True)
    assert task3.complete_trigger_expression.ast is not None
    task4.add_complete_trigger("./task3 == complete", parse=False)
    assert task4.complete_trigger_expression.ast is None
