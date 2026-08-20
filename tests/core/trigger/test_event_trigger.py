def test_task_add_trigger_event(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    task2 = trigger_simple_flow.task2
    task3 = trigger_simple_flow.task3

    task1.add_event("event_a")
    task1.add_event("event_b")

    task2.add_trigger("./task1:event_a == set")
    task3.add_trigger("../task1:event_b == set")

    flow1.requeue()
    assert not task2.evaluate_trigger()
    assert not task3.evaluate_trigger()

    task1.set_event("event_a", True)
    assert task2.evaluate_trigger()
    assert not task3.evaluate_trigger()

    task1.set_event("event_b", True)
    assert task2.evaluate_trigger()
    assert task3.evaluate_trigger()


def test_container_add_trigger_event(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    container1 = trigger_simple_flow.container1

    task1.add_event("event_a")
    task1.add_event("event_b")

    container1.add_trigger("./task1:event_a == set")

    flow1.requeue()
    assert not container1.evaluate_trigger()

    task1.set_event("event_a", True)
    assert container1.evaluate_trigger()
