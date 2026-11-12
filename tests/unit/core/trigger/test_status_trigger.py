def test_task_add_trigger_status_complete(trigger_simple_flow):
    task1 = trigger_simple_flow.task1
    task2 = trigger_simple_flow.task2

    task2.add_trigger("./task1 == complete")
    assert not task2.evaluate_trigger()

    task1.complete()
    assert task2.evaluate_trigger()


def test_container_add_trigger_status_complete(trigger_simple_flow):
    container1 = trigger_simple_flow.container1
    task9 = trigger_simple_flow.task9
    task3 = trigger_simple_flow.task3
    task4 = trigger_simple_flow.task4

    container1.add_trigger("/flow1/task9 eq complete")
    task3.add_trigger("./task4 == complete")

    assert not container1.evaluate_trigger()
    assert not task3.evaluate_trigger()
    assert task4.evaluate_trigger()

    task9.complete()
    assert container1.evaluate_trigger()
    assert not task3.evaluate_trigger()
    assert task4.evaluate_trigger()

    task4.complete()
    assert task3.evaluate_trigger()


def test_task_add_trigger_status_aborted(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    task2 = trigger_simple_flow.task2
    task2.add_trigger("./task1 == aborted")

    flow1.requeue()
    assert not task2.evaluate_trigger()

    task1.abort()
    assert task2.evaluate_trigger()

    flow1.requeue()
    task1.complete()
    assert not task2.evaluate_trigger()


def test_task_add_trigger_status_active(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    task2 = trigger_simple_flow.task2
    task2.add_trigger("./task1 == active")

    flow1.requeue()
    assert not task2.evaluate_trigger()

    task1.init("task_id_1")
    assert task2.evaluate_trigger()

    flow1.requeue()
    task1.complete()
    assert not task2.evaluate_trigger()
