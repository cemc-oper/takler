def test_task_add_trigger_status_complex(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    task2 = trigger_simple_flow.task2
    task3 = trigger_simple_flow.task3
    task4 = trigger_simple_flow.task4
    task10 = trigger_simple_flow.task10

    flow1.requeue()
    task10.add_trigger(
        "((./task1 == complete) or (./task2 == complete)) "
        "and ((./container1/task3 == complete) or (./container1/task4 == complete))"
    )
    assert not task10.evaluate_trigger()

    flow1.requeue()
    task1.complete()
    assert not task10.evaluate_trigger()

    task3.complete()
    assert task10.evaluate_trigger()

    flow1.requeue()
    task1.complete()
    task2.complete()
    assert not task10.evaluate_trigger()

    flow1.requeue()
    task3.complete()
    task4.complete()
    assert not task10.evaluate_trigger()

    flow1.requeue()
    task1.abort("trap")
    task2.complete()
    task3.complete()
    assert task10.evaluate_trigger()
