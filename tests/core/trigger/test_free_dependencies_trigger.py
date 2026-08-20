def test_task_free_dependencies_trigger(trigger_simple_flow):
    task2 = trigger_simple_flow.task2

    task2.add_trigger("./task1 == complete")
    assert not task2.evaluate_trigger()

    task2.free_dependencies("trigger")
    assert task2.evaluate_trigger()


def test_task_free_dependencies_all(trigger_simple_flow):
    task2 = trigger_simple_flow.task2

    task2.add_trigger("./task1 == complete")
    assert not task2.evaluate_trigger()

    task2.free_dependencies()
    assert task2.evaluate_trigger()
