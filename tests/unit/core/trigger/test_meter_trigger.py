def test_task_add_trigger_meter(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    task2 = trigger_simple_flow.task2
    task3 = trigger_simple_flow.task3

    task1.add_meter("meter_a", 0, 10)
    task2.add_trigger("./task1:meter_a >= 4")
    task3.add_trigger("../task1:meter_a == 4")

    flow1.requeue()

    assert not task2.evaluate_trigger()
    assert not task3.evaluate_trigger()

    task1.set_meter("meter_a", 2)
    assert not task2.evaluate_trigger()
    assert not task3.evaluate_trigger()

    task1.set_meter("meter_a", 4)
    assert task2.evaluate_trigger()
    assert task3.evaluate_trigger()

    task1.set_meter("meter_a", 6)
    assert task2.evaluate_trigger()
    assert not task3.evaluate_trigger()

    task1.set_meter("meter_a", 8)
    assert task2.evaluate_trigger()
    assert not task3.evaluate_trigger()

    task1.set_meter("meter_a", 10)
    assert task2.evaluate_trigger()
    assert not task3.evaluate_trigger()


def test_container_add_trigger_meter(trigger_simple_flow):
    flow1 = trigger_simple_flow.flow1
    task1 = trigger_simple_flow.task1
    container1 = trigger_simple_flow.container1

    task1.add_meter("meter_a", 0, 10)
    container1.add_trigger("./task1:meter_a >= 4")

    flow1.requeue()

    assert not container1.evaluate_trigger()

    task1.set_meter("meter_a", 2)
    assert not container1.evaluate_trigger()

    task1.set_meter("meter_a", 4)
    assert container1.evaluate_trigger()

    task1.set_meter("meter_a", 6)
    assert container1.evaluate_trigger()

    task1.set_meter("meter_a", 8)
    assert container1.evaluate_trigger()

    task1.set_meter("meter_a", 10)
    assert container1.evaluate_trigger()
