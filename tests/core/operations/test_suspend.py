

def test_suspend_on_task(simple_flow_for_operation):
    task1 = simple_flow_for_operation.task1
    flow1 = simple_flow_for_operation.flow1

    flow1.requeue()
    assert task1.check_dependencies()
    assert not task1.state.suspended

    task1.suspend()
    assert task1.state.suspended
    assert not task1.check_dependencies()

    task1.resume()
    assert task1.check_dependencies()
    assert not task1.state.suspended


def test_suspend_on_container(simple_flow_for_operation):
    flow1 = simple_flow_for_operation.flow1
    container1 = simple_flow_for_operation.container1

    flow1.requeue()
    assert container1.check_dependencies()
    assert not container1.state.suspended

    container1.suspend()
    assert container1.state.suspended
    assert not container1.check_dependencies()

    container1.resume()
    assert not container1.state.suspended
    assert container1.check_dependencies()


def test_suspend_on_flow(simple_flow_for_operation):
    flow1 = simple_flow_for_operation.flow1
    flow1.requeue()
    assert flow1.check_dependencies()
    assert not flow1.state.suspended

    flow1.suspend()
    assert flow1.state.suspended
    assert not flow1.check_dependencies()

    flow1.resume()
    assert not flow1.state.suspended
    assert flow1.check_dependencies()
