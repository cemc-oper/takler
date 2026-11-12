from takler.core import Bunch, NodeContainer


def test_node_get_flow_in_flow(simple_flow):
    """Every node under a ``Flow`` returns that flow."""
    flow1 = simple_flow.flow1

    assert flow1.get_flow() is flow1
    assert simple_flow.container1.get_flow() is flow1
    assert simple_flow.task1.get_flow() is flow1
    assert simple_flow.container2.get_flow() is flow1
    assert simple_flow.task2.get_flow() is flow1
    assert simple_flow.task3.get_flow() is flow1
    assert simple_flow.task4.get_flow() is flow1
    assert simple_flow.container3.get_flow() is flow1
    assert simple_flow.task5.get_flow() is flow1
    assert simple_flow.task6.get_flow() is flow1


def test_node_get_flow_in_bunch(simple_flow):
    """Adding the flow into a ``Bunch`` doesn't change the result."""
    flow1 = simple_flow.flow1
    bunch = Bunch()
    bunch.add_flow(flow1)

    assert flow1.get_flow() is flow1
    assert simple_flow.task1.get_flow() is flow1


def test_node_get_flow_without_flow():
    """A bare node tree whose root is not a ``Flow`` returns ``None``."""
    container1 = NodeContainer("container1")
    task1 = container1.add_task("task1")
    container2 = container1.add_container("container2")
    task2 = container2.add_task("task2")

    assert container1.get_flow() is None
    assert task1.get_flow() is None
    assert container2.get_flow() is None
    assert task2.get_flow() is None
