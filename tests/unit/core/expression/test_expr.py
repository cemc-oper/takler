from pydantic import BaseModel, ConfigDict
import pytest


from takler.core import Flow, NodeContainer, Task, NodeStatus
from takler.core.expression import Expression
from takler.core.expression_ast import (
    AstNodePath,
    AstNodeStatus,
    AstVariablePath,
    AstInteger,
    AstOpGe,
    AstOpEq,
)


class SimpleFlow(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    flow1: Flow
    container1: NodeContainer
    task1: Task
    container2: NodeContainer
    task2: Task
    task3: Task
    task4: Task
    container3: NodeContainer
    task5: Task
    task6: Task


@pytest.fixture
def simple_flow() -> SimpleFlow:
    """
    A simple flow with event, meter and trigger:

        |- flow1 [unknown]
          |- container1 [unknown]
            |- task1 [unknown]
                 event event1 unset
                 meter meter1 0 10 0
            |- container2 [unknown]
              |- task2 [unknown]
                   event event2 unset
              |- task3 [unknown]
          |- task4 [unknown]
               meter meter2 0 10 0
          |- container3 [unknown]
            |- task5 [unknown]
          |- task6 [unknown]

    """
    with Flow("flow1") as flow1:
        with flow1.add_container("container1") as container1:
            with container1.add_task("task1") as task1:
                task1.add_meter("meter1", 0, 10)
                task1.add_event("event1")
            with container1.add_container("container2") as container2:
                # container2.add_trigger("./task1 == complete")
                with container2.add_task("task2") as task2:
                    task2.add_event("event2")
                with container2.add_task("task3") as task3:
                    pass
        with flow1.add_task("task4") as task4:
            task4.add_meter("meter2", 0, 10)
            # task4.add_trigger("./container1/container2 == complete")
        with flow1.add_container("container3") as container3:
            with container3.add_task("task5") as task5:
                # task5.add_trigger("../task4 == complete")
                pass
        with flow1.add_task("task6") as task6:
            # task6.add_trigger("./container3 == complete")
            pass
        flow1.requeue()

        f = SimpleFlow(
            flow1=flow1,
            task1=task1,
            container1=container1,
            task2=task2,
            task3=task3,
            container2=container2,
            task4=task4,
            task5=task5,
            task6=task6,
            container3=container3,
        )

    return f


def test_node_status_expr(simple_flow):
    # task3   /flow1/container1/task1 == complete
    task3 = simple_flow.task3
    task1 = simple_flow.task1
    expr_string = "/flow1/container1/task1 == complete"
    expr = Expression(expr_string)
    expr.create_ast(task3)

    expr_ast = expr.ast
    expr_ast_node_path = expr_ast.left
    expr_ast_node_status = expr_ast.right

    assert isinstance(expr_ast_node_path, AstNodePath)
    assert expr_ast_node_path.node_path == "/flow1/container1/task1"
    assert expr_ast_node_path.parent_node == task3
    assert expr_ast_node_path.get_reference_node() == task1

    assert isinstance(expr_ast_node_status, AstNodeStatus)
    assert expr_ast_node_status.node_status == NodeStatus.complete

    assert expr_ast_node_path.value() == NodeStatus.queued
    assert expr_ast_node_status.value() == NodeStatus.complete
    assert not expr_ast.evaluate()

    task1.complete()
    assert expr_ast_node_path.value() == NodeStatus.complete
    assert expr_ast_node_status.value() == NodeStatus.complete
    assert expr_ast.evaluate()


def test_event_set_expr(simple_flow):
    task1 = simple_flow.task1
    task3 = simple_flow.task3

    # task3 "/flow1/container1/task1:event == set"
    expr_string = "/flow1/container1/task1:event1 == set"
    expr = Expression(expr_string)
    expr.create_ast(task3)

    expr_ast = expr.ast
    expr_ast_variable_path = expr_ast.left
    expr_ast_int = expr_ast.right

    assert isinstance(expr_ast_variable_path, AstVariablePath)
    assert expr_ast_variable_path.node.parent_node == task3
    assert expr_ast_variable_path.variable_name == "event1"
    assert expr_ast_variable_path._node_variable == task1.events[0]

    assert isinstance(expr_ast_int, AstInteger)
    assert expr_ast_int.number == 1

    assert expr_ast_variable_path.value() == 0
    assert expr_ast_int.value() == 1
    assert not expr_ast.evaluate()

    task1.set_event("event1", True)
    assert expr_ast_variable_path.value() == 1
    assert expr_ast_int.value() == 1
    assert expr_ast.evaluate()


def test_event_unset_expr(simple_flow):
    task2 = simple_flow.task2
    task3 = simple_flow.task3

    # task3 "/flow1/container1/container2/task2:event2 == unset"
    task2.set_event("event2", True)

    expr_string = "/flow1/container1/container2/task2:event2 == unset"
    expr = Expression(expr_string)
    expr.create_ast(task3)

    expr_ast = expr.ast
    expr_ast_variable_path = expr_ast.left
    expr_ast_int = expr_ast.right

    assert isinstance(expr_ast_variable_path, AstVariablePath)
    assert expr_ast_variable_path.node.parent_node == task3
    assert expr_ast_variable_path.variable_name == "event2"
    assert expr_ast_variable_path._node_variable == task2.events[0]

    assert isinstance(expr_ast_int, AstInteger)
    assert expr_ast_int.number == 0

    assert expr_ast_variable_path.value() == 1
    assert expr_ast_int.value() == 0
    assert not expr_ast.evaluate()

    task2.set_event("event2", False)
    assert expr_ast_variable_path.value() == 0
    assert expr_ast_int.value() == 0
    assert expr_ast.evaluate()


def test_meter_gt_expr(simple_flow):
    task1 = simple_flow.task1
    task3 = simple_flow.task3

    # task3 "/flow1/container1/task1:meter1 > 5"
    expr_string = "/flow1/container1/task1:meter1 > 5"
    expr = Expression(expr_string)
    expr.create_ast(task3)

    expr_ast = expr.ast
    expr_ast_variable_path = expr_ast.left
    expr_ast_int = expr_ast.right

    assert isinstance(expr_ast_variable_path, AstVariablePath)
    assert expr_ast_variable_path.node.parent_node == task3
    assert expr_ast_variable_path.variable_name == "meter1"
    assert expr_ast_variable_path._node_variable == task1.meters[0]

    assert isinstance(expr_ast_int, AstInteger)
    assert expr_ast_int.number == 5

    assert expr_ast_variable_path.value() == 0
    assert expr_ast_int.value() == 5
    assert not expr_ast.evaluate()

    task1.set_meter("meter1", 5)
    assert expr_ast_variable_path.value() == 5
    assert expr_ast_int.value() == 5
    assert not expr_ast.evaluate()

    task1.set_meter("meter1", 10)
    assert expr_ast_variable_path.value() == 10
    assert expr_ast_int.value() == 5
    assert expr_ast.evaluate()


def test_meter_ge_expr(simple_flow):
    task1 = simple_flow.task1
    task3 = simple_flow.task3

    # task3 "/flow1/container1/task1:meter1 >= 5"
    expr_string = "/flow1/container1/task1:meter1 >= 5"
    expr = Expression(expr_string)
    expr.create_ast(task3)

    expr_ast = expr.ast
    expr_ast_variable_path = expr_ast.left
    expr_ast_int = expr_ast.right

    assert isinstance(expr_ast_variable_path, AstVariablePath)
    assert expr_ast_variable_path.node.parent_node == task3
    assert expr_ast_variable_path.variable_name == "meter1"
    assert expr_ast_variable_path._node_variable == task1.meters[0]

    assert isinstance(expr_ast_int, AstInteger)
    assert expr_ast_int.number == 5

    assert expr_ast_variable_path.value() == 0
    assert expr_ast_int.value() == 5
    assert not expr_ast.evaluate()

    task1.set_meter("meter1", 5)
    assert expr_ast_variable_path.value() == 5
    assert expr_ast_int.value() == 5
    assert expr_ast.evaluate()

    task1.set_meter("meter1", 10)
    assert expr_ast_variable_path.value() == 10
    assert expr_ast_int.value() == 5
    assert expr_ast.evaluate()


def test_or_expr(simple_flow):
    task1 = simple_flow.task1
    task3 = simple_flow.task3

    # task3 "/flow1/container1/task1:meter1 >=5 or /flow1/container1/task1==complete"
    expr_string = (
        "/flow1/container1/task1:meter1 >=5 or /flow1/container1/task1==complete"
    )
    expr = Expression(expr_string)
    expr.create_ast(task3)

    expr_ast = expr.ast
    expr_ast_meter = expr_ast.left
    expr_ast_status = expr_ast.right

    assert isinstance(expr_ast_meter, AstOpGe)
    assert isinstance(expr_ast_meter.left, AstVariablePath)
    assert expr_ast_meter.left.node.parent_node == task3
    assert expr_ast_meter.left.variable_name == "meter1"
    assert expr_ast_meter.left._node_variable == task1.meters[0]

    assert isinstance(expr_ast_meter.right, AstInteger)
    assert expr_ast_meter.right.number == 5

    assert isinstance(expr_ast_status, AstOpEq)
    assert isinstance(expr_ast_status.left, AstNodePath)
    assert expr_ast_status.left.node_path == "/flow1/container1/task1"
    assert expr_ast_status.left.parent_node == task3
    assert expr_ast_status.left.get_reference_node() == task1

    assert isinstance(expr_ast_status.right, AstNodeStatus)
    assert expr_ast_status.right.node_status == NodeStatus.complete

    # queue
    assert not expr_ast_meter.evaluate()
    assert not expr_ast_status.evaluate()
    assert not expr.evaluate()

    # meter
    task1.set_meter("meter1", 10)
    assert expr_ast_meter.evaluate()
    assert not expr_ast_status.evaluate()
    assert expr.evaluate()

    # complete
    task1.reset_meter("meter1")
    task1.complete()
    assert not expr_ast_meter.evaluate()
    assert expr_ast_status.evaluate()
    assert expr.evaluate()

    # meter and complete
    task1.set_meter("meter1", 10)
    task1.complete()
    assert expr_ast_meter.evaluate()
    assert expr_ast_status.evaluate()
    assert expr.evaluate()


def test_and_expr(simple_flow):
    task1 = simple_flow.task1
    task2 = simple_flow.task2
    task3 = simple_flow.task3

    # task3 "/flow1/container1/task1:meter1 >=5 and /flow1/container1/container2/task2==complete"
    expr_string = "/flow1/container1/task1:meter1 >=5 and /flow1/container1/container2/task2==complete"
    expr = Expression(expr_string)
    expr.create_ast(task3)

    expr_ast = expr.ast
    expr_ast_meter = expr_ast.left
    expr_ast_status = expr_ast.right

    assert isinstance(expr_ast_meter, AstOpGe)
    assert isinstance(expr_ast_meter.left, AstVariablePath)
    assert expr_ast_meter.left.node.parent_node == task3
    assert expr_ast_meter.left.variable_name == "meter1"
    assert expr_ast_meter.left._node_variable == task1.meters[0]

    assert isinstance(expr_ast_meter.right, AstInteger)
    assert expr_ast_meter.right.number == 5

    assert isinstance(expr_ast_status, AstOpEq)
    assert isinstance(expr_ast_status.left, AstNodePath)
    assert expr_ast_status.left.node_path == "/flow1/container1/container2/task2"
    assert expr_ast_status.left.parent_node == task3
    assert expr_ast_status.left.get_reference_node() == task2

    assert isinstance(expr_ast_status.right, AstNodeStatus)
    assert expr_ast_status.right.node_status == NodeStatus.complete

    # queue
    assert not expr_ast_meter.evaluate()
    assert not expr_ast_status.evaluate()
    assert not expr.evaluate()

    # meter
    task1.set_meter("meter1", 10)
    assert expr_ast_meter.evaluate()
    assert not expr_ast_status.evaluate()
    assert not expr.evaluate()

    # complete
    task1.reset_meter("meter1")
    task2.complete()
    assert not expr_ast_meter.evaluate()
    assert expr_ast_status.evaluate()
    assert not expr.evaluate()

    # meter and complete
    task1.set_meter("meter1", 10)
    task2.complete()
    assert expr_ast_meter.evaluate()
    assert expr_ast_status.evaluate()
    assert expr.evaluate()
