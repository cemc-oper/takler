import sys
from pathlib import Path

from takler.core import Bunch, Flow
from takler.tasks.shell import ShellScriptTask
from takler.visitor import pre_order_travel, PrintVisitor


TAKLER_HOME = Path(__file__).parent


def create_flow():
    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", str(TAKLER_HOME))
    # A flow-level user parameter. Every task under this flow can see it,
    # unless a task defines a parameter with the same name.
    flow.add_parameter("GREETING", "hello from flow")

    group1 = flow.add_container("group1")
    # A container-level user parameter, visible to every task under group1.
    group1.add_parameter("GREETING", "hello from group1")

    task1 = flow.add_task(ShellScriptTask("t1"))
    task1.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task1.takler")))

    task2 = group1.add_task(ShellScriptTask("t2"))
    task2.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task2.takler")))
    # A task-level user parameter overriding the container's GREETING.
    task2.add_parameter("GREETING", "hello from t2")

    task3 = group1.add_task(ShellScriptTask("t3"))
    task3.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task3.takler")))

    return flow


if __name__ == "__main__":
    test_flow = create_flow()
    bunch = Bunch()
    bunch.add_flow(test_flow)
    pre_order_travel(test_flow, PrintVisitor(sys.stdout, show_parameter=True))

    task1 = test_flow.find_node("/test/t1")
    task2 = test_flow.find_node("/test/group1/t2")
    task3 = test_flow.find_node("/test/group1/t3")

    # GREETING resolves along the node tree: task's own value, then the
    # nearest ancestor (container, flow, bunch) that defines it.
    print("t1 sees GREETING:", task1.find_parent_parameter("GREETING").value)
    print("t2 sees GREETING:", task2.find_parent_parameter("GREETING").value)
    print("t3 sees GREETING:", task3.find_parent_parameter("GREETING").value)
