import sys
from pathlib import Path

from takler.core import Bunch, Flow
from takler.tasks.shell import ShellScriptTask
from takler.visitor import pre_order_travel, PrintVisitor


TAKLER_HOME = Path(__file__).parent


def create_flow():
    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", str(TAKLER_HOME))

    group1 = flow.add_container("group1")
    # The limit "work" allows at most 2 tasks under group1 to run at the same time.
    group1.add_limit("work", 2)

    task1 = group1.add_task(ShellScriptTask("t1"))
    task1.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task1.takler")))
    # Each task consumes 1 token of the limit "work" while it is running.
    task1.add_in_limit("work")

    task2 = group1.add_task(ShellScriptTask("t2"))
    task2.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task1.takler")))
    task2.add_in_limit("work")

    task3 = group1.add_task(ShellScriptTask("t3"))
    task3.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task1.takler")))
    task3.add_in_limit("work")

    task4 = group1.add_task(ShellScriptTask("t4"))
    task4.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task1.takler")))
    task4.add_in_limit("work")

    return flow


if __name__ == "__main__":
    test_flow = create_flow()
    bunch = Bunch()
    bunch.add_flow(test_flow)
    pre_order_travel(test_flow, PrintVisitor(sys.stdout))
