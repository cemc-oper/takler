import sys
from pathlib import Path

from takler.core import Bunch, Flow
from takler.tasks.shell import ShellScriptTask
from takler.visitor import pre_order_travel, PrintVisitor


TAKLER_HOME = Path(__file__).parent


def create_flow():
    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", str(TAKLER_HOME))

    task1 = flow.add_task(ShellScriptTask("t1"))
    task1.add_parameter(
        "TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task1_with_events.takler"))
    )
    # t1 reports its progress while running: an event "a" and a meter "step".
    task1.add_event("a")
    task1.add_meter("step", 0, 100)

    task2 = flow.add_task(ShellScriptTask("t2"))
    task2.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task2.takler")))
    # t2 runs as soon as t1 sets event "a", without waiting for t1 to complete.
    task2.add_trigger("./t1:a == set")

    task3 = flow.add_task(ShellScriptTask("t3"))
    task3.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task3.takler")))
    # t3 runs once t1's meter "step" reaches 50.
    task3.add_trigger("./t1:step >= 50")

    return flow


if __name__ == "__main__":
    test_flow = create_flow()
    bunch = Bunch()
    bunch.add_flow(test_flow)
    pre_order_travel(test_flow, PrintVisitor(sys.stdout, show_trigger=True))
