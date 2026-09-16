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
        "TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task1_with_event.takler"))
    )
    # t1 sets event "a" while running, once its result is ready.
    task1.add_event("a")

    task2 = flow.add_task(ShellScriptTask("t2"))
    task2.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task2.takler")))
    # If t1 has already set event "a", t2 is marked complete directly
    # without ever running its script.
    task2.add_complete_trigger("./t1:a == set")

    return flow


if __name__ == "__main__":
    test_flow = create_flow()
    bunch = Bunch()
    bunch.add_flow(test_flow)
    pre_order_travel(test_flow, PrintVisitor(sys.stdout, show_trigger=True))
