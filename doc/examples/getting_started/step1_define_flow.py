import sys
from pathlib import Path

from takler.core import Flow, Bunch
from takler.tasks.shell import ShellScriptTask
from takler.visitor import pre_order_travel, PrintVisitor


TAKLER_HOME = Path(__file__).parent


def create_flow():
    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", str(TAKLER_HOME))
    task1 = flow.add_task(ShellScriptTask("t1"))
    task1.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/task1.takler")))
    return flow


if __name__ == "__main__":
    test_flow = create_flow()
    bunch = Bunch()
    bunch.add_flow(test_flow)
    pre_order_travel(test_flow, PrintVisitor(sys.stdout))
