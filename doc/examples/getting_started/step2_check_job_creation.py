import sys
from pathlib import Path

from takler.core import Bunch, Flow
from takler.tasks.shell import ShellScriptTask, check_job_creation
from takler.visitor import pre_order_travel, PrintVisitor


TAKLER_HOME = Path(__file__).parent


def create_flow():
    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", TAKLER_HOME)
    task1 = flow.add_task(ShellScriptTask("t1"))
    task1.add_parameter("TAKLER_SCRIPT", Path(TAKLER_HOME, "test/task1.takler"))
    return flow


if __name__ == "__main__":
    flow = create_flow()
    bunch = Bunch()
    bunch.add_flow(flow)
    pre_order_travel(flow, PrintVisitor(sys.stdout))
    check_job_creation(flow)
