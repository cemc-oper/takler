import asyncio
import sys
from pathlib import Path

from takler.core import Flow
from takler.server import TaklerServer
from takler.tasks.shell import ShellScriptTask
from takler.visitor import pre_order_travel, PrintVisitor


TAKLER_HOME = Path(__file__).parent


def create_flow():
    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", TAKLER_HOME)
    task1 = flow.add_task(ShellScriptTask("t1"))
    task1.add_parameter("TAKLER_SCRIPT", Path(TAKLER_HOME, "test/task1.takler"))
    return flow


async def run_takler(server):
    await server.start()
    await server.run()


def main():
    flow = create_flow()
    pre_order_travel(flow, PrintVisitor(sys.stdout))

    server = TaklerServer()
    server.bunch.add_flow(flow)
    asyncio.run(run_takler(server))


if __name__ == "__main__":
    main()
