"""Real server process with named checkpoint barriers for crash-window tests."""

import asyncio
import errno
import json
from pathlib import Path
import shlex
import sys
import time

from takler.core import Flow
from takler.server import TaklerServer
from takler.server.connect_config import Address, ConnectConfig, HttpSettings, Server
from takler.tasks.shell import ShellScriptTask


async def main() -> None:
    work = Path(sys.argv[1])
    grpc_port, http_port = map(int, sys.argv[2:4])
    fresh = sys.argv[4] == "fresh"
    config = ConnectConfig(
        server=Server(
            address=Address(hostname="127.0.0.1", ip="127.0.0.1", port=str(grpc_port)),
            http=HttpSettings(host="127.0.0.1", port=str(http_port)),
        )
    )
    server = TaklerServer(
        host="127.0.0.1",
        port=grpc_port,
        connect_config=config,
        checkpoint_file=work / "state.check",
        checkpoint_interval=3600,
    )
    server.scheduler.interval_main_loop = 0.05
    if fresh:
        template = work / "task.takler"
        template.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        flow = server.bunch.add_flow(Flow("flow"))
        flow.add_parameter("TAKLER_HOME", str(work / "jobs"))
        for name in ("a", "b"):
            task = ShellScriptTask(name, script_path=template)
            task.add_parameter(
                "TAKLER_SHELL_JOB_CMD",
                " ".join(
                    shlex.quote(value)
                    for value in (
                        sys.executable,
                        str(Path(__file__).with_name("fake_submitter.py")),
                        str(work / "submissions.log"),
                        name,
                    )
                ),
            )
            flow.add_task(task)
        flow.begin()
        flow.suspend()

    await server.start()
    (work / "ready").write_text("ready", encoding="utf-8")
    serving = asyncio.create_task(server.run())
    pending_checkpoint = None
    sequence = 1
    while not serving.done():
        path = work / "commands" / f"{sequence:04d}.json"
        if path.exists():
            command = json.loads(path.read_text(encoding="utf-8"))
            operation = command["operation"]
            if operation == "checkpoint":
                success = server.checkpoint_manager.write_checkpoint()
            elif operation == "checkpoint_async_block":
                entered = work / "checkpoint_entered"
                release = work / "checkpoint_release"
                entered.unlink(missing_ok=True)
                release.unlink(missing_ok=True)

                def block(index, name):
                    if name == "write_temp_file":
                        entered.write_text("entered", encoding="utf-8")
                        while not release.exists():
                            time.sleep(0.01)

                server.checkpoint_manager._after_write_step = block
                pending_checkpoint = asyncio.create_task(
                    server.checkpoint_manager.write_checkpoint_async()
                )
                success = True
            elif operation == "checkpoint_release":
                (work / "checkpoint_release").write_text("release", encoding="utf-8")
                success = await asyncio.wait_for(pending_checkpoint, 10)
                del server.checkpoint_manager._after_write_step
                pending_checkpoint = None
            elif operation in ("fail_checkpoint", "fail_checkpoint_late"):
                failing_step = (
                    "write_temp_file"
                    if operation == "fail_checkpoint"
                    else "replace_checkpoint"
                )

                def fail(index, name):
                    if name == failing_step:
                        raise OSError(errno.ENOSPC, "test storage full")

                server.checkpoint_manager._after_write_step = fail
                try:
                    success = server.checkpoint_manager.write_checkpoint()
                finally:
                    del server.checkpoint_manager._after_write_step
            else:
                raise ValueError(operation)
            ack = work / "acks" / f"{sequence:04d}.json"
            ack.write_text(
                json.dumps({"success": success, "operation": operation}),
                encoding="utf-8",
            )
            sequence += 1
        await asyncio.sleep(0.01)
    await serving


if __name__ == "__main__":
    asyncio.run(main())
