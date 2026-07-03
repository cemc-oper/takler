import os
import sys
import asyncio

from anyio import run_process


class ShellRunner:
    def spwan(self, command: str):
        """
        Run command in sub progress using ``anyio.run_process``.

        Progress will be created from current running loop.

        The command will be run as follows:

            /bin/sh -c command_string
        """
        async def run_shell_command():
            await run_process(["/bin/sh", "-c", command])
        loop = asyncio.get_running_loop()
        _ = loop.create_task(run_shell_command())

    def spwan_v2(self, command: str):
        """
        Double fork to run shell command `command` to avoid zombie process.

        The command will be run as follows:

            /bin/sh -c command_string
        """
        try:
            pid = os.fork()
            if pid > 0:
                # parent process, return and keep running
                return
        except OSError:
            pass

        os.setsid()

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:

                # loop = asyncio.get_running_loop()
                # loop.stop()

                # exit from second parent
                sys.exit(0)
        except OSError:
            pass

        # loop = asyncio.get_running_loop()
        # loop.stop()

        os.execl("/bin/sh", "sh", "-c", command)

        os._exit(127)
