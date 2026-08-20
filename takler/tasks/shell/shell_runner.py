"""Spawn shell jobs from the running event loop.

``ShellRunner`` is the single place where takler derives a job process. It keeps
a strong reference to every spawned :class:`asyncio.Task` until that task
finishes, so the loop cannot garbage collect a running job and silently drop its
exception. Every task gets a done callback which logs the failure first and only
then triggers the caller supplied state change.
"""

from __future__ import annotations

import asyncio
from subprocess import CalledProcessError
from typing import Callable, Dict, Optional, Set, Tuple

from anyio import run_process

from takler.exceptions import JobSubmissionError
from takler.logging import get_logger


logger = get_logger("tasks.shell")


OnFailure = Callable[[BaseException], None]


class ShellRunner:
    """Run job commands in subprocesses derived from the running event loop."""

    def __init__(self):
        # Strong references to the in-flight job tasks. Without this set the
        # event loop only keeps a weak reference, and a task may be collected
        # before it completes.
        self._job_tasks: Set[asyncio.Task] = set()
        # Per task submit context, used by the done callback to build the log
        # message and to trigger the state change.
        self._job_context: Dict[asyncio.Task, Tuple[str, str, Optional[OnFailure]]] = {}

    def spwan(
            self,
            command: str,
            node_path: str = "",
            on_failure: Optional[OnFailure] = None,
    ) -> asyncio.Task:
        """
        Run command in a subprocess using ``anyio.run_process``.

        The task is created from the current running loop, and the runner keeps
        a reference to it until it finishes.

        The command will be run as follows:

            /bin/sh -c command_string

        Parameters
        ----------
        command
            The submit command, run by ``/bin/sh -c``.
        node_path
            Path of the node this job belongs to, used in failure logs.
        on_failure
            Called with the exception when the job task ends with an exception,
            after the ERROR log has been recorded.

        Returns
        -------
        asyncio.Task
            The created job task.

        Raises
        ------
        JobSubmissionError
            When the job task cannot be created, for example when there is no
            running event loop or the system refuses the call.
        """
        async def run_shell_command():
            await run_process(["/bin/sh", "-c", command])

        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(run_shell_command())
        except (RuntimeError, OSError) as exc:
            raise JobSubmissionError(
                f"spawn job failed: command={command!r}: {exc}"
            ) from exc

        self._job_tasks.add(task)
        self._job_context[task] = (command, node_path, on_failure)
        task.add_done_callback(self._on_job_done)
        return task

    def _on_job_done(
            self,
            task: asyncio.Task,
            command: Optional[str] = None,
            node_path: Optional[str] = None,
            on_failure: Optional[OnFailure] = None,
    ) -> None:
        """
        Done callback of a job task: log the failure first, then trigger the
        state change.

        The submit context is taken from the context recorded by ``spwan``;
        the keyword arguments override it when the callback is invoked directly.
        """
        context_command, context_node_path, context_on_failure = self._job_context.pop(
            task, ("", "", None)
        )
        if command is None:
            command = context_command
        if node_path is None:
            node_path = context_node_path
        if on_failure is None:
            on_failure = context_on_failure

        self._job_tasks.discard(task)

        if task.cancelled():
            return

        exc = task.exception()
        if exc is None:
            return

        if isinstance(exc, CalledProcessError):
            logger.error(
                f"job failed: node={node_path}, command={command!r}, "
                f"returncode={exc.returncode}"
            )
        else:
            logger.error(
                f"job failed: node={node_path}, command={command!r}, "
                f"{type(exc).__name__}: {exc}"
            )

        if on_failure is not None:
            on_failure(exc)
