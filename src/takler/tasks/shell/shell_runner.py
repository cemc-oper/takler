"""Spawn shell jobs from the running event loop.

``ShellRunner`` is the single place where takler derives a job process. It keeps
a strong reference to every spawned :class:`asyncio.Task` until that task
finishes, so the loop cannot garbage collect a running job and silently drop its
exception. Every task gets a done callback which logs the failure first and only
then triggers the caller supplied state change.
"""

from __future__ import annotations

import asyncio
import os
import signal
import warnings
from subprocess import CalledProcessError, DEVNULL
from typing import Callable, Dict, Optional, Set, Tuple

from takler.exceptions import JobSubmissionError
from takler.logging import get_logger


logger = get_logger("tasks.shell")


OnFailure = Callable[[BaseException], None]
OnSkipped = Callable[[], None]

# These limits cover all ShellRunner instances on one event loop. A submitted
# command owns one pending slot until its callback has run, including time spent
# waiting for a process slot. Rejecting a new slot is a definite non-start.
MAX_PENDING_COMMANDS = 128
MAX_RUNNING_COMMANDS = 16
COMMAND_TIMEOUT_SECONDS = 60.0
QUEUE_WAIT_TIMEOUT_SECONDS = 60.0


class _CommandBudget:
    def __init__(self):
        self.pending = 0
        self.processes = asyncio.Semaphore(MAX_RUNNING_COMMANDS)
        self.waiting: Dict[asyncio.Task, Optional[OnSkipped]] = {}


def skip_pending_commands() -> None:
    """Discard definite non-start work when the service halts or stops."""
    try:
        budget = getattr(asyncio.get_running_loop(), "_takler_command_budget", None)
    except RuntimeError:
        return
    if budget is None:
        return
    for task, on_skipped in list(budget.waiting.items()):
        budget.waiting.pop(task, None)
        try:
            if on_skipped is not None:
                on_skipped()
        except Exception:  # noqa: BLE001 - still cancel every pending start
            logger.error("pending submit skip callback failed", exc_info=True)
        finally:
            task.cancel()


async def run_process(command, *, stdout=DEVNULL, stderr=DEVNULL):
    """Run one submit command and reap its process group on cancellation."""
    process = await asyncio.create_subprocess_exec(
        *command, stdout=stdout, stderr=stderr, start_new_session=True
    )
    try:
        returncode = await process.wait()
    except BaseException:
        # A timeout only says the reply is unknown. Kill the local wrapper and
        # descendants so timed-out submissions cannot consume process slots.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.wait()
        raise
    if returncode:
        raise CalledProcessError(returncode, command)


def redact_job_password(message: str, password: Optional[str]) -> str:
    """Remove this submission's exact credential from diagnostics."""
    return message.replace(password, "<redacted>") if password else message


class ShellRunner:
    """Run job commands in subprocesses derived from the running event loop."""

    def __init__(self, job_password: Optional[str] = None):
        self._job_password = job_password
        # Strong references to the in-flight job tasks. Without this set the
        # event loop only keeps a weak reference, and a task may be collected
        # before it completes.
        self._job_tasks: Set[asyncio.Task] = set()
        # Per task submit context, used by the done callback to build the log
        # message and to trigger the state change.
        self._job_context: Dict[asyncio.Task, Tuple[str, str, Optional[OnFailure]]] = {}
        self._job_budgets: Dict[asyncio.Task, _CommandBudget] = {}

    def spawn(
        self,
        command: str,
        node_path: str = "",
        on_failure: Optional[OnFailure] = None,
        before_start: Optional[Callable[[], bool | None]] = None,
        on_skipped: Optional[OnSkipped] = None,
    ) -> asyncio.Task:
        """
        Run a bounded submit command in its own subprocess session.

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
        before_start
            Final submission check immediately before starting the process.
            Returning ``False`` skips a stale, not-yet-started command without
            treating it as an external submission failure.
        on_skipped
            Called when ``before_start`` returns ``False`` or a service
            halt/stop discards a command still waiting for a process slot.
            The caller must recheck identity before changing task state.

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

        budget = None
        reserved = False
        try:
            loop = asyncio.get_running_loop()
            budget = getattr(loop, "_takler_command_budget", None)
            if budget is None:
                budget = _CommandBudget()
                loop._takler_command_budget = budget
            if budget.pending >= MAX_PENDING_COMMANDS:
                raise JobSubmissionError(
                    f"submit command capacity exhausted ({MAX_PENDING_COMMANDS} pending)"
                )
            budget.pending += 1
            reserved = True

            async def run_shell_command():
                if before_start is not None and before_start() is False:
                    if on_skipped is not None:
                        on_skipped()
                    return
                current_task = asyncio.current_task()
                budget.waiting[current_task] = on_skipped
                try:
                    # asyncio.timeout does not create an inner acquire task;
                    # cancellation cannot strand a semaphore token between
                    # that task finishing and this task resuming.
                    async with asyncio.timeout(QUEUE_WAIT_TIMEOUT_SECONDS):
                        await budget.processes.acquire()
                except TimeoutError as exc:
                    raise JobSubmissionError(
                        f"submit command queue wait exceeded {QUEUE_WAIT_TIMEOUT_SECONDS} seconds"
                    ) from exc
                finally:
                    budget.waiting.pop(current_task, None)
                try:
                    if before_start is not None and before_start() is False:
                        if on_skipped is not None:
                            on_skipped()
                        return
                    # Submit commands are control operations. Their stdout and
                    # stderr are not an artifact; never buffer an arbitrary
                    # amount of child output in the server process.
                    await asyncio.wait_for(
                        run_process(
                            ["/bin/sh", "-c", command],
                            stdout=DEVNULL,
                            stderr=DEVNULL,
                        ),
                        timeout=COMMAND_TIMEOUT_SECONDS,
                    )
                finally:
                    budget.processes.release()

            task = loop.create_task(run_shell_command())
        except (RuntimeError, OSError) as exc:
            if reserved:
                budget.pending -= 1
            raise JobSubmissionError(
                redact_job_password(
                    f"spawn job failed: command={command!r}: {exc}", self._job_password
                )
            ) from exc
        except BaseException:
            # create_task may reject after the pending slot was reserved.
            if reserved:
                budget.pending -= 1
            raise

        self._job_tasks.add(task)
        self._job_budgets[task] = budget
        self._job_context[task] = (command, node_path, on_failure)
        task.add_done_callback(self._on_job_done)
        return task

    def spwan(
        self,
        command: str,
        node_path: str = "",
        on_failure: Optional[OnFailure] = None,
    ) -> asyncio.Task:
        """Deprecated misspelling of :meth:`spawn`, kept as an alias.

        Emits a :class:`DeprecationWarning` and delegates to :meth:`spawn`
        unchanged. The alias exists so existing flows and tooling calling the
        old name keep working while they migrate; it will be removed in a
        future release.
        """
        warnings.warn(
            "ShellRunner.spwan is deprecated, use ShellRunner.spawn instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.spawn(command, node_path=node_path, on_failure=on_failure)

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

        The submit context is taken from the context recorded by ``spawn``;
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
        budget = self._job_budgets.pop(task, None)
        if budget is not None:
            budget.pending -= 1

        if task.cancelled():
            return

        exc = task.exception()
        if exc is None:
            return

        if isinstance(exc, CalledProcessError):
            message = (
                f"job failed: node={node_path}, command={command!r}, "
                f"returncode={exc.returncode}"
            )
        else:
            message = (
                f"job failed: node={node_path}, command={command!r}, "
                f"{type(exc).__name__}: {exc}"
            )
        logger.error(redact_job_password(message, self._job_password))

        if on_failure is not None:
            on_failure(exc)
