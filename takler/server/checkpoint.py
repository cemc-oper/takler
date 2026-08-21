"""Checkpoint snapshot writing and restoring for the Takler server.

The :class:`CheckpointManager` owns everything about the Checkpoint_File: where
it lives, how often it is written, how a snapshot is built from the in-memory
:class:`~takler.core.bunch.Bunch`, and how the server recovers from it at
startup.

It deliberately depends only on ``takler.core`` and
:mod:`takler.server.connect_config`, never on ``Scheduler`` or the gRPC
``TaklerService``, so snapshot behaviour can be tested without standing up a
server.

This module holds the configuration layer (constants, path and interval
resolution), the snapshot write path (payload building plus the atomic
temporary-file dance), the periodic snapshot task and the startup restore with
its Checkpoint_File -> Checkpoint_Backup_File -> empty bunch fallback chain.

Requirements: 5.x, 6.x, 7.2, 7.3, 7.4, 7.5, 7.6, 12.5.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import shutil
import time
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Tuple, Union

import takler
from takler.core.bunch import Bunch
from takler.core.flow import Flow
from takler.core.node import Node
from takler.core.state import NodeStatus
from takler.core.task_node import Task
from takler.core.util import SerializationType
from takler.logging import get_logger
from takler.server.connect_config import ConnectConfig

__all__ = [
    "CHECKPOINT_FORMAT_VERSION",
    "EARLIEST_SUPPORTED_FORMAT_VERSION",
    "DEFAULT_CHECKPOINT_INTERVAL",
    "MIN_CHECKPOINT_INTERVAL",
    "DEFAULT_CHECKPOINT_FILE",
    "BACKUP_SUFFIX",
    "TEMP_SUFFIX",
    "CHECKPOINT_FILE_MODE",
    "JOB_PASSWORDS_KEY",
    "UNKNOWN_VERSION",
    "CheckpointManager",
]

logger = get_logger("server.checkpoint")


#: Format version stamped into every snapshot this implementation writes
#: (requirement 6.14).
CHECKPOINT_FORMAT_VERSION: int = 1

#: Oldest format version this implementation can read. A snapshot without a
#: ``format_version`` key is treated as this version (requirement 6.15).
EARLIEST_SUPPORTED_FORMAT_VERSION: int = 1

#: Snapshot period used when nothing is configured, in seconds
#: (requirement 7.2). Also the fallback for a rejected period
#: (requirement 7.6).
DEFAULT_CHECKPOINT_INTERVAL: float = 120.0

#: Smallest accepted snapshot period, in seconds (requirement 7.6). Shorter
#: periods would spend more time serializing the bunch than running flows.
MIN_CHECKPOINT_INTERVAL: float = 10.0

#: Checkpoint_File name used when no path is configured, relative to the
#: current working directory (requirement 7.3).
DEFAULT_CHECKPOINT_FILE: str = "takler.check"

#: Suffix appended to the Checkpoint_File path to derive the
#: Checkpoint_Backup_File path (requirement 7.4).
BACKUP_SUFFIX: str = ".b"

#: Suffix template of the temporary files a snapshot is written through. The pid
#: keeps two server processes that share a Checkpoint_File path from writing
#: into the same temporary file, and keeps a stale temporary file recognizable.
TEMP_SUFFIX: str = ".tmp"

#: Permissions of the Checkpoint_File, the Checkpoint_Backup_File and the
#: temporary files they are written through: owner read/write only
#: (requirement 12.5). A snapshot carries the Job_Passwords of every in-flight
#: job, so it must not be readable by other users on a shared login node.
CHECKPOINT_FILE_MODE: int = 0o600

#: Top level key of the snapshot's "node path -> Job_Password" mapping, a
#: sibling of ``bunch`` rather than a node field (requirement 5.1). ``show``
#: and the snapshot share one :meth:`Bunch.to_dict`, so anything put inside the
#: node tree would also be handed to every caller of ``show``.
JOB_PASSWORDS_KEY: str = "job_passwords"

#: The only node statuses whose Job_Password is worth persisting
#: (requirements 5.2, 5.3). A non-empty password is equivalent to
#: ``try_no > 0``, so persisting every non-empty one would also carry the
#: passwords of complete and aborted tasks -- passwords that can never be
#: accepted, since a Child_Command against a task in those statuses hits
#: Zombie_Condition ``Z2`` whether or not it matches. Narrowing the scope keeps
#: the payload small and the number of live secrets on disk down.
_PERSISTED_STATUSES: Tuple[NodeStatus, ...] = (NodeStatus.submitted, NodeStatus.active)

#: ``takler_version`` value used when the running package exposes no version,
#: which happens when takler is imported from a source tree without being
#: installed. The field is diagnostic only, so a placeholder is better than
#: failing the snapshot.
UNKNOWN_VERSION: str = "unknown"


def _is_blank(value: Optional[str]) -> bool:
    """Return ``True`` when a configured path counts as "not provided".

    Empty and whitespace-only strings are treated as absent, so a
    ``connect.yaml`` holding ``file: ""`` falls through to the next precedence
    source instead of resolving to a nameless path. This mirrors the same
    helper in :mod:`takler.server.connect_config`.
    """
    return value is None or value.strip() == ""


def _takler_version() -> str:
    """Return the running takler version for the snapshot's diagnostic field.

    ``takler/__init__.py`` derives ``__version__`` from the installed package
    metadata and leaves the attribute unset when the package is not installed,
    so the attribute is read defensively rather than imported directly.
    """
    version = getattr(takler, "__version__", None)
    if version is None:
        return UNKNOWN_VERSION
    return str(version)


def _count_nodes(node: Node) -> int:
    """Return the number of nodes in the tree rooted at ``node``.

    The root counts as one node, so a flow holding a single task counts as two.
    Used for the "restored N flows / M nodes" INFO of requirement 6.10, where the
    point is to give the operator a number they can compare against what they
    expect to be loaded, not to match any particular definition of "node".
    """
    return 1 + sum(_count_nodes(child) for child in node.children)


def _resolve_interval(
    explicit: Optional[float] = None,
    connect_config: Optional[ConnectConfig] = None,
) -> float:
    """Resolve the effective snapshot period in seconds.

    Applies the per-source precedence ``explicit argument > Connect_Config
    file > built-in default`` (requirements 7.2, 7.5): a missing explicit
    argument (``None``) lets the configuration file value take effect, and a
    missing file value falls back to :data:`DEFAULT_CHECKPOINT_INTERVAL`.

    Whichever source wins is then validated: a value that is not positive, or
    positive but below :data:`MIN_CHECKPOINT_INTERVAL`, is rejected with a
    single WARNING naming the offending value and replaced by
    :data:`DEFAULT_CHECKPOINT_INTERVAL` (requirement 7.6). Validation happens
    once on the winning value only, so a rejected file value cannot produce a
    warning when an explicit argument overrides it anyway.

    This function is pure apart from the WARNING it may log, which keeps it
    directly unit testable without constructing a manager.

    Args:
        explicit: Period passed as a constructor argument, in seconds.
            ``None`` means "not provided".
        connect_config: Loaded configuration file, or ``None`` when the server
            runs without one. Only ``checkpoint.interval`` is consulted.

    Returns:
        The snapshot period in seconds, always positive and never below
        :data:`MIN_CHECKPOINT_INTERVAL`.
    """
    value = explicit
    if value is None and connect_config is not None:
        value = connect_config.checkpoint.interval

    if value is None:
        return DEFAULT_CHECKPOINT_INTERVAL

    # ``not (value > 0)`` rather than ``value <= 0`` so that a NaN period is
    # rejected as well instead of slipping through every comparison.
    if not (value > 0) or value < MIN_CHECKPOINT_INTERVAL:
        logger.warning(
            f"invalid checkpoint interval {value!r}; it must be at least "
            f"{MIN_CHECKPOINT_INTERVAL:g} seconds, "
            f"falling back to {DEFAULT_CHECKPOINT_INTERVAL:g} seconds."
        )
        return DEFAULT_CHECKPOINT_INTERVAL

    return float(value)


def _resolve_path(
    explicit: Optional[Union[str, Path]] = None,
    connect_config: Optional[ConnectConfig] = None,
) -> Path:
    """Resolve the effective Checkpoint_File path.

    Applies the same precedence as :func:`_resolve_interval` -- explicit
    argument > ``checkpoint.file`` in the Connect_Config file > built-in
    default (requirements 7.3, 7.5). The default
    :data:`DEFAULT_CHECKPOINT_FILE` is returned as a relative path, so it
    resolves against the current working directory at write time.

    The path is not made absolute: keeping it exactly as configured makes the
    Checkpoint_Backup_File path a literal ``+ ".b"`` of it and keeps log
    messages recognizable against what the operator wrote down.

    Args:
        explicit: Path passed as a constructor argument. ``None`` means "not
            provided".
        connect_config: Loaded configuration file, or ``None``. Only
            ``checkpoint.file`` is consulted; a blank value counts as absent.

    Returns:
        The Checkpoint_File path.
    """
    if explicit is not None and not (isinstance(explicit, str) and _is_blank(explicit)):
        return Path(explicit)

    if connect_config is not None:
        configured = connect_config.checkpoint.file
        if not _is_blank(configured):
            return Path(configured)

    return Path(DEFAULT_CHECKPOINT_FILE)


class CheckpointManager:
    """Owns the Checkpoint_File of one server process.

    The manager keeps a reference to the live :class:`~takler.core.bunch.Bunch`
    instead of a copy of its state, and it never replaces that object:
    ``Scheduler`` and ``TaklerService`` already hold the same reference, so
    swapping it out would leave them pointing at a stale bunch.

    Attributes:
        bunch: The live bunch that snapshots are built from and restored into.
        interval: The resolved snapshot period in seconds.
    """

    def __init__(
        self,
        bunch: Bunch,
        checkpoint_file: Optional[Union[str, Path]] = None,
        interval: Optional[float] = None,
        connect_config: Optional[ConnectConfig] = None,
    ):
        """Resolve the snapshot configuration for ``bunch``.

        Configuration is resolved once, here, so that every later snapshot and
        every log message refers to the same path and period. Both resolvers
        follow the precedence ``explicit argument > connect_config > built-in
        default`` (requirement 7.5).

        Args:
            bunch: The bunch to snapshot and to restore into.
            checkpoint_file: Explicit Checkpoint_File path, highest precedence.
            interval: Explicit snapshot period in seconds, highest precedence.
            connect_config: Loaded ``connect.yaml``, consulted when an explicit
                argument is not provided.
        """
        self.bunch: Bunch = bunch
        self._checkpoint_file: Path = _resolve_path(checkpoint_file, connect_config)
        self.interval: float = _resolve_interval(interval, connect_config)

        # Set once the periodic snapshot task is running, and cancelled on
        # stop; ``None`` means "no periodic task in flight".
        self._snapshot_task: Optional[asyncio.Task] = None

        # The snapshot write the periodic task is currently awaiting, kept so
        # that ``stop`` can drain it instead of racing it.
        self._write_task: Optional[asyncio.Future] = None

    # Paths -----------------------------------------------

    @property
    def checkpoint_file(self) -> Path:
        """The Checkpoint_File path (requirements 7.3, 7.5)."""
        return self._checkpoint_file

    @property
    def backup_file(self) -> Path:
        """The Checkpoint_Backup_File path: Checkpoint_File plus ``.b``.

        Derived rather than configured, so the pair can never drift apart
        (requirement 7.4).
        """
        return Path(f"{self._checkpoint_file}{BACKUP_SUFFIX}")

    # Snapshot writing ------------------------------------

    def build_payload(self) -> str:
        """Build one snapshot and serialize it to a JSON string.

        This must run on the event loop thread. takler is a single event loop,
        lock-free model, so building the dictionary there is what guarantees the
        snapshot cannot observe a node tree that an RPC handler is halfway
        through mutating. Serializing here too (rather than handing the
        dictionary to the worker thread) means the value the thread writes can
        no longer change underneath it.

        Layout (requirements 5.11, 6.14): ``format_version`` /
        ``takler_version`` / ``written_at`` at the top level plus a ``bunch``
        subtree that is exactly :meth:`Bunch.to_dict`, so no second snapshot
        format is introduced. ``takler_version`` and ``written_at`` are
        diagnostic only and take no part in restoring.

        The :data:`JOB_PASSWORDS_KEY` mapping is a sibling of ``bunch``
        (requirement 5.1) and is collected here, at the same instant as
        ``Bunch.to_dict``, so the passwords and the node statuses they were
        selected by are one consistent view.

        ``format_version`` stays at 1 even though a top level key is added
        (requirement 5.7): loading only validates ``format_version`` and
        ``bunch`` and ignores unknown top level keys, so the new key is
        compatible in both directions and needs no migration.

        Returns:
            The snapshot as a JSON string.

        Raises:
            Exception: Anything raised while serializing the bunch. Callers
                (:meth:`write_checkpoint`, :meth:`write_checkpoint_async`) turn
                that into an ERROR log and a ``False`` result.
        """
        snapshot = {
            "format_version": CHECKPOINT_FORMAT_VERSION,
            "takler_version": _takler_version(),
            "written_at": datetime.datetime.now().isoformat(),
            "bunch": self.bunch.to_dict(),
            JOB_PASSWORDS_KEY: self._collect_job_passwords(),
        }
        return json.dumps(snapshot)

    def _collect_job_passwords(self) -> Dict[str, str]:
        """Collect the Job_Passwords of the in-flight tasks, keyed by node path.

        Only tasks whose password is non-empty *and* whose status is submitted
        or active are collected (requirements 5.2, 5.3); see
        :data:`_PERSISTED_STATUSES` for why the other statuses are dropped
        rather than carried along.

        The path of each node is built by passing the already joined parent
        prefix down the recursion, and ``node.node_path`` is deliberately **not
        used** (requirement 5.11): it is a property that walks up the parent
        chain and rebuilds the string on every evaluation, which measured 13~16x
        slower over a whole tree. That matters here more than on the restore
        side, because this runs on the event loop thread on every snapshot
        period, where it is 13.8ms rather than 182.5ms of blocking at 50k tasks.

        Returns:
            A ``{node path: Job_Password}`` mapping, empty when nothing is in
            flight. Node paths are absolute and formatted like
            :attr:`Node.node_path`, e.g. ``/flow1/family1/task1``.
        """
        result: Dict[str, str] = {}

        def walk(node: Node, prefix: str) -> None:
            path = f"{prefix}/{node.name}"
            if (
                isinstance(node, Task)
                and node.job_password
                and node.state.node_status in _PERSISTED_STATUSES
            ):
                result[path] = node.job_password
            for child in node.children:
                walk(child, path)

        for flow in self.bunch.flows.values():
            walk(flow, "")

        return result

    def write_checkpoint(self) -> bool:
        """Write one snapshot synchronously.

        Used by the shutdown path and by tests, where there is no benefit in
        moving the IO off the calling thread. Never raises: a failure is an
        ERROR log plus a ``False`` result, because a server that cannot write
        its snapshot must keep running (requirement 5.8).

        Returns:
            ``True`` when the Checkpoint_File now holds the new snapshot.
        """
        try:
            payload = self.build_payload()
        except Exception as exc:
            logger.error(
                f"failed to build checkpoint snapshot for "
                f"{self.checkpoint_file}: {exc!r}"
            )
            return False
        return self._write_payload(payload)

    async def write_checkpoint_async(self) -> bool:
        """Write one snapshot without blocking the event loop.

        The payload is built on the loop thread (consistent view) and only the
        file IO goes to a worker thread, so a large bunch cannot stall the main
        loop or the RPC handlers while it is being written to disk.

        Returns:
            ``True`` when the Checkpoint_File now holds the new snapshot.
        """
        try:
            payload = self.build_payload()
        except Exception as exc:
            logger.error(
                f"failed to build checkpoint snapshot for "
                f"{self.checkpoint_file}: {exc!r}"
            )
            return False
        return await asyncio.to_thread(self._write_payload, payload)

    # Snapshot writing: file operations -------------------

    def _temp_path(self, path: Path) -> Path:
        """Return the temporary file a write to ``path`` goes through.

        The temporary file is a sibling of its target so that the final
        :func:`os.replace` stays within one file system, which is what makes it
        atomic (requirement 5.2).
        """
        return Path(f"{path}{TEMP_SUFFIX}.{os.getpid()}")

    def _ensure_parent_directory(self) -> None:
        """Create the Checkpoint_File's parent directory when it is missing.

        Requirement 7.7 only asks for this before the first snapshot, but the
        check is a single ``stat`` per write and re-checking survives the
        directory being removed while the server runs, so it is not cached.
        """
        parent = self.checkpoint_file.parent
        if not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _open_restricted(path: Path, mode: str, encoding: Optional[str] = None):
        """Create ``path`` with owner-only permissions and return it open.

        Both snapshot files are written through a temporary file that is later
        renamed into place, and the renamed file keeps the temporary file's
        mode. Requirement 12.5 is therefore satisfied by creating the temporary
        file restricted in the first place, rather than by relaxing it and
        ``chmod``-ing after the rename -- the latter leaves a window in which a
        snapshot holding Job_Passwords is world readable.

        :func:`os.open`'s mode argument is masked by the process umask, so the
        permissions are additionally set explicitly on the open descriptor.
        Using the descriptor rather than the path means the file whose mode is
        tightened is provably the one just created.

        Args:
            path: File to create or truncate.
            mode: Mode string for :func:`os.fdopen`, e.g. ``"w"`` or ``"wb"``.
            encoding: Text encoding, or ``None`` for a binary mode.

        Returns:
            The open file object, owned by the caller.
        """
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, CHECKPOINT_FILE_MODE)
        try:
            try:
                os.fchmod(fd, CHECKPOINT_FILE_MODE)
            except (AttributeError, NotImplementedError):
                # Platform without fchmod; the path is a pid-suffixed temporary
                # file this process has just created, so re-resolving it here is
                # an acceptable fallback.
                os.chmod(path, CHECKPOINT_FILE_MODE)
            return os.fdopen(fd, mode, encoding=encoding)
        except Exception:
            os.close(fd)
            raise

    def _write_temp_file(self, path: Path, payload: str) -> None:
        """Write ``payload`` to ``path`` and force it out to the device.

        ``flush`` + :func:`os.fsync` before the file is renamed into place is
        what makes requirement 5.2 hold across a machine crash rather than only
        across a process kill: without it the rename could reach the disk ahead
        of the file contents.

        The file is created owner-read-write only, so the snapshot is never
        readable by anyone else, not even between its creation and the rename
        (requirement 12.5).
        """
        with self._open_restricted(path, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())

    def _copy_to_backup_temp(self, backup_temp: Path) -> None:
        """Copy the current Checkpoint_File aside as the next backup.

        Copy-then-replace rather than renaming the live file (requirement 5.3):
        renaming would leave a window in which the Checkpoint_File does not
        exist, while copying keeps it complete and in place the whole time.
        Nothing to do on the very first snapshot, when no Checkpoint_File
        exists yet.

        Only the bytes are copied, not the source file's mode: the destination
        is created owner-read-write only so that a Checkpoint_Backup_File
        rotated out of a pre-M2 snapshot with wider permissions still ends up at
        0600 (requirement 12.5).
        """
        if not self.checkpoint_file.exists():
            return

        with open(self.checkpoint_file, "rb") as source:
            with self._open_restricted(backup_temp, "wb") as target:
                shutil.copyfileobj(source, target)

    def _replace_backup(self, backup_temp: Path) -> None:
        """Move the copied snapshot onto the Checkpoint_Backup_File path."""
        if backup_temp.exists():
            os.replace(backup_temp, self.backup_file)

    def _replace_checkpoint(self, main_temp: Path) -> None:
        """Move the new snapshot onto the Checkpoint_File path.

        The last step on purpose: until it runs, the Checkpoint_File still holds
        the previous complete snapshot, and after it runs it holds the new
        complete one. There is no state in between (requirement 5.2).
        """
        os.replace(main_temp, self.checkpoint_file)

    def _write_steps(
        self,
        payload: str,
        main_temp: Path,
        backup_temp: Path,
    ) -> List[Tuple[str, Callable[[], None]]]:
        """Return the ordered file operations of one snapshot write.

        The write is expressed as an explicit sequence of named steps rather
        than straight-line code so that the atomicity property test can stop it
        after step *k* and inspect what is on disk. Order matters and is the
        contract: parent directory, new snapshot into a temporary file, backup
        copy, backup into place, snapshot into place.
        """
        return [
            ("ensure_parent_directory", self._ensure_parent_directory),
            ("write_temp_file", lambda: self._write_temp_file(main_temp, payload)),
            ("copy_to_backup_temp", lambda: self._copy_to_backup_temp(backup_temp)),
            ("replace_backup", lambda: self._replace_backup(backup_temp)),
            ("replace_checkpoint", lambda: self._replace_checkpoint(main_temp)),
        ]

    def _after_write_step(self, index: int, name: str) -> None:
        """Hook called after each step of :meth:`_write_payload` succeeds.

        A no-op in production. It exists as the injection seam for the
        atomicity property test, which overrides it to raise ``OSError`` after
        a chosen step and then asserts that the Checkpoint_File on disk is
        either absent or a complete snapshot.

        Args:
            index: Zero-based position of the step that just completed.
            name: Name of that step, as listed in :meth:`_write_steps`.
        """

    def _cleanup_temp_files(self, *paths: Path) -> None:
        """Remove leftover temporary files, best effort.

        Failures here are swallowed: the snapshot outcome has already been
        decided, and a temporary file that cannot be removed is a nuisance
        rather than a reason to report a second error.
        """
        for path in paths:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass

    def _write_payload(self, payload: str) -> bool:
        """Put ``payload`` on disk as the Checkpoint_File, atomically.

        Runs the steps of :meth:`_write_steps` in order. Any failure stops the
        sequence, logs one ERROR naming the target path, the step and the
        reason, drops the temporary files and returns ``False``; the existing
        Checkpoint_File and Checkpoint_Backup_File are left exactly as they
        were, since neither is touched before its own :func:`os.replace`
        (requirement 5.8).

        Safe to run on a worker thread: it only touches the paths resolved in
        ``__init__`` and the payload string handed to it.

        Args:
            payload: A snapshot JSON string from :meth:`build_payload`.

        Returns:
            ``True`` when every step succeeded.
        """
        main_temp = self._temp_path(self.checkpoint_file)
        backup_temp = self._temp_path(self.backup_file)

        for index, (name, action) in enumerate(
            self._write_steps(payload, main_temp, backup_temp)
        ):
            try:
                action()
                self._after_write_step(index, name)
            except Exception as exc:
                logger.error(
                    f"failed to write checkpoint to {self.checkpoint_file} "
                    f"at step {name!r}: {exc!r}"
                )
                self._cleanup_temp_files(main_temp, backup_temp)
                return False

        return True

    # Periodic snapshot task ------------------------------

    async def start(self) -> None:
        """Create and hold the periodic snapshot task (requirement 5.1).

        Called last in ``TaklerServer.start``, after the scheduler and the
        network service are up, so the first snapshot cannot race the startup
        restore. The task reference is kept on the manager because a task that
        nobody holds may be garbage collected mid-run.

        Calling it twice is a no-op with a WARNING rather than a second task:
        two loops writing the same Checkpoint_File through the same temporary
        file paths would fight over them.
        """
        if self._snapshot_task is not None and not self._snapshot_task.done():
            logger.warning(
                f"periodic checkpoint task is already running for "
                f"{self.checkpoint_file}; ignoring this start request."
            )
            return

        self._snapshot_task = asyncio.create_task(self._snapshot_loop())
        logger.info(
            f"periodic checkpoint task started: file={self.checkpoint_file}, "
            f"interval={self.interval:g} seconds."
        )

    async def stop(self) -> None:
        """Cancel the periodic task, then write one last snapshot.

        The order is the contract (requirement 5.9): the periodic task is
        cancelled first so that the final snapshot is the only writer left, and
        the snapshot is written before this coroutine returns so a clean
        shutdown never loses the state accumulated since the last period.
        ``TaklerServer._shutdown`` stops the network service and the scheduler
        before calling this, so the bunch is already quiesced by the time the
        snapshot is built.

        Cancelling the periodic task is not enough on its own: a write it had
        already handed to a worker thread keeps running, so
        :meth:`_drain_in_flight_write` waits for it before the final snapshot is
        built.

        The final write goes through the synchronous :meth:`write_checkpoint`
        on purpose: during shutdown there is nothing left to keep the event loop
        responsive for, and not depending on a worker thread removes the risk of
        the loop closing while the write is still in flight.

        Never raises: :meth:`write_checkpoint` reports failure as an ERROR plus
        ``False`` (requirement 5.8), so a Checkpoint_File that cannot be written
        does not turn a clean shutdown into a crash.
        """
        task = self._snapshot_task
        self._snapshot_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                # Expected: this is our own cancellation being acknowledged.
                pass
            except Exception as exc:  # noqa: BLE001 - boundary is intentional
                logger.error(
                    f"periodic checkpoint task for {self.checkpoint_file} "
                    f"ended with an unexpected exception: {exc!r}"
                )

        await self._drain_in_flight_write()

        logger.info(
            f"writing final checkpoint to {self.checkpoint_file} before shutdown."
        )
        self.write_checkpoint()

    async def _snapshot_loop(self) -> None:
        """Write one snapshot per configured period until cancelled.

        Sleeps first, so ``start`` does not immediately rewrite the snapshot the
        server has just restored from.

        Each iteration is wrapped in its own exception boundary (requirements
        5.1, 5.8): a single failing write logs one ERROR and the loop goes on to
        the next period, because the alternative -- a dead loop -- silently
        stops all snapshots after one transient disk error.
        ``asyncio.CancelledError`` is re-raised before that boundary so
        :meth:`stop` can still shut the loop down.

        The duration of each write is measured and compared against the
        configured period (requirement 5.10). An overrun only produces a
        WARNING: no catch-up write is issued, since writing more often is
        exactly what a manager that cannot keep up should not do.
        """
        while True:
            try:
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                raise

            started_at = time.monotonic()
            write_task = asyncio.ensure_future(self.write_checkpoint_async())
            self._write_task = write_task
            try:
                # Shielded so that a cancellation arriving mid-write does not
                # abandon a write that is already touching the temporary files:
                # the write runs to the end and :meth:`stop` drains it before
                # writing the final snapshot.
                await asyncio.shield(write_task)
                elapsed = time.monotonic() - started_at
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - boundary is intentional
                logger.error(
                    f"periodic checkpoint write to {self.checkpoint_file} "
                    f"failed: {exc!r}; keeping the periodic task running."
                )
                continue
            finally:
                if write_task.done():
                    self._write_task = None

            if elapsed > self.interval:
                logger.warning(
                    f"checkpoint write to {self.checkpoint_file} took "
                    f"{elapsed:.3f} seconds, which is longer than the "
                    f"configured interval of {self.interval:g} seconds; "
                    f"no extra write is issued for this period."
                )

    async def _drain_in_flight_write(self) -> None:
        """Wait for a snapshot write the cancelled periodic task left running.

        :meth:`write_checkpoint_async` hands the file IO to a worker thread, and
        cancelling the periodic task does not stop that thread. Without this
        wait the final snapshot of :meth:`stop` would race the abandoned write:
        both go through the same pid-suffixed temporary files, so one of them
        loses its temporary file to the other's :func:`os.replace` and reports a
        spurious ERROR -- and in the worst interleaving both write the same
        temporary file at once, which is exactly the partially written
        Checkpoint_File requirement 5.2 rules out.

        Never raises: the drained write's own failure is reported as an ERROR
        (requirement 5.8), and the final snapshot is written either way.
        """
        write_task = self._write_task
        self._write_task = None
        if write_task is None:
            return

        try:
            await write_task
        except asyncio.CancelledError:
            # The write was cancelled rather than merely abandoned; nothing is
            # left in flight, which is all this method needs to establish.
            pass
        except Exception as exc:  # noqa: BLE001 - boundary is intentional
            logger.error(
                f"checkpoint write to {self.checkpoint_file} that was still in "
                f"flight when the periodic task was cancelled failed: {exc!r}"
            )

    # Restoring -------------------------------------------

    def restore(self) -> bool:
        """Restore the bunch from the Checkpoint_File, or fall back.

        Runs the fallback chain Checkpoint_File -> Checkpoint_Backup_File ->
        empty bunch (requirements 6.1, 6.7, 6.8, 6.9). The first source that
        yields a usable snapshot wins; every failing level has already been
        reported by :meth:`_load_snapshot`, and a level whose file simply does
        not exist is an INFO rather than a failure (requirement 6.9).

        Called synchronously from ``TaklerServer.start`` before the scheduler
        loop is created, so the first dependency resolution already sees the
        restored tree (requirement 6.1).

        Never raises. An empty bunch is a valid, if unwelcome, starting point,
        while a server that refuses to start leaves the operator without even a
        ``show`` of what was running.

        Returns:
            ``True`` when a snapshot was restored, ``False`` when the server
            starts with whatever the bunch already held (normally nothing).
        """
        failed: List[Path] = []

        for path in (self.checkpoint_file, self.backup_file):
            if not path.exists():
                logger.info(f"checkpoint file does not exist: {path}.")
                continue

            snapshot = self._load_snapshot(path)
            if snapshot is None:
                # _load_snapshot already logged the path and the reason.
                failed.append(path)
                continue

            try:
                flow_count, node_count = self._restore_into_bunch(snapshot)
            except Exception as exc:  # noqa: BLE001 - boundary is intentional
                logger.error(
                    f"failed to restore the bunch from checkpoint file {path}: {exc!r}"
                )
                failed.append(path)
                continue

            logger.info(  # requirement 6.10
                f"restored {flow_count} flow(s) and {node_count} node(s) "
                f"from checkpoint file {path}."
            )
            self._verify_server_address(snapshot)
            return True

        if failed:
            logger.error(  # requirement 6.8
                f"could not restore from the checkpoint file "
                f"{self.checkpoint_file} nor from the backup file "
                f"{self.backup_file}; starting with an empty bunch."
            )
        else:
            logger.info(  # requirement 6.9
                f"no checkpoint to restore from ({self.checkpoint_file}, "
                f"{self.backup_file}); starting with an empty bunch."
            )
        return False

    def _load_snapshot(self, path: Path) -> Optional[dict]:
        """Read and validate one snapshot file.

        Everything that makes a file unusable ends the same way -- one ERROR
        naming the path and the reason plus a ``None`` result -- so that
        :meth:`restore` can treat "unreadable", "not JSON", "not a snapshot"
        and "too new" identically and simply move on to the next level of the
        fallback chain (requirement 6.7).

        Version handling (requirements 6.14, 6.15): a missing ``format_version``
        is treated as :data:`EARLIEST_SUPPORTED_FORMAT_VERSION` and the file is
        used, because that is what a snapshot written before the field existed
        is; a version above :data:`CHECKPOINT_FORMAT_VERSION` is refused, since
        a newer takler may have written keys whose meaning this version cannot
        guess -- silently ignoring them would restore a subtly wrong state,
        which is worse than falling back.

        Args:
            path: The file to read, either the Checkpoint_File or the
                Checkpoint_Backup_File.

        Returns:
            The parsed snapshot dictionary, or ``None`` when the file cannot be
            used.
        """
        try:
            snapshot = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 - boundary is intentional
            logger.error(f"failed to parse checkpoint file {path}: {exc!r}")
            return None

        if not isinstance(snapshot, dict):
            logger.error(
                f"failed to parse checkpoint file {path}: expected a snapshot "
                f"object, found {type(snapshot).__name__}."
            )
            return None

        if not isinstance(snapshot.get("bunch"), dict):
            logger.error(
                f"failed to parse checkpoint file {path}: the 'bunch' key is "
                f"missing or is not an object."
            )
            return None

        version = snapshot.get("format_version")
        if version is None:
            # Requirement 6.15: no version field means the oldest format this
            # implementation can read.
            version = EARLIEST_SUPPORTED_FORMAT_VERSION
            logger.info(
                f"checkpoint file {path} carries no format version; reading it "
                f"as version {EARLIEST_SUPPORTED_FORMAT_VERSION}."
            )
        if isinstance(version, bool) or not isinstance(version, (int, float)):
            logger.error(
                f"failed to parse checkpoint file {path}: format version "
                f"{version!r} is not a number."
            )
            return None

        if version > CHECKPOINT_FORMAT_VERSION:
            logger.error(  # requirement 6.14
                f"checkpoint file {path} has format version {version}, which "
                f"is newer than the supported version "
                f"{CHECKPOINT_FORMAT_VERSION}; this file cannot be used."
            )
            return None

        return snapshot

    def _restore_into_bunch(self, snapshot: dict) -> Tuple[int, int]:
        """Restore every flow of ``snapshot`` into the live bunch.

        Deserializes with :attr:`SerializationType.Status` so that node status,
        ``suspended``, event and meter values, limit ``value`` / ``node_paths``,
        repeat counters, the time-attribute ``free`` latch, ``task_id`` /
        ``try_no`` / ``aborted_reason`` and the flow's ``begun`` flag plus its
        calendar all come back as they were (requirements 6.2, 6.3, 6.11, 6.13).

        Nothing here requeues or otherwise touches status: submitted and active
        tasks stay exactly where they are (requirement 6.4), which is the only
        thing that keeps a restart from submitting their jobs a second time.

        Flows are added through ``Bunch.add_flow``, which sets the flow's back
        reference to *this* bunch. The bunch object itself is never replaced --
        ``Scheduler`` and ``TaklerService`` hold the same reference -- and the
        snapshot's ``server_state`` is deliberately dropped, so ``TAKLER_HOST``
        and ``TAKLER_PORT`` keep announcing the current process rather than the
        process that wrote the snapshot (requirements 6.5, 6.22).

        One flow that fails to deserialize is skipped with an ERROR naming it,
        and the remaining flows are still restored: losing one flow out of ten
        is bad, losing all ten because of it would be worse.

        Args:
            snapshot: A snapshot dictionary from :meth:`_load_snapshot`.

        Returns:
            ``(flow_count, node_count)`` of what was actually restored, with
            ``node_count`` including the flow nodes themselves.
        """
        flow_dicts = snapshot["bunch"].get("flows") or []

        flow_count = 0
        node_count = 0
        for flow_dict in flow_dicts:
            name = flow_dict.get("name") if isinstance(flow_dict, dict) else None
            try:
                flow = Flow.from_dict(flow_dict, method=SerializationType.Status)
            except Exception as exc:  # noqa: BLE001 - boundary is intentional
                logger.error(
                    f"failed to restore flow {name!r} from checkpoint file "
                    f"{self.checkpoint_file}: {exc!r}; skipping this flow."
                )
                continue

            self.bunch.add_flow(flow)
            flow_count += 1
            node_count += _count_nodes(flow)

        return flow_count, node_count

    def _iter_restored_tasks(self) -> Iterator[Task]:
        """Yield every Task_Node of the restored node tree.

        :meth:`restore` runs before the scheduler loop and before any RPC can
        add a flow, so every flow the bunch holds at this point is one
        :meth:`_restore_into_bunch` has just added; walking the bunch is
        therefore the same as walking what was restored, and it stays correct
        for the flow that had to be skipped as broken.

        Yields:
            Each :class:`~takler.core.task_node.Task` in the tree, containers
            and flows excluded: only tasks carry a job, so only they can be
            submitted or active in the sense of requirements 6.19 and 6.20.
        """

        def walk(node: Node) -> Iterator[Task]:
            if isinstance(node, Task):
                yield node
            for child in node.children:
                yield from walk(child)

        for flow in self.bunch.flows.values():
            yield from walk(flow)

    def _verify_server_address(self, snapshot: dict) -> None:
        """Compare the snapshot's host / port against the current process.

        Called right after :meth:`_restore_into_bunch`, because the graded
        outcome depends on which restored tasks are submitted or active, and
        that is only knowable once the node tree exists.

        The three outcomes (requirements 6.18 - 6.20) are graded by how much
        damage a changed address does:

        * Same address: one INFO naming the host and port, so the operator can
          see in the log that the announced address survived the restart.
        * Different address, nothing in flight: one WARNING with both
          addresses. Harmless in itself -- jobs submitted from now on inherit
          the new ``TAKLER_HOST`` / ``TAKLER_PORT`` -- so making this an ERROR
          would cry wolf on the routine "move an idle server to another host".
        * Different address with submitted / active tasks: one ERROR with both
          addresses, the number of affected tasks and all of their paths.
          Those jobs already have the old address baked into the job scripts on
          disk, so their child commands will retry against nobody for the whole
          Retry_Window and then fail. The paths are listed in full because they
          are exactly what the operator needs in order to decide between
          restarting on the old address and writing those jobs off.

        All three branches only log and return: no exception, no status change,
        and the snapshot's host / port are never written back into the bunch, so
        ``TAKLER_HOST`` / ``TAKLER_PORT`` keep announcing this process
        (requirements 6.21, 6.22). Exactly one record is emitted per call.

        Args:
            snapshot: The snapshot dictionary that was just restored from.
        """
        server_state = (snapshot.get("bunch") or {}).get("server_state") or {}
        snapshot_host = server_state.get("host")
        snapshot_port = server_state.get("port")
        current_host = self.bunch.server_state.host
        current_port = self.bunch.server_state.port

        if snapshot_host == current_host and snapshot_port == current_port:
            logger.info(  # requirement 6.18
                f"checkpoint server address matches current process: "
                f"host={current_host}, port={current_port}"
            )
            return

        in_flight = [
            node.node_path
            for node in self._iter_restored_tasks()
            if node.state.node_status in (NodeStatus.submitted, NodeStatus.active)
        ]
        address_text = (
            f"checkpoint host={snapshot_host}, port={snapshot_port}; "
            f"current host={current_host}, port={current_port}"
        )

        if not in_flight:
            logger.warning(  # requirement 6.19
                f"checkpoint server address differs from current process, "
                f"no submitted/active task affected: {address_text}"
            )
            return

        logger.error(  # requirement 6.20
            f"checkpoint server address differs from current process while "
            f"{len(in_flight)} task(s) are submitted/active; their child "
            f"commands will keep connecting to the old address: "
            f"{address_text}; affected tasks: {', '.join(in_flight)}"
        )
