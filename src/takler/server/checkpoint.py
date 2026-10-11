"""Checkpoint snapshot writing and restoring for the Takler server.

The :class:`CheckpointManager` owns the Checkpoint_File: configuration,
ordered publication, periodic writes, and startup recovery. A running server
pins a state version before background validation and writing; standalone
managers retain the original live capture path for compatibility.

It deliberately depends only on ``takler.core`` and
:mod:`takler.server.connect_config`, never on ``Scheduler`` or the gRPC
``GrpcTransport``, so snapshot behaviour can be tested without standing up a
server.

This module holds the configuration layer (constants, path and interval
resolution), the snapshot write path (payload building plus the atomic
temporary-file dance), the periodic snapshot task and the startup restore with
its Checkpoint_File -> Checkpoint_Backup_File recovery chain.

Requirements: 5.x, 6.x, 7.2, 7.3, 7.4, 7.5, 7.6, 12.5.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor
import errno
import multiprocessing
import os
import shutil
import stat
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Tuple, Union
from uuid import uuid4

import takler
from takler.core.bunch import Bunch, ServerState
from takler.core.node import Node
from takler.core.state import HaltCause, NodeStatus
from takler.core.task_node import Task
from takler.serialization.checkpoint import (
    FORMAT_VERSION,
    CheckpointDocument,
    capture_checkpoint,
    restore_checkpoint,
)
from takler.serialization.version import (
    CheckpointTransfer,
    StateVersion,
    VersionUnavailable,
)
from takler.schema.definition import DefinitionError, _decode_json
from takler.logging import get_logger
from takler.server.connect_config import AuthMode, ConnectConfig, resolve_auth_mode

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
    "CheckpointRestoreError",
]

logger = get_logger("server.checkpoint")


class CheckpointRestoreError(RuntimeError):
    """A snapshot exists, but neither primary nor backup is usable."""


class CheckpointResourceError(RuntimeError):
    """A fixed snapshot exceeds the bounded background work budget."""


MAX_BACKGROUND_WRITES = 3  # one worker and at most two waiting versions
MAX_BACKGROUND_BYTES = 1536 * 1024 * 1024
MAX_CHECKPOINT_OUTPUT_BYTES = 768 * 1024 * 1024
MAX_BACKGROUND_SECONDS = 120


def _prepare_fixed_version(
    version: StateVersion,
    saved_at: str,
    path: str,
    registry=None,
) -> str:
    """Validate and fsync a private temporary file from fixed records only."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, CHECKPOINT_FILE_MODE)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), CHECKPOINT_FILE_MODE)
            written = 0

            def bounded_write(chunk: str) -> None:
                nonlocal written
                encoded = chunk.encode("utf-8")
                written += len(encoded)
                if written > MAX_CHECKPOINT_OUTPUT_BYTES:
                    raise CheckpointResourceError("checkpoint output limit")
                stream.write(encoded)

            version.checkpoint_stream(
                bounded_write, saved_at=saved_at, registry=registry
            )
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return path


def _snapshot_path_present(path: Path) -> bool:
    """Treat broken links and inaccessible paths as recovery candidates."""
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


#: Format version stamped into every snapshot this implementation writes
#: (requirement 6.14).
CHECKPOINT_FORMAT_VERSION: int = FORMAT_VERSION

#: Oldest readable version; currently only the exact current integer version
#: is accepted. Missing versions are rejected (R0, no historical compatibility).
EARLIEST_SUPPORTED_FORMAT_VERSION: int = FORMAT_VERSION

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

#: Key inside ``secrets`` for path-keyed, attempt-bound in-flight passwords.
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

    The manager retains the live :class:`~takler.core.bunch.Bunch` for restore
    and service outcomes. Production writes pin the scheduler's immutable
    version root; the Bunch itself is never sent to a background worker.

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
        auth_mode: Optional[Union[str, AuthMode]] = None,
    ):
        """Resolve the snapshot configuration for ``bunch``.

        Configuration is resolved once, here, so that every later snapshot and
        every log message refers to the same path and period. Both resolvers
        follow the precedence ``explicit argument > connect_config > built-in
        default`` (requirement 7.5).

        The Auth_Mode is resolved the same way, through
        :func:`~takler.server.connect_config.resolve_auth_mode`, because the
        restore self-check of requirement 5.9 only applies when authentication
        is enabled. Resolving it here rather than reading it off a server object
        keeps this module's dependency on ``takler.server`` limited to
        :mod:`takler.server.connect_config`, so the self-check stays testable
        without standing up a server.

        Args:
            bunch: The bunch to snapshot and to restore into.
            checkpoint_file: Explicit Checkpoint_File path, highest precedence.
            interval: Explicit snapshot period in seconds, highest precedence.
            connect_config: Loaded ``connect.yaml``, consulted when an explicit
                argument is not provided.
            auth_mode: Explicit Auth_Mode, highest precedence. ``None`` lets
                ``TAKLER_AUTH_MODE``, then ``connect_config``, then the
                built-in default decide.
        """
        self.bunch: Bunch = bunch
        self._checkpoint_file: Path = _resolve_path(checkpoint_file, connect_config)
        self.interval: float = _resolve_interval(interval, connect_config)
        self._auth_mode: AuthMode = resolve_auth_mode(
            auth_mode, connect_config=connect_config
        )
        self.last_checkpoint_at: Optional[str] = None
        self.restore_summary: Optional[dict] = None
        self.checkpoint_failure_reason: Optional[str] = None
        self._captured_at: Optional[str] = None
        self._capture_lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._next_sequence = 0
        self._last_attempted_sequence = 0
        self._last_attempt_succeeded = False
        self._can_link_backup = False
        self._last_outcome_sequence = 0
        self._last_success_sequence = 0
        self._write_error_kind: Optional[str] = None
        self._newer_failed_sequence = 0
        self._background_pool: ProcessPoolExecutor | None = None
        self._background_slots: asyncio.Semaphore | None = None
        self._background_writes: set[asyncio.Task] = set()
        self._background_reserved = 0
        self._stopping = False

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
        """Return the Checkpoint_Backup_File path (Checkpoint_File plus ``.b``).

        Derived rather than configured, so the pair can never drift apart
        (requirement 7.4).
        """
        return Path(f"{self._checkpoint_file}{BACKUP_SUFFIX}")

    # Snapshot writing ------------------------------------

    def build_payload(self) -> str:
        """Build one standalone snapshot and serialize it to JSON.

        A server-linked manager uses the fixed version path instead. This
        compatibility path runs on its caller's thread and is only suitable
        for standalone/quiescent use.

        Format v3 stores one trusted definition, path-keyed runtime, and
        attempt-bound credentials in separate ``secrets``.

        Returns:
            The snapshot as a JSON string.

        Raises:
            Exception: Anything raised while serializing the bunch. Callers
                (:meth:`write_checkpoint`, :meth:`write_checkpoint_async`) turn
                that into an ERROR log and a ``False`` result.
        """
        document = capture_checkpoint(self.bunch)
        self._captured_at = document.saved_at
        return document.model_dump_json(exclude_unset=True)

    def _capture_for_write(self) -> tuple[int, Optional[str], str, Optional[Exception]]:
        """Order views at capture time, before any file I/O starts."""
        with self._capture_lock:
            self._next_sequence += 1
            sequence = self._next_sequence
            self._captured_at = None
            try:
                payload = self.build_payload()
            except Exception as exc:
                # A failed newer view must also supersede any older worker
                # that has not entered the publication lock yet.
                with self._publish_lock:
                    if sequence > self._last_attempted_sequence:
                        self._last_attempted_sequence = sequence
                        self._last_attempt_succeeded = False
                return sequence, None, "", exc
            saved_at = self._captured_at or datetime.now().astimezone().isoformat()
            return sequence, payload, saved_at, None

    def _pin_fixed_for_write(self) -> tuple[int, StateVersion | None, str]:
        """Reserve publication order and pin a complete root on the writer."""
        with self._capture_lock:
            self._next_sequence += 1
            sequence = self._next_sequence
        scheduler = self.query_scheduler
        try:
            version = scheduler.version_store.pin()
        except VersionUnavailable as exc:
            if str(exc) == "state_changing":
                return sequence, None, ""
            self._fixed_failure(sequence, exc)
            return sequence, None, ""
        except Exception as exc:
            self._fixed_failure(sequence, exc)
            return sequence, None, ""
        return sequence, version, datetime.now(timezone.utc).isoformat()

    def _fixed_failure(self, sequence: int, exc: Exception) -> None:
        self._newer_failed_sequence = max(self._newer_failed_sequence, sequence)
        category = self._failure_category(exc)
        logger.error(
            f"failed to build checkpoint snapshot for "
            f"{self.checkpoint_file}: {category}"
        )
        self._record_outcome(sequence, False, False, "", category)

    @staticmethod
    def _failure_category(exc: Exception) -> str:
        """Expose a stable diagnosis without paths or raw exception text."""
        if isinstance(exc, CheckpointResourceError) or (
            isinstance(exc, VersionUnavailable)
            and str(exc) in {"version_memory_limit", "field_too_large"}
        ):
            return "resource exhausted"
        if isinstance(exc, OSError):
            if exc.errno in (errno.EACCES, errno.EPERM, errno.EROFS):
                return "permission denied"
            if exc.errno in (errno.ENOSPC, errno.EDQUOT):
                return "storage full"
            return "storage I/O error"
        return "snapshot validation error"

    def _record_outcome(
        self,
        sequence: int,
        success: bool,
        published: bool,
        saved_at: str,
        reason: Optional[str],
    ) -> None:
        """Apply only the newest resolved write to the live service state."""
        scheduler = getattr(self, "query_scheduler", None)
        before = (
            self.last_checkpoint_at,
            self.checkpoint_failure_reason,
            self.bunch.server_state,
        )
        if scheduler is not None:
            scheduler.query_revision += 1
        try:
            self._apply_outcome(sequence, success, published, saved_at, reason)
        finally:
            if scheduler is not None:
                if before != (
                    self.last_checkpoint_at,
                    self.checkpoint_failure_reason,
                    self.bunch.server_state,
                ):
                    scheduler.notify_query_change("/", "service")
                    scheduler.version_store.flush()
                scheduler.query_revision += 1

    def _apply_outcome(
        self,
        sequence: int,
        success: bool,
        published: bool,
        saved_at: str,
        reason: Optional[str],
    ) -> None:
        if success and published and sequence > self._last_success_sequence:
            self.last_checkpoint_at = saved_at
            self._last_success_sequence = sequence
        if sequence < self._last_outcome_sequence:
            return
        if not published and success:
            return  # A newer view is already on disk and will report its result.
        self._last_outcome_sequence = sequence
        causes = list(self.bunch.server_state.halt_causes)
        if success:
            self.checkpoint_failure_reason = None
            causes = [
                cause for cause in causes if cause is not HaltCause.checkpoint_failure
            ]
        else:
            self.checkpoint_failure_reason = reason or "storage I/O error"
            if HaltCause.checkpoint_failure not in causes:
                causes.append(HaltCause.checkpoint_failure)
        if causes != self.bunch.server_state.halt_causes:
            self.bunch.server_state = ServerState.from_causes(causes)

    def _publish_captured(
        self, sequence: int, payload: str
    ) -> tuple[bool, bool, Optional[str]]:
        """Serialize disk publication and discard a view older than disk."""
        with self._publish_lock:
            if (
                sequence <= self._newer_failed_sequence
                and self._newer_failed_sequence > self._last_attempted_sequence
            ):
                return False, False, None
            if sequence <= self._last_attempted_sequence:
                return self._last_attempt_succeeded, False, None
            self._last_attempted_sequence = sequence
            self._write_error_kind = None
            try:
                success = self._write_payload(payload)
            except Exception as exc:
                logger.error(
                    f"failed to write checkpoint to {self.checkpoint_file}: "
                    f"{self._failure_category(exc)}"
                )
                self._write_error_kind = self._failure_category(exc)
                success = False
            self._last_attempt_succeeded = success
            return success, success, self._write_error_kind

    def _prepared_path(self, sequence: int) -> Path:
        return Path(
            f"{self.checkpoint_file}{TEMP_SUFFIX}.{os.getpid()}."
            f"{sequence}.{uuid4().hex}"
        )

    def _publish_prepared(
        self, sequence: int, prepared: Path
    ) -> tuple[bool, bool, Optional[str]]:
        """Rotate a validated, fsynced temp file under publication order."""
        backup_temp = self._temp_path(self.backup_file)
        with self._publish_lock:
            if (
                sequence <= self._newer_failed_sequence
                and self._newer_failed_sequence > self._last_attempted_sequence
            ):
                self._cleanup_temp_files(prepared)
                return False, False, None
            if sequence <= self._last_attempted_sequence:
                self._cleanup_temp_files(prepared)
                return self._last_attempt_succeeded, False, None
            self._last_attempted_sequence = sequence
            self._write_error_kind = None
            try:
                self._ensure_parent_directory()
                self._copy_to_backup_temp(backup_temp)
                self._replace_backup(backup_temp)
                self._replace_checkpoint(prepared)
                directory_fd = os.open(
                    self.checkpoint_file.parent,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                )
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except Exception as exc:
                self._write_error_kind = self._failure_category(exc)
                logger.error(
                    f"failed to write checkpoint to {self.checkpoint_file}: "
                    f"{self._write_error_kind}"
                )
                self._last_attempt_succeeded = False
                self._cleanup_temp_files(prepared, backup_temp)
                return False, False, self._write_error_kind
            self._last_attempt_succeeded = True
            self._can_link_backup = True
            self._cleanup_temp_files(backup_temp)
            return True, True, None

    def _finish_async_write(
        self, task: asyncio.Task, sequence: int, saved_at: str
    ) -> None:
        """Apply the result on the loop even if the caller was cancelled."""
        try:
            success, published, reason = task.result()
        except asyncio.CancelledError:
            self._newer_failed_sequence = max(self._newer_failed_sequence, sequence)
            self._record_outcome(sequence, False, False, "", "storage I/O error")
        except Exception as exc:
            self._newer_failed_sequence = max(self._newer_failed_sequence, sequence)
            category = self._failure_category(exc)
            logger.error(f"checkpoint worker failed: {category}")
            self._record_outcome(sequence, False, False, "", category)
        else:
            self._record_outcome(sequence, success, published, saved_at, reason)

    async def _write_fixed_background(
        self, sequence: int, version: StateVersion, saved_at: str
    ) -> tuple[bool, bool, Optional[str]]:
        """Validate and encode away from the loop, then publish in order."""
        assert self._background_slots is not None
        async with self._background_slots:
            prepared = self._prepared_path(sequence)
            try:
                registry = self.query_scheduler.version_store.registry
                if all(type_id.startswith("takler.") for type_id in registry._ids):
                    if self._background_pool is None:
                        self._background_pool = ProcessPoolExecutor(
                            max_workers=1,
                            mp_context=multiprocessing.get_context("spawn"),
                        )
                    preparation = asyncio.get_running_loop().run_in_executor(
                        self._background_pool,
                        _prepare_fixed_version,
                        CheckpointTransfer(version),
                        saved_at,
                        str(prepared),
                    )
                    try:
                        await asyncio.wait_for(
                            preparation, timeout=MAX_BACKGROUND_SECONDS
                        )
                    except TimeoutError:
                        pool = self._background_pool
                        self._background_pool = None
                        await asyncio.to_thread(pool.terminate_workers)
                        raise CheckpointResourceError("checkpoint timeout") from None
                else:
                    # Trusted plugin codecs can be local classes and cannot be
                    # imported in a spawn worker. Keep the same fixed-root
                    # semantics, with a single bounded thread worker.
                    await asyncio.to_thread(
                        _prepare_fixed_version,
                        version,
                        saved_at,
                        str(prepared),
                        registry,
                    )
                return await asyncio.to_thread(
                    self._publish_prepared, sequence, prepared
                )
            except Exception as exc:
                self._cleanup_temp_files(prepared)
                self._newer_failed_sequence = max(self._newer_failed_sequence, sequence)
                category = self._failure_category(exc)
                logger.error(f"checkpoint worker failed: {category}")
                return False, False, category

    async def _write_fixed_async(self) -> bool:
        if self._stopping:
            return False
        sequence, version, saved_at = self._pin_fixed_for_write()
        if version is None:
            return False
        reservation = (version.retained_bytes * 5 + 1) // 2
        if (
            len(self._background_writes) >= MAX_BACKGROUND_WRITES
            or self._background_reserved + reservation > MAX_BACKGROUND_BYTES
        ):
            self._fixed_failure(sequence, CheckpointResourceError())
            return False
        self._background_reserved += reservation
        if self._background_slots is None:
            self._background_slots = asyncio.Semaphore(1)
        worker = asyncio.create_task(
            self._write_fixed_background(sequence, version, saved_at)
        )
        self._background_writes.add(worker)

        def finish(task):
            self._background_writes.discard(task)
            self._background_reserved -= reservation
            self._finish_async_write(task, sequence, saved_at)

        worker.add_done_callback(finish)
        try:
            success, _, _ = await asyncio.shield(worker)
        except asyncio.CancelledError:
            raise
        except Exception:
            return False  # the completion callback records the worker fault
        return success

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
        if getattr(self, "query_scheduler", None) is not None:
            sequence, version, saved_at = self._pin_fixed_for_write()
            if version is None:
                return False
            prepared = self._prepared_path(sequence)
            try:
                _prepare_fixed_version(
                    version,
                    saved_at,
                    str(prepared),
                    self.query_scheduler.version_store.registry,
                )
            except Exception as exc:
                self._fixed_failure(sequence, exc)
                return False
            success, published, reason = self._publish_prepared(sequence, prepared)
            self._record_outcome(sequence, success, published, saved_at, reason)
            return success
        sequence, payload, saved_at, error = self._capture_for_write()
        if error is not None:
            logger.error(
                f"failed to build checkpoint snapshot for "
                f"{self.checkpoint_file}: {self._failure_category(error)}"
            )
            self._record_outcome(
                sequence, False, False, "", self._failure_category(error)
            )
            return False
        assert payload is not None
        success, published, reason = self._publish_captured(sequence, payload)
        self._record_outcome(sequence, success, published, saved_at, reason)
        return success

    async def write_checkpoint_async(self) -> bool:
        """Write a fixed state version with background validation and encoding.

        A server with a scheduler pins on the loop, then sends only immutable
        records to a spawned process. Standalone manager use retains the old
        capture path for callers that have no version store.

        Returns:
            ``True`` when the Checkpoint_File now holds the new snapshot.
        """
        if getattr(self, "query_scheduler", None) is not None:
            return await self._write_fixed_async()
        sequence, payload, saved_at, error = self._capture_for_write()
        if error is not None:
            logger.error(
                f"failed to build checkpoint snapshot for "
                f"{self.checkpoint_file}: {self._failure_category(error)}"
            )
            self._record_outcome(
                sequence, False, False, "", self._failure_category(error)
            )
            return False
        assert payload is not None
        worker = asyncio.create_task(
            asyncio.to_thread(self._publish_captured, sequence, payload)
        )
        worker.add_done_callback(
            lambda task: self._finish_async_write(task, sequence, saved_at)
        )
        try:
            success, _, _ = await asyncio.shield(worker)
        except asyncio.CancelledError:
            raise
        except Exception:
            return False  # _finish_async_write reports and pauses the service.
        return success

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

        Preserve the live file until the new one is ready (requirement 5.3):
        renaming it first would leave a window with no Checkpoint_File.
        Nothing to do on the very first snapshot, when no Checkpoint_File
        exists yet.

        Only the bytes are copied, not the source file's mode: the destination
        is created owner-read-write only so that a Checkpoint_Backup_File
        rotated out of a pre-M2 snapshot with wider permissions still ends up at
        0600 (requirement 12.5).
        """
        if not self.checkpoint_file.exists():
            return

        if self._can_link_backup:
            try:
                source = self.checkpoint_file.lstat()
                if (
                    stat.S_ISREG(source.st_mode)
                    and stat.S_IMODE(source.st_mode) == CHECKPOINT_FILE_MODE
                    and source.st_nlink == 1
                ):
                    # The old primary was fsynced by this process and is never
                    # modified in place. The link becomes the previous version
                    # after the new primary is atomically installed.
                    os.link(self.checkpoint_file, backup_temp, follow_symlinks=False)
                    return
            except (OSError, NotImplementedError):
                # Filesystems without hard links retain the copy path.
                pass

        with open(self.checkpoint_file, "rb") as source:
            with self._open_restricted(backup_temp, "wb") as target:
                shutil.copyfileobj(source, target)
                target.flush()
                os.fsync(target.fileno())

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
                self._write_error_kind = self._failure_category(exc)
                logger.error(
                    f"failed to write checkpoint to {self.checkpoint_file} "
                    f"at step {name!r}: {self._failure_category(exc)}"
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

        self._stopping = False
        self._snapshot_task = asyncio.create_task(self._snapshot_loop())
        logger.info(
            f"periodic checkpoint task started: file={self.checkpoint_file}, "
            f"interval={self.interval:g} seconds."
        )

    async def stop(self) -> bool:
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

        Returns whether the final snapshot reached disk. Callers can make a
        failed final write visible in their process exit status.
        """
        self._stopping = True
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
                    f"ended with an unexpected exception: {self._failure_category(exc)}"
                )

        await self._drain_in_flight_write()

        if self._background_pool is not None:
            pool = self._background_pool
            self._background_pool = None
            await asyncio.to_thread(pool.shutdown, wait=True, cancel_futures=True)

        logger.info(
            f"writing final checkpoint to {self.checkpoint_file} before shutdown."
        )
        success = self.write_checkpoint()
        if not success:
            logger.error("final checkpoint failed; shutdown state may be lost")
        return success

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
                with self._capture_lock:
                    self._next_sequence += 1
                    sequence = self._next_sequence
                self._record_outcome(
                    sequence, False, False, "", self._failure_category(exc)
                )
                logger.error(
                    f"periodic checkpoint write to {self.checkpoint_file} "
                    f"failed: {self._failure_category(exc)}; keeping the periodic task running."
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
        """Wait for every snapshot write left by a cancelled caller or loop.

        A caller cancellation is shielded from its process or thread worker.
        Draining before the final write prevents an older worker from touching
        the checkpoint files after shutdown completes.

        Never raises: the drained write's own failure is reported as an ERROR
        (requirement 5.8), and the final snapshot is written either way.
        """
        write_task = self._write_task
        self._write_task = None
        pending = set(self._background_writes)
        if write_task is not None:
            pending.add(write_task)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
            await asyncio.sleep(0)  # let outcome callbacks run before final write

    # Restoring -------------------------------------------

    def restore(self) -> bool:
        """Restore one complete snapshot or refuse to start in recovery mode.

        With neither file present this is a fresh service and returns ``False``.
        Once either file exists, failure of both candidates raises before any
        scheduler or transport starts. Only a validated candidate is published.
        """
        self.restore_summary = None
        self.last_checkpoint_at = None
        self.checkpoint_failure_reason = None
        recovery_mode = any(
            _snapshot_path_present(path)
            for path in (self.checkpoint_file, self.backup_file)
        )
        for path in (self.checkpoint_file, self.backup_file):
            if not _snapshot_path_present(path):
                logger.info(f"checkpoint file does not exist: {path}.")
                continue

            snapshot = self._load_snapshot(path)
            if snapshot is None:
                # _load_snapshot already logged the path and the reason.
                continue

            try:
                flow_count, node_count = self._restore_into_bunch(snapshot)
            except Exception as exc:  # noqa: BLE001 - boundary is intentional
                logger.error(
                    f"failed to restore the bunch from checkpoint file {path}: {exc!r}"
                )
                continue

            snapshot_state = snapshot["runtime"]["server_state"]
            causes = list(self.bunch.server_state.halt_causes)
            if HaltCause.recovery not in causes:
                causes.append(HaltCause.recovery)
            self.bunch.server_state = ServerState.from_causes(causes)
            if HaltCause.checkpoint_failure in causes:
                self.checkpoint_failure_reason = "previous checkpoint failure"
            in_flight_count = sum(
                task.state.node_status in _PERSISTED_STATUSES
                for task in self._iter_restored_tasks()
            )
            saved_at = datetime.fromisoformat(snapshot["saved_at"]).isoformat()
            self.last_checkpoint_at = saved_at
            self.restore_summary = {
                "source": "primary" if path == self.checkpoint_file else "backup",
                "saved_at": saved_at,
                "fallback": path == self.backup_file,
                "flow_count": flow_count,
                "node_count": node_count,
                "in_flight_count": in_flight_count,
                "snapshot_status": snapshot_state["status"],
                "snapshot_status_reason": snapshot_state["status_reason"],
                "effective_status": self.bunch.server_state.status.value,
                "effective_status_reason": self.bunch.server_state.status_reason,
            }
            logger.info(  # requirement 6.10
                f"restored {flow_count} flow(s) and {node_count} node(s) "
                f"from checkpoint file {path}."
            )
            self._verify_server_address(snapshot)
            return True

        if recovery_mode:
            logger.error(  # requirement 6.8
                f"could not restore from the checkpoint file "
                f"{self.checkpoint_file} nor from the backup file "
                f"{self.backup_file}; refusing to start."
            )
            raise CheckpointRestoreError(
                "no valid primary or backup checkpoint; service was not started"
            )
        else:
            logger.info(  # requirement 6.9
                f"no checkpoint to restore from ({self.checkpoint_file}, "
                f"{self.backup_file}); keeping the current bunch."
            )
        return False

    def _load_snapshot(self, path: Path) -> Optional[dict]:
        """Read and validate one snapshot file.

        Everything that makes a file unusable ends the same way -- one ERROR
        naming the path and the reason plus a ``None`` result -- so that
        :meth:`restore` can treat "unreadable", "not JSON", "not a snapshot"
        and "too new" identically and simply move on to the next level of the
        fallback chain (requirement 6.7).

        Only the current integer format version is accepted. Missing versions,
        older/newer formats, booleans and floats are unusable; v3 does not
        provide historical format migration.

        Args:
            path: The file to read, either the Checkpoint_File or the
                Checkpoint_Backup_File.

        Returns:
            The parsed snapshot dictionary, or ``None`` when the file cannot be
            used.
        """
        try:
            snapshot = _decode_json(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 - boundary is intentional
            logger.error(f"failed to parse checkpoint file {path}: {exc!r}")
            return None

        if not isinstance(snapshot, dict):
            logger.error(
                f"failed to parse checkpoint file {path}: expected a snapshot "
                f"object, found {type(snapshot).__name__}."
            )
            return None

        version = snapshot.get("format_version")
        if type(version) is not int or version != CHECKPOINT_FORMAT_VERSION:
            logger.error(
                f"checkpoint file {path} has unsupported format version "
                f"{version if type(version) is int else type(version).__name__}; "
                f"expected integer {CHECKPOINT_FORMAT_VERSION}; this file cannot be used."
            )
            return None

        if not isinstance(snapshot.get("definition"), dict):
            logger.error(
                f"failed to parse checkpoint file {path}: the 'definition' key is "
                f"missing or is not an object."
            )
            return None

        try:
            CheckpointDocument.model_validate(snapshot)
        except Exception:
            logger.error(f"checkpoint file {path} has an invalid v3 document.")
            return None

        return snapshot

    def _restore_into_bunch(self, snapshot: dict) -> Tuple[int, int]:
        """Validate an entire temporary tree and credentials, then publish once.

        Unknown types or invalid runtime reject the complete snapshot, allowing
        restore() to try the backup without any changes to the live tree.
        """
        candidate = restore_checkpoint(snapshot)
        if not isinstance(candidate, Bunch):
            raise DefinitionError("invalid_structure")
        # Rebuild before publishing the candidate. A bad retained path must
        # reject this snapshot without partly replacing the live bunch, and
        # the first post-recovery submission must not scan the full tree.
        from takler.serialization.artifact_path_index import ArtifactPathIndex

        candidate._artifact_path_index = ArtifactPathIndex(candidate)
        mapping = snapshot["secrets"][JOB_PASSWORDS_KEY]
        deployment = self.bunch.server_config
        self.bunch.__dict__.update(vars(candidate))
        self.bunch.server_config = deployment
        self.bunch.in_limit_manager.node = self.bunch
        for limit in self.bunch.limits:
            limit.node = self.bunch
        for child in self.bunch.children:
            child.parent = self.bunch
        for expression in (
            self.bunch.trigger_expression,
            self.bunch.complete_trigger_expression,
        ):
            if expression is not None:
                expression.ast.set_parent_node(self.bunch)
        for flow in self.bunch.flows.values():
            flow.bunch = self.bunch
        logger.info(
            f"recovered the job password of {len(mapping)} task(s) from checkpoint file {self.checkpoint_file}."
        )
        return len(self.bunch.flows), sum(
            _count_nodes(flow) for flow in self.bunch.flows.values()
        )

    def _iter_restored_tasks(self) -> Iterator[Task]:
        """Yield every Task_Node of the restored node tree.

        :meth:`restore` runs before the scheduler loop and before any RPC can
        add a flow, so every flow the bunch holds at this point is one
        :meth:`_restore_into_bunch` has just added; walking the bunch is
        therefore the same as walking what was restored, and it stays correct
        after the complete temporary tree has been committed.

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
        server_config = snapshot.get("source_address") or {}
        snapshot_host = server_config.get("host")
        snapshot_port = server_config.get("port")
        current_host = self.bunch.server_config.host
        current_port = self.bunch.server_config.port

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
