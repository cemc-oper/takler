import asyncio
import contextlib
import os
import stat
from pathlib import Path
from typing import Union, Optional

from takler.core import Bunch, NodeStatus
from takler.logging import configure, get_logger

from .scheduler import Scheduler
from .network_service import TaklerService
from .checkpoint import CheckpointManager
from .audit import AuditLogger
from .auth import AuthInterceptor, CredentialStore
from .tls import build_server_credentials, resolve_tls_paths
from .zombie import ZombieDetector
from .connect_config import (
    AuthMode,
    ConnectConfig,
    ExceptionPolicy,
    SecuritySettings,
    ZombiePolicy,
    resolve_audit_file,
    resolve_auth_mode,
    resolve_exception_policy,
    resolve_zombie_policy,
)


logger = get_logger("server")


# Permission bits of a newly created *regular* file before the umask is applied.
# ``open()`` / ``Path.write_text()`` request 0o666 and the kernel clears whatever
# the umask masks out, so ``_NEW_FILE_BASE_MODE & ~umask`` is exactly the mode a
# freshly written job script gets (Requirement 12.6).
_NEW_FILE_BASE_MODE = 0o666

# The two read bits that must stay clear for a job script -- and therefore the
# Job_Password it exports -- to be unreadable to anyone but its owner. The
# execute bits are irrelevant here (a new regular file never gets them from the
# base mode) and so are the write bits: being able to write a job script is a
# separate problem from being able to read the password out of it.
_OTHER_READ_BITS = stat.S_IRGRP | stat.S_IROTH

# The umask recommended when Auth_Mode is enabled: it clears every group and
# other bit, so job scripts come out 0o600 before takler adds the owner execute
# bit (Requirements 12.8, 17.6).
_RECOMMENDED_UMASK = 0o077


def _read_umask() -> int:
    """Return the current process umask without changing it.

    POSIX offers no way to read the umask on its own: ``os.umask(mask)`` sets a
    new value and returns the previous one. Reading it therefore means setting a
    temporary value, taking the old value back and immediately restoring it. The
    temporary value is the restrictive :data:`_RECOMMENDED_UMASK` rather than
    something permissive, so that the brief window in between cannot widen the
    permissions of a file created by another thread.

    The umask is per-process, not per-thread, so this pair of calls is only safe
    while the process is still single-threaded -- hence the one caller runs
    during startup, before any worker thread or task exists.
    """
    current = os.umask(_RECOMMENDED_UMASK)
    os.umask(current)
    return current


def _as_optional_path_str(value: Optional[Union[str, Path]]) -> Optional[str]:
    """Normalize an optional path argument to ``Optional[str]``.

    A ``Path`` becomes its string form, and a blank or whitespace-only string
    becomes ``None``: throughout takler an empty value from a config source
    means "not provided", so it must let the next precedence source apply
    rather than count as a configured path.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


class TaklerServer:
    """
    Takler server which will create three members when init:

    * bunch: A bunch for flows.
    * scheduler: A scheduler to check dependencies in loop.
    * network service: A gRPC server to receive client command.
    * checkpoint manager: owns the Checkpoint_File of this server process.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[Union[str, int]] = None,
        exception_policy: "Optional[Union[str, ExceptionPolicy]]" = None,
        connect_config: "Optional[ConnectConfig]" = None,
        checkpoint_file: Optional[Union[str, Path]] = None,
        checkpoint_interval: Optional[float] = None,
        tls_cert_file: Optional[Union[str, Path]] = None,
        tls_key_file: Optional[Union[str, Path]] = None,
    ):
        """Build the bunch and the three services that operate on it.

        Args:
            host: Host name announced to clients and to job scripts.
            port: Port the gRPC service listens on.
            exception_policy: Explicit exception-handling policy, highest
                precedence (Requirement 2.6).
            connect_config: Loaded ``connect.yaml``, consulted for the snapshot
                settings when the explicit arguments below are not provided
                (Requirement 7.5).
            checkpoint_file: Explicit Checkpoint_File path, highest precedence.
                ``None`` leaves the resolution to ``CheckpointManager``, which
                falls back to ``connect_config`` and then to ``takler.check``
                relative to the current working directory (Requirement 7.3).
            checkpoint_interval: Explicit snapshot period in seconds, highest
                precedence (Requirement 7.2).
            tls_cert_file: Explicit server certificate path, highest precedence
                over the ``security`` section of ``connect_config``
                (Requirement 1.10).
            tls_key_file: Explicit server private key path, same precedence as
                ``tls_cert_file`` (Requirement 1.10).
        """
        # Resolve the effective exception-handling policy following the source
        # precedence: explicit argument > ``TAKLER_EXCEPTION_POLICY`` env var >
        # built-in default ``RESILIENT`` (Requirement 2.6).
        self.exception_policy: ExceptionPolicy = resolve_exception_policy(
            exception_policy
        )

        # Kept because several security settings live in the ``security`` section
        # of ``connect.yaml`` and are needed after construction, not just
        # forwarded to the checkpoint manager (Requirement 3.5).
        self.connect_config: Optional[ConnectConfig] = connect_config

        # Resolved once here: ``TAKLER_AUTH_MODE`` env var > the ``auth_mode``
        # field of the Connect_Config ``security`` section > ``disabled``
        # (Requirements 3.5, 3.6).
        self.auth_mode: AuthMode = resolve_auth_mode(connect_config=connect_config)

        # Resolved with the same precedence as the Auth_Mode (Requirement 3.5).
        # Held on the server because it is a server-global setting that the
        # start-up record reports together with the Auth_Mode (Requirement 3.11).
        self.zombie_policy: ZombiePolicy = resolve_zombie_policy(
            connect_config=connect_config
        )

        # Explicit TLS pair, the highest precedence source for the server
        # certificate and private key (Requirement 1.10). Only kept here; the
        # pair is turned into gRPC server credentials -- and the Connect_Config
        # ``security`` section consulted for whatever the command line left out
        # -- when the network service is started.
        self.tls_cert_file: Optional[str] = _as_optional_path_str(tls_cert_file)
        self.tls_key_file: Optional[str] = _as_optional_path_str(tls_key_file)

        # The credential store and the interceptor are built here, before any
        # service exists, because ``grpc.aio`` only accepts interceptors as an
        # argument of ``grpc.aio.server()``: there is no way to add one to a
        # server that is already constructed. Neither construction touches the
        # filesystem, so building them is safe even when authentication is off
        # and the credential files do not exist -- whether they must exist is
        # decided by the explicit start-up validation in :meth:`start`.
        security = self.security_settings
        self.credential_store: CredentialStore = CredentialStore(
            secret_file=None if security is None else security.operator_secret_file,
            whitelist_file=(
                None if security is None else security.operator_whitelist_file
            ),
        )
        # One Audit_Logger for the whole server, shared by its record points:
        # the Control_Command handler in the Network_Service (Requirement 11.2)
        # and the rejection path of the Auth_Interceptor (Requirement 11.3).
        # Resolved with the usual precedence -- ``TAKLER_AUDIT_FILE`` env var >
        # the ``audit_file`` field of the Connect_Config ``security`` section >
        # no Audit_File (Requirement 3.5) -- and resolved here rather than in
        # ``start()`` because ``configure()`` needs the same value to install
        # the audit sink.
        self.audit_file: Optional[str] = resolve_audit_file(
            connect_config=connect_config
        )
        self.audit_logger: AuditLogger = AuditLogger(self.audit_file)
        self.auth_interceptor: AuthInterceptor = AuthInterceptor(
            auth_mode=self.auth_mode,
            credential_store=self.credential_store,
            audit_logger=self.audit_logger,
        )

        # Shared fatal-error signal. In ``FAIL_FAST`` mode the scheduler / service
        # request a clean server exit by triggering this event; ``run()`` waits on
        # it alongside the scheduler task so both modes share one clean shutdown
        # path (Requirements 2.4, 2.5, 3.4).
        self._fatal_error_event: asyncio.Event = asyncio.Event()
        # Guard so the clean shutdown flow runs at most once, whether it is
        # reached through ``stop()`` or through ``run()`` after the scheduler
        # task completes / the fatal-error event fires.
        self._stopped: bool = False

        # The one Zombie_Detector of this server, built from the settings already
        # resolved above so the Scheduler receives a detector that is in force
        # from the very first Child_Command. Without this the Scheduler's
        # ``zombie_detector`` would stay ``None`` in a real server -- the "no
        # policy configured" case meant for a directly driven scheduler -- and
        # the whole feature would be unreachable in production
        # (Requirements 9.1, 10.1).
        self.zombie_detector: ZombieDetector = ZombieDetector(
            auth_mode=self.auth_mode,
            zombie_policy=self.zombie_policy,
            audit_logger=self.audit_logger,
        )

        port_str = str(port)
        self.bunch: Bunch = Bunch(host=host, port=port_str)
        self.scheduler: Scheduler = Scheduler(
            bunch=self.bunch,
            exception_policy=self.exception_policy,
            fatal_shutdown=self._trigger_fatal_shutdown,
            zombie_detector=self.zombie_detector,
        )
        self.network_service: TaklerService = TaklerService(
            scheduler=self.scheduler,
            host="[::]",
            port=port,
            exception_policy=self.exception_policy,
            fatal_shutdown=self._trigger_fatal_shutdown,
            # The TLS credentials are *not* passed here: resolving them reads
            # files and may abort the start-up, which belongs to ``start()``
            # (Requirement 1.6). ``start()`` assigns them to the service before
            # it binds the port.
            interceptors=(self.auth_interceptor,),
            audit_logger=self.audit_logger,
        )
        # The manager keeps a reference to the same live bunch the scheduler and
        # the network service hold, so a restored snapshot is visible to both
        # without any of them being re-wired (Requirements 5.1, 6.1).
        self.checkpoint_manager: CheckpointManager = CheckpointManager(
            bunch=self.bunch,
            checkpoint_file=checkpoint_file,
            interval=checkpoint_interval,
            connect_config=connect_config,
        )

    @property
    def security_settings(self) -> Optional[SecuritySettings]:
        """The ``security`` section of the Connect_Config, or ``None``.

        ``None`` when the server runs without a config file at all. Every
        consumer treats that the same way as an all-default section, so the
        command line options alone can still enable TLS (Requirement 1.10).
        """
        if self.connect_config is None:
            return None
        return self.connect_config.security

    def _trigger_fatal_shutdown(self):
        """Signal that the server must exit (the ``FAIL_FAST`` path).

        Sets the shared fatal-error event so that ``run()`` -- which waits on it
        together with the scheduler task -- wakes up and drives the shared clean
        shutdown flow. Safe to call more than once.
        """
        self._fatal_error_event.set()

    def _check_job_script_umask(self):
        """Warn once at startup when the umask leaves job scripts world-readable.

        A job script exports ``TAKLER_PASS``, and its read/write bits come from
        the process umask (Requirement 12.6). Under the common default umask
        ``0022`` the script is group- and other-readable, so on a shared file
        system every account can read the Job_Password of every in-flight job and
        authentication buys nothing. When Auth_Mode is enabled, that combination
        earns exactly one WARNING naming the current umask, the risk and the
        recommended value (Requirement 12.8).

        Nothing is warned about when Auth_Mode is disabled: without
        authentication the password is not a credential in the first place.

        This runs once, from :meth:`start`. Checking per job script instead would
        emit one identical line per task, which for a flow with a thousand tasks
        is a thousand lines saying the same thing.
        """
        if self.auth_mode is not AuthMode.ENABLED:
            return

        current_umask = _read_umask()
        # Mode a new regular file ends up with, then the group / other read bits
        # within it. Non-zero means someone other than the owner can read it.
        new_file_mode = _NEW_FILE_BASE_MODE & ~current_umask
        if not new_file_mode & _OTHER_READ_BITS:
            return

        logger.warning(
            f"umask {current_umask:04o} lets users other than the owner read "
            f"newly created files (a job script would be {new_file_mode:04o}); "
            f"job scripts export TAKLER_PASS, so on a shared file system the "
            f"one-time password of every running job is readable by other "
            f"accounts and authentication is defeated; "
            f"set the server process umask to {_RECOMMENDED_UMASK:04o}."
        )

    def _start_security(self):
        """Validate the security configuration and report the resulting posture.

        Three things happen here, in this order, and all of them before the
        service binds its port:

        1. the credential files are validated against the Auth_Mode, which
           aborts the start-up when authentication is enabled but the
           Operator_Secret_File is unusable (Requirements 7.3, 7.4);
        2. the TLS pair is turned into gRPC server credentials, which aborts the
           start-up when the pair is half configured or unreadable
           (Requirements 1.4, 1.5), and is handed to the Network_Service so it
           can pick ``add_secure_port`` over ``add_insecure_port``
           (Requirements 1.1, 1.2);
        3. the effective Auth_Mode is stated in the log.

        Both failures raise :class:`~takler.exceptions.SecurityConfigError`,
        which the Server_CLI turns into one line on stderr and exit code 1
        (Requirement 1.6). Refusing to start is the point: an operator who asked
        for TLS or for authentication and silently got neither has no way to
        find out, because both sides of the wire keep working.

        The Auth_Mode record is asymmetric on purpose. ``enabled`` is an INFO
        naming the Auth_Mode, the Zombie_Policy and the Operator_Whitelist_File,
        which is the configuration an operator wants echoed back after a change
        (Requirement 3.11). ``disabled`` is a WARNING spelling out what it means
        in practice -- any caller who can reach the port may run a
        Control_Command -- because it is the default, and therefore the state a
        server ends up in without anyone choosing it (Requirement 3.12).
        """
        # Raises when authentication is enabled but the secret file is unusable;
        # also warns about a missing whitelist and about wide file permissions.
        self.credential_store.validate_at_startup(self.auth_mode)

        security = self.security_settings
        # Resolved separately from the credentials because the INFO record of
        # Requirement 1.7 names the certificate file, and a ServerCredentials
        # object does not remember where its bytes came from.
        cert_file, _ = resolve_tls_paths(
            security, self.tls_cert_file, self.tls_key_file
        )
        # Warns about the mTLS extension point, raises on a half configured or
        # unreadable pair, returns ``None`` when TLS is simply not configured.
        credentials = build_server_credentials(
            security, self.tls_cert_file, self.tls_key_file
        )
        self.network_service.server_credentials = credentials
        self.network_service.tls_cert_file = cert_file

        if self.auth_mode is AuthMode.ENABLED:
            whitelist_file = self.credential_store.whitelist_file
            logger.info(
                f"authentication enabled: auth_mode={self.auth_mode.value}, "
                f"zombie_policy={self.zombie_policy.value}, "
                f"operator_whitelist_file="
                f"{None if whitelist_file is None else str(whitelist_file)!r}"
            )
        else:
            logger.warning(
                f"authentication is disabled (auth_mode="
                f"{self.auth_mode.value}): any caller able to reach the served "
                f"port may run control commands such as requeue, suspend, force "
                f"and load, and may read the whole node tree; set auth_mode to "
                f"{AuthMode.ENABLED.value!r} to require operator credentials"
            )

    async def start(self):
        """
        Start services:

        * configure logging
        * check the umask that job script permissions will be derived from
        * validate the security configuration and report the security posture
        * restore the bunch from the Checkpoint_File
        * start scheduler
        * start network service
        * start the periodic snapshot task

        The order is the contract. The umask check reads the process umask with a
        set-and-restore pair, so it must run while the process is still
        single-threaded, i.e. before any service is started (Requirement 12.8).
        ``restore()`` runs synchronously before the scheduler is started, so the
        very first dependency resolution already sees the restored node tree
        (Requirement 6.1), and the periodic snapshot task is created last, so its
        first write cannot race the restore (Requirement 5.1).
        """
        # Configure logging before the first server record is emitted so that
        # startup, command, and shutdown activity is captured at the configured
        # level and destinations. Only the Audit_File is passed explicitly: the
        # rest is derived from environment variables and built-in defaults
        # (Requirements 10.1, 10.2), while the Audit_File may come from the
        # ``security`` section of the Connect_Config, which the logging
        # subsystem does not read (Requirements 11.1, 11.12).
        #
        # Configuration is guarded so that any failure does not abort server
        # startup: on failure we fall back to a console sink at INFO, emit a
        # WARNING describing the failure, and let startup proceed
        # (Requirements 10.3, 10.4).
        #
        # The Audit_File is passed only when one is configured, so that a server
        # without auditing calls ``configure()`` with no explicit argument at
        # all and the whole configuration keeps coming from the environment and
        # the defaults, exactly as in M1.
        configure_kwargs = {}
        if self.audit_file is not None:
            configure_kwargs["audit_file"] = self.audit_file
        try:
            configure(**configure_kwargs)
        except Exception as exc:  # noqa: BLE001 - never let logging abort startup
            try:
                configure(level="INFO", console=True)
            except Exception:  # noqa: BLE001 - keep the fallback itself robust
                # Even the fallback configuration failed; do not let startup
                # crash. The logging subsystem applies a default INFO-to-console
                # configuration lazily on first use, so logging still works.
                pass
            get_logger("server").warning(
                f"logging configuration failed, falling back to console at INFO: {exc}"
            )

        logger.info("start server...")

        # Reading the umask needs a set-and-restore pair (see ``_read_umask``),
        # which is only safe while the process is single-threaded. Hence this
        # position: after ``configure()`` so the WARNING reaches the configured
        # destinations, before ``restore()`` and before any of the three services
        # -- and therefore any worker thread or task -- exists (Requirement 12.8).
        self._check_job_script_umask()

        # Before ``restore()``: an unusable security configuration must abort the
        # start-up while nothing has been brought up yet, so the Server_CLI can
        # exit non-zero without a half-started server to shut down
        # (Requirement 1.6).
        self._start_security()

        # Restore before the scheduler main loop exists (Requirement 6.1).
        # ``restore()`` never raises: an unusable snapshot degrades to the backup
        # file and then to an empty bunch, so startup always proceeds.
        self.checkpoint_manager.restore()
        await self.scheduler.start()
        await self.network_service.start()
        await self.checkpoint_manager.start()
        logger.info("start server...done")

    async def run(self):
        """
        Run services:

        * run network service
        * run scheduler

        ``run()`` returns when either the scheduler task finishes (e.g. after a
        normal ``stop()``) or the shared fatal-error event fires (the
        ``FAIL_FAST`` path). In both cases it drives the same clean shutdown flow
        as :meth:`stop` before returning, so the two modes share one identical
        clean shutdown path (Requirements 2.4, 2.5, 3.4).
        """
        loop = asyncio.get_running_loop()
        loop.create_task(
            self.network_service.run(), name="takler.server.network_service"
        )

        scheduler_task = loop.create_task(
            self.scheduler.run(), name="takler.server.scheduler"
        )
        # ``asyncio.wait`` requires awaitables wrapped as tasks/futures, so wrap
        # the event wait in its own task and wait for whichever finishes first.
        fatal_task = asyncio.ensure_future(self._fatal_error_event.wait())

        try:
            await asyncio.wait(
                {scheduler_task, fatal_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            # Tidy up the fatal-error waiter if it did not complete (scheduler
            # finished first), so it does not linger as a pending task.
            if not fatal_task.done():
                fatal_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await fatal_task

        # Whichever branch woke us up, go through the shared clean shutdown flow.
        await self._shutdown()

    async def _shutdown(self):
        """
        Clean shutdown flow shared by :meth:`stop` and :meth:`run`.

        Stops the network service, the scheduler and the checkpoint manager
        without raising. Guarded by ``self._stopped`` so it runs at most once
        even when both an external ``stop()`` call and ``run()`` reach it
        (Requirement 3.4).

        The checkpoint manager is stopped last on purpose: its ``stop()`` writes
        the final snapshot, and by then no RPC handler and no scheduler pass can
        still change the bunch, so that snapshot is a consistent view of a
        quiesced tree (Requirement 5.9).
        """
        if self._stopped:
            return
        self._stopped = True
        logger.info("stop server...")
        await self.network_service.stop()
        await self.scheduler.stop()
        # Cancels the periodic task and writes the last snapshot; never raises.
        await self.checkpoint_manager.stop()
        logger.info("stop server...done")

    async def stop(self):
        """
        Stop all services:

        * stop network service
        * stop scheduler
        """
        # Record the server shutdown event at INFO level through the named
        # "server" logger, mirroring the start event (Requirement 10.7).
        await self._shutdown()


async def run_server_until_complete(server: TaklerServer, check_interval: int = 10):
    """
    Start and run takler server until all flows in bunch are complete.

    Parameters
    ----------
    server
    check_interval
        check interval seconds

    Examples
    --------
    Run a simple flow.

    >>> import asyncio
    >>> from takler.core import Flow
    >>> from takler.server import TaklerServer, run_server_until_complete
    >>> server = TaklerServer(host="login_a06", port=33083)
    >>> flow = Flow("flow1")
    >>> task1 = flow.add_task("task1")
    >>> server.bunch.add_flow(flow)
    >>> flow.begin()
    >>> asyncio.run(run_server_until_complete(server))

    """
    await start_server(server)

    await wait_server_until_complete(server, check_interval)

    await stop_server(server)


async def start_server(server: TaklerServer):
    """
    Start server, and run the server in current running loop.

    Parameters
    ----------
    server
        takler server
    """
    await server.start()
    loop = asyncio.get_running_loop()
    task = loop.create_task(server.run(), name="takler.server")
    return task


async def wait_server_until_complete(server: TaklerServer, check_interval: int = 10):
    """
    Loop check until all flows in bunch are complete.

    Parameters
    ----------
    server
        takler server with some flows.
    check_interval
        sleep seconds between checks.
    """
    while True:
        status = server.bunch.get_node_status()
        if status == NodeStatus.complete:
            break

        await asyncio.sleep(check_interval)


async def stop_server(server: TaklerServer, seconds_before_stop: int = 10):
    """
    Stop takler server.

    Parameters
    ----------
    server
        takler server.
    seconds_before_stop
        sleep seconds before stop the server.
    """
    logger.info(
        f"all flows are complete, about to exit, sleep for {seconds_before_stop} seconds..."
    )
    await asyncio.sleep(seconds_before_stop)
    logger.info("stop server...")
    await server.stop()
    logger.info("stop server...done")
