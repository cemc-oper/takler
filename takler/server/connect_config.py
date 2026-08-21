import enum
import os
import socket
from pathlib import Path
from typing import Mapping, Optional, Union

import yaml
from pydantic import BaseModel

from takler.logging import get_logger


logger = get_logger("server.config")


# Environment variable names recognized by the server configuration.
TAKLER_CONNECT_FILE = "TAKLER_CONNECT_FILE"
TAKLER_EXCEPTION_POLICY = "TAKLER_EXCEPTION_POLICY"
# Security related environment variables (Requirement 3.7).
TAKLER_AUTH_MODE = "TAKLER_AUTH_MODE"
TAKLER_ZOMBIE_POLICY = "TAKLER_ZOMBIE_POLICY"
TAKLER_AUDIT_FILE = "TAKLER_AUDIT_FILE"


class ExceptionPolicy(enum.Enum):
    """How the server handles unexpected exceptions at its boundaries.

    The policy governs both the :class:`~takler.server.scheduler.Scheduler`
    main loop and the gRPC ``TaklerService`` command/query handlers:

    * :attr:`RESILIENT` (default): catch, log and recover from unexpected
      exceptions so the server process keeps running (skip the offending flow
      / return an error response).
    * :attr:`FAIL_FAST`: log the exception first, then let the server process
      exit cleanly. This restores the pre-fix legacy behaviour, but as an
      explicit opt-in rather than the default.

    ``RESILIENT`` is the default; ``FAIL_FAST`` is only ever selected when a
    caller explicitly opts in (Requirement 2.6).
    """

    RESILIENT = "resilient"
    FAIL_FAST = "fail_fast"

    @classmethod
    def from_str(cls, value: "Union[str, ExceptionPolicy]") -> "ExceptionPolicy":
        """Parse a policy name into an :class:`ExceptionPolicy`.

        Name matching is case-insensitive and tolerates ``-`` in place of
        ``_`` (so ``"fail-fast"``, ``"FAIL_FAST"`` and ``"fail_fast"`` all
        resolve to :attr:`FAIL_FAST`). Surrounding whitespace is ignored. An
        :class:`ExceptionPolicy` value is returned unchanged for convenience.

        Unlike a strict parser, an unrecognized (or blank/non-string) value
        does not raise: it degrades gracefully to the default
        :attr:`RESILIENT` and emits a WARNING identifying the offending value,
        so a misconfigured policy can never make the server less resilient
        (Requirement 2.6).

        Args:
            value: A recognized policy name (any letter case) or an existing
                :class:`ExceptionPolicy`.

        Returns:
            The matching :class:`ExceptionPolicy` member, or
            :attr:`RESILIENT` when ``value`` is not recognized.
        """
        if isinstance(value, ExceptionPolicy):
            return value

        if isinstance(value, str):
            normalized = value.strip().upper().replace("-", "_")
            member = cls.__members__.get(normalized)
            if member is not None:
                return member

        logger.warning(
            f"Invalid {TAKLER_EXCEPTION_POLICY} value {value!r}; "
            f"falling back to {DEFAULT_EXCEPTION_POLICY.name}."
        )
        return DEFAULT_EXCEPTION_POLICY


# Built-in default applied when neither an explicit argument nor an
# environment variable provides a value (Requirement 2.6).
DEFAULT_EXCEPTION_POLICY = ExceptionPolicy.RESILIENT


def _is_blank(value: Optional[str]) -> bool:
    """Return ``True`` when an environment value counts as "not provided".

    Empty strings and whitespace-only strings are treated as absent so that a
    blank environment variable falls back to the built-in default.
    """
    return value is None or value.strip() == ""


def resolve_exception_policy(
    explicit: "Optional[Union[str, ExceptionPolicy]]" = None,
    env: Optional[Mapping[str, str]] = None,
) -> ExceptionPolicy:
    """Resolve the effective :class:`ExceptionPolicy`.

    Applies per-source precedence -- explicit argument > ``TAKLER_EXCEPTION_POLICY``
    environment variable > built-in default :attr:`ExceptionPolicy.RESILIENT`
    (Requirement 2.6). A missing explicit argument (``None``) lets the
    environment value take effect; a blank/whitespace-only environment value is
    treated as absent so the default applies.

    Both the explicit argument and the environment value are parsed with
    :meth:`ExceptionPolicy.from_str`, so an unrecognized value degrades to
    :attr:`ExceptionPolicy.RESILIENT` with a WARNING rather than raising.

    Args:
        explicit: An explicitly supplied policy (constructor argument), either
            an :class:`ExceptionPolicy` or its name. ``None`` means "not
            provided" so the next precedence source applies.
        env: A mapping of environment variables (defaults to ``os.environ``).
            Only ``TAKLER_EXCEPTION_POLICY`` is consulted.

    Returns:
        The fully resolved :class:`ExceptionPolicy`.
    """
    if explicit is not None:
        return ExceptionPolicy.from_str(explicit)

    if env is None:
        env = os.environ

    env_value = env.get(TAKLER_EXCEPTION_POLICY)
    if _is_blank(env_value):
        return DEFAULT_EXCEPTION_POLICY

    return ExceptionPolicy.from_str(env_value)


class AuthMode(enum.Enum):
    """Whether the server authenticates the callers of its RPCs.

    * :attr:`DISABLED` (default): the Auth_Interceptor lets every RPC through
      without looking at the Credential_Metadata, which keeps an M1 deployment
      working unchanged after an upgrade.
    * :attr:`ENABLED`: a Child_Command needs a job password and an
      Operator_Command needs the shared operator secret plus a whitelisted OS
      user name.

    :attr:`DISABLED` is the built-in default (Requirement 3.6); enabling
    authentication is always an explicit opt-in.
    """

    DISABLED = "disabled"
    ENABLED = "enabled"

    @classmethod
    def from_str(cls, value: "Union[str, AuthMode]") -> "AuthMode":
        """Parse an Auth_Mode name into an :class:`AuthMode`.

        Name matching follows :meth:`ExceptionPolicy.from_str`: it is
        case-insensitive, tolerates ``-`` in place of ``_`` and ignores
        surrounding whitespace. An :class:`AuthMode` value is returned
        unchanged for convenience.

        An unrecognized (or blank/non-string) value does not raise: it degrades
        to the default :attr:`DISABLED` and emits a WARNING naming the
        offending value (Requirement 3.8). Degrading is safe here -- unlike a
        half-configured TLS setup -- because a server running with
        :attr:`DISABLED` immediately logs the Requirement 3.12 WARNING stating
        that any caller able to reach the port may run a Control_Command, so
        the effective posture is never silently misread.

        Args:
            value: A recognized Auth_Mode name (any letter case), coming from
                either ``TAKLER_AUTH_MODE`` or the ``security`` section of a
                Connect_Config file, or an existing :class:`AuthMode`.

        Returns:
            The matching :class:`AuthMode` member, or :attr:`DISABLED` when
            ``value`` is not recognized.
        """
        if isinstance(value, AuthMode):
            return value

        if isinstance(value, str):
            normalized = value.strip().upper().replace("-", "_")
            member = cls.__members__.get(normalized)
            if member is not None:
                return member

        logger.warning(
            f"Invalid {TAKLER_AUTH_MODE} value {value!r}; "
            f"falling back to {DEFAULT_AUTH_MODE.name}."
        )
        return DEFAULT_AUTH_MODE


# Built-in default applied when neither an explicit argument, an environment
# variable nor a Connect_Config file provides a value (Requirement 3.6).
DEFAULT_AUTH_MODE = AuthMode.DISABLED


class ZombiePolicy(enum.Enum):
    """How the server handles a Child_Command that hits a Zombie_Condition.

    The policy is server-global and applies to every zombie, whichever
    condition it hit:

    * :attr:`FAIL` (default): leave the target task untouched and answer with
      ``ZombieError``, so the stale job sees a failure.
    * :attr:`FOB`: leave the target task untouched but answer success, so the
      stale job goes on quietly.
    * :attr:`ADOPT`: run the command anyway, adopting the credentials it
      carries.

    :attr:`FAIL` is the built-in default (Requirement 3.6): it is the only one
    of the three that neither hides the zombie nor lets it write to the current
    run of the task.
    """

    FAIL = "fail"
    FOB = "fob"
    ADOPT = "adopt"

    @classmethod
    def from_str(cls, value: "Union[str, ZombiePolicy]") -> "ZombiePolicy":
        """Parse a Zombie_Policy name into a :class:`ZombiePolicy`.

        Name matching follows :meth:`ExceptionPolicy.from_str`: it is
        case-insensitive, tolerates ``-`` in place of ``_`` and ignores
        surrounding whitespace. A :class:`ZombiePolicy` value is returned
        unchanged for convenience.

        An unrecognized (or blank/non-string) value does not raise: it degrades
        to the default :attr:`FAIL` and emits a WARNING naming the offending
        value (Requirement 3.9). Degrading to :attr:`FAIL` cannot weaken the
        server, since it is the strictest of the three policies.

        Args:
            value: A recognized Zombie_Policy name (any letter case), coming
                from either ``TAKLER_ZOMBIE_POLICY`` or the ``security``
                section of a Connect_Config file, or an existing
                :class:`ZombiePolicy`.

        Returns:
            The matching :class:`ZombiePolicy` member, or :attr:`FAIL` when
            ``value`` is not recognized.
        """
        if isinstance(value, ZombiePolicy):
            return value

        if isinstance(value, str):
            normalized = value.strip().upper().replace("-", "_")
            member = cls.__members__.get(normalized)
            if member is not None:
                return member

        logger.warning(
            f"Invalid {TAKLER_ZOMBIE_POLICY} value {value!r}; "
            f"falling back to {DEFAULT_ZOMBIE_POLICY.name}."
        )
        return DEFAULT_ZOMBIE_POLICY


# Built-in default applied when neither an explicit argument, an environment
# variable nor a Connect_Config file provides a value (Requirement 3.6).
DEFAULT_ZOMBIE_POLICY = ZombiePolicy.FAIL


class Address(BaseModel):
    hostname: str
    ip: str
    port: str


class Server(BaseModel):
    address: Address


class CheckpointSettings(BaseModel):
    """Checkpoint related settings of the :class:`ConnectConfig` file.

    Both fields are optional and default to ``None``, which means "not
    configured": the ``Checkpoint_Manager`` then falls back to its built-in
    defaults (120 seconds and ``takler.check`` in the current working
    directory). Keeping ``None`` as the "absent" marker -- instead of baking
    the defaults into the model -- lets the manager apply the config-source
    precedence ``explicit argument > Connect_Config file > built-in default``
    (Requirements 7.1, 7.2, 7.3, 7.5).

    Attributes:
        interval: Snapshot period in seconds.
        file: Checkpoint_File path.
    """

    interval: Optional[float] = None
    file: Optional[str] = None


class SecuritySettings(BaseModel):
    """The ``security`` section of the :class:`ConnectConfig` file.

    The section gathers the four groups of security knobs of a deployment --
    TLS, authentication, zombie handling and auditing -- so that a single
    ``connect.yaml`` describes the complete security posture of a server
    (Requirements 3.1, 3.3).

    Every field is optional and defaults to ``None``, which means "not
    configured". As with :class:`CheckpointSettings`, keeping ``None`` as the
    "absent" marker -- instead of baking the built-in defaults into the model
    -- is what lets the ``resolve_*`` function family apply the config-source
    precedence ``explicit argument > environment variable > Connect_Config
    file > built-in default`` (Requirements 3.4, 3.5).

    Attributes:
        server_cert_file: Server certificate path. Enables TLS together with
            ``server_key_file``; both unset means plaintext (Requirement 1.1).
        server_key_file: Server private key path (Requirement 1.1).
        client_ca_file: CA certificate used to verify client certificates.
            Reserved for mTLS: this version only warns and never verifies
            client certificates (Requirements 1.8, 1.9).
        ca_file: CA certificate a client trusts when connecting
            (Requirement 2.1).
        server_name: Certificate hostname override used by a client
            (Requirement 2.4).
        auth_mode: Auth_Mode name, ``disabled`` (built-in default) or
            ``enabled`` (Requirements 3.3, 3.6).
        operator_secret_file: Operator_Secret_File path (Requirement 7.1).
        operator_whitelist_file: Operator_Whitelist_File path
            (Requirement 7.2).
        zombie_policy: Zombie_Policy name, ``fail`` (built-in default),
            ``fob`` or ``adopt`` (Requirements 3.3, 3.6).
        audit_file: Audit_File path (Requirement 11.12).
    """

    # TLS (Requirements 1.1, 1.8, 2.1, 2.4)
    server_cert_file: Optional[str] = None
    server_key_file: Optional[str] = None
    client_ca_file: Optional[str] = None
    ca_file: Optional[str] = None
    server_name: Optional[str] = None

    # Authentication (Requirements 3.3, 7.1, 7.2)
    auth_mode: Optional[str] = None
    operator_secret_file: Optional[str] = None
    operator_whitelist_file: Optional[str] = None

    # Zombie handling and auditing (Requirements 3.3, 10.1, 11.12)
    zombie_policy: Optional[str] = None
    audit_file: Optional[str] = None


class ConnectConfig(BaseModel):
    """Content of the ``connect.yaml`` file shared by server and clients.

    The ``checkpoint`` and ``security`` sections both carry a default value, so
    a legacy ``connect.yaml`` holding only the ``server`` section -- or only
    the ``server`` and ``checkpoint`` sections -- is still loadable and yields
    an all-``None`` :class:`CheckpointSettings` / :class:`SecuritySettings`;
    newly written files gain both sections (Requirements 7.1, 3.2, 3.10).
    """

    server: Server
    checkpoint: CheckpointSettings = CheckpointSettings()
    security: SecuritySettings = SecuritySettings()


def generate_connect_config() -> ConnectConfig:
    """Build a fresh :class:`ConnectConfig` for the current host.

    The ``checkpoint`` and ``security`` sections are filled with their
    all-``None`` defaults, so a newly generated file documents both sections
    and every configurable knob in them (Requirements 7.1, 3.10).

    Returns:
        A :class:`ConnectConfig` holding this host's name, IP and an available
        port.
    """
    hostname = socket.gethostname()
    ip = get_ip()
    port = str(get_port())

    c = ConnectConfig(
        server=Server(
            address=Address(
                hostname=hostname,
                ip=ip,
                port=port,
            )
        )
    )
    c.dict()
    return c


def save_connect_config(config: ConnectConfig, file_path: Union[str, Path]):
    d = config.model_dump()
    with open(file_path, "w") as f:
        yaml.safe_dump(d, f)


def load_connect_config(file_path: Union[str, Path]) -> ConnectConfig:
    with open(file_path, "r") as f:
        d = yaml.safe_load(f)
        c = ConnectConfig(**d)
        return c


def get_ip() -> str:
    """
    get ip address

    References
    -----------
    https://stackoverflow.com/questions/24196932/how-can-i-get-the-ip-address-from-a-nic-network-interface-controller-in-python

    Returns
    -------
    str
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]


def get_port():
    """
    get an available port.

    References
    ----------
    https://stackoverflow.com/questions/1365265/on-localhost-how-do-i-pick-a-free-port-number

    Returns
    -------
    int
    """
    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]
