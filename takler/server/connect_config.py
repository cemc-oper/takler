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


class ConnectConfig(BaseModel):
    """Content of the ``connect.yaml`` file shared by server and clients.

    The ``checkpoint`` section carries a default value, so a legacy
    ``connect.yaml`` holding only the ``server`` section is still loadable and
    yields an all-``None`` :class:`CheckpointSettings`; newly written files
    gain an extra ``checkpoint`` section (Requirement 7.1).
    """

    server: Server
    checkpoint: CheckpointSettings = CheckpointSettings()


def generate_connect_config() -> ConnectConfig:
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
    sock.bind(('', 0))
    return sock.getsockname()[1]
