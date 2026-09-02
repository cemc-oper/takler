"""Credential and TLS path resolution for the client side.

This module answers three questions for :mod:`takler.client.service_client`,
deliberately kept out of it so that the RPC plumbing stays about RPCs:

* *where* the client's TLS and credential inputs live -- the CA certificate
  file, the certificate host name override and the Operator_Secret_File
  (:func:`resolve_ca_file`, :func:`resolve_server_name`,
  :func:`resolve_secret_file`),
* *what* the Operator_Secret_File carries (:func:`read_first_secret`),
* *who* the caller is (:func:`current_user_name`), and under which metadata
  keys the three credentials travel (:data:`METADATA_KEY_JOB_PASSWORD` and its
  two siblings).

The three ``resolve_*`` functions share the shape of the ``resolve_*`` family
in :mod:`takler.server.connect_config`: four precedence levels, from an
explicit argument down to "not configured", with a blank value counting as
absent at every level. The server side family resolves the knobs a server
reads; this one resolves the knobs a client reads, which is why the two live in
different modules while sharing the same signature ``(explicit,
connect_config, env)``.

The three metadata key names are re-exported from :mod:`takler.server.auth`
rather than spelled out again here. A second copy of ``"takler-pass"`` would not
fail loudly if it drifted from the server's: the interceptor would simply see a
call carrying no credentials. :mod:`takler.server.auth` depends on nothing but
the standard library and :mod:`takler.logging`, and the client already reads
:mod:`takler.server.connect_config` and the generated protocol modules, so the
import direction is the established one in this package rather than a new one.

The environment variable names, the metadata key names, the comment marker, the
"first valid line" rule and the user name fallback order are part of the
cross-language contract shared with the Go client's ``common/tlsconfig.go`` and
``common/credentials.go``; any change here must be applied there as well.

Requirements: 2.3, 2.5, 8.2, 8.4, 8.5, 8.6, 8.8, 12.7.
"""

from __future__ import annotations

import getpass
import os
from pathlib import Path
from typing import Mapping, Optional, Tuple, Union

from takler.logging import get_logger
from takler.server.auth import (
    METADATA_KEY_JOB_PASSWORD,
    METADATA_KEY_SECRET,
    METADATA_KEY_USER,
)
from takler.server.connect_config import ConnectConfig

__all__ = [
    "ENV_JOB_PASSWORD",
    "ENV_TLS_CA_FILE",
    "ENV_TLS_SERVER_NAME",
    "ENV_SECRET_FILE",
    "COMMENT_PREFIX",
    "USER_NAME_ENV_NAMES",
    "METADATA_KEY_JOB_PASSWORD",
    "METADATA_KEY_SECRET",
    "METADATA_KEY_USER",
    "current_user_name",
    "resolve_ca_file",
    "resolve_server_name",
    "resolve_secret_file",
    "read_first_secret",
]

logger = get_logger("client")


#: Environment variable holding the Job_Password of the running job
#: (Requirement 8.2).
#:
#: The name is the same as the ``TAKLER_PASS`` generated parameter of
#: :mod:`takler.core.parameter`, and that is the whole mechanism: the job script
#: exports the generated parameter, and the client of a Child_Command reads it
#: back out of its own environment.
ENV_JOB_PASSWORD: str = "TAKLER_PASS"

#: Environment variable holding the CA certificate file the client trusts as
#: its root of trust (Requirement 2.3).
ENV_TLS_CA_FILE: str = "TAKLER_TLS_CA_FILE"

#: Environment variable holding the name to verify the server certificate's
#: host name against, for the case where the certificate's CN/SAN differs from
#: the host name the client connects to (Requirement 2.5).
ENV_TLS_SERVER_NAME: str = "TAKLER_TLS_SERVER_NAME"

#: Environment variable holding the Operator_Secret_File path the client reads
#: its shared secret from (Requirement 8.6).
ENV_SECRET_FILE: str = "TAKLER_SECRET_FILE"

#: Line prefix that marks a comment in the Operator_Secret_File. The marker is
#: tested against the stripped line, so an indented comment is a comment while
#: a ``#`` inside a secret stays part of the secret (Requirement 8.5).
COMMENT_PREFIX: str = "#"


def _is_blank(value: Optional[str]) -> bool:
    """Return ``True`` when a configured value counts as "not provided".

    Empty and whitespace-only strings are treated as absent at *every*
    precedence level, so an exported-but-empty ``TAKLER_TLS_CA_FILE`` or a
    ``connect.yaml`` holding ``ca_file: ""`` falls through to the next level
    instead of resolving to a nameless path. This mirrors the same helper in
    :mod:`takler.server.connect_config`, :mod:`takler.server.auth` and the Go
    client's ``isBlank``.
    """
    return value is None or value.strip() == ""


#: Environment variables naming the current user, consulted in this order when
#: :func:`getpass.getuser` cannot answer. Same list and same order as the Go
#: client's ``osUsername``, so both clients send the same ``takler-user``.
USER_NAME_ENV_NAMES: Tuple[str, str] = ("LOGNAME", "USER")


def current_user_name() -> Optional[str]:
    """Return the OS user name of the current process (Requirement 8.4).

    :func:`getpass.getuser` is the primary source: it consults ``LOGNAME``,
    ``USER``, ``LNAME`` and ``USERNAME`` before falling back to the process
    owner's passwd entry. It *raises* when neither source answers -- a process
    running under a uid with no passwd entry and no name in the environment,
    which is the normal state of a container started with ``--user $(id -u)``.

    That exception is caught rather than propagated, and the two environment
    variables the Go client's ``osUsername`` falls back to are tried next.
    Reason: ``takler-user`` is one metadata key among three, and a missing user
    name must not take a command down before it reaches the server. Sending the
    call without the key leaves the decision to the server, which is what the
    rest of the credential path does for every absent credential (Requirements
    8.3, 8.7).

    Returns:
        The user name with surrounding whitespace removed, or ``None`` when no
        source names the user, in which case the caller omits ``takler-user``.
    """
    try:
        name = getpass.getuser()
    except (KeyError, OSError, ImportError):
        # KeyError: no passwd entry (Python < 3.13). OSError: the same, as
        # raised since 3.13. ImportError: no ``pwd`` module, i.e. Windows.
        name = None

    if not _is_blank(name):
        return name.strip()

    for env_name in USER_NAME_ENV_NAMES:
        value = os.environ.get(env_name)
        if not _is_blank(value):
            return value.strip()

    return None


def _security_setting(
    connect_config: Optional[ConnectConfig], field: str
) -> Optional[str]:
    """Read one field of a Connect_Config ``security`` section.

    Args:
        connect_config: A loaded Connect_Config, or ``None`` when no config
            file is in play.
        field: Name of the
            :class:`~takler.server.connect_config.SecuritySettings` field to
            read.

    Returns:
        The field value with surrounding whitespace removed, or ``None`` when
        it is not provided. A missing Connect_Config, a field left unset and a
        blank field are all reported the same way, since all three mean the
        next precedence source applies.
    """
    if connect_config is None:
        return None

    value = getattr(connect_config.security, field)
    if _is_blank(value):
        return None

    return value.strip()


def _resolve_path(
    explicit: Optional[str],
    env_name: str,
    connect_config: Optional[ConnectConfig],
    field: str,
    env: Optional[Mapping[str, str]],
) -> Optional[str]:
    """Resolve one client side path or name across its four precedence levels.

    Shared body of the three ``resolve_*`` functions: explicit argument >
    environment variable ``env_name`` > the ``field`` field of the
    Connect_Config ``security`` section > not configured. Every level is
    trimmed of surrounding whitespace, and a blank value at any level lets the
    next one take effect.

    Args:
        explicit: An explicitly supplied value (command line option or
            constructor argument).
        env_name: Name of the environment variable holding this value.
        connect_config: A loaded Connect_Config whose ``security`` section is
            consulted, or ``None`` when no config file is in play.
        field: Name of the ``SecuritySettings`` field holding this value.
        env: A mapping of environment variables (defaults to ``os.environ``).

    Returns:
        The resolved value, or ``None`` when no source provides one.
    """
    if not _is_blank(explicit):
        return explicit.strip()

    if env is None:
        env = os.environ

    env_value = env.get(env_name)
    if not _is_blank(env_value):
        return env_value.strip()

    return _security_setting(connect_config, field)


def resolve_ca_file(
    explicit: Optional[str] = None,
    connect_config: Optional[ConnectConfig] = None,
    env: Optional[Mapping[str, str]] = None,
) -> Optional[str]:
    """Resolve the CA certificate file the client trusts.

    Applies per-source precedence -- explicit argument > ``TAKLER_TLS_CA_FILE``
    environment variable > the ``ca_file`` field of the Connect_Config
    ``security`` section > no CA certificate (Requirement 2.3). An absent value
    at any level (``None``, empty or whitespace-only) lets the next source take
    effect.

    Args:
        explicit: An explicitly supplied CA certificate path.
        connect_config: A loaded Connect_Config whose ``security`` section is
            consulted, or ``None`` when no config file is in play.
        env: A mapping of environment variables (defaults to ``os.environ``).
            Only ``TAKLER_TLS_CA_FILE`` is consulted.

    Returns:
        The resolved CA certificate path, or ``None`` when no source provides
        one, in which case the client connects unencrypted, just as in M1
        (Requirement 2.2).
    """
    return _resolve_path(explicit, ENV_TLS_CA_FILE, connect_config, "ca_file", env)


def resolve_server_name(
    explicit: Optional[str] = None,
    connect_config: Optional[ConnectConfig] = None,
    env: Optional[Mapping[str, str]] = None,
) -> Optional[str]:
    """Resolve the certificate host name override of a TLS connection.

    Applies per-source precedence -- explicit argument >
    ``TAKLER_TLS_SERVER_NAME`` environment variable > the ``server_name`` field
    of the Connect_Config ``security`` section > no override (Requirement 2.5).
    An absent value at any level (``None``, empty or whitespace-only) lets the
    next source take effect.

    Args:
        explicit: An explicitly supplied host name override.
        connect_config: A loaded Connect_Config whose ``security`` section is
            consulted, or ``None`` when no config file is in play.
        env: A mapping of environment variables (defaults to ``os.environ``).
            Only ``TAKLER_TLS_SERVER_NAME`` is consulted.

    Returns:
        The resolved host name override, or ``None`` when no source provides
        one, in which case the host name the client connects to is verified as
        is (Requirement 2.4).
    """
    return _resolve_path(
        explicit, ENV_TLS_SERVER_NAME, connect_config, "server_name", env
    )


def resolve_secret_file(
    explicit: Optional[str] = None,
    connect_config: Optional[ConnectConfig] = None,
    env: Optional[Mapping[str, str]] = None,
) -> Optional[str]:
    """Resolve the Operator_Secret_File path the client reads.

    Applies per-source precedence -- explicit argument > ``TAKLER_SECRET_FILE``
    environment variable > the ``operator_secret_file`` field of the
    Connect_Config ``security`` section > no shared secret (Requirement 8.6).
    An absent value at any level (``None``, empty or whitespace-only) lets the
    next source take effect.

    The field is the same one the server reads its Operator_Secret_Set from, so
    on a host where operator and server share a ``connect.yaml`` no extra
    client configuration is needed.

    Args:
        explicit: An explicitly supplied Operator_Secret_File path.
        connect_config: A loaded Connect_Config whose ``security`` section is
            consulted, or ``None`` when no config file is in play.
        env: A mapping of environment variables (defaults to ``os.environ``).
            Only ``TAKLER_SECRET_FILE`` is consulted.

    Returns:
        The resolved Operator_Secret_File path, or ``None`` when no source
        provides one, in which case the client carries no ``takler-secret`` and
        the server decides whether to refuse the call (Requirement 8.7).
    """
    return _resolve_path(
        explicit, ENV_SECRET_FILE, connect_config, "operator_secret_file", env
    )


def _first_secret_line(text: str) -> Optional[str]:
    """Return the first line of a secret file's content that carries a value.

    A value is taken from the first line that is neither blank nor a comment,
    with the surrounding whitespace removed (Requirement 8.5). ``None`` means
    the content holds no such line.

    A client sends a single value while the server accepts the whole set of
    values the file carries: that asymmetry is the no-downtime rotation
    mechanism (Requirement 7.12). A new secret is appended at the *top* of the
    file, the clients pick it up one by one, and the old secret is removed
    afterwards -- during which both values verify on the server.

    Args:
        text: The full content of an Operator_Secret_File.

    Returns:
        The first Operator_Secret the file carries, or ``None``.
    """
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith(COMMENT_PREFIX):
            continue
        return stripped
    return None


def read_first_secret(path: Optional[Union[str, Path]]) -> Optional[str]:
    """Read the Operator_Secret a client sends from an Operator_Secret_File.

    Returns the first line that is neither blank nor a comment, with the
    surrounding whitespace removed (Requirement 8.5).

    Every failure is reported the same way -- ``None`` plus one WARNING naming
    the path and the reason -- and none of them raises (Requirement 8.8): an
    unreadable file, a file that is not text, and a file holding nothing but
    blanks and comments all leave the caller to send the call without
    ``takler-secret`` and let the server decide. Failing in the client instead
    would mean an operator who happens to point ``TAKLER_SECRET_FILE`` at a
    stale path could not talk to an ``Auth_Mode=disabled`` server at all.

    No log line carries the file's content (Requirement 12.7). That is also why
    a decoding failure is reported as "not valid UTF-8 text" rather than
    through the :class:`UnicodeDecodeError` message, which quotes the offending
    byte of the file.

    Args:
        path: The resolved Operator_Secret_File path, typically from
            :func:`resolve_secret_file`. ``None`` or blank means no secret file
            is configured, which is not a failure and draws no WARNING
            (Requirement 8.7).

    Returns:
        The Operator_Secret to send, or ``None`` when there is none to send.
    """
    if path is None:
        return None

    text_path = str(path)
    if _is_blank(text_path):
        return None

    file_path = Path(text_path.strip())
    try:
        content = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        logger.warning(
            f"Operator_Secret_File {str(file_path)!r} is not valid UTF-8 text; "
            f"sending the request without an operator secret."
        )
        return None
    except OSError as exc:
        logger.warning(
            f"Cannot read Operator_Secret_File {str(file_path)!r}: "
            f"{type(exc).__name__}: {exc.strerror or exc}; "
            f"sending the request without an operator secret."
        )
        return None

    secret = _first_secret_line(content)
    if secret is None:
        logger.warning(
            f"Operator_Secret_File {str(file_path)!r} holds no secret: "
            f"every line is blank or a comment; "
            f"sending the request without an operator secret."
        )
    return secret
