"""Server side TLS credentials for the Takler server.

This module does one thing: turn the TLS part of the Security_Settings (plus the
two command line options that override it) into a
:class:`grpc.ServerCredentials`, or state unambiguously that TLS is not
configured by returning ``None``. Binding the port is the caller's job -- the
Network_Service picks ``add_secure_port`` or ``add_insecure_port`` from that
return value -- which keeps every decision about certificates in one testable
function that needs no gRPC server to exercise.

The guiding rule is that a misconfiguration must stop the start-up rather than
degrade quietly (Requirements 1.4, 1.5): an operator who asked for TLS and
silently got a plaintext port is worse off than one whose server refused to
start, because nothing on either side of the wire would report the difference.

Requirements: 1.1, 1.4, 1.5, 1.8, 1.9.
"""

from __future__ import annotations

import os
import ssl
from pathlib import Path
from typing import NoReturn, Optional, Union

import grpc

from takler.exceptions import SecurityConfigError
from takler.logging import get_logger
from takler.server.connect_config import SecuritySettings


logger = get_logger("server.tls")


__all__ = [
    "CERT_OPTION_NAME",
    "CERT_SETTING_NAME",
    "CLIENT_CA_SETTING_NAME",
    "KEY_OPTION_NAME",
    "KEY_SETTING_NAME",
    "build_server_credentials",
]


#: Names of the two Security_Settings fields carrying the TLS pair, and of the
#: Server_CLI options overriding them. They are spelled out as constants
#: because they are the entire content of the half-configuration message: an
#: operator who set only one of the two has to learn from that single line which
#: knob was seen and which one is missing, since the process exits right after
#: it (Requirement 1.4).
CERT_SETTING_NAME: str = "server_cert_file"
KEY_SETTING_NAME: str = "server_key_file"
CERT_OPTION_NAME: str = "--tls-cert"
KEY_OPTION_NAME: str = "--tls-key"

#: Name of the mTLS extension point, used in the Requirement 1.9 warning.
CLIENT_CA_SETTING_NAME: str = "client_ca_file"


def _is_blank(value: Optional[str]) -> bool:
    """Return ``True`` when a configured path counts as "not provided".

    Empty and whitespace-only strings are treated as absent, so a
    ``connect.yaml`` holding ``server_cert_file: ""`` reads as "no certificate
    configured" instead of resolving to a nameless path. This mirrors the same
    helper in :mod:`takler.server.connect_config`,
    :mod:`takler.server.checkpoint` and :mod:`takler.server.auth`.
    """
    return value is None or value.strip() == ""


def _resolve_path(
    explicit: "Optional[Union[str, Path]]",
    configured: Optional[str],
) -> Optional[str]:
    """Pick the effective path of one half of the TLS pair.

    Precedence is explicit argument (the Server_CLI option) over the
    Connect_Config ``security`` section (Requirement 1.10). Blankness is
    checked at both levels, so ``--tls-cert ""`` does not shadow a path coming
    from the config file.

    Args:
        explicit: The command line value, as :class:`str` or :class:`Path`
            (typer hands over a ``Path``).
        configured: The value of the matching Security_Settings field.

    Returns:
        The effective path with surrounding whitespace removed, or ``None``
        when neither source provides one.
    """
    if explicit is not None:
        text = os.fspath(explicit)
        if not _is_blank(text):
            return text.strip()

    if not _is_blank(configured):
        return configured.strip()

    return None


def _fatal(message: str, cause: Optional[BaseException] = None) -> NoReturn:
    """Log a security configuration failure as ERROR and raise it.

    Every rejection in this module goes through here, so the ERROR log and the
    exception text are the same string by construction (Requirements 1.4, 1.5).
    Logging in addition to raising is not redundant: the Server_CLI writes the
    exception text to standard error, which nobody reads once the server runs
    under a supervisor, while the log file is where a failed start-up is
    diagnosed afterwards.

    Args:
        message: The failure description, naming the offending path(s) and the
            reason.
        cause: The underlying exception, chained onto the raised error when
            there is one.

    Raises:
        SecurityConfigError: Always.
    """
    logger.error(message)
    raise SecurityConfigError(message) from cause


def _read_pem_file(path: str, description: str) -> bytes:
    """Read one PEM file, turning any read failure into a fatal config error.

    Args:
        path: The configured file path.
        description: Human readable name of the file, for example
            ``"certificate file"``.

    Returns:
        The raw file content, ready to hand to
        :func:`grpc.ssl_server_credentials`.

    Raises:
        SecurityConfigError: The file does not exist, is not readable, or is
            empty. The message carries the path and the reason
            (Requirement 1.5).
    """
    try:
        content = Path(path).read_bytes()
    except OSError as exc:
        _fatal(
            f"cannot read TLS {description} {path!r}: {type(exc).__name__}: {exc}",
            exc,
        )

    if not content.strip():
        _fatal(f"TLS {description} {path!r} is empty")

    return content


def _validate_pair(cert_path: str, key_path: str) -> None:
    """Check that the two files really are a usable certificate/key pair.

    :func:`grpc.ssl_server_credentials` accepts arbitrary bytes without looking
    at them: an unparseable certificate, a private key that belongs to a
    different certificate, or the two paths swapped all build a credentials
    object happily and only fail later, per connection, as a handshake error
    with no server side log naming the file. Requirement 1.5 asks for the
    opposite, so the pair is parsed here first.

    The parser is :meth:`ssl.SSLContext.load_cert_chain` from the standard
    library, which is OpenSSL doing the real thing -- no extra dependency, and
    the same PEM reader gRPC itself ends up using. It also verifies that the
    key matches the certificate, which is worth having at start-up for the same
    reason: a mismatched pair is a configuration mistake whose only other
    symptom is every client failing to connect.

    ``password`` is a callback returning an empty passphrase so that an
    encrypted private key fails right here with a reason instead of making
    OpenSSL prompt on the terminal, which would hang a server started under a
    tty.

    Args:
        cert_path: Path of the certificate file.
        key_path: Path of the private key file.

    Raises:
        SecurityConfigError: Either file cannot be parsed, or the key does not
            match the certificate. The message carries both paths and the
            reason reported by OpenSSL (Requirement 1.5).
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        context.load_cert_chain(
            certfile=cert_path, keyfile=key_path, password=lambda: b""
        )
    except (ssl.SSLError, OSError, ValueError) as exc:
        _fatal(
            f"cannot use TLS certificate file {cert_path!r} with private key "
            f"file {key_path!r}: {type(exc).__name__}: {exc}",
            exc,
        )


def _raise_half_configured(
    cert_path: Optional[str], key_path: Optional[str]
) -> NoReturn:
    """Reject a TLS pair with exactly one half configured (Requirement 1.4).

    Args:
        cert_path: The effective certificate path, exactly one of which and
            ``key_path`` is ``None``.
        key_path: The effective private key path.

    Raises:
        SecurityConfigError: Always.
    """
    if cert_path is not None:
        given_name, given_path = CERT_SETTING_NAME, cert_path
        given_option, missing_name = CERT_OPTION_NAME, KEY_SETTING_NAME
        missing_option = KEY_OPTION_NAME
    else:
        given_name, given_path = KEY_SETTING_NAME, key_path
        given_option, missing_name = KEY_OPTION_NAME, CERT_SETTING_NAME
        missing_option = CERT_OPTION_NAME

    _fatal(
        f"TLS is half configured: {given_name} ({given_option}) is set to "
        f"{given_path!r} but {missing_name} ({missing_option}) is not "
        f"configured; configure both to enable TLS, or neither to serve "
        f"plaintext"
    )


def build_server_credentials(
    settings: Optional[SecuritySettings],
    cert_file: "Optional[Union[str, Path]]" = None,
    key_file: "Optional[Union[str, Path]]" = None,
) -> Optional[grpc.ServerCredentials]:
    """Build the server TLS credentials, or ``None`` when TLS is not configured.

    The certificate and the private key are resolved independently, each one
    from its command line option first and from the Security_Settings second
    (Requirement 1.10), and the pair is then all-or-nothing:

    * neither configured: return ``None``, and the caller binds a plaintext
      port and warns about it (Requirements 1.2, 1.3);
    * both configured: read both files and return the credentials
      (Requirement 1.1);
    * exactly one configured: raise (Requirement 1.4).

    ``client_ca_file`` is honoured only as far as saying so: this version does
    not verify client certificates, and a configured path earns a WARNING
    before the start-up continues (Requirements 1.8, 1.9). Concretely, neither
    ``root_certificates`` nor ``require_client_auth`` is passed to
    :func:`grpc.ssl_server_credentials`, so the setting stays a pure extension
    point for mTLS.

    Args:
        settings: The ``security`` section of a Connect_Config, or ``None``
            when the server runs without a config file (the command line
            options alone can then still enable TLS).
        cert_file: Certificate path from the Server_CLI ``--tls-cert`` option,
            taking precedence over ``settings.server_cert_file``.
        key_file: Private key path from the Server_CLI ``--tls-key`` option,
            taking precedence over ``settings.server_key_file``.

    Returns:
        A :class:`grpc.ServerCredentials` for ``add_secure_port``, or ``None``
        when no certificate and no private key are configured.

    Raises:
        SecurityConfigError: Only one of the certificate and the private key is
            configured (Requirement 1.4), or a file does not exist, is not
            readable, or cannot be parsed as a certificate / private key
            (Requirement 1.5).
    """
    # Warned before the pair is looked at, so the warning appears whatever the
    # rest of the TLS configuration turns out to be: Requirement 1.9 ties it to
    # the setting being present, not to TLS being enabled, and an operator who
    # configured a client CA and nothing else is precisely the one who needs to
    # read it.
    if settings is not None and not _is_blank(settings.client_ca_file):
        logger.warning(
            f"{CLIENT_CA_SETTING_NAME} is set to "
            f"{settings.client_ca_file.strip()!r}, but this version does not "
            f"verify client certificates: the setting is reserved for mTLS and "
            f"has no effect on the served port"
        )

    cert_path = _resolve_path(
        cert_file, None if settings is None else settings.server_cert_file
    )
    key_path = _resolve_path(
        key_file, None if settings is None else settings.server_key_file
    )

    if cert_path is None and key_path is None:
        logger.debug("no TLS certificate and no private key configured")
        return None

    if cert_path is None or key_path is None:
        _raise_half_configured(cert_path, key_path)

    cert_bytes = _read_pem_file(cert_path, "certificate file")
    key_bytes = _read_pem_file(key_path, "private key file")
    _validate_pair(cert_path, key_path)

    logger.debug(
        f"built TLS server credentials from certificate file {cert_path!r} "
        f"and private key file {key_path!r}"
    )
    # Mind the order inside the pair: ``ssl_server_credentials`` takes
    # ``(private_key, certificate_chain)``, key first. Swapping the two builds
    # a credentials object without complaint and fails at handshake time.
    return grpc.ssl_server_credentials([(key_bytes, cert_bytes)])
