"""The ``takler-client-py`` command line interface (Client_CLI).

Every subcommand runs its body through :func:`_run_command`, which owns the
contract a job script relies on (requirements 10.1 ~ 10.6):

* success exits with ``0``;
* a response carrying a non zero ``flag`` prints one stderr line holding the
  Error_Code classification name and the server ``message``, then exits with the
  code that Error_Code maps to;
* a client side failure (typically the Call_Wrapper giving up after the
  Retry_Window) prints one stderr line and exits with the code its exception
  type maps to;
* nothing ever puts a Python traceback in front of the caller.

The last point is what makes the exit codes usable at all: the job script
wrapper uses ``set -e``, so a traceback on stderr is noise the script cannot act
on, while the exit code is the only signal it can branch on. An unexpected
exception is still diagnosable -- its traceback goes to the log file, never to
stderr (see :func:`_log_unexpected_error`).

Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6.
"""

import os
import traceback
import warnings
from typing import Any, Callable, Optional, List, Union, Tuple

import typer

import takler.logging
from takler.client.exit_code import (
    exit_code_for_error_code,
    exit_code_for_exception,
)
from takler.client.service_client import TaklerServiceClient
from takler.exceptions import TaklerError
from takler.logging import get_logger
from takler.logging.config import resolve_config
from takler.server.connect_config import (
    ConnectConfig,
    load_connect_config,
    TAKLER_CONNECT_FILE,
)
from takler.server.protocol.error_code import SUCCESS, error_name_for_code
from takler.constant import DEFAULT_HOST, DEFAULT_PORT


TAKLER_HOST = "TAKLER_HOST"
TAKLER_PORT = "TAKLER_PORT"
TAKLER_NAME = "TAKLER_NAME"
NO_TAKLER = "NO_TAKLER"

HOST_HELP_STRING = f"takler service host, or use env var {TAKLER_HOST}"
PORT_HELP_STRING = f"takler service port, or use env var {TAKLER_PORT}"

#: Component name used for the CLI's own log records.
LOGGER_NAME = "client.cli"


app = typer.Typer()


# Exit code and error reporting -------------------------------------


def _echo_error(message: str) -> None:
    """Print ``message`` on stderr as exactly one line.

    Embedded newlines are folded into spaces: a server ``message`` may be
    multi line, and a caller parsing stderr has a much easier time with one
    record per line (requirements 10.2, 10.3, 10.4).
    """
    typer.echo(" ".join(message.splitlines()), err=True)


def _log_unexpected_error(exc: BaseException) -> None:
    """Record the traceback of an unexpected failure in the log file.

    Requirement 10.6 forbids handing a traceback to the caller, and requirement
    10.2 ~ 10.4 fix stderr at one line, so the traceback cannot go through the
    console sink -- which is enabled by default. It is therefore written only
    when a log file is configured (``TAKLER_LOG_FILE``), with the console sink
    switched off for this record. The process is about to exit, so turning the
    console off here has no further effect on the run.

    Without a log file there is nowhere to put the traceback that does not
    violate the stderr contract, so it is dropped; the one line diagnosis
    printed by :func:`_fail` still names the exception type and its description.
    """
    if resolve_config({}, os.environ).log_file is None:
        return

    takler.logging.configure(console=False)
    detail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    get_logger(LOGGER_NAME).error(
        f"unexpected error while running a takler client command: "
        f"{type(exc).__name__}: {exc}\n{detail}"
    )


def _fail(message: str, exit_code: int) -> None:
    """Report ``message`` on stderr and end the process with ``exit_code``."""
    _echo_error(message)
    raise typer.Exit(code=exit_code)


def _check_response(response: Any) -> Any:
    """Turn a non zero ``flag`` into a stderr line plus an exit code.

    A business failure comes back as a response, not as an exception
    (requirement 9.7 keeps the Call_Wrapper from retrying it), so the Error_Code
    in ``flag`` is translated here (requirements 10.2, 10.3). Query responses
    carry no ``flag`` at all and are treated as success.
    """
    flag = getattr(response, "flag", SUCCESS)
    if flag == SUCCESS:
        return response

    message = getattr(response, "message", "")
    _fail(
        f"{error_name_for_code(flag)}: {message}",
        exit_code_for_error_code(flag),
    )


def _run_command(command: Callable[[], Any]) -> Any:
    """Run ``command`` translating its outcome into the CLI's exit contract.

    Parameters
    ----------
    command
        A zero argument callable performing one client command. It returns the
        server response, or ``None`` for commands without one.

    Returns
    -------
    The value returned by ``command`` when the command succeeded.

    Raises
    ------
    typer.Exit
        With the exit code of the failure (requirements 10.2, 10.3, 10.4). The
        exception is the CLI's normal way out, so this is not an error path for
        the caller of :func:`_run_command`.
    """
    try:
        response = command()
    except (typer.Exit, typer.Abort):
        # Raised by the command itself, already carrying its own exit code.
        raise
    except TaklerError as exc:
        # A classified failure: its type already says what went wrong, so the
        # type name plus the description is the whole diagnosis. For
        # ClientConnectionError the description holds the server address and the
        # total number of attempts (requirement 10.4).
        _fail(f"{type(exc).__name__}: {exc}", exit_code_for_exception(exc))
    except Exception as exc:  # noqa: BLE001 - the CLI is the last boundary
        _log_unexpected_error(exc)
        _fail(f"{type(exc).__name__}: {exc}", exit_code_for_exception(exc))

    return _check_response(response)


def _load_connect_config() -> Optional[ConnectConfig]:
    """Load the Connect_Config named by ``TAKLER_CONNECT_FILE``, if any.

    Returns
    -------
    Optional[ConnectConfig]
        The parsed config, or ``None`` when the environment variable is unset.
        A parse failure propagates, so it lands on the one line plus exit code
        contract of :func:`_run_command` like any other failure.
    """
    path = os.environ.get(TAKLER_CONNECT_FILE)
    if path is None:
        return None
    return load_connect_config(path)


def _create_client(
    host: Optional[str] = None,
    port: Optional[Union[str, int]] = None,
) -> TaklerServiceClient:
    """Resolve the server address and build a client for it.

    The Connect_Config is parsed once and used for both the address and the
    ``security`` section, which is the third precedence level of the client's
    TLS knobs (requirements 2.3, 2.5).
    """
    connect_config = _load_connect_config()
    resolved_host, resolved_port = get_host_and_prot(host, port, connect_config)
    return TaklerServiceClient(
        host=resolved_host,
        port=resolved_port,
        connect_config=connect_config,
    )


def _run_client_command(
    host: Optional[str],
    port: Optional[Union[str, int]],
    body: Callable[[TaklerServiceClient], Any],
) -> Any:
    """Run ``body`` against a freshly built client under :func:`_run_command`.

    Address resolution is inside the wrapper on purpose: a broken
    ``TAKLER_CONNECT_FILE`` must land on the same one line plus exit code
    contract as any other failure, not on a traceback.
    """
    return _run_command(lambda: body(_create_client(host, port)))


# Child command -----------------------------------------------------


@app.command()
def init(
    task_id: str = typer.Option(..., help="task id (TAKLER_RID)."),
    node_path: str = typer.Option(..., envvar=TAKLER_NAME, help="node path."),
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
):
    """
    [child] init the task.

    Mark the task as active. Used only in takler script.
    """
    if NO_TAKLER in os.environ:
        typer.echo("ignore because NO_TAKLER is set.")
        return
    _run_client_command(
        host,
        port,
        lambda client: client.init(node_path=node_path, task_id=task_id),
    )


@app.command()
def complete(
    node_path: str = typer.Option(..., envvar=TAKLER_NAME, help="node path."),
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
):
    """
    [child] complete the task.

    Mark the task as complete. Used only in takler script.
    """
    if NO_TAKLER in os.environ:
        typer.echo("ignore because NO_TAKLER is set.")
        return
    _run_client_command(
        host,
        port,
        lambda client: client.complete(node_path=node_path),
    )


@app.command()
def abort(
    node_path: str = typer.Option(..., envvar=TAKLER_NAME, help="node path."),
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    reason: str = typer.Option("", help="abort reason"),
):
    """
    [child] abort the task.

    Mark the task as complete and save abort reason into task node. Used only in takler script.
    """
    if NO_TAKLER in os.environ:
        typer.echo("ignore because NO_TAKLER is set.")
        return
    _run_client_command(
        host,
        port,
        lambda client: client.abort(node_path=node_path, reason=reason),
    )


@app.command()
def event(
    node_path: str = typer.Option(..., envvar=TAKLER_NAME, help="node path."),
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    event_name: str = typer.Option(..., help="event name"),
):
    """
    [child] change Event.

    Set the event.
    """
    if NO_TAKLER in os.environ:
        typer.echo("ignore because NO_TAKLER is set.")
        return
    _run_client_command(
        host,
        port,
        lambda client: client.event(node_path=node_path, event_name=event_name),
    )


@app.command()
def meter(
    node_path: str = typer.Option(..., envvar=TAKLER_NAME, help="node path."),
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    meter_name: str = typer.Option(..., help="meter name"),
    meter_value: str = typer.Option(..., help="meter value"),
):
    """
    [child] change Meter.

    Update meter's value.
    """
    if NO_TAKLER in os.environ:
        typer.echo("ignore because NO_TAKLER is set.")
        return
    _run_client_command(
        host,
        port,
        lambda client: client.meter(
            node_path=node_path,
            meter_name=meter_name,
            meter_value=meter_value,
        ),
    )


# Control command --------------------------------------------------------


@app.command()
def requeue(
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    node_path: List[str] = typer.Argument(..., help="node paths"),
):
    """
    [control] requeue given node(s).
    """
    _run_client_command(
        host,
        port,
        lambda client: client.requeue(node_path=node_path),
    )


@app.command()
def suspend(
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    node_path: List[str] = typer.Argument(..., help="node paths"),
):
    """
    [control] suspend the node(s). prevent job creation for the node and all its children nodes.
    """
    _run_client_command(
        host,
        port,
        lambda client: client.suspend(node_path=node_path),
    )


@app.command()
def resume(
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    node_path: List[str] = typer.Argument(..., help="node paths"),
):
    """
    [control] resume the node(s) from suspended status.
    """
    _run_client_command(
        host,
        port,
        lambda client: client.resume(node_path=node_path),
    )


@app.command()
def run(
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    node_path: List[str] = typer.Argument(..., help="node paths"),
    force: bool = typer.Option(False, help="force run"),
):
    """
    [control] run the task.
    """
    _run_client_command(
        host,
        port,
        lambda client: client.run(node_path=node_path, force=force),
    )


@app.command()
def force(
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    recursive: bool = typer.Option(True, help="recursive"),
    state: str = typer.Argument(..., help="state"),
    variable_path: List[str] = typer.Argument(..., help="variable paths"),
):
    """
    [control] change the node's state force, ignore whatever state it is now.
    """
    _run_client_command(
        host,
        port,
        lambda client: client.force(
            variable_paths=variable_path, state=state, recursive=recursive
        ),
    )


@app.command()
def free_dep(
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    dep_type: str = typer.Option(True, help="dependency type, [all, time, trigger]"),
    node_path: List[str] = typer.Argument(..., help="variable paths"),
):
    """
    [control] free dependencies for the node(s).
    """
    _run_client_command(
        host,
        port,
        lambda client: client.free_dep(node_paths=node_path, dep_type=dep_type),
    )


@app.command()
def load(
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    flow_type: str = typer.Option("json", help="flow file type, [json]"),
    flow_file_path: str = typer.Argument(..., help="flow file path"),
):
    """
    [control] load flow from file to server.
    """
    _run_client_command(
        host,
        port,
        lambda client: client.load(flow_file_path=flow_file_path),
    )


@app.command()
def begin(
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    flow_name: str = typer.Argument("", help="flow name, omit it to begin all flows"),
    force: bool = typer.Option(False, help="begin an already begun flow again"),
):
    """
    [control] begin the flow(s): start the calendar and reset the node tree.

    Omitting FLOW_NAME sends an empty name, which means all flows.
    """
    _run_client_command(
        host,
        port,
        lambda client: client.begin(flow_name=flow_name, force=force),
    )


# Query command --------------------------------------------------------


@app.command()
def show(
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
    show_trigger: bool = typer.Option(False, help="show triggers"),
    show_parameter: bool = typer.Option(False, help="show parameters"),
    show_limit: bool = typer.Option(True, help="show limits"),
    show_event: bool = typer.Option(True, help="show events"),
    show_meter: bool = typer.Option(True, help="show meters"),
    show_all: bool = typer.Option(False, help="show all items, ignore other options."),
):
    """
    [query] print bunch tree.
    """
    if show_all:
        show_trigger = True
        show_parameter = True
        show_limit = True
        show_event = True
        show_meter = True

    _run_client_command(
        host,
        port,
        lambda client: client.show(
            show_trigger=show_trigger,
            show_parameter=show_parameter,
            show_limit=show_limit,
            show_event=show_event,
            show_meter=show_meter,
        ),
    )


@app.command()
def ping(
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
):
    """
    [query] check the server is running with given host and hort.
    """
    _run_client_command(host, port, lambda client: client.ping())


@app.command()
def coroutine(
    host: str = typer.Option(None, help=HOST_HELP_STRING),
    port: str = typer.Option(None, help=PORT_HELP_STRING),
):
    """
    [show] print current coroutine in server. for debug.
    """
    _run_client_command(host, port, lambda client: client.coroutine())


# ----------------------------
def get_host_and_prot(
    host: Optional[str] = None,
    port: Optional[Union[str, int]] = None,
    connect_config: Optional[ConnectConfig] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    get host and port.

    Priority:
    * function options: host, port
    * connect config file, TAKLER_CONNECT_FILE
    * env variables for host and port, TAKLER_HOST and TAKLER_PORT

    Parameters
    ----------
    host
    port
    connect_config
        An already parsed Connect_Config, so a caller that needs other sections
        of the same file does not have to read it twice. ``None`` keeps the
        previous behaviour of loading it from ``TAKLER_CONNECT_FILE``.

    Returns
    -------
    (Optional[str], Optional[str])
    """
    result_host = DEFAULT_HOST
    result_port = DEFAULT_PORT

    if TAKLER_HOST in os.environ:
        result_host = os.environ[TAKLER_HOST]

    if TAKLER_PORT in os.environ:
        result_port = os.environ[TAKLER_PORT]

    if connect_config is None and TAKLER_CONNECT_FILE in os.environ:
        connect_config = load_connect_config(os.environ[TAKLER_CONNECT_FILE])
    if connect_config is not None:
        result_host = connect_config.server.address.hostname
        result_port = connect_config.server.address.port

    if host is not None:
        result_host = host
    if port is not None:
        result_port = port

    return result_host, result_port


def get_host(host: Optional[str] = None) -> Optional[str]:
    """
    Get takler server's host. If ``host`` is ``None``, check environment variable ``TAKLER_HOST``.

    Parameters
    ----------
    host

    Returns
    -------
    Optional[str]
    """
    warnings.warn(
        "The `get_host` method is deprecated; use `get_host_and_port` instead.",
        DeprecationWarning,
    )
    if host is not None:
        return host
    if TAKLER_HOST in os.environ:
        return os.environ[TAKLER_HOST]
    return DEFAULT_HOST


def get_port(port: Optional[Union[str, int]] = None) -> Optional[str]:
    """
    Get takler server's port. If ``port`` is ``None``, check environment variable ``TAKLER_PORT``.

    Parameters
    ----------
    port

    Returns
    -------
    Optional[str]
    """
    warnings.warn(
        "The `get_port` method is deprecated; use `get_host_and_port` instead.",
        DeprecationWarning,
    )
    if port is not None:
        return str(port)
    if TAKLER_PORT in os.environ:
        return os.environ[TAKLER_PORT]
    return DEFAULT_PORT


def get_node_path(node_path: Optional[str] = None) -> Optional[str]:
    """
    Get node path. If ``node_path`` is ``None``, check environment variable ``TAKLER_NAME``.

    Parameters
    ----------
    node_path

    Returns
    -------
    Optional[str]
    """
    if node_path is not None:
        return node_path
    if TAKLER_NAME in os.environ:
        return os.environ[TAKLER_NAME]
    return None
