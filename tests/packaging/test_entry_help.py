"""``--help`` tests for the three console entry points.

The console scripts declared in ``pyproject.toml`` are checked here through the
objects they point at (``takler.client.cli:app``, ``takler.server.cli:main`` and
``takler.tui.__main__:app``), not through executables found on ``PATH``: a test
run inside a source checkout has no guarantee that the installed scripts belong
to the interpreter running the tests, and spawning three processes to print help
text buys nothing. ``typer``'s ``CliRunner`` drives the same click command tree
the console script would build, in process.

Two things are asserted beyond "exit code is 0":

* the *option and command surface* is read from the click command tree rather
  than from the rendered help text wherever the exact name matters
  (requirements 15.4, 15.6). Rendered help is wrapped to the terminal width and
  a long option name can be broken across lines, which would make a substring
  assertion depend on the width rather than on the CLI;
* ``takler-server --help`` must not bind a port. ``serve_forever`` is replaced
  by a recorder, so a regression that starts serving while merely printing help
  fails here instead of hanging the suite.

Requirement 15.6 pins the Client_CLI surface to what it was before M1: the
baseline lists below are the subcommands and option names of the pre-M1
``takler/client/cli.py``. Additions are allowed, renames and removals are not,
so every assertion on them is a subset check. The exhaustive per-option
comparison is the job of the property test for requirement 15.6; what is kept
here is the entry point view, i.e. the surface reached through
``python -m takler.client``.

Validates: Requirements 15.2, 15.3, 15.4, 15.5, 15.6
"""

from __future__ import annotations

from typing import Any

import pytest
import typer
from typer.main import get_command
from typer.testing import CliRunner


#: Subcommands of the Client_CLI before M1. ``begin`` is intentionally absent:
#: it was added by M1, and requirement 15.6 only forbids losing names.
PRE_M1_CLIENT_COMMANDS = (
    "init",
    "complete",
    "abort",
    "event",
    "meter",
    "requeue",
    "suspend",
    "resume",
    "run",
    "force",
    "free-dep",
    "load",
    "show",
    "ping",
    "coroutine",
)

#: Long option names of each pre-M1 subcommand. Boolean flags are listed by
#: their positive form only; click keeps the ``--no-...`` form as a secondary
#: option of the same parameter.
PRE_M1_CLIENT_OPTIONS = {
    "init": {"--task-id", "--node-path", "--host", "--port"},
    "complete": {"--node-path", "--host", "--port"},
    "abort": {"--node-path", "--host", "--port", "--reason"},
    "event": {"--node-path", "--host", "--port", "--event-name"},
    "meter": {"--node-path", "--host", "--port", "--meter-name", "--meter-value"},
    "requeue": {"--host", "--port"},
    "suspend": {"--host", "--port"},
    "resume": {"--host", "--port"},
    "run": {"--host", "--port", "--force"},
    "force": {"--host", "--port", "--recursive"},
    "free-dep": {"--host", "--port", "--dep-type"},
    "load": {"--host", "--port", "--flow-type"},
    "show": {
        "--host",
        "--port",
        "--show-trigger",
        "--show-parameter",
        "--show-limit",
        "--show-event",
        "--show-meter",
        "--show-all",
    },
    "ping": {"--host", "--port"},
    "coroutine": {"--host", "--port"},
}

#: Startup options ``takler-server`` must offer (requirement 15.4) plus the
#: checkpoint and exception policy options of the design.
SERVER_ADDRESS_OPTIONS = {"--host", "--port", "--config"}
SERVER_OTHER_OPTIONS = {
    "--checkpoint-file",
    "--checkpoint-interval",
    "--exception-policy",
}

#: Wide enough that no option name gets wrapped when help text is rendered.
WIDE_TERMINAL = {"COLUMNS": "200"}


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _click_command(app: typer.Typer) -> Any:
    """Return the click command a console script would build from ``app``.

    ``click`` is not imported here: recent typer versions vendor it, so the
    command classes are reached through ``typer`` only and are treated
    structurally (``.commands`` / ``.params``).
    """
    return get_command(app)


def _subcommands(app: typer.Typer) -> dict[str, Any]:
    command = _click_command(app)
    assert hasattr(command, "commands"), command
    return dict(command.commands)


def _option_names(command: Any) -> set[str]:
    """Return every long option name accepted by ``command``."""
    names: set[str] = set()
    for param in command.params:
        names.update(opt for opt in param.opts if opt.startswith("--"))
        names.update(opt for opt in param.secondary_opts if opt.startswith("--"))
    return names


# ---------------------------------------------------------------------------
# takler-client-py (Requirement 15.2)
# ---------------------------------------------------------------------------

def test_client_help_lists_commands_and_exits_zero(runner: CliRunner):
    """``takler-client-py --help`` prints the command list, exit code 0."""
    from takler.client.cli import app

    result = runner.invoke(app, ["--help"], env=WIDE_TERMINAL)

    assert result.exit_code == 0, result.output
    missing = [
        name for name in PRE_M1_CLIENT_COMMANDS if name not in result.output
    ]
    assert missing == [], result.output


def test_client_help_of_each_command_exits_zero(runner: CliRunner):
    """Per command help works too, which is what a job script author reads."""
    from takler.client.cli import app

    failed = {}
    for name in PRE_M1_CLIENT_COMMANDS:
        result = runner.invoke(app, [name, "--help"], env=WIDE_TERMINAL)
        if result.exit_code != 0:
            failed[name] = result.output

    assert failed == {}


# ---------------------------------------------------------------------------
# takler-server (Requirements 15.3, 15.4)
# ---------------------------------------------------------------------------

def test_server_help_shows_startup_options_and_exits_zero(runner: CliRunner):
    """``takler-server --help`` prints the startup options, exit code 0."""
    from takler.server import cli as server_cli

    result = runner.invoke(server_cli.app, ["--help"], env=WIDE_TERMINAL)

    assert result.exit_code == 0, result.output
    missing = [
        option
        for option in SERVER_ADDRESS_OPTIONS | SERVER_OTHER_OPTIONS
        if option not in result.output
    ]
    assert missing == [], result.output


def test_server_console_script_help_exits_zero_without_serving(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """The declared target ``takler.server.cli:main`` handles ``--help`` itself.

    Printing help must stop before ``serve_forever``, i.e. before a port is
    bound or a checkpoint is read. The recorder makes that observable.
    """
    from takler.server import cli as server_cli

    calls: list[Any] = []
    monkeypatch.setattr(server_cli, "serve_forever", calls.append)
    monkeypatch.setattr("sys.argv", ["takler-server", "--help"])
    monkeypatch.setenv("COLUMNS", "200")

    with pytest.raises(SystemExit) as exc_info:
        server_cli.main()

    assert exc_info.value.code == 0
    assert calls == []
    assert "--host" in capsys.readouterr().out


def test_server_entry_accepts_host_port_and_config_options():
    """Requirement 15.4: host, port and a Connect_Config file path."""
    from takler.server import cli as server_cli

    names = _option_names(_click_command(server_cli.app))

    assert SERVER_ADDRESS_OPTIONS <= names
    assert SERVER_OTHER_OPTIONS <= names


# ---------------------------------------------------------------------------
# takler-tui (Requirement 15.5)
# ---------------------------------------------------------------------------

def test_tui_help_exits_zero(runner: CliRunner):
    """``takler-tui --help`` runs the existing ``takler.tui`` entry, exit 0."""
    pytest.importorskip("textual", reason="the tui extra is not installed")
    pytest.importorskip("rich", reason="the tui extra is not installed")

    from takler.tui.__main__ import app as tui_app

    result = runner.invoke(tui_app, ["--help"], env=WIDE_TERMINAL)

    assert result.exit_code == 0, result.output
    # The entry is the existing TUI launcher, so it offers its connection
    # options rather than a subcommand list.
    assert {"--host", "--port", "--connect-file"} <= _option_names(
        _click_command(tui_app)
    )


def test_tui_entry_is_the_module_entry_point():
    """The console script target is the same object ``python -m takler.tui`` runs."""
    pytest.importorskip("textual", reason="the tui extra is not installed")

    import takler.tui.__main__ as tui_main

    assert isinstance(tui_main.app, typer.Typer)


# ---------------------------------------------------------------------------
# python -m takler.client (Requirement 15.6)
# ---------------------------------------------------------------------------

def test_module_entry_is_the_client_cli_app():
    """``python -m takler.client`` runs the very same Typer app as the script."""
    import takler.client.__main__ as client_main
    from takler.client.cli import app

    assert client_main.app is app


def test_module_entry_keeps_pre_m1_subcommand_names():
    """No pre-M1 subcommand was renamed or dropped."""
    import takler.client.__main__ as client_main

    names = set(_subcommands(client_main.app))

    missing = sorted(set(PRE_M1_CLIENT_COMMANDS) - names)
    assert missing == []


def test_module_entry_keeps_pre_m1_option_names():
    """No pre-M1 option was renamed or dropped, per subcommand."""
    import takler.client.__main__ as client_main

    commands = _subcommands(client_main.app)

    missing = {
        name: sorted(expected - _option_names(commands[name]))
        for name, expected in PRE_M1_CLIENT_OPTIONS.items()
        if not expected <= _option_names(commands[name])
    }
    assert missing == {}
