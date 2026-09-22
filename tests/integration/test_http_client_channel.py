"""End-to-end: the Python client drives every command over the HTTP channel.

The acceptance scenario of M3 task 8: with ``takler[http]`` installed, a
``TaklerServiceClient`` whose transport selection resolves to ``http`` talks
to the server's HTTP listener and runs the whole command surface through it
-- real uvicorn socket, real server, real client, nothing faked. The
walkthrough deliberately mirrors ``test_end_to_end_commands.py`` (the gRPC
sweep): the same commands in the same order with the same state assertions,
so "the client behaves identically on both transports" is pinned by
construction, not by prose.

A second test pins the credential translation end to end: against an
``Auth_Mode=enabled`` server, the HTTP client is refused without credentials
and accepted with them, for an Operator_Command and a Child_Command alike.
"""

from __future__ import annotations

# ruff: noqa: E402 -- the takler imports below intentionally follow the
# importorskip guards, so a checkout without the ``http`` extra skips this
# module instead of failing at collection.

import getpass
import json
from pathlib import Path
from typing import List, Optional

import pytest

pytest.importorskip(
    "fastapi", reason="the HTTP transport lives behind the takler[http] extra"
)
pytest.importorskip(
    "httpx", reason="the HTTP client transport lives behind the takler[http] extra"
)

from takler.client.http_transport import HttpTransport
from takler.client.service_client import TaklerServiceClient
from takler.core import Flow, NodeStatus
from takler.exceptions import PermissionDeniedError
from takler.server.connect_config import (
    Address,
    ConnectConfig,
    HttpSettings,
    SecuritySettings,
    Server,
)

LOCALHOST = "127.0.0.1"

FLOW_NAME = "flow_client"
TASK1 = f"/{FLOW_NAME}/container1/task1"
TASK2 = f"/{FLOW_NAME}/container1/task2"
TASK3 = f"/{FLOW_NAME}/task3"

ALL_COMMANDS = {
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
    "begin",
    "show",
    "ping",
    "coroutine",
}

CLIENT_TIMEOUT = 10.0


def _connect_config(
    grpc_port: int,
    http_port: int,
    security: Optional[SecuritySettings] = None,
) -> ConnectConfig:
    """A Connect_Config mounting HTTP next to gRPC and selecting it."""
    return ConnectConfig(
        server=Server(
            address=Address(hostname=LOCALHOST, ip=LOCALHOST, port=str(grpc_port)),
            http=HttpSettings(host=LOCALHOST, port=str(http_port)),
            transport="http",
        ),
        security=security or SecuritySettings(),
    )


def _write_flow_definition(directory: Path) -> Path:
    """The same scenario shape as the gRPC sweep's."""
    flow = Flow(FLOW_NAME)
    container1 = flow.add_container("container1")
    task1 = container1.add_task("task1")
    task1.add_event("event_a")
    task1.add_meter("meter_a", 0, 10)
    task2 = container1.add_task("task2")
    task2.add_trigger("./task1 == complete")
    flow.add_task("task3")

    flow_file = directory / "flow_client.json"
    flow_file.write_text(json.dumps(flow.to_dict()), encoding="utf-8")
    return flow_file


def _clean_client_env(monkeypatch) -> None:
    """Drop every environment variable the client would pick up."""
    for name in (
        "TAKLER_PASS",
        "TAKLER_SECRET_FILE",
        "TAKLER_TRANSPORT",
        "TAKLER_TLS_CA_FILE",
        "TAKLER_TLS_SERVER_NAME",
    ):
        monkeypatch.delenv(name, raising=False)


class CommandLog:
    """Records which commands ran and checks each response is a success."""

    def __init__(self) -> None:
        self.commands: List[str] = []

    def command(self, name: str, response):
        self.commands.append(name)
        assert response.flag == 0, (
            f"command {name} failed: flag={response.flag}, message={response.message!r}"
        )
        return response

    def query(self, name: str, response):
        self.commands.append(name)
        return response


def test_python_client_runs_all_sixteen_commands_over_http(
    server_runner_factory, free_port_fixture, tmp_path: Path, monkeypatch
):
    """The whole command surface, over the HTTP transport only.

    The client is built with no explicit transport: the Connect_Config's
    ``server.transport: http`` field selects it, which is also the
    resolution a job script gets.
    """
    _clean_client_env(monkeypatch)
    grpc_port = free_port_fixture()
    http_port = free_port_fixture()
    connect_config = _connect_config(grpc_port, http_port)
    runner = server_runner_factory(port=grpc_port, connect_config=connect_config)
    bunch = runner.server.bunch
    log = CommandLog()

    client = TaklerServiceClient(
        host=LOCALHOST,
        port=http_port,
        connect_config=connect_config,
        single_timeout=CLIENT_TIMEOUT,
        retry_window=CLIENT_TIMEOUT,
    )
    # The config field, not the test, chose the wire protocol.
    assert isinstance(client.transport, HttpTransport)

    flow_file = _write_flow_definition(tmp_path)

    client.start()
    try:
        # -- Query: ping ------------------------------------------------
        log.query("ping", client.run_request_ping())

        # -- Control: load ----------------------------------------------
        log.command("load", client.run_command_load(flow_file_path=str(flow_file)))
        flow = bunch.find_flow(FLOW_NAME)
        assert flow is not None, "load did not register the flow in the bunch"
        assert flow.begun is False

        task1 = bunch.find_node(TASK1)
        task2 = bunch.find_node(TASK2)
        task3 = bunch.find_node(TASK3)
        assert None not in (task1, task2, task3)

        # -- Control: suspend (before begin, so the running main loop does
        # not submit the queued tasks and race the assertions below) ------
        log.command("suspend", client.run_command_suspend(node_path=[f"/{FLOW_NAME}"]))
        assert flow.is_suspended() is True

        # -- Control: begin ---------------------------------------------
        log.command("begin", client.run_command_begin(flow_name=FLOW_NAME))
        assert flow.begun is True
        for node in (task1, task2, task3):
            assert node.state.node_status == NodeStatus.queued

        # -- Control: run, then the Child_Commands as its job -----------
        log.command("run", client.run_command_run(node_path=[TASK1], force=False))
        assert task1.state.node_status == NodeStatus.submitted

        log.command("init", client.run_command_init(node_path=TASK1, task_id="job-42"))
        assert task1.state.node_status == NodeStatus.active

        log.command(
            "event", client.run_command_event(node_path=TASK1, event_name="event_a")
        )
        assert task1.find_event("event_a").value is True

        log.command(
            "meter",
            client.run_command_meter(
                node_path=TASK1, meter_name="meter_a", meter_value="5"
            ),
        )
        assert task1.find_meter("meter_a").value == 5

        log.command("complete", client.run_command_complete(node_path=TASK1))
        assert task1.state.node_status == NodeStatus.complete

        # -- Child: abort (against a second submitted task) -------------
        log.command("run", client.run_command_run(node_path=[TASK3], force=False))
        log.command("abort", client.run_command_abort(node_path=TASK3, reason="boom"))
        assert task3.state.node_status == NodeStatus.aborted
        assert task3.aborted_reason == "boom"

        # -- Control: requeue -------------------------------------------
        log.command("requeue", client.run_command_requeue(node_path=[f"/{FLOW_NAME}"]))
        for node in (task1, task2, task3):
            assert node.state.node_status == NodeStatus.queued
        assert flow.begun is True
        assert flow.is_suspended() is True

        # -- Control: force ---------------------------------------------
        log.command(
            "force",
            client.run_command_force(
                variable_paths=[TASK1], state="complete", recursive=False
            ),
        )
        assert task1.state.node_status == NodeStatus.complete

        # -- Control: free-dep ------------------------------------------
        log.command(
            "free-dep",
            client.run_command_free_dep(node_paths=[TASK2], dep_type="trigger"),
        )
        assert task2.trigger_expression.free is True

        # -- Control: resume --------------------------------------------
        log.command("resume", client.run_command_resume(node_path=[f"/{FLOW_NAME}"]))
        assert flow.is_suspended() is False

        # -- Query: show ------------------------------------------------
        show_response = log.query(
            "show",
            client.run_request_show(
                show_trigger=True,
                show_parameter=False,
                show_limit=True,
                show_event=True,
                show_meter=True,
            ),
        )
        shown = json.loads(show_response.output)
        assert FLOW_NAME in {flow_dict["name"] for flow_dict in shown["flows"]}

        # -- Query: coroutine -------------------------------------------
        coroutine_response = log.query("coroutine", client.run_query_coroutine())
        assert len(coroutine_response.coroutines) > 0

        log.query("ping", client.run_request_ping())
    finally:
        client.close_channel()

    assert set(log.commands) == ALL_COMMANDS, (
        f"commands never exercised: {sorted(ALL_COMMANDS - set(log.commands))}"
    )


def test_http_transport_selected_by_the_environment(
    server_runner_factory, free_port_fixture, monkeypatch
):
    """``TAKLER_TRANSPORT=http`` is the config-free selection a job script
    uses when no Connect_Config is in play."""
    _clean_client_env(monkeypatch)
    monkeypatch.setenv("TAKLER_TRANSPORT", "http")
    grpc_port = free_port_fixture()
    http_port = free_port_fixture()
    server_runner_factory(
        port=grpc_port, connect_config=_connect_config(grpc_port, http_port)
    )

    client = TaklerServiceClient(
        host=LOCALHOST,
        port=http_port,
        single_timeout=CLIENT_TIMEOUT,
        retry_window=CLIENT_TIMEOUT,
    )
    assert isinstance(client.transport, HttpTransport)

    client.start()
    try:
        client.run_request_ping()
    finally:
        client.close_channel()


def test_http_client_auth_is_refused_and_accepted_like_grpc(
    server_runner_factory, free_port_fixture, tmp_path: Path, monkeypatch
):
    """Against an ``Auth_Mode=enabled`` server the HTTP client sends the
    same three credential keys as headers: an Operator_Command without a
    secret and a Child_Command without a job password are refused
    (client-side ``PermissionDeniedError``, the 401 mapping), and the same
    commands go through with the credentials in place.
    """
    _clean_client_env(monkeypatch)
    secret_file = tmp_path / "operator.secret"
    secret_file.write_text("# the test secret\ns3cret-value\n", encoding="utf-8")
    whitelist_file = tmp_path / "operator.whitelist"
    whitelist_file.write_text(f"{getpass.getuser()}\n", encoding="utf-8")

    grpc_port = free_port_fixture()
    http_port = free_port_fixture()
    connect_config = _connect_config(
        grpc_port,
        http_port,
        security=SecuritySettings(
            auth_mode="enabled",
            operator_secret_file=str(secret_file),
            operator_whitelist_file=str(whitelist_file),
        ),
    )
    runner = server_runner_factory(port=grpc_port, connect_config=connect_config)

    def make_client(with_config: bool = True, **kwargs) -> TaklerServiceClient:
        # A client built with the Connect_Config resolves its secret file
        # from the config's security section (the shared-connect.yaml shape),
        # so the anonymous client below is built without it -- selecting the
        # transport explicitly instead.
        client = TaklerServiceClient(
            host=LOCALHOST,
            port=http_port,
            connect_config=connect_config if with_config else None,
            transport_name=None if with_config else "http",
            single_timeout=CLIENT_TIMEOUT,
            retry_window=CLIENT_TIMEOUT,
            **kwargs,
        )
        client.start()
        return client

    # An Operator_Command without a secret is refused at the gate.
    anonymous = make_client(with_config=False)
    try:
        with pytest.raises(PermissionDeniedError):
            anonymous.run_command_begin(flow_name=FLOW_NAME)
    finally:
        anonymous.close_channel()

    # With the secret file configured, the same command reaches the handler:
    # beginning an unknown flow is a business failure, not a refusal.
    operator = make_client(secret_file=str(secret_file))
    try:
        refused = operator.run_command_begin(flow_name=FLOW_NAME)
        assert refused.flag != 0
        assert "NodeNotFoundError" in refused.message

        flow_file = _write_flow_definition(tmp_path)
        assert operator.run_command_load(flow_file_path=str(flow_file)).flag == 0
        assert operator.run_command_suspend(node_path=[f"/{FLOW_NAME}"]).flag == 0
        assert operator.run_command_begin(flow_name=FLOW_NAME).flag == 0
        assert operator.run_command_run(node_path=[TASK1], force=False).flag == 0
    finally:
        operator.close_channel()

    # A Child_Command without a job password is refused at the gate...
    monkeypatch.delenv("TAKLER_PASS", raising=False)
    childless = make_client(secret_file=str(secret_file))
    try:
        with pytest.raises(PermissionDeniedError):
            childless.run_command_init(node_path=TASK1, task_id="job-1")
    finally:
        childless.close_channel()

    # ... and accepted with one, exactly as on gRPC: the gate passes the
    # call to the scheduler, where the zombie check judges the password
    # against the one the submission recorded on the task.
    task1 = runner.server.bunch.find_node(TASK1)
    monkeypatch.setenv("TAKLER_PASS", task1.job_password)
    child = make_client(secret_file=str(secret_file))
    try:
        assert child.run_command_init(node_path=TASK1, task_id="job-1").flag == 0
        assert task1.state.node_status == NodeStatus.active
    finally:
        child.close_channel()

    # A stale password is not an auth refusal -- it passed the gate -- but a
    # zombie, answered with the same ZombieError as on gRPC.
    monkeypatch.setenv("TAKLER_PASS", "stale-password")
    stale = make_client(secret_file=str(secret_file))
    try:
        zombie = stale.run_command_complete(node_path=TASK1)
        assert zombie.flag != 0
        assert "ZombieError" in zombie.message
    finally:
        stale.close_channel()
