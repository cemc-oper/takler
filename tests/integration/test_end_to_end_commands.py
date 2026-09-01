"""End-to-end coverage of every client command over the real wire.

Requirement 16.8: a real :class:`~takler.server.TaklerServer` is started in this
test process and **every** Child_Command, Control_Command and Query_Command is
driven through a real :class:`~takler.client.service_client.TaklerServiceClient`.
Nothing here talks to the servicer directly -- each command travels client ->
gRPC socket -> ``TaklerService`` handler -> ``Scheduler`` -> node tree -- so the
test covers exactly the path an operator's ``takler-client-py`` invocation and a
job script's child command take (Requirement 16.7).

What is asserted, per command:

* the response is a success response -- ``flag == 0`` for the command RPCs,
  which is what the server sets only when the handler ran to completion
  (Requirement 3.3),
* the state the command is supposed to produce is visible on the server's
  ``Bunch``, read straight off the live object the servicer just mutated.

Command order is dictated by the semantics, not by taste:

* ``load`` first, since every other command needs a flow to act on,
* ``suspend`` on the flow root next -- see :func:`test_all_commands_over_the_wire`
  for why this has to happen before ``begin``,
* ``begin`` before ``requeue`` / ``run`` / ``force`` / ``free-dep``: those four
  are rejected on a flow which has not begun (``_require_begun``,
  Requirement 8.10), so ``begin`` is what makes them legal,
* ``run`` before the Child_Commands of a task: a Child_Command on a queued task
  is a zombie (``Z2``), so each task is submitted before its job reports,
* the ``Query_Command``s last, once there is interesting state to query.

The flow is built from plain :class:`~takler.core.Task` nodes on purpose: a
``ShellScriptTask`` would really spawn a job when ``run`` reaches it, which an
integration test of the *command path* has no business doing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List

from takler.core import Flow, NodeStatus


# ---------------------------------------------------------------------------
# The command inventory (Glossary)
# ---------------------------------------------------------------------------

CHILD_COMMANDS = frozenset({"init", "complete", "abort", "event", "meter"})

CONTROL_COMMANDS = frozenset(
    {"requeue", "suspend", "resume", "run", "force", "free-dep", "load", "begin"}
)

QUERY_COMMANDS = frozenset({"show", "ping", "coroutine"})

ALL_COMMANDS = CHILD_COMMANDS | CONTROL_COMMANDS | QUERY_COMMANDS


# ---------------------------------------------------------------------------
# Flow definition under test
# ---------------------------------------------------------------------------

FLOW_NAME = "flow1"
TASK1 = "/flow1/container1/task1"
TASK2 = "/flow1/container1/task2"
TASK3 = "/flow1/task3"


def write_flow_definition(directory: Path) -> Path:
    """Write the flow definition ``load`` will read, and return its path.

    ``load`` takes a json flow definition from a file, so the definition is
    built with the core API and serialized -- the same round trip a user's
    ``takler-client-py load`` performs.

    The tree carries one of each attribute the commands need to act on::

        flow1
        |- container1
        |  |- task1   event: event_a, meter: meter_a [0, 10]
        |  |- task2   trigger: ./task1 == complete
        |- task3
    """
    flow = Flow(FLOW_NAME)
    container1 = flow.add_container("container1")
    task1 = container1.add_task("task1")
    task1.add_event("event_a")
    task1.add_meter("meter_a", 0, 10)
    task2 = container1.add_task("task2")
    # A real trigger, so ``free-dep`` has a dependency to free.
    task2.add_trigger("./task1 == complete")
    flow.add_task("task3")

    flow_file = directory / "flow1.json"
    flow_file.write_text(json.dumps(flow.to_dict()), encoding="utf-8")
    return flow_file


class CommandLog:
    """Records which commands ran and checks each response is a success.

    Keeping the check and the bookkeeping in one place is what lets the test
    close with "every command in the inventory was exercised" instead of a
    reader having to count call sites.
    """

    def __init__(self) -> None:
        self.commands: List[str] = []

    def command(self, name: str, response):
        """Record a command RPC and assert the server reported success."""
        self.commands.append(name)
        assert response.flag == 0, (
            f"command {name} failed: flag={response.flag}, message={response.message!r}"
        )
        return response

    def query(self, name: str, response):
        """Record a query RPC.

        Query responses carry no ``flag`` field: their success is expressed by
        the response payload, which each call site asserts on.
        """
        self.commands.append(name)
        return response


# ---------------------------------------------------------------------------
# The walkthrough
# ---------------------------------------------------------------------------


def test_all_commands_over_the_wire(takler_server, tmp_path: Path):
    """Drive every command against a live server and check the state it leaves.

    Validates: Requirements 16.7, 16.8
    """
    server = takler_server.server
    bunch = server.bunch
    log = CommandLog()

    flow_file = write_flow_definition(tmp_path)

    client = takler_server.make_client()
    # One channel for the whole walkthrough (the way the TUI uses the client),
    # so the ``run_command_*`` / ``run_request_*`` methods can be called
    # directly and every command shares one connection.
    client.start()
    try:
        # -- Query: ping ------------------------------------------------
        # First command of all: it proves the socket is really being served
        # before anything else is blamed for a failure.
        log.query("ping", client.run_request_ping())

        # -- Control: load ----------------------------------------------
        log.command("load", client.run_command_load(flow_file_path=str(flow_file)))
        flow = bunch.find_flow(FLOW_NAME)
        assert flow is not None, "load did not register the flow in the bunch"
        # Requirement 8.8: load registers the definition and nothing more.
        assert flow.begun is False
        assert [node.name for node in flow.children] == ["container1", "task3"]

        task1 = bunch.find_node(TASK1)
        task2 = bunch.find_node(TASK2)
        task3 = bunch.find_node(TASK3)
        assert None not in (task1, task2, task3)

        # -- Control: suspend -------------------------------------------
        # Issued *before* ``begin`` deliberately. The scheduler main loop is
        # really running in this process, and it submits every dependency-free
        # queued task of a begun flow; suspending the flow root makes it skip
        # this flow entirely (``check_dependencies`` stops at a suspended
        # node), which is what makes the state each command below produces
        # observable instead of racing the loop. ``suspend`` is also not
        # gated on ``begun`` (Requirement 8.10), so this order is legal, and
        # ``requeue`` / ``begin`` do not clear the flag.
        log.command("suspend", client.run_command_suspend(node_path=[f"/{FLOW_NAME}"]))
        assert flow.is_suspended() is True

        # -- Control: begin ---------------------------------------------
        log.command("begin", client.run_command_begin(flow_name=FLOW_NAME))
        assert flow.begun is True
        assert flow.calendar.initial_time is not None
        for node in (task1, task2, task3):
            assert node.state.node_status == NodeStatus.queued

        # -- Control: run (to give the Child_Commands a job to report as) -
        # Every Child_Command is judged against the run instance the server
        # records for its target: a report on a *queued* task is the
        # requeue-then-report zombie (``Z2``), and the default Zombie_Policy
        # rejects it with ``flag=31`` (Requirements 9.5, 10.2). A queued task is
        # precisely the state no job of it exists in, so the four
        # Child_Commands below are preceded by ``run``, which submits the task
        # the way the scheduler would before a real job starts talking.
        log.command("run", client.run_command_run(node_path=[TASK1], force=False))
        assert task1.state.node_status == NodeStatus.submitted

        # -- Child: init ------------------------------------------------
        log.command("init", client.run_command_init(node_path=TASK1, task_id="job-42"))
        assert task1.state.node_status == NodeStatus.active
        assert task1.task_id == "job-42"

        # -- Child: event -----------------------------------------------
        log.command(
            "event", client.run_command_event(node_path=TASK1, event_name="event_a")
        )
        assert task1.find_event("event_a").value is True

        # -- Child: meter -----------------------------------------------
        log.command(
            "meter",
            client.run_command_meter(
                node_path=TASK1, meter_name="meter_a", meter_value="5"
            ),
        )
        assert task1.find_meter("meter_a").value == 5

        # -- Child: complete --------------------------------------------
        log.command("complete", client.run_command_complete(node_path=TASK1))
        assert task1.state.node_status == NodeStatus.complete

        # -- Child: abort -----------------------------------------------
        # Submitted first, for the same reason as ``init`` above: ``abort`` is a
        # Child_Command, so it only belongs to a task that has a job.
        log.command("run", client.run_command_run(node_path=[TASK3], force=False))
        assert task3.state.node_status == NodeStatus.submitted
        log.command("abort", client.run_command_abort(node_path=TASK3, reason="boom"))
        assert task3.state.node_status == NodeStatus.aborted
        assert task3.aborted_reason == "boom"

        # -- Control: requeue -------------------------------------------
        # On the flow root, so the whole subtree goes back to its default
        # status and the attributes touched above are reset.
        log.command("requeue", client.run_command_requeue(node_path=[f"/{FLOW_NAME}"]))
        for node in (task1, task2, task3):
            assert node.state.node_status == NodeStatus.queued
        assert task1.find_event("event_a").value is False
        assert task1.find_meter("meter_a").value == 0
        assert task3.aborted_reason is None
        # requeue resets the node tree only; the calendar stays as begun left
        # it (Requirement 8.7) and the flow stays begun and suspended.
        assert flow.begun is True
        assert flow.is_suspended() is True

        # -- Control: run -----------------------------------------------
        log.command("run", client.run_command_run(node_path=[TASK3], force=False))
        assert task3.state.node_status == NodeStatus.submitted
        assert task3.try_no == 1

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
        # ``run_request_show`` raises ServerResponseError when the server
        # answered with an error text or with something that is not a bunch,
        # and it deserializes the payload into a real ``Bunch`` -- so a
        # successful return is already an assertion about the whole query path.
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
        shown_flows = {flow_dict["name"]: flow_dict for flow_dict in shown["flows"]}
        assert FLOW_NAME in shown_flows
        assert shown_flows[FLOW_NAME]["begun"] is True

        # -- Query: coroutine -------------------------------------------
        coroutine_response = log.query("coroutine", client.run_query_coroutine())
        # The server is serving this very request, so its loop has at least the
        # handler task plus the scheduler / network service tasks.
        assert len(coroutine_response.coroutines) > 0

        # -- Query: ping (again, on a busy server) ----------------------
        log.query("ping", client.run_request_ping())
    finally:
        client.close_channel()

    # Requirement 16.8: the whole command surface, not a sample of it.
    assert set(log.commands) == ALL_COMMANDS, (
        f"commands never exercised: {sorted(ALL_COMMANDS - set(log.commands))}"
    )
