"""Show projects public live fields without executing codecs or mutating state."""

import json

import pytest

from takler.core import Bunch, Flow, Task, HaltCause, ServerStatus
from takler.core.bunch import ServerState
from takler.core.repeat import RepeatDate
from takler.query import parse_show
from takler.server.connect_config import ConnectConfig
from takler.serialization.query import project_show
from takler.serialization.registry import (
    NodeRegistration,
    builtin_registry,
    using_registry,
)
from takler.tasks.shell import ShellScriptTask
from takler.protocol.commands import ShowRequest


def test_projection_filters_all_parameter_sources_without_mutation(
    monkeypatch, tmp_path
):
    bunch = Bunch()
    bunch.add_parameter("ROOT", "retained")
    bunch.add_parameter("TAKLER_PASS", "ROOT_SECRET")
    bunch.add_parameter("operator_secret", "SERVER_SECRET")
    flow = bunch.add_flow(Flow("flow"))
    task = ShellScriptTask("task", script_path=tmp_path / "task.takler")
    flow.append_child(task)
    task.add_parameter("PRIVATE", "CUSTOM_SECRET")
    task.add_parameter("TAKLER_HOME", str(tmp_path))
    task.add_parameter("Takler-Secret", "ALIAS_SECRET")
    task.add_repeat(RepeatDate("YMD", 20240101, 20240103))
    task.try_no = 3
    task.task_id = "job-3"
    task.job_password = "JOB_SECRET"
    task.aborted_reason = "reason"
    before = (
        task.generated_parameters.takler_try_no.value,
        task.repeat.value(),
        task.state.node_status,
    )

    def forbidden(*args, **kwargs):
        pytest.fail("show must not mutate or use execution serializers")

    monkeypatch.setattr(Bunch, "to_dict", forbidden)
    monkeypatch.setattr(Task, "update_generated_parameters", forbidden)
    output = json.dumps(project_show(bunch, ["private"]))
    for secret in (
        "ROOT_SECRET",
        "SERVER_SECRET",
        "CUSTOM_SECRET",
        "ALIAS_SECRET",
        "JOB_SECRET",
    ):
        assert secret not in output
    snapshot = parse_show(output)
    view = snapshot.get("/flow/task")
    assert view.node_kind == "task"
    assert view.try_no == 3
    assert view.aborted_reason == "reason"
    assert snapshot.lookup_parameter(view.path, "ROOT") == "retained"
    assert snapshot.resolve_parameter(view.path, "PRIVATE").redacted
    assert snapshot.lookup_parameter(view.path, "TAKLER_TRY_NO") == "3"
    assert snapshot.lookup_parameter(view.path, "TAKLER_SCRIPT") == str(
        tmp_path / "task.takler"
    )
    assert snapshot.lookup_parameter(view.path, "TAKLER_JOB").endswith(
        "/flow/task.job3"
    )
    assert before == (
        task.generated_parameters.takler_try_no.value,
        task.repeat.value(),
        task.state.node_status,
    )


def test_custom_type_does_not_require_query_or_definition_codec():
    class CustomTask(Task):
        def to_dict(self):
            pytest.fail("private execution exporter invoked")

    registry = builtin_registry()
    registry.register(
        NodeRegistration(
            "acme.custom", CustomTask, "task", secret_fields=frozenset({"api_token"})
        )
    )
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    task = CustomTask("t")
    task.api_token = "PLUGIN_SECRET"
    flow.append_child(task)
    with using_registry(registry):
        output = json.dumps(project_show(bunch))
    assert "PLUGIN_SECRET" not in output
    snapshot = parse_show(output)
    assert snapshot.get("/f/t").type_id == "acme.custom"
    assert snapshot.get("/f/t").node_kind == "task"


def test_show_separates_service_status_from_node_status_and_keeps_root_parameters():
    bunch = Bunch(host="login02", port="44084")
    bunch.add_parameter("TAKLER_HOME", "/jobs")
    bunch.server_state = ServerState(
        status=ServerStatus.halted,
        status_reason="manual pause",
        halt_causes=[HaltCause.manual],
    )
    bunch.add_flow(Flow("f")).add_task("t")

    payload = project_show(bunch)
    assert payload["server_state"] == {
        "status": "halted",
        "status_reason": "manual pause",
        "halt_causes": ["manual"],
    }
    assert payload["state"]["status"] == 1
    snapshot = parse_show(json.dumps(payload))
    assert snapshot.lookup_parameter("/f/t", "TAKLER_HOST") == "login02"
    assert snapshot.lookup_parameter("/f/t", "TAKLER_HOME") == "/jobs"


def test_unregistered_local_task_still_has_generic_view():
    class LocalTask(Task):
        pass

    bunch = Bunch()
    bunch.add_flow(Flow("f")).append_child(LocalTask("t"))
    view = parse_show(json.dumps(project_show(bunch))).get("/f/t")
    assert view.name == "t"
    assert view.node_kind == "task"


def test_query_policy_is_loaded_and_passed_to_scheduler():
    from takler.server import TaklerServer

    config = ConnectConfig.model_validate(
        {
            "server": {
                "address": {"hostname": "localhost", "ip": "127.0.0.1", "port": "33083"}
            },
            "security": {"query_redacted_parameters": ["API_KEY"]},
        }
    )
    server = TaklerServer(connect_config=config)
    assert server.scheduler.query_redacted_parameters == ("API_KEY",)
    assert (
        ConnectConfig.model_validate(
            {"server": config.server.model_dump()}
        ).security.query_redacted_parameters
        == []
    )


def test_redacted_artifact_parameters_do_not_leak_through_derived_fields(tmp_path):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    task = ShellScriptTask("t", script_path=tmp_path / "PRIVATE_SCRIPT")
    flow.append_child(task)
    task.add_parameter("TAKLER_HOME", str(tmp_path / "PRIVATE_HOME"))
    task.job_password = "JOB_SECRET"
    task.aborted_reason = "failed with JOB_SECRET"
    payload = json.dumps(project_show(bunch, ["TAKLER_HOME", "TAKLER_SCRIPT"]))
    assert "PRIVATE_SCRIPT" not in payload
    assert "PRIVATE_HOME" not in payload
    assert "JOB_SECRET" not in payload
    snapshot = parse_show(payload)
    assert snapshot.lookup_parameter("/f/t", "TAKLER_JOB") is None
    assert snapshot.lookup_parameter("/f/t", "TAKLER_JOBOUT") is None
    assert snapshot.lookup_parameter("/f/t", "TAKLER_SCRIPT") is None


def test_show_projects_current_attempt_files_and_service_report(tmp_path):
    from takler.server import TaklerServer

    server = TaklerServer(checkpoint_file=tmp_path / "checkpoint")
    flow = server.bunch.add_flow(Flow("f"))
    task = flow.add_task("t")
    flow.begin()
    task.run()
    task.job_path = str(tmp_path / "current.job")
    task.jobout_path = str(tmp_path / "current.out")
    task.orvix_info_path = str(tmp_path / "current.info.yaml")
    server.bunch.server_state = ServerState.from_causes([HaltCause.recovery])
    server.scheduler.restore_summary = {
        "source": "backup",
        "in_flight_count": 1,
    }
    server.checkpoint_manager.last_checkpoint_at = "2026-10-05T00:00:00+00:00"

    payload = server.scheduler.handle_request_show(ShowRequest())
    snapshot = parse_show(payload)
    current = snapshot.get("/f/t")
    assert snapshot.service.status == "halted"
    assert snapshot.service.reason == "recovery pending"
    assert snapshot.service.restore_summary["source"] == "backup"
    assert snapshot.service.last_checkpoint_at == "2026-10-05T00:00:00+00:00"
    assert current.attempt_id == task.attempt_id
    assert current.task_id is None
    assert current.submitted_at == task.submitted_at
    assert current.job_path == str(tmp_path / "current.job")
    assert current.jobout_path == str(tmp_path / "current.out")
    assert current.orvix_info_path == str(tmp_path / "current.info.yaml")
    assert task.job_password not in payload
    assert snapshot.get("/f/t").path == "/f/t"


def test_show_handles_missing_attempt_and_hides_redacted_file_refs(tmp_path):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    task = ShellScriptTask("t", script_path=tmp_path / "task.takler")
    flow.append_child(task)
    task.job_path = str(tmp_path / "private.job")
    task.jobout_path = str(tmp_path / "private.out")
    task.orvix_info_path = str(tmp_path / "private.info.yaml")
    without_attempt = parse_show(json.dumps(project_show(bunch))).get("/f/t")
    assert without_attempt.job_path is None
    assert without_attempt.jobout_path is None
    assert without_attempt.orvix_info_path is None
    task.attempt_id = "current-attempt"
    payload = json.dumps(project_show(bunch, ["TAKLER_HOME"]))
    current = parse_show(payload).get("/f/t")
    assert current.attempt_id == "current-attempt"
    assert current.task_id is None
    assert current.job_path is None
    assert current.jobout_path is None
    assert current.orvix_info_path is None
    assert "private.job" not in payload
