"""Show projects public live fields without executing codecs or mutating state."""

import json

import pytest

from takler.core import Bunch, Flow, Task
from takler.core.parameter import Parameter
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


def test_projection_filters_all_parameter_sources_without_mutation(
    monkeypatch, tmp_path
):
    bunch = Bunch()
    bunch.add_parameter("ROOT", "retained")
    bunch.add_parameter("TAKLER_PASS", "ROOT_SECRET")
    bunch.server_state.server_parameters.append(
        Parameter("operator_secret", "SERVER_SECRET")
    )
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
