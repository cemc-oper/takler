"""R2-05 live query projection stays data-only and preserves show semantics."""

import json
from datetime import datetime, timezone

import pytest

from takler.core import Bunch, Flow, HaltCause, Task
from takler.core.bunch import ServerState
from takler.core.repeat import RepeatDate
from takler.query import parse_show
from takler.serialization.query import project_show
from takler.serialization.query_projection import (
    iter_query_summaries,
    project_query_detail,
)
from takler.serialization.registry import (
    NodeRegistration,
    builtin_registry,
    using_registry,
)
from takler.tasks.shell import ShellScriptTask


def test_summary_scope_depth_and_large_detail_are_separate(monkeypatch):
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    container = flow.add_container("stage")
    task = container.add_task("t")
    task.add_parameter("LARGE", "x" * 20_000)

    def forbidden(*_args, **_kwargs):
        pytest.fail("query must not call execution serializer")

    monkeypatch.setattr(Bunch, "to_dict", forbidden)
    monkeypatch.setattr(Task, "to_dict", forbidden)
    monkeypatch.setattr(Task, "update_generated_parameters", forbidden)
    before = (task.state.node_status, task.try_no, dict(task.user_parameters))

    projected = list(iter_query_summaries(bunch))
    assert [row.path for row, _ in projected] == ["/", "/f", "/f/stage", "/f/stage/t"]
    assert [generation for _, generation in projected] == [
        None,
        flow.definition_generation,
        flow.definition_generation,
        flow.definition_generation,
    ]
    assert projected[0][0].node_kind == "bunch"
    assert [
        row.path for row, _ in iter_query_summaries(bunch, flow_name="f", depth=1)
    ] == [
        "/f",
        "/f/stage",
    ]
    assert [
        row.path
        for row, _ in iter_query_summaries(bunch, scope_path="/f/stage", depth=0)
    ] == ["/f/stage"]
    summary_json = json.dumps([row.model_dump() for row, _ in projected])
    assert "LARGE" not in summary_json and "x" * 100 not in summary_json
    assert len(summary_json) < 1_000
    detail = project_query_detail(bunch, "/f/stage/t", ["parameters"])
    assert detail.parameters.user[0].value == "x" * 20_000
    assert before == (task.state.node_status, task.try_no, dict(task.user_parameters))


def test_empty_tree_and_summary_states_match_legacy_show():
    bunch = Bunch("empty")
    root = list(iter_query_summaries(bunch))
    assert len(root) == 1
    assert root[0][0].path == "/" and root[0][0].status == "unknown"
    assert root[0][1] is None
    flow = bunch.add_flow(Flow("f"))
    task = flow.add_task("t")
    task.state.suspended = True
    legacy = parse_show(json.dumps(project_show(bunch)))
    for row, _ in iter_query_summaries(bunch, flow_name="f"):
        old = legacy.get(row.path)
        assert (row.node_kind, row.type_id, row.status, row.suspended) == (
            old.node_kind,
            old.type_id,
            old.state,
            old.suspended,
        )


def test_parameter_sources_override_null_and_redaction(tmp_path):
    bunch = Bunch()
    bunch.add_parameter("ROOT_ONLY", "root")
    bunch.add_parameter("SECRET", "private")
    flow = bunch.add_flow(Flow("f"))
    flow.add_parameter("QUEUE", "standard")
    container = flow.add_container("c")
    container.add_parameter("QUEUE", "priority")
    task = container.add_task("t")
    task.add_parameter({"QUEUE": None})
    task.add_parameter("LOCAL", "visible")
    task.add_parameter("PATH", tmp_path / "file")
    groups = project_query_detail(
        bunch, "/f/c/t", ["parameters"], redacted_parameters=["secret", "local"]
    )
    parameters = groups.parameters
    assert [
        (item.name, item.value) for item in parameters.user if not item.redacted
    ] == [
        ("QUEUE", None),
        ("PATH", str(tmp_path / "file")),
    ]
    assert next(item for item in parameters.user if item.name == "LOCAL").redacted
    assert [source.source_path for source in parameters.ancestors] == [
        "/f/c",
        "/f",
        "/",
    ]
    assert parameters.ancestors[0].user[0].value == "priority"
    assert parameters.ancestors[1].user[0].value == "standard"
    root_secret = next(
        item for item in parameters.ancestors[-1].user if item.name == "SECRET"
    )
    assert root_secret.redacted and "value" not in root_secret.model_fields_set
    assert "private" not in groups.model_dump_json()
    assert groups.model_fields_set == {"parameters"}


def test_definition_runtime_artifacts_match_public_show_without_execution_codecs(
    tmp_path, monkeypatch
):
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    task = ShellScriptTask("t", script_path=tmp_path / "task.sh")
    flow.append_child(task)
    task.add_parameter("TAKLER_HOME", str(tmp_path))
    task.add_repeat(RepeatDate("YMD", 20240101, 20240103))
    task.add_trigger("../other == complete")
    task.add_complete_trigger("READY")
    task.add_time("12:30")
    task.add_event("READY", initial_value=True)
    task.add_meter("COUNT", 0, 10)
    task.set_meter("COUNT", 4)
    task.add_limit("SLOTS", 2)
    task.add_in_limit("SLOTS", tokens=1)
    task.try_no = 3
    task.submitted_at = datetime(2026, 10, 7, tzinfo=timezone.utc).isoformat()
    task.job_password = "SENSITIVE_PASSWORD"
    task.aborted_reason = "failed SENSITIVE_PASSWORD"

    legacy = project_show(bunch)["flows"][0]["children"][0]

    def forbidden(*_args, **_kwargs):
        pytest.fail(
            "query must not call execution serializer or mutate generated values"
        )

    monkeypatch.setattr(Task, "to_dict", forbidden)
    monkeypatch.setattr(Task, "update_generated_parameters", forbidden)
    detail = project_query_detail(bunch, "/f/t", ["definition", "runtime", "artifacts"])
    assert detail.definition.script_path == legacy["script_path"]
    assert detail.definition.trigger == legacy["trigger"]
    assert detail.definition.complete_trigger == legacy["complete_trigger"]
    assert detail.definition.times == legacy["times"]
    assert detail.definition.repeat == {
        "name": "YMD",
        "start_date": "2024-01-01",
        "end_date": "2024-01-03",
        "step": 1,
    }
    assert detail.definition.events == [{"name": "READY", "initial_value": True}]
    assert detail.definition.meters == [
        {"name": "COUNT", "min_value": 0, "max_value": 10}
    ]
    assert detail.definition.limits == [{"name": "SLOTS", "limit": 2}]
    assert detail.definition.in_limits == [
        {"limit_name": "SLOTS", "tokens": 1, "node_path": None}
    ]
    assert detail.runtime.events == [{"name": "READY", "value": True}]
    assert detail.runtime.meters == [{"name": "COUNT", "value": 4}]
    assert detail.runtime.limits == [{"name": "SLOTS", "value": 0}]
    assert detail.runtime.submitted_at == "2026-10-07T00:00:00Z"
    assert detail.runtime.aborted_reason == "failed <redacted>"
    assert detail.runtime.repeat == {"name": "YMD", "value": 20240101}
    hidden_repeat = project_query_detail(
        bunch, "/f/t", ["runtime"], redacted_parameters=["YMD"]
    )
    assert hidden_repeat.runtime.repeat == {"name": "YMD", "value": "<redacted>"}
    assert detail.artifacts.job_ref.origin == "anticipated"
    assert detail.artifacts.job_ref.path == next(
        item["value"]
        for item in legacy["generated_parameters"]
        if item["name"] == "TAKLER_JOB"
    )
    assert detail.artifacts.jobout_ref.origin == "anticipated"
    assert "SENSITIVE_PASSWORD" not in detail.model_dump_json()

    flow.begun = True
    flow.calendar.flow_time = datetime(2026, 10, 7, 12, 0)
    flow_runtime = project_query_detail(bunch, "/f", ["runtime"]).runtime
    assert flow_runtime.begun
    assert flow_runtime.calendar_flow_time == "2026-10-07T12:00:00"

    task.attempt_id = "current-attempt"
    task.job_path = str(tmp_path / "current.job")
    task.jobout_path = str(tmp_path / "current.out")
    task.orvix_info_path = str(tmp_path / "current.info.yaml")
    current = project_query_detail(bunch, "/f/t", ["artifacts"])
    assert current.artifacts.job_ref.path == task.job_path
    assert current.artifacts.job_ref.origin == "current"
    assert current.artifacts.orvix_info_path == task.orvix_info_path
    hidden = project_query_detail(
        bunch,
        "/f/t",
        ["definition", "artifacts"],
        redacted_parameters=["TAKLER_HOME", "TAKLER_SCRIPT"],
    )
    assert hidden.definition.script_path == "<redacted>"
    assert hidden.artifacts.job_ref is None
    assert str(tmp_path) not in hidden.model_dump_json()


def test_root_service_defaults_and_unknown_type_do_not_use_plugin_query():
    class CustomTask(Task):
        pass

    registry = builtin_registry()

    def forbidden(_node):
        pytest.fail("plugin query hook must not run")

    registry.register(
        NodeRegistration("vendor.custom", CustomTask, "task", query=forbidden)
    )
    bunch = Bunch(host="login02", port="44084")
    bunch.add_parameter("TAKLER_HOME", "/root-override")
    bunch.add_flow(Flow("f")).append_child(CustomTask("t"))
    with using_registry(registry):
        summary = list(iter_query_summaries(bunch))[-1][0]
        detail = project_query_detail(bunch, "/f/t", ["definition", "runtime"])
    assert summary.type_id == "vendor.custom" and summary.node_kind == "task"
    assert detail.runtime.try_no == 0
    assert detail.definition.script_path is None

    bunch.server_state = ServerState.from_causes([HaltCause.manual])
    root = project_query_detail(
        bunch,
        "/",
        ["parameters", "service"],
        redacted_parameters=["TAKLER_HOST"],
        server_status={
            **bunch.server_state.to_dict(),
            "message": "paused",
            "last_checkpoint_at": "2026-10-07T00:00:00Z",
            "restore_summary": None,
        },
    )
    assert root.parameters.user[0].value == "/root-override"
    assert root.service.status == "halted"
    assert root.service.status_reason == "manual pause"
    assert root.service.message == "paused"
    assert root.service.last_checkpoint_at == "2026-10-07T00:00:00Z"
    host = next(
        item for item in root.service.generated_defaults if item.name == "TAKLER_HOST"
    )
    assert host.redacted and "value" not in host.model_fields_set


def test_deep_tree_and_invalid_projection_inputs():
    bunch = Bunch()
    current = bunch.add_flow(Flow("f"))
    for index in range(1_100):
        current = current.add_container(f"c{index}")
    projected = list(iter_query_summaries(bunch, scope_path="/f", depth=1_100))
    assert len(projected) == 1_101
    with pytest.raises(ValueError):
        list(iter_query_summaries(bunch, scope_path="/f", flow_name="f"))
    with pytest.raises(ValueError):
        list(iter_query_summaries(bunch, flow_name="bad/name"))
    with pytest.raises(ValueError):
        list(iter_query_summaries(bunch, flow_name=""))
    with pytest.raises(ValueError):
        list(iter_query_summaries(bunch, scope_path="/absent"))
    with pytest.raises(ValueError):
        project_query_detail(bunch, "/f", ["summary"])
    with pytest.raises(ValueError):
        project_query_detail(bunch, "/f", ["service"])


def test_very_long_declaration_survives_detail_projection():
    bunch = Bunch()
    task = bunch.add_flow(Flow("f")).add_task("t")
    value = "a" * 300_000
    task.add_parameter("BLOB", value)
    summary = list(iter_query_summaries(bunch))
    assert "BLOB" not in json.dumps([node.model_dump() for node, _ in summary])
    detail = project_query_detail(bunch, "/f/t", ["parameters"])
    assert detail.parameters.user[0].value == value
