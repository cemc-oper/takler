"""The streaming index must retain the document path's validation semantics."""

import json
from dataclasses import replace
from io import StringIO
from pathlib import Path

import pytest

from takler.core import Bunch
from takler.schema.definition import DefinitionError, parse_definition
from takler.serialization.checkpoint import restore_checkpoint
from takler.server.scheduler import Scheduler


@pytest.fixture
def version():
    fixture = Path(__file__).parents[1] / "fixtures" / "checkpoint_v3.json"
    return Scheduler(restore_checkpoint(fixture.read_bytes())).version_store.pin()


def changed(version, path, field, edit):
    shard, offset = version.index[path]
    rows = list(version.nodes[shard])
    value = getattr(rows[offset], field)
    if field != "children":
        value = json.loads(value)
    value = edit(value)
    if field != "children":
        value = json.dumps(value, ensure_ascii=False).encode()
    rows[offset] = replace(rows[offset], **{field: value})
    shards = list(version.nodes)
    shards[shard] = tuple(rows)
    return replace(version, nodes=tuple(shards))


def assert_document_matches_stream(version):
    output = StringIO()
    version.checkpoint_stream(output.write)
    data = json.loads(output.getvalue())
    # The streaming format omits some default definition fields. Compare their
    # schema meaning, preserving exact equality for all other checkpoint data.
    data["definition"] = parse_definition(data["definition"]).model_dump(mode="json")
    assert data == version.checkpoint_document().model_dump(mode="json")
    restored = restore_checkpoint(data)
    again = Scheduler(restored).version_store.pin().checkpoint_document()
    assert again.definition == data["definition"]
    assert again.secrets.model_dump(mode="json") == data["secrets"]
    assert again.runtime.nodes == version.checkpoint_document().runtime.nodes


def test_stream_matches_complete_document_with_root_attributes_and_attempts(version):
    assert_document_matches_stream(version)


def test_stream_matches_document_for_nested_unicode_and_empty_flows():
    bunch = Bunch("根")
    flow = bunch.add_flow("流程")
    family = flow.add_container("分组")
    task = family.add_task("任务😀")
    task.add_event("就绪")
    task.add_meter("进度", 0, 100)
    bunch.add_flow("空")
    assert_document_matches_stream(Scheduler(bunch).version_store.pin())


@pytest.mark.parametrize(
    "path,field,edit",
    [
        ("/", "children", lambda value: value + (value[0],)),
        ("/f", "definition", lambda value: {**value, "name": "renamed"}),
        ("/f", "children", lambda value: value[:-1]),
        ("/f/s", "definition", lambda value: {**value, "name": "bad/name"}),
        (
            "/f/s",
            "definition",
            lambda value: {**value, "trigger": "/missing == complete"},
        ),
        ("/f/s", "runtime", lambda value: {**value, "kind": "flow"}),
        (
            "/f/s",
            "runtime",
            lambda value: {**value, "event_values": {"ready": "false"}},
        ),
        ("/f/s", "runtime", lambda value: {**value, "event_values": {}}),
        ("/f/s", "runtime", lambda value: {**value, "attempt_id": None}),
        (
            "/f/s",
            "runtime",
            lambda value: {
                **value,
                "definition_generation": "00000000-0000-4000-8000-000000000001",
            },
        ),
        ("/", "runtime", lambda value: {**value, "limit_usage": {}}),
        ("/", "runtime", lambda value: {**value, "meter_values": {"progress": 999999}}),
    ],
    ids=[
        "duplicate-flow",
        "flow-path",
        "missing-node",
        "name",
        "reference",
        "kind",
        "strict-bool",
        "events",
        "attempt",
        "generation",
        "limits",
        "meter",
    ],
)
def test_invalid_fixed_view_is_rejected_without_output(version, path, field, edit):
    damaged = changed(version, path, field, edit)
    # Both entry points must reject; stream must fail before emitting JSON.
    with pytest.raises((DefinitionError, ValueError, KeyError)):
        damaged.checkpoint_document()
    output = StringIO()
    with pytest.raises((DefinitionError, ValueError, KeyError)):
        damaged.checkpoint_stream(output.write)
    assert output.getvalue() == ""


def test_secret_attempt_mismatch_rejected_before_output(version):
    shard, offset = version.index["/f/s"]
    secrets = [list(items) for items in version.secrets]
    secret = json.loads(secrets[shard][offset])
    secret["attempt_id"] = "00000000-0000-4000-8000-000000000001"
    secrets[shard][offset] = json.dumps(secret).encode()
    damaged = replace(version, secrets=tuple(tuple(items) for items in secrets))
    with pytest.raises(DefinitionError):
        damaged.checkpoint_document()
    output = StringIO()
    with pytest.raises(DefinitionError):
        damaged.checkpoint_stream(output.write)
    assert output.getvalue() == ""
