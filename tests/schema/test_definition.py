import copy
import json
import subprocess
import sys

import pytest
from hypothesis import given, strategies as st

from takler.schema import DefinitionDocument, DefinitionError, parse_definition


def document(**fields):
    return dict(
        kind="takler.definition",
        schema_version=1,
        root=dict(type_id="takler.flow", name="f", **fields),
    )


def test_defaults_and_json_roundtrip():
    parsed = parse_definition(document())
    assert parsed.root.children == []
    assert parsed.root.default_node_status == "queued"
    assert parsed.root.trigger is None
    assert DefinitionDocument.model_validate_json(parsed.model_dump_json()) == parsed


@pytest.mark.parametrize("version", [None, True, False, "1", 1.0, 0, 2, -1])
def test_bad_version(version):
    data = document()
    data["schema_version"] = version
    with pytest.raises(DefinitionError, match="unsupported_version"):
        parse_definition(data)


@pytest.mark.parametrize(
    "raw",
    [
        '{"kind":"takler.definition","kind":"takler.definition"}',
        '{"x":{"y":1,"y":2}}',
        "{} {}",
        "{",
        "NaN",
        "Infinity",
        "-Infinity",
        '{"x":NaN}',
        "null",
        "[]",
        "42",
        b"\xff",
    ],
)
def test_invalid_json(raw):
    with pytest.raises(DefinitionError):
        parse_definition(raw)


def test_direct_json_validation_rejects_duplicate_keys():
    raw = json.dumps(document()).replace('"name": "f"', '"name": "f", "name": "g"')
    with pytest.raises(DefinitionError):
        DefinitionDocument.model_validate_json(raw)


@pytest.mark.parametrize(
    "field",
    [
        "state",
        "status",
        "suspended",
        "task_id",
        "try_no",
        "job_password",
        "begun",
        "calendar",
        "trigger_free",
        "complete_trigger_free",
        "is_complete_triggered",
        "parent",
        "node_path",
        "class_type",
        "server_state",
        "server_parameters",
        "generated_parameters",
        "auth",
        "secret",
    ],
)
def test_runtime_and_unknown_fields_rejected_without_value_disclosure(field):
    with pytest.raises(DefinitionError) as error:
        parse_definition(document(**{field: "DO_NOT_DISCLOSE"}))
    assert "DO_NOT_DISCLOSE" not in str(error.value)


@pytest.mark.parametrize(
    "fields",
    [
        {"name": ""},
        {"name": "."},
        {"name": ".."},
        {"name": "a/b"},
        {"name": "a:b"},
        {"name": "a\0b"},
        {"name": 1},
        {"events": None},
        {"children": None},
        {"user_parameters": {}},
        {"default_node_status": 3},
        {"trigger": ""},
        {"trigger": False},
        {"type_data": {"job_password": "SECRET"}},
        {"events": [{"name": "e", "initial_value": 1}]},
        {"events": [{"name": "e", "value": True}]},
        {"meters": [{"name": "m", "min_value": True, "max_value": 2}]},
        {"meters": [{"name": "m", "min_value": "0", "max_value": 2}]},
        {"meters": [{"name": "m", "min_value": 3, "max_value": 2}]},
        {"limits": [{"name": "l", "limit": -1}]},
        {"in_limits": [{"limit_name": "l", "tokens": 0}]},
        {"times": [{"time": "6:00"}]},
        {"times": [{"time": "24:00"}]},
        {"times": [{"time": "06:60"}]},
        {"times": [{"time": "06:00", "free": True}]},
        {"user_parameters": [{"name": "p"}]},
        {"user_parameters": [{"name": "p", "value": []}]},
        {"user_parameters": [{"name": "p", "value": float("nan")}]},
        {"user_parameters": [{"name": "p", "value": float("inf")}]},
    ],
)
def test_strict_fields(fields):
    data = document()
    data["root"].update(fields)
    with pytest.raises(DefinitionError):
        parse_definition(data)


@pytest.mark.parametrize(
    "name", ["TAKLER_PASS", "takler_secret", "TAKLER_HOST", "TAKLER_PORT"]
)
def test_forbidden_parameters(name):
    with pytest.raises(DefinitionError, match="forbidden_field"):
        parse_definition(document(user_parameters=[dict(name=name, value="SECRET")]))


@pytest.mark.parametrize(
    "start,end,step",
    [
        ("20260230", "20260301", 1),
        ("2026011", "20260102", 1),
        ("20260103", "20260101", 1),
        ("20260101", "20260102", 0),
        ("20260101", "20260102", True),
    ],
)
def test_repeat_validation(start, end, step):
    with pytest.raises(DefinitionError):
        parse_definition(
            document(
                repeat=dict(
                    type_id="takler.repeat.date",
                    name="D",
                    start_date=start,
                    end_date=end,
                    step=step,
                )
            )
        )


@pytest.mark.parametrize(
    "field,item",
    [
        ("events", {"name": "e"}),
        ("limits", {"name": "l", "limit": 2}),
        ("meters", {"name": "m", "min_value": 0, "max_value": 1}),
        ("user_parameters", {"name": "p", "value": None}),
        ("in_limits", {"limit_name": "l"}),
        ("times", {"time": "06:00"}),
        ("children", {"type_id": "takler.task", "name": "t"}),
    ],
)
def test_duplicates(field, item):
    with pytest.raises(DefinitionError, match="duplicate_name"):
        parse_definition(document(**{field: [item, copy.deepcopy(item)]}))


@pytest.mark.parametrize(
    "child",
    [
        {"type_id": "takler.flow", "name": "f2"},
        {"type_id": "takler.bunch", "name": ""},
        {"type_id": "evil.module.Task", "name": "t"},
        {
            "type_id": "takler.task",
            "name": "t",
            "children": [{"type_id": "takler.task", "name": "c"}],
        },
    ],
)
def test_structure(child):
    with pytest.raises(DefinitionError):
        parse_definition(document(children=[child]))


@given(
    st.one_of(
        st.none(),
        st.booleans(),
        st.integers(),
        st.floats(allow_nan=False, allow_infinity=False),
        st.text(),
    )
)
def test_scalar_types_preserved(value):
    parsed = parse_definition(document(user_parameters=[dict(name="p", value=value)]))
    actual = parse_definition(parsed.model_dump_json()).root.user_parameters[0].value
    assert type(actual) is type(value)
    assert actual == value


def test_schema_does_not_import_execution_objects():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import takler.schema; "
            "assert not any(x.startswith(('takler.core', 'takler.server', 'takler.client', 'takler.tasks')) for x in sys.modules)",
        ],
        check=True,
    )


@pytest.mark.parametrize("field", ["kind", "schema_version", "root"])
def test_required_document_fields(field):
    data = document()
    del data[field]
    with pytest.raises(DefinitionError):
        parse_definition(data)


def test_wrong_kind_and_missing_node_type():
    data = document()
    data["kind"] = "takler.checkpoint"
    with pytest.raises(DefinitionError):
        parse_definition(data)
    data = document()
    del data["root"]["type_id"]
    with pytest.raises(DefinitionError):
        parse_definition(data)


def test_bunch_shape_and_duplicates():
    flow = document()["root"]
    data = dict(
        kind="takler.definition",
        schema_version=1,
        root=dict(type_id="takler.bunch", name="", flows=[flow, flow]),
    )
    with pytest.raises(DefinitionError, match="duplicate_name"):
        parse_definition(data)
    data["root"]["flows"] = []
    data["root"]["children"] = []
    with pytest.raises(DefinitionError):
        parse_definition(data)
