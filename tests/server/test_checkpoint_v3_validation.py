"""S2 v3 schema, linkage, and no-execution restore checks."""

import copy
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from takler.core import Bunch, Flow, Task
from takler.core.limit import Limit
from takler.schema.definition import DefinitionError
from takler.serialization.checkpoint import capture_checkpoint, restore_checkpoint

FIXTURE = Path(__file__).parents[1] / "fixtures" / "checkpoint_v3.json"


def _fixture():
    return json.loads(FIXTURE.read_text())


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(extra="forbidden"),
        lambda d: d["runtime"].update(extra="forbidden"),
        lambda d: d["runtime"]["nodes"]["/f/s"].update(extra="forbidden"),
        lambda d: d["runtime"]["nodes"].pop("/f/q"),
        lambda d: d["runtime"]["nodes"].update(
            {"/missing": d["runtime"]["nodes"]["/f/q"]}
        ),
        lambda d: d["runtime"]["server_state"].update(status="complete"),
        lambda d: d["runtime"]["server_state"].update(halt_causes=["unknown"]),
        lambda d: d["definition"]["root"]["flows"][0].update(type_id="unknown.flow"),
        lambda d: d["definition"]["root"].update(limits=[]),
        lambda d: d["runtime"]["nodes"]["/f/s"].update(attempt_id=None),
        lambda d: d["runtime"]["nodes"]["/f/a"].update(definition_generation=None),
        lambda d: d["runtime"]["nodes"]["/f/s"].update(
            definition_generation="44444444-4444-4444-8444-444444444444"
        ),
        lambda d: d["secrets"]["job_passwords"].pop("/f/s"),
        lambda d: d["secrets"]["job_passwords"].update(
            {"/f/q": d["secrets"]["job_passwords"]["/f/s"]}
        ),
        lambda d: d["secrets"]["job_passwords"]["/f/s"].update(
            attempt_id="44444444-4444-4444-8444-444444444444"
        ),
        lambda d: d["runtime"]["nodes"]["/"]["limit_usage"]["slots"].update(value=3),
        lambda d: d["runtime"]["nodes"]["/"]["limit_usage"]["slots"].update(value=6),
        lambda d: d["runtime"]["nodes"]["/f"].update(repeat_value=20261101),
        lambda d: d["runtime"]["nodes"]["/f"].update(begun=None),
        lambda d: d["runtime"]["nodes"]["/f"]["calendar"].update(
            flow_time="2026-10-04T08:15:00"
        ),
        lambda d: d["runtime"]["nodes"]["/f/q"].update(job_password="forbidden"),
    ],
)
def test_invalid_snapshot_is_rejected_as_a_whole(mutate):
    data = _fixture()
    mutate(data)
    with pytest.raises(DefinitionError):
        restore_checkpoint(data)


def test_v2_checkpoint_has_no_compatibility_reader():
    data = _fixture()
    data["format_version"] = 2
    with pytest.raises(DefinitionError):
        restore_checkpoint(data)


def test_restore_never_starts_or_releases_work(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("restore invoked an execution hook")

    for cls, name in (
        (Task, "run"),
        (Flow, "begin"),
        (Limit, "increment"),
        (Limit, "decrement"),
    ):
        monkeypatch.setattr(cls, name, forbidden)
    bunch = restore_checkpoint(_fixture())
    assert bunch.limits[0].value == 2
    assert bunch.find_node("/f/s").state.node_status.name == "submitted"
    assert bunch.find_node("/f/a").state.node_status.name == "active"


def test_decode_does_not_mutate_source_document():
    data = _fixture()
    before = copy.deepcopy(data)
    restore_checkpoint(data)
    assert data == before


def test_live_calendar_is_written_with_timezone_and_can_advance_after_restore():
    bunch = Bunch()
    flow = bunch.add_flow("f")
    flow.add_task("t")
    flow.begin()
    document = capture_checkpoint(bunch)
    calendar = document.runtime.nodes["/f"].calendar
    for key in ("initial_time", "flow_time", "initial_real_time", "last_real_time"):
        assert datetime.fromisoformat(getattr(calendar, key)).tzinfo is not None
    restored = restore_checkpoint(document.model_dump())
    restored_flow = restored.find_flow("f")
    restored_flow.update_calendar(
        restored_flow.calendar.last_real_time + timedelta(seconds=1)
    )
    assert restored_flow.calendar.increment.total_seconds() == 1
