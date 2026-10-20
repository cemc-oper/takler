"""G0 T4: the actual six-snapshot entry on an explicit 250-task fixture."""

import hashlib
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_snapshot_diagnostic as diagnostic
from takler.server import batch
from takler.core import Bunch, Task
from takler.serialization.registry import TypeRegistry

CONFIG = dict(stage="G0", tasks=250)


def original_objects():
    return (
        Bunch.to_dict,
        TypeRegistry._check_execution,
        batch.deepcopy,
        Task.run,
        Task.before_run,
        Task.do_run,
        Task.after_run,
    )


def test_six_sample_actual_entry_and_partition(tmp_path):
    before = original_objects()
    result = diagnostic.diagnose(tmp_path, CONFIG)
    assert result["success"], result
    assert result["validation_complete"] and result["restored"]
    assert not result["candidate_selection_allowed"]
    assert [row["label"] for row in result["samples"]] == list(diagnostic.ORDER)
    for row in result["samples"]:
        assert row["validation_complete"]
        if row["label"].endswith("d"):
            counts = row["observation"]["completed"]
            assert counts["U-hit"] == 996 and counts["U-build"] == 4
            assert counts["R"] == 252 and counts["V"] == 1
            shape = "singleton" if row["label"] == "Wd" else "empty"
            assert row["observation"]["branches"]["U-hit:" + shape + ":true"] == 996
            for values in row["closure"].values():
                assert (
                    values["snapshot"]
                    == values["to_dict"] + values["deepcopy"] + values["X"]
                )
                assert values["to_dict"] == sum(
                    values[k] for k in ("U", "K", "V", "R", "F", "T")
                )
    assert original_objects() == before
    with pytest.raises(FileExistsError):
        diagnostic.diagnose(tmp_path, CONFIG)
    assert not (tmp_path / "unused.check").exists()


@pytest.mark.parametrize(
    "fault",
    ["clock", "stack", "expression", "none", "structure", "restore", "fingerprint"],
)
def test_invalid_entry_stops_and_restores(tmp_path, fault):
    before = original_objects()
    result = diagnostic.diagnose(tmp_path, CONFIG, fault=fault)
    assert not result["success"] and not result["validation_complete"]
    assert result["restored"] and not result["candidate_selection_allowed"]
    assert len(result["samples"]) < 6 and result["error"]
    assert original_objects() == before
    if fault in {"clock", "stack", "expression", "restore"}:
        assert result["samples"][-1]["observation"]["invalid"]
        expected = {
            "clock": "clock",
            "stack": "stack",
            "expression": "interval_exception:U-hit",
            "restore": "restore",
        }[fault]
        assert result["samples"][-1]["observation"]["errors"][0] == expected
    elif fault == "fingerprint":
        assert result["samples"][-1]["observation"]["errors"][0] == "fingerprint"
    elif fault == "none":
        assert "snapshot_unavailable" in result["error"]
    elif fault == "structure":
        assert "node_or_meter_count" in result["error"]
    with pytest.raises(FileExistsError):
        diagnostic.diagnose(tmp_path, CONFIG)


def test_real_deepcopy_exception_is_swallowed_but_not_valid(tmp_path, monkeypatch):
    calls = []

    def broken(value):
        calls.append(True)
        raise ValueError("injected copy failure")

    monkeypatch.setattr(batch, "deepcopy", broken)
    result = diagnostic.diagnose(tmp_path, CONFIG)
    assert calls == [True]
    assert not result["success"] and result["restored"]
    assert "snapshot_unavailable" in result["error"]
    assert len(result["samples"]) == 1


def test_capacity_invalid_is_sticky_through_snapshot_catch(tmp_path):
    result = diagnostic.diagnose(tmp_path, CONFIG | dict(interval_limit=30))
    assert not result["success"] and len(result["samples"]) == 3
    row = result["samples"][-1]
    assert row["observation"]["invalid"]
    assert "interval_capacity" in row["observation"]["errors"]
    assert not row["validation_complete"] and result["restored"]


@pytest.mark.parametrize(
    "config",
    [
        dict(stage="G0", tasks=100000),
        dict(stage="G1", tasks=250),
        dict(stage="G1", tasks=100000),
    ],
)
def test_unauthed_scope_and_100k_rejected_before_attempt(tmp_path, config):
    with pytest.raises(diagnostic.InvalidDiagnostic, match="no_100k"):
        diagnostic.diagnose(tmp_path, config)
    assert not (tmp_path / "attempt.json").exists()


def test_future_g1_expired_admission_refused_before_fixture(tmp_path, monkeypatch):
    import mix100k01

    monkeypatch.setattr(
        mix100k01, "fixture", lambda size: pytest.fail("no 100k fixture may run")
    )
    now = diagnostic.time.monotonic_ns()
    admission = dict(
        stage="G1",
        authorized=True,
        g0_gate_satisfied=True,
        sources_verified=True,
        prep_deadline_ns=now + 3000 * diagnostic.NS,
        total_deadline_ns=now + 5400 * diagnostic.NS,
        operational_ns=now + 150 * diagnostic.NS,
    )
    with pytest.raises(diagnostic.InvalidDiagnostic, match="no_100k"):
        diagnostic.diagnose(
            tmp_path, dict(stage="G1", tasks=100000), admission=admission
        )
    assert not (tmp_path / "attempt.json").exists()


def test_product_sources_remain_identical(tmp_path):
    root = Path(__file__).resolve().parents[2] / "src"
    before = {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob("*.py")
    }
    result = diagnostic.diagnose(tmp_path, CONFIG)
    assert result["success"], result
    assert before == {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob("*.py")
    }
