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


CLI_SCOPES = diagnostic.json.loads(
    (Path(__import__("os").environ["RUN_SNAPSHOT_REPAIR_ROOT"]) / "scope-table.json").read_text()
)


@pytest.fixture
def cli_owner(tmp_path):
    import importlib
    import run_snapshot_control as control

    module = importlib.import_module("tests.scripts.test_run_snapshot_control")
    context = control.Client.current().rpc("case_context")
    owner = module.Owner(tmp_path / "owner", context)
    previous, control._OWNER = control._OWNER, owner
    try:
        yield owner
    finally:
        result = owner.finish(context["hard_ns"])
        control._OWNER = previous
        control.Client.current().rpc(
            "owner_closed", complete=result["complete"],
            thread_alive=result["thread_alive"],
            evidence=str(owner.directory / "independent-owner.json"),
        )
        assert result["complete"], result


@pytest.mark.parametrize("scope", CLI_SCOPES, ids=[s["id"] for s in CLI_SCOPES])
def test_cli_scope(tmp_path, scope, cli_owner):
    import run_snapshot_control as control

    client = control.Client.current()
    context = client.rpc("case_context")
    step = context["spec"]["steps"][0]
    job = dict(
        stage="G0", start_ns=context["start_ns"],
        operational_ns=context["operational_ns"], hard_ns=context["hard_ns"],
        parent_operational_ns=context["parent_operational_ns"],
        budget=str(Path(client.header["root"]) / "header.json"),
        batch_directory=client.header["root"],
        sources=control.read(Path(client.header["root"]) / "manifest.json")["sources"],
        cwd=step["cwd"], command=step["command"],
        fault=None, view=None, coordinator_mode="normal", pss=False,
        owner_fd=cli_owner.child_channel.fileno(),
    )
    result = control.run_owned(tmp_path / "entry", job)
    assert result["parent_sampling_valid"] and result["owner_complete"], result
    assert result["tail_known"] and not result["hard_breached"], result
    assert result["cleanup_within_deadline"] and result["evidence_complete"], result
    assert result["error"] == step["expected"], result
    text = (tmp_path / "entry/worker.log").read_text()
    if scope["id"] in {"g1", "g0_100k", "future_boolean", "no_fd"}:
        assert "service_fences_installed" in text
    if scope["id"] == "stop":
        assert result["success"] and "child_domain_stopped" in text
        assert not (tmp_path / "second/attempt.json").exists()
    else:
        assert not result["success"] and scope["message"] in text
    if scope["id"] not in {"replay", "stop"}:
        assert not (tmp_path / "diagnostic/attempt.json").exists()
    else:
        first = control.read(tmp_path / "diagnostic/result.json")
        assert not first["candidate_selection_allowed"]
        assert first["validation_complete"] == (scope["id"] == "replay")
