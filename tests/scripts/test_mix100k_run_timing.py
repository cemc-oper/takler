"""No-service regression for bounded, transparent RUN call-tree timing."""

import asyncio
from collections import Counter
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import mix100k_prepare_probe as probe
import mix100k_run_timing as timing


@pytest.mark.parametrize("failure", [False, True])
def test_nested_timings_preserve_results_errors_and_inherited_methods(
    monkeypatch, failure
):
    clock = [0]
    scheduler = timing.Scheduler(timing.Bunch())
    task = timing.Task("task")

    def tick(amount, value=None):
        clock[0] += amount
        return value

    def command(self, request):
        tick(2)
        timing.batch._snapshot(self)
        self._run_node(request.node_paths[0])
        timing.batch._snapshot(self)
        tick(3)
        return SimpleNamespace(flag=0)

    def run_node(self, path):
        tick(11)
        self.bunch.find_node(path).run()
        tick(13)
        return True

    def run_task(self):
        tick(10)
        self.before_run()
        self.do_run()
        self.after_run()
        return True

    def do_run(self):
        tick(30)
        if failure:
            raise RuntimeError("original failure")
        return True

    monkeypatch.setattr(timing.Scheduler, "run_command_run", command)
    monkeypatch.setattr(timing.Scheduler, "_run_node", run_node)
    monkeypatch.setattr(timing.Bunch, "find_node", lambda self, p: tick(7, task))
    monkeypatch.setattr(timing.Bunch, "to_dict", lambda self: tick(40, {}))
    monkeypatch.setattr(
        timing.batch, "_snapshot", lambda s: timing.batch.deepcopy(s.bunch.to_dict())
    )
    monkeypatch.setattr(timing.batch, "deepcopy", lambda v: tick(100, v))
    monkeypatch.setattr(timing.Task, "run", run_task)
    monkeypatch.setattr(timing.Task, "before_run", lambda self: tick(20))
    monkeypatch.setattr(timing.Task, "do_run", do_run)
    monkeypatch.setattr(timing.Task, "after_run", lambda self: tick(40))
    monkeypatch.setattr(timing.time, "monotonic_ns", lambda: clock[0])
    inherited = {
        n: n in vars(timing.Task)
        for n in [
            "notify_query_change",
            "update_generated_parameters",
            "unresolved_limits_up",
        ]
    }
    events = []
    with timing.timed_run_chain(
        scheduler, lambda k, **v: events.append(dict(kind=k, **v))
    ):

        def call():
            return scheduler.run_command_run(SimpleNamespace(node_paths=["/flow/task"]))

        if failure:
            with pytest.raises(RuntimeError, match="original failure"):
                call()
        else:
            assert call().flag == 0
    assert timing.Scheduler.run_command_run is command and timing.Task.run is run_task
    assert inherited == {n: n in vars(timing.Task) for n in inherited}
    row = events[-1]
    assert row["kind"] == "run_chain" and not row["instrumentation_error"]
    spans = row["spans"]
    assert [s["id"] for s in spans] == list(range(len(spans)))
    assert all(s["end_ns"] is not None for s in spans)
    exclusive = []
    for span in spans:
        children = [s for s in spans if s["parent"] == span["id"]]
        value = (
            span["end_ns"]
            - span["start_ns"]
            - sum(s["end_ns"] - s["start_ns"] for s in children)
        )
        assert value >= 0
        exclusive.append(value)
    assert sum(exclusive) == spans[0]["end_ns"] - spans[0]["start_ns"]
    if failure:
        assert spans[0]["error"] == "RuntimeError"
    else:
        assert sum(exclusive) == 416
        assert exclusive[0] == 5
        assert (
            sum(
                s["end_ns"] - s["start_ns"]
                for s in spans
                if s["name"] == "batch._snapshot"
            )
            == 280
        )


def test_capacity_failure_is_retained_and_wrappers_are_restored(monkeypatch):
    scheduler = timing.Scheduler(timing.Bunch())

    def command(self, request):
        self._run_node(request.node_paths[0])

    monkeypatch.setattr(timing.Scheduler, "run_command_run", command)
    monkeypatch.setattr(timing, "MAX_SPANS", 1)
    events = []
    with timing.timed_run_chain(
        scheduler, lambda k, **v: events.append(dict(kind=k, **v))
    ):
        with pytest.raises(ValueError, match="capacity"):
            scheduler.run_command_run(SimpleNamespace(node_paths=["/flow/task"]))
    assert timing.Scheduler.run_command_run is command
    assert len(events[-1]["spans"]) == 1
    assert events[-1]["instrumentation_error"] == "span capacity exceeded"


def test_real_worker_entry_records_run_chain_and_legal_final_state(
    tmp_path, monkeypatch
):
    from takler.server import TaklerServer
    from takler.server.grpc_transport import GrpcTransport
    from takler.server.checkpoint import CheckpointManager

    async def forbidden(*a, **k):
        raise AssertionError("services must not start")

    for owner in [TaklerServer, GrpcTransport, CheckpointManager]:
        monkeypatch.setattr(owner, "start", forbidden)
    probe.save(
        tmp_path / "config.json", dict(tasks=1000, nodes=1002, run_chain_timing=True)
    )
    probe.save(
        tmp_path / "lifecycle.json",
        dict(operational_deadline_ns=probe.time.monotonic_ns() + 30 * probe.NS),
    )
    with asyncio.Runner() as runner:
        result = runner.run(probe.worker(tmp_path))
    assert result["success"], result
    assert result["pool"]["verified"] and result["pool"]["pool"] == 250
    rows = [
        json.loads(line)
        for line in (tmp_path / "commands.jsonl").read_text().splitlines()
    ]
    chains = [r for r in rows if r["kind"] == "run_chain"]
    commands = [r for r in rows if r["kind"] == "command_completed"]
    assert len(chains) == 250 and len(commands) == 500
    assert [r["path"] for r in chains] == [
        r["path"] for r in commands if r["command"] == "run"
    ]
    assert [r["run_id"] for r in chains] == list(range(1, 251))
    for row in chains:
        assert row["flag"] == 0 and row["instrumentation_error"] is None
        counts = Counter(s["name"] for s in row["spans"])
        assert (
            counts["Scheduler.run_command_run"]
            == counts["Scheduler._run_node"]
            == counts["Task.run"]
            == 1
        )
        assert (
            counts["batch._snapshot"]
            == counts["Bunch.to_dict"]
            == counts["batch.deepcopy"]
            == 2
        )
        assert len(row["spans"]) <= timing.MAX_SPANS
        for span in row["spans"]:
            assert span["error"] is None
            if span["parent"] is not None:
                parent = row["spans"][span["parent"]]
                assert (
                    parent["start_ns"]
                    <= span["start_ns"]
                    <= span["end_ns"]
                    <= parent["end_ns"]
                )
    assert (
        max(
            len(line)
            for line in (tmp_path / "commands.jsonl").read_bytes().splitlines()
        )
        < 16384
    )
    assert not (tmp_path / "unused.check").exists()
