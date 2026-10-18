"""PERF-08A correctness only: no extra real-service performance runs."""

import asyncio
import gc
import importlib
import json
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS))
hooks = importlib.import_module("r3_14_perf08a_hooks")
analyzer = importlib.import_module("r3_14_perf08a_analyze")
runner = importlib.import_module("r3_14_perf08a_run")
worker = importlib.import_module("r3_14_isolated_worker")


class Sink:
    def __init__(self):
        self.events = []

    def emit(self, kind, start=0, end=0, **kwargs):
        self.events.append(dict(kind=kind, start=start, end=end, **kwargs))

    def close(self):
        return {}


def event(kind, pid=10, rid=1, attempt=1, sid="", start=10, end=20, values=None):
    return dict(
        kind=kind,
        pid=pid,
        start_ns=start,
        end_ns=end,
        at_ns=end,
        identity=sid,
        values=values or [rid, 2, attempt, 0, 0, 0, 0, 0],
    )


def summary_fixture():
    # Replayed page 0 is a distinct attempt; final snapshot is joined explicitly.
    es = [
        event("request_sent", values=[1, 2, 0, 0, 0, 0, 0, 0]),
        event("request", end=100, values=[1, 2, 0, 10, 100101, 0, 0, 0]),
        event("summary_identity", sid=json.dumps(["session", "s", 2])),
        event(
            "summary_capture",
            sid="s",
            start=20,
            end=40,
            values=[8, 3, 0, 0, 900, 2, 0, 0],
        ),
        event("summary_capture_link", sid="s", values=[10, 1, 1, 0, 0, 0, 0, 0]),
    ]
    for attempt, page in [(1, 0), (2, 0), (3, 1)]:
        es.extend(
            [
                event("rpc_attempt", attempt=attempt, start=11, end=60),
                event(
                    "summary_rpc_boundary",
                    start=12,
                    end=59,
                    sid="s",
                    values=[10, 1, attempt, page, 0, 0, 0, 0],
                ),
                event(
                    "summary_attempt_reply",
                    sid="s",
                    values=[1, 2, attempt, page, 0, 0, 0, 0],
                ),
            ]
        )
    clock = dict(uncertainty_ns=10, offset_midpoint_ns=0)
    manifest = dict(
        clock_start={"server": clock, "S": clock},
        clock_exit={"server": clock, "S": clock},
        measurement_end_ns=90,
    )
    return es, manifest


def test_100k_fixture_has_exact_tree_and_no_extra_payload():
    from query_a_matrix_100k import make_tree
    from takler.core import NodeStatus

    bunch, tasks = make_tree(100000, False)
    assert len(tasks) == 100000 and len(bunch.flows) == 100
    assert 1 + len(bunch.flows) + len(tasks) == 100101
    assert all(t.state.node_status == NodeStatus.complete for t in tasks)
    assert all("PAYLOAD" not in t.parameters() for t in tasks)
    assert tasks[0].name == "task0000" and tasks[-1].name == "task0999"


def test_busy_slots_never_catch_up_and_boundary_slot_is_kept():
    assert list(hooks.missed_slots(10, 25, 60, 10)) == [10, 20]
    assert list(hooks.missed_slots(30, 30, 60, 10)) == []
    assert list(hooks.missed_slots(40, 100, 60, 10)) == [40, 50]


def test_identity_joins_replay_cross_page_and_cold_tail():
    es, m = summary_fixture()
    result = analyzer.matched_summaries(es, m)
    assert not result["problems"]
    r = result["requests"][0]
    assert r["associated"] and r["E_ns"] == 90 and r["W_ns"] == 20 and r["C_ns"] == 8
    assert r["W_minus_C_ns"] == 12 and len(r["attempt_details"]) == 3
    assert r["crosses_measurement_end"]
    assert [a["attempt_id"] for a in r["attempt_details"]] == [1, 2, 3]
    assert r["entrance_before_sum_ns"] == 3


def test_retry_initial_snapshot_work_is_kept_without_double_counting_final_stage():
    es, m = summary_fixture()
    es.extend(
        [
            event("rpc_attempt", attempt=4),
            event(
                "summary_capture_link",
                sid="abandoned",
                values=[10, 1, 4, 0, 0, 0, 0, 0],
            ),
            event(
                "summary_capture",
                sid="abandoned",
                start=25,
                end=45,
                values=[9, 0, 0, 0, 50, 1, 0, 0],
            ),
        ]
    )
    result = analyzer.matched_summaries(es, m)
    assert not result["problems"]
    row = result["requests"][0]
    assert row["associated"] and row["capture_work_overlap"]
    assert len(row["snapshots"]) == 2 and row["capture_work_C_ns"] == 17
    assert row["W_ns"] == 20 and row["C_ns"] == 8
    assert row["attempt_details"][-1]["secondary"] == "unknown"


@pytest.mark.parametrize("change", ["ambiguous_secondary", "wide_clock"])
def test_secondary_degrades_without_losing_required_evidence(change):
    es, m = summary_fixture()
    if change == "ambiguous_secondary":
        es.append(event("summary_rpc_boundary", values=[10, 1, 1, 0, 0, 0, 0, 0]))
    else:
        m["clock_start"]["server"] = dict(uncertainty_ns=2_000_000)
    result = analyzer.matched_summaries(es, m)
    assert not result["problems"] and result["requests"][0]["associated"]
    assert result["requests"][0]["entrance_before_sum_ns"] is None


def test_required_link_failure_and_unfinished_request_are_visible():
    es, m = summary_fixture()
    es = [e for e in es if e["kind"] != "summary_capture_link"]
    result = analyzer.matched_summaries(es, m)
    assert result["problems"] and not result["requests"][0]["associated"]
    es, m = summary_fixture()
    result = analyzer.matched_summaries([e for e in es if e["kind"] != "request"], m)
    assert result["unfinished"] and result["problems"]


def test_capture_metadata_context_survives_worker_thread():
    sink = Sink()

    async def rpc(request, context):
        await asyncio.to_thread(
            server.scheduler.query_service._capture, None, None, None, "snapshot"
        )
        return SimpleNamespace(json=b'{"snapshot_id":"snapshot","page_index":0}')

    server = SimpleNamespace(
        scheduler=SimpleNamespace(
            query_service=SimpleNamespace(_capture=lambda *a: None)
        ),
        grpc_transport=SimpleNamespace(ReadPage=rpc),
    )
    hooks.install_capture_link(server, sink)
    context = SimpleNamespace(
        invocation_metadata=lambda: [(hooks.METADATA_KEY, "4:5:6")]
    )
    asyncio.run(server.grpc_transport.ReadPage(None, context))
    link, boundary = sink.events
    assert (
        link["a"] == 4
        and link["b"] == 5
        and link["c"] == 6
        and link["identity"] == "snapshot"
    )
    assert boundary["kind"] == 30 and hooks.ATTEMPT.get() == (0, 0, 0)


def test_publication_success_without_publish_and_exit_are_distinct():
    sink = Sink()

    def original_publish(sequence, prepared):
        return True, sequence != 1, None

    manager = SimpleNamespace(
        _publish_prepared=original_publish, _record_outcome=lambda *a: None
    )

    def sync():
        result = manager._publish_prepared(3, None)
        manager._record_outcome(3, *result[:2], "", None)
        return result[0]

    manager.write_checkpoint = sync
    observation = SimpleNamespace(
        server=SimpleNamespace(checkpoint_manager=manager),
        emitter=sink,
        originals=[(manager, "_publish_prepared", original_publish)],
    )
    hooks.install_checkpoint_boundaries(observation)
    assert manager._publish_prepared(1, None) == (True, False, None)
    assert manager._publish_prepared(2, None) == (True, True, None)
    assert manager.write_checkpoint()
    ps = [e for e in sink.events if e["kind"] == 7]
    assert [(e["a"], e["b"], e["c"], e["d"]) for e in ps] == [
        (1, 1, 0, 0),
        (2, 1, 1, 0),
        (3, 1, 1, 1),
    ]
    assert sink.events[-1]["kind"] == 33


def test_resource_consecutive_points_reset_and_stop():
    limits = dict(owned_rss_bytes=100, collector_rss_bytes=50, consecutive_points=2)
    state = {}

    def sample(n):
        return dict(processes=[dict(role="collector", rss_bytes=n)])

    assert hooks.resource_violation(sample(60), limits, state) is None
    assert hooks.resource_violation(sample(40), limits, state) is None
    assert hooks.resource_violation(sample(60), limits, state) is None
    assert "collector_rss" in hooks.resource_violation(sample(60), limits, state)


def test_clock_and_tick_capacity_and_external_bound(tmp_path):
    settings = json.loads((runner.ROOT / "PERF-08A-config.json").read_text())[
        "observation"
    ]
    sampler = runner.Sampler(settings)
    for _ in range(1024):
        sampler.tick()
    assert len(sampler.clocks.records) == 6144 <= 8192
    assert (
        sampler.metadata()["retained_evidence_bound_bytes"]
        < settings["external_retained_evidence_max_bytes"]
    )
    with pytest.raises(RuntimeError, match="tick capacity"):
        sampler.tick()

    # A generous 4096B per clock dictionary covers observed object storage.
    def size(o):
        n = sys.getsizeof(o)
        if isinstance(o, dict):
            n += sum(size(k) + size(v) for k, v in o.items())
        elif isinstance(o, (list, tuple)):
            n += sum(size(v) for v in o)
        return n

    assert all(size(r) < 4096 for r in sampler.clocks.records)
    assert all(size(t) < 2048 for t in sampler.ticks)


def test_thread_detail_capacity_checked_before_reads(tmp_path):
    folder = tmp_path / "7/task"
    folder.mkdir(parents=True)
    for i in range(129):
        (folder / str(i)).mkdir()
    with pytest.raises(RuntimeError, match="capacity"):
        runner.detail_sample(7, proc_root=tmp_path)


def test_client_watchdog_closes_blocked_rpc_and_keeps_failure(tmp_path, monkeypatch):
    started, released = threading.Event(), threading.Event()

    class Client:
        def __init__(self, *a, **kw):
            self.transport = SimpleNamespace(
                query_stub=SimpleNamespace(),
                stub=SimpleNamespace(),
                query=lambda d: None,
            )

        def start(self):
            pass

        def run_query_snapshot(self):
            started.set()
            assert released.wait(2)
            raise ValueError("channel closed")

        def close_channel(self):
            released.set()

    import takler.client.service_client as service_client

    monkeypatch.setattr(service_client, "TaklerServiceClient", Client)
    sink = Sink()
    monkeypatch.setattr(worker, "Emitter", lambda *a: sink)
    monkeypatch.setattr(worker, "handshake", lambda: None)
    real_ns = time.monotonic_ns
    epoch = real_ns()
    monkeypatch.setattr(
        worker, "control", lambda: dict(start_ns=epoch, end_ns=epoch + 120_000_000_000)
    )
    monkeypatch.setattr(
        worker,
        "time",
        SimpleNamespace(
            monotonic_ns=lambda: real_ns() + (61_000_000_000 if started.is_set() else 0)
        ),
    )
    monkeypatch.setattr(
        worker,
        "select",
        SimpleNamespace(select=lambda *a: time.sleep(0.001) or ([], [], [])),
    )
    options = SimpleNamespace(
        role="S",
        socket="unused",
        port=1,
        secret="unused",
        run_dir=str(tmp_path),
        perf08a=False,
        idle=False,
        tasks=1000,
        summary_period_ns=10_000_000_000,
    )
    worker.client(options)
    final = json.loads((tmp_path / "S-final.json").read_text())
    assert final["threads_released"] and final["watchdog"] and released.is_set()
    assert any(e["kind"] == 24 for e in sink.events)
    assert any(e["kind"] == 20 and e.get("c") == 1 for e in sink.events)


def test_hard_deadline_only_kills_owned_live_groups(tmp_path, monkeypatch):
    import os

    killed = []
    monkeypatch.setattr(os, "killpg", lambda pid, sig: killed.append(pid))
    live = SimpleNamespace(pid=123, proc=SimpleNamespace(poll=lambda: None))
    dead = SimpleNamespace(pid=456, proc=SimpleNamespace(poll=lambda: 0))
    timer = hooks.arm_hard_deadline([live, dead], 0.01, tmp_path)
    timer.join(1)
    assert killed == [123] and (tmp_path / "hard-deadline.json").exists()


def test_default_gc_diagnostic_patch_still_exact(tmp_path):
    before = (gc.isenabled(), gc.get_threshold())
    source = tmp_path / "source"
    runner.probe.diagnostic_copy(SCRIPTS.parent, source)
    previous = json.loads(
        (runner.ROOT / "PERF-03-native-reference-execution.json").read_text()
    )
    assert runner.probe.digest(source / "gc.patch") == previous["gc_patch_sha256"]
    assert (gc.isenabled(), gc.get_threshold()) == before


def test_insufficient_full_samples_continue_but_short_failure_stops(monkeypatch):
    config = json.loads((runner.ROOT / "PERF-08A-config.json").read_text())
    seen = []

    def execute(base, source, rows, variant, pair, short):
        seen.append((variant, pair, short))
        rows.append(dict(analysis_sufficiency=False))

    monkeypatch.setattr(runner, "execute_one", execute)
    runner.execute_schedule(config, None, [])
    assert seen == [
        ("A", None, True),
        ("B", None, True),
        ("A", 1, False),
        ("B", 1, False),
        ("B", 2, False),
        ("A", 2, False),
        ("A", 3, False),
        ("B", 3, False),
    ]
    seen.clear()

    def fail(*args):
        seen.append(args[-3:])
        raise RuntimeError("short correctness failure")

    monkeypatch.setattr(runner, "execute_one", fail)
    with pytest.raises(RuntimeError):
        runner.execute_schedule(config, None, [])
    assert len(seen) == 1


def test_simultaneous_inflight_coverage_is_window_clipped():
    assert (
        analyzer.intersect_duration([(0, 30), (40, 70)], [(10, 50), (60, 90)], 20, 80)
        == 30
    )


def test_swallowed_full_cancel_failure_cannot_be_success(tmp_path, monkeypatch):
    from takler.client.full_snapshot import FullSnapshotComplete
    from takler.protocol.query_v1 import CancelFullSnapshotRequest

    class Client:
        def __init__(self, *a, **kw):
            def query(document):
                raise ValueError("cancel failed")

            self.transport = SimpleNamespace(
                query_stub=SimpleNamespace(), stub=SimpleNamespace(), query=query
            )

        def start(self):
            pass

        def run_iter_full_snapshot(self):
            # Simulate a verified completion followed by the product's swallowed cancel exception.
            yield from ({} for _ in range(100101))
            yield FullSnapshotComplete(
                "s", "f", 1, "now", "/", 100101, 1, "hash", [], None
            )
            try:
                self.transport.query(
                    CancelFullSnapshotRequest(
                        kind="cancel_full_snapshot_request",
                        schema_version=1,
                        session_id="s",
                        full_snapshot_id="f",
                    )
                )
            except Exception:
                pass

        def close_channel(self):
            pass

    import takler.client.service_client as service_client

    monkeypatch.setattr(service_client, "TaklerServiceClient", Client)
    sink = Sink()
    monkeypatch.setattr(worker, "Emitter", lambda *a: sink)
    monkeypatch.setattr(worker, "handshake", lambda: None)
    now = time.monotonic_ns()
    monkeypatch.setattr(
        worker, "control", lambda: dict(start_ns=now, end_ns=now + 1_000_000_000)
    )
    exits = []
    monkeypatch.setattr(worker.os, "_exit", lambda code: exits.append(code))
    monkeypatch.setattr(
        worker,
        "select",
        SimpleNamespace(select=lambda *a: time.sleep(0.001) or ([], [], [])),
    )
    options = SimpleNamespace(
        role="F1",
        socket="unused",
        port=1,
        secret="unused",
        run_dir=str(tmp_path),
        perf08a=True,
        idle=False,
        tasks=100000,
        summary_period_ns=10_000_000_000,
    )
    worker.client(options)
    assert exits == [4]
    assert any(e["kind"] == 25 and e["c"] == 1 for e in sink.events)
    assert any(e["kind"] == 20 and e["c"] == 1 for e in sink.events)
