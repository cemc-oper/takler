"""No real service: PERF-04 grids, model, and normal in-memory transactions."""

import asyncio
from collections import Counter
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from r3_14_perf04 import Grid, NS, Pool, STEPS, candidates, prefix, simulate
from r3_14_perf04_fixture import (
    assert_stage,
    check_limits,
    child_payload,
    make_fixture,
    require_load_state,
    structural_documents,
)


def test_fixed_grid_exact_counts_and_mixture():
    grid = Grid(123)
    assert grid.total == 27000
    assert Counter(STEPS[i % 10][0] for i in range(grid.total)) == {
        "init": 2700,
        "event": 10800,
        "meter": 10800,
        "complete": 2700,
    }
    assert grid.at(6000) == 123 + 60 * NS
    assert grid.at(16000) == 123 + 70 * NS
    assert grid.at(27000) == 123 + 180 * NS
    assert Grid(0, 60, 10, False).total == 600


@pytest.mark.parametrize(
    "now,expired,current",
    [
        (60 * NS + 5_000_000, (0, 6005), 6005),
        (70 * NS, (0, 16000), 16000),
        (70 * NS + 9_000_000, (0, 16000), 16000),
        (180 * NS, (0, 27000), None),
    ],
)
def test_late_wake_never_catches_up_across_burst(now, expired, current):
    grid = Grid(0)
    assert grid.poll(now) == (expired, current)
    assert grid.poll(now)[1] is None


def test_boundary_expiry_uses_actual_following_slot():
    grid = Grid(0)
    assert grid.at(6000) - grid.at(5999) == 10_000_000
    assert grid.at(16000) - grid.at(15999) == 1_000_000
    assert grid.at(16001) - grid.at(16000) == 10_000_000


def test_two_second_response_keeps_all_offered_and_exposes_underload():
    result = simulate(2 * NS)
    assert result["offered"] == 27000
    assert result["offered"] == result["sent"] + result["not_sent"]
    assert result["busy"] > 0 and result["expired"] == 0
    assert result["completed"] + result["pending"] == result["sent"]
    assert result["sent"] < result["offered"]


def test_immediate_completion_and_long_pause_have_closed_accounting():
    assert simulate()["sent"] == 27000
    result = simulate(pauses=[(59 * NS, 71 * NS)])
    assert result["offered"] == 27000
    assert result["offered"] == result["sent"] + result["not_sent"]
    assert result["expired"] == 10200  # 1s at 100 + 10s at 1000 + 1s at 100


def test_busy_pool_out_of_order_response_and_loss_never_retry():
    pool = Pool(candidates(1000, False)[0])
    slots = [pool.reserve(0) for _ in range(25)]
    assert pool.reserve(0) is None
    for i in reversed(slots[1:]):
        pool.finish(i, success=True)
    pool.finish(slots[0], success=False)
    assert pool.tasks[slots[0]].unknown and pool.tasks[slots[0]].busy
    assert pool.reserve(0) is None
    assert sum(t.stage == 1 for t in pool.tasks) == 49
    assert len(pool.tasks) == 250


@pytest.mark.parametrize(
    "size,dispersed", [(1000, False), (10000, False), (10000, True)]
)
def test_tree_exact_scale_limits_and_pool_distribution(size, dispersed):
    bunch, paths, sentinel, bound = make_fixture(size, dispersed)
    assert len(paths) == len(set(paths)) == 250 and sentinel not in paths
    assert len(bound) == size // 10
    assert len(set(paths) & set(bound)) == 25 and sentinel in bound
    assert sum(len(f.children) for f in bunch.flows.values()) == size
    assert len(bunch.flows) + size + 1 == (1002 if size == 1000 else 10011)
    assert Counter(p.split("/")[1] for p in paths) == (
        {"flow000": 32, "flow001": 32, **{f"flow{i:03d}": 31 for i in range(2, 8)}}
        if dispersed
        else {"flow000": 250}
    )
    for path in paths:
        task = bunch.find_node(path)
        assert task.state.suspended and task.default_node_status.name == "queued"
        assert not task.check_dependencies()
    assert all(f.find_limit("capacity").limit == 100 for f in bunch.flows.values())
    assert all(
        "PAYLOAD" not in t.user_parameters
        for f in bunch.flows.values()
        for t in f.children
    )


def test_real_handlers_rotate_effectively_and_preserve_unknown_and_limits():
    from takler.protocol.commands import Command, REQUEST_TYPE_BY_COMMAND
    from takler.server.handlers import CommandHandlers
    from takler.server.scheduler import Scheduler

    async def exercise():
        bunch, paths, sentinel, bound = make_fixture(1000)
        scheduler = Scheduler(bunch)
        handler = CommandHandlers(scheduler)
        path = next(p for p in paths if p in bound)
        task = bunch.find_node(path)
        unknown = bunch.find_node(sentinel)
        # Allowed only in the initialization transaction.
        scheduler.query_changes.begin()
        unknown.run()
        unknown.mark_submission_unknown("test sentinel")
        scheduler.query_changes.end()
        scheduler.version_store.flush()
        sentinel_attempt = unknown.attempt_id

        async def call(op, data):
            code = Command(op)
            return await handler.handle(code, REQUEST_TYPE_BY_COMMAND[code](**data))

        old = None
        for _ in range(2):
            reply = await call("run", dict(node_paths=[path], force=False))
            assert reply.flag == 0 and reply.results[0].effect == "applied"
            assert task.attempt_id != old and task.try_no == 1
            assert task.job_password and task.state.suspended
            for stage in range(10):
                assert_stage(task, stage)
                before = scheduler.version_store.pin().state_version
                op, data = child_payload(task, stage)
                reply = await call(op, data)
                assert reply.flag == 0
                assert_stage(task, stage + 1)
                assert scheduler.version_store.pin().state_version > before
                check_limits(bunch)
                if stage == 0:
                    version = scheduler.version_store.pin().state_version
                    assert (await call(op, data)).flag == 0
                    assert scheduler.version_store.pin().state_version == version
            assert path not in task.parent.find_limit("capacity").node_paths
            old = task.attempt_id
            assert (
                await call("init", dict(node_path=path, attempt_id=old, task_id="late"))
            ).flag != 0
            assert (await call("requeue", dict(node_paths=[path]))).flag == 0
            assert task.attempt_id is None and task.try_no == 0
            assert [e.value for e in task.events] == [False] * 4
            assert task.find_meter("progress").value == 0
            assert (
                await call(
                    "event",
                    dict(
                        node_path=path,
                        attempt_id=old,
                        event_name="e1",
                        source_task_path=path,
                    ),
                )
            ).flag != 0
        assert (
            unknown.attempt_id == sentinel_attempt and unknown.submission_unknown_reason
        )
        assert sentinel in unknown.parent.find_limit("capacity").node_paths
        assert (await call("requeue", dict(node_paths=[sentinel]))).flag != 0

    asyncio.run(exercise())


def test_structural_templates_bounded_load_growth_and_protected_replace():
    from takler.exceptions import FlowStateError
    from takler.protocol.commands import LoadCommand, ReplaceCommand
    from takler.server.scheduler import Scheduler

    docs = structural_documents()
    assert len(docs) == 4 and all(len(v) <= 4 * 1024**2 for v in docs.values())
    bunch, paths, sentinel, bound = make_fixture(10000, True)
    scheduler = Scheduler(bunch)
    for label, total in [("load-A", 11000), ("load-B", 12000)]:
        assert scheduler.run_command_load(LoadCommand(flow_bytes=docs[label])) is None
        assert sum(len(f.children) for f in bunch.flows.values()) == total
        flow = bunch.find_flow("flow010" if label == "load-A" else "flow011")
        assert not flow.begun and all(
            t.state.node_status.name == "unknown" for t in flow.children
        )
        assert all(t.default_node_status.name == "queued" for t in flow.children)
        with pytest.raises(ValueError, match="postcondition is not supported"):
            require_load_state(flow)
    for label in ("replace-A", "replace-B"):
        assert (
            scheduler.run_command_replace(
                ReplaceCommand(target_path="/flow009", flow_bytes=docs[label])
            ).flag
            == 0
        )
        flow = bunch.find_flow("flow009")
        assert flow.state.suspended and flow.begun
        assert all(t.state.node_status.name == "queued" for t in flow.children)
        assert sum(len(f.children) for f in bunch.flows.values()) == 12000
    protected = bunch.find_node(paths[0])
    protected.run()
    # Use a correctly named definition so rejection actually tests active state.
    raw = docs["replace-A"].replace(b"flow009", b"flow000")
    with pytest.raises(FlowStateError):
        scheduler.run_command_replace(
            ReplaceCommand(target_path="/flow000", flow_bytes=raw)
        )


def test_prefix_has_four_distinct_once_events_and_increasing_meters():
    assert [prefix(i)["meter"] for i in range(11)] == [
        0,
        0,
        0,
        25,
        25,
        50,
        50,
        75,
        75,
        100,
        100,
    ]
    assert prefix(10)["events"] == [True] * 4
