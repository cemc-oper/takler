"""PERF-04 completion regressions. All transports/services are simulated."""

import asyncio
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from r3_14_perf04 import Pool, candidates
from r3_14_perf04_fixture import (
    check_limits,
    make_fixture,
    require_load_state,
    structural_documents,
)


def test_duplicate_callback_cannot_consume_the_next_reservation():
    pool = Pool(candidates(1000, False)[0])
    first = pool.reserve(0)
    pool.finish(first, success=True)
    pool.next[1] = first if isinstance(first, int) else first.task
    second = pool.reserve(1)
    before = pool.tasks[first if isinstance(first, int) else first.task].stage
    pool.finish(first, success=True)
    assert pool.tasks[first if isinstance(first, int) else first.task].stage == before
    assert second != first


def test_unknown_callback_is_terminal_even_if_success_arrives_later():
    pool = Pool(candidates(1000, False)[0])
    reservation = pool.reserve(0)
    pool.finish(reservation, success=False)
    i = reservation if isinstance(reservation, int) else reservation.task
    before = vars(pool.tasks[i]).copy()
    pool.finish(reservation, success=True)
    assert vars(pool.tasks[i]) == before


def test_limit_missing_reservation_is_detected():
    from takler.server.scheduler import Scheduler

    bunch, paths, sentinel, bound = make_fixture(1000)
    Scheduler(bunch)
    task = bunch.find_node(next(p for p in paths if p in bound))
    task.run()
    limit = task.parent.find_limit("capacity")
    limit.node_paths.remove(task.node_path)
    limit.value -= 1
    with pytest.raises(ValueError, match="limit"):
        check_limits(bunch)


def test_new_load_gate_accepts_unknown_unbegun_with_no_submission():
    from takler.protocol.commands import Command, LoadCommand
    from takler.server.handlers import CommandHandlers
    from takler.server.scheduler import Scheduler

    async def exercise():
        bunch, *_ = make_fixture(10000, True)
        scheduler = Scheduler(bunch)
        before = scheduler.version_store.pin()
        raw = structural_documents()["load-A"]
        reply = await CommandHandlers(scheduler).handle(
            Command.LOAD, LoadCommand(flow_bytes=raw)
        )
        assert reply.flag == 0
        require_load_state(bunch.find_flow("flow010"))
        assert (
            len(before.index) == 10011
            and len(scheduler.version_store.pin().index) == 11012
        )

    asyncio.run(exercise())


def test_reservations_are_once_only_and_old_attempts_cannot_advance():
    pool = Pool(candidates(1000, False)[0])
    old = pool.reserve(0)
    assert pool.finish(old, success=True)
    assert not pool.finish(old, success=True)
    pool.next[1] = old.task
    new = pool.reserve(1)
    assert new.stage == 1 and new.attempt == old.attempt and new.serial != old.serial
    assert not pool.finish(old, success=True) and pool.current(new)
    assert pool.cancel_unsent(new) and not pool.finish(new, success=True)
    assert pool.tasks[new.task].stage == 1


@pytest.mark.parametrize("fault", ["missing", "extra", "tokens", "value"])
def test_limit_audit_checks_both_directions_and_tokens(fault):
    from takler.server.scheduler import Scheduler

    bunch, paths, sentinel, bound = make_fixture(1000)
    Scheduler(bunch)
    task = bunch.find_node(next(p for p in paths if p in bound))
    task.run()
    limit = task.parent.find_limit("capacity")
    if fault == "missing":
        limit.node_paths.clear()
        limit.value = 0
    elif fault == "extra":
        limit.node_paths.add("/flow000/task0998")
        limit.value += 1
    elif fault == "tokens":
        task.in_limit_manager.in_limit_list[0].tokens = 2
    else:
        limit.value += 1
    with pytest.raises(ValueError, match="limit"):
        check_limits(bunch)


def fixture_case(directory):
    from collections import Counter
    import json
    from takler.protocol.commands import Command, REQUEST_TYPE_BY_COMMAND
    from takler.server.handlers import CommandHandlers
    from takler.server.scheduler import Scheduler
    from takler.server.checkpoint import CheckpointManager
    from r3_14_perf04_fixture import (
        prepare_pool,
        assert_stage,
        child_payload,
        task_state,
    )
    from r3_14_perf04_verify import (
        WriteVerifier,
        fixed_view,
        verify_view,
        checkpoint_equivalence,
    )

    async def exercise():
        bunch, paths, sentinel, _ = make_fixture(1000)
        scheduler = Scheduler(bunch)
        counts = Counter()

        def setup_record(row):
            counts[row["operation"]] += 1

        identities = await prepare_pool(scheduler, paths, sentinel, record=setup_record)
        assert len(identities) == 250 and counts["run"] == 250
        assert Counter(
            bunch.find_node(p).state.node_status.name for p in paths
        ) == dict(submitted=25, active=225)
        unknown = task_state(bunch.find_node(sentinel))
        assert unknown["status"] == "submitted" and unknown["unknown"]
        before = scheduler.version_store.pin()
        view = fixed_view(before)
        model = Pool(paths, identities)
        sentinel_record = view["nodes"][sentinel]
        assert verify_view(view, pool=model, sentinel=(sentinel, sentinel_record))
        handler = CommandHandlers(scheduler)
        rows = []
        verifier = WriteVerifier(handler, paths, sentinel, rows.append).install()
        path = paths[0]
        task = bunch.find_node(path)
        original = task.attempt_id
        for stage in range(12):
            if stage < 10:
                operation, payload = child_payload(task, stage)
            else:
                operation = "requeue" if stage == 10 else "run"
                payload = dict(node_paths=[path])
                if stage == 11:
                    payload["force"] = False
            code = Command(operation)
            reply = await handler.handle(code, REQUEST_TYPE_BY_COMMAND[code](**payload))
            assert reply.flag == 0
            handoff = verifier.handoff(
                dict(path=path, reservation=dict(stage=stage, attempt=original))
            )
            assert handoff["proof"]["effective"]
            if stage == 11:
                assert handoff["identity"]["attempt"] != original
                assert handoff["identity"]["password"] == task.job_password
        assert task_state(bunch.find_node(sentinel)) == unknown
        assert task.attempt_id != original and task.try_no == 1
        assert_stage(task, 0)
        model.tasks[0].attempt = task.attempt_id
        assert verify_view(
            fixed_view(scheduler.version_store.pin()),
            pool=model,
            sentinel=(sentinel, sentinel_record),
        )
        assert verify_view(view)
        assert all(
            identity["password"] not in json.dumps(rows)
            for identity in identities.values()
        )
        verifier.close()
        manager = CheckpointManager(
            bunch, checkpoint_file=directory / "state.check", interval=10
        )
        manager.query_scheduler = scheduler
        first = scheduler.version_store.pin()
        assert manager.write_checkpoint()
        assert checkpoint_equivalence(manager.checkpoint_file, first)["equivalent"]
        op, payload = child_payload(task, 0)
        code = Command(op)
        assert (
            await handler.handle(code, REQUEST_TYPE_BY_COMMAND[code](**payload))
        ).flag == 0
        second = scheduler.version_store.pin()
        assert manager.write_checkpoint()
        assert checkpoint_equivalence(manager.backup_file, first)["equivalent"]
        assert checkpoint_equivalence(manager.checkpoint_file, second)["equivalent"]
        raw = manager.checkpoint_file.read_bytes()
        manager.checkpoint_file.write_bytes(b"{")
        with pytest.raises(ValueError):
            checkpoint_equivalence(manager.checkpoint_file, second)
        manager.checkpoint_file.write_bytes(raw)
        (directory / "fixture-result.json").write_text(
            json.dumps(
                dict(
                    setup=dict(counts),
                    pool=250,
                    child_proofs=len(rows),
                    sentinel_unchanged=True,
                    fixed_digest=view["sha256"],
                    checkpoint_main_backup_equivalent=True,
                )
            )
        )

    asyncio.run(exercise())


def test_full_250_setup_write_proofs_and_checkpoint_boundaries_have_30s_limit(tmp_path):
    import subprocess

    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--fixture-case",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert (tmp_path / "fixture-result.json").exists()


@pytest.mark.parametrize("kind", ["load", "replace", "batch_set", "batch_clear"])
def test_structural_rounds_are_independent_and_versions_visible(kind):
    from takler.protocol.commands import Command, REQUEST_TYPE_BY_COMMAND
    from takler.server.handlers import CommandHandlers
    from takler.server.scheduler import Scheduler
    from r3_14_perf04_verify import verify_structure

    async def exercise():
        bunch, *_ = make_fixture(10000, True)
        if kind == "batch_clear":
            for task in bunch.find_flow("flow008").children[:100]:
                task.set_event("batch_event", True)
        scheduler = Scheduler(bunch)
        handler = CommandHandlers(scheduler)
        docs = structural_documents()
        for i in range(1 if kind.startswith("batch") else 2):
            before = scheduler.version_store.pin()
            if kind == "load":
                op = "load"
                payload = dict(flow_bytes=docs["load-" + ("A" if i == 0 else "B")])
                expected = 11012 + i * 1001
            elif kind == "replace":
                op = "replace"
                payload = dict(
                    target_path="/flow009",
                    flow_bytes=docs["replace-" + ("A" if i == 0 else "B")],
                )
                expected = 10011
            else:
                op = "force"
                payload = dict(
                    paths=[f"/flow008/task{j:04d}:batch_event" for j in range(100)],
                    state="set" if kind == "batch_set" else "clear",
                    recursive=False,
                )
                expected = 10011
            code = Command(op)
            reply = await handler.handle(code, REQUEST_TYPE_BY_COMMAND[code](**payload))
            assert verify_structure(scheduler, op, reply, before, expected)["effective"]
            assert len(before.index) == (10011 if i == 0 or kind != "load" else 11012)

    asyncio.run(exercise())


@pytest.mark.parametrize("state", ["submitted", "active", "unknown_submission"])
def test_normal_handler_replace_protects_each_running_state(state):
    from takler.protocol.commands import Command, InitCommand, ReplaceCommand
    from takler.server.handlers import CommandHandlers
    from takler.server.scheduler import Scheduler

    async def exercise():
        bunch, paths, *_ = make_fixture(10000, True)
        scheduler = Scheduler(bunch)
        handler = CommandHandlers(scheduler)
        task = bunch.find_node(paths[0])
        task.run()
        if state == "active":
            assert (
                await handler.handle(
                    Command.INIT,
                    InitCommand(
                        node_path=task.node_path,
                        attempt_id=task.attempt_id,
                        task_id="active",
                    ),
                )
            ).flag == 0
        elif state == "unknown_submission":
            task.mark_submission_unknown("test")
        assert task.state.node_status.name == (
            "active" if state == "active" else "submitted"
        )
        scheduler.version_store.flush()
        before = scheduler.version_store.pin()
        raw = structural_documents()["replace-A"].replace(b"flow009", b"flow000")
        reply = await handler.handle(
            Command.REPLACE, ReplaceCommand(target_path="/flow000", flow_bytes=raw)
        )
        assert reply.flag != 0 and scheduler.version_store.pin().index == before.index

    asyncio.run(exercise())


if __name__ == "__main__":
    assert sys.argv[1] == "--fixture-case"
    fixture_case(Path(sys.argv[2]))


def test_checkpoint_archive_retains_sequence_and_bounds_roots(tmp_path):
    import time
    from takler.server.scheduler import Scheduler
    from takler.server.checkpoint import CheckpointManager
    from r3_14_perf04_verify import CheckpointAudit
    from r3_14_perf08a_hooks import EXIT_SAVE

    bunch, *_ = make_fixture(1000)
    scheduler = Scheduler(bunch)
    manager = CheckpointManager(
        bunch, checkpoint_file=tmp_path / "main.check", interval=10
    )
    manager.query_scheduler = scheduler
    records = []
    audit = CheckpointAudit(manager, tmp_path, records.append).install()
    assert manager.write_checkpoint()
    token = EXIT_SAVE.set(True)
    try:
        assert manager.write_checkpoint()
    finally:
        EXIT_SAVE.reset(token)
    audit.close(time.monotonic_ns() + 2 * 1_000_000_000)
    assert len(audit.verify_final(lambda: None)) == 2 and not audit.pending
    links = [r for r in records if r["kind"] == "checkpoint_link"]
    archives = [r for r in records if r["kind"] == "checkpoint_archive"]
    assert [r["checkpoint_sequence"] for r in links] == [1, 2]
    assert [r["exit_save"] for r in links] == [False, True]
    assert len(archives) == 2 and all(
        (tmp_path / r["file"]).stat().st_mode & 0o077 == 0 for r in archives
    )
    released = audit.release()
    assert not released["pending_sequences"] and audit.main is audit.backup is None
