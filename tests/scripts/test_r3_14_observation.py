"""Tool-risk regression tests, independent of formal performance acceptance."""

import ast
import asyncio
import gc
import importlib
from pathlib import Path
import socket
import sys
import threading
import time
from types import SimpleNamespace

import pytest


SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS))
observation = importlib.import_module("r3_14_observation")
probe = importlib.import_module("r3_14_isolated_probe")


def test_lag_grid_skips_blocked_ticks_without_zero_backfill():
    target = 1_000_000_000
    following, lag, missed = observation.next_grid(target, target + 27_000_000)
    assert (lag, missed) == (27_000_000, 5)
    assert following == target + 30_000_000
    assert observation.next_grid(following, following)[0] == target + 35_000_000


def test_batched_and_partial_control_messages_are_not_hidden_in_text_buffer():
    from r3_14_isolated_worker import read_commands
    import os

    read_fd, write_fd = os.pipe()
    try:
        buffer = bytearray()
        os.write(write_fd, b'{"command":"track"}\n{"command":"clock"}\n{"command":')
        assert read_commands(read_fd, buffer) == [
            {"command": "track"},
            {"command": "clock"},
        ]
        os.write(write_fd, b'"stop"}\n')
        assert read_commands(read_fd, buffer) == [{"command": "stop"}]
        assert not buffer
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_rpc_close_value_error_does_not_hide_or_invent_integrity_failure():
    from r3_14_isolated_worker import IntegrityError, semantic_failure

    assert not semantic_failure(ValueError("Cannot invoke RPC on closed channel"))
    assert semantic_failure(IntegrityError("full snapshot node count failure"))


def test_ring_overflow_lock_reentry_and_sequence_evidence(tmp_path):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    path = str(tmp_path / "s")
    sock.bind(path)
    sock.settimeout(1)
    emitter = observation.Emitter(path, 2, start_sender=False)
    assert emitter.emit(9)
    assert emitter.emit(9)
    assert not emitter.emit(9)
    assert not emitter.emit(2, a=0)
    final = emitter.close()
    records = list(observation.decode_packet(sock.recv(512)))
    assert [r["sequence"] for r in records] == [1, 2]
    assert (final["produced"], final["emitted"], final["dropped"]) == (4, 2, 2)
    assert final["buffer_peak"] == 2
    assert final["sender_stopped"]
    sock.close()


def test_stalled_and_dead_collector_cannot_block_producers(tmp_path):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    path = str(tmp_path / "s")
    sock.bind(path)
    emitter = observation.Emitter(path, 256)
    started = time.monotonic()
    for _ in range(10000):
        emitter.emit(9)
    sock.close()
    for _ in range(10000):
        emitter.emit(9)
    final = emitter.close()
    assert time.monotonic() - started < 3
    assert final["produced"] == 20000
    assert final["dropped"] > 0
    assert final["emitted"] + final["dropped"] == 20000
    assert final["buffer_peak"] <= 256
    assert final["sender_stopped"]


def test_full_size_observation_allocations_remain_bounded(tmp_path):
    import tracemalloc

    tracemalloc.start()
    try:
        emitter = observation.Emitter(
            str(tmp_path / "absent"), 8192, start_sender=False
        )
        for _ in range(10000):
            emitter.emit(9)
        held_first, peak_first = tracemalloc.get_traced_memory()
        for _ in range(50000):
            emitter.emit(9)
        held_last, peak_last = tracemalloc.get_traced_memory()
        assert peak_last < 8 * 1024 * 1024
        assert held_last - held_first < 65536
        assert peak_last - peak_first < 65536
        assert emitter.close()["buffer_peak"] == 8192
    finally:
        tracemalloc.stop()


def test_multithread_sequences_and_gc_callback_reentry(tmp_path):
    emitter = observation.Emitter(str(tmp_path / "absent"), 256)

    def callback(phase, info):
        emitter.emit(2 if phase == "start" else 3, a=info["generation"])

    gc.callbacks.append(callback)
    try:
        gc.collect(0)  # Deliberate tool test, never a performance-run intervention.
        threads = [
            threading.Thread(target=lambda: [emitter.emit(9) for _ in range(1000)])
            for _ in range(4)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)
            assert not thread.is_alive()
    finally:
        gc.callbacks.remove(callback)
    final = emitter.close()
    assert final["produced"] >= 4002
    assert final["produced"] == final["emitted"] + final["dropped"]


def test_reentrant_record_has_a_distinct_slot(tmp_path):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    path = str(tmp_path / "s")
    sock.bind(path)
    sock.settimeout(1)
    emitter = observation.Emitter(path, 256, start_sender=False)
    assert emitter.emit(2, a=0)
    assert emitter.emit(3, a=0)
    final = emitter.close()
    records = list(observation.decode_packet(sock.recv(512)))
    assert [r["kind"] for r in records] == ["gc_start", "gc_stop"]
    assert [r["sequence"] for r in records] == [1, 2]
    assert final["drop_reasons"]["contention"] == 0
    sock.close()


def test_gc_diagnostic_copy_removes_only_product_interventions(tmp_path):
    repo = SCRIPTS.parent
    before = (repo / "src/takler/server/full_snapshot.py").read_text()
    probe.diagnostic_copy(repo, tmp_path / "diagnostic")
    after = (tmp_path / "diagnostic/src/takler/server/full_snapshot.py").read_text()
    assert (repo / "src/takler/server/full_snapshot.py").read_text() == before
    tree = ast.parse(after)
    service = next(
        n
        for n in tree.body
        if isinstance(n, ast.ClassDef) and n.name == "FullSnapshotService"
    )
    calls = [ast.unparse(n.func) for n in ast.walk(service) if isinstance(n, ast.Call)]
    assert "gc.collect" not in calls
    assert "_acquire_large_snapshot_gc" not in calls
    assert "_release_large_snapshot_gc" not in calls
    assert "lease.expiry_handle.cancel" in calls
    assert "self._drop" in calls
    assert (tmp_path / "diagnostic/gc.patch").exists()


def test_close_restores_checkpoint_methods_and_gc_callbacks(tmp_path):
    async def run():
        async def write():
            return True

        manager = SimpleNamespace(
            _pin_fixed_for_write=lambda: (1, object(), ""),
            _publish_prepared=lambda *args: (True, True, None),
            write_checkpoint_async=write,
        )
        originals = (manager._pin_fixed_for_write, manager._publish_prepared, write)
        server = SimpleNamespace(checkpoint_manager=manager)
        emitter = observation.Emitter(str(tmp_path / "absent"), 256)
        callbacks = list(gc.callbacks)
        hook = observation.ServiceObservation(server, emitter).install()
        assert hook.gc_callback in gc.callbacks
        await manager.write_checkpoint_async()
        await hook.close()
        assert gc.callbacks == callbacks
        assert (
            manager._pin_fixed_for_write,
            manager._publish_prepared,
            manager.write_checkpoint_async,
        ) == originals
        assert all(t.done() for t in hook.tasks)
        assert emitter.close()["sender_stopped"]

    asyncio.run(run())


@pytest.mark.parametrize("reuse", [False, True])
def test_fixed_grid_callback_records_actual_block_and_cancels_timer(tmp_path, reuse):
    async def run():
        async def write():
            return True

        manager = SimpleNamespace(
            _pin_fixed_for_write=lambda: (1, object(), ""),
            _publish_prepared=lambda *args: (True, True, None),
            write_checkpoint_async=write,
            _background_writes=[],
            _background_reserved=0,
        )
        query = SimpleNamespace(
            full_snapshots=SimpleNamespace(_leases={}), _snapshots={}
        )
        server = SimpleNamespace(
            checkpoint_manager=manager, scheduler=SimpleNamespace(query_service=query)
        )
        emitter = observation.Emitter(str(tmp_path / "absent"), 256, start_sender=False)
        hook = observation.ServiceObservation(server, emitter).install()
        hook.reuse_lag_timer = reuse
        original_handle = hook.lag_handle
        independent = asyncio.Event()
        asyncio.get_running_loop().call_later(0.018, independent.set)
        await asyncio.sleep(0.01)
        time.sleep(0.027)
        await asyncio.sleep(0.01)
        assert independent.is_set()
        if hook.reuse_lag_timer:
            assert hook.lag_handle is original_handle
        assert asyncio.get_running_loop()._scheduled.count(hook.lag_handle) <= 1
        await hook.close()
        raw = []
        while not emitter.ready.empty():
            record = emitter.ready.get_nowait()
            raw.append(observation.decode_record(bytes(record)))
            emitter.available.put(record)
        lag = [r for r in raw if r["kind"] == "loop_lag"]
        assert any(r["values"][1] >= 4 for r in lag)
        assert len({r["start_ns"] % 5_000_000 for r in lag}) == 1
        for previous, current in zip(lag, lag[1:]):
            assert (
                current["start_ns"]
                == previous["start_ns"] + (previous["values"][1] + 1) * 5_000_000
            )
        assert hook.lag_handle.cancelled()
        emitter.close()

    asyncio.run(run())


@pytest.mark.parametrize("payload", [b"", b"x" * 255, b"x" * (129 * 256)])
def test_packet_framing_rejects_partial_and_unbudgeted_payload(payload):
    with pytest.raises(ValueError):
        list(observation.decode_packet(payload))


def test_cleanup_releases_worker_when_service_leader_is_killed(tmp_path):
    import os
    import signal

    code = "import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(120)']); print(p.pid,flush=True); time.sleep(120)"
    process = probe.Process(
        "orphan-test", [sys.executable, "-c", code], dict(os.environ), tmp_path
    )
    try:
        worker = int(process.proc.stdout.readline())
        os.kill(process.pid, signal.SIGKILL)
        process.proc.wait(timeout=2)
        result = process.cleanup(timeout=1)
        assert worker in result["forced"]
        assert result["remaining_owned_descendants"] == []
    finally:
        if process.proc.poll() is None:
            process.cleanup(timeout=1)


@pytest.mark.parametrize("capacity", [0, 8193])
def test_reject_unbudgeted_ring(capacity):
    with pytest.raises(ValueError):
        observation.Emitter("/unused", capacity)
