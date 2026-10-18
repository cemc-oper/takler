"""Real collector IPC and deterministic receipt faults; no Takler service starts."""

import hashlib
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import mix100k01 as mix
from r3_14_observation import Emitter, MAX_PACKET_RECORDS, RECORD

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
PID = 123456


def frame(*sequences, pid=PID, kind=9):
    return b"".join(
        RECORD.pack(s, pid, 1, kind, 1, 0, 0, *([0] * 8), b"") for s in sequences
    )


def final_counts(produced, dropped=0, **changes):
    return dict(
        produced=produced,
        emitted=produced - dropped,
        dropped=dropped,
        sender_stopped=True,
        abandoned_ring_records=0,
        **changes,
    )


def setup_receipt(directory, produced=None):
    mix.save(directory / "registry.json", dict(entries=[dict(pid=PID, role="server")]))
    if produced is not None:
        mix.save(
            directory / "server-final.json", dict(observation=final_counts(produced))
        )
    log = mix.Ledger(directory / "events.jsonl")
    return mix.ObservationReceipt(directory, log), log


def test_reordering_preserves_arrival_order_and_closes_only_contiguous_prefix(tmp_path):
    receipt, log = setup_receipt(tmp_path, 4)
    try:
        receipt.accept(frame(2, 4))
        assert receipt.state()["contiguous"] == 0
        assert receipt.state()["pending_count"] == 2
        receipt.accept(frame(1, 3))
        result = receipt.finish()
    finally:
        log.close()
    assert result["sequence_accounting_verified"]
    assert result["sequences"] == {str(PID): 4}
    assert [r["event"]["sequence"] for r in mix.rows(tmp_path / "events.jsonl")] == [
        2,
        4,
        1,
        3,
    ]
    assert not (tmp_path / "collector-anomaly.bin").exists()


def test_window_bound_and_boundary_sequence(tmp_path):
    receipt, log = setup_receipt(tmp_path, 8192)
    try:
        for begin in range(2, 8193, 128):
            receipt.accept(frame(*range(begin, min(8193, begin + 128))))
        assert receipt.state()["pending_count"] == 8191
        receipt.accept(frame(1))
        assert receipt.state()["pending_count"] == 0
        assert receipt.finish()["sequences"] == {str(PID): 8192}
    finally:
        log.close()


@pytest.mark.parametrize(
    "prefix,bad,category,index",
    [
        ([], [1, 1, 3], "duplicate", 1),
        ([1], [1, 2], "duplicate", 0),
        ([2], [2, 1], "duplicate", 0),
        ([], [8193, 1], "out_of_window", 0),
        ([], [0, 1], "invalid_sequence", 0),
    ],
)
def test_rejected_frame_keeps_offender_and_unprocessed_suffix(
    tmp_path, prefix, bad, category, index
):
    receipt, log = setup_receipt(tmp_path)
    packet = frame(*bad)
    try:
        if prefix:
            receipt.accept(frame(*prefix))
        with pytest.raises(mix.ReceiptError) as error:
            receipt.accept(packet)
    finally:
        log.close()
    assert error.value.category == category
    metadata = mix.read(tmp_path / "collector-anomaly.json")
    assert metadata["record_index"] == index
    assert metadata["offending_sequence"] == bad[index]
    assert metadata["frame_role"] == "offending_frame"
    assert metadata["sha256"] == hashlib.sha256(packet).hexdigest()
    assert (tmp_path / "collector-anomaly.bin").read_bytes() == packet


@pytest.mark.parametrize(
    "sequences,produced,first_missing",
    [([1, 3], 3, 2), ([1, 2], 3, 3), ([2, 3], 3, 1), ([], 1, 1)],
)
def test_gaps_and_missing_tail_need_stopped_sender_final(
    tmp_path, sequences, produced, first_missing
):
    receipt, log = setup_receipt(tmp_path, produced)
    try:
        if sequences:
            receipt.accept(frame(*sequences))
        with pytest.raises(mix.ReceiptError) as error:
            receipt.finish()
    finally:
        log.close()
    assert error.value.category == "missing"
    metadata = mix.read(tmp_path / "collector-anomaly.json")
    assert metadata["receipt"]["first_unreceived"] == first_missing
    assert metadata["frame_role"] == "last_received_frame"
    assert metadata["frame_present"] == bool(sequences)
    assert (tmp_path / "collector-anomaly.bin").read_bytes() == (
        frame(*sequences) if sequences else b""
    )


@pytest.mark.parametrize(
    "observation,category",
    [
        (None, "invalid_final"),
        (
            dict(
                produced=1,
                emitted=1,
                dropped=0,
                sender_stopped=False,
                abandoned_ring_records=0,
            ),
            "unknown_final",
        ),
        (
            dict(
                produced=1,
                emitted=2,
                dropped=0,
                sender_stopped=True,
                abandoned_ring_records=0,
            ),
            "invalid_final",
        ),
        (
            dict(
                produced=True,
                emitted=1,
                dropped=0,
                sender_stopped=True,
                abandoned_ring_records=0,
            ),
            "invalid_final",
        ),
        (final_counts(2, dropped=1), "missing"),
    ],
)
def test_final_accounting_does_not_hide_loss_or_unknown(
    tmp_path, observation, category
):
    receipt, log = setup_receipt(tmp_path)
    mix.save(tmp_path / "server-final.json", dict(observation=observation))
    try:
        receipt.accept(frame(1))
        with pytest.raises(mix.ReceiptError) as error:
            receipt.finish()
    finally:
        log.close()
    assert error.value.category == category
    assert mix.read(tmp_path / "collector-anomaly.json")["category"] == category


def test_absent_producer_final_is_unknown_not_success(tmp_path):
    receipt, log = setup_receipt(tmp_path)
    try:
        receipt.accept(frame(1))
        with pytest.raises(mix.ReceiptError) as error:
            receipt.finish()
    finally:
        log.close()
    assert error.value.category == "unknown_final"


def test_unregistered_producer_does_not_allocate_more_windows(tmp_path):
    receipt, log = setup_receipt(tmp_path)
    try:
        with pytest.raises(mix.ReceiptError) as error:
            receipt.accept(frame(1, pid=PID + 1))
    finally:
        log.close()
    assert error.value.category == "producer_identity"
    assert receipt.window.received == 0
    assert receipt.state()["pid"] == PID


def test_decode_failure_retains_exact_record_position(tmp_path):
    receipt, log = setup_receipt(tmp_path)
    packet = frame(1) + frame(2, kind=0) + frame(3)
    try:
        with pytest.raises(mix.ReceiptError) as error:
            receipt.accept(packet)
    finally:
        log.close()
    assert error.value.category == "malformed_frame"
    metadata = mix.read(tmp_path / "collector-anomaly.json")
    assert metadata["record_index"] == 1
    assert metadata["offending_sequence"] is None
    assert (tmp_path / "collector-anomaly.bin").read_bytes() == packet


def test_anomaly_retention_failure_is_fatal(tmp_path, monkeypatch):
    receipt, log = setup_receipt(tmp_path)
    save = mix.save

    def fail_metadata(path, value):
        if path.name == "collector-anomaly.json":
            raise OSError("controlled metadata write failure")
        save(path, value)

    monkeypatch.setattr(mix, "save", fail_metadata)
    try:
        with pytest.raises(mix.ReceiptError) as error:
            receipt.accept(frame(1, 1))
    finally:
        log.close()
    assert error.value.category == "anomaly_retention_failed"
    assert "duplicate" in str(error.value)
    assert (tmp_path / "collector-anomaly.bin").read_bytes() == frame(1, 1)


@pytest.mark.parametrize(
    "sequences,produced,category",
    [
        ([2, 1], 2, None),
        ([1, 1, 2], 2, "duplicate"),
        ([1, 3], 3, "missing"),
        ([], 1, "missing"),
    ],
)
def test_actual_collector_final_drain_uses_same_receipt(
    tmp_path, monkeypatch, sequences, produced, category
):
    # Actual collector entry, deterministically queued at the withdrawal boundary.
    # Substituting the socket avoids racing the test against the stop marker.
    import select

    packets = [frame(*sequences)] if sequences else []
    fake = SimpleNamespace(
        setsockopt=lambda *a: None,
        bind=lambda *a: None,
        setblocking=lambda *a: None,
        close=lambda: None,
        recvmsg=lambda *a: (packets.pop(0), [], 0, None),
    )
    monkeypatch.setattr(mix.socket, "socket", lambda *a: fake)
    monkeypatch.setattr(
        select, "select", lambda *a: ([fake] if packets else [], [], [])
    )
    mix.save(tmp_path / "config.json", dict(socket="substituted"))
    mix.save(tmp_path / "registry.json", dict(entries=[dict(pid=PID, role="server")]))
    mix.save(tmp_path / "server-final.json", dict(observation=final_counts(produced)))
    mix.save(tmp_path / "stop-collector.json", {})
    if category:
        with pytest.raises(mix.ReceiptError):
            mix.collector(tmp_path)
        assert mix.read(tmp_path / "collector-failure.json")["category"] == category
        assert not (tmp_path / "collector-final.json").exists()
        assert (tmp_path / "collector-anomaly.bin").read_bytes() == (
            frame(*sequences) if sequences else b""
        )
    else:
        mix.collector(tmp_path)
        final = mix.read(tmp_path / "collector-final.json")
        assert final["sequence_accounting_verified"]
        assert final["sequences"] == {str(PID): produced}


def test_no_frames_and_zero_produced_is_a_verified_empty_stream(tmp_path):
    receipt, log = setup_receipt(tmp_path, 0)
    try:
        assert receipt.finish()["sequences"] == {str(PID): 0}
    finally:
        log.close()


def test_coordinator_exit_gate_rejects_unverified_final(tmp_path):
    mix.save(
        tmp_path / "collector-final.json", dict(sequences={str(PID): 1}, released=True)
    )
    process = SimpleNamespace(poll=lambda: 0, returncode=0)
    with pytest.raises(mix.ReceiptError) as exc:
        mix.finish_collector(tmp_path, process, PID, 1, time.monotonic_ns() + mix.NS)
    assert exc.value.category == "invalid_final"


def _collector_case(directory, case):
    """Controlled child helper; its collector stays in the parent's owned group."""
    with tempfile.TemporaryDirectory(prefix="mix-receipt-") as sockets:
        address = str(Path(sockets) / "events.sock")
        mix.save(
            directory / "config.json",
            dict(
                socket=address, coordinator_pid=os.getpid(), supervisor_pid=os.getppid()
            ),
        )
        mix.save(
            directory / "registry.json",
            dict(entries=[dict(pid=os.getpid(), role="server", ended_ns=None)]),
        )
        with (directory / "collector.log").open("w") as output:
            proc = subprocess.Popen(
                [
                    sys.executable,
                    str(SCRIPTS / "mix100k01.py"),
                    "collector",
                    "--directory",
                    str(directory),
                ],
                stdout=output,
                stderr=subprocess.STDOUT,
            )
            deadline = time.monotonic() + 12

            def wait(predicate):
                while not predicate():
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            "collector helper including cleanup deadline"
                        )
                    time.sleep(0.005)

            try:
                wait(
                    lambda: (
                        (directory / "collector-ready.json").exists()
                        or proc.poll() is not None
                    )
                )
                assert proc.poll() is None, (directory / "collector.log").read_text()
                with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sender:
                    if case == "reentrant_threads":
                        emitter = Emitter(address, start_sender=False)
                        queue = emitter.ready

                        def put(record):
                            if RECORD.unpack(record)[0] == 1:
                                emitter.emit(
                                    9
                                )  # Deterministic GC-style reentry: enqueue 2 before 1.
                            queue.put(record)

                        emitter.ready = SimpleNamespace(
                            put=put,
                            get_nowait=queue.get_nowait,
                            empty=queue.empty,
                            qsize=queue.qsize,
                        )
                        emitter.emit(9)
                        workers = [
                            threading.Thread(
                                target=lambda: [emitter.emit(9) for _ in range(64)]
                            )
                            for _ in range(4)
                        ]
                        for worker in workers:
                            worker.start()
                        for worker in workers:
                            worker.join(max(0, deadline - time.monotonic()))
                            assert not worker.is_alive()
                        observation = emitter.close()
                        assert observation["produced"] == observation["emitted"] == 258
                        mix.save(
                            directory / "server-final.json",
                            dict(observation=observation),
                        )
                        wait(
                            lambda: (
                                (directory / "collector-health.json").exists()
                                and mix.read(directory / "collector-health.json")[
                                    "receipt"
                                ]["received"]
                                == 258
                            )
                        )
                    else:
                        packet = {
                            "missing": frame(1, 3, pid=os.getpid()),
                            "producer_drop": frame(1, pid=os.getpid()),
                            "duplicate": frame(1, 1, 3, pid=os.getpid()),
                            "oversize": frame(
                                *([1] * (MAX_PACKET_RECORDS + 1)), pid=os.getpid()
                            ),
                            "malformed": b"broken",
                        }[case]
                        sender.sendto(packet, address)
                        if case in {"missing", "producer_drop"}:
                            expected_received = 2 if case == "missing" else 1
                            wait(
                                lambda: (
                                    (directory / "collector-health.json").exists()
                                    and mix.read(directory / "collector-health.json")[
                                        "receipt"
                                    ]["received"]
                                    == expected_received
                                )
                            )
                            health = mix.read(directory / "collector-health.json")
                            assert health["receipt"]["contiguous"] == 1
                            assert health["receipt"]["pending_count"] == (
                                1 if case == "missing" else 0
                            )
                            assert (
                                proc.poll() is None
                            )  # Open gap alone is not proven loss.
                            mix.save(
                                directory / "server-final.json",
                                dict(
                                    observation=final_counts(3)
                                    if case == "missing"
                                    else final_counts(2, dropped=1)
                                ),
                            )
                    if case in {"reentrant_threads", "missing", "producer_drop"}:
                        produced = {
                            "reentrant_threads": 258,
                            "missing": 3,
                            "producer_drop": 2,
                        }[case]
                        try:
                            mix.finish_collector(
                                directory,
                                proc,
                                os.getpid(),
                                produced,
                                int(deadline * mix.NS),
                            )
                        except mix.ReceiptError as exc:
                            assert (
                                case != "reentrant_threads"
                                and exc.category == "missing"
                            )
                        else:
                            assert case == "reentrant_threads"
                    wait(lambda: proc.poll() is not None)
                    if case == "reentrant_threads":
                        final = mix.read(directory / "collector-final.json")
                        assert (
                            proc.returncode == 0
                            and final["sequence_accounting_verified"]
                        )
                        assert final["sequences"] == {str(os.getpid()): 258}
                        assert final["receipts"][str(os.getpid())]["pending_count"] == 0
                        arrivals = [
                            r["event"]["sequence"]
                            for r in mix.rows(directory / "events.jsonl")
                        ]
                        assert arrivals[:2] == [2, 1] and sorted(arrivals) == list(
                            range(1, 259)
                        )
                    else:
                        category = (
                            "malformed_frame"
                            if case in {"oversize", "malformed"}
                            else "missing"
                            if case == "producer_drop"
                            else case
                        )
                        assert proc.returncode != 0
                        assert (
                            mix.read(directory / "collector-failure.json")["category"]
                            == category
                        )
                        anomaly = mix.read(directory / "collector-anomaly.json")
                        raw = (directory / "collector-anomaly.bin").read_bytes()
                        assert raw == packet[: RECORD.size * MAX_PACKET_RECORDS + 1]
                        assert anomaly["sha256"] == hashlib.sha256(raw).hexdigest()
                        assert anomaly["datagram_truncated"] == (case == "oversize")
                        assert not (directory / "collector-final.json").exists()
                mix.save(
                    directory / "helper-result.json",
                    dict(
                        case=case,
                        collector_exit_code=proc.returncode,
                        collector_command=proc.args,
                        service_starts=0,
                    ),
                )
            finally:
                if proc.poll() is None:
                    proc.kill()
                proc.wait(timeout=2)


@pytest.mark.parametrize(
    "case",
    [
        "reentrant_threads",
        "missing",
        "producer_drop",
        "duplicate",
        "oversize",
        "malformed",
    ],
)
def test_real_collector_ipc_combination(tmp_path, case):
    # Hard outer supervisor <=20s; kill the entire owned helper/collector group.
    proc = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--collector-case",
            str(tmp_path),
            case,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate(timeout=2)
        raise
    assert proc.returncode == 0, (
        stdout + stderr + (tmp_path / "collector.log").read_text()
    )
    assert mix.read(tmp_path / "helper-result.json")["service_starts"] == 0


if __name__ == "__main__":
    assert sys.argv[1] == "--collector-case"
    _collector_case(Path(sys.argv[2]), sys.argv[3])
