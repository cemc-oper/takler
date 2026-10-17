"""External clock envelopes and unchanged metadata return values."""

import importlib
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
clock = importlib.import_module("r3_14_clock_probe")


def test_utc_conversion_avoids_float_epoch_rounding():
    assert clock.utc_ns("2026-10-17T12:34:56.123456+00:00") == 1792240496123456000
    assert clock.utc_ns("1969-12-31T23:59:59.999999+00:00") == -1000


def test_boundary_preserves_original_values_order_and_elapsed(monkeypatch):
    ticks = iter([100, 110, 120, 130, 140, 150, 160, 170])
    monkeypatch.setattr(clock.time, "monotonic_ns", lambda: next(ticks))
    probe = clock.ClockProbe()
    values = ["2026-10-17T00:00:00+00:00", 1.25, "2026-10-17T00:00:02+00:00", 3.5]
    labels = ["start_utc", "started", "end_utc", "ended"]
    actual = [
        probe.read(label, lambda value=value: value)
        for label, value in zip(labels, values)
    ]
    assert all(a is b for a, b in zip(actual, values))
    assert actual[3] - actual[1] == 2.25
    assert [r["seq"] for r in probe.records] == list(range(4))
    assert [r["label"] for r in probe.records] == labels
    assert all(r["width_ns"] == 10 for r in probe.records)
    probe.check()


def test_read_failure_is_evidence_and_stops():
    probe = clock.ClockProbe()
    with pytest.raises(OSError):
        probe.read("raw", lambda: (_ for _ in ()).throw(OSError("unavailable")))
    assert "unavailable" in probe.records[0]["error"]
    with pytest.raises(RuntimeError, match="unavailable"):
        probe.check()


def test_capacity_failure_keeps_bounded_records():
    probe = clock.ClockProbe(capacity=2)
    for _ in range(4):
        probe.read("test", lambda: 1)
    assert len(probe.records) == 2
    with pytest.raises(RuntimeError, match="capacity"):
        probe.check()


def test_backwards_monotonic_stops_without_reset(monkeypatch):
    ticks = iter([100, 90])
    monkeypatch.setattr(clock.time, "monotonic_ns", lambda: next(ticks))
    probe = clock.ClockProbe()
    probe.read("test", lambda: 1)
    assert probe.records[0]["width_ns"] == -10
    with pytest.raises(RuntimeError, match="backwards"):
        probe.check()


def test_wide_envelope_is_retained_not_silently_removed(monkeypatch):
    ticks = iter([0, 2_000_000])
    monkeypatch.setattr(clock.time, "monotonic_ns", lambda: next(ticks))
    probe = clock.ClockProbe()
    probe.read("test", lambda: 1)
    assert probe.records[0]["width_ns"] == 2_000_000
    probe.check()  # Precision classification is external, not a product budget.


def test_supported_native_clocks_and_float_ns_interfaces():
    probe = clock.ClockProbe()
    result = probe.tick(0)
    probe.check()
    assert result["end_seq"] == 6
    for r in probe.records:
        assert r["width_ns"] >= 0 and "error" not in r
        if r["label"] in {"monotonic", "python_monotonic_ns"}:
            assert r["before_ns"] <= r["value"] <= r["after_ns"]
        if r["label"] == "python_monotonic":
            assert r["before_ns"] - 1 <= r["value"] * 1e9 <= r["after_ns"] + 1


def test_original_float_backwards_is_fatal_even_with_forward_ns():
    probe = clock.ClockProbe()
    probe.read("started", lambda: 2.0)
    probe.read("ended", lambda: 1.0)
    with pytest.raises(RuntimeError, match="float moved backwards"):
        probe.check()
