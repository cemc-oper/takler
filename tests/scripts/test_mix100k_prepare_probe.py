"""No-service regressions for preparation timing boundaries and legality."""

import asyncio
import inspect
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import mix100k_prepare_probe as probe


@pytest.mark.parametrize("async_body", [False, True])
@pytest.mark.parametrize("failure", [None, "body", "flush"])
@pytest.mark.parametrize("nested_flush", [False, True])
def test_timing_accounts_for_awaits_overlap_and_exceptions(
    monkeypatch, async_body, failure, nested_flush
):
    clock = [0]
    monkeypatch.setattr(probe.time, "monotonic_ns", lambda: clock[0])
    store = SimpleNamespace(
        _pending={},
        _needs_rebuild=False,
        ready=True,
        error=None,
        current=SimpleNamespace(state_version=1, index={"/": 0}),
    )

    def flush():
        clock[0] += 1000
        if failure == "flush":
            raise RuntimeError("flush failed")

    store.flush = flush
    scheduler = SimpleNamespace(version_store=store)

    class Handlers:
        def __init__(self):
            self.scheduler = scheduler

        def _run(self, command, request):
            def work():
                clock[0] += 50
                if nested_flush:
                    store.flush()
                if failure == "body":
                    raise RuntimeError("body failed")
                return SimpleNamespace(flag=0)

            async def later():
                await asyncio.sleep(0)
                return work()

            return later() if async_body else work()

        async def handle(self, command, request):
            clock[0] += 3
            try:
                value = self._run(command, request)
                return await value if inspect.isawaitable(value) else value
            finally:
                store.flush()
                clock[0] += 7

    monkeypatch.setattr(probe, "CommandHandlers", Handlers)
    original_handle, original_run = Handlers.handle, Handlers._run
    events = []
    with probe.timed_handlers(
        scheduler, lambda kind, **v: events.append(dict(kind=kind, **v))
    ):
        with asyncio.Runner() as runner:
            call = Handlers().handle(
                SimpleNamespace(value="init" if async_body else "run"),
                SimpleNamespace(node_path="/flow/task"),
            )
            if failure:
                with pytest.raises(RuntimeError):
                    runner.run(call)
            else:
                assert runner.run(call).flag == 0
    assert Handlers.handle is original_handle and Handlers._run is original_run
    assert store.flush is flush
    row = events[-1]
    assert row["kind"] == "command_completed"
    assert row["body_exclusive_ns"] == 50
    assert row["flush_ns"] == 1000 * (2 if nested_flush else 1)
    assert (
        row["body_exclusive_ns"] + row["flush_ns"] + row["remainder_ns"]
        == row["total_ns"]
    )
    assert row["remainder_ns"] == (3 if failure == "flush" else 10)
    assert row["error"] == ("RuntimeError" if failure else None)


def test_real_worker_entry_prepares_and_verifies_without_services(
    tmp_path, monkeypatch
):
    from takler.server import TaklerServer

    async def forbidden(*args, **kwargs):
        raise AssertionError("network services must not start")

    monkeypatch.setattr(TaklerServer, "start", forbidden)
    probe.save(tmp_path / "config.json", dict(tasks=1000, nodes=1002))
    probe.save(
        tmp_path / "lifecycle.json",
        dict(operational_deadline_ns=probe.time.monotonic_ns() + 30 * probe.NS),
    )
    with asyncio.Runner() as runner:
        result = runner.run(probe.worker(tmp_path))
    assert result["success"], result
    assert result["pool"]["verified"] and result["pool"]["pool"] == 250
    initial = probe.read(tmp_path / "initial-state.json")
    assert not initial["grpc_started"] and not initial["checkpoint_task"]
    assert not (tmp_path / "unused.check").exists()
    from mix100k01 import rows

    events = list(rows(tmp_path / "commands.jsonl"))
    completed = [r for r in events if r["kind"] == "command_completed"]
    assert len(completed) == 500
    assert [r["command"] for r in completed] == ["run", "init"] * 250
    assert all(r["flag"] == 0 and not r["error"] for r in completed)
    assert all(len(r["body"]) == len(r["flush"]) == 1 for r in completed)
    assert all(r["overlap_ns"] == 0 and r["remainder_ns"] >= 0 for r in completed)


def test_run_rejects_a_second_attempt_before_any_child(tmp_path):
    probe.save(tmp_path / "config.json", {})
    (tmp_path / "diagnostic").mkdir()
    probe.save(tmp_path / "diagnostic/source-index.json", {})
    probe.save(tmp_path / "lifecycle.json", {})
    with pytest.raises(ValueError, match="already attempted"):
        probe.run(tmp_path)
    assert not (tmp_path / "registry.json").exists()


def test_config_tamper_is_rejected_before_any_child(tmp_path):
    probe.save(tmp_path / "config.json", {})
    (tmp_path / "config.sha256").write_text("wrong-digest\n")
    (tmp_path / "diagnostic").mkdir()
    probe.save(tmp_path / "diagnostic/source-index.json", {})
    with pytest.raises(ValueError, match="config changed"):
        probe.run(tmp_path)
    assert not (tmp_path / "registry.json").exists()
