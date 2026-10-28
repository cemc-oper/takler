"""Paired takflow -> takler -> real orvix local -> Go child exercise."""

import asyncio
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

pytest.importorskip("takflow")

from takler.core import NodeStatus, ServerStatus  # noqa: E402
from takler.tasks.shell import shell_runner  # noqa: E402
from takflow.backends.runtime.orvix import orvix_job  # noqa: E402
from takflow.backends.takler.backend import TaklerBackend  # noqa: E402
from takflow.flow import WorkflowEngine  # noqa: E402
from takflow.jobspec.model import ResourceSpec  # noqa: E402
from takflow.jobspec.render import render_resource_block  # noqa: E402


pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX shell")


def _wait_until(predicate, description, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.03)
    pytest.fail(f"timed out waiting for {description}")


def _record(name, data):
    directory = os.environ.get("R3_13_EVIDENCE_DIR")
    if directory:
        destination = Path(directory)
        destination.mkdir(parents=True, exist_ok=True)
        (destination / f"{name}.json").write_text(
            json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )


def _paired_binaries(monkeypatch):
    orvix = os.environ.get("ORVIX_BIN")
    client = os.environ.get("TAKLER_CLIENT_BIN")
    if (
        not orvix
        or not Path(orvix).is_file()
        or not client
        or not Path(client).is_file()
    ):
        pytest.skip("set ORVIX_BIN and TAKLER_CLIENT_BIN to built binaries")
    monkeypatch.setenv("PATH", f"{Path(orvix).parent}:{os.environ['PATH']}")
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost,::1")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost,::1")
    return Path(orvix), Path(client)


def _definition(tmp_path, client, preinit_first=False):
    engine = WorkflowEngine(TaklerBackend())
    suite = engine.Suite("paired")
    suite.add_variable("TAKLER_HOME", str(tmp_path / "jobs with spaces"))
    suite.add_limit("slots", 1)
    suite.add_repeat_date("DAY", 20261011, 20261011)
    scripts = tmp_path / "source scripts"
    scripts.mkdir()
    for name in ("first", "second"):
        task = suite.add_task(name)
        task.add_inlimit("slots")
        task.add_event("ready")
        task.add_meter("step", 0, 10)
        source = scripts / f"{name}.takler"
        gate = tmp_path / f"release-{name}"
        ledger = tmp_path / "starts.log"
        marker = (
            f'printf \'%s %s %s\\n\' {name} "$TAKLER_ATTEMPT_ID" "$$"'
            f" >> {shlex.quote(str(ledger))}\n"
        )
        wait_gate = f"while [ ! -f {shlex.quote(str(gate))} ]; do sleep 0.05; done\n"
        before_init = preinit_first and name == "first"
        source.write_text(
            "#!/bin/sh\n"
            + render_resource_block(
                ResourceSpec(
                    job_name=name,
                    output="{{ TAKLER_JOBOUT }}",
                    error="{{ TAKLER_JOBOUT }}.err",
                )
            )
            + "\nset -eu\n"
            + "export TAKLER_HOST='{{ TAKLER_HOST }}'\n"
            + "export TAKLER_PORT='{{ TAKLER_PORT }}'\n"
            + "export TAKLER_NAME='{{ TAKLER_NAME }}'\n"
            + "export TAKLER_ATTEMPT_ID='{{ TAKLER_ATTEMPT_ID }}'\n"
            + "export TAKLER_PASS='{{ TAKLER_PASS }}'\n"
            + f"CLIENT={shlex.quote(str(client))}\n"
            + (marker + wait_gate if before_init else "")
            + '"$CLIENT" init --task-id "$$"\n'
            + '"$CLIENT" event --event-name ready\n'
            + '"$CLIENT" meter --meter-name step --meter-value 7\n'
            + ("" if before_init else marker + wait_gate)
            + f"echo done-{name}\n"
            + '"$CLIENT" complete\n',
            encoding="utf-8",
        )
        task.add_variables({"TAKLER_SCRIPT": str(source)})
        task.add_variables(orvix_job(engine, "local"))
    output = tmp_path / "paired.json"
    engine.save_suite(suite, output)
    return output


def test_real_local_child_limit_output_and_restart(
    takler_server, tmp_path, monkeypatch
):
    orvix, child_cli = _paired_binaries(monkeypatch)
    definition = _definition(tmp_path, child_cli)
    client = takler_server.make_client()
    client.start()
    try:
        assert client.run_command_load(str(definition)).flag == 0
        assert client.run_command_begin("paired").flag == 0
        bunch = takler_server.server.bunch
        first = bunch.find_node("/paired/first")
        second = bunch.find_node("/paired/second")
        limit = bunch.find_node("/paired").find_limit("slots")
        ledger = tmp_path / "starts.log"
        _wait_until(lambda: first.state.node_status is NodeStatus.active, "first init")
        _wait_until(
            lambda: ledger.exists() and len(ledger.read_text().splitlines()) == 1,
            "first start",
        )
        _wait_until(lambda: first.find_event("ready").value, "first event")
        _wait_until(lambda: first.find_meter("step").value == 7, "first meter")
        assert limit.value == 1 and second.state.node_status is NodeStatus.queued
        assert second.job_path is None
        _wait_until(lambda: Path(first.orvix_info_path).is_file(), "first info sidecar")
        status = subprocess.run(
            [str(orvix), "status", first.orvix_info_path],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        assert status == "RUNNING"
        (tmp_path / "release-first").touch()
        _wait_until(
            lambda: first.state.node_status is NodeStatus.complete, "first complete"
        )
        _wait_until(
            lambda: second.state.node_status is NodeStatus.active, "second init"
        )
        assert limit.value == 1
        (tmp_path / "release-second").touch()
        _wait_until(
            lambda: second.state.node_status is NodeStatus.complete, "second complete"
        )
        assert limit.value == 0
        assert [line.split()[0] for line in ledger.read_text().splitlines()] == [
            "first",
            "second",
        ]
        for task in (first, second):
            assert Path(task.job_path).is_file()
            assert Path(task.orvix_info_path).is_file()
            assert Path(task.jobout_path).is_file()
            assert Path(task.jobout_path + ".err").is_file()
            assert f"done-{task.name}" in Path(task.jobout_path).read_text()
        lines = ledger.read_text().splitlines()
        infos = {
            task.name: yaml.safe_load(Path(task.orvix_info_path).read_text())
            for task in (first, second)
        }
        assert [infos[name]["job_id"] for name in ("first", "second")] == [
            line.split()[2] for line in lines
        ]
        for task in (first, second):
            info = infos[task.name]
            directives = {item["key"]: item["value"] for item in info["directives"]}
            assert info["script_source"] == task.job_path
            assert directives["output"] == task.jobout_path
            assert directives["error"] == task.jobout_path + ".err"
        old_attempt = first.attempt_id
    finally:
        (tmp_path / "release-first").touch()
        (tmp_path / "release-second").touch()
        client.close_channel()
        takler_server.stop()

    assert takler_server.checkpoint_file.is_file()
    runner = type(takler_server)(checkpoint_file=takler_server.checkpoint_file)
    runner.start()
    try:
        restored = runner.server.bunch
        assert restored.server_state.status is ServerStatus.halted
        assert (
            restored.find_node("/paired/first").state.node_status is NodeStatus.complete
        )
        assert (
            restored.find_node("/paired/second").state.node_status
            is NodeStatus.complete
        )
        assert restored.find_node("/paired").find_limit("slots").value == 0
        time.sleep(0.15)  # Let several scheduler passes observe the restored halt.
        assert len(ledger.read_text().splitlines()) == 2
        control = runner.make_client()
        control.start()
        try:
            assert control.run_command_requeue(["/paired/first"]).flag == 0
            stale = subprocess.run(
                [
                    str(child_cli),
                    "event",
                    "--host",
                    runner.host,
                    "--port",
                    str(runner.port),
                    "--node-path",
                    "/paired/first",
                    "--event-name",
                    "ready",
                    "--attempt-id",
                    old_attempt,
                ],
                capture_output=True,
                text=True,
            )
            assert stale.returncode != 0
            assert "attempt_mismatch" in stale.stdout + stale.stderr
            time.sleep(0.15)
            assert len(ledger.read_text().splitlines()) == 2
            _record(
                "success_and_recovery",
                {
                    "starts": ledger.read_text().splitlines(),
                    "outputs": {
                        task.name: task.jobout_path for task in (first, second)
                    },
                    "orvix_job_ids": {
                        name: infos[name]["job_id"] for name in ("first", "second")
                    },
                    "checkpoint": str(takler_server.checkpoint_file),
                    "restored_status": restored.server_state.status.value,
                    "restored_limit": restored.find_node("/paired")
                    .find_limit("slots")
                    .value,
                    "stale_report_exit": stale.returncode,
                    "starts_after_restart": len(ledger.read_text().splitlines()),
                },
            )
        finally:
            control.close_channel()
    finally:
        runner.stop()


def test_requeued_pending_submit_never_starts_orvix(
    takler_server, tmp_path, monkeypatch
):
    _, child_cli = _paired_binaries(monkeypatch)
    definition = _definition(tmp_path, child_cli)

    async def block_process_slot():
        budget = shell_runner._CommandBudget()
        budget.processes = asyncio.Semaphore(0)
        asyncio.get_running_loop()._takler_command_budget = budget
        return budget

    budget = asyncio.run_coroutine_threadsafe(
        block_process_slot(), takler_server._loop
    ).result(timeout=5)
    client = takler_server.make_client()
    client.start()
    try:
        assert client.run_command_load(str(definition)).flag == 0
        assert client.run_command_suspend(["/paired"]).flag == 0
        assert client.run_command_begin("paired").flag == 0
        assert client.run_command_run(["/paired/first"], force=False).flag == 0
        first = takler_server.server.bunch.find_node("/paired/first")
        limit = takler_server.server.bunch.find_node("/paired").find_limit("slots")
        _wait_until(lambda: len(budget.waiting) == 1, "pending submit slot")
        old_job = first.job_path
        assert first.state.node_status is NodeStatus.submitted and limit.value == 1
        assert client.run_command_requeue(["/paired/first"]).flag == 0
        takler_server._loop.call_soon_threadsafe(budget.processes.release)
        _wait_until(lambda: budget.pending == 0, "stale submit discarded")
        assert first.state.node_status is NodeStatus.queued and limit.value == 0
        assert not Path(old_job + ".info.yaml").exists()
        assert not (tmp_path / "starts.log").exists()
        _record(
            "pending_invalidation",
            {
                "rendered_job": old_job,
                "info_exists": False,
                "process_marker_exists": False,
                "final_status": first.state.node_status.name,
                "final_limit": limit.value,
            },
        )
    finally:
        client.close_channel()


def test_missing_source_aborts_before_submit_and_releases_limit(
    takler_server, tmp_path, monkeypatch
):
    _, child_cli = _paired_binaries(monkeypatch)
    definition = _definition(tmp_path, child_cli)
    (tmp_path / "source scripts" / "first.takler").unlink()
    client = takler_server.make_client()
    client.start()
    try:
        assert client.run_command_load(str(definition)).flag == 0
        assert client.run_command_begin("paired").flag == 0
        bunch = takler_server.server.bunch
        first = bunch.find_node("/paired/first")
        second = bunch.find_node("/paired/second")
        limit = bunch.find_node("/paired").find_limit("slots")
        _wait_until(
            lambda: first.state.node_status is NodeStatus.aborted,
            "definite render failure",
        )
        assert "not a regular file" in first.aborted_reason
        _wait_until(
            lambda: second.state.node_status is NodeStatus.active,
            "second after failure",
        )
        assert limit.value == 1
        (tmp_path / "release-second").touch()
        _wait_until(
            lambda: second.state.node_status is NodeStatus.complete, "second completes"
        )
        assert limit.value == 0
        assert [
            line.split()[0]
            for line in (tmp_path / "starts.log").read_text().splitlines()
        ] == [
            "second",
        ]
        _record(
            "definite_failure",
            {
                "first_abort_reason": first.aborted_reason,
                "starts": (tmp_path / "starts.log").read_text().splitlines(),
                "final_limit": limit.value,
            },
        )
    finally:
        (tmp_path / "release-second").touch()
        client.close_channel()


def test_real_submit_lost_ack_keeps_limit_until_child_report(
    takler_server, tmp_path, monkeypatch
):
    orvix, child_cli = _paired_binaries(monkeypatch)
    definition = _definition(tmp_path, child_cli, preinit_first=True)
    actual_run_process = shell_runner.run_process
    calls = 0

    async def lose_first_ack(*args, **kwargs):
        nonlocal calls
        result = await actual_run_process(*args, **kwargs)
        calls += 1
        if calls == 1:
            raise RuntimeError("submit acknowledgement lost after real orvix exit")
        return result

    monkeypatch.setattr(shell_runner, "run_process", lose_first_ack)
    client = takler_server.make_client()
    client.start()
    try:
        assert client.run_command_load(str(definition)).flag == 0
        assert client.run_command_begin("paired").flag == 0
        bunch = takler_server.server.bunch
        first = bunch.find_node("/paired/first")
        second = bunch.find_node("/paired/second")
        limit = bunch.find_node("/paired").find_limit("slots")
        ledger = tmp_path / "starts.log"
        _wait_until(
            lambda: first.submission_unknown_reason is not None, "unknown submit"
        )
        _wait_until(lambda: ledger.exists(), "real first process marker")
        assert first.state.node_status is NodeStatus.submitted
        assert limit.value == 1 and second.state.node_status is NodeStatus.queued
        assert client.run_command_requeue(["/paired/first"]).flag != 0
        assert [line.split()[0] for line in ledger.read_text().splitlines()] == [
            "first"
        ]
        assert Path(first.orvix_info_path).is_file()
        info = yaml.safe_load(Path(first.orvix_info_path).read_text())
        assert info["job_id"] == ledger.read_text().splitlines()[0].split()[2]
        assert (
            subprocess.run(
                [str(orvix), "status", first.orvix_info_path],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            == "RUNNING"
        )
        unknown_record = {
            "starts_at_unknown": ledger.read_text().splitlines(),
            "first_info": first.orvix_info_path,
            "orvix_job_id": info["job_id"],
            "first_status": first.state.node_status.name,
            "unknown_reason": first.submission_unknown_reason,
            "limit_at_unknown": limit.value,
            "second_status_at_unknown": second.state.node_status.name,
            "orvix_status_at_unknown": "RUNNING",
        }
        (tmp_path / "release-first").touch()
        _wait_until(
            lambda: first.state.node_status is NodeStatus.complete,
            "confirmed child complete",
        )
        assert first.submission_unknown_reason is None
        _wait_until(
            lambda: second.state.node_status is NodeStatus.active, "second starts"
        )
        (tmp_path / "release-second").touch()
        _wait_until(
            lambda: second.state.node_status is NodeStatus.complete, "second completes"
        )
        assert limit.value == 0 and calls == 2
        assert [line.split()[0] for line in ledger.read_text().splitlines()] == [
            "first",
            "second",
        ]
        _record(
            "unknown_then_child_confirmation",
            {
                **unknown_record,
                "starts_final": ledger.read_text().splitlines(),
                "first_final_status": first.state.node_status.name,
                "second_final_status": second.state.node_status.name,
                "final_limit": limit.value,
            },
        )
    finally:
        (tmp_path / "release-first").touch()
        (tmp_path / "release-second").touch()
        client.close_channel()
