"""G0 T5/T6: owned real S/C/W entries and bounded synthetic input failures."""

import hashlib
import ctypes
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import threading
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_snapshot_control as control


def owner_pidfd_open(pid):
    library = ctypes.CDLL("/usr/lib/x86_64-linux-gnu/libc.so.6", use_errno=True)
    function = library.pidfd_open
    function.argtypes, function.restype = [ctypes.c_int, ctypes.c_uint], ctypes.c_int
    result = function(pid, 0)
    if result < 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))
    return result


def owner_signal(descriptor, signum):
    library = ctypes.CDLL("/usr/lib/x86_64-linux-gnu/libc.so.6", use_errno=True)
    function = library.pidfd_send_signal
    function.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
    function.restype = ctypes.c_int
    if function(descriptor, signum, None, 0) < 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))


class Owner:
    """Independent kernel observer in the existing pytest process."""

    def __init__(self, directory, context, failure_domain=None):
        self.directory, self.context = Path(directory), context
        self.directory.mkdir(parents=True, exist_ok=True)
        self.entries, self.fds, self.exits = {}, {}, {}
        self.rows, self.signals = [], []
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.trigger_ns = None
        self.cleanup_end_ns = context["hard_ns"] - 10 * 10**9
        self.unknown = []
        self.fired = False
        self.error = None
        self.finished = None
        self.failure_domain = failure_domain
        if failure_domain is not None and not context["spec"].get("owner_fixture"):
            raise RuntimeError("owner_failure_domain_not_frozen")
        self.channel, self.child_channel = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_SEQPACKET
        )
        self.channel.setblocking(False)
        self.thread = threading.Thread(target=self.loop, name="independent-owner")
        self.thread.start()

    @staticmethod
    def kernel(pid):
        raw = Path(f"/proc/{pid}/stat").read_text()
        fields = raw.rpartition(")")[2].split()
        return dict(pid=pid, ppid=int(fields[1]), pgid=int(fields[2]),
                    session=int(fields[3]), ticks=int(fields[19]), state=fields[0])

    def append(self, kind, **data):
        if len(self.rows) >= 4096:
            raise RuntimeError("owner_capacity")
        self.rows.append(dict(kind=kind, at_ns=time.monotonic_ns(), **data))

    def register(self, hint):
        try:
            return self.register_checked(hint)
        except BaseException as exc:
            self.fail("registration:"+type(exc).__name__+":"+str(exc))
            raise

    def register_checked(self, hint):
        with self.lock:
            key = (hint["pid"], hint["ticks"])
            if key in self.entries:
                return
            raw = self.kernel(hint["pid"])
            if raw["ticks"] != hint["ticks"]:
                raise RuntimeError("owner_identity")
            parents = {v["pid"] for v in self.entries.values()}
            if raw["ppid"] != os.getpid() and raw["ppid"] not in parents:
                raise RuntimeError("owner_parent")
            if len(self.entries) >= 64:
                raise RuntimeError("owner_identity_capacity")
            fd = owner_pidfd_open(raw["pid"])
            if self.kernel(raw["pid"])["ticks"] != raw["ticks"]:
                os.close(fd)
                raise RuntimeError("owner_identity_after_pidfd")
            value = dict(hint, kernel_at_registration=raw,
                         registered_ns=time.monotonic_ns())
            self.entries[key], self.fds[key] = value, fd
            self.append("identity_ack", item=value)
            if hint["role"] == "fixture_child":
                control.Client.current().rpc("owner_observation", item=value)

    def barrier(self, name, item=None):
        self.append("barrier", name=name, item=item)
        spec = self.context["spec"]
        fault = spec.get("owner_fault")
        if fault is None or self.fired or fault["barrier"] != name:
            return
        with self.lock:
            target = next(v for v in self.entries.values()
                          if v["role"] == fault["role"])
            raw = self.kernel(target["pid"])
            if raw["ticks"] != target["ticks"] or raw["state"] == "Z":
                raise RuntimeError("owner_fault_target_not_live")
            self.fired = True
            self.trigger_ns = time.monotonic_ns()
            context = control.Client.current().rpc("case_context")
            step = context["spec"]["steps"][0]
            control.Client.current().rpc("death_intent", fault=fault, target=raw,
                                         at_ns=self.trigger_ns, nonce=step["fault_nonce"])
            self.tighten(dict(at_ns=self.trigger_ns))
            owner_signal(self.fds[(target["pid"], target["ticks"])], signal.SIGKILL)
            self.append("fault_injected", target=raw, fault=fault,
                        nonce=step["fault_nonce"])

    def loop(self):
        try:
            if self.failure_domain is not None:
                raw = self.kernel(os.getpid())
                self.append("injected_owner_read_boundary", raw=raw,
                            nonce=self.context["spec"]["owner_fixture"]["nonce"])
                raise OSError("injected_owner_thread_read_failure")
            while not self.stop_event.is_set():
                tick = time.monotonic_ns()
                if select.select([self.channel], [], [], 0)[0]:
                    raw = self.channel.recv(4096)
                    if raw:
                        message = json.loads(raw)
                        if message["seq"] != 1:
                            raise RuntimeError("owner_channel_sequence")
                        self.register(message["identity"])
                        self.channel.send(json.dumps(dict(ack=message["seq"])).encode())
                with self.lock:
                    for key, value in list(self.entries.items()):
                        if key in self.exits:
                            continue
                        if select.select([self.fds[key]], [], [], 0)[0]:
                            self.exits[key] = tick
                            self.append("first_exit", pid=value["pid"],
                                        ticks=value["ticks"], observed_ns=tick)
                    c = self.trigger_ns
                    if tick >= self.context["operational_ns"]:
                        c = c if c is not None else tick
                        self.trigger_ns = c
                    if c is not None:
                        end = min(c + 20 * control.NS, self.cleanup_end_ns,
                                  self.context["hard_ns"] - 10 * control.NS)
                        for key, fd in list(self.fds.items()):
                            if key in self.exits:
                                continue
                            sig = signal.SIGKILL if tick >= min(
                                c + 150_000_000, end - control.NS
                            ) else signal.SIGTERM
                            mark = (key, int(sig))
                            if mark not in self.signals:
                                owner_signal(fd, sig)
                                self.signals.append(mark)
                                self.append("owner_signal", pid=key[0],
                                            signal=int(sig), C_end=end)
                if tick >= self.context["hard_ns"]:
                    self.error = "owner_hard_tail"
                    break
                self.stop_event.wait(0.005)
        except Exception as exc:
            self.fail(type(exc).__name__ + ":" + str(exc))

    def fail(self, reason):
        if self.error is None:
            self.error = reason
            at = time.monotonic_ns()
            self.tighten(dict(at_ns=at))
            try:
                if self.failure_domain is None:
                    control.Client.current().rpc("stop", reason="owner:" + reason, at_ns=at)
                else:
                    self.failure_domain(reason, at)
            except Exception as exc:
                self.append("root_notification_failed", error=repr(exc))

    def tighten(self, trigger):
        at = trigger["at_ns"]
        self.trigger_ns = min(self.trigger_ns, at) if self.trigger_ns is not None else at
        self.cleanup_end_ns = min(self.cleanup_end_ns, at + 20 * 10**9,
                                  trigger.get("cleanup_deadline_ns", self.cleanup_end_ns),
                                  trigger.get("ancestor_end_ns", self.cleanup_end_ns))
        self.append("ancestor_deadline", original_ns=at, C_end=self.cleanup_end_ns)

    def scan_tree(self):
        """Independent group and descendant closure; unknowns never get signals."""
        observed = {}
        for path in Path("/proc").glob("[0-9]*/stat"):
            try:
                value = self.kernel(int(path.parent.name))
            except FileNotFoundError:
                continue
            observed[(value["pid"], value["ticks"])] = value
        known = dict(self.entries)
        groups = {(v["pgid"], v["session"]) for v in known.values()}
        while True:
            parents = {v["pid"] for v in known.values()}
            additions = {k: v for k, v in observed.items()
                         if k not in known and (v["ppid"] in parents
                         or (v["pgid"], v["session"]) in groups)}
            if not additions:
                break
            known.update(additions)
            if len(known) > 64:
                raise RuntimeError("owner_scan_capacity")
        rows = [dict(value, independently_registered=key in self.entries,
                     live=value["state"] != "Z", scan_ns=time.monotonic_ns())
                for key, value in observed.items() if key in known]
        unknown = [v for v in rows if not v["independently_registered"]]
        if unknown:
            self.unknown.extend(unknown)
            self.fail("unregistered_owner_descendant")
        self.append("independent_tree_scan", rows=rows, unknown=unknown)
        return rows

    def configure(self, job):
        self.context["operational_ns"] = min(self.context["operational_ns"], job["operational_ns"])
        self.context["hard_ns"] = min(self.context["hard_ns"], job["hard_ns"])
        self.append("job_deadlines", operational_ns=job["operational_ns"],
                    hard_ns=job["hard_ns"])

    def checkpoint(self, deadline):
        while time.monotonic_ns() < deadline:
            with self.lock:
                if len(self.exits) == len(self.entries):
                    break
            time.sleep(0.005)
        scan = self.scan_tree()
        return dict(complete=self.error is None and not self.unknown
                    and not any(v["live"] for v in scan)
                    and len(self.exits) == len(self.entries),
                    error=self.error, fault_fired=self.fired,
                    events=list(self.rows), thread_alive=self.thread.is_alive(),
                    scope="case; final join follows at fixture teardown")

    def finish(self, deadline):
        if self.finished is not None:
            return self.finished
        while time.monotonic_ns() < deadline:
            with self.lock:
                if len(self.exits) == len(self.entries):
                    break
            time.sleep(0.005)
        self.stop_event.set()
        self.thread.join(max(0, (deadline - time.monotonic_ns()) / control.NS))
        tree_scan = self.scan_tree()
        scan = []
        for key, value in self.entries.items():
            try:
                current = self.kernel(value["pid"])
                live = current["ticks"] == key[1] and current["state"] != "Z"
            except FileNotFoundError:
                current, live = None, False
            scan.append(dict(pid=key[0], ticks=key[1], current=current, live=live,
                             at_ns=time.monotonic_ns()))
            if key in self.exits:
                try:
                    pid, status = os.waitpid(key[0], os.WNOHANG)
                    if pid:
                        self.append("owner_reap", pid=pid, status=status)
                except ChildProcessError:
                    pass
            os.close(self.fds[key])
        required_fault = self.context["spec"].get("owner_fault")
        complete = (not self.thread.is_alive() and self.error is None
                    and len(self.exits) == len(self.entries)
                    and not any(v["live"] for v in scan)
                    and not any(v["live"] for v in tree_scan) and not self.unknown
                    and (not required_fault or self.fired))
        self.finished = dict(complete=complete, error=self.error,
                             events=self.rows, scans=scan, fault_fired=self.fired,
                             thread_join_ns=time.monotonic_ns(),
                             thread_alive=self.thread.is_alive(), tree_scan=tree_scan,
                             unknown=self.unknown)
        (self.directory / "independent-owner.json").write_text(
            json.dumps(self.finished, indent=2) + "\n"
        )
        self.channel.close()
        self.child_channel.close()
        return self.finished


@pytest.fixture(autouse=True)
def independent_owner(tmp_path):
    if os.environ.get("RUN_SNAPSHOT_TEST_DOMAIN"):
        yield None
        return
    context = control.Client.current().rpc("case_context")
    owner = Owner(tmp_path / "owner", context)
    previous, control._OWNER = control._OWNER, owner
    try:
        yield owner
    finally:
        result = owner.finish(context["hard_ns"])
        control._OWNER = previous
        if not result["complete"]:
            control.Client.current().rpc("stop", reason="independent_owner_failed")
        control.Client.current().rpc("owner_closed", complete=result["complete"],
                                     thread_alive=result["thread_alive"],
                                     evidence=str(owner.directory / "independent-owner.json"))
        assert result["complete"], result


def job_for(
    tmp_path, *, program="import time; time.sleep(0.25)", mode="normal", fault=None
):
    client = control.Client.current()
    context = client.rpc("case_context")
    header = client.header
    return dict(
        stage="G0", start_ns=context["start_ns"],
        operational_ns=min(context["operational_ns"], context["start_ns"] + 2 * control.NS),
        hard_ns=context["hard_ns"], parent_operational_ns=context["parent_operational_ns"],
        budget=str(Path(header["root"]) / "header.json"),
        batch_directory=header["root"],
        sources=control.read(Path(header["root"]) / "manifest.json")["sources"],
        command=[sys.executable, "-c", program], cwd=str(Path(control.SCRIPT).parents[1]),
        coordinator_mode=mode, fault=fault, pss=False,
        owner_fd=control._OWNER.child_channel.fileno(),
        view=context["spec"]["steps"][0].get("view") if context["spec"]["steps"] else None,
    )


def assert_known(result):
    assert result["tail_known"] and result["startup"]["tail_known"], result
    assert not result["hard_breached"]
    assert result["end_ns"] <= result["hard_ns"]
    for item in result["entries"]:
        assert item["first_exit_observed_ns"] and item["reaped"]
        assert not control.members(item)


def test_real_entry_gate_identity_order_and_exit(tmp_path):
    result = control.run_owned(tmp_path, job_for(tmp_path))
    assert result["success"], result
    assert_known(result)
    events = [control.read(tmp_path / "job.json")]
    assert events[0]["pss"] is False
    rows = result["events"]
    worker_register = next(i for i in result["entries"] if i["role"] == "worker")
    worker_release = next(
        r for r in rows if r["kind"] == "released" and r["role"] == "worker"
    )
    ready = next(r for r in rows if r["kind"] == "watchdog_ready")
    gate = __import__("json").loads(
        (tmp_path / "worker-gate.jsonl").read_text().splitlines()[0]
    )
    assert (
        ready["at_ns"]
        < worker_register["spawn_after_ns"]
        <= worker_release["at_ns"]
        <= gate["at_ns"]
    )
    with pytest.raises(RuntimeError, match="already_attempted"):
        control.run_owned(tmp_path, job_for(tmp_path))


def test_sampling_uses_actual_supervisor_birth_after_prelaunch_audit(tmp_path):
    job = job_for(tmp_path)
    job["prelaunch_audit_ns"] = job["start_ns"] - control.NS
    result = control.run_owned(tmp_path, job)
    assert result["success"], result
    assert_known(result)


@pytest.mark.parametrize("mode", ["blocked", "early_exit"])
def test_coordinator_loss_or_block_does_not_disable_watchdog(tmp_path, mode):
    job = job_for(tmp_path, mode=mode)
    if mode == "blocked":
        job["operational_ns"] = job["start_ns"] + 700_000_000
    result = control.run_owned(tmp_path, job)
    assert not result["success"]
    assert_known(result)
    if mode == "blocked":
        assert result["trigger"]["reason"] == "operational_deadline"
        assert (
            job["operational_ns"]
            <= result["trigger"]["at_ns"]
            < job["hard_ns"] - 10 * control.NS
        )
        watch = control.read(tmp_path / "watchdog-result.json")
        assert (
            watch["trigger"]["cleanup_deadline_ns"] <= job["hard_ns"] - 10 * control.NS
        )
    else:
        assert "coordinator_early_exit" in result["error"]


def test_worker_blocked_ignoring_term_is_killed_and_reaped(tmp_path):
    job = job_for(
        tmp_path,
        program="import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(20)",
    )
    job["operational_ns"] = job["start_ns"] + 700_000_000
    result = control.run_owned(tmp_path, job)
    assert not result["success"]
    assert_known(result)
    worker = next(e for e in result["entries"] if e["role"] == "worker")
    assert worker["exit_code"] == -9


def test_dead_leader_live_child_group_is_cleaned(tmp_path):
    child_pid = tmp_path / "child.pid"
    program = (
        "import subprocess,sys,os,pathlib; p=subprocess.Popen([sys.executable,'-c',"
        "'import time;time.sleep(20)']); pathlib.Path("
        + repr(str(child_pid))
        + ").write_text(str(p.pid)); os._exit(0)"
    )
    result = control.run_owned(tmp_path, job_for(tmp_path, program=program))
    assert not result["success"] and "descendant_tail" in result["error"]
    assert_known(result)
    pid = int(child_pid.read_text())
    assert not Path(f"/proc/{pid}").exists()
    assert any(i.get("reaped") for i in result["observed_descendants"])


def test_registration_failure_never_releases_business(tmp_path):
    marker = tmp_path / "business.txt"
    result = control.run_owned(
        tmp_path,
        job_for(
            tmp_path,
            fault="registration",
            program="import pathlib;pathlib.Path("
            + repr(str(marker))
            + ").write_text('bad')",
        ),
    )
    assert not result["success"] and "registration" in result["error"]
    assert_known(result)
    assert not marker.exists() and not (tmp_path / "worker-gate.jsonl").exists()


@pytest.mark.parametrize("failure", ["source", "expired", "unclosed"])
def test_admission_rejects_before_any_child(tmp_path, failure):
    job = job_for(tmp_path)
    previous = None
    if failure == "source":
        job["sources"][control.SCRIPT] = "0" * 64
    elif failure == "expired":
        job["operational_ns"] = time.monotonic_ns() - 1
    else:
        previous = dict(closed=False, started_ns=1)
        control.save(tmp_path / "attempt.json", previous)
    expected = {"source": "source_mismatch", "expired": "budget_admission",
                "unclosed": "already_attempted"}[failure]
    with pytest.raises(RuntimeError, match=expected):
        control.run_owned(tmp_path, job)
    assert not (tmp_path / "W.log").exists()
    if previous:
        assert control.read(tmp_path / "attempt.json") == previous


def test_critical_one_ns_boundary_rejected_at_real_admission(tmp_path):
    client = control.Client.current()
    job = job_for(tmp_path)
    for tick, expected in [(job["operational_ns"], "budget_admission"),
                           (job["operational_ns"] - 1, None)]:
        value = client.rpc("view", name="critical", decision=dict(
            header=client.header, job=job, tick=tick,
        ))
        assert value == dict(reason=expected, grants_spawn=False)
    assert not (tmp_path / "job.json").exists()


def test_unrelated_owned_by_test_process_is_not_signalled(tmp_path):
    client = control.Client.current()
    intent = client.rpc("aux_intent")
    begin = time.monotonic_ns()
    unrelated = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(10)"])
    item = dict(Owner.kernel(unrelated.pid), role="unrelated", birth_lower=begin,
                spawn_before_ns=begin, spawn_after_ns=time.monotonic_ns(),
                attempt=os.environ["RUN_SNAPSHOT_CURRENT_ATTEMPT"])
    fd = owner_pidfd_open(unrelated.pid)
    try:
        client.rpc("identity", item=item, intent=intent["id"])
        control.await_ancestors(client, item, begin + control.NS)
        result = control.run_owned(tmp_path, job_for(tmp_path, mode="early_exit"))
        assert_known(result)
        assert unrelated.poll() is None
        assert Owner.kernel(unrelated.pid)["ticks"] == item["ticks"]
        assert all(row["pid"] != unrelated.pid for row in result["startup"]["events"])
        control._OWNER.append("unrelated_live", item=Owner.kernel(unrelated.pid))
    finally:
        owner_signal(fd, signal.SIGTERM)
        deadline = time.monotonic_ns() + control.NS
        while not select.select([fd], [], [], 0)[0] and time.monotonic_ns() < deadline:
            time.sleep(0.005)
        client.rpc("exited", item=item, at_ns=time.monotonic_ns())
        unrelated.wait(timeout=1)
        os.close(fd)


@pytest.mark.parametrize(
    "fault", ["unavailable", "identity", "negative", "gap", "resource"]
)
def test_actual_sampling_failure_stops_entry_and_keeps_tail(tmp_path, fault):
    result = control.run_owned(
        tmp_path, job_for(tmp_path, fault=fault, program="import time;time.sleep(2)")
    )
    assert not result["success"] and result["error"]
    assert_known(result)


def sample_input():
    item = dict(pid=123, role="worker", ticks=7, started_ns=1_000_000_000)
    value = dict(
        at_ns=1_100_000_000,
        mem_available=8 * control.GIB,
        disk_free=40 * control.GIB,
        swap_used=0,
        evidence_bytes=0,
        batch_bytes=0,
        processes=[dict(item, cpu=0.1, rss=1024)],
    )
    return item, value


@pytest.mark.parametrize(
    "fault",
    [
        "missing",
        "unavailable",
        "identity",
        "negative",
        "gap",
        "first_gap",
        "exit_gap",
        "memory",
        "disk",
        "swap",
        "evidence",
        "batch",
        "tool_rss",
        "rss_sum",
    ],
)
def test_actual_sampler_guard_rejects_required_data_and_stop_lines(fault):
    item, value = sample_input()
    guard = control.Samples(0)
    guard.accept(value, [item])
    value["at_ns"] += 100_000_000
    if fault == "missing":
        value["processes"] = []
    elif fault == "unavailable":
        value["processes"][0]["unavailable"] = True
    elif fault == "identity":
        value["processes"][0]["ticks"] += 1
    elif fault == "negative":
        value["processes"][0]["cpu"] = -1
    elif fault == "gap":
        value["at_ns"] += 250_000_001
    elif fault == "first_gap":
        guard = control.Samples(0)
        value["at_ns"] = item["started_ns"] + 250_000_001
    elif fault == "exit_gap":
        with pytest.raises(RuntimeError, match="exit_sample_gap"):
            guard.exited(item, value["at_ns"] + 250_000_001)
        return
    elif fault in {"memory", "disk", "swap", "evidence", "batch"}:
        key, bad = {
            "memory": ("mem_available", 0),
            "disk": ("disk_free", 0),
            "swap": ("swap_used", control.GIB + 1),
            "evidence": ("evidence_bytes", 256 * 1024**2 + 1),
            "batch": ("batch_bytes", 2 * control.GIB + 1),
        }[fault]
        value[key] = bad
    elif fault == "tool_rss":
        item["role"] = value["processes"][0]["role"] = "S"
        value["processes"][0]["rss"] = 513 * 1024**2
    elif fault == "rss_sum":
        value["processes"][0]["rss"] = 8 * control.GIB + 1
        guard.accept(value, [item])
        value["at_ns"] += 100_000_000
    with pytest.raises(RuntimeError):
        guard.accept(value, [item])


def test_pss_is_not_read_and_cannot_erase_cpu_rss(monkeypatch):
    original = Path.read_text

    def read_text(path, *args, **kwargs):
        assert "smaps_rollup" not in str(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_text)
    item = control.identity(os.getpid()) | dict(role="S")
    value = control.sample_process(item)
    assert "cpu" in value and "rss" in value and not value.get("unavailable")


def test_real_supervised_small_diagnostic_cli(tmp_path):
    config = Path(control.Client.current().header["root"]) / "configs/g0-small.json"
    directory = tmp_path / "owned"
    diagnostic = Path(control.SCRIPT).with_name("run_snapshot_diagnostic.py")
    job = job_for(directory)
    job["command"] = [
        sys.executable,
        str(diagnostic),
        str(tmp_path / "diagnostic"),
        str(config),
    ]
    job["sources"] = dict(job["sources"])
    job["sources"].update(
        {
            str(config): hashlib.sha256(config.read_bytes()).hexdigest(),
            str(diagnostic): hashlib.sha256(diagnostic.read_bytes()).hexdigest(),
        }
    )
    job["operational_ns"] = job["start_ns"] + 5 * control.NS
    result = control.run_owned(directory, job)
    assert result["success"], result
    assert_known(result)
    diagnostic_result = control.read(tmp_path / "diagnostic/result.json")
    assert diagnostic_result["validation_complete"] and diagnostic_result["restored"]
    assert len(diagnostic_result["samples"]) == 6
    assert diagnostic_result["fixture"]["tasks"] == 250
    assert not diagnostic_result["candidate_selection_allowed"]


def test_owned_descendant_rss_cpu_are_in_actual_samples(tmp_path):
    program = (
        "import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c',"
        "'import time;time.sleep(0.45)']); p.wait(); time.sleep(0.1)"
    )
    result = control.run_owned(tmp_path, job_for(tmp_path, program=program))
    assert result["success"], result
    assert_known(result)
    assert result["observed_descendants"]
    rows = [
        __import__("json").loads(line)["sample"]
        for line in (tmp_path / "real-resources.jsonl").read_text().splitlines()
    ]
    child_samples = [
        process
        for row in rows
        for process in row["processes"]
        if process["role"] == "worker_child"
    ]
    assert child_samples and all("cpu" in row and "rss" in row for row in child_samples)
    assert all(
        item["first_exit_observed_ns"] and item["reaped_ns"]
        and item["group_scan_ns"] >= item["first_exit_observed_ns"]
        and not control.same_process(item)
        for item in result["observed_descendants"]
    )


SCENARIOS = control.read(
    Path(os.environ["RUN_SNAPSHOT_REPAIR_ROOT"]) / "scene-table.json"
)

INITIALIZATION_FAULTS = [
    "capability_call", "lease_open", "socket_create", "capability", "lease",
    "selector", "monitor", "fd", "writer", "sampler_started", "thread",
    "cutoff_thread", "client", "writer_io", "writer_ack_block",
]

OUTER_FIXTURES = ["normal", "capability_failure", "entry_fatal", "early_exit", "ready_timeout"]
MONITOR_FIXTURES = ["unavailable", "cpu_missing", "identity", "read_gap", "finish_race"]


@pytest.mark.parametrize("monitor_fixture", MONITOR_FIXTURES, ids=MONITOR_FIXTURES)
def test_finite_monitor_registration_actual_entry(tmp_path, monitor_fixture):
    context = control.Client.current().rpc("case_context")
    job = job_for(tmp_path)
    job.update(command=context["spec"]["steps"][0]["command"],
               operational_ns=context["operational_ns"])
    result = control.run_owned(tmp_path / "entry", job)
    assert result["success"], result
    assert_independent(result)
    observed = control.read(tmp_path / "monitor-fixture/monitor-boundary.json")
    assert observed["variant"] == monitor_fixture
    assert observed["nonce"] == context["spec"]["monitor_fixture"]["nonce"]
    assert observed["invalid"] == (monitor_fixture != "finish_race")


def finite_owner_fixture(directory):
    client = control.Client.current()
    context = client.rpc("case_context")
    specification = context["spec"]["owner_fixture"]
    acknowledged = []

    def domain(reason, at):
        client.rpc("owner_fixture_failure", nonce=specification["nonce"],
                   reason=reason, at_ns=at)
        acknowledged.append(time.monotonic_ns())

    value = Owner(directory, context, failure_domain=domain)
    value.thread.join(max(0, (context["operational_ns"]-time.monotonic_ns())/control.NS))
    observed = value.finish(context["hard_ns"])
    assert acknowledged and not observed["complete"] and not observed["thread_alive"]
    assert observed["error"] == "OSError:injected_owner_thread_read_failure"
    with pytest.raises(RuntimeError, match="owner_fixture_stopped"):
        client.rpc("owner_fixture_probe", nonce=specification["nonce"])
    return observed


def test_finite_owner_thread_failure_actual_entry(tmp_path):
    context = control.Client.current().rpc("case_context")
    job = job_for(tmp_path)
    job.update(command=context["spec"]["steps"][0]["command"],
               operational_ns=context["operational_ns"])
    result = control.run_owned(tmp_path / "entry", job)
    assert result["success"], result
    assert_independent(result)
    observed = control.read(tmp_path / "finite-owner/independent-owner.json")
    assert not observed["complete"] and not observed["thread_alive"]


@pytest.mark.parametrize("outer_fixture", OUTER_FIXTURES, ids=OUTER_FIXTURES)
def test_finite_outer_actual_entry(tmp_path, outer_fixture):
    client = control.Client.current()
    context = client.rpc("case_context")
    step = context["spec"]["steps"][0]
    job = job_for(tmp_path)
    job.update(command=step["command"], operational_ns=context["operational_ns"])
    result = control.run_owned(tmp_path / "entry", job)
    assert result["success"], result
    assert_independent(result)
    observed = control.read(tmp_path / "outer-fixture/outer-result.json")
    assert observed["fixture"]["mode"] == outer_fixture
    assert observed["fixture"]["nonce"] == context["spec"]["outer_fixture"]["nonce"]
    assert observed["physical_tail_known"] and observed["threads_joined"]
    assert observed["cleanup_within_deadline"]
    assert observed["measurement_valid"] is (None if outer_fixture == "capability_failure" else True)
    reason = observed["state"]["stop"]["reason"] if observed["state"]["stop"] else None
    assert reason in context["spec"]["outer_fixture"]["expected"]
    assert not (tmp_path / "outer-fixture/header.json").exists()


def test_pidfd_named_backend_errno_and_descriptor_cleanup():
    import errno

    before = set(os.listdir("/proc/self/fd"))
    descriptor = control.pidfd_open(os.getpid())
    try:
        control.pidfd_signal(descriptor, 0)
        assert not select.select([descriptor], [], [], 0)[0]
    finally:
        os.close(descriptor)
    with pytest.raises(OSError) as raised:
        control.pidfd_signal(-1, 0)
    assert raised.value.errno == errno.EBADF
    with pytest.raises(OSError) as raised:
        owner_signal(-1, 0)
    assert raised.value.errno == errno.EBADF
    with pytest.raises(OSError) as raised:
        control.pidfd_open(-1)
    assert raised.value.errno == errno.EINVAL
    with pytest.raises(OSError) as raised:
        owner_pidfd_open(-1)
    assert raised.value.errno == errno.EINVAL
    assert set(os.listdir("/proc/self/fd")) == before


@pytest.mark.parametrize("initialization_fault", INITIALIZATION_FAULTS,
                         ids=INITIALIZATION_FAULTS)
def test_finite_initialization_transaction_actual_entry(tmp_path, initialization_fault):
    client = control.Client.current()
    context = client.rpc("case_context")
    step = context["spec"]["steps"][0]
    job = job_for(tmp_path)
    job.update(command=step["command"], operational_ns=context["operational_ns"])
    result = control.run_owned(tmp_path / "entry", job)
    assert result["success"], result
    assert_independent(result)
    observed = control.read(tmp_path / "fixture/fixture-rollback.json")
    assert observed["fault"] == initialization_fault
    assert observed["nonce"] == context["spec"]["initialization_fixture"]["nonce"]
    assert observed["rollback_complete"] and observed["fd_set_restored"]
    assert observed["threads_restored"] and observed["at_ns"] <= observed["C_end"]


def assert_independent(result):
    assert result["parent_sampling_valid"], result
    assert result["owner_complete"], result
    assert result["independent_owner"]["complete"], result
    assert not result["hard_breached"], result


@pytest.mark.parametrize("scene", SCENARIOS, ids=[v["id"] for v in SCENARIOS])
def test_scene(tmp_path, scene):
    client = control.Client.current()
    context = client.rpc("case_context")
    spec = context["spec"]
    action = scene["action"]
    if action == "admission":
        job = job_for(tmp_path)
        variant = scene["variant"]
        if variant in {"stopped", "pending"}:
            response = client.rpc("view", name=scene["id"], decision=dict(
                header=client.header, job=job, tick=job["start_ns"],
                stopped=variant == "stopped", pending=variant == "pending",
            ))
            assert response["reason"] == spec["refusal"]
            assert not response["grants_spawn"]
        else:
            directory = tmp_path / "refused"
            previous = os.environ.get("RUN_SNAPSHOT_AUTH_FD")
            if variant == "expired":
                job["operational_ns"] = time.monotonic_ns() - 1
            elif variant == "wrong_budget":
                job["budget"] = str(tmp_path / "copied-header.json")
            elif variant == "wrong_root":
                directory = Path(client.header["root"]).parent / "unapproved-root"
            elif variant == "alias":
                alias = tmp_path / "alias"
                alias.symlink_to(tmp_path, target_is_directory=True)
                directory = alias / "entry"
            elif variant == "no_fd":
                os.environ.pop("RUN_SNAPSHOT_AUTH_FD", None)
            elif variant == "source":
                job["sources"] = dict(job["sources"], **{control.SCRIPT: "0" * 64})
            try:
                with pytest.raises(RuntimeError, match=spec["refusal"]):
                    control.run_owned(directory, job)
            finally:
                if previous is not None:
                    os.environ["RUN_SNAPSHOT_AUTH_FD"] = previous
            assert not (tmp_path / "refused/W.log").exists()
        assert not control._OWNER.entries
        return
    if action == "bootstrap":
        outcome = client.rpc("bootstrap_view", name=scene["id"], variant=scene["variant"])
        assert outcome["reason"] == spec["refusal"]
        assert not outcome["grants_spawn"]
        assert not control._OWNER.entries
        return
    if action == "once":
        job = job_for(tmp_path)
        job.update(spec["steps"][0]["job_overrides"])
        directory = tmp_path / "entry"
        if scene["variant"] == "concurrent":
            results = []
            thread = threading.Thread(target=lambda: results.append(
                control.run_owned(directory, job)
            ))
            thread.start()
            deadline = min(time.monotonic_ns() + control.NS, context["operational_ns"])
            while not (directory / "job.json").exists() and time.monotonic_ns() < deadline:
                time.sleep(0.002)
            with pytest.raises(RuntimeError, match="unclosed_sibling_attempt"):
                control.admit(tmp_path / "sibling", job)
            thread.join(max(0, (context["hard_ns"] - time.monotonic_ns()) / control.NS))
            assert not thread.is_alive() and len(results) == 1
            result = results[0]
        else:
            result = control.run_owned(directory, job)
        assert result["success"], result
        assert_independent(result)
        if scene["variant"] == "duplicate_close":
            ticket = control.read(directory / "attempt.json")["ticket"]
            with pytest.raises(RuntimeError, match="duplicate_close_or_owner"):
                client.rpc("close", id=ticket, result=dict(success=True))
        elif scene["variant"] == "replay":
            with pytest.raises(RuntimeError, match="already_attempted"):
                control.run_owned(directory, job)
        return
    if action == "swap_chain":
        for index, step in enumerate(spec["steps"]):
            job = job_for(tmp_path)
            job.update(step["job_overrides"])
            job["view"] = step["view"]
            result = control.run_owned(tmp_path / f"domain-{index}", job)
            assert_independent(result)
            assert result["error"] == step["expected"], result
        with pytest.raises(RuntimeError, match="child_domain_stopped"):
            control.admit(tmp_path / "domain-2", job)
        baseline = Path(client.header["root"]) / "bootstrap.json"
        assert hashlib.sha256(baseline.read_bytes()).hexdigest() == client.header["bootstrap_hash"]
        parts = {line.split()[0]: int(line.split()[1]) * 1024
                 for line in control.read(baseline)["swap_raw"]}
        assert parts["SwapTotal:"] - parts["SwapFree:"] == client.header["swap_start"]
        return
    job = job_for(tmp_path)
    job.update(spec["steps"][0]["job_overrides"])
    if "operational_offset_ns" in job:
        job["operational_ns"] = job["start_ns"] + job.pop("operational_offset_ns")
    result = control.run_owned(tmp_path / "entry", job)
    assert_independent(result)
    expected = spec["steps"][0].get("expected")
    if expected is None:
        assert result["success"], result
    else:
        assert not result["success"], result
        assert result["error"] in (expected if isinstance(expected, list) else [expected]), result
    assert result["tail_known"] == spec["steps"][0].get("tail_known", True), result
    if action == "sample":
        rows = [json.loads(line) for line in
                (tmp_path / "entry/injected-resources.jsonl").read_text().splitlines()]
        assert len(rows) == 1 and rows[0]["spec"] == scene["view"]
        assert rows[0]["error"] == expected
        assert rows[0]["invalid"] == (expected is not None)
        assert not rows[0]["host_deadlines_changed"]
        assert (tmp_path / "entry/real-resources.jsonl").stat().st_size > 0
    if scene.get("fault") == "ancestor_late":
        assert result["startup_trigger"]["cleanup_deadline_ns"] <= result["startup_trigger"]["at_ns"] + 1200_000_000
        assert result["cleanup_deadline_ns"] <= result["startup_trigger"]["cleanup_deadline_ns"]
    if action == "death":
        assert result["independent_owner"]["fault_fired"]
    if action == "descendant":
        owner_children = [v for v in control._OWNER.entries.values()
                          if v["role"] == "fixture_child"]
        assert owner_children
        if scene["mode"] != "omit_descendant":
            assert result["observed_descendants"]
        assert all((i["pid"], i["ticks"]) in control._OWNER.exits for i in owner_children)
    if action == "plugin_domain":
        assert "worker_failed" == result["error"]
        text = (tmp_path / "entry/worker.log").read_text()
        assert "MUST_NOT_RUN" not in text


@pytest.fixture
def _domain_failure_fixture():
    from run_snapshot_g0 import domain_injection

    phase = os.environ["RUN_SNAPSHOT_DOMAIN_PHASE"]
    if phase == "setup":
        pytest.fail(domain_injection())
    yield
    if phase == "teardown":
        pytest.fail(domain_injection())


def _controlled_failure(_domain_failure_fixture):
    from run_snapshot_g0 import domain_injection

    phase = os.environ["RUN_SNAPSHOT_DOMAIN_PHASE"]
    if phase == "call":
        pytest.fail(domain_injection())
    if phase == "skip":
        pytest.skip(domain_injection())
    if phase == "xfail":
        pytest.xfail(domain_injection())


def _must_not_run():
    raise AssertionError("MUST_NOT_RUN")
