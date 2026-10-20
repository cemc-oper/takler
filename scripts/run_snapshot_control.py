"""Bounded G0-only supervision through an inherited root authority descriptor."""

import argparse
import array
import ctypes
import hashlib
import json
import os
from pathlib import Path
import queue
import select
import signal
import socket
import subprocess
import sys
import threading
import time

NS = 10**9
GIB = 1024**3
SCRIPT = str(Path(__file__).resolve())
_CLIENTS = {}
_OWNER = None


def now():
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    temporary.replace(path)


def event(path, kind, **values):
    with Path(path).open("a") as stream:
        stream.write(json.dumps(dict(kind=kind, at_ns=now(), **values)) + "\n")


def identity(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    return dict(pid=pid, ppid=int(fields[1]), pgid=int(fields[2]), session=int(fields[3]), ticks=int(fields[19]), state=fields[0])


def same_process(item):
    try:
        value = identity(item["pid"])
        return value["ticks"] == item["ticks"] and value["state"] != "Z"
    except (FileNotFoundError, ProcessLookupError):
        return False


def members(item):
    values = []
    for path in Path("/proc").glob("[0-9]*/stat"):
        try:
            value = identity(int(path.parent.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        if value["state"] != "Z" and value["pgid"] == item["pid"] and value["session"] == item["pid"]:
            if value["ticks"] < item["ticks"]:
                raise RuntimeError("group_identity_changed")
            values.append(value)
    if len(values) > 64:
        raise RuntimeError("group_capacity")
    return values


class Client:
    def __init__(self, descriptor):
        self.fd = int(descriptor)
        self.socket = socket.socket(fileno=os.dup(self.fd))
        self.socket.setblocking(False)
        self.lock = threading.Lock()
        self.sequence = 0
        self.header = self.rpc("hello")
        _CLIENTS[(os.getpid(), str(self.fd))] = self

    @classmethod
    def current(cls):
        key = (os.getpid(), os.environ.get("RUN_SNAPSHOT_AUTH_FD"))
        if key[1] is None:
            raise RuntimeError("missing_live_authority")
        if key not in _CLIENTS:
            _CLIENTS[key] = cls(int(key[1]))
        return _CLIENTS[key]

    def rpc(self, op, **data):
        if not self.lock.acquire(timeout=0.1):
            raise RuntimeError("authority_lock_timeout")
        try:
            self.sequence += 1
            end = now() + 100_000_000
            payload = json.dumps(dict(seq=self.sequence, op=op, data=data)).encode()
            if len(payload) > 65536:
                raise RuntimeError("message_capacity")
            if not select.select([], [self.socket], [], max(0, (end - now()) / NS))[1]:
                raise RuntimeError("authority_send_timeout")
            self.socket.send(payload)
            if not select.select([self.socket], [], [], max(0, (end - now()) / NS))[0]:
                raise RuntimeError("authority_ack_timeout")
            raw, ancillary, _, _ = self.socket.recvmsg(65536, 1024)
            if not raw:
                raise RuntimeError("authority_lost")
            response = json.loads(raw)
            if response["seq"] != self.sequence:
                raise RuntimeError("authority_sequence")
            if "error" in response:
                raise RuntimeError(response["error"])
            result = response["result"]
            if isinstance(result, dict) and result.get("has_fd"):
                descriptors = array.array("i")
                for level, kind, value in ancillary:
                    if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                        descriptors.frombytes(value[: len(value) - len(value) % descriptors.itemsize])
                if len(descriptors) != 1:
                    raise RuntimeError("descriptor_transfer")
                result["fd"] = descriptors[0]
            return result
        finally:
            self.lock.release()


def sample_process(item):
    begin = now()
    try:
        first = identity(item["pid"])
        fields = Path(f"/proc/{item['pid']}/stat").read_text().rpartition(")")[2].split()
        rss = int(Path(f"/proc/{item['pid']}/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
        last = identity(item["pid"])
        if first["ticks"] != last["ticks"] or last["ticks"] != item["ticks"]:
            raise RuntimeError("sample_identity")
        return dict(last, role=item["role"], rss=rss, cpu=(int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"), read_begin_ns=begin, read_end_ns=now())
    except (OSError, ValueError):
        return dict(pid=item["pid"], role=item["role"], unavailable=True, read_begin_ns=begin, read_end_ns=now())


def owned_processes(roots):
    known = {v["pid"]: dict(v) for v in roots}
    rows = {}
    for path in Path("/proc").glob("[0-9]*/stat"):
        try:
            item = identity(int(path.parent.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        if item["state"] != "Z":
            rows[item["pid"]] = item
    while True:
        added = False
        for pid, item in rows.items():
            parent = known.get(item["ppid"])
            if pid in known or parent is None:
                continue
            if item["ticks"] < parent["ticks"]:
                raise RuntimeError("descendant_identity")
            known[pid] = dict(item, role=parent["role"] + "_child", birth_lower=parent["birth_lower"], spawn_before_ns=parent["birth_lower"], spawn_after_ns=now(), inferred=True)
            added = True
        if len(known) > 64:
            raise RuntimeError("identity_capacity")
        if not added:
            return list(known.values())


class Samples:
    def __init__(self, swap_start):
        self.swap_start = swap_start
        self.first = {}
        self.last = {}
        self.count = {}
        self.over = 0
        self.invalid = False

    def accept(self, value, expected):
        if (
            value["mem_available"] < 4 * GIB or value["disk_free"] < 20 * GIB
            or value["swap_used"] - self.swap_start > GIB
            or value["evidence_bytes"] > 256 * 1024**2 or value["batch_bytes"] > 2 * GIB
        ):
            self.invalid = True
            raise RuntimeError("resource_stop")
        rows = {r["pid"]: r for r in value["processes"]}
        total = 0
        for item in expected:
            row = rows.get(item["pid"])
            if row is None or row.get("unavailable"):
                self.invalid = True
                raise RuntimeError("sample_unavailable")
            if row["ticks"] != item["ticks"]:
                self.invalid = True
                raise RuntimeError("sample_identity")
            if min(row["cpu"], row["rss"]) < 0:
                self.invalid = True
                raise RuntimeError("negative_sample")
            begin = row.get("read_begin_ns", value["at_ns"])
            end = row.get("read_end_ns", value["at_ns"])
            previous = self.last.get(item["pid"])
            if end < begin or end - begin > 250_000_000:
                self.invalid = True
                raise RuntimeError("sample_read_gap")
            if previous and (end - previous[0] > 250_000_000 or row["cpu"] < previous[2]):
                self.invalid = True
                raise RuntimeError("sample_gap_or_cpu_regression")
            birth = item.get("birth_lower", item.get("started_ns"))
            if previous is None and end - birth > 250_000_000:
                self.invalid = True
                raise RuntimeError("first_sample_gap")
            self.first.setdefault(item["pid"], dict(row))
            self.last[item["pid"]] = (begin, end, row["cpu"])
            self.count[item["pid"]] = self.count.get(item["pid"], 0) + 1
            total += row["rss"]
            if item["role"] in {"S", "C", "W"} and row["rss"] > 512 * 1024**2:
                self.invalid = True
                raise RuntimeError("tool_rss")
        self.over = self.over + 1 if total > 8 * GIB else 0
        if self.over >= 2:
            self.invalid = True
            raise RuntimeError("rss_sum")

    def exited(self, item, at_ns, *, worker=False):
        previous = self.last.get(item["pid"])
        if previous is None or at_ns - previous[0] > 250_000_000:
            self.invalid = True
            raise RuntimeError("exit_sample_gap")
        if worker and self.count.get(item["pid"], 0) < 2:
            self.invalid = True
            raise RuntimeError("worker_cpu_points")


def host_sample(directory, entries, batch_directory):
    import shutil

    memory = {s.split(":")[0]: int(s.split()[1]) * 1024 for s in Path("/proc/meminfo").read_text().splitlines() if len(s.split()) == 3}

    def size(base):
        total = 0
        for path in Path(base).rglob("*"):
            try:
                if path.is_file():
                    total += path.stat().st_size
            except FileNotFoundError:
                continue
        return total

    return dict(at_ns=now(), mem_available=memory["MemAvailable"], disk_free=shutil.disk_usage(directory).free, swap_used=memory["SwapTotal"] - memory["SwapFree"], evidence_bytes=size(directory), batch_bytes=size(batch_directory), processes=[sample_process(i) for i in entries])


class RealMonitor:
    """Real samples only. The deadline thread never performs these file reads."""

    def __init__(self, directory, header, trigger):
        self.directory = Path(directory)
        self.client = Client.current()
        self.header = header
        self.trigger = trigger
        self.entries = {}
        self.guard = Samples(header["swap_start"])
        self.stopping = threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self.loop, name="real-proc-sampler")
        self.thread.start()

    def register(self, item):
        previous = self.entries.get(item["pid"])
        if previous and previous["ticks"] != item["ticks"]:
            raise RuntimeError("registration_reused_pid")
        if previous and previous.get("inferred"):
            previous.update(item)
        self.entries.setdefault(item["pid"], dict(item))

    def loop(self):
        while not self.stopping.is_set():
            start = now()
            live = []
            try:
                registered = self.client.rpc("status")["identities"]
                for item in registered:
                    if item.get("collection") == int(os.environ.get("RUN_SNAPSHOT_COLLECTION", "0")) and same_process(item):
                        self.register(item)
            except Exception as exc:
                self.fail("real_authority:" + str(exc))
            for item in list(self.entries.values()):
                if item.get("first_exit_observed_ns"):
                    continue
                if same_process(item):
                    live.append(item)
                else:
                    item["first_exit_observed_ns"] = now()
                    try:
                        self.guard.exited(item, item["first_exit_observed_ns"], worker=item["role"] == "worker")
                    except RuntimeError as exc:
                        self.fail(str(exc))
            try:
                if live:
                    discovered = owned_processes(live)
                    for item in discovered:
                        if item["pid"] not in self.entries:
                            self.register(item)
                    live = [i for i in discovered if not self.entries[i["pid"]].get("first_exit_observed_ns")]
                    value = host_sample(self.directory, live, self.header["root"])
                    event(self.directory / "real-resources.jsonl", "sample", sample=value)
                    self.guard.accept(value, live)
            except Exception as exc:
                self.fail(type(exc).__name__ + ":" + str(exc))
            self.stopping.wait(max(0, (start + 100_000_000 - now()) / NS))

    def fail(self, reason):
        if self.error is None:
            self.error = reason
            self.trigger(reason, now())

    def finish(self, deadline):
        self.stopping.set()
        self.thread.join(max(0, (deadline - now()) / NS))
        if self.thread.is_alive():
            self.error = self.error or "sampler_thread_tail"
        return self.error is None and not self.thread.is_alive()


def signal_fixed(item, descriptor, sig, events):
    try:
        signal.pidfd_send_signal(descriptor, sig)
        events.append(dict(kind="signal", at_ns=now(), pid=item["pid"], signal=int(sig)))
    except ProcessLookupError:
        pass


def admit(directory, job):
    client = Client.current()
    header = client.header
    root = Path(header["root"])
    if (
        Path(job["budget"]) != root / "header.json" or job.get("stage") != "G0"
        or Path(job["batch_directory"]) != root
        or Path(directory) != Path(directory).resolve()
    ):
        raise RuntimeError("scope_or_root")
    tick = job.get("admission_now_ns", now())
    if tick >= job["operational_ns"] or job["hard_ns"] > header["D_work"] or job["hard_ns"] + 600 * NS > header["D_total"]:
        raise RuntimeError("budget_admission")
    for name, value in job["sources"].items():
        if hashlib.sha256(Path(name).read_bytes()).hexdigest() != value:
            raise RuntimeError("source_mismatch")
    return client.rpc("attempt", directory=str(directory), job=job, collection=job.get("collection", False), view=job.get("admission_view", {}))


def diagnostic_permit(directory, config):
    if config.get("stage") != "G0" or config.get("tasks") != 250:
        raise RuntimeError("G0_only_no_100k_authorization")
    client = Client.current()
    return client.rpc("diagnose", directory=str(Path(directory)), config=config)


def diagnostic_closed(ticket, result):
    Client.current().rpc("close", id=ticket["id"], result=dict(diagnostic_valid=result["validation_complete"], error=result["error"]))


def watchdog(channel, job):
    channel.setblocking(False)
    cache = {}
    events = []
    stopped = None
    complete = False
    while True:
        tick = now()
        if tick >= job["operational_ns"] and stopped is None:
            stopped = dict(reason="operational_deadline", at_ns=tick)
        while select.select([channel], [], [], 0)[0]:
            raw, ancillary, _, _ = channel.recvmsg(65536, 1024)
            if not raw:
                stopped = stopped or dict(reason="supervisor_lost", at_ns=now())
                break
            message = json.loads(raw)
            if message["op"] == "identity":
                descriptors = array.array("i")
                for level, kind, value in ancillary:
                    if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                        descriptors.frombytes(value[: len(value) - len(value) % descriptors.itemsize])
                if len(descriptors) != 1:
                    raise RuntimeError("watchdog_descriptor")
                cache[message["item"]["pid"]] = (message["item"], descriptors[0])
                channel.send(json.dumps(dict(ack=message["item"]["pid"])).encode())
            elif message["op"] in {"stop", "complete"}:
                stopped = stopped or message["trigger"]
                complete = message["op"] == "complete"
        if stopped:
            end = min(stopped["at_ns"] + 20 * NS, job["hard_ns"] - 10 * NS, stopped.get("ancestor_end_ns", job["hard_ns"]))
            for item, descriptor in cache.values():
                if not select.select([descriptor], [], [], 0)[0]:
                    signal_fixed(item, descriptor, signal.SIGKILL if tick >= min(stopped["at_ns"] + 150_000_000, end - NS) else signal.SIGTERM, events)
            if all(select.select([fd], [], [], 0)[0] for _, fd in cache.values()) or tick >= end:
                break
        if tick >= job["hard_ns"]:
            break
        time.sleep(0.005)
    for _, descriptor in cache.values():
        os.close(descriptor)
    # No persistence occurs until the independent cutoff loop has finished.
    return dict(events=events, trigger=stopped, complete=complete, end_ns=now())


def send_identity(channel, item, descriptor):
    channel.sendmsg([json.dumps(dict(op="identity", item=item)).encode()], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [descriptor]))])
    if not select.select([channel], [], [], 0.1)[0]:
        raise RuntimeError("watchdog_ack_timeout")
    if json.loads(channel.recv(65536)).get("ack") != item["pid"]:
        raise RuntimeError("watchdog_ack_identity")


def role_main(role, directory, descriptor):
    directory = Path(directory)
    channel = socket.socket(fileno=descriptor)
    job = read(directory / "job.json")
    if role == "W":
        channel.send(b'{"ready": true}')
        result = watchdog(channel, job)
        save(directory / "watchdog-result.json", result)
        return 0
    gate = channel.recv(65536)
    if gate != b"G":
        return 80
    if role == "C":
        if job.get("coordinator_mode") == "early_exit":
            return 7
        if job.get("coordinator_mode") == "blocked":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            while True:
                time.sleep(0.05)
        while channel.recv(65536) != b"F":
            pass
        return 0
    event(directory / "worker-gate.jsonl", "released", pid=os.getpid())
    os.set_inheritable(int(os.environ["RUN_SNAPSHOT_AUTH_FD"]), True)
    os.execvpe(job["command"][0], job["command"], os.environ.copy())


def supervise(directory, parent_fd):
    directory = Path(directory)
    job = read(directory / "job.json")
    parent = socket.socket(fileno=parent_fd)
    client = Client.current()
    handles = {}
    channels = {}
    cache = {}
    events = []
    trigger = None
    error = None
    watchdog_result = None
    ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0)

    def notify(message):
        parent.send(json.dumps(message).encode())
        if not select.select([parent], [], [], 0.1)[0]:
            raise RuntimeError("startup_ack_timeout")
        if parent.recv(65536) != b"A":
            raise RuntimeError("startup_ack")

    def launch(role):
        if now() >= job["operational_ns"]:
            raise RuntimeError("launch_after_deadline")
        request = client.rpc("channel", role=role)
        auth_fd = request["fd"]
        here, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        environment = os.environ.copy()
        environment["RUN_SNAPSHOT_AUTH_FD"] = str(auth_fd)
        output = (directory / (role + ".log")).open("xb", buffering=0)
        begin = now()
        proc = subprocess.Popen([sys.executable, SCRIPT, "role", role, str(directory), str(child.fileno())], stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, start_new_session=True, pass_fds=(auth_fd, child.fileno()), env=environment)
        descriptor = os.pidfd_open(proc.pid)
        item = dict(identity(proc.pid), role=role, birth_lower=begin, spawn_before_ns=begin, spawn_after_ns=now(), first_exit_observed_ns=None)
        handles[role] = (proc, descriptor, output)
        item["collection"] = int(os.environ.get("RUN_SNAPSHOT_COLLECTION", "0"))
        channels[role] = here
        child.close()
        os.close(auth_fd)
        cache[proc.pid] = item
        client.rpc("identity", item=item)
        notify(dict(op="identity", item=item))
        save(directory / "registry.json", dict(entries=list(cache.values())))
        if role == "W":
            if not select.select([here], [], [], 0.1)[0] or not json.loads(here.recv(65536)).get("ready"):
                raise RuntimeError("watchdog_not_ready")
        else:
            send_identity(channels["W"], item, descriptor)
            if role == "worker" and job.get("fault") == "registration":
                here.send(b"E")
                raise RuntimeError("injected_registration_failure")
            notify(dict(op="barrier", name=role + "_registered", item=item))
            here.send(b"G")
            events.append(dict(kind="released", role=role, pid=proc.pid, at_ns=now()))
        return item

    try:
        notify(dict(op="barrier", name="S_intent", item=identity(os.getpid())))
        launch("W")
        launch("C")
        launch("worker")
        finished = False
        while trigger is None:
            tick = now()
            if tick >= job["operational_ns"]:
                trigger = dict(reason="operational_deadline", at_ns=tick)
            if select.select([parent], [], [], 0)[0]:
                raw = parent.recv(65536)
                if not raw:
                    trigger = trigger or dict(reason="startup_owner_lost", at_ns=tick)
                else:
                    message = json.loads(raw)
                    if message.get("op") == "stop":
                        trigger = message["trigger"]
            if handles["W"][0].poll() is not None:
                raise RuntimeError("watchdog_lost")
            worker = handles["worker"][0]
            if worker.poll() is not None and not finished:
                if worker.returncode:
                    raise RuntimeError("worker_failed")
                finished = True
                channels["C"].send(b"F")
            coordinator = handles["C"][0]
            if coordinator.poll() is not None:
                if finished and coordinator.returncode == 0:
                    trigger = trigger or dict(reason="completed", at_ns=tick)
                else:
                    raise RuntimeError("coordinator_early_exit")
            time.sleep(0.005)
    except Exception as exc:
        error = type(exc).__name__ + ":" + str(exc)
        trigger = trigger or dict(reason=error, at_ns=now())
    finally:
        trigger = trigger or dict(reason="supervisor_failure", at_ns=now())
        end = min(trigger["at_ns"] + 20 * NS, job["hard_ns"] - 10 * NS, trigger.get("ancestor_end_ns", job["hard_ns"]))
        if "W" in channels:
            try:
                channels["W"].send(json.dumps(dict(op="complete" if trigger["reason"] == "completed" else "stop", trigger=trigger)).encode())
            except OSError:
                pass
        while now() < end:
            live = False
            for role, (proc, descriptor, _) in handles.items():
                if select.select([descriptor], [], [], 0)[0]:
                    item = cache[proc.pid]
                    item.setdefault("first_exit_observed_ns", now())
                    if item["first_exit_observed_ns"] is None:
                        item["first_exit_observed_ns"] = now()
                    proc.poll()
                    if proc.returncode is not None and "reaped_ns" not in item:
                        item.update(reaped_ns=now(), reaped=True, exit_code=proc.returncode)
                    continue
                live = True
                if role != "W":
                    signal_fixed(cache[proc.pid], descriptor, signal.SIGKILL if now() >= min(trigger["at_ns"] + 150_000_000, end - NS) else signal.SIGTERM, events)
            if not live:
                break
            time.sleep(0.005)
        for role, (proc, descriptor, output) in handles.items():
            proc.poll()
            cache[proc.pid].update(group_scan_ns=now(), group_live=members(cache[proc.pid]))
            output.close()
            os.close(descriptor)
            channels[role].close()
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            if not pid:
                break
            events.append(dict(kind="adopted_reaped", pid=pid, wait_status=status, at_ns=now()))
        if (directory / "watchdog-result.json").exists():
            watchdog_result = read(directory / "watchdog-result.json")
        result = dict(success=error is None and trigger["reason"] == "completed", error=error, trigger=trigger, cleanup_deadline_ns=end, entries=list(cache.values()), events=events, watchdog=watchdog_result, end_ns=now(), hard_ns=job["hard_ns"], operational_ns=job["operational_ns"], observed_descendants=[])
        result["tail_known"] = all(i.get("reaped") and not i.get("group_live") for i in cache.values())
        result["success"] &= result["tail_known"] and result["end_ns"] <= end
        save(directory / "result.json", result)
        try:
            parent.send(json.dumps(dict(op="result_written")).encode())
        except OSError:
            pass
    return result


def run_owned(directory, job):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / "job.json").exists():
        raise RuntimeError("entry_already_attempted")
    ticket = admit(directory, job)
    client = Client.current()
    header = client.header
    save(directory / "job.json", job)
    save(directory / "attempt.json", dict(ticket=ticket["id"], closed=False))
    request = client.rpc("channel", role="S")
    auth_fd = request["fd"]
    here, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    here.setblocking(False)
    environment = os.environ.copy()
    environment["RUN_SNAPSHOT_AUTH_FD"] = str(auth_fd)
    output = (directory / "startup.log").open("xb", buffering=0)
    birth = now()
    proc = subprocess.Popen([sys.executable, SCRIPT, "supervise", str(directory), str(child.fileno())], stdout=output, stderr=subprocess.STDOUT, start_new_session=True, pass_fds=(auth_fd, child.fileno()), env=environment)
    descriptor = os.pidfd_open(proc.pid)
    s_item = dict(identity(proc.pid), role="S", birth_lower=birth, spawn_before_ns=birth, spawn_after_ns=now())
    s_item["collection"] = int(os.environ.get("RUN_SNAPSHOT_COLLECTION", "0"))
    cache = {proc.pid: (s_item, descriptor)}
    child.close()
    os.close(auth_fd)
    client.rpc("identity", item=s_item)
    events = []
    first_stop = []
    shared = dict(end=None, complete=False)

    def trigger(reason, at_ns):
        if not first_stop:
            first_stop.append(dict(reason=reason, at_ns=at_ns))
            shared["end"] = min(at_ns + 20 * NS, job["hard_ns"] - 10 * NS)

    monitor = RealMonitor(directory, header, trigger)
    monitor.register(s_item)
    if _OWNER:
        _OWNER.register(s_item, descriptor)

    def cutoff():
        while not shared["complete"]:
            tick = now()
            if tick >= job["operational_ns"]:
                trigger("operational_deadline", tick)
            if first_stop:
                for item, fd in list(cache.values()):
                    if not select.select([fd], [], [], 0)[0]:
                        signal_fixed(item, fd, signal.SIGKILL if tick >= min(first_stop[0]["at_ns"] + 150_000_000, shared["end"] - NS) else signal.SIGTERM, events)
                if tick >= job["hard_ns"]:
                    break
            time.sleep(0.005)

    guard = threading.Thread(target=cutoff, name="startup-cutoff")
    guard.start()
    result = None
    error = None
    try:
        while now() < job["hard_ns"]:
            if select.select([here], [], [], 0.005)[0]:
                raw = here.recv(65536)
                if not raw:
                    break
                message = json.loads(raw)
                if message["op"] == "identity":
                    item = message["item"]
                    fd = os.pidfd_open(item["pid"])
                    if identity(item["pid"])["ticks"] != item["ticks"]:
                        raise RuntimeError("startup_identity")
                    cache[item["pid"]] = (item, fd)
                    monitor.register(item)
                    if _OWNER:
                        _OWNER.register(item, fd)
                    here.send(b"A")
                elif message["op"] == "barrier":
                    if _OWNER:
                        _OWNER.barrier(message["name"], message.get("item"))
                    here.send(b"A")
                elif message["op"] == "result_written":
                    if _OWNER:
                        _OWNER.barrier("result_written", s_item)
            if monitor.error:
                trigger("real_sampling:" + monitor.error, now())
            if proc.poll() is not None:
                break
        if proc.poll() is None:
            trigger("supervisor_hard_tail", now())
        if (directory / "result.json").exists():
            result = read(directory / "result.json")
        else:
            result = dict(success=False, tail_known=False, error="missing_supervisor_result", entries=[])
    except Exception as exc:
        error = str(exc)
        trigger("startup_failure:" + error, now())
    finally:
        tick = now()
        owner_result = _OWNER.finish(job["hard_ns"]) if _OWNER else None
        if proc.poll() is not None:
            item = monitor.entries[proc.pid]
            if not item.get("first_exit_observed_ns"):
                item["first_exit_observed_ns"] = tick
                try:
                    monitor.guard.exited(item, tick)
                except RuntimeError as exc:
                    monitor.fail(str(exc))
        sampling_valid = monitor.finish(min(job["hard_ns"], now() + 200_000_000))
        shared["complete"] = True
        guard.join(max(0, (job["hard_ns"] - now()) / NS))
        outer_known = proc.poll() is not None and not guard.is_alive()
        actual = []
        for item, fd in list(cache.values()):
            exited = bool(select.select([fd], [], [], 0)[0])
            group = members(item)
            outer_known &= exited and not group
            observed = dict(item, exited=exited, group_scan_ns=now(), group_live=group)
            actual.append(observed)
            os.close(fd)
        output.close()
        here.close()
        outer = dict(tail_known=bool(outer_known), supervisor=s_item, exit_code=proc.returncode, observed_exit_ns=tick, identities=actual, events=events)
        save(directory / "startup-result.json", outer)
        result = result or dict(success=False, error=error, tail_known=False, entries=[])
        result.update(startup=outer, sampling_valid=sampling_valid, cleanup_complete=bool(outer_known), diagnostic_valid=bool(result.get("success")), hard_breached=now() > job["hard_ns"], end_ns=now())
        result["tail_known"] = bool(result["tail_known"] and outer_known and not guard.is_alive())
        result["success"] = bool(result.get("success") and result["tail_known"] and sampling_valid and not result["hard_breached"])
        result["control_outcome"] = "completed" if result["success"] else "rejected"
        if first_stop:
            result["startup_trigger"] = first_stop[0]
        if owner_result:
            result["independent_owner"] = owner_result
        save(directory / "collection-result.json", result)
        save(directory / "attempt.json", dict(ticket=ticket["id"], closed=result["tail_known"], end_ns=now()))
        client.rpc("close", id=ticket["id"], result=dict(success=result["success"], unexpected=bool(monitor.error or not result["tail_known"]), error=result.get("error")))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["supervise", "role"])
    parser.add_argument("arguments", nargs="+")
    arguments = parser.parse_args()
    if arguments.action == "supervise":
        supervise(Path(arguments.arguments[0]), int(arguments.arguments[1]))
    else:
        raise SystemExit(role_main(arguments.arguments[0], arguments.arguments[1], int(arguments.arguments[2])))
