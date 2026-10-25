"""Bounded G0 supervision. Inherited authority descriptors are required."""

import argparse
import array
import ctypes
import json
import os
from pathlib import Path
import select
import shutil
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
_CHANNELS = {}
LIBC_PATH = "/usr/lib/x86_64-linux-gnu/libc.so.6"
_LIBC = None


def pidfd_backend():
    """The sealed libc named-function ABI; never fall back to PID signals."""
    global _LIBC
    if _LIBC is None:
        library = ctypes.CDLL(LIBC_PATH, use_errno=True)
        library.pidfd_open.argtypes = [ctypes.c_int, ctypes.c_uint]
        library.pidfd_open.restype = ctypes.c_int
        library.pidfd_send_signal.argtypes = [
            ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint
        ]
        library.pidfd_send_signal.restype = ctypes.c_int
        library.prctl.argtypes = [
            ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong,
            ctypes.c_ulong, ctypes.c_ulong
        ]
        library.prctl.restype = ctypes.c_int
        _LIBC = library
    return _LIBC


def pidfd_open(pid):
    descriptor = pidfd_backend().pidfd_open(pid, 0)
    if descriptor < 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))
    return descriptor


def pidfd_signal(descriptor, signum):
    if pidfd_backend().pidfd_send_signal(descriptor, signum, None, 0) < 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))


def become_subreaper():
    library = pidfd_backend()
    if library.prctl(36, 1, 0, 0, 0) != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))
    result = ctypes.c_int()
    pointer = ctypes.cast(ctypes.pointer(result), ctypes.c_void_p).value
    if library.prctl(37, pointer, 0, 0, 0) != 0 or result.value != 1:
        raise RuntimeError("subreaper_not_confirmed")


def capability_check(directory):
    """Run only inside the protected actual entry or a frozen case."""
    clock_before = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    clock_middle = time.monotonic_ns()
    clock_after = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    if not clock_before <= clock_middle <= clock_after:
        raise RuntimeError("monotonic_clock_source")
    first = identity(os.getpid())
    descriptor = pidfd_open(first["pid"])
    pair = []
    received = []
    try:
        pidfd_signal(descriptor, 0)
        if select.select([descriptor], [], [], 0)[0]:
            raise RuntimeError("self_pidfd_readable")
        if identity(first["pid"])["ticks"] != first["ticks"]:
            raise RuntimeError("capability_identity")
        pair = list(socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET))
        pair[1].setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
        for channel in pair:
            channel.setblocking(False)
        pair[0].sendmsg([b"capability"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                         array.array("i", [descriptor]))])
        raw, anc, flags, _ = pair[1].recvmsg(128, 1024)
        credentials = []
        for level, kind, value in anc:
            if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                fds = array.array("i")
                fds.frombytes(value)
                received.extend(fds)
            elif level == socket.SOL_SOCKET and kind == socket.SCM_CREDENTIALS:
                credentials.append(value)
        if raw != b"capability" or flags or len(received) != 1 or len(credentials) != 1:
            raise RuntimeError("capability_descriptor_transfer")
        pidfd_signal(received[0], 0)
        become_subreaper()
        sample = host_sample(directory, [dict(first, role="startup")], directory)
        row = sample["processes"][0]
        if row.get("unavailable") or min(row["cpu"], row["rss"]) < 0:
            raise RuntimeError("capability_sampling")
        return dict(backend="libc-named-pidfd", library=LIBC_PATH,
                    identity=first, sample=sample, at_ns=now(),
                    monotonic_ns=time.clock_gettime_ns(time.CLOCK_MONOTONIC),
                    clock_correlation=[clock_before, clock_middle, clock_after],
                    boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip())
    finally:
        for fd in received:
            os.close(fd)
        for channel in pair:
            channel.close()
        os.close(descriptor)


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
    row = dict(values)
    row.setdefault("at_ns", now())
    row["kind"] = kind
    with Path(path).open("a") as stream:
        stream.write(json.dumps(row) + "\n")


def identity(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    return dict(
        pid=pid, ppid=int(fields[1]), pgid=int(fields[2]),
        session=int(fields[3]), ticks=int(fields[19]), state=fields[0],
    )


def generation(item):
    return (item["pid"], item["ticks"])


def same_process(item):
    try:
        value = identity(item["pid"])
        if value["ticks"] != item["ticks"]:
            raise RuntimeError("sample_identity")
        return value["state"] != "Z"
    except (FileNotFoundError, ProcessLookupError):
        return False


def members(item):
    result = []
    for path in Path("/proc").glob("[0-9]*/stat"):
        try:
            value = identity(int(path.parent.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        if value["state"] != "Z" and value["pgid"] == item["pgid"]:
            if value["session"] != item["session"]:
                raise RuntimeError("group_identity_changed")
            result.append(value)
    if len(result) > 64:
        raise RuntimeError("group_capacity")
    return result


class Client:
    def __init__(self, descriptor):
        self.fd = int(descriptor)
        duplicate = os.dup(self.fd)
        self.socket = None
        self.lock = threading.Lock()
        self.sequence = 0
        self.operational_ns = None
        try:
            self.socket = socket.socket(fileno=duplicate)
            self.socket.setblocking(False)
            self.header = self.rpc("hello")
        except BaseException:
            if self.socket is None:
                os.close(duplicate)
            else:
                self.socket.close()
            raise
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
        requested_end = data.pop("_deadline_ns", 2**63 - 1)
        end = min(now() + 100_000_000, self.operational_ns or 2**63 - 1, requested_end)
        if not self.lock.acquire(timeout=max(0, (end - now()) / NS)):
            raise RuntimeError("authority_lock_timeout")
        try:
            self.sequence += 1
            domain = os.environ.get("RUN_SNAPSHOT_TEST_DOMAIN")
            if domain:
                data = dict(data, _domain=domain)
            raw = json.dumps(dict(seq=self.sequence, op=op, data=data)).encode()
            if len(raw) > 65536:
                raise RuntimeError("message_capacity")
            if not select.select([], [self.socket], [], max(0, (end - now()) / NS))[1]:
                raise RuntimeError("authority_send_timeout")
            self.socket.send(raw)
            if not select.select([self.socket], [], [], max(0, (end - now()) / NS))[0]:
                raise RuntimeError("authority_ack_timeout")
            raw, ancillary, flags, _ = self.socket.recvmsg(65536, 1024)
            if not raw or flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
                raise RuntimeError("authority_lost_or_truncated")
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
                        descriptors.frombytes(value)
                if len(descriptors) != 1:
                    for fd in descriptors:
                        os.close(fd)
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
        rss = int(Path(f"/proc/{item['pid']}/statm").read_text().split()[1])
        last = identity(item["pid"])
        return dict(
            last, role=item["role"], rss=rss * os.sysconf("SC_PAGE_SIZE"),
            cpu=(int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
            read_begin_ns=begin, read_end_ns=now(),
            identity_before=first["ticks"], identity_after=last["ticks"],
        )
    except (OSError, ValueError):
        return dict(
            pid=item["pid"], role=item["role"], unavailable=True,
            read_begin_ns=begin, read_end_ns=now(),
        )


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
            known[pid] = dict(
                item, role=parent["role"] + "_child",
                birth_lower=parent["birth_lower"], inferred=True,
                spawn_before_ns=parent["birth_lower"], spawn_after_ns=now(),
                attempt=parent["attempt"],
            )
            added = True
        if len(known) > 64:
            raise RuntimeError("identity_capacity")
        if not added:
            return list(known.values())


class Samples:
    def __init__(self, swap_start):
        self.swap_start = swap_start
        self.first, self.last, self.count = {}, {}, {}
        self.over = 0
        self.invalid = False

    def fail(self, reason):
        self.invalid = True
        raise RuntimeError(reason)

    def accept(self, value, expected, *, track_total=True):
        if (value["mem_available"] < 4 * GIB or value["disk_free"] < 20 * GIB
                or value["swap_used"] - self.swap_start > GIB
                or value["evidence_bytes"] > 256 * 1024**2
                or value["batch_bytes"] > 2 * GIB):
            self.fail("resource_stop")
        rows = {r["pid"]: r for r in value["processes"]}
        total = 0
        for item in expected:
            row = rows.get(item["pid"])
            if (row is None or row.get("unavailable")
                    or "cpu" not in row or "rss" not in row):
                self.fail("sample_unavailable")
            if (row["ticks"] != item["ticks"]
                    or row.get("identity_before", row["ticks"]) != item["ticks"]
                    or row.get("identity_after", row["ticks"]) != item["ticks"]):
                self.fail("sample_identity")
            if min(row["cpu"], row["rss"]) < 0:
                self.fail("negative_sample")
            begin = row.get("read_begin_ns", value["at_ns"])
            end = row.get("read_end_ns", value["at_ns"])
            key = generation(item)
            previous = self.last.get(key)
            if end < begin or end - begin > 250_000_000:
                self.fail("sample_read_gap")
            if previous and end - previous[0] > 250_000_000:
                self.fail("sample_gap")
            if previous and row["cpu"] < previous[2]:
                self.fail("cpu_regression")
            birth = item.get("birth_lower", item.get("started_ns"))
            if previous is None and end - birth > 250_000_000:
                self.fail("first_sample_gap")
            self.first.setdefault(key, dict(row))
            self.last[key] = (begin, end, row["cpu"])
            self.count[key] = self.count.get(key, 0) + 1
            total += row["rss"]
            if item["role"] in {"S", "C", "W"} and row["rss"] > 512 * 1024**2:
                self.fail("tool_rss")
        if track_total:
            self.over = self.over + 1 if total > 8 * GIB else 0
        if self.over >= 2:
            self.fail("rss_sum")

    def exited(self, item, at_ns, *, worker=False):
        key = generation(item)
        previous = self.last.get(key)
        if previous is None or at_ns - previous[0] > 250_000_000:
            self.fail("exit_sample_gap")
        if at_ns < previous[1]:
            self.fail("exit_observation_order")
        if worker and self.count.get(key, 0) < 2:
            self.fail("worker_cpu_points")


def host_sample(directory, entries, batch_directory):
    memory = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        parts = line.split()
        if len(parts) == 3:
            memory[parts[0][:-1]] = int(parts[1]) * 1024

    def size(base):
        total = 0
        for path in Path(base).rglob("*"):
            try:
                if path.is_file():
                    total += path.stat().st_size
            except FileNotFoundError:
                continue
        return total

    entry_bytes = size(directory)
    if Path(directory) == Path(batch_directory):
        entry_bytes = max([size(Path(directory) / ("collection-" + str(i))) + size(Path(directory) / ("collection-" + str(i) + "-cases")) for i in (1, 2, 3)])
    return dict(
        at_ns=now(), mem_available=memory["MemAvailable"],
        disk_free=shutil.disk_usage(directory).free,
        swap_used=memory["SwapTotal"] - memory["SwapFree"],
        evidence_bytes=entry_bytes, batch_bytes=size(batch_directory),
        processes=[sample_process(i) for i in entries],
    )


class MeasurementView:
    """Child-only read/exit boundary feeding the production monitor state machine."""

    def __init__(self, directory, spec, swap_start):
        self.directory, self.spec = Path(directory), spec
        self.finished = False
        self.error, self.invalid = None, False
        self.phase_ready = spec.get("phase") != "cleanup"
        self.sequence = 0
        self.rows = []
        self.raw_first = None
        self.reported = False
        self.virtual = RealMonitor(
            directory, dict(swap_start=swap_start, root=str(directory)),
            self.failed, autostart=False, measurement_only=True,
        )

    def failed(self, reason, at_ns):
        self.invalid = True
        if self.error is None:
            self.error = reason

    def run(self, raw, real_entries):
        if self.finished or self.invalid:
            return
        entries = json.loads(json.dumps(real_entries))
        value = json.loads(json.dumps(raw))
        if not any(i["role"] == "worker" for i in entries):
            return
        self.raw_first = self.raw_first or raw
        self.sequence += 1
        at = NS + self.sequence * 100_000_000
        kind, amount = self.spec["kind"], self.spec.get("amount")
        if kind == "first" and self.sequence == 1:
            at = NS + amount
        if kind == "adjacent" and self.sequence >= 2:
            at = self.virtual.guard.last[generation(next(i for i in entries if i["role"] == "worker"))][0] + amount
        for item in entries:
            item["birth_lower"] = NS
            self.virtual.entries.setdefault(generation(item), item)
        if self.virtual.guard.last:
            at = max(at, max(value[1] for value in self.virtual.guard.last.values()))
        for row in value["processes"]:
            row.update(read_begin_ns=at, read_end_ns=at)
        value["at_ns"] = at
        target = next(row for row in value["processes"] if row["role"] == "worker")
        selected = entries
        if kind == "points":
            if self.sequence > amount:
                selected = [i for i in entries if i["role"] != "worker"]
                value["processes"] = [i for i in value["processes"] if i["role"] != "worker"]
        elif kind == "read_span" and self.sequence == 1:
            for row in value["processes"]:
                row.update(read_begin_ns=NS, read_end_ns=NS + amount)
        elif self.sequence >= 2:
            if kind == "unavailable":
                target["unavailable"] = True
            elif kind == "missing":
                value["processes"].remove(target)
            elif kind in {"cpu_missing", "rss_missing"}:
                target.pop(kind.split("_")[0])
            elif kind == "identity":
                target["ticks"] += 1
            elif kind == "negative_cpu":
                target["cpu"] = -1
            elif kind == "negative_rss":
                target["rss"] = -1
            elif kind == "cpu_regression":
                target["cpu"] = self.virtual.guard.last[generation(next(i for i in entries if i["role"] == "worker"))][2] - 1
            elif kind == "resource":
                value[self.spec["field"]] = amount
            elif kind == "role_rss":
                next(row for row in value["processes"] if row["role"] == self.spec["role"])["rss"] = amount
            elif kind == "rss_sum":
                target["rss"] = amount - sum(row["rss"] for row in value["processes"] if row is not target)
                if self.spec.get("recover") and self.sequence >= 3:
                    target["rss"] = 1024
            elif kind == "swap_cumulative":
                value["swap_used"] = self.virtual.guard.swap_start + (600 if self.sequence == 2 else 1200) * 1024**2
        self.rows.append(dict(raw=raw, view=value, sequence=self.sequence))
        self.virtual.accept_return(value, selected)

    def observe_exit(self, item, original_tick, source):
        if generation(item) not in self.virtual.entries:
            return
        last = self.virtual.guard.last.get(generation(item))
        tick = last[1] if last else NS
        if self.spec["kind"] == "exit" and item["role"] == "worker":
            tick += self.spec["amount"]
        self.virtual.observe_exit(item, tick, "injected_exit:" + source)
        self.rows.append(dict(exit_identity=item, original_exit_ns=original_tick,
                              view_exit_ns=tick, source=source))

    def finish(self, deadline):
        if self.sequence == 0:
            self.failed("view_unstarted", now())
        valid = self.virtual.finish(deadline)
        self.virtual.close_descriptors()
        self.invalid = self.invalid or not valid
        self.finished = True
        event(self.directory / "injected-resources.jsonl", "child_view",
              spec=self.spec, raw=self.raw_first, view_rows=self.rows,
              error=self.error, invalid=self.invalid,
              verdicts=[dict(pid=k[0], ticks=k[1], valid=v)
                        for k, v in self.virtual.verdicts.items()],
              swap_start=self.virtual.guard.swap_start, host_deadlines_changed=False,
              boundary="RealMonitor.accept_return/observe_exit/finish")
        return valid


class RealMonitor:
    """Independent real stream; synthetic views never enter this guard."""

    def __init__(self, directory, header, trigger, *, authority=None,
                 root_monitor=False, attempt=None, discovered=None, view=None,
                 autostart=True, measurement_only=False, fault_nonce=None):
        self.directory, self.header = Path(directory), header
        self.trigger, self.authority = trigger, authority
        self.root_monitor, self.attempt = root_monitor, attempt
        self.discovered = discovered
        self.measurement_only = measurement_only
        self.fault_nonce = fault_nonce
        self.view_proof = False
        self.entries, self.descriptors, self.verdicts = {}, {}, {}
        self.guard = Samples(header["swap_start"])
        self.lock = threading.RLock()
        self.stopping = threading.Event()
        self.error = None
        self.sequence = 0
        self.identity_log = self.directory / (
            "injected-identities.jsonl" if measurement_only else "identities.jsonl"
        )
        self.view = MeasurementView(directory, view, header["swap_start"]) if view else None
        self.observation_start_ns = now()
        self.thread = threading.Thread(target=self.loop, name="real-proc-sampler")
        self.closed = False
        self.finish_lock = threading.Lock()
        self.finished_result = None
        try:
            if root_monitor:
                item = dict(identity(os.getpid()), role="startup",
                            birth_lower=self.observation_start_ns,
                            observation_only=True, attempt="root")
                self.register(item)
                outer = dict(identity(os.getppid()), role="outer",
                             birth_lower=now(), observation_only=True,
                             attempt="root")
                self.register(outer)
            if autostart:
                self.start()
        except BaseException:
            self.close_descriptors()
            raise

    def start(self):
        self.thread.start()

    def close_descriptors(self):
        if self.closed:
            return
        if self.thread.is_alive():
            raise RuntimeError("closing_live_sampler")
        for fd in self.descriptors.values():
            os.close(fd)
        self.closed = True

    def register(self, item):
        key = generation(item)
        with self.lock:
            if key in self.entries:
                if self.entries[key].get("inferred") and not item.get("inferred"):
                    self.entries[key].update(item)
                return
            fd = None
            self.entries[key] = dict(item, registration_started_ns=now())
            try:
                if len(self.active()) > 64:
                    raise RuntimeError("identity_capacity")
                fd = pidfd_open(item["pid"])
                self.descriptors[key] = fd
                current = identity(item["pid"])
                if current["ticks"] != item["ticks"]:
                    raise RuntimeError("registration_identity")
                pidfd_signal(fd, 0)
                event(self.identity_log, "registered", item=item)
                value = host_sample(self.directory, [item], self.header["root"])
                event(self.directory / "real-resources.jsonl", "registration_sample",
                      sample=value)
                self.guard.accept(value, [item], track_total=False)
            except BaseException as exc:
                self.guard.invalid = True
                self.verdicts[key] = False
                self.fail("registration:" + type(exc).__name__ + ":" + str(exc))
                raise

    def active(self):
        return [i for k, i in list(self.entries.items()) if k not in self.verdicts]

    def fail(self, reason):
        self.guard.invalid = True
        if self.error is None:
            self.error = reason
            self.trigger(reason, now())
            if not self.root_monitor and not self.measurement_only:
                try:
                    Client.current().rpc("stop", reason="real_sampling:" + reason,
                                         at_ns=now())
                except Exception:
                    self.error = reason + ":root_notification_failed"

    def accept_return(self, value, entries):
        try:
            self.guard.accept(value, entries)
        except Exception as exc:
            self.fail(str(exc))

    def observe_exit(self, item, tick, source):
        key = generation(item)
        with self.lock:
            if key not in self.entries or key in self.verdicts:
                return
            entry = self.entries[key]
            entry.setdefault("first_exit_observed_ns", tick)
            entry.setdefault("first_exit_source", source)
            try:
                self.guard.exited(entry, entry["first_exit_observed_ns"],
                                  worker=entry["role"] == "worker")
                valid = True
            except RuntimeError as exc:
                valid = False
                self.fail(str(exc))
            self.verdicts[key] = valid and not self.guard.invalid
            if self.view is not None:
                self.view.observe_exit(entry, entry["first_exit_observed_ns"], source)
            event(self.identity_log, "finalize_identity",
                  item=entry, sampling_valid=valid,
                  cpu_points=self.guard.count.get(key, 0))

    def loop(self):
        while not self.stopping.is_set():
            start = now()
            try:
                with self.lock:
                    live = []
                    for key, item in list(self.entries.items()):
                        if key in self.verdicts:
                            continue
                        if select.select([self.descriptors[key]], [], [], 0)[0]:
                            self.observe_exit(item, now(), "pidfd")
                        else:
                            current = identity(item["pid"])
                            if current["ticks"] != item["ticks"]:
                                raise RuntimeError("sample_identity")
                            live.append(item)
                    if live:
                        # Root discovery is confined to registered controlled
                        # subtrees. The pre-existing startup host is not a root.
                        roots = [i for i in live if not i.get("observation_only") and not i.get("owner_only")]
                        found = owned_processes(roots) if roots and self.root_monitor else []
                        for item in found:
                            key = generation(item)
                            pending_parent = self.authority is not None and any(
                                not intent.get("resolved")
                                and intent["parent_pid"] == item["ppid"]
                                for intent in list(self.authority.intents.values())
                            )
                            owner_only = self.entries.get(key, {}).get("owner_only")
                            if (key not in self.entries or owner_only) and not pending_parent:
                                self.register(item)
                                if owner_only:
                                    self.entries[key]["owner_only"] = False
                                if self.authority is not None:
                                    item = self.authority.prepare_identity(item)
                                if self.discovered is not None:
                                    self.discovered(item)
                        live = self.active()
                        value = host_sample(self.directory, live, self.header["root"])
                        self.sequence += 1
                        event(self.directory / "real-resources.jsonl", "sample",
                              sequence=self.sequence, sample=value)
                        self.guard.accept(value, live)
                        ready = any(i["role"] == "worker" and self.guard.count.get(generation(i), 0) >= 2 for i in live)
                        if self.view and ready and self.view.phase_ready and not self.view.finished:
                            self.view.run(value, live)
                            if self.view.sequence and not self.view_proof:
                                Client.current().rpc(
                                    "fault_observed", attempt=self.attempt,
                                    nonce=self.fault_nonce, boundary="monitor_read_return",
                                    original_ns=now(), evidence=dict(
                                        view=self.view.spec, physical_sequence=self.sequence,
                                        raw_at_ns=value["at_ns"],
                                    ),
                                )
                                self.view_proof = True
                            if self.view.error and not self.view.reported:
                                self.view.reported = True
                                detected = now()
                                Client.current().rpc("child_trigger", attempt=self.attempt, reason=self.view.error, at_ns=detected)
                                self.trigger(self.view.error, detected)
            except Exception as exc:
                self.fail(type(exc).__name__ + ":" + str(exc))
            self.stopping.wait(max(0, (start + 100_000_000 - now()) / NS))

    def finish(self, deadline):
        if not self.finish_lock.acquire(timeout=max(0, (deadline-now())/NS)):
            self.fail("finish_lock_deadline")
            return False
        try:
            if self.finished_result is None:
                self.finished_result = self.finish_once(deadline)
            return self.finished_result
        finally:
            self.finish_lock.release()

    def finish_once(self, deadline):
        while not self.measurement_only and now() < deadline:
            with self.lock:
                active = [i for i in self.active() if not i.get("observation_only")]
            if not active:
                break
            time.sleep(0.005)
        with self.lock:
            if any(not i.get("observation_only") for i in self.active()):
                self.fail("sampling_unfinalized_identity")
            # The existing startup process is observed over this explicit
            # interval, not represented as a newly born/exited subprocess.
            for item in self.active():
                if not item.get("observation_only"):
                    self.verdicts[generation(item)] = False
                    continue
                try:
                    self.guard.exited(item, now(), worker=False)
                    self.verdicts[generation(item)] = True
                except RuntimeError as exc:
                    self.fail(str(exc))
                    self.verdicts[generation(item)] = False
                event(self.identity_log, "observation_end",
                      item=item, end_ns=now(), begin_ns=self.observation_start_ns)
        if self.view is not None:
            self.view.finish(deadline)
        self.stopping.set()
        if self.thread.ident is not None:
            self.thread.join(max(0, (deadline - now()) / NS))
        if self.thread.is_alive():
            self.fail("sampler_thread_tail")
        good = (self.error is None and not self.guard.invalid
                and not self.thread.is_alive()
                and set(self.verdicts) == set(self.entries)
                and all(self.verdicts.values()))
        return good


def finite_monitor_fixture(directory, variant):
    global host_sample
    client = Client.current()
    context = client.rpc("case_context")
    frozen = context["spec"]["monitor_fixture"]
    if frozen["variant"] != variant:
        raise RuntimeError("monitor_fixture_not_frozen")
    directory = Path(directory)
    directory.mkdir()
    failures, rows = [], []
    original = host_sample
    monitor = RealMonitor(directory, client.header,
                          lambda reason, at: failures.append(dict(reason=reason, at_ns=at)),
                          autostart=False, measurement_only=True)
    item = dict(identity(os.getpid()), role="fixture_observer", birth_lower=now(),
                observation_only=True)

    def injected(*args, **kwargs):
        raw = original(*args, **kwargs)
        view = json.loads(json.dumps(raw))
        row = view["processes"][0]
        if variant == "unavailable":
            row["unavailable"] = True
        elif variant == "cpu_missing":
            row.pop("cpu")
        elif variant == "identity":
            row["ticks"] += 1
        elif variant == "read_gap":
            row["read_end_ns"] = row["read_begin_ns"] + 250_000_001
        rows.append(dict(raw=raw, injected=view, nonce=frozen["nonce"]))
        return view

    try:
        host_sample = injected
        if variant == "finish_race":
            monitor.register(item)
            results = []
            threads = [threading.Thread(target=lambda: results.append(monitor.finish(context["operational_ns"]))) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(max(0, (context["operational_ns"]-now())/NS))
            assert results == [True, True] and not any(t.is_alive() for t in threads)
        else:
            try:
                monitor.register(item)
            except RuntimeError:
                pass
            else:
                raise RuntimeError("registration_injection_not_rejected")
            first = monitor.error
            assert monitor.guard.invalid and first == frozen["expected"]
            host_sample = original
            monitor.accept_return(original(directory, [item], client.header["root"]), [item])
            assert monitor.guard.invalid and monitor.error == first
            assert monitor.finish(context["operational_ns"]) is False
    finally:
        host_sample = original
        monitor.finish(context["operational_ns"])
        monitor.close_descriptors()
        save(directory / "monitor-boundary.json", dict(
            variant=variant, nonce=frozen["nonce"], rows=rows, failures=failures,
            invalid=monitor.guard.invalid, result=monitor.finished_result,
            verdicts=[dict(pid=k[0], ticks=k[1], valid=v) for k, v in monitor.verdicts.items()],
            actual_entry="RealMonitor.register/accept_return/finish", at_ns=now(),
        ))


def diagnostic_permit(directory, config, fault=None):
    if config.get("stage") != "G0" or config.get("tasks") != 250:
        raise RuntimeError("G0_only_no_100k_authorization")
    return Client.current().rpc("diagnose", directory=str(directory), config=config, fault=fault)


def diagnostic_closed(ticket, result):
    Client.current().rpc("close", id=ticket["id"], result=result)


def fault_observed(job, boundary, **evidence):
    Client.current().rpc("fault_observed", attempt=job["attempt"],
                         nonce=job["fault_nonce"], boundary=boundary,
                         original_ns=now(), evidence=evidence)


def admit(directory, job):
    from run_snapshot_g0 import admission_reason, canonical, check_sources

    client = Client.current()
    canonical(str(directory), Path(client.header["root"]))
    reason = admission_reason(client.header, job, now())
    if reason:
        raise RuntimeError(reason)
    check_sources(job["sources"])
    return client.rpc("attempt", directory=str(directory), job=job)



def await_ancestors(client, item, operational_ns):
    end = min(now() + 100_000_000, operational_ns)
    while now() < end:
        state = client.rpc("status", _deadline_ns=end)
        if state["stop"]:
            raise RuntimeError("root_stopped")
        found = next((i for i in state["identities"]
                      if generation(i) == generation(item)), None)
        if (found is not None and set(found["ancestor_nodes"]) <= set(found["acknowledged"])
                and set(found["controller_nodes"]) <= set(found["controller_acknowledged"])):
            return
        time.sleep(0.001)
    raise RuntimeError("ancestor_ack_timeout")


class ControlStop(RuntimeError):
    def __init__(self, trigger):
        super().__init__("ancestor_stop_during_ack")
        self.trigger = trigger


def send(channel, message, *, descriptor=None, deadline=None):
    channel.setblocking(False)
    end = min(now() + 100_000_000, deadline or 2**63 - 1)
    if not select.select([], [channel], [], max(0, (end - now()) / NS))[1]:
        raise RuntimeError("channel_send_timeout")
    payload = message if isinstance(message, bytes) else json.dumps(message).encode()
    state = _CHANNELS.setdefault(channel, dict(sent=0, received=0))
    state["sent"] += 1
    envelope = dict(seq=state["sent"], payload=payload.decode())
    if message == b"A":
        envelope["ack"] = state["received"]
    raw = json.dumps(envelope).encode()
    if len(raw) > 65536:
        raise RuntimeError("message_capacity")
    anc = [] if descriptor is None else [
        (socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [descriptor]))
    ]
    channel.sendmsg([raw], anc)
    return state["sent"]


def receive(channel, deadline):
    if not select.select([channel], [], [], max(0, (deadline - now()) / NS))[0]:
        raise RuntimeError("channel_receive_timeout")
    raw, anc, flags, _ = channel.recvmsg(65536, 1024)
    if not raw or flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
        raise RuntimeError("channel_eof_or_capacity")
    envelope = json.loads(raw)
    state = _CHANNELS.setdefault(channel, dict(sent=0, received=0))
    if envelope["seq"] != state["received"] + 1:
        raise RuntimeError("channel_sequence")
    state["received"] = envelope["seq"]
    state["last_ack"] = envelope.get("ack")
    return envelope["payload"].encode(), anc


def notify(channel, message, deadline, descriptor=None):
    end = min(now() + 100_000_000, deadline)
    sequence = send(channel, message, descriptor=descriptor, deadline=end)
    raw, _ = receive(channel, end)
    if raw != b"A":
        try:
            message = json.loads(raw)
            if message.get("op") == "stop":
                raise ControlStop(message["trigger"])
        except (ValueError, UnicodeError):
            pass
        raise RuntimeError("channel_ack")
    if _CHANNELS[channel].get("last_ack") != sequence:
        raise RuntimeError("channel_ack_sequence")


def shrink(trigger, incoming, hard):
    """Retain the first cause and independently tighten every cleanup bound."""
    end = min(incoming["at_ns"] + 20 * NS, hard - 10 * NS,
              incoming.get("ancestor_end_ns", hard),
              incoming.get("cleanup_deadline_ns", hard))
    if trigger is None:
        return dict(incoming, cleanup_deadline_ns=end,
                    first_reason_at_ns=incoming["at_ns"])
    trigger["at_ns"] = min(trigger["at_ns"], incoming["at_ns"])
    trigger["cleanup_deadline_ns"] = min(trigger["cleanup_deadline_ns"], end)
    return trigger


def signal_fixed(item, descriptor, sig, events, sent):
    key = (generation(item), int(sig))
    if key in sent:
        return
    try:
        pidfd_signal(descriptor, sig)
        events.append(dict(kind="signal", at_ns=now(), pid=item["pid"],
                           ticks=item["ticks"], signal=int(sig)))
        sent.add(key)
    except ProcessLookupError:
        pass
    if len(events) > 1024:
        raise RuntimeError("event_capacity")


def watchdog(channel, job):
    channel.setblocking(False)
    cache, events, sent = {}, [], set()
    retired = []
    trigger = None
    done = False
    send(channel, dict(ready=True), deadline=job["operational_ns"])
    while now() < job["hard_ns"]:
        tick = now()
        for key, (item, fd) in list(cache.items()):
            if select.select([fd], [], [], 0)[0]:
                retired.append(dict(item=item, first_exit_observed_ns=tick))
                os.close(fd)
                del cache[key]
        if len(retired) > 1024:
            raise RuntimeError("watchdog_history_capacity")
        if tick >= job["operational_ns"]:
            trigger = shrink(trigger, dict(reason="operational_deadline",
                                          at_ns=tick), job["hard_ns"])
        for _ in range(64):
            if not select.select([channel], [], [], 0)[0]:
                break
            try:
                raw, anc = receive(channel, now())
                message = json.loads(raw)
                if message["op"] == "identity":
                    fds = array.array("i")
                    for level, kind, data in anc:
                        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                            fds.frombytes(data)
                    if len(fds) != 1 or len(cache) >= 64:
                        raise RuntimeError("watchdog_identity_capacity")
                    item = message["item"]
                    cache[generation(item)] = (item, fds[0])
                    send(channel, b"A", deadline=job["operational_ns"])
                elif message["op"] in {"stop", "complete"}:
                    trigger = shrink(trigger, message["trigger"], job["hard_ns"])
                    done = message["op"] == "complete"
            except Exception:
                trigger = shrink(trigger, dict(reason="supervisor_lost",
                                              at_ns=now()), job["hard_ns"])
                break
        if trigger:
            tick = now()
            end = trigger["cleanup_deadline_ns"]
            for item, fd in list(cache.values()):
                if not select.select([fd], [], [], 0)[0]:
                    sig = signal.SIGKILL if tick >= min(
                        trigger["at_ns"] + 150_000_000, end - NS
                    ) else signal.SIGTERM
                    signal_fixed(item, fd, sig, events, sent)
            if all(select.select([fd], [], [], 0)[0] for _, fd in cache.values()):
                break
            if tick >= end:
                break
        time.sleep(0.005)
    for _, fd in cache.values():
        os.close(fd)
    return dict(events=events, trigger=trigger, complete=done, retired=retired, end_ns=now())


def role_main(role, directory, descriptor, job):
    channel = socket.socket(fileno=descriptor)
    channel.setblocking(False)
    if role == "W":
        raw, _ = receive(channel, job["operational_ns"])
        if raw != b"R":
            return 83
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        result = watchdog(channel, job)
        save(Path(directory) / "watchdog-result.json", result)
        return 0
    raw, _ = receive(channel, job["operational_ns"])
    if raw != b"G":
        return 80
    if role == "C":
        if job.get("coordinator_mode") == "early_exit":
            raw, _ = receive(channel, job["operational_ns"])
            return 7 if raw == b"E" else 84
        if job.get("coordinator_mode") == "blocked":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            while now() < job["hard_ns"]:
                time.sleep(0.05)
            return 81
        raw, _ = receive(channel, job["operational_ns"])
        return 0 if raw == b"F" else 82
    event(Path(directory) / "worker-gate.jsonl", "released", pid=os.getpid())
    if job.get("inherit_authority", True):
        os.set_inheritable(int(os.environ["RUN_SNAPSHOT_AUTH_FD"]), True)
    else:
        os.environ.pop("RUN_SNAPSHOT_AUTH_FD", None)
    if "owner_fd" in job:
        os.set_inheritable(job["owner_fd"], True)
        os.environ["RUN_SNAPSHOT_OWNER_FD"] = str(job["owner_fd"])
    os.environ["RUN_SNAPSHOT_CURRENT_ATTEMPT"] = str(job.get("attempt", "static"))
    os.execvpe(job["command"][0], job["command"], os.environ.copy())


def observe_handle(proc, item, client, parent, job, events):
    """Record the first kernel exit before Popen.poll can reap it."""
    if proc.returncode is not None:
        return True
    try:
        state = identity(proc.pid)
        if state["ticks"] != item["ticks"]:
            raise RuntimeError("exit_identity_changed")
        exited = state["state"] == "Z"
    except FileNotFoundError:
        exited = True
    if not exited:
        return False
    tick = now()
    item.setdefault("first_exit_observed_ns", tick)
    item.setdefault("first_exit_source", "proc_Z_or_gone")
    client.rpc("exited", item=item, at_ns=item["first_exit_observed_ns"])
    if parent is not None:
        notify(parent, dict(op="exited", item=item), job["hard_ns"])
    code = proc.poll()
    if code is not None:
        item.setdefault("reaped_ns", now())
        item.update(reaped=True, exit_code=code)
        events.append(dict(kind="reaped", pid=proc.pid, at_ns=item["reaped_ns"]))
    return code is not None


def supervise(directory, parent_fd):
    directory = Path(directory)
    parent = socket.socket(fileno=parent_fd)
    parent.setblocking(False)
    # The parent has already installed its pidfd cutoff before this gate.
    raw, _ = receive(parent, now() + 100_000_000)
    job = json.loads(raw)
    client = Client.current()
    handles, channels, cache = {}, {}, {}
    extra_fds = {}
    events, sent = [], set()
    trigger, error = None, None

    def term_handler(signum, frame):
        nonlocal trigger
        trigger = shrink(trigger, dict(reason="operational_deadline" if now() >= job["operational_ns"] else "supervisor_term", at_ns=now()), job["hard_ns"])

    signal.signal(signal.SIGTERM, term_handler)
    become_subreaper()

    def barrier(name, item=None):
        notify(parent, dict(op="barrier", name=name, item=item),
               job["operational_ns"])

    def launch(role):
        if now() >= job["operational_ns"]:
            raise RuntimeError("launch_after_deadline")
        intent = client.rpc("intent", role=role, attempt=job["attempt"])
        barrier(role + "_before_spawn")
        auth = client.rpc("channel", role=role, attempt=job["attempt"])["fd"]
        try:
            here, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        except BaseException:
            os.close(auth)
            raise
        here.setblocking(False)
        channels[role] = here
        try:
            output = (directory / (role + ".log")).open("xb", buffering=0)
        except BaseException:
            here.close()
            child.close()
            os.close(auth)
            raise
        environment = os.environ.copy()
        environment["RUN_SNAPSHOT_AUTH_FD"] = str(auth)
        proc, fd = None, None
        begin = now()
        try:
            proc = subprocess.Popen(
                [sys.executable, SCRIPT, "role", role, str(directory),
                 str(child.fileno()), json.dumps(job)],
                stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                start_new_session=True, pass_fds=(auth, child.fileno(),
                                                *([job["owner_fd"]] if "owner_fd" in job else [])),
                env=environment, cwd=job["cwd"],
            )
            handles[role] = [proc, fd, output]
            fd = pidfd_open(proc.pid)
            handles[role][1] = fd
            item = dict(identity(proc.pid), role=role, birth_lower=begin,
                        spawn_before_ns=begin, spawn_after_ns=now(),
                        attempt=job["attempt"])
            cache[generation(item)] = item
            channels[role] = here
            # Owner sees the real post-Popen identity before production ACK.
            notify(parent, dict(op="identity", item=item), job["operational_ns"])
            barrier(role + "_spawned", item)
            client.rpc("identity", item=item, intent=intent["id"])
            if role == "W":
                await_ancestors(client, item, job["operational_ns"])
                barrier("W_before_ready", item)
                send(here, b"R", deadline=job["operational_ns"])
                raw, _ = receive(here, min(now() + 100_000_000,
                                          job["operational_ns"]))
                if not json.loads(raw).get("ready"):
                    raise RuntimeError("watchdog_not_ready")
                barrier("W_ready", item)
                events.append(dict(kind="watchdog_ready", at_ns=now(), pid=item["pid"]))
            else:
                notify(channels["W"], dict(op="identity", item=item),
                       job["operational_ns"], fd)
                client.rpc("controller_ack", pid=item["pid"], ticks=item["ticks"], ancestor=job["attempt"])
                await_ancestors(client, item, job["operational_ns"])
                barrier(role + "_registered", item)
                if role == "worker" and job.get("fault") == "registration":
                    while now() < begin + 200_000_000:
                        if now() >= job["operational_ns"]:
                            raise RuntimeError("registration_fault_deadline")
                        time.sleep(0.005)
                    fault_observed(job, "worker_registered_gate_held", item=item)
                    raise RuntimeError("injected_registration_failure")
                if client.rpc("status")["stop"] or now() >= job["operational_ns"]:
                    raise RuntimeError("release_refused")
                send(here, b"G", deadline=job["operational_ns"])
                events.append(dict(kind="released", role=role, pid=proc.pid,
                                   at_ns=now()))
        finally:
            child.close()
            os.close(auth)
            if proc is None:
                output.close()
                here.close()
        return item

    try:
        barrier("S_intent")
        if job.get("fault") == "supervisor_hang":
            fault_observed(job, "S_intent_before_launch")
            while now() < job["hard_ns"]:
                time.sleep(0.005)
        launch("W")
        launch("C")
        launch("worker")
        finished = False
        active_barrier = False
        business_start = now()
        while trigger is None:
            tick = now()
            if tick >= job["operational_ns"]:
                fault_observed(job, "operational_deadline", O=job["operational_ns"], observed_ns=tick)
                trigger = shrink(trigger, dict(reason="operational_deadline",
                                              at_ns=tick), job["hard_ns"])
                break
            if not active_barrier and tick >= business_start + 200_000_000:
                barrier("business_active")
                active_barrier = True
                if job.get("fault") == "record_truncated":
                    fault_observed(job, "S_registry_return_truncated")
                    send(parent, b"{", deadline=job["operational_ns"])
                elif job.get("fault") == "identity_capacity":
                    fault_observed(job, "S_registry_return_capacity")
                    send(parent, dict(op="registry_snapshot", entries=list(cache.values()) * 22), deadline=job["operational_ns"])
                if job.get("coordinator_mode") == "early_exit":
                    send(channels["C"], b"E", deadline=job["operational_ns"])
            state = client.rpc("status")
            expected_stop = state.get("expected_triggers", {}).get(job["attempt"])
            if expected_stop:
                trigger = shrink(trigger, expected_stop, job["hard_ns"])
                break
            if state["stop"]:
                trigger = shrink(trigger, state["stop"], job["hard_ns"])
                break
            if select.select([parent], [], [], 0)[0]:
                raw, _ = receive(parent, now())
                message = json.loads(raw)
                if message["op"] == "stop":
                    trigger = shrink(trigger, message["trigger"], job["hard_ns"])
                    break
            for item in state["identities"]:
                if job["attempt"] not in item.get("ancestor_nodes", []) or item["pid"] == os.getpid():
                    continue
                if job.get("fault") == "omit_descendant":
                    continue
                if generation(item) not in cache:
                    fd = pidfd_open(item["pid"])
                    cache[generation(item)] = item
                    extra_fds[generation(item)] = fd
                    notify(channels["W"], dict(op="identity", item=item),
                           job["operational_ns"], fd)
                    notify(parent, dict(op="identity", item=item),
                           job["operational_ns"])
                    if job["attempt"] in item.get("controller_nodes", []) and job["attempt"] not in item.get("controller_acknowledged", []):
                        client.rpc("controller_ack", pid=item["pid"], ticks=item["ticks"], ancestor=job["attempt"])
            for key, fd in list(extra_fds.items()):
                value = cache[key]
                if select.select([fd], [], [], 0)[0] and not value.get("first_exit_observed_ns"):
                    value["first_exit_observed_ns"] = now()
                    client.rpc("exited", item=value, at_ns=value["first_exit_observed_ns"])
                    notify(parent, dict(op="exited", item=value), job["hard_ns"])
            for role in ("W", "worker", "C"):
                proc = handles[role][0]
                item = next(i for i in cache.values() if i["pid"] == proc.pid)
                ended = observe_handle(proc, item, client, parent, job, events)
                if not ended:
                    continue
                if role == "W":
                    raise RuntimeError("watchdog_lost")
                if role == "worker" and not finished:
                    if proc.returncode:
                        fault_observed(job, "worker_return", returncode=proc.returncode,
                                       item=item)
                        raise RuntimeError("worker_failed")
                    if any(not select.select([fd], [], [], 0)[0] for fd in extra_fds.values()):
                        fault_observed(job, "leader_exit_with_live_registered_descendant", item=item)
                        raise RuntimeError("descendant_tail")
                    finished = True
                    send(channels["C"], b"F", deadline=job["operational_ns"])
                if role == "C":
                    if not finished or proc.returncode:
                        fault_observed(job, "coordinator_return", returncode=proc.returncode,
                                       item=item)
                        raise RuntimeError("coordinator_early_exit")
                    trigger = shrink(trigger, dict(reason="completed", at_ns=tick),
                                     job["hard_ns"])
            time.sleep(0.005)
    except Exception as exc:
        error = str(exc)
        if isinstance(exc, ControlStop):
            trigger = shrink(trigger, exc.trigger, job["hard_ns"])
            events.append(dict(kind="ancestor_stop_during_ack", at_ns=now(),
                               exception=str(exc), original=exc.trigger))
            error = exc.trigger["reason"]
        if (str(exc) in {"channel_eof_or_capacity", "channel_receive_timeout",
                         "channel_send_timeout", "channel_ack", "watchdog_lost"}
                and "W" in handles and handles["W"][1] is not None
                and select.select([handles["W"][1]], [], [], 0)[0]):
            events.append(dict(kind="watchdog_exit_cause", at_ns=now(), exception=str(exc)))
            error = "watchdog_lost"
        trigger = shrink(trigger, dict(reason=error, at_ns=now()), job["hard_ns"])
    finally:
        trigger = trigger or shrink(None, dict(reason="supervisor_failure",
                                              at_ns=now()), job["hard_ns"])
        if trigger["reason"] != "completed":
            try:
                client.rpc("child_trigger", attempt=job["attempt"], reason=trigger["reason"], at_ns=trigger["at_ns"])
            except Exception:
                error = error or "root_trigger_notification_failed"
        try:
            notify(parent, dict(op="barrier", name="cleanup_started", item=None,
                                trigger=trigger), job["hard_ns"])
        except ControlStop as exc:
            trigger = shrink(trigger, exc.trigger, job["hard_ns"])
        except Exception:
            pass
        if "W" in channels:
            try:
                send(channels["W"], dict(op="complete" if trigger["reason"] ==
                                        "completed" else "stop", trigger=trigger))
            except Exception:
                pass
        last_sent_end = None
        while now() < trigger["cleanup_deadline_ns"]:
            if "W" in channels and last_sent_end != trigger["cleanup_deadline_ns"]:
                try:
                    send(channels["W"], dict(op="stop", trigger=trigger))
                    last_sent_end = trigger["cleanup_deadline_ns"]
                except Exception:
                    pass
            active = False
            for role, (proc, fd, _) in list(handles.items()):
                item = next((i for i in cache.values() if i["pid"] == proc.pid), None)
                if item is None:
                    if proc.returncode is None:
                        channels.get(role, parent).close()
                        active = True
                        if fd is not None:
                            pidfd_signal(fd, signal.SIGTERM)
                        try:
                            proc.wait(timeout=0)
                        except subprocess.TimeoutExpired:
                            pass
                    error = error or "partial_spawn_identity_unknown"
                    continue
                try:
                    ended = observe_handle(proc, item, client, parent, job, events)
                except Exception as exc:
                    if isinstance(exc, ControlStop):
                        trigger = shrink(trigger, exc.trigger, job["hard_ns"])
                    error = error or str(exc)
                    ended = proc.poll() is not None
                    if ended:
                        item.setdefault("reaped_ns", now())
                        item.update(reaped=True, exit_code=proc.returncode)
                if not ended:
                    active = True
                    if role != "W":
                        sig = signal.SIGKILL if now() >= min(
                            trigger["at_ns"] + 150_000_000,
                            trigger["cleanup_deadline_ns"] - NS,
                        ) else signal.SIGTERM
                        signal_fixed(item, fd, sig, events, sent)
            for key, fd in list(extra_fds.items()):
                value = cache[key]
                if select.select([fd], [], [], 0)[0]:
                    if not value.get("first_exit_observed_ns"):
                        value["first_exit_observed_ns"] = now()
                        try:
                            client.rpc("exited", item=value, at_ns=value["first_exit_observed_ns"])
                            notify(parent, dict(op="exited", item=value), job["hard_ns"])
                        except Exception as exc:
                            error = error or str(exc)
                else:
                    active = True
                    sig = signal.SIGKILL if now() >= min(trigger["at_ns"] + 150_000_000, trigger["cleanup_deadline_ns"] - NS) else signal.SIGTERM
                    signal_fixed(value, fd, sig, events, sent)
            if not active:
                break
            time.sleep(0.005)
        for item in cache.values():
            item.update(group_scan_ns=now(), group_live=members(item))
        descendants = [i for i in cache.values() if i.get("inferred")]
        for item in descendants:
            try:
                pid, status = os.waitpid(item["pid"], os.WNOHANG)
            except ChildProcessError:
                pid = 0
            if pid:
                item.update(reaped=True, reaped_ns=now(), wait_status=status)
        error = error or (trigger["reason"] if trigger["reason"] != "completed" else None)
        tail = all(proc.poll() is not None for proc, _, _ in handles.values())
        tail = tail and all(not i["group_live"] for i in cache.values())
        result = dict(
            success=error is None and trigger["reason"] == "completed" and tail,
            error=error, trigger=trigger, entries=[i for i in cache.values() if any(i["pid"] == proc.pid for proc, _, _ in handles.values())], events=events,
            tail_known=tail, cleanup_deadline_ns=trigger["cleanup_deadline_ns"],
            observed_descendants=descendants, end_ns=now(),
            cleanup_within_deadline=now() <= trigger["cleanup_deadline_ns"] and tail,
            physical_tail_known=tail,
            evidence_complete=all(value.get("first_exit_observed_ns")
                                  and value.get("reaped_ns")
                                  for value in cache.values()),
        )
        result["success"] = bool(result["success"] and result["cleanup_within_deadline"]
                                 and result["evidence_complete"])
        barrier("result_before") if now() < job["operational_ns"] else None
        if job.get("fault") == "persist_fail":
            fault_observed(job, "S_result_write_failure")
            raise OSError("EXPECTED_PERSISTENCE_FAILURE")
        if job.get("fault") == "persist_hang":
            fault_observed(job, "S_result_write_block")
            while now() < job["hard_ns"]:
                time.sleep(0.005)
        save(directory / "result.json", result)
        try:
            count = sum(1 for value in cache.values() if value["attempt"] == job["attempt"]) + (1 if job.get("fault") == "outer_unknown" else 0)
            notify(parent, dict(op="result_written", final_registry_count=count), job["hard_ns"])
        except Exception:
            pass
        for fd in extra_fds.values():
            os.close(fd)
        for role, (_, fd, output) in handles.items():
            output.close()
            if fd is not None:
                os.close(fd)
            if role in channels:
                channels[role].close()
    return result


def run_owned(directory, job):
    directory = Path(directory)
    if (directory / "job.json").exists() or (directory / "attempt.json").exists():
        raise RuntimeError("entry_already_attempted")
    ticket = admit(directory, job)
    client, header = Client.current(), Client.current().header
    job = dict(job, attempt=ticket["id"], fault_nonce=ticket["step"]["fault_nonce"])
    directory.mkdir(parents=True, exist_ok=True)
    save(directory / "job.json", job)
    save(directory / "attempt.json", dict(ticket=ticket["id"], closed=False))
    become_subreaper()
    cache, events, sent = {}, [], set()
    trigger, complete = [None], threading.Event()
    monitor, proc, descriptor, guard = None, None, None, None
    here, child, output = None, None, None
    auth = None
    outer_error, result = None, None
    reported_trigger = False
    owner = _OWNER
    stop_lock = threading.Lock()
    if owner is not None:
        owner.configure(job)

    def stop(reason, at_ns=None, ancestor_end_ns=None):
        value = dict(reason=reason, at_ns=at_ns if at_ns is not None else now())
        if ancestor_end_ns is not None:
            value["ancestor_end_ns"] = ancestor_end_ns
        with stop_lock:
            trigger[0] = shrink(trigger[0], value, job["hard_ns"])
            current = dict(trigger[0])
        if owner is not None:
            owner.tighten(current)

    def discovered(item):
        if generation(item) not in cache:
            fd = pidfd_open(item["pid"])
            cache[generation(item)] = (dict(item), fd)
            if owner is not None:
                owner.register(item)

    def cutoff():
        while not complete.is_set() and now() < job["hard_ns"]:
            tick = now()
            if tick >= job["operational_ns"]:
                stop("operational_deadline", tick)
            if trigger[0] is not None:
                end = trigger[0]["cleanup_deadline_ns"]
                for item, fd in list(cache.values()):
                    if select.select([fd], [], [], 0)[0]:
                        item.setdefault("first_exit_observed_ns", tick)
                        item.setdefault("first_exit_source", "startup_cutoff_pidfd")
                    else:
                        sig = signal.SIGKILL if tick >= min(
                            trigger[0]["at_ns"] + 150_000_000, end - NS
                        ) else signal.SIGTERM
                        try:
                            signal_fixed(item, fd, sig, events, sent)
                        except Exception as exc:
                            stop(str(exc))
            time.sleep(0.005)

    try:
        intent = client.rpc("intent", role="S", attempt=ticket["id"])
        auth = client.rpc("channel", role="S", attempt=ticket["id"])["fd"]
        here, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        here.setblocking(False)
        output = (directory / "startup.log").open("xb", buffering=0)
        environment = os.environ.copy()
        environment["RUN_SNAPSHOT_AUTH_FD"] = str(auth)
        birth = now()
        try:
            proc = subprocess.Popen(
                [sys.executable, SCRIPT, "supervise", str(directory),
                 str(child.fileno())],
                stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
                stdin=subprocess.DEVNULL, pass_fds=(auth, child.fileno(),
                                                     *([job["owner_fd"]] if "owner_fd" in job else [])),
                env=environment, cwd=job["cwd"],
            )
        finally:
            child.close()
            os.close(auth)
            auth = None
        # Every operation after Popen is inside the same cleanup protection.
        descriptor = pidfd_open(proc.pid)
        item = dict(identity(proc.pid), role="S", birth_lower=birth,
                    spawn_before_ns=birth, spawn_after_ns=now(),
                    attempt=ticket["id"])
        cache[generation(item)] = (item, descriptor)
        guard = threading.Thread(target=cutoff, name="startup-cutoff")
        guard.start()
        if owner is not None:
            owner.register(item)
        client.rpc("identity", item=item, intent=intent["id"])
        monitor = RealMonitor(directory, header, stop, attempt=ticket["id"],
                              discovered=discovered, view=job.get("view"),
                              fault_nonce=job["fault_nonce"])
        monitor.register(item)
        client.rpc("ancestor_ack", pid=item["pid"], ticks=item["ticks"],
                   ancestor=ticket["id"])
        await_ancestors(client, item, job["operational_ns"])
        if now() >= job["operational_ns"] or client.rpc("status")["stop"]:
            raise RuntimeError("release_refused")
        send(here, job, deadline=job["operational_ns"])
        while now() < job["hard_ns"]:
            tick = now()
            if select.select([here], [], [], 0.005)[0]:
                try:
                    raw, _ = receive(here, now())
                except RuntimeError:
                    raw = None
                if raw:
                    message = json.loads(raw)
                    if message["op"] == "identity":
                        value = message["item"]
                        discovered(value)
                        monitor.register(value)
                        client.rpc("owner_identity", item=value, ancestor=ticket["id"])
                        send(here, b"A", deadline=job["hard_ns"])
                    elif message["op"] == "exited":
                        value = message["item"]
                        monitor.observe_exit(value, value["first_exit_observed_ns"],
                                             "S_notice")
                        for entry, _ in cache.values():
                            if generation(entry) == generation(value):
                                entry.setdefault("first_exit_observed_ns",
                                                 value["first_exit_observed_ns"])
                        send(here, b"A", deadline=job["hard_ns"])
                    elif message["op"] == "barrier":
                        if message["name"] == "cleanup_started":
                            value = message["trigger"]
                            stop(value["reason"], value["at_ns"], value["cleanup_deadline_ns"])
                        if message["name"] == "cleanup_started" and monitor.view is not None:
                            monitor.view.phase_ready = True
                        if owner is not None:
                            owner.barrier(message["name"], message.get("item"))
                            if owner.fired:
                                fault = owner.context["spec"]["steps"][0]
                                reason = fault["expected"]
                                stop(reason[0] if isinstance(reason, list) else reason,
                                     owner.trigger_ns, owner.cleanup_end_ns)
                        killed_sender = bool(owner is not None and owner.fired
                            and owner.context["spec"].get("owner_fault", {}).get("role") == "S"
                            and owner.context["spec"]["owner_fault"]["barrier"] == message["name"])
                        if not killed_sender:
                            send(here, b"A", deadline=job["hard_ns"])
                        if message["name"] == "business_active" and job.get("fault") == "ancestor_late":
                            fault_observed(job, "ancestor_stop_before_late_delivery")
                            detected = now()
                            stop("ancestor_stop", detected, detected + 1200_000_000)
                            time.sleep(0.005)
                            send(here, dict(op="stop", trigger=trigger[0]), deadline=job["hard_ns"])
                        if message["name"] == "business_active" and job.get("fault") == "startup_block":
                            fault_observed(job, "startup_business_barrier_block")
                            time.sleep(max(0, (job["operational_ns"] + 250_000_000 - now()) / NS))
                    elif message["op"] == "registry_snapshot":
                        if len(message["entries"]) > 64:
                            raise RuntimeError("outer_tail_unknown")
                    elif message["op"] == "result_written":
                        if message.get("final_registry_count") != sum(1 for value, _ in cache.values() if value["attempt"] == ticket["id"] and value["role"] != "S"):
                            outer_error = "outer_tail_unknown"
                            stop(outer_error)
                        if owner is not None:
                            owner.barrier("result_after", item)
                        send(here, b"A", deadline=job["hard_ns"])
            state = client.rpc("status")
            expected_stop = state.get("expected_triggers", {}).get(ticket["id"])
            if expected_stop:
                stop(expected_stop["reason"], expected_stop["at_ns"],
                     expected_stop.get("cleanup_deadline_ns"))
            if state["stop"]:
                value = state["stop"]
                stop(value["reason"], value["at_ns"], value.get("ancestor_end_ns"))
            for value in state["identities"]:
                if ticket["id"] in value.get("ancestor_nodes", []):
                    if value.get("inferred") and job.get("fault") == "omit_descendant":
                        continue
                    discovered(value)
                    monitor.register(value)
                    if ticket["id"] not in value.get("acknowledged", []):
                        client.rpc("ancestor_ack", pid=value["pid"], ticks=value["ticks"],
                                   ancestor=ticket["id"])
            if trigger[0] and not reported_trigger and trigger[0]["reason"] != "completed":
                client.rpc("child_trigger", attempt=ticket["id"], reason=trigger[0]["reason"], at_ns=trigger[0]["at_ns"])
                reported_trigger = True
            if trigger[0]:
                try:
                    send(here, dict(op="stop", trigger=trigger[0]))
                except Exception:
                    pass
            if select.select([descriptor], [], [], 0)[0]:
                item.setdefault("first_exit_observed_ns", now())
                client.rpc("exited", item=item, at_ns=item["first_exit_observed_ns"])
                monitor.observe_exit(item, item["first_exit_observed_ns"], "pidfd")
                proc.poll()
                item.setdefault("reaped_ns", now())
                item.update(reaped=True, exit_code=proc.returncode)
                if trigger[0] and trigger[0]["reason"] == "missing_supervisor_result":
                    outer_error = "missing_supervisor_result"
                if not (directory / "result.json").exists():
                    outer_error = "missing_supervisor_result"
                    stop(outer_error)
                else:
                    result = read(directory / "result.json")
                    value = result["trigger"]
                    stop(value["reason"], value["at_ns"], value["cleanup_deadline_ns"])
                break
        if proc.poll() is None:
            stop("supervisor_hard_tail")
    except Exception as exc:
        outer_error = "outer_tail_unknown" if isinstance(exc, json.JSONDecodeError) else str(exc)
        stop(outer_error)
        try:
            client.rpc("child_trigger", attempt=ticket["id"], reason=outer_error, at_ns=now())
        except Exception:
            outer_error += ":trigger_notification_failed"
    finally:
        # Keep the cutoff and sampler running until every fixed pidfd is exited.
        end = trigger[0]["cleanup_deadline_ns"] if trigger[0] else job["hard_ns"] - 10 * NS
        while now() < end:
            live = [(i, fd) for i, fd in cache.values()
                    if not select.select([fd], [], [], 0)[0]]
            if not live:
                break
            if trigger[0] is None:
                stop("startup_finally")
            time.sleep(0.005)
        if proc is not None:
            if descriptor is not None and not select.select([descriptor], [], [], 0)[0]:
                pidfd_signal(descriptor, signal.SIGKILL)
            try:
                proc.wait(timeout=max(0, (end - now()) / NS))
            except subprocess.TimeoutExpired:
                outer_error = outer_error or "supervisor_unreaped_tail"
        sampling_valid = monitor.finish(end) if monitor else False
        if all(select.select([fd], [], [], 0)[0] for _, fd in cache.values()) and (proc is None or proc.returncode is not None):
            complete.set()
        if guard is not None:
            guard.join(max(0, (job["hard_ns"] - now()) / NS))
        if monitor is not None and not monitor.thread.is_alive():
            monitor.close_descriptors()
        actual, outer_known = [], guard is not None and not guard.is_alive()
        for value, fd in list(cache.values()):
            exited = bool(select.select([fd], [], [], 0)[0])
            value.setdefault("first_exit_observed_ns", now() if exited else None)
            if value["pid"] != (proc.pid if proc else None):
                try:
                    pid, status = os.waitpid(value["pid"], os.WNOHANG)
                    if pid:
                        value.update(reaped_ns=now(), wait_status=status)
                except ChildProcessError:
                    pass
            group = members(value)
            value.update(exited=exited, group_scan_ns=now(), group_live=group)
            actual.append(value)
            outer_known = outer_known and exited and not group
            if guard is None or not guard.is_alive():
                os.close(fd)
        owner_result = owner.checkpoint(job["hard_ns"]) if owner is not None else None
        owner_complete = owner_result is not None and owner_result["complete"]
        if job.get("collection"):
            owner_complete = True
        inner = result or dict(success=False, error=outer_error, tail_known=False,
                               entries=[], observed_descendants=[])
        physical_outer_cache = outer_known
        outer_known = outer_known and all(
            i.get("resolved") for i in client.rpc("status")["intents"]
            if i["attempt"] == ticket["id"]
        )
        parent_sampling_valid = sampling_valid
        view = monitor.view if monitor else None
        if view is not None:
            sampling_valid = sampling_valid and view.finished and not view.invalid
            if view.error:
                inner["error"] = view.error
            inner["success"] = inner["success"] and sampling_valid
        physical_outer = physical_outer_cache and owner_complete
        outer_known = outer_known and outer_error is None
        tail = bool(inner["tail_known"] and outer_known and parent_sampling_valid)
        cleanup_ok = bool(physical_outer and now() <= end
                          and inner.get("cleanup_within_deadline", True))
        result = dict(
            inner, startup=dict(tail_known=bool(outer_known), identities=actual,
                                events=events, end_ns=now()),
            tail_known=tail, sampling_valid=sampling_valid,
            parent_sampling_valid=parent_sampling_valid,
            cleanup_complete=bool(outer_known), cleanup_within_deadline=cleanup_ok,
            physical_tail_known=bool(outer_known and inner["tail_known"]),
            measurement_valid=sampling_valid, evidence_complete=bool(monitor is not None
                and not monitor.thread.is_alive() and all(i.get("first_exit_observed_ns")
                and i.get("group_scan_ns") for i in actual)), owner_complete=owner_complete,
            independent_owner=owner_result, hard_ns=job["hard_ns"],
            operational_ns=job["operational_ns"], hard_breached=now() > job["hard_ns"],
            end_ns=now(), diagnostic_valid=None,
        )
        result["success"] = bool(inner["success"] and tail and owner_complete and cleanup_ok
                                 and result["evidence_complete"]
                                 and not result["hard_breached"])
        summary = client.rpc("diagnostic_summary", attempt=ticket["id"])
        result["diagnostic_valid"] = summary["valid"] if summary["applicable"] else None
        result["diagnostic_sources"] = summary["ids"]
        result["control_outcome"] = "completed" if result["success"] else "rejected"
        if outer_error:
            result["error"] = outer_error
        if job.get("fault") == "omit_descendant":
            fault_observed(job, "omit_descendant_return")
            result["tail_known"] = False
            result["success"] = False
            result["error"] = "descendant_record_unknown"
        if trigger[0]:
            result["startup_trigger"] = trigger[0]
        save(directory / "collection-result.json", result)
        save(directory / "attempt.json", dict(ticket=ticket["id"],
                                              closed=tail, end_ns=now()))
        client.rpc("close", id=ticket["id"], result=result)
        if output is not None:
            output.close()
        if here is not None:
            here.close()
        if child is not None:
            child.close()
        if auth is not None:
            os.close(auth)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["supervise", "role"])
    parser.add_argument("arguments", nargs="+")
    args = parser.parse_args()
    if args.action == "supervise":
        supervise(Path(args.arguments[0]), int(args.arguments[1]))
        Client.current().rpc("channel_closing")
    else:
        raise SystemExit(role_main(args.arguments[0], args.arguments[1],
                                   int(args.arguments[2]), json.loads(args.arguments[3])))
