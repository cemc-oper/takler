"""Single-authority, G0-only repair runner and first-failure pytest plugin."""

import argparse
import array
import fcntl
import hashlib
import json
import os
from pathlib import Path
import queue
import selectors
import socket
import struct
import threading
import time
import sys


def protected_project(root, descriptor, fixture=None):
    """Install the fatal boundary before importing project dependencies."""
    import runpy
    import select
    import traceback

    channel = socket.socket(fileno=descriptor)
    channel.setblocking(False)
    try:
        transition = json.loads((root / "validation-begin.json").read_text())
        end = transition["v"] + 5_000_000_000
        if fixture is not None:
            end = fixture["operational_ns"]
        if not select.select([channel], [], [], max(0, (end - time.monotonic_ns()) / 1e9))[0]:
            raise RuntimeError("outer_protection_not_ready")
        if channel.recv(65536) != b"G":
            raise RuntimeError("outer_protection_refused")
        if fixture is not None:
            mode = fixture["mode"]
            if mode == "entry_fatal":
                raise RuntimeError("injected_before_project_import")
            if mode == "early_exit":
                return 17
            if mode == "ready_timeout":
                while time.monotonic_ns() < fixture["hard_ns"]:
                    time.sleep(0.005)
                return 18
            if mode == "normal":
                channel.send(json.dumps(dict(event="ready", at_ns=time.monotonic_ns())).encode())
                channel.send(json.dumps(dict(event="project_done", at_ns=time.monotonic_ns(), success=True)).encode())
                return 0
            raise RuntimeError("fixture_mode_not_frozen")
        namespace = runpy.run_path(__file__, run_name="g0_protected_execution")
        outcome = namespace["execute"](root, channel)
        channel.send(json.dumps(dict(event="project_done", at_ns=time.monotonic_ns(),
                                     success=outcome)).encode())
        return 0 if outcome else 1
    except BaseException as exc:
        failure = dict(event="fatal", at_ns=time.monotonic_ns(),
                       phase="project_boundary", error=type(exc).__name__ + ":" + str(exc))
        try:
            channel.send(json.dumps(failure).encode())
        except OSError:
            pass
        traceback.print_exc()
        return 1
    finally:
        channel.close()


def outer_entry(root, anchor, fixture=None):
    """Replace timeout at the host position, without business authority."""
    import select
    import signal
    import subprocess
    import ctypes

    library = None

    def outer_pidfd_open(pid, flags=0):
        result = library.pidfd_open(pid, flags)
        if result < 0:
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code))
        return result

    def outer_pidfd_signal(descriptor, signum):
        if library.pidfd_send_signal(descriptor, signum, None, 0) < 0:
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code))


    def tick():
        return time.clock_gettime_ns(time.CLOCK_MONOTONIC)

    def kernel(pid):
        fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
        return dict(pid=pid, ticks=int(fields[19]), ppid=int(fields[1]),
                    pgid=int(fields[2]), session=int(fields[3]), state=fields[0])

    transition = json.loads((root / "validation-begin.json").read_text())
    v, h = transition["v"], transition["v"] + 30_000_000_000
    parent_client = None
    directory = root
    if fixture is not None:
        # This mode is the already protected test worker, not another root.
        # control imports stdlib only; the real host mode never imports it.
        from run_snapshot_control import Client
        parent_client = Client.current()
        context = parent_client.rpc("case_context")
        specification = context["spec"]["outer_fixture"]
        if specification["mode"] != fixture:
            raise RuntimeError("outer_fixture_not_frozen")
        fixture = dict(specification, start_ns=context["start_ns"],
                       operational_ns=min(context["start_ns"]+2_000_000_000,
                                          context["operational_ns"]),
                       hard_ns=context["hard_ns"])
        v, h = context["start_ns"], context["hard_ns"]
        directory = Path(context["spec"]["directory"]) / "outer-fixture"
        directory.mkdir()
        anchor = parent_client.header["manifest_hash"]
    state = dict(stop=None, ready=False, done=False, H=h, O=v + 5_000_000_000)
    if fixture is not None:
        state["O"] = fixture["operational_ns"]
    lock, finished = threading.Lock(), threading.Event()
    entries, descriptors, events = {}, {}, []
    sample_done = threading.Event()
    sampler = None
    sampling = dict(valid=True, error=None, first={}, last={}, counts={}, exits={})
    output, proc, here, child = None, None, None, None
    gate_parent = gate_child = None
    registered = None
    guard = None

    def stop(reason, original=None, ancestor=None):
        at = tick() if original is None else original
        with lock:
            end = min(at + 20_000_000_000, state["H"] - 10_000_000_000,
                      ancestor if ancestor is not None else state["H"])
            if state["stop"] is None:
                state["stop"] = dict(reason=reason, at_ns=at, C_end=end,
                                     first_reason_at_ns=at)
            else:
                state["stop"]["C_end"] = min(state["stop"]["C_end"], end)
                state["stop"]["at_ns"] = min(state["stop"]["at_ns"], at)
            events.append(dict(event="outer_stop", reason=reason, at_ns=tick(),
                               original_ns=at, C_end=state["stop"]["C_end"]))

    def cutoff():
        try:
            while not finished.is_set() and tick() < state["H"]:
                at = tick()
                if not state["ready"] and at >= state["O"]:
                    stop("initialization_operational_deadline", state["O"])
                if at >= state["H"]:
                    stop("outer_hard_deadline", state["H"])
                trigger = state["stop"]
                for key, fd in list(descriptors.items()):
                    if select.select([fd], [], [], 0)[0]:
                        entries[key].setdefault("first_exit_observed_ns", at)
                    elif trigger is not None:
                        sig = signal.SIGKILL if at >= trigger["C_end"] - 1_000_000_000 else signal.SIGTERM
                        mark = (key, int(sig))
                        if mark not in state.setdefault("signals", set()):
                            try:
                                outer_pidfd_signal(fd, sig)
                            except ProcessLookupError:
                                pass
                            else:
                                events.append(dict(event="outer_signal", key=key,
                                                   signal=int(sig), at_ns=tick()))
                            state["signals"].add(mark)
                finished.wait(0.005)
        except BaseException as exc:
            stop("outer_cutoff_failure:" + type(exc).__name__)

    def resource_loop():
        import shutil
        own = kernel(os.getpid())
        own.update(role="outer", spawn_before_ns=tick(), observation_only=True)
        own_key = str(own["pid"]) + ":" + str(own["ticks"])
        swap_start = json.loads((root / "bootstrap-adoption-audit.json").read_text())["swap_start"]
        over = 0
        try:
            with (directory / "outer-real-resources.jsonl").open("x", buffering=1) as stream:
                while not sample_done.is_set():
                    began = tick()
                    active = {key: entries[key] for key in list(descriptors)}
                    active[own_key] = own
                    rows, total = [], 0
                    for key, value in active.items():
                        if key in sampling["exits"]:
                            continue
                        if key != own_key and select.select([descriptors[key]], [], [], 0)[0]:
                            observed = tick()
                            sampling["exits"][key] = observed
                            previous = sampling["last"].get(key)
                            if previous is None or observed - previous[0] > 250_000_000:
                                raise RuntimeError("outer_exit_sample_gap")
                            if value["role"] == "worker" and sampling["counts"][key] < 2:
                                raise RuntimeError("outer_worker_cpu_points")
                            continue
                        begin = tick()
                        first = kernel(value["pid"])
                        fields = Path(f"/proc/{value['pid']}/stat").read_text().rpartition(")")[2].split()
                        rss = int(Path(f"/proc/{value['pid']}/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
                        last = kernel(value["pid"])
                        end = tick()
                        cpu = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
                        if first["ticks"] != value["ticks"] or last["ticks"] != value["ticks"]:
                            raise RuntimeError("outer_sample_identity")
                        if min(cpu, rss) < 0 or end - begin > 250_000_000:
                            raise RuntimeError("outer_sample_read_or_negative")
                        previous = sampling["last"].get(key)
                        birth = value.get("birth_lower", value["spawn_before_ns"])
                        if previous is None and end - birth > 250_000_000:
                            raise RuntimeError("outer_first_sample_gap")
                        if previous and (end - previous[0] > 250_000_000 or cpu < previous[2]):
                            raise RuntimeError("outer_gap_or_cpu_regression")
                        if value["role"] in {"S", "C", "W"} and rss > 512 * 1024**2:
                            raise RuntimeError("outer_tool_rss")
                        sampling["first"].setdefault(key, [begin, end, cpu])
                        sampling["last"][key] = [begin, end, cpu]
                        sampling["counts"][key] = sampling["counts"].get(key, 0) + 1
                        total += rss
                        rows.append(dict(item=value, identity_before=first, identity_after=last,
                                         cpu=cpu, rss=rss, read_begin_ns=begin, read_end_ns=end))
                    memory = {line.split()[0]: int(line.split()[1])*1024
                              for line in Path("/proc/meminfo").read_text().splitlines()
                              if line.startswith(("MemAvailable:", "SwapTotal:", "SwapFree:"))}
                    disk = shutil.disk_usage(root).free
                    used = memory["SwapTotal:"] - memory["SwapFree:"]
                    over = over + 1 if total > 8 * 1024**3 else 0
                    if memory["MemAvailable:"] < 4 * 1024**3 or disk < 20 * 1024**3 or used-swap_start > 1024**3 or over >= 2:
                        raise RuntimeError("outer_resource_stop")
                    stream.write(json.dumps(dict(at_ns=tick(), processes=rows, total_rss=total,
                                                  memory=memory, disk_free=disk,
                                                  swap_start=swap_start, pss_enabled=False))+"\n")
                    sample_done.wait(max(0, (began+100_000_000-tick())/1e9))
                if tick()-sampling["last"][own_key][0] > 250_000_000:
                    raise RuntimeError("outer_observation_end_gap")
                for key, value in entries.items():
                    if key in sampling["exits"]:
                        continue
                    if not select.select([descriptors[key]], [], [], 0)[0]:
                        raise RuntimeError("outer_unfinalized_identity")
                    observed = value.get("first_exit_observed_ns", tick())
                    previous = sampling["last"].get(key)
                    sampling["exits"][key] = observed
                    if previous is None or observed-previous[0] > 250_000_000:
                        raise RuntimeError("outer_final_exit_gap")
                    if value["role"] == "worker" and sampling["counts"][key] < 2:
                        raise RuntimeError("outer_worker_cpu_points")
                stream.write(json.dumps(dict(event="outer_sampling_finalized", at_ns=tick(),
                                              exits=sampling["exits"], valid=True))+"\n")
        except BaseException as exc:
            sampling.update(valid=False, error=type(exc).__name__+":"+str(exc))
            stop("outer_real_sampling:"+sampling["error"])

    try:
        if root != root.resolve() or transition["manifest_sha256"] != anchor:
            raise RuntimeError("outer_anchor")
        if hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest() != anchor:
            raise RuntimeError("outer_manifest_changed")
        if (root / "STOP.json").exists():
            raise RuntimeError("outer_root_stopped")
        manifest = json.loads((root / "manifest.json").read_text())
        if [root.stat().st_dev, root.stat().st_ino] != manifest["root_identity"]:
            raise RuntimeError("outer_root_identity")
        for name in ("header.json", "authority-start.json", "attempt-index.json", "DONE.json"):
            if fixture is None and (root / name).exists():
                raise RuntimeError("outer_root_consumed")
        library_path = "/usr/lib/x86_64-linux-gnu/libc.so.6"
        if hashlib.sha256(Path(library_path).read_bytes()).hexdigest() != manifest["native_sources"][str(Path(library_path).resolve())]:
            raise RuntimeError("outer_libc_source")
        library = ctypes.CDLL(library_path, use_errno=True)
        library.pidfd_open.argtypes = [ctypes.c_int, ctypes.c_uint]
        library.pidfd_open.restype = ctypes.c_int
        library.pidfd_send_signal.argtypes = [ctypes.c_int, ctypes.c_int,
                                             ctypes.c_void_p, ctypes.c_uint]
        library.pidfd_send_signal.restype = ctypes.c_int
        if fixture is not None and fixture["mode"] == "capability_failure":
            raise RuntimeError("injected_outer_capability_failure")
        first = kernel(os.getpid())
        capability = outer_pidfd_open(first["pid"], 0)
        try:
            outer_pidfd_signal(capability, 0)
            if select.select([capability], [], [], 0)[0]:
                raise RuntimeError("outer_self_pidfd")
        finally:
            os.close(capability)
        if kernel(first["pid"])["ticks"] != first["ticks"]:
            raise RuntimeError("outer_self_identity")
        library.prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong,
                                  ctypes.c_ulong, ctypes.c_ulong]
        library.prctl.restype = ctypes.c_int
        value = ctypes.c_int()
        pointer = ctypes.cast(ctypes.pointer(value), ctypes.c_void_p).value
        if library.prctl(36, 1, 0, 0, 0) != 0 or library.prctl(37, pointer, 0, 0, 0) != 0 or value.value != 1:
            raise RuntimeError("outer_subreaper_capability")
        sampler = threading.Thread(target=resource_loop, name="outer-real-sampler")
        sampler.start()
        here, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        here.setblocking(False)
        output = (directory / "project-output.txt").open("xb", buffering=0)
        command = [manifest["project_python"], str(Path(__file__).resolve()),
                   "--protected-project", str(root), str(child.fileno())]
        gate_parent = gate_child = None
        intent = None
        inherited = (child.fileno(),)
        if fixture is not None:
            from run_snapshot_control import SCRIPT
            intent = parent_client.rpc("fixture_intent", mode=fixture["mode"], nonce=fixture["nonce"])
            gate_parent, gate_child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            command = [manifest["project_python"], SCRIPT, "role", "worker", str(directory),
                       str(gate_child.fileno()), json.dumps(dict(
                           command=[manifest["project_python"], str(Path(__file__).resolve()),
                                    "--protected-fixture", str(root), str(child.fileno()), json.dumps(fixture)],
                           operational_ns=fixture["operational_ns"], hard_ns=fixture["hard_ns"],
                           inherit_authority=False, attempt=intent["attempt"],
                       ))]
            inherited += (gate_child.fileno(),)
        spawn_before = tick()
        proc = subprocess.Popen(command, pass_fds=inherited, cwd=manifest["cwd"],
                                env=manifest["environment"], stdin=subprocess.DEVNULL,
                                stdout=output, stderr=subprocess.STDOUT,
                                start_new_session=True)
        # Popen is owned before any subsequent identity/descriptor operation.
        fd = outer_pidfd_open(proc.pid, 0)
        outer_pidfd_signal(fd, 0)
        item = dict(kernel(proc.pid), spawn_before_ns=spawn_before,
                    spawn_after_ns=tick(), role="project")
        key = str(proc.pid) + ":" + str(item["ticks"])
        entries[key], descriptors[key] = item, fd
        if kernel(proc.pid)["ticks"] != item["ticks"]:
            raise RuntimeError("outer_project_identity")
        guard = threading.Thread(target=cutoff, name="outer-cutoff")
        guard.start()
        if fixture is not None:
            from run_snapshot_control import await_ancestors, send
            registered = dict(item, role="worker_child", birth_lower=spawn_before,
                              attempt=intent["attempt"], inferred=True)
            parent_client.rpc("identity", item=registered, intent=intent["id"])
            await_ancestors(parent_client, registered, fixture["operational_ns"])
            send(gate_parent, b"G", deadline=fixture["operational_ns"])
            gate_child.close()
            gate_parent.close()
        here.send(b"G")
        child.close()
        while tick() < state["H"]:
            if select.select([here], [], [], 0.005)[0]:
                raw, ancillary, flags, _ = here.recvmsg(65536, 1024)
                if not raw or flags:
                    if not state["done"]:
                        stop("project_eof_or_truncation")
                    break
                message = json.loads(raw)
                kind = message["event"]
                if kind == "register":
                    incoming = []
                    for level, name, data in ancillary:
                        if level == socket.SOL_SOCKET and name == socket.SCM_RIGHTS:
                            fds = array.array("i")
                            fds.frombytes(data)
                            incoming.extend(fds)
                    value = message["item"]
                    active = sum(not select.select([d], [], [], 0)[0] for d in descriptors.values())
                    if len(incoming) != 1 or active >= 64:
                        for d in incoming:
                            os.close(d)
                        raise RuntimeError("outer_registration_capacity")
                    actual = kernel(value["pid"])
                    known_parents = {i["pid"] for i in entries.values()}
                    if actual["ticks"] != value["ticks"] or actual["ppid"] not in known_parents:
                        os.close(incoming[0])
                        raise RuntimeError("outer_registration_identity")
                    entry_key = str(value["pid"]) + ":" + str(value["ticks"])
                    if entry_key in descriptors:
                        os.close(incoming[0])
                        raise RuntimeError("outer_registration_duplicate")
                    entries[entry_key], descriptors[entry_key] = dict(value), incoming[0]
                    here.send(json.dumps(dict(event="registered", key=entry_key)).encode())
                elif kind == "ready":
                    if tick() > state["O"] or state["stop"] is not None:
                        raise RuntimeError("late_project_ready")
                    state.update(ready=True, H=transition["D_val"] if fixture is None else h,
                                 O=transition["D_val"] if fixture is None else fixture["operational_ns"])
                elif kind == "fatal":
                    stop(message["error"], message["at_ns"], message.get("C_end"))
                elif kind == "project_done":
                    state["done"] = True
                    if not message["success"]:
                        stop("project_rejected", message["at_ns"])
                else:
                    raise RuntimeError("outer_message")
                events.append(dict(message, received_ns=tick()))
            if select.select([fd], [], [], 0)[0]:
                item.setdefault("first_exit_observed_ns", tick())
                if not state["done"]:
                    stop("project_early_exit", item["first_exit_observed_ns"])
                if all(select.select([d], [], [], 0)[0] for d in descriptors.values()):
                    break
        else:
            stop("outer_hard_tail", state["H"])
    except BaseException as exc:
        stop("outer:" + type(exc).__name__ + ":" + str(exc))
    finally:
        if proc is not None:
            # Preserve the direct handle even when pidfd acquisition failed.
            if proc.pid not in {i["pid"] for i in entries.values()}:
                try:
                    fd = outer_pidfd_open(proc.pid, 0)
                    actual = kernel(proc.pid)
                    k = str(proc.pid) + ":" + str(actual["ticks"])
                    entries[k], descriptors[k] = actual, fd
                except ProcessLookupError:
                    events.append(dict(event="unregistered_direct_child_exited", at_ns=tick()))
                except OSError as exc:
                    stop("outer_unresolved_direct_handle:" + str(exc))
            if state["stop"] is None and not state["done"]:
                stop("outer_finally")
            if guard is None and descriptors:
                guard = threading.Thread(target=cutoff, name="outer-cutoff")
                guard.start()
            end = state["stop"]["C_end"] if state["stop"] else state["H"]
            while tick() < end and any(not select.select([d], [], [], 0)[0] for d in descriptors.values()):
                time.sleep(0.005)
            tail = (proc.pid in {i["pid"] for i in entries.values()}
                    and all(select.select([d], [], [], 0)[0] for d in descriptors.values()))
            if not tail:
                stop("outer_cleanup_deadline", end)
            try:
                proc.wait(timeout=max(0.001, (state["H"] - tick()) / 1e9))
                item = next((i for i in entries.values() if i["pid"] == proc.pid), None)
                if item is not None:
                    item["reaped_ns"], item["returncode"] = tick(), proc.returncode
            except subprocess.TimeoutExpired:
                stop("outer_unreaped_handle")
        else:
            tail = True
        if tail:
            finished.set()
        if guard is not None:
            guard.join(max(0, (state["H"] - tick()) / 1e9))
        joined = guard is None or not guard.is_alive()
        sample_done.set()
        if sampler is not None:
            sampler.join(max(0, (state["H"]-tick())/1e9))
            joined = joined and not sampler.is_alive()
        if not sampling["valid"]:
            stop("outer_measurement_invalid")
        scans = []
        groups = {(item["pgid"], item["session"]) for item in entries.values()}
        for path in Path("/proc").glob("[0-9]*/stat"):
            try:
                current = kernel(int(path.parent.name))
            except FileNotFoundError:
                continue
            if (current["pgid"], current["session"]) in groups and current["state"] != "Z":
                scans.append(dict(current, at_ns=tick()))
        if scans:
            tail = False
            stop("outer_group_tail_unknown")
        for key, value in entries.items():
            if proc is not None and value["pid"] == proc.pid:
                continue
            try:
                waited, status = os.waitpid(value["pid"], os.WNOHANG)
                if waited:
                    value.update(reaped_ns=tick(), wait_status=status)
            except ChildProcessError:
                pass
        if not joined:
            stop("outer_guard_thread_tail")
        if joined:
            for descriptor in descriptors.values():
                os.close(descriptor)
        for channel in (here, child, gate_parent, gate_child):
            if channel is not None:
                channel.close()
        if output is not None:
            output.close()
        state.pop("signals", None)
        collection_spans = []
        ledger = root / "validation-ledger.jsonl"
        if fixture is None and ledger.exists():
            for line in ledger.read_text().splitlines():
                row = json.loads(line)
                if row.get("kind") == "collection_end":
                    collection_spans.append(row["end_ns"] - row["start_ns"])
        gap = tick() - v - sum(collection_spans)
        if fixture is None and gap > 60_000_000_000:
            stop("outer_gap_exceeded")
        cleanup_ok = not state["stop"] or tick() <= state["stop"]["C_end"]
        result = dict(state=state, events=events, entries=entries, physical_tail_known=tail,
                      cleanup_within_deadline=cleanup_ok,
                      measurement_valid=sampling["valid"] if sampler is not None else None,
                      sampling=sampling, group_scans=scans,
                      threads_joined=joined, at_ns=tick(), v=v,
                      g_ns=gap, final_chat_return="unmeasured")
        result["fixture"] = fixture
        (directory / "outer-result.json").write_text(json.dumps(result, indent=2)+"\n")
        stop_path = directory / ("STOP.json" if fixture is None else "fixture-STOP.json")
        if state["stop"] and not stop_path.exists():
            with stop_path.open("x") as stream:
                json.dump(state["stop"], stream, indent=2)
        if fixture is not None and registered is not None:
            parent_client.rpc("exited", item=registered,
                              at_ns=entries[key].get("first_exit_observed_ns", tick()))
    if fixture is not None:
        expected = fixture["expected"]
        reason = state["stop"]["reason"] if state["stop"] else None
        qualified = (reason in expected and tail and joined and sampling["valid"]
                     and cleanup_ok)
        parent_client.rpc("channel_closing")
        return 0 if qualified else 1
    return 0 if state["done"] and state["stop"] is None and tail and joined and sampling["valid"] else 1


# The host and the earliest project exception boundary need stdlib only.
if __name__ == "__main__" and len(sys.argv) > 1:
    if sys.argv[1] == "--outer":
        raise SystemExit(outer_entry(Path(sys.argv[2]), sys.argv[3]))
    if sys.argv[1] == "--protected-project":
        raise SystemExit(protected_project(Path(sys.argv[2]), int(sys.argv[3])))
    if sys.argv[1] == "--outer-fixture":
        raise SystemExit(outer_entry(Path(sys.argv[2]), None, sys.argv[3]))
    if sys.argv[1] == "--protected-fixture":
        raise SystemExit(protected_project(Path(sys.argv[2]), int(sys.argv[3]), json.loads(sys.argv[4])))

import pytest  # noqa: E402

from run_snapshot_control import (  # noqa: E402
    NS,
    Client,
    RealMonitor,
    event,
    identity,
    read,
    run_owned,
    save,
    capability_check,
    pidfd_open,
)

REPO = Path(__file__).resolve().parents[1]
AUTHORIZATION = "G0-TOOL-REPAIR-FOLLOWUP-V2-01"
PLAN_HASH = "06374b97810a855ce8f3d0d925667a91dbd149e3af1627e724cd8ae2b7948123"
REVIEW_HASH = "e7e54bbef0acd64ce100ec27580951776aa52278cf32a39d7d236a8d08204dba"
FILES = [
    "scripts/run_snapshot_control.py",
    "scripts/run_snapshot_g0.py",
    "scripts/run_snapshot_diagnostic.py",
    "tests/scripts/test_run_snapshot_control.py",
    "tests/scripts/test_run_snapshot_diagnostic.py",
]
LIMITS = ((570, 600), (150, 180), (30, 60))
_SESSION = None
_DOMAIN_PROOF = None


def clock():
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check_sources(sources):
    if not sources or any(digest(p) != h for p, h in sources.items()):
        raise RuntimeError("source_mismatch")


def canonical(raw, root):
    path = Path(raw)
    if not path.is_absolute() or path != path.resolve():
        raise RuntimeError("wrong_root_or_alias")
    if not path.is_relative_to(root):
        raise RuntimeError("wrong_root_or_alias")
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise RuntimeError("wrong_root_or_alias")
    return str(path)


def admission_reason(header, job, tick, *, stopped=False, pending=False):
    """The same refusal decision is used by live admission and bound views."""
    if stopped:
        return "root_stopped"
    if pending:
        return "unclosed_sibling_attempt"
    if job.get("stage") != "G0":
        return "scope_or_root"
    root = Path(header["root"])
    if job.get("budget") != str(root / "header.json"):
        return "scope_or_root"
    if job.get("batch_directory") != str(root):
        return "scope_or_root"
    s, o, h = (job[k] for k in ("start_ns", "operational_ns", "hard_ns"))
    if tick < s or tick >= o or o >= h:
        return "budget_admission"
    if h > header["D_work"] or h + 600 * NS > header["D_total"]:
        return "budget_admission"
    if not job.get("collection"):
        if h != s + 30 * NS or o > s + 5 * NS:
            return "case_budget"
        if h > job["parent_operational_ns"] or h > header["D_val"]:
            return "case_budget"
    return None


def bootstrap_header(root, channel, bootstrap, *, actual_boot, root_stat, expected_root=None):
    """Pure validation also used by frozen refusal-only V06 views."""
    lines = channel["output"].splitlines()
    if len(lines) != 3:
        raise RuntimeError("bootstrap_channel")
    first, record, end = map(json.loads, lines)
    if channel["exit_code"] != 0 or record != bootstrap:
        raise RuntimeError("bootstrap_channel")
    if (
        first.get("event") != "first_clock"
        or first.get("clock") != "CLOCK_MONOTONIC"
        or first.get("unit") != "ns"
        or first.get("t_r") != bootstrap["t_r"]
        or end.get("event") != "bootstrap_end"
        or bootstrap.get("clock") != "CLOCK_MONOTONIC"
        or bootstrap.get("unit") != "ns"
        or not (
            bootstrap["t_r"]
            <= bootstrap["read_before_ns"]
            <= bootstrap["read_after_ns"]
            <= end["at_ns"]
        )
    ):
        raise RuntimeError("bootstrap_clock_order")
    if bootstrap["boot_id"] != actual_boot:
        raise RuntimeError("bootstrap_boot")
    if bootstrap["authorization_id"] != AUTHORIZATION:
        raise RuntimeError("bootstrap_authorization")
    if bootstrap["root"] != str(root):
        raise RuntimeError("bootstrap_root")
    canonical(str(root), root)
    if expected_root is not None and (root_stat.st_dev, root_stat.st_ino) != tuple(expected_root):
        raise RuntimeError("bootstrap_root_identity")
    swap = {}
    for line in bootstrap["swap_raw"]:
        parts = line.split()
        if len(parts) != 3 or parts[2] != "kB":
            raise RuntimeError("swap_baseline")
        if parts[0] not in {"SwapTotal:", "SwapFree:"} or parts[0] in swap:
            raise RuntimeError("swap_baseline")
        swap[parts[0]] = int(parts[1]) * 1024
    if set(swap) != {"SwapTotal:", "SwapFree:"}:
        raise RuntimeError("swap_baseline")
    if not 0 <= swap["SwapFree:"] <= swap["SwapTotal:"]:
        raise RuntimeError("swap_baseline")
    return dict(
        authorization_id=AUTHORIZATION,
        root=str(root),
        device=root_stat.st_dev,
        inode=root_stat.st_ino,
        boot_id=actual_boot,
        t_r=bootstrap["t_r"],
        D_work=bootstrap["t_r"] + 6600 * NS,
        D_total=bootstrap["t_r"] + 7200 * NS,
        swap_start=swap["SwapTotal:"] - swap["SwapFree:"],
        scope="G0-tool-repair-only",
    )


def adopt(root, manifest):
    root = Path(root)
    canonical(str(root), root)
    if any((root / p).exists() for p in (
        "header.json", "authority-start.json", "attempt-index.json", "STOP.json",
        "DONE.json",
    )):
        raise RuntimeError("root_already_consumed")
    d = root.parent.parent
    if digest(d / "g0-tool-repair-followup-plan-v2.md") != PLAN_HASH:
        raise RuntimeError("plan_binding")
    if digest(d / "g0-tool-repair-followup-review-v1.md") != REVIEW_HASH:
        raise RuntimeError("review_binding")
    channel = read(root / "bootstrap-channel.json")
    bootstrap = read(root / "bootstrap.json")
    if bootstrap["t_r"] != manifest["t_new"]:
        raise RuntimeError("bootstrap_time_binding")
    if hashlib.sha256(channel["cmd"].encode()).hexdigest() != manifest["bootstrap_command_sha256"]:
        raise RuntimeError("bootstrap_command_binding")
    budget = read(root / "budget.json")
    if budget.get("stop") or budget.get("state") != "SEALED":
        raise RuntimeError("root_already_consumed")
    command = channel["cmd"]
    if not all(s in command for s in (
        "/usr/bin/timeout --signal=TERM --kill-after=1s 29s",
        "/usr/bin/python3 -I -S -c", str(root), AUTHORIZATION,
    )):
        raise RuntimeError("bootstrap_command")
    header = bootstrap_header(
        root, channel, bootstrap,
        actual_boot=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        root_stat=root.stat(),
        expected_root=(read(root / "bootstrap-adoption-audit.json")["root_device"],
                       read(root / "bootstrap-adoption-audit.json")["root_inode"]),
    )
    for path, expected in manifest["binaries"].items():
        if digest(path) != expected:
            raise RuntimeError("binary_binding")
    check_sources(manifest["sources"])
    header.update(
        authorization_hash=digest(root / "authorization.txt"),
        bootstrap_hash=digest(root / "bootstrap.json"),
        plan_hash=PLAN_HASH,
        manifest_hash=digest(root / "manifest.json"),
        D_val=read(root / "validation-begin.json")["D_val"],
    )
    with (root / "header.json").open("x") as stream:
        json.dump(header, stream, indent=2)
    return header


def latch_stop(previous, reason, tick):
    return previous if previous is not None else dict(
        reason=reason, at_ns=tick, received_ns=clock()
    )


class StopCell:
    """One irreversible in-memory cause and monotonically tightening bounds."""

    def __init__(self):
        self.lock = threading.Lock()
        self.value = None
        self.later = []

    def trip(self, reason, tick, hard, ancestor=None):
        with self.lock:
            first = self.value is None
            if first:
                self.value = dict(reason=reason, at_ns=tick, first_reason_at_ns=tick,
                                  received_ns=clock())
            else:
                self.later.append(dict(reason=reason, at_ns=tick, received_ns=clock()))
                self.value["at_ns"] = min(self.value["at_ns"], tick)
            end = min(tick+20*NS, hard-10*NS, hard if ancestor is None else ancestor)
            self.value["ancestor_end_ns"] = min(self.value.get("ancestor_end_ns", end), end)
            return first, self.value


def enqueue_or_stop(target, value, stop):
    try:
        target.put_nowait(value)
        return True
    except queue.Full:
        stop("authority_queue_full")
        return False


def persist_row(path, row, stop, opener=None):
    try:
        with (opener or Path.open)(Path(path), "a") as stream:
            stream.write(json.dumps(row) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        return True
    except Exception:
        stop("persistence_failure")
        return False


class Resources:
    """Acquisition journal shared by real initialization and finite rollback cases.

    It owns resources only: no root identity, license or business dispatcher.
    """

    def __init__(self):
        self.closers = []
        self.threads = []
        self.events = []
        self.closed = False

    def acquire(self, name, factory, closer):
        value = factory()
        self.closers.append((name, lambda: closer(value)))
        self.events.append(dict(event="acquired", name=name, at_ns=clock()))
        return value

    def start(self, name, target, stop):
        thread = threading.Thread(target=target, name=name)
        self.threads.append((thread, stop))
        thread.start()
        self.events.append(dict(event="thread_started", name=name, at_ns=clock()))
        return thread

    def close(self, deadline):
        if self.closed:
            return all(not thread.is_alive() for thread, _ in self.threads)
        errors = []
        for thread, stop in reversed(self.threads):
            stop()
            if thread.ident is not None:
                thread.join(max(0, (deadline - clock()) / NS))
            self.events.append(dict(event="thread_join", name=thread.name,
                                    alive=thread.is_alive(), at_ns=clock()))
        joined = all(not thread.is_alive() for thread, _ in self.threads)
        if joined:
            for name, close in reversed(self.closers):
                try:
                    close()
                    self.events.append(dict(event="closed", name=name, at_ns=clock()))
                except Exception as exc:
                    errors.append(dict(name=name, error=repr(exc), at_ns=clock()))
            self.closed = not errors
        self.events.extend(errors)
        return joined and self.closed


INITIALIZATION_STAGES = (
    "capability", "lease", "selector", "monitor", "fd", "writer",
    "sampler_started", "thread", "cutoff_thread", "client",
)


def initialize_resources(owner, operations, fail_after=None):
    """The single acquisition transaction, also used by refusal-only fixtures."""
    if tuple(operations) != INITIALIZATION_STAGES:
        raise RuntimeError("initialization_stage_order")
    if fail_after is not None and fail_after not in INITIALIZATION_STAGES:
        raise RuntimeError("initialization_fault_not_frozen")
    for stage, operation in operations.items():
        setattr(owner, stage, operation())
        owner.resources.events.append(dict(event="initialization_stage", stage=stage,
                                          at_ns=clock()))
        if fail_after == stage:
            raise RuntimeError("injected_initialization_after:" + stage)


def journal_loop(logs, done, path, stop, evidence_failed, *, blocker=None, opener=None):
    """The actual bounded writer; fixtures change only the I/O return boundary."""
    try:
        while not done.is_set() or not logs.empty():
            try:
                row, ack = logs.get(timeout=0.01)
            except queue.Empty:
                continue
            if blocker is not None:
                blocker.wait()
            if persist_row(path, row, stop, opener=opener):
                ack.set()
            else:
                evidence_failed("persistence_failure")
                done.set()
                return
    except BaseException as exc:
        evidence_failed("writer_thread:" + type(exc).__name__)
        stop("writer_thread:" + type(exc).__name__)


def finite_initialization_fixture(directory, fault):
    """Resource transaction only; no Authority, lease takeover or business grant."""
    from types import SimpleNamespace

    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    context = Client.current().rpc("case_context")
    frozen = context["spec"]["initialization_fixture"]
    if fault != frozen["fault"] or clock() >= context["operational_ns"]:
        raise RuntimeError("initialization_fixture_not_frozen")
    before_fds = set(os.listdir("/proc/self/fd"))
    before_threads = {t.ident for t in threading.enumerate()}
    state = SimpleNamespace(resources=Resources(), monitor=None, client=None)
    logs, done, blocked = queue.Queue(maxsize=8), threading.Event(), threading.Event()
    wire_done, cutoff_done = threading.Event(), threading.Event()
    cause, errors = [], []
    cause_lock = threading.Lock()

    def trip(reason, at_ns=None):
        with cause_lock:
            if not cause:
                cause.append(dict(reason=reason, at_ns=clock() if at_ns is None else at_ns))

    def capability():
        if fault == "capability_call":
            raise RuntimeError("injected_capability_call_failure")
        return capability_check(directory)

    def lease():
        if fault == "lease_open":
            raise OSError("injected_lease_open_failure")
        value = state.resources.acquire("fixture_lease", lambda: (directory / "fixture.lock").open("x"), lambda v: v.close())
        fcntl.flock(value, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return value

    def monitor():
        value = state.resources.acquire("fixture_monitor", lambda: RealMonitor(
            directory, Client.current().header, trip, autostart=False),
            lambda v: v.close_descriptors())
        value.register(dict(identity(os.getpid()), role="fixture_observer",
                            birth_lower=clock(), observation_only=True))
        return value

    def channel():
        if fault == "socket_create":
            raise OSError("injected_socket_create_failure")
        server, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        server.setblocking(False)
        state.server = server
        state.resources.closers.append(("fixture_socket", server.close))
        descriptor = child.detach()
        state.resources.closers.append(("fixture_fd", lambda: os.close(descriptor)))
        return descriptor

    # Client.current() must not be resolved in the fixture wire thread. Freeze
    # the actual parent header before starting it instead.
    parent_header = Client.current().header

    def wire_loop():
        import select
        try:
            while not wire_done.is_set():
                if not select.select([state.server], [], [], 0.005)[0]:
                    continue
                raw = state.server.recv(65536)
                if not raw:
                    return
                message = json.loads(raw)
                response = dict(seq=message["seq"])
                if message["op"] == "hello":
                    response["result"] = parent_header
                else:
                    response["error"] = "fixture_has_no_business_authority"
                state.server.send(json.dumps(response).encode())
        except BaseException as exc:
            trip("fixture_wire:" + type(exc).__name__)

    def broken_opener(path, mode):
        raise OSError("injected_writer_io_failure")

    def writer():
        def stop_writer():
            done.set()
            blocked.set()
        return state.resources.start("fixture-writer", lambda: journal_loop(
            logs, done, directory / "fixture-journal.jsonl", trip, errors.append,
            blocker=blocked if fault == "writer_ack_block" else None,
            opener=broken_opener if fault == "writer_io" else None,
        ), stop_writer)

    try:
        initialize_resources(state, dict(
            capability=capability, lease=lease,
            selector=lambda: state.resources.acquire("fixture_selector", selectors.DefaultSelector, lambda v: v.close()),
            monitor=monitor, fd=channel, writer=writer,
            sampler_started=lambda: state.monitor.start(),
            thread=lambda: state.resources.start("fixture-wire", wire_loop, wire_done.set),
            cutoff_thread=lambda: state.resources.start("fixture-cutoff", lambda: cutoff_done.wait(max(0, (context["operational_ns"] - clock()) / NS)), cutoff_done.set),
            client=lambda: Client(state.fd),
        ), fail_after=fault if fault in INITIALIZATION_STAGES else None)
        if fault in {"writer_io", "writer_ack_block"}:
            ack = threading.Event()
            logs.put_nowait((dict(kind="fixture_actual_write", nonce=frozen["nonce"]), ack))
            if not ack.wait(0.05):
                trip("persistence_ack_timeout")
            if fault == "writer_io":
                while not cause and clock() < context["operational_ns"]:
                    time.sleep(0.001)
        else:
            raise RuntimeError("fixture_missing_expected_failure")
    except BaseException as exc:
        trip(str(exc))
    finally:
        if not cause:
            trip("fixture_no_failure")
        c = cause[0]["at_ns"]
        end = min(c + 20 * NS, context["hard_ns"] - 10 * NS)
        if state.client is not None:
            state.client.socket.close()
        if state.monitor is not None:
            state.monitor.finish(end)
        complete = state.resources.close(end)
        after_fds = set(os.listdir("/proc/self/fd"))
        after_threads = {t.ident for t in threading.enumerate()}
        result = dict(cause=cause, errors=errors, resources=state.resources.events,
                      rollback_complete=complete, c=c, C_end=end, at_ns=clock(),
                      fd_set_restored=before_fds == after_fds,
                      threads_restored=before_threads == after_threads,
                      fault=fault, nonce=frozen["nonce"],
                      scope="production resource transaction, no root initialization or business license")
        save(directory / "fixture-rollback.json", result)
    if not (result["rollback_complete"] and result["fd_set_restored"]
            and result["threads_restored"] and result["at_ns"] <= end
            and cause[0]["reason"] == frozen["expected"]):
        raise RuntimeError("initialization_fixture_rejected:" + json.dumps(result))
    return result


class Authority:
    """One root, one control thread, one bounded persistence thread."""

    def __init__(self, root, manifest, header, outer=None):
        self.root, self.manifest, self.header = Path(root), manifest, header
        self.resources = Resources()
        self.outer, self.outer_lock = outer, threading.Lock()
        self.outer_registered = set()
        self.stop_lock = threading.Lock()
        self.stop_cell = StopCell()
        self.stop_followups = []
        self.lease = self.selector = self.monitor = self.client = None
        self.writer = self.thread = self.cutoff_thread = None
        self.fd = None
        self.state = "VALIDATING"
        self.stop_value = None
        self.evidence_error = None
        self.nodes = {}
        self.paths = set()
        self.used_cases = set()
        self.identities = {}
        self.intents = {}
        self.serial = 0
        self.collection = None
        self.collection_o = header["D_work"]
        self.collection_h = header["D_val"]
        self.case = None
        self.case_position = 0
        self.channels = {}
        self.logs = queue.Queue(maxsize=20000)
        self.writer_done = threading.Event()
        self.cutoff_done = threading.Event()
        self.cutoff_events = []
        self.cutoff_sent = set()
        self.domains = {}
        self.domain_queues = {}
        self.domain_cells = {}
        self.control_done = threading.Event()
        try:
            def capability():
                value = capability_check(root)
                save(self.root / "project-capability.json", value)
                return value

            def lease():
                value = self.resources.acquire(
                    "lease", lambda: (self.root / "authority.lock").open("x"),
                    lambda item: item.close(),
                )
                fcntl.flock(value, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with (self.root / "authority-start.json").open("x") as stream:
                    json.dump(dict(identity(os.getpid()), at_ns=clock()), stream)
                return value

            def channel():
                value = self.new_channel("startup", None, None)
                self.resources.closers.append(("startup_fd", lambda: os.close(value)))
                return value

            initialize_resources(self, dict(
                capability=capability, lease=lease,
                selector=lambda: self.resources.acquire(
                    "selector", selectors.DefaultSelector, lambda value: value.close()),
                monitor=lambda: self.resources.acquire(
                    "monitor", lambda: RealMonitor(
                        self.root, header, self.stop, authority=self,
                        root_monitor=True, autostart=False),
                    lambda value: value.close_descriptors()),
                fd=channel,
                writer=lambda: self.resources.start("root-writer", self.write_loop,
                                                     self.writer_done.set),
                sampler_started=lambda: self.monitor.start(),
                thread=lambda: self.resources.start("root-control", self.safe_loop,
                                                     self.control_done.set),
                cutoff_thread=lambda: self.resources.start("root-cutoff", self.safe_cutoff,
                                                            self.cutoff_done.set),
                client=lambda: Client(self.fd),
            ))
        except BaseException as exc:
            self.stop("initialization:" + type(exc).__name__ + ":" + str(exc))
            end = min(self.header["init_hard_ns"] - 10 * NS,
                      self.stop_value["at_ns"] + 20 * NS)
            if self.monitor is not None:
                self.monitor.finish(end)
            self.resources.close(end)
            save(self.root / "initialization-rollback.json", dict(
                events=self.resources.events, stop=self.stop_value,
                complete=self.resources.closed, at_ns=clock(), C_end=end,
            ))
            raise

    def notify_outer(self, value, descriptor=None, ack=False):
        if self.outer is None:
            raise RuntimeError("missing_outer_guard")
        import select
        end = min(clock() + 100_000_000, self.header["D_val"])
        if not self.outer_lock.acquire(timeout=max(0, (end-clock()) / NS)):
            raise RuntimeError("outer_lock_timeout")
        try:
            if not select.select([], [self.outer], [], max(0, (end-clock()) / NS))[1]:
                raise RuntimeError("outer_send_timeout")
            ancillary = [] if descriptor is None else [(
                socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [descriptor])
            )]
            self.outer.sendmsg([json.dumps(value).encode()], ancillary)
            if ack:
                if not select.select([self.outer], [], [], max(0, (end-clock()) / NS))[0]:
                    raise RuntimeError("outer_ack_timeout")
                response = json.loads(self.outer.recv(65536))
                if response != dict(event="registered", key=str(value["item"]["pid"]) + ":" + str(value["item"]["ticks"])):
                    raise RuntimeError("outer_ack_identity")
        finally:
            self.outer_lock.release()

    def safe_loop(self):
        try:
            self.loop()
        except BaseException as exc:
            self.stop("control_thread:" + type(exc).__name__)

    def safe_cutoff(self):
        try:
            self.cutoff()
        except BaseException as exc:
            self.stop("cutoff_thread:" + type(exc).__name__)

    def cutoff(self):
        from run_snapshot_control import signal_fixed
        import select
        import signal

        while not self.cutoff_done.is_set():
            tick = clock()
            case = self.case
            if case is not None and (tick >= case["hard_ns"] or ((case["spec"]["owner_required"] or case["spec"]["steps"]) and tick >= case["operational_ns"])):
                self.stop("case_deadline", tick)
            if self.collection is not None and tick >= self.collection_o:
                self.stop("collection_operational_deadline", tick)
            if self.stop_value is not None and self.collection is not None:
                end = min(self.stop_value["at_ns"] + 20 * NS,
                          self.collection_h - 10 * NS,
                          self.stop_value.get("ancestor_end_ns", self.collection_h))
                for key, fd in list(self.monitor.descriptors.items()):
                    item = self.monitor.entries[key]
                    if item.get("observation_only"):
                        continue
                    if not select.select([fd], [], [], 0)[0]:
                        sig = signal.SIGKILL if tick >= min(
                            self.stop_value["at_ns"] + 150_000_000, end - NS
                        ) else signal.SIGTERM
                        try:
                            signal_fixed(item, fd, sig, self.cutoff_events,
                                         self.cutoff_sent)
                        except Exception:
                            self.evidence_error = "root_signal_failure"
            time.sleep(0.005)

    def record(self, kind, **data):
        row = dict(data)
        row.setdefault("at_ns", clock())
        row["kind"] = kind
        ack = threading.Event()
        if not enqueue_or_stop(self.logs, (row, ack), self.stop):
            self.evidence_error = "authority_queue_full"
        return ack

    def stop(self, reason, at_ns=None, ancestor_end_ns=None):
        tick = at_ns if at_ns is not None else clock()
        with self.stop_lock:
            hard = self.case["hard_ns"] if self.case else self.collection_h
            first, self.stop_value = self.stop_cell.trip(reason, tick, hard, ancestor_end_ns)
            self.state = "STOP"
            self.stop_followups = self.stop_cell.later
            value = dict(self.stop_value)
        # Do not call the queue-full callback recursively and do no I/O in lock.
        if first:
            try:
                self.logs.put_nowait((dict(value, kind="ROOT_STOP"), threading.Event()))
            except queue.Full:
                self.evidence_error = "authority_queue_full"
            try:
                self.notify_outer(dict(event="fatal", error=reason, at_ns=tick,
                                       C_end=value["ancestor_end_ns"]))
            except Exception:
                self.evidence_error = "outer_stop_notification_failed"
        return value

    def write_loop(self):
        def failed(reason):
            self.evidence_error = reason
        journal_loop(self.logs, self.writer_done, self.root / "authority.jsonl",
                     self.stop, failed)

    def durable(self, kind, **data):
        ack = self.record(kind, **data)
        if not ack.wait(0.05):
            self.stop("persistence_ack_timeout")
            raise RuntimeError("persistence_ack_timeout")

    def new_channel(self, role, parent_pid, attempt):
        server, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        try:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
            server.setblocking(False)
            self.selector.register(server, selectors.EVENT_READ)
            self.channels[server.fileno()] = dict(
                socket=server, role=role, parent=parent_pid, attempt=attempt,
                pid=None, ticks=None, sequence=0,
            )
            self.resources.closers.append(("authority_socket", server.close))
            return child.detach()
        except BaseException:
            server.close()
            child.close()
            raise

    def check(self):
        if self.stop_value or self.state != "VALIDATING":
            raise RuntimeError("root_stopped")
        st = self.root.stat()
        if (st.st_dev, st.st_ino) != (self.header["device"], self.header["inode"]):
            self.stop("root_identity")
            raise RuntimeError("root_identity")
        if clock() >= self.header["D_work"]:
            self.stop("work_deadline")
            raise RuntimeError("work_deadline")
        if self.evidence_error:
            raise RuntimeError(self.evidence_error)

    def parent_node(self, ch):
        key = ch["attempt"]
        if key is None:
            return None
        node = self.nodes[key]
        if node["state"] != "PENDING":
            raise RuntimeError("closed_parent")
        return node

    def issue(self, ch, op, data):
        self.check()
        collection = data.get("job", {}).get("collection", False)
        if collection:
            if ch["role"] != "startup" or self.case is not None:
                raise RuntimeError("invalid_collection_parent")
            parent = None
        else:
            if self.case is None:
                raise RuntimeError("no_active_case")
            parent = ch["attempt"]
            if parent is None or self.parent_node(ch) is None:
                raise RuntimeError("no_active_parent")
        if not collection and self.case.get("child_stop"):
            raise RuntimeError("child_domain_stopped")
        path = canonical(data["directory"], self.root)
        if not collection and not Path(path).is_relative_to(Path(self.case["spec"]["directory"])):
            raise RuntimeError("wrong_case_directory")
        if path in self.paths:
            raise RuntimeError("attempt_already_used")
        pending = [
            n for n in self.nodes.values()
            if n["state"] == "PENDING" and n["parent"] == parent
        ]
        if pending:
            raise RuntimeError("unclosed_sibling_attempt")
        if not collection:
            parent_node = self.nodes[parent]
            nested = not parent_node["data"].get("job", {}).get("collection")
            allowed = parent_node["step"].get("children", []) if nested else self.case["spec"]["steps"]
            position = parent_node.get("child_step", 0) if nested else self.case["step"]
            if position >= len(allowed):
                raise RuntimeError("step_already_consumed")
            step = allowed[position]
            if step["op"] != op:
                raise RuntimeError("step_kind")
            if nested:
                parent_node["child_step"] = position + 1
            else:
                self.case["step"] += 1
        else:
            step = dict(op="attempt", fault=None, expected=None,
                        fault_nonce=self.manifest["collections"][self.collection - 1]["fault_nonce"])
        check_sources(self.manifest["sources"])
        if op == "attempt":
            job = data["job"]
            error = admission_reason(self.header, job, clock())
            if error:
                raise RuntimeError(error)
            if not collection:
                if job["start_ns"] != self.case["start_ns"]:
                    raise RuntimeError("case_clock_reset")
                if job["command"] != step["command"] or job["cwd"] != step["cwd"]:
                    raise RuntimeError("command_or_cwd_not_frozen")
                if job.get("view") != step.get("view"):
                    raise RuntimeError("view_not_frozen")
                if job.get("fault") != step.get("fault"):
                    raise RuntimeError("fault_not_frozen")
                if job.get("coordinator_mode", "normal") != step.get("mode", "normal"):
                    raise RuntimeError("mode_not_frozen")
        elif data["config"].get("stage") != "G0" or data["config"].get("tasks") != 250:
            raise RuntimeError("G0_only_no_100k_authorization")
        elif data["config"] != step.get("config", {"stage": "G0", "tasks": 250}):
            raise RuntimeError("diagnostic_config_not_frozen")
        elif data.get("fault") != step.get("fault"):
            raise RuntimeError("fault_not_frozen")
        self.serial += 1
        key = str(self.serial)
        node = dict(
            id=key, kind=op, parent=parent, state="PENDING", owner=ch["pid"],
            directory=path, case=self.case["nodeid"] if self.case else None,
            step=step, data=data, start_ns=clock(),
        )
        self.nodes[key] = node
        self.paths.add(path)
        self.durable("permit", node=node)
        return dict(id=key, header=self.header, step=step)

    def prepare_identity(self, item):
        key = str(item["pid"]) + ":" + str(item["ticks"])
        previous = self.identities.get(key, {})
        ancestors = []
        parent = item["attempt"]
        while parent is not None:
            ancestors.append(parent)
            parent = self.nodes[parent]["parent"]
        required = ancestors if item["role"] not in {"S", "W"} else ancestors[1:]
        item = dict(item, ancestor_nodes=ancestors, controller_nodes=required,
                    controller_acknowledged=previous.get("controller_acknowledged", []),
                    acknowledged=previous.get("acknowledged", []))
        self.identities[key] = item
        if key not in self.outer_registered:
            descriptor = pidfd_open(item["pid"])
            try:
                self.notify_outer(dict(event="register", item=item), descriptor, ack=True)
                self.outer_registered.add(key)
            finally:
                os.close(descriptor)
        return item

    def bootstrap_view(self, variant):
        bootstrap = read(self.root / "bootstrap.json")
        channel = read(self.root / "bootstrap-channel.json")
        first, _, end = map(json.loads, channel["output"].splitlines())
        stat = self.root.stat()
        expected_root = (stat.st_dev, stat.st_ino)
        if variant == "order":
            bootstrap["read_before_ns"] = bootstrap["t_r"] - 1
        elif variant == "unit":
            bootstrap["unit"] = "s"
        elif variant == "boot":
            bootstrap["boot_id"] = "wrong-boot"
        elif variant == "authorization":
            bootstrap["authorization_id"] = "old-authority"
        elif variant == "root":
            bootstrap["root"] = str(self.root / "other")
        elif variant == "swap_unit":
            bootstrap["swap_raw"][0] = bootstrap["swap_raw"][0].replace("kB", "B")
        elif variant == "inode":
            expected_root = (stat.st_dev, stat.st_ino + 1)
        if variant == "bad_end":
            end["event"] = "wrong_end"
        channel["output"] = "\n".join(map(json.dumps, (first, bootstrap, end))) + "\n"
        if variant == "missing_head":
            channel["output"] = "\n".join(channel["output"].splitlines()[1:]) + "\n"
        try:
            if variant in {"stopped", "replay"}:
                adopt(self.root, self.manifest)
            elif variant == "source":
                sources = dict(self.manifest["sources"])
                sources[next(iter(sources))] = "0" * 64
                check_sources(sources)
            elif variant in {"budget_equal", "budget_one_ns", "expired"}:
                header = dict(self.header, D_work=31 * NS, D_total=631 * NS,
                              D_val=31 * NS)
                job = dict(stage="G0", start_ns=NS, operational_ns=6 * NS,
                           hard_ns=31 * NS, parent_operational_ns=31 * NS,
                           budget=str(self.root / "header.json"),
                           batch_directory=str(self.root))
                if variant == "budget_one_ns":
                    header["D_work"] -= 1
                reason = admission_reason(header, job, 6 * NS if variant == "expired" else NS)
                return dict(reason=reason, grants_spawn=False)
            else:
                bootstrap_header(self.root, channel, bootstrap,
                                 actual_boot=self.header["boot_id"], root_stat=stat,
                                 expected_root=expected_root)
        except RuntimeError as exc:
            return dict(reason=str(exc), grants_spawn=False)
        return dict(reason=None, grants_spawn=False)

    def call(self, ch, request):
        op, data = request["op"], request.get("data", {})
        if ch.get("closing") and op not in {"status", "close", "exited", "channel_closing"}:
            raise RuntimeError("channel_business_closed")
        domain = self.domains.get(data.get("_domain"))
        if data.get("_domain") and op not in {"domain_start", "hello"}:
            if domain is None or domain["attempt"] != ch["attempt"]:
                raise RuntimeError("domain_identity")
            if op == "domain_event":
                variant = data["variant"]
                if variant != domain.get("event"):
                    raise RuntimeError("domain_event_not_frozen")
                def trip(reason):
                    cell = self.domain_cells.setdefault(data["_domain"], StopCell())
                    _, domain["stop"] = cell.trip(reason, clock(), self.case["hard_ns"])
                if variant == "queue_full":
                    target = self.domain_queues.setdefault(data["_domain"], queue.Queue(maxsize=1))
                    enqueue_or_stop(target, dict(value=1), trip)
                    enqueue_or_stop(target, dict(value=2), trip)
                elif variant == "persist_fail":
                    def broken(path, mode):
                        raise OSError("EXPECTED_PERSISTENCE_FAILURE")
                    persist_row(self.root / "unused-domain-row", {}, trip, opener=broken)
                elif variant in {"duplicate_stop", "lost_ack"}:
                    trip("domain_first_stop")
                    threads = [threading.Thread(target=trip, args=("domain_late_stop",)) for _ in range(2)]
                    for thread in threads:
                        thread.start()
                    for thread in threads:
                        thread.join(0.02)
                    if any(thread.is_alive() for thread in threads):
                        self.stop("domain_stop_thread_tail")
                        raise RuntimeError("domain_stop_thread_tail")
                else:
                    raise RuntimeError("domain_event_not_frozen")
                self.durable("expected_domain_event", domain=domain, variant=variant)
                return dict(stop=domain.get("stop"), drop_reply=variant == "lost_ack")
            if op == "domain_probe":
                if domain.get("stop"):
                    raise RuntimeError("domain_stopped")
                raise RuntimeError("domain_has_no_spawn_grant")
            if op in {"attempt", "diagnose"}:
                raise RuntimeError("domain_stopped" if domain.get("stop") else "domain_has_no_spawn_grant")
            if op == "stop":
                expected_reason = domain.get("reason") or ("pytest_" + domain["phase"] + "_failure")
                proof = domain.get("injection")
                allowed = [expected_reason] + domain.get("secondary_reasons", [])
                if (data["reason"] not in allowed or proof is None
                        or data.get("nonce") != domain["nonce"]
                        or data.get("marker") != proof["marker"]):
                    self.stop("unexpected_domain_failure:" + data["reason"])
                    raise RuntimeError("unexpected_domain_failure")
                domain.setdefault("stop", dict(reason=data["reason"], at_ns=clock()))
                self.record("EXPECTED_DOMAIN_STOP", domain=domain)
                return domain["stop"]
            if op == "begin_case":
                if domain.get("stop") or domain.get("active"):
                    raise RuntimeError("domain_stopped_or_pending")
                if data["nodeid"] != domain["nodeids"][domain["position"]]:
                    raise RuntimeError("domain_case_order")
                domain["active"] = data["nodeid"]
                return dict(domain=True)
            if op == "end_case":
                if domain.get("active") != data["nodeid"]:
                    raise RuntimeError("domain_case_identity")
                domain["active"] = None
                domain["position"] += 1
                return True
            if op == "session_context":
                return dict(nodeids=domain["nodeids"], domain=True,
                            marker=domain["marker"], nonce=domain["nonce"],
                            expected_collection_failure=bool(domain.get("injection")
                                and domain.get("stop", {}).get("reason") == "pytest_collection_failure"))
            if op == "domain_injection":
                if (domain.get("injection") or data["marker"] != domain["marker"]
                        or data["nonce"] != domain["nonce"]):
                    raise RuntimeError("domain_injection_not_frozen")
                domain["injection"] = dict(data, at_ns=clock(), pid=ch["pid"])
                self.durable("domain_injection", domain=data["_domain"], proof=domain["injection"])
                return True
        self.record("request", op=op, pid=ch["pid"], data=data)
        if op == "domain_start":
            self.check()
            node = self.nodes[ch["attempt"]]
            specification = node["step"].get("domain")
            key = data.get("_domain")
            if not specification or key != node["id"] or key in self.domains:
                raise RuntimeError("domain_not_frozen")
            self.domains[key] = dict(attempt=node["id"], nodeids=specification["nodeids"],
                                     phase=specification.get("phase"), reason=specification.get("reason"),
                                     event=specification.get("event"),
                                     marker=specification.get("marker"),
                                     nonce=specification["nonce"],
                                     secondary_reasons=specification.get("secondary_reasons", []),
                                     position=0, active=None)
            return True
        if op == "session_context":
            return dict(nodeids=[c["nodeid"] for c in self.manifest["collections"][self.collection - 1]["cases"]], domain=False)
        if op == "hello":
            if data.get("_domain"):
                ch["domain"] = data["_domain"]
            return self.header
        if op == "channel_closing":
            ch["closing"] = True
            self.durable("channel_business_closed", pid=ch["pid"], attempt=ch["attempt"])
            return True
        if op == "death_intent":
            fault = self.case["spec"].get("owner_fault") if self.case else None
            if fault != data["fault"]:
                raise RuntimeError("death_not_frozen")
            target = data["target"]
            item = next((i for i in self.identities.values() if i["pid"] == target["pid"] and i["ticks"] == target["ticks"]), None)
            if item is None or self.nodes[item["attempt"]]["owner"] != ch["pid"]:
                raise RuntimeError("death_owner")
            node = self.nodes[item["attempt"]]
            if data.get("nonce") != node["step"]["fault_nonce"]:
                raise RuntimeError("death_nonce")
            node["injection"] = dict(kind="owner_death", nonce=data["nonce"],
                                     target=target, at_ns=data["at_ns"], barrier=fault["barrier"])
            node["expected_trigger"] = dict(reason=node["step"]["expected"], at_ns=data["at_ns"])
            for channel in self.channels.values():
                if channel["pid"] == target["pid"]:
                    channel["closing"] = True
            self.durable("expected_death_intent", target=target, fault=fault, original_ns=data["at_ns"])
            return True
        if op == "status":
            return dict(
                stop=self.stop_value, identities=[i for i in list(self.identities.values())
                            if self.nodes[i["attempt"]]["state"] == "PENDING"],
                intents=[i for i in self.intents.values()
                         if self.nodes[i["attempt"]]["state"] == "PENDING"],
                expected_triggers={key: value["expected_trigger"] for key, value
                                   in self.nodes.items() if value["state"] == "PENDING"
                                   and value.get("expected_trigger")},
            )
        if op == "stop":
            return self.stop(data["reason"], data.get("at_ns"))
        if op == "identity":
            item = data["item"]
            node = self.nodes[item["attempt"]]
            if node["state"] != "PENDING":
                raise RuntimeError("identity_closed_attempt")
            current = identity(item["pid"])
            if current["ticks"] != item["ticks"] or current["ppid"] != ch["pid"]:
                raise RuntimeError("registration_identity")
            key = str(item["pid"]) + ":" + str(item["ticks"])
            previous = next((i for i in self.identities.values()
                             if (i["pid"], i["ticks"]) == (item["pid"], item["ticks"])), None)
            if previous is not None and not (previous.get("inferred") or previous.get("provisional")):
                raise RuntimeError("duplicate_identity")
            if len(self.monitor.active()) >= 64:
                self.stop("identity_capacity")
                raise RuntimeError("identity_capacity")
            item = self.prepare_identity(item)
            self.monitor.register(item)
            self.intents[data["intent"]]["resolved"] = key
            self.durable("identity_ack", item=item)
            return item
        if op == "controller_ack":
            item = self.identities[str(data["pid"]) + ":" + str(data["ticks"])]
            ancestor = data["ancestor"]
            if ch["role"] != "S" or ch["attempt"] != ancestor or ancestor not in item["controller_nodes"]:
                raise RuntimeError("controller_owner")
            if ancestor not in item["controller_acknowledged"]:
                self.durable("controller_watchdog_ack", ancestor=ancestor, pid=item["pid"], ticks=item["ticks"])
                item["controller_acknowledged"].append(ancestor)
            return True
        if op == "ancestor_ack":
            key = str(data["pid"]) + ":" + str(data["ticks"])
            item = self.identities[key]
            ancestor = data["ancestor"]
            if ancestor not in item["ancestor_nodes"] or self.nodes[ancestor]["owner"] != ch["pid"]:
                raise RuntimeError("ancestor_owner")
            if ancestor not in item["acknowledged"]:
                self.durable("ancestor_ack", ancestor=ancestor, item=key)
                item["acknowledged"].append(ancestor)
            return True
        if op == "owner_observation":
            self.check()
            item = data["item"]
            if self.nodes[item["attempt"]]["owner"] != ch["pid"]:
                raise RuntimeError("independent_owner")
            current = identity(item["pid"])
            parent = next((i for i in self.identities.values() if i["pid"] == current["ppid"] and i["attempt"] == item["attempt"]), None)
            if current["ticks"] != item["ticks"] or parent is None:
                raise RuntimeError("independent_owner_parent")
            self.monitor.register(dict(item, owner_only=True))
            self.durable("independent_owner_observation", item=item, grants_tool_membership=False)
            return True
        if op == "owner_identity":
            if self.case is None and ch["role"] != "startup":
                raise RuntimeError("owner_outside_case")
            item = data["item"]
            current = identity(item["pid"])
            chain = []
            parent = item["attempt"]
            while parent is not None:
                chain.append(parent)
                parent = self.nodes[parent]["parent"]
            known_parent = any(i["pid"] == current["ppid"] and i["attempt"] in chain for i in self.identities.values())
            if current["ticks"] != item["ticks"] or not known_parent:
                raise RuntimeError("owner_identity")
            previous = self.identities.get(str(item["pid"]) + ":" + str(item["ticks"]))
            provisional = self.prepare_identity(dict(item, provisional=previous is None or previous.get("provisional", False)))
            self.monitor.register(provisional)
            ancestor = data["ancestor"]
            if self.nodes[ancestor]["owner"] != ch["pid"] or ancestor not in provisional["ancestor_nodes"]:
                raise RuntimeError("ancestor_owner")
            if ancestor not in provisional["acknowledged"]:
                provisional["acknowledged"].append(ancestor)
            self.durable("owner_identity_ack", item=provisional)
            return True
        if op == "static_context":
            if self.collection != 3 or ch["role"] != "worker":
                raise RuntimeError("static_scope")
            return dict(job=self.nodes[ch["attempt"]]["data"]["job"], commands=self.manifest["static_commands"])
        if op == "static_intent":
            self.check()
            if self.collection != 3 or ch["role"] != "worker":
                raise RuntimeError("static_scope")
            node = self.nodes[ch["attempt"]]
            index = node.get("static_position", 0)
            if data["index"] != index or data["command"] != self.manifest["static_commands"][index]:
                raise RuntimeError("static_order")
            node["static_position"] = index + 1
            key = str(len(self.intents) + 1)
            value = dict(id=key, attempt=ch["attempt"], role="worker_child", parent_pid=ch["pid"], at_ns=clock(), resolved=None)
            self.intents[key] = value
            self.durable("static_intent", intent=value)
            return dict(id=key, attempt=ch["attempt"])
        if op == "fixture_intent":
            self.check()
            if self.case is None or ch["role"] != "worker":
                raise RuntimeError("fixture_parent")
            frozen = self.case["spec"].get("outer_fixture")
            node = self.nodes[ch["attempt"]]
            if (not frozen or frozen["mode"] != data["mode"]
                    or frozen["nonce"] != data["nonce"] or node.get("fixture_used")):
                raise RuntimeError("fixture_not_frozen_or_consumed")
            node["fixture_used"] = True
            key = str(len(self.intents)+1)
            value = dict(id=key, attempt=node["id"], role="worker_child",
                         parent_pid=ch["pid"], at_ns=clock(), resolved=None)
            self.intents[key] = value
            self.durable("finite_fixture_intent", intent=value, nonce=data["nonce"])
            return dict(id=key, attempt=node["id"])
        if op in {"owner_fixture_failure", "owner_fixture_probe"}:
            self.check()
            specification = self.case["spec"].get("owner_fixture") if self.case else None
            if (ch["role"] != "worker" or not specification
                    or specification["nonce"] != data["nonce"]):
                raise RuntimeError("owner_fixture_not_frozen")
            key = "owner:"+ch["attempt"]
            if op == "owner_fixture_probe":
                if key in self.domains:
                    raise RuntimeError("owner_fixture_stopped")
                raise RuntimeError("owner_fixture_not_failed")
            if key in self.domains or data["reason"] != specification["expected"]:
                self.stop("unexpected_owner_fixture_failure")
                raise RuntimeError("unexpected_owner_fixture_failure")
            self.domains[key] = dict(state="STOP", reason=data["reason"],
                                     at_ns=data["at_ns"], received_ns=clock(),
                                     nonce=data["nonce"], business_grants=False)
            self.durable("finite_owner_failure", proof=self.domains[key])
            return True
        if op == "aux_intent":
            self.check()
            if self.case is None or not self.case["spec"].get("auxiliary") or self.case.get("aux_used"):
                raise RuntimeError("auxiliary_not_frozen")
            self.case["aux_used"] = True
            key = str(len(self.intents) + 1)
            value = dict(id=key, attempt=ch["attempt"], role="unrelated", parent_pid=ch["pid"], at_ns=clock(), resolved=None)
            self.intents[key] = value
            self.durable("auxiliary_intent", intent=value)
            return dict(id=key)
        if op == "intent":
            self.check()
            node = self.nodes[data["attempt"]]
            role = data["role"]
            valid = (role == "S" and node["owner"] == ch["pid"]) or (role in {"W", "C", "worker"} and ch["role"] == "S" and ch["attempt"] == node["id"])
            if node["state"] != "PENDING" or node.get("expected_trigger") or not valid:
                raise RuntimeError("intent_parent")
            used = node.setdefault("intent_roles", [])
            if role in used:
                raise RuntimeError("intent_consumed")
            used.append(role)
            key = str(len(self.intents) + 1)
            value = dict(id=key, attempt=data["attempt"], role=data["role"],
                         parent_pid=ch["pid"], at_ns=clock(), resolved=None)
            self.intents[key] = value
            self.durable("launch_intent", intent=value)
            return dict(id=key)
        if op == "exited":
            self.monitor.observe_exit(data["item"], data["at_ns"], "child_notice")
            return True
        if op == "child_trigger":
            node = self.nodes[data["attempt"]]
            if ch["pid"] != node["owner"] and ch["attempt"] != node["id"]:
                raise RuntimeError("trigger_owner")
            expected = node["step"].get("expected")
            allowed = expected if isinstance(expected, list) else [expected] if expected is not None else []
            allowed = list(allowed) + node["step"].get("trigger_expected", [])
            matches = data["reason"] in allowed
            if not matches:
                self.stop("unexpected_child_trigger:" + data["reason"], data["at_ns"])
            else:
                node.setdefault("expected_trigger", dict(reason=data["reason"], at_ns=data["at_ns"]))
            self.durable("child_trigger", attempt=node["id"], reason=data["reason"], original_ns=data["at_ns"])
            return True
        if op == "fault_observed":
            node = self.nodes[data["attempt"]]
            if ch["attempt"] != node["id"] and ch["pid"] != node["owner"]:
                raise RuntimeError("fault_observer_owner")
            if data["nonce"] != node["step"]["fault_nonce"]:
                raise RuntimeError("fault_nonce")
            if data["boundary"] not in node["step"].get("fault_boundaries", []):
                self.stop("unfrozen_fault_boundary")
                raise RuntimeError("unfrozen_fault_boundary")
            if data["boundary"] == "worker_return" and data["evidence"]["returncode"] not in node["step"].get("worker_exit_codes", []):
                self.stop("unexpected_worker_exit_code")
                raise RuntimeError("unexpected_worker_exit_code")
            records = node.setdefault("fault_observations", [])
            if len(records) >= 64:
                self.stop("fault_evidence_capacity")
                raise RuntimeError("fault_evidence_capacity")
            records.append(dict(data, observer_pid=ch["pid"]))
            self.durable("fault_observed", proof=data)
            return True
        if op == "close":
            node = self.nodes[data["id"]]
            if node["owner"] != ch["pid"] or node["state"] != "PENDING":
                raise RuntimeError("duplicate_close_or_owner")
            if any(n["parent"] == node["id"] and n["state"] == "PENDING"
                   for n in self.nodes.values()):
                raise RuntimeError("unclosed_child")
            if node.get("child_step", 0) != len(node["step"].get("children", [])):
                self.stop("child_steps_incomplete")
                raise RuntimeError("child_steps_incomplete")
            result = data["result"]
            expected = node["step"].get("expected")
            actual = result.get("error")
            good = bool(result.get("success"))
            if expected is not None:
                possibilities = expected if isinstance(expected, list) else [expected]
                good = actual in possibilities
                if node["kind"] == "attempt":
                    good = good and result.get("tail_known") == node["step"].get("tail_known", True)
                    good = good and result.get("parent_sampling_valid", result.get("sampling_valid"))
                    good = good and result.get("owner_complete")
                    good = good and result.get("cleanup_within_deadline")
                    good = good and (node.get("fault_observations") or node.get("injection")
                                     or node["step"].get("domain"))
            if node["step"].get("domain"):
                domain_spec = node["step"]["domain"]
                observed = self.domains.get(node["id"], {}).get("stop", {}).get("reason")
                required = domain_spec.get("reason") or ("pytest_" + domain_spec["phase"] + "_failure")
                domain_result = self.domains.get(node["id"], {})
                good = good and observed == required and bool(
                    domain_result.get("injection") or domain_spec.get("event")
                )
            if not good:
                self.stop("unexpected_child_result")
            elif expected is not None and self.case is not None:
                for intent in self.intents.values():
                    if intent["attempt"] == node["id"] and not intent.get("resolved"):
                        owner = result.get("independent_owner") or {}
                        known = [row["item"] for row in owner.get("events", []) if row["kind"] == "identity_ack" and row["item"]["role"] == intent["role"]]
                        fault = self.case["spec"].get("owner_fault", {})
                        before = fault.get("barrier") == intent["role"] + "_before_spawn"
                        if known or before:
                            intent["outer_resolution"] = "known_owned_identity" if known else "owner_confirmed_not_created"
                        else:
                            self.stop("unresolved_physical_intent")
                self.case.setdefault("child_stop", dict(reason=actual, at_ns=clock()))
            node.update(state="EXPECTED_REJECTION" if expected and good else
                        "CLOSED" if good else "STOP", result=result, end_ns=clock())
            self.durable("close", node=node)
            return good
        if op == "owner_closed":
            if self.case is None or not data["complete"] or data["thread_alive"]:
                self.stop("owner_not_closed")
                raise RuntimeError("owner_not_closed")
            canonical(data["evidence"], Path(self.case["spec"]["directory"]))
            self.case["owner_closed"] = data
            self.durable("owner_closed", evidence=data)
            return True
        if op == "diagnostic_summary":
            children = [n for n in self.nodes.values() if n["parent"] == data["attempt"] and n["kind"] == "diagnose"]
            return dict(applicable=bool(children), valid=all(n.get("result", {}).get("validation_complete") for n in children), ids=[n["id"] for n in children])
        if op == "case_context":
            if not self.case:
                raise RuntimeError("no_active_case")
            return self.case
        if op == "end_case":
            if self.case is None or self.case["nodeid"] != data["nodeid"]:
                raise RuntimeError("case_identity")
            if any(n["state"] == "PENDING" and n["case"] == data["nodeid"]
                   for n in self.nodes.values()):
                self.stop("unclosed_case_attempt")
                raise RuntimeError("unclosed_case_attempt")
            if self.case["spec"].get("owner_required") and not self.case.get("owner_closed"):
                self.stop("owner_join_missing")
                raise RuntimeError("owner_join_missing")
            if self.case["step"] != len(self.case["spec"]["steps"]):
                self.stop("case_steps_incomplete")
                raise RuntimeError("case_steps_incomplete")
            self.case = None
            self.case_position += 1
            return True
        self.check()
        if op == "channel":
            node = self.nodes[data["attempt"]]
            role = data["role"]
            if node["state"] != "PENDING":
                raise RuntimeError("closed_parent")
            if role == "S":
                allowed = ch["pid"] == node["owner"]
            else:
                allowed = ch["role"] == "S" and ch["attempt"] == node["id"]
            if not allowed or role not in {"S", "W", "C", "worker"}:
                raise RuntimeError("role_parent")
            used = node.setdefault("roles", [])
            if role in used:
                raise RuntimeError("role_consumed")
            used.append(role)
            return dict(fd=self.new_channel(role, ch["pid"], node["id"]))
        if op == "begin_case":
            if self.case is not None or not self.collection:
                raise RuntimeError("unclosed_case")
            entries = self.manifest["collections"][self.collection - 1]["cases"]
            if self.case_position >= len(entries):
                raise RuntimeError("case_already_consumed")
            spec = entries[self.case_position]
            if spec["nodeid"] != data["nodeid"] or data["nodeid"] in self.used_cases:
                raise RuntimeError("case_order")
            tick = clock()
            if tick + 30 * NS > min(self.collection_o, self.header["D_val"]):
                self.stop("case_budget")
                raise RuntimeError("case_budget")
            self.used_cases.add(data["nodeid"])
            self.case = dict(nodeid=data["nodeid"], spec=spec, step=0,
                             start_ns=tick, operational_ns=tick + 5 * NS,
                             hard_ns=tick + 30 * NS,
                             parent_operational_ns=self.collection_o)
            return self.case
        if op in {"attempt", "diagnose"}:
            return self.issue(ch, op, data)
        if op == "bootstrap_view":
            if self.case is None or data["name"] not in self.case["spec"].get("views", []):
                raise RuntimeError("view_not_frozen")
            outcome = self.bootstrap_view(data["variant"])
            self.durable("bootstrap_view", variant=data["variant"], outcome=outcome)
            return outcome
        if op == "view":
            if self.case is None or data["name"] not in self.case["spec"].get("views", []):
                raise RuntimeError("view_not_frozen")
            # Refusal-only views call the live decision; no permit can be issued.
            return dict(reason=admission_reason(**data["decision"]),
                        grants_spawn=False)
        raise RuntimeError("unknown_request")

    def loop(self):
        while not self.control_done.is_set():
            for key, _ in self.selector.select(0.005)[:64]:
                ch = self.channels[key.fd]
                request = None
                try:
                    raw, ancillary, flags, _ = ch["socket"].recvmsg(65536, 1024)
                    if not raw:
                        expected_domain_end = self.domains.get(ch.get("domain"), {}).get("stop")
                        if ch["pid"] is not None and not ch.get("closing") and not expected_domain_end:
                            try:
                                current = identity(ch["pid"])
                                if current["ticks"] == ch["ticks"] and current["state"] != "Z":
                                    self.stop("live_authority_channel_lost")
                            except FileNotFoundError:
                                pass
                        self.selector.unregister(ch["socket"])
                        ch["socket"].close()
                        continue
                    if flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
                        raise RuntimeError("message_capacity")
                    credentials = [struct.unpack("3i", value) for level, kind, value
                                   in ancillary if level == socket.SOL_SOCKET
                                   and kind == socket.SCM_CREDENTIALS]
                    if len(credentials) != 1:
                        raise RuntimeError("credentials")
                    pid = credentials[0][0]
                    current = identity(pid)
                    if ch["pid"] is None:
                        if ch["parent"] is not None and current["ppid"] != ch["parent"]:
                            raise RuntimeError("channel_parent_identity")
                        ch.update(pid=pid, ticks=current["ticks"])
                    if (ch["pid"], ch["ticks"]) != (pid, current["ticks"]):
                        raise RuntimeError("channel_identity")
                    request = json.loads(raw)
                    if request["seq"] != ch["sequence"] + 1:
                        raise RuntimeError("channel_replay")
                    ch["sequence"] = request["seq"]
                    try:
                        result = self.call(ch, request)
                        response = dict(seq=request["seq"], result=result)
                    except (RuntimeError, FileNotFoundError) as exc:
                        response = dict(seq=request["seq"], error=str(exc))
                    descriptors = []
                    if isinstance(response.get("result"), dict) and "fd" in response["result"]:
                        descriptor = response["result"].pop("fd")
                        response["result"]["has_fd"] = True
                        descriptors = [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                        array.array("i", [descriptor]))]
                    if response.get("result", {}).get("drop_reply") if isinstance(response.get("result"), dict) else False:
                        continue
                    ch["socket"].sendmsg([json.dumps(response).encode()], descriptors)
                    if descriptors:
                        os.close(descriptor)
                except Exception as exc:
                    self.stop("authority_protocol:" + type(exc).__name__)

    def close(self, deadline):
        sampling = self.monitor.finish(deadline) if self.monitor is not None else False
        if self.client is not None:
            try:
                self.client.rpc("channel_closing")
            except Exception:
                self.stop("startup_channel_close_failed")
            self.client.socket.close()
        joined = self.resources.close(deadline)
        if not joined or not sampling:
            self.stop("authority_tail_or_sampling")
        save(self.root / "attempt-index.json", dict(
            nodes=self.nodes, identities=self.identities, intents=self.intents,
            stop=self.stop_value, subsequent_reasons=self.stop_followups,
            state=self.state, threads_joined=joined, sampling_valid=sampling,
            cutoff_events=self.cutoff_events, domains=self.domains,
            resources=self.resources.events,
        ))
        if self.stop_value and not (self.root / "STOP.json").exists():
            with (self.root / "STOP.json").open("x") as stream:
                json.dump(self.stop_value, stream, indent=2)
        return joined and sampling and self.evidence_error is None


def plugin_client():
    return Client.current()


def domain_injection():
    global _DOMAIN_PROOF
    client = plugin_client()
    info = client.rpc("session_context")
    if not info.get("domain") or not info.get("marker"):
        raise RuntimeError("not_a_frozen_injection_domain")
    proof = dict(marker=info["marker"], nonce=info["nonce"])
    client.rpc("domain_injection", **proof)
    _DOMAIN_PROOF = proof
    return proof["marker"]


def plugin_stop(reason, detail=""):
    data = dict(reason=reason)
    if os.environ.get("RUN_SNAPSHOT_TEST_DOMAIN"):
        proof = _DOMAIN_PROOF
        if proof is not None and proof["marker"] in str(detail):
            data.update(proof)
    return plugin_client().rpc("stop", **data)


def pytest_sessionstart(session):
    global _SESSION
    _SESSION = session
    client = plugin_client()
    if os.environ.get("RUN_SNAPSHOT_TEST_DOMAIN"):
        client.rpc("domain_start")
    import takler
    import run_snapshot_no_services

    event(Path(client.header["root"]) / "pytest-events.jsonl", "session_start",
          executable=os.path.realpath(os.sys.executable), cwd=os.getcwd(),
          takler=takler.__file__, control=__import__("run_snapshot_control").__file__,
          plugin=__file__, fence=run_snapshot_no_services.__file__,
          plugins=[dict(name=name, path=getattr(plugin, "__file__", None))
                   for name, plugin in session.config.pluginmanager.list_name_plugin()])
    if Path.cwd() != REPO:
        if os.environ.get("RUN_SNAPSHOT_DOMAIN_PHASE") == "wrong_cwd":
            marker = domain_injection()
            plugin_stop("pytest_cwd", marker)
            raise RuntimeError(marker)
        plugin_stop("pytest_cwd")
        raise RuntimeError("pytest_cwd")
    if not session.config.pluginmanager.hasplugin("run_snapshot_no_services"):
        if os.environ.get("RUN_SNAPSHOT_DOMAIN_PHASE") == "missing_plugin":
            marker = domain_injection()
            plugin_stop("missing_fence", marker)
            raise RuntimeError(marker)
        plugin_stop("missing_fence")
        raise RuntimeError("missing_fence")


def pytest_make_parametrize_id(config, val, argname):
    if isinstance(val, type):
        value = val.__module__ + "." + val.__qualname__
    else:
        value = val
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return argname + "_" + hashlib.sha256(raw.encode()).hexdigest()[:12]



def pytest_generate_tests(metafunc):
    if (os.environ.get("RUN_SNAPSHOT_DOMAIN_PHASE") == "collect"
            and metafunc.function.__name__ == "_controlled_failure"):
        raise RuntimeError(domain_injection())

def pytest_collection_finish(session):
    if os.environ.get("RUN_SNAPSHOT_DOMAIN_PHASE") == "internal":
        raise RuntimeError(domain_injection())
    context = plugin_client().rpc("session_context")
    actual = [item.nodeid for item in session.items]
    if context.get("expected_collection_failure") and not actual:
        return
    root = Path(os.environ["RUN_SNAPSHOT_REPAIR_ROOT"])
    suffix = os.environ.get("RUN_SNAPSHOT_TEST_DOMAIN", "root")
    event(root / "collection-nodeids.jsonl", "collection", domain=suffix,
          expected=context["nodeids"], actual=actual)
    if not actual:
        if os.environ.get("RUN_SNAPSHOT_DOMAIN_PHASE") == "empty":
            marker = domain_injection()
            plugin_stop("collection_empty", marker)
            raise RuntimeError(marker)
        plugin_stop("collection_empty")
        raise RuntimeError("collection_empty")
    if actual != context["nodeids"]:
        plugin_stop("collection_mismatch")
        raise RuntimeError("collection_mismatch")


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(session, config, items):
    context = plugin_client().rpc("session_context")
    expected = context["nodeids"]
    actual = [item.nodeid for item in items]
    if context.get("expected_collection_failure") and not actual:
        return
    event(Path(os.environ["RUN_SNAPSHOT_REPAIR_ROOT"]) / "collection-order.jsonl",
          "predeclared_order", actual=actual, expected=expected,
          domain=os.environ.get("RUN_SNAPSHOT_TEST_DOMAIN"))
    if len(actual) != len(set(actual)) or set(actual) != set(expected):
        plugin_stop("collection_mismatch")
        raise RuntimeError("collection_mismatch")
    positions = {nodeid: index for index, nodeid in enumerate(expected)}
    items.sort(key=lambda item: positions[item.nodeid])


@pytest.fixture
def tmp_path(request):
    context = plugin_client().rpc("case_context")
    directory = Path(context["spec"]["directory"])
    directory.mkdir(parents=True, exist_ok=True)
    return directory



@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    plugin_client().rpc("begin_case", nodeid=item.nodeid)


def pytest_runtest_logreport(report):
    if report.failed or report.skipped or hasattr(report, "wasxfail"):
        plugin_stop("pytest_" + report.when + "_failure",
                    str(report.longrepr) + str(getattr(report, "wasxfail", "")))
        if _SESSION is not None:
            _SESSION.shouldstop = "G0 first unexpected failure"
    if report.when == "teardown":
        plugin_client().rpc("end_case", nodeid=report.nodeid)


def pytest_collectreport(report):
    if report.failed:
        plugin_stop("pytest_collection_failure", report.longrepr)


def pytest_internalerror(excrepr, excinfo):
    plugin_stop("pytest_internal_error", excrepr)



def pytest_sessionfinish(session, exitstatus):
    root = Path(os.environ["RUN_SNAPSHOT_REPAIR_ROOT"])
    modules = {name: str(value.__file__) for name, value in list(os.sys.modules.items())
               if getattr(value, "__file__", None)}
    event(root / "import-paths.jsonl", "session_finish", modules=modules,
          exitstatus=int(exitstatus), executable=os.sys.executable,
          domain=os.environ.get("RUN_SNAPSHOT_TEST_DOMAIN"))
    plugin_client().rpc("channel_closing")


def full_source_audit(manifest):
    check_sources(manifest["sources"])
    for base, entries in manifest["dependency_sources"].items():
        for relative, expected in entries.items():
            if digest(Path(base) / relative) != expected:
                raise RuntimeError("dependency_source_changed")


def loaded_environment(root, manifest, phase):
    """Record actual loading, and compare it to the pre-existing seal."""
    maps = Path("/proc/self/maps").read_text()
    expected = dict(manifest["native_sources"])
    expected.update(manifest["binaries"])
    loaded = {}
    for line in maps.splitlines():
        parts = line.split(maxsplit=5)
        if len(parts) != 6 or not parts[5].startswith("/"):
            continue
        path = str(Path(parts[5]).resolve())
        if path in loaded:
            continue
        actual = digest(path)
        loaded[path] = actual
        if expected.get(path) != actual:
            raise RuntimeError("unsealed_loaded_native:" + path)
    modules = {name: str(value.__file__) for name, value in list(sys.modules.items())
               if getattr(value, "__file__", None)}
    for path in set(modules.values()):
        actual_path = Path(path).resolve()
        actual = digest(actual_path)
        bound = manifest["sources"].get(str(actual_path))
        if bound is None:
            for base, entries in manifest["dependency_sources"].items():
                if actual_path.is_relative_to(base):
                    bound = entries.get(str(actual_path.relative_to(base)))
                    break
        if bound != actual:
            raise RuntimeError("unsealed_loaded_module:" + str(actual_path))
    event(Path(root) / "actual-environment.jsonl", "loaded_environment", phase=phase,
          executable=sys.executable, real_executable=os.path.realpath(sys.executable),
          version=sys.version, maps=maps, loaded_native=loaded, modules=modules,
          environment={key: os.environ.get(key) for key in manifest["environment"]})
    return loaded


def validate_manifest(manifest):
    if len(manifest["collections"]) != 3:
        raise RuntimeError("collection_count")
    for number, (entry, (o, h)) in enumerate(zip(manifest["collections"], LIMITS, strict=True), 1):
        if (entry["operational_s"], entry["hard_s"]) != (o, h):
            raise RuntimeError("collection_limits")
        if number < 3:
            argv = entry["argv"]
            prefix = [manifest["project_python"], "-m", "pytest", "-x", "--maxfail=1",
                      "-p", "run_snapshot_g0", "-p", "run_snapshot_no_services"]
            suffix = ["-o", "cache_dir="+str(Path(manifest["root"])/"pytest-cache"), "-vv"]
            if number == 2:
                suffix += ["-k", "not test_server_start_rejects_async_hook_before_restore_or_network"]
            if argv != prefix + suffix + entry["selectors"]:
                raise RuntimeError("pytest_arguments")
            if not entry["cases"] or len({c["nodeid"] for c in entry["cases"]}) != len(entry["cases"]):
                raise RuntimeError("case_manifest")
        elif entry["cases"]:
            raise RuntimeError("static_collection_cases")


def run_static(root):
    import select
    import subprocess
    import sys
    import signal
    from run_snapshot_control import SCRIPT, await_ancestors, send, pidfd_signal, members

    root = Path(root)
    client = Client.current()
    if str(root) != client.header["root"]:
        raise RuntimeError("static_root")
    context = client.rpc("static_context")
    original = context["job"]
    for index, command in enumerate(context["commands"]):
        begin = clock()
        if begin >= original["operational_ns"]:
            raise RuntimeError("static_operational_deadline")
        intent = client.rpc("static_intent", index=index, command=command)
        directory = root / "collection-3" / ("step-" + str(index))
        directory.mkdir()
        job = dict(original, command=command, inherit_authority=False)
        here, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        output = (directory / "worker.log").open("xb", buffering=0)
        proc, fd, item = None, None, None
        trigger = None
        try:
            birth = clock()
            proc = subprocess.Popen(
                [sys.executable, SCRIPT, "role", "worker", str(directory),
                 str(child.fileno()), json.dumps(job)],
                stdout=output, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                pass_fds=(child.fileno(),), start_new_session=True, cwd=REPO,
            )
            fd = pidfd_open(proc.pid)
            item = dict(identity(proc.pid), role="worker_child", birth_lower=birth,
                        spawn_before_ns=birth, spawn_after_ns=clock(), inferred=True,
                        attempt=intent["attempt"])
            client.rpc("identity", item=item, intent=intent["id"])
            await_ancestors(client, item, original["operational_ns"])
            send(here, b"G", deadline=original["operational_ns"])
            while clock() < original["hard_ns"]:
                if select.select([fd], [], [], 0.005)[0]:
                    observed = clock()
                    client.rpc("exited", item=item, at_ns=observed)
                    code = proc.poll()
                    event(root / "static-steps.jsonl", "command_end", index=index,
                          command=command, cwd=str(REPO), start_ns=begin,
                          first_exit_observed_ns=observed, reaped_ns=clock(),
                          returncode=code, output=str(directory / "worker.log"))
                    if code:
                        trigger = client.rpc("stop", reason="static_check_failed", at_ns=observed)
                        return False
                    break
                state = client.rpc("status")
                if state["stop"]:
                    raise RuntimeError("root_stopped")
            else:
                raise RuntimeError("static_hard_tail")
        except BaseException as exc:
            c = clock()
            trigger = client.rpc("stop", reason="static:"+type(exc).__name__, at_ns=c)
            raise
        finally:
            here.close()
            child.close()
            if proc is not None and proc.returncode is None:
                c = trigger["at_ns"] if trigger else clock()
                end = min(c+20*NS, original["hard_ns"]-10*NS,
                          trigger.get("ancestor_end_ns", original["hard_ns"]) if trigger else original["hard_ns"])
                if fd is not None:
                    pidfd_signal(fd, signal.SIGTERM)
                    while not select.select([fd], [], [], 0.005)[0] and clock() < end:
                        if clock() >= end-NS:
                            pidfd_signal(fd, signal.SIGKILL)
                    if item is not None and select.select([fd], [], [], 0)[0]:
                        item.setdefault("first_exit_observed_ns", clock())
                        client.rpc("exited", item=item, at_ns=item["first_exit_observed_ns"])
                try:
                    proc.wait(timeout=max(0, (end-clock())/NS))
                except subprocess.TimeoutExpired:
                    client.rpc("stop", reason="static_unreaped_tail", at_ns=c)
                event(root / "static-steps.jsonl", "cleanup", index=index, c=c,
                      C_end=end, H=original["hard_ns"], item=item,
                      reaped_ns=clock() if proc.returncode is not None else None,
                      group_scan_ns=clock(), group_live=members(item) if item else None,
                      cleanup_within_deadline=clock() <= end and proc.returncode is not None)
            if fd is not None:
                os.close(fd)
            output.close()
    return True

def execute(root, outer=None):
    # v is written by the seal transition, before any validation-only work.
    if outer is None:
        raise RuntimeError("missing_outer_guard")
    root = Path(root)
    transition = read(root / "validation-begin.json")
    v = transition["v"]
    if digest(root / "manifest.json") != transition["manifest_sha256"]:
        raise RuntimeError("sealed_manifest_changed")
    manifest = read(root / "manifest.json")
    if transition["D_val"] != min(v + 900 * NS, manifest["t_new"] + 6000 * NS):
        raise RuntimeError("validation_clock")
    validate_manifest(manifest)
    header = adopt(root, manifest)
    header["init_hard_ns"] = v + 30 * NS
    loaded_environment(root, manifest, "protected_before_authority")
    authority = Authority(root, manifest, header, outer)
    try:
        loaded_environment(root, manifest, "protected_after_authority")
        authority.check()
        authority.notify_outer(dict(event="ready", at_ns=clock()))
    except BaseException as exc:
        authority.stop("post_initialization:"+type(exc).__name__)
        authority.close(min(v+20*NS, authority.stop_value["ancestor_end_ns"]))
        raise
    os.environ.update(
        RUN_SNAPSHOT_AUTH_FD=str(authority.fd),
        RUN_SNAPSHOT_REPAIR_ROOT=str(root),
        PYTHONPATH=os.pathsep.join([str(REPO / "scripts"), str(REPO / "src")]),
        PYTHONDONTWRITEBYTECODE="1",
        HYPOTHESIS_STORAGE_DIRECTORY=str(root / "hypothesis"),
    )
    gap, previous, results = 0, v, []
    close_ok = False
    try:
        for number, entry in enumerate(manifest["collections"], 1):
            s = clock()
            gap += s - previous
            remaining = sum(h for _, h in LIMITS[number - 1:]) * NS
            if (gap > 30 * NS or s + remaining + 60 * NS - gap > header["D_val"]
                    or s + entry["hard_s"] * NS > header["D_work"]
                    or s + entry["hard_s"] * NS + 600 * NS > header["D_total"]):
                authority.stop("collection_budget_refused")
                break
            authority.check()
            full_source_audit(manifest)
            authority.collection, authority.case_position = number, 0
            authority.collection_o = s + entry["operational_s"] * NS
            authority.collection_h = s + entry["hard_s"] * NS
            os.environ["RUN_SNAPSHOT_COLLECTION"] = str(number)
            job = dict(
                stage="G0", start_ns=s, operational_ns=authority.collection_o,
                hard_ns=s + entry["hard_s"] * NS, command=entry["argv"],
                sources=manifest["sources"], budget=str(root / "header.json"),
                batch_directory=str(root), collection=True, cwd=str(REPO),
                fault=None, coordinator_mode="normal",
            )
            result = run_owned(root / f"collection-{number}", job)
            full_source_audit(manifest)
            if clock() > job["hard_ns"]:
                authority.stop("collection_audit_hard_tail")
            results.append(result)
            previous = clock()
            event(root / "validation-ledger.jsonl", "collection_end", number=number,
                  start_ns=s, end_ns=previous, gap_ns=gap)
            save(root / "collection-results.json", results)
            if (not result["success"] or authority.stop_value
                    or authority.case_position != len(entry["cases"])):
                authority.stop("collection_failed")
                break
            authority.collection = None
    except Exception as exc:
        authority.stop("runner:" + type(exc).__name__ + ":" + str(exc))
    finally:
        close_deadline = min(header["D_val"], previous + max(0, 30 * NS - gap))
        if authority.stop_value is not None:
            close_deadline = min(close_deadline, authority.stop_value["ancestor_end_ns"])
        close_ok = authority.close(close_deadline)
        gap += clock() - previous
        event(root / "validation-ledger.jsonl", "validation_end",
              gap_ns=gap, end_ns=clock(), threads_closed=close_ok,
              reserved_outer_tail_ns=30 * NS)
    good = (len(results) == 3 and all(r["success"] for r in results)
            and not authority.stop_value and close_ok and gap <= 60 * NS
            and clock() <= header["D_val"])
    if good:
        save(root / "VALIDATION-COMPLETE.json", dict(at_ns=clock(), state="REVIEWING", authorizes_G1=False))
    elif not (root / "STOP.json").exists():
        save(root / "STOP.json", authority.stop_value or dict(
            reason="final_gate", at_ns=clock()))
    return good


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--static", action="store_true")
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    raise SystemExit(0 if (run_static(args.root) if args.static else execute(args.root)) else 1)
