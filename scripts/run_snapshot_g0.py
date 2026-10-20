"""G0 repair authority, single-use validation runner and first-failure plugin.

Only inherited Unix descriptors grant access. A JSON header is never a permit.
"""

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

from run_snapshot_control import NS, Client, event, identity, read, run_owned, save

REPO = Path(__file__).resolve().parents[1]
PLAN_HASH = "0082a8e838e41fb6e24f2c33064f7f7236877733764286bdec91be3805bb60c8"
AUTHORIZATION = "G0-TOOL-REPAIR-V2-01"
FILES = [
    "scripts/run_snapshot_control.py",
    "scripts/run_snapshot_g0.py",
    "scripts/run_snapshot_diagnostic.py",
    "tests/scripts/test_run_snapshot_control.py",
    "tests/scripts/test_run_snapshot_diagnostic.py",
]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def clock():
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


class Authority:
    """One existing startup process; authority and disk work use a thread.

    Deadline/sampling threads are independent of RPC persistence. Each child
    receives its own inherited descriptor, never a filesystem socket address.
    """

    def __init__(self, root, manifest):
        self.root = Path(root).resolve()
        self.manifest = manifest
        self.header = read(self.root / "header.json")
        self.lease = (self.root / "authority.lock").open("a+")
        fcntl.flock(self.lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with (self.root / "authority-start.json").open("x") as stream:
            json.dump(dict(identity(os.getpid()), at_ns=clock()), stream)
        self.selector = selectors.DefaultSelector()
        self.channels = {}
        self.nodes = {}
        self.used_paths = set()
        self.identities = {}
        self.serial = 0
        self.current_collection = None
        self.current_case = None
        self.stop_value = None
        self.done = False
        self.source_hashes = manifest["sources"]
        self.monitor = None
        self.events = queue.Queue(maxsize=10000)
        self.thread = threading.Thread(target=self.loop, name="root-authority")
        self.thread.start()
        self.fd = self.channel("startup", None)
        self.client = Client(self.fd)

    def channel(self, role, parent):
        server, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
        server.setblocking(False)
        self.channels[server.fileno()] = dict(
            socket=server, role=role, parent=parent, pid=None, sequence=0
        )
        self.selector.register(server, selectors.EVENT_READ)
        return child.detach()

    def record(self, kind, **data):
        self.events.put_nowait(dict(kind=kind, at_ns=clock(), **data))

    def stop(self, reason, at_ns=None):
        if self.stop_value is None:
            self.stop_value = dict(reason=reason, at_ns=at_ns or clock())
            self.record("ROOT_STOP", **self.stop_value)
        return self.stop_value

    def persist(self):
        save(
            self.root / "attempt-index.json",
            dict(nodes=self.nodes, stop=self.stop_value),
        )
        while True:
            try:
                row = self.events.get_nowait()
            except queue.Empty:
                break
            with (self.root / "authority.jsonl").open("a") as stream:
                stream.write(json.dumps(row) + "\n")
        if self.stop_value and not (self.root / "STOP.json").exists():
            with (self.root / "STOP.json").open("x") as stream:
                json.dump(self.stop_value, stream)

    def allowed(self, channel):
        if self.stop_value:
            raise RuntimeError("root_stopped")
        if clock() >= self.header["D_work"]:
            self.stop("work_deadline")
            raise RuntimeError("work_deadline")
        if channel["role"] != "startup" and not self.current_collection:
            raise RuntimeError("no_active_parent")

    def canonical(self, raw):
        path = Path(raw)
        if path != path.resolve() or not path.is_relative_to(self.root):
            raise RuntimeError("wrong_root_or_alias")
        if any(p.is_symlink() for p in [path, *path.parents]):
            raise RuntimeError("wrong_root_or_alias")
        return str(path)

    def call(self, ch, request):
        op = request["op"]
        data = request.get("data", {})
        self.record("request", op=op, pid=ch["pid"], data=data)
        if op == "hello":
            return self.header
        if op == "status":
            return dict(stop=self.stop_value, identities=list(self.identities.values()))
        if op == "stop":
            return self.stop(data["reason"], data.get("at_ns"))
        if op == "identity":
            item = data["item"]
            actual = identity(item["pid"])
            if actual["ticks"] != item["ticks"]:
                raise RuntimeError("registration_identity")
            if actual["ppid"] != ch["pid"]:
                raise RuntimeError("registration_parent")
            for previous in self.identities.values():
                try:
                    alive = identity(previous["pid"])
                    if alive["ticks"] != previous["ticks"] or alive["state"] == "Z":
                        previous["exited"] = clock()
                except (FileNotFoundError, ProcessLookupError):
                    previous["exited"] = clock()
            if len([i for i in self.identities.values() if not i.get("exited")]) >= 64:
                raise RuntimeError("identity_capacity")
            self.identities[item["pid"]] = item
            if self.monitor:
                self.monitor.register(item)
            return item
        if op == "exited":
            item = self.identities.get(data["pid"])
            if item:
                item["exited"] = data["first_exit_observed_ns"]
            return True
        if op == "close":
            node = self.nodes[data["id"]]
            if node["owner"] != ch["pid"]:
                raise RuntimeError("ticket_owner")
            node.update(state="CLOSED", result=data["result"], end_ns=clock())
            if data["result"].get("unexpected"):
                self.stop("unexpected_child_result")
            self.persist()
            return True
        self.allowed(ch)
        if op == "channel":
            if data["role"] not in {"S", "W", "C", "worker"}:
                raise RuntimeError("role")
            return dict(fd=self.channel(data["role"], ch["pid"]))
        if op == "begin_case":
            if self.current_case is not None:
                raise RuntimeError("unclosed_case")
            nodeid = data["nodeid"]
            files = self.manifest["collections"][self.current_collection - 1]["files"]
            if nodeid.split("::")[0] not in files:
                raise RuntimeError("selector_not_frozen")
            self.current_case = nodeid
            return dict(case=nodeid)
        if op == "end_case":
            if self.current_case != data["nodeid"]:
                raise RuntimeError("case_identity")
            if any(n["state"] == "PENDING" and not n["data"].get("collection") for n in self.nodes.values()):
                self.stop("unclosed_case_attempt")
                raise RuntimeError("unclosed_case_attempt")
            self.current_case = None
            return True
        if op in {"attempt", "diagnose"}:
            if ch["role"] != "startup" and self.current_case is None:
                raise RuntimeError("no_active_case")
            path = self.canonical(data["directory"])
            if path in self.used_paths:
                raise RuntimeError("attempt_already_used")
            view = data.get("view", {})
            if view:
                # Views only test refusal; they never mint execution permission.
                permitted = self.current_case and "test_v01" in self.current_case
                if not permitted:
                    raise RuntimeError("view_not_declared")
                raise RuntimeError("view_refused")
            pending = [
                n for n in self.nodes.values()
                if n["state"] == "PENDING" and n["kind"] == op and not n["data"].get("collection")
            ]
            if pending:
                raise RuntimeError("unclosed_sibling_attempt")
            for name, value in self.source_hashes.items():
                if digest(name) != value:
                    self.stop("source_changed")
                    raise RuntimeError("source_changed")
            self.serial += 1
            key = str(self.serial)
            self.nodes[key] = dict(
                kind=op, directory=path, owner=ch["pid"], case=self.current_case,
                parent_collection=self.current_collection, state="PENDING",
                start_ns=clock(), data=data,
            )
            self.used_paths.add(path)
            self.persist()
            return dict(id=key, header=self.header)
        raise RuntimeError("unknown_request")

    def loop(self):
        while not self.done:
            for key, _ in self.selector.select(0.01):
                ch = self.channels[key.fd]
                try:
                    raw, ancillary, _, _ = ch["socket"].recvmsg(65536, 1024)
                    if not raw:
                        self.selector.unregister(ch["socket"])
                        ch["socket"].close()
                        continue
                    credentials = next(
                        struct.unpack("3i", value)
                        for level, kind, value in ancillary
                        if level == socket.SOL_SOCKET and kind == socket.SCM_CREDENTIALS
                    )
                    pid = credentials[0]
                    request = json.loads(raw)
                    if ch["pid"] is None:
                        if ch["parent"] is not None and identity(pid)["ppid"] != ch["parent"]:
                            raise RuntimeError("channel_parent_identity")
                        ch["pid"] = pid
                    if ch["pid"] != pid or request["seq"] != ch["sequence"] + 1:
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
                        descriptors = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [descriptor]))]
                        response["result"]["has_fd"] = True
                    ch["socket"].sendmsg([json.dumps(response).encode()], descriptors)
                    if descriptors:
                        os.close(descriptor)
                except Exception as exc:
                    self.stop("authority_protocol:" + type(exc).__name__)
            try:
                self.persist()
            except Exception:
                self.stop_value = self.stop_value or dict(reason="authority_persistence", at_ns=clock())

    def close(self):
        self.done = True
        self.thread.join(0.1)
        if self.thread.is_alive():
            self.stop("authority_thread_tail")
        self.persist()
        for ch in self.channels.values():
            ch["socket"].close()
        self.lease.close()


def adopt(root):
    root = Path(root)
    bootstrap = read(root / "bootstrap.json")
    channel = read(root / "bootstrap-channel.json")
    first, record, end = [json.loads(s) for s in channel["output"].splitlines()]
    if (
        channel["exit_code"] != 0 or record != bootstrap
        or first["t_r"] != bootstrap["t_r"] or end["event"] != "bootstrap_end"
        or bootstrap["authorization_id"] != AUTHORIZATION
        or bootstrap["root"] != str(root.resolve()) or root != root.resolve()
        or bootstrap["clock"] != "CLOCK_MONOTONIC" or bootstrap["unit"] != "ns"
        or bootstrap["boot_id"] != Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    ):
        raise RuntimeError("bootstrap_binding")
    stat = root.stat()
    swap = {s.split(":")[0]: int(s.split()[1]) * 1024 for s in bootstrap["swap_raw"]}
    if len(swap) != 2 or swap["SwapTotal"] < swap["SwapFree"] or swap["SwapFree"] < 0:
        raise RuntimeError("swap_baseline")
    header = dict(
        authorization_id=AUTHORIZATION, authorization_hash=digest(root / "authorization.txt"),
        plan_hash=PLAN_HASH, scope="tool-remediation-only", t_r=bootstrap["t_r"],
        D_work=bootstrap["t_r"] + 3000 * NS, D_total=bootstrap["t_r"] + 3600 * NS,
        swap_start=swap["SwapTotal"] - swap["SwapFree"], boot_id=bootstrap["boot_id"],
        device=stat.st_dev, inode=stat.st_ino, root=str(root),
    )
    with (root / "header.json").open("x") as stream:
        json.dump(header, stream, indent=2)
    return header


def plugin_client():
    return Client.current()


def pytest_sessionstart(session):
    plugin_client().rpc("hello")
    event(Path(os.environ["RUN_SNAPSHOT_REPAIR_ROOT"]) / "pytest-events.jsonl", "session_start")


def pytest_collection_modifyitems(session, config, items):
    root = Path(os.environ["RUN_SNAPSHOT_REPAIR_ROOT"])
    manifest = read(root / "manifest.json")
    number = int(os.environ["RUN_SNAPSHOT_COLLECTION"])
    selected = manifest["collections"][number - 1]
    actual = [item.nodeid for item in items]
    if len(actual) != selected["expected_count"]:
        plugin_client().rpc("stop", reason="collection_mismatch")
        raise RuntimeError("collection_mismatch")
    save(root / ("collection-" + str(number) + "-nodeids.json"), actual)


def pytest_runtest_setup(item):
    plugin_client().rpc("begin_case", nodeid=item.nodeid)


def pytest_runtest_logreport(report):
    if report.failed or report.skipped or hasattr(report, "wasxfail"):
        plugin_client().rpc("stop", reason="pytest_" + report.when + "_failure")
    if report.when == "teardown":
        plugin_client().rpc("end_case", nodeid=report.nodeid)


def pytest_collectreport(report):
    if report.failed:
        plugin_client().rpc("stop", reason="pytest_collection_failure")


def pytest_internalerror(excrepr, excinfo):
    plugin_client().rpc("stop", reason="pytest_internal_error")


def execute(root):
    root = Path(root).resolve()
    header = adopt(root)
    manifest = read(root / "manifest.json")
    authority = Authority(root, manifest)
    os.environ["RUN_SNAPSHOT_AUTH_FD"] = str(authority.fd)
    os.environ["RUN_SNAPSHOT_REPAIR_ROOT"] = str(root)
    os.environ["PYTHONPATH"] = os.pathsep.join([str(REPO / "scripts"), str(REPO / "src")])
    v = clock()
    dval = min(v + 900 * NS, header["t_r"] + 2400 * NS)
    save(root / "validation-begin.json", dict(v=v, D_val=dval))
    gap = 0
    previous = v
    results = []
    try:
        for number, entry in enumerate(manifest["collections"], 1):
            s = clock()
            gap += s - previous
            remaining = sum(c["hard_s"] for c in manifest["collections"][number - 1:]) * NS
            if (
                gap > 60 * NS or s + remaining + 60 * NS - gap > dval
                or s + entry["hard_s"] * NS > header["D_work"]
                or s + entry["hard_s"] * NS + 600 * NS > header["D_total"]
                or authority.stop_value
            ):
                authority.stop("collection_budget_refused")
                break
            authority.current_collection = number
            os.environ["RUN_SNAPSHOT_COLLECTION"] = str(number)
            directory = root / ("collection-" + str(number))
            job = dict(
                stage="G0", start_ns=s, operational_ns=s + entry["operational_s"] * NS,
                hard_ns=s + entry["hard_s"] * NS, command=entry["argv"],
                sources=manifest["sources"], budget=str(root / "header.json"),
                batch_directory=str(root), collection=True, cwd=str(REPO),
            )
            result = run_owned(directory, job)
            results.append(result)
            previous = clock()
            save(root / "collection-results.json", results)
            event(root / "validation-ledger.jsonl", "collection_end", number=number, start_ns=s, end_ns=previous, gap_ns=gap)
            if not result["success"] or authority.stop_value:
                authority.stop("collection_failed")
                break
            authority.current_collection = None
        gap += clock() - previous
        event(root / "validation-ledger.jsonl", "validation_end", gap_ns=gap, at_end_ns=clock())
        if len(results) == 3 and all(r["success"] for r in results) and not authority.stop_value and gap <= 60 * NS:
            save(root / "DONE.json", dict(at_ns=clock(), authorizes_G1=False))
    finally:
        authority.close()
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    arguments = parser.parse_args()
    final = execute(arguments.root)
    raise SystemExit(0 if len(final) == 3 and all(r["success"] for r in final) else 1)
