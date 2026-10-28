"""RUN snapshot A-only diagnostic entry; G0 uses small fixtures, never B.

This repair executable only accepts G0/250 and a live inherited permit.
G1/100k is always refused. No server.start, RUN, INIT or profiler is used.
"""

import argparse
import ast
from collections import Counter
from contextlib import ExitStack, contextmanager
import gc
import inspect
import json
from pathlib import Path
import textwrap
import time
from types import FunctionType
import sys


ORDER = ("N0", "W0", "Wd", "Nd", "W1", "N1")
NS = 10**9


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


class InvalidDiagnostic(ValueError):
    pass


class Observation:
    """Bounded aggregate clocks; invalid survives swallowed product exceptions."""

    def __init__(self, limit=2_000_000, fault=None):
        self.invalid = False
        self.errors = []
        self.entered = Counter()
        self.completed = Counter()
        self.wall = Counter()
        self.cpu = Counter()
        self.exclusive_wall = Counter()
        self.exclusive_cpu = Counter()
        self.branches = Counter()
        self.stack = []
        self.limit = limit
        self.fault = fault
        self.fired = False
        self.shapes = {}
        self.objects_restored = True

    def fail(self, code):
        self.invalid = True
        if len(self.errors) < 16:
            self.errors.append(code)

    def clocks(self, name):
        if self.fault == "clock" and name == "U-hit" and not self.fired:
            self.fired = True
            raise OSError("injected clock read failure")
        return time.monotonic_ns(), time.process_time_ns()

    @contextmanager
    def span(self, name):
        if sum(self.entered.values()) >= self.limit or len(self.stack) >= 128:
            self.fail("interval_capacity")
            raise InvalidDiagnostic("interval_capacity")
        if name not in self.entered and len(self.entered) >= 16:
            self.fail("bucket_capacity")
            raise InvalidDiagnostic("bucket_capacity")
        try:
            wall, cpu = self.clocks(name)
        except Exception:
            self.fail("clock")
            raise
        frame = [name, wall, cpu, 0, 0]
        self.entered[name] += 1
        self.stack.append(frame)
        try:
            if not self.fired and name == "U-hit" and self.fault == "expression":
                self.fired = True
                raise InvalidDiagnostic("injected_expression_failure")
            yield
            self.completed[name] += 1
        except Exception:
            self.fail("interval_exception:" + name)
            raise
        finally:
            try:
                used_wall = time.monotonic_ns() - wall
                used_cpu = time.process_time_ns() - cpu
                if self.fault == "stack" and name == "U-hit" and not self.fired:
                    self.fired = True
                    self.stack[-1] = ["injected stack corruption"]
                if not self.stack or self.stack[-1] is not frame:
                    self.fail("stack")
                    if self.stack:
                        self.stack.pop()
                    raise InvalidDiagnostic("stack")
                self.stack.pop()
                own_wall, own_cpu = used_wall - frame[3], used_cpu - frame[4]
                if min(used_wall, used_cpu, own_wall, own_cpu) < 0:
                    self.fail("negative_interval")
                    raise InvalidDiagnostic("negative_interval")
                self.wall[name] += used_wall
                self.cpu[name] += used_cpu
                self.exclusive_wall[name] += own_wall
                self.exclusive_cpu[name] += own_cpu
                if self.stack:
                    self.stack[-1][3] += used_wall
                    self.stack[-1][4] += used_cpu
            except Exception:
                self.fail("interval_finish")
                raise

    def call(self, name, operation):
        with self.span(name):
            return operation()

    def attribute(self, name, operation, hook):
        # The thunk contains the unchanged expression. No extra bool conversion,
        # key comparison or lookup of a potentially customized dict is added.
        result = self.call(name, operation)
        outcome = "true" if result is True else "false" if result is False else "other"
        shape = self.shapes.get(id(hook), "unknown")
        self.branches[name + ":" + shape + ":" + outcome] += 1
        return result

    def summary(self):
        return {
            name: dict(getattr(self, name))
            for name in (
                "entered",
                "completed",
                "wall",
                "cpu",
                "exclusive_wall",
                "exclusive_cpu",
                "branches",
            )
        } | dict(
            invalid=self.invalid,
            errors=self.errors,
            stack_depth=len(self.stack),
            objects_restored=self.objects_restored,
        )


def _thunk(expression):
    return ast.Lambda(
        ast.arguments(
            posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]
        ),
        expression,
    )


def _call(bucket, expression, method="call"):
    return ast.copy_location(
        ast.Call(
            ast.Attribute(
                ast.Name("_snapshot_observer", ast.Load()), method, ast.Load()
            ),
            [ast.Constant(bucket), _thunk(expression)],
            [],
        ),
        expression,
    )


def _block(bucket, body):
    context = ast.Call(
        ast.Attribute(ast.Name("_snapshot_observer", ast.Load()), "span", ast.Load()),
        [ast.Constant(bucket)],
        [],
    )
    return ast.copy_location(ast.With([ast.withitem(context)], body), body[0])


class Instrument(ast.NodeTransformer):
    def __init__(self, role):
        self.role = role

    def visit_Compare(self, node):
        node = self.generic_visit(node)
        if self.role == "registry" and isinstance(node.left, ast.Call):
            f = node.left.func
            if isinstance(f, ast.Attribute) and f.attr == "keys":
                call = _call(
                    "U-hit" if node.lineno < 150 else "U-build", node, "attribute"
                )
                call.args.append(ast.Name("hook", ast.Load()))
                return call
        return node

    def visit_Call(self, node):
        node = self.generic_visit(node)
        if self.role == "registry" and isinstance(node.func, ast.Name):
            if node.func.id == "ensure_synchronous_task":
                return _call("V", node)
        if self.role == "Node" and isinstance(node.func, ast.Attribute):
            if node.func.attr == "by_type":
                return _call("R", node)
        return node

    def visit_FunctionDef(self, node):
        node.decorator_list = []
        node = self.generic_visit(node)
        if self.role == "registry":
            for statement in node.body:
                if isinstance(statement, ast.If) and statement.lineno in (135, 150):
                    statement.body = [
                        _block(
                            "G-hit" if statement.lineno == 135 else "G-build",
                            statement.body,
                        )
                    ]
        else:
            bounds = {
                "Node": [(179, 187), (190, 215)],
                "Task": [(129, 150)],
                "Flow": [(47, 50)],
                "Bunch": [(42, 43)],
            }[self.role]
            for low, high in reversed(bounds):
                indices = [
                    i for i, s in enumerate(node.body) if low <= s.lineno <= high
                ]
                if not indices:
                    raise InvalidDiagnostic("source_region_missing")
                lo, hi = indices[0], indices[-1] + 1
                node.body[lo:hi] = [_block("F", node.body[lo:hi])]
            if self.role == "Bunch":
                node.body = [_block("to_dict", node.body)]
        return node


def instrumented(original, role, observer):
    lines, first = inspect.getsourcelines(original)
    tree = ast.parse(textwrap.dedent("".join(lines)))
    ast.increment_lineno(tree, first - 1)
    function = Instrument(role).visit(tree.body[0])
    namespace = dict(original.__globals__, _snapshot_observer=observer)
    if original.__closure__:
        module = ast.Module(
            [ast.ClassDef("_DiagnosticMethod", [], [], [function], [])], []
        )
    else:
        module = ast.Module([function], [])
    ast.fix_missing_locations(module)
    exec(compile(module, original.__code__.co_filename, "exec"), namespace)
    generated = (
        namespace["_DiagnosticMethod"].__dict__[original.__name__]
        if original.__closure__
        else namespace[original.__name__]
    )
    if generated.__code__.co_freevars != original.__code__.co_freevars:
        raise InvalidDiagnostic("closure_changed")
    result = FunctionType(
        generated.__code__,
        namespace,
        original.__name__,
        original.__defaults__,
        original.__closure__,
    )
    return result, ast.unparse(module)


def fingerprint():
    from takler.core import task_node

    for name in task_node._EXECUTION_HOOKS:
        hook = task_node.Task.__dict__[name]
        if type(hook) is not FunctionType or type(hook.__dict__) is not dict:
            raise InvalidDiagnostic("nonplain_diagnostic_hook")
        if any(type(key) is not str for key in hook.__dict__):
            raise InvalidDiagnostic("nonplain_diagnostic_metadata")
    return (
        id(task_node.ensure_synchronous_task),
        tuple(
            (
                name,
                id(hook),
                id(hook.__code__),
                type(hook.__dict__).__name__,
                tuple((type(key).__name__, str(key)) for key in hook.__dict__),
            )
            for name in task_node._EXECUTION_HOOKS
            for hook in [task_node.Task.__dict__[name]]
        ),
    )


@contextmanager
def installed(observer, directory):
    from takler.core import Task, Flow, Bunch
    from takler.core.node import Node
    from takler.serialization.registry import TypeRegistry
    from takler.server import batch

    originals = []
    try:
        for owner, attribute, role in [
            (TypeRegistry, "_check_execution", "registry"),
            (Node, "to_dict", "Node"),
            (Task, "to_dict", "Task"),
            (Flow, "to_dict", "Flow"),
            (Bunch, "to_dict", "Bunch"),
        ]:
            raw = vars(owner)[attribute]
            original = getattr(owner, attribute)
            replacement, source = instrumented(original, role, observer)
            originals.append((owner, attribute, raw))
            (directory / (role + ".instrumented.py")).write_text(source + "\n")
            setattr(
                owner,
                attribute,
                staticmethod(replacement) if role == "registry" else replacement,
            )
        original_copy = batch.deepcopy
        originals.append((batch, "deepcopy", original_copy))
        batch.deepcopy = lambda value: observer.call(
            "deepcopy", lambda: original_copy(value)
        )
        yield
    finally:
        for owner, attribute, original in reversed(originals):
            try:
                if observer.fault == "restore" and not observer.fired:
                    observer.fired = True
                    raise InvalidDiagnostic("injected_restore_failure")
                setattr(owner, attribute, original)
            except Exception:
                observer.fail("restore")
                # Controlled restore-failure tests still return all product
                # objects to their original state, with invalid left sticky.
                try:
                    setattr(owner, attribute, original)
                except Exception:
                    observer.objects_restored = False
        if any(
            vars(owner)[attribute] is not original
            for owner, attribute, original in originals
        ):
            observer.fail("restore")
            observer.objects_restored = False


def structure(value, tasks, flows, paths):
    from takler.core.state import NodeStatus

    if not isinstance(value, dict):
        raise InvalidDiagnostic("snapshot_unavailable")
    if (
        not {"name", "state", "type_id", "flows", "server_config", "server_state"}
        <= value.keys()
    ):
        raise InvalidDiagnostic("root_fields")
    if len(value["flows"]) != flows:
        raise InvalidDiagnostic("flow_count")
    seen, meters = set(), set()
    flow_names = set()
    for flow in value["flows"]:
        if flow["name"] in flow_names:
            raise InvalidDiagnostic("duplicate_flow")
        flow_names.add(flow["name"])
        if (
            not {"calendar", "begun", "user_parameters", "children", "state", "type_id"}
            <= flow.keys()
        ):
            raise InvalidDiagnostic("flow_fields")
        if not flow["begun"] or len(flow["user_parameters"]) != 1:
            raise InvalidDiagnostic("fixture_flow")
        for node in flow["children"]:
            path = "/" + flow["name"] + "/" + node["name"]
            if (
                path in seen
                or not {"state", "type_id", "task_id", "try_no", "aborted_reason"}
                <= node.keys()
            ):
                raise InvalidDiagnostic("task_fields_or_duplicate")
            seen.add(path)
            if (
                node["try_no"] != 0
                or node["task_id"] is not None
                or node["state"]
                != {"status": NodeStatus.complete.value, "suspended": False}
            ):
                raise InvalidDiagnostic("fixture_task")
            if "meters" in node:
                if node["meters"] != [
                    dict(name="progress", min_value=0, max_value=100, value=0)
                ]:
                    raise InvalidDiagnostic("fixture_meter")
                meters.add(path)
    expected = {f"/flow{i // 1000:03d}/task{i % 1000:04d}" for i in range(tasks)}
    if (
        seen != expected
        or flow_names != {f"flow{i:03d}" for i in range(flows)}
        or meters != set(paths)
    ):
        raise InvalidDiagnostic("node_or_meter_count")


def validate_observation(observer, tasks, nodes):
    expected = {
        "snapshot": 1,
        "to_dict": 1,
        "deepcopy": 1,
        "R": nodes,
        "V": 1,
        "G-hit": tasks - 1,
        "G-build": 1,
        "U-hit": (tasks - 1) * 4,
        "U-build": 4,
    }
    for name, count in expected.items():
        if observer.entered[name] != count or observer.completed[name] != count:
            observer.fail("count:" + name)
    if observer.stack or observer.invalid:
        raise InvalidDiagnostic("invalid_observation")
    # Every interval is partitioned only by its direct children. No recursive
    # inclusive total is added to an ancestor or to its own U subregions.
    closure = {}
    for clock in ("wall", "cpu"):
        own = getattr(observer, "exclusive_" + clock)
        inclusive = getattr(observer, clock)
        if sum(own.values()) != inclusive["snapshot"]:
            observer.fail("closure:" + clock)
            raise InvalidDiagnostic("closure")
        u = inclusive["U-hit"] + inclusive["U-build"]
        k = own["G-hit"] + own["G-build"]
        rest = own["R"] + inclusive["V"] + inclusive["F"] + own["to_dict"]
        if u + k + rest != inclusive["to_dict"]:
            observer.fail("to_dict_closure:" + clock)
            raise InvalidDiagnostic("to_dict_closure")
        closure[clock] = dict(
            U=u,
            K=k,
            V=inclusive["V"],
            R=own["R"],
            F=inclusive["F"],
            T=own["to_dict"],
            deepcopy=inclusive["deepcopy"],
            X=own["snapshot"],
            to_dict=inclusive["to_dict"],
            snapshot=inclusive["snapshot"],
        )
    return closure


def diagnose(directory, config, *, fault=None, admission=None):
    """One actual entry, with a G0-only size/scope guard before any fixture."""
    from takler.server import batch
    from takler.core import Task
    from mix100k01 import fixture
    from mix100k_run_timing import timed_run_chain
    from takler.server import TaklerServer
    from takler.server.connect_config import (
        Address,
        ConnectConfig,
        SecuritySettings,
        Server,
    )

    directory = Path(directory)
    if config.get("stage") != "G0" or config.get("tasks") != 250:
        raise InvalidDiagnostic("G0_only_no_100k_authorization")
    if (directory / "attempt.json").exists():
        raise FileExistsError("diagnostic_already_attempted")
    from run_snapshot_control import diagnostic_permit, diagnostic_closed

    ticket = diagnostic_permit(directory, config, fault)
    directory.mkdir(parents=True, exist_ok=True)
    small = True
    # Exclusive attempt registration is never removed, even when setup fails.
    with (directory / "attempt.json").open("x") as target:
        json.dump(
            dict(
                start_ns=time.monotonic_ns(),
                scope="G0-small-validation" if small else "G1",
                config=config,
            ),
            target,
        )
    save(directory / "config.json", config)
    result = dict(
        success=False,
        validation_complete=False,
        stage=config["stage"],
        candidate_selection_allowed=False,
        samples=[],
        error=None,
        network_service_starts=0,
        restored=False,
    )
    before_all = fingerprint()
    initial_gc = dict(enabled=gc.isenabled(), threshold=gc.get_threshold())
    result["initial_gc"] = initial_gc
    originals = [
        (owner, key, raw) for owner in [Task] for key, raw in vars(owner).copy().items()
    ]
    reference = None
    try:
        server = TaklerServer(
            host="127.0.0.1",
            port=0,
            connect_config=ConnectConfig(
                server=Server(
                    address=Address(hostname="127.0.0.1", ip="127.0.0.1", port="0")
                ),
                security=SecuritySettings(auth_mode="enabled"),
            ),
            checkpoint_file=directory / "unused.check",
            checkpoint_interval=10,
        )
        bunch, paths = fixture(config["tasks"])
        for flow in bunch.flows.values():
            server.bunch.add_flow(flow)
        if server.checkpoint_manager.restore():
            raise InvalidDiagnostic("unexpected_restore")
        scheduler = server.scheduler
        scheduler.last_checkpoint_at = server.checkpoint_manager.last_checkpoint_at
        scheduler.restore_summary = server.checkpoint_manager.restore_summary
        scheduler.version_store.rebuild(state_version=scheduler.query_changes.revision)
        bunch = server.bunch
        nodes = config["tasks"] + len(bunch.flows) + 1
        if (
            not scheduler.version_store.ready
            or len(scheduler.version_store.pin().index) != nodes
        ):
            raise InvalidDiagnostic("fixed_root")
        result["fixture"] = dict(
            tasks=config["tasks"],
            flows=len(bunch.flows),
            nodes=nodes,
            pool=len(paths),
            paths=paths,
            state_version=scheduler.version_store.pin().state_version,
            source=__import__("takler").__file__,
            grpc_started=server.grpc_transport.grpc_server is not None,
            checkpoint_task=server.checkpoint_manager._snapshot_task is not None,
        )
        for label in ORDER:
            row = dict(label=label, started=True, validation_complete=False)
            result["samples"].append(row)
            observer = Observation(
                config.get("interval_limit", 2_000_000),
                fault if label.endswith("d") else None,
            )
            sample_dir = directory / label
            sample_dir.mkdir()
            with ExitStack() as stack:
                if label.endswith("d"):
                    stack.enter_context(installed(observer, sample_dir))
                if label.startswith("W"):
                    stack.enter_context(
                        timed_run_chain(scheduler, lambda *a, **kw: None)
                    )
                before = fingerprint()
                row["fingerprint"] = before
                observer.shapes = {
                    item[1]: "empty"
                    if not item[-1]
                    else "singleton"
                    if item[-1] == (("str", "__wrapped__"),)
                    else "other"
                    for item in before[1]
                }
                wall, cpu = time.monotonic_ns(), time.process_time_ns()
                if label.endswith("d"):
                    value = observer.call(
                        "snapshot", lambda: batch._snapshot(scheduler)
                    )
                else:
                    value = batch._snapshot(scheduler)
                row.update(
                    wall_ns=time.monotonic_ns() - wall,
                    cpu_ns=time.process_time_ns() - cpu,
                )
                if fault == "fingerprint" and label == "N0":
                    Task.do_run.__dict__["_g0_fault"] = True
                if fingerprint() != before:
                    observer.fail("fingerprint")
                if fault == "deepcopy" and label == "N0":
                    # Real deepcopy exception injection is installed by tests;
                    # this name alone must not synthesize a successful result.
                    raise InvalidDiagnostic("external_fault_required")
            if fault == "none" and label == "N0":
                value = None
            if fault == "structure" and label == "N0":
                value["flows"][0]["children"].pop()
            row["observation"] = observer.summary()
            if observer.invalid:
                raise InvalidDiagnostic("invalid_observation")
            structure(value, config["tasks"], len(bunch.flows), paths)
            if reference is None:
                reference = value
            elif value != reference:
                raise InvalidDiagnostic("snapshot_unequal")
            if label.endswith("d"):
                row["closure"] = validate_observation(observer, config["tasks"], nodes)
            row["validation_complete"] = True
            row["finished"] = True
            del value
        result["success"] = result["validation_complete"] = True
    except Exception as exc:
        if result["samples"]:
            observer.fail("entry_validation")
            result["samples"][-1]["observation"] = observer.summary()
        result["error"] = type(exc).__name__ + (
            ":" + str(exc)[:160] if type(exc) is InvalidDiagnostic else ""
        )
    finally:
        for owner, key, raw in originals:
            if key in ("run", "before_run", "do_run", "after_run"):
                raw.__dict__.pop("_g0_fault", None)
        result["restored"] = fingerprint() == before_all and all(
            row.get("observation", {}).get("objects_restored", True)
            for row in result["samples"]
        )
        result["final_gc"] = dict(enabled=gc.isenabled(), threshold=gc.get_threshold())
        if result["final_gc"] != initial_gc or not initial_gc["enabled"]:
            result["success"] = result["validation_complete"] = False
            result["error"] = "gc_changed"
        if not result["restored"]:
            result["success"] = result["validation_complete"] = False
            result["error"] = "restore"
        result["end_ns"] = time.monotonic_ns()
        save(directory / "result.json", result)
        diagnostic_closed(ticket, result)
    return result


@contextmanager
def cli_service_fence():
    from takler.server import TaklerServer
    from takler.server.grpc_transport import GrpcTransport
    from takler.server.checkpoint import CheckpointManager
    from takler.server.scheduler import Scheduler

    owners = (TaklerServer, GrpcTransport, CheckpointManager, Scheduler)
    originals = [(owner, owner.start) for owner in owners]

    async def forbidden(*args, **kwargs):
        raise AssertionError("G0 diagnostic forbids service startup")

    try:
        for owner in owners:
            owner.start = forbidden
        print(json.dumps(dict(event="service_fences_installed",
                              owners=[owner.__module__+"."+owner.__qualname__ for owner in owners])), flush=True)
        yield
    finally:
        for owner, original in originals:
            owner.start = original


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("config", type=Path)
    parser.add_argument("--admission", type=Path)
    arguments = parser.parse_args()
    try:
        with cli_service_fence():
            outcome = diagnose(
                arguments.directory,
                json.loads(arguments.config.read_text()),
                admission=json.loads(arguments.admission.read_text())
                if arguments.admission
                else None,
            )
        print(
            json.dumps(
                dict(
                    stage=outcome["stage"],
                    success=outcome["success"],
                    validation_complete=outcome["validation_complete"],
                    candidate_selection_allowed=False,
                )
            )
        )
        sys.exit(0 if outcome["success"] else 1)
    finally:
        import os
        import run_snapshot_control as control

        key = (os.getpid(), os.environ.get("RUN_SNAPSHOT_AUTH_FD"))
        client = control._CLIENTS.get(key)
        if client is not None:
            client.rpc("channel_closing")


if __name__ == "__main__":
    main()
