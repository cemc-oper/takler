"""PERF-04 synthetic fixture and bounded writer-side correctness checks."""

from r3_14_perf04 import STEPS, candidates, prefix


def make_fixture(size, dispersed=False):
    from query_a_matrix_100k import make_tree
    from takler.core import NodeStatus

    bunch, tasks = make_tree(size, False)
    paths, sentinel = candidates(size, dispersed)
    pool = set(paths)
    # Choose exactly 25 bound pool tasks, and fill each flow to exactly 100.
    bound_pool = set(paths[::10])
    bound = set(bound_pool) | {sentinel}
    for flow in bunch.flows.values():
        own = [t for t in tasks if t.parent is flow]
        selected = {t.node_path for t in own} & bound
        selected.update(
            [t.node_path for t in own if t.node_path not in pool | {sentinel}][
                : 100 - len(selected)
            ]
        )
        bound.update(selected)
        flow.add_limit("capacity", 100)
        # Background tasks stay complete; explicitly queued default is required
        # for requeue and for future structural definitions.
        for t in own:
            t.default_node_status = NodeStatus.queued
            if t.node_path in selected:
                t.add_in_limit("capacity", node_path=flow.node_path)
            if t.node_path in pool:
                t.state.node_status = NodeStatus.queued
                t.suspend()
                for name in ("e1", "e2", "e3", "e4"):
                    t.add_event(name)
                t.add_meter("progress", 0, 100)
        if flow.name == "flow009":
            flow.suspend()
    for t in tasks:
        if t.parent.name == "flow008" and t.node_path != sentinel:
            t.add_event("batch_event")
    unknown = bunch.find_node(sentinel)
    unknown.state.node_status = NodeStatus.queued
    unknown.suspend()
    for flow in bunch.flows.values():
        flow.state.node_status = flow.computed_status(immediate=True)
    return bunch, paths, sentinel, sorted(bound)


def child_payload(task, stage):
    operation, value = STEPS[stage]
    data = dict(node_path=task.node_path, attempt_id=task.attempt_id)
    if operation == "init":
        data["task_id"] = "perf04-" + task.attempt_id
    elif operation == "event":
        data.update(event_name=value, source_task_path=task.node_path)
    elif operation == "meter":
        data.update(
            meter_name="progress", meter_value=value, source_task_path=task.node_path
        )
    return operation, data


def task_state(task):
    return dict(
        path=task.node_path,
        status=task.state.node_status.name,
        suspended=task.state.suspended,
        attempt=task.attempt_id,
        generation=task.definition_generation,
        try_no=task.try_no,
        rid=task.task_id,
        unknown=task.submission_unknown_reason,
        events=[task.find_event(f"e{i}").value for i in range(1, 5)]
        if task.find_event("e1")
        else [],
        meter=task.find_meter("progress").value
        if task.find_meter("progress")
        else None,
    )


def assert_stage(task, stage):
    expected = prefix(stage)
    actual = task_state(task)
    for key in ("status", "events", "meter"):
        if actual[key] != expected[key]:
            raise ValueError(f"ineffective child: {task.node_path} stage {stage} {key}")
    if not actual["attempt"] or actual["unknown"] or not actual["suspended"]:
        raise ValueError("invalid pool identity/state")
    return actual


def check_limits(bunch):
    """Full boundary audit: derive reservations from BOTH bindings and state."""
    from takler.core import NodeStatus

    result = {}
    for flow in bunch.flows.values():
        limit = flow.find_limit("capacity")
        expected = {}
        bindings = 0
        for task in flow.children:
            for binding in task.in_limit_manager.in_limit_list:
                bindings += 1
                if (
                    binding.limit_name != "capacity"
                    or binding.node_path != flow.node_path
                ):
                    raise ValueError("unexpected limit binding")
                if binding.tokens != 1:
                    raise ValueError("limit token mismatch")
                if task.state.node_status in (NodeStatus.submitted, NodeStatus.active):
                    expected[task.node_path] = binding.tokens
        if (
            not limit
            or limit.limit != 100
            or bindings != 100
            or limit.node_paths != set(expected)
            or limit.value != sum(expected.values())
            or not 0 <= limit.value <= limit.limit
        ):
            raise ValueError("limit bidirectional accounting mismatch")
        result[flow.node_path] = limit.to_dict()
    return result


def structural_documents():
    from takler.core import Flow, NodeStatus
    from takler.serialization import export_definition

    documents = {}
    for label, name in (
        ("load-A", "flow010"),
        ("load-B", "flow011"),
        ("replace-A", "flow009"),
        ("replace-B", "flow009"),
    ):
        flow = Flow(name)
        flow.add_parameter("FLOW_LABEL", f"batch-{int(name[-3:]):03d}")
        flow.add_parameter("PERF04_GENERATION", label)
        flow.default_node_status = NodeStatus.queued
        flow.add_limit("capacity", 100)
        for i in range(1000):
            task = flow.add_task(f"task{i:04d}")
            task.default_node_status = NodeStatus.queued
            if i < 100:
                task.add_in_limit("capacity", node_path=f"/{name}")
        raw = export_definition(flow).model_dump_json().encode()
        if len(raw) > 4 * 1024 * 1024:
            raise ValueError("structure template exceeds 4 MiB")
        documents[label] = raw
    if sum(map(len, documents.values())) > 16 * 1024 * 1024:
        raise ValueError("structure templates exceed 16 MiB")
    return documents


def require_load_state(flow):
    """Current adopted LOAD semantics; distinct from submission uncertainty."""
    if flow.begun or len(flow.children) != 1000:
        raise ValueError("LOAD shape or begun mismatch")
    for task in flow.children:
        if (
            task.default_node_status.name != "queued"
            or task.state.node_status.name != "unknown"
            or task.attempt_id
            or task.task_id
            or task.job_password
            or task.try_no != 0
            or task.submission_unknown_reason is not None
        ):
            raise ValueError("LOAD initialized or has submission identity")
    if sum(len(t.in_limit_manager.in_limit_list) for t in flow.children) != 100:
        raise ValueError("LOAD binding count mismatch")
    limit = flow.find_limit("capacity")
    if limit.limit != 100 or limit.value or limit.node_paths:
        raise ValueError("LOAD limit occupied")
    return True


async def prepare_pool(scheduler, paths, sentinel, *, record, check=lambda: None):
    """Writer-side setup only. Return credentials for private IPC, never logs."""
    from takler.protocol.commands import Command, REQUEST_TYPE_BY_COMMAND
    from takler.server.handlers import CommandHandlers

    handler = CommandHandlers(scheduler)

    async def call(operation, payload):
        check()
        code = Command(operation)
        reply = await handler.handle(code, REQUEST_TYPE_BY_COMMAND[code](**payload))
        record(
            dict(
                phase="setup", operation=operation, reply=reply.model_dump(mode="json")
            )
        )
        if reply.flag or any(
            r.flag or r.effect != "applied" for r in getattr(reply, "results", [])
        ):
            raise ValueError("setup failed or ineffective")

    async with scheduler.query_state_lock:
        scheduler.query_changes.begin()
        try:
            unknown = scheduler.bunch.find_node(sentinel)
            unknown.run()
            unknown.mark_submission_unknown("PERF-04 synthetic submission sentinel")
        finally:
            scheduler.query_changes.end()
            scheduler.version_store.flush()
    record(dict(phase="setup", operation="unknown_sentinel", state=task_state(unknown)))
    for i, path in enumerate(paths):
        await call("run", dict(node_paths=[path], force=False))
        task = scheduler.bunch.find_node(path)
        for stage in range(i // 25):
            operation, payload = child_payload(task, stage)
            await call(operation, payload)
        assert_stage(task, i // 25)
    check_limits(scheduler.bunch)
    return {
        p: dict(
            attempt=scheduler.bunch.find_node(p).attempt_id,
            password=scheduler.bunch.find_node(p).job_password,
            generation=scheduler.bunch.find_node(p).definition_generation,
        )
        for p in paths
    }
