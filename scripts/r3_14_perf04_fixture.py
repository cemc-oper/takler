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
    """At most 100 reservation paths per flow; never project the whole tree."""
    from takler.core import NodeStatus

    result = {}
    for flow in bunch.flows.values():
        limit = flow.find_limit("capacity")
        if (
            not limit
            or limit.value != len(limit.node_paths)
            or not 0 <= limit.value <= 100
        ):
            raise ValueError("limit accounting mismatch")
        for path in limit.node_paths:
            node = bunch.find_node(path)
            if node.state.node_status not in (NodeStatus.submitted, NodeStatus.active):
                raise ValueError("stale limit reservation")
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
    """Fail closed against the adopted queued/unbegun LOAD postcondition."""
    if flow.begun or any(t.state.node_status.name != "queued" for t in flow.children):
        raise ValueError("adopted LOAD queued/unbegun postcondition is not supported")
