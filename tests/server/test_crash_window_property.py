"""The chosen checkpoint, never later volatile actions, defines recovery."""

import json
from uuid import uuid4

from hypothesis import HealthCheck, given, settings, strategies as st

from takler.core import Bunch, Flow, NodeStatus
from takler.server.checkpoint import CheckpointManager


@settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    st.lists(
        st.tuples(
            st.sampled_from(("run", "init", "complete", "requeue")),
            st.booleans(),
        ),
        min_size=1,
        max_size=8,
    ),
    st.booleans(),
)
def test_recovery_equals_selected_snapshot_after_random_crash_window(
    tmp_path, steps, corrupt_primary
):
    checkpoint = tmp_path / str(uuid4()) / "state.check"
    source = Bunch(host="127.0.0.1", port="33083")
    flow = source.add_flow(Flow("flow"))
    task = flow.add_task("task")
    flow.begin()
    manager = CheckpointManager(source, checkpoint_file=checkpoint)
    assert manager.write_checkpoint()

    for action, save_after in steps:
        status = task.state.node_status
        if action == "run" and status is NodeStatus.queued:
            task.run()
        elif action == "init" and status is NodeStatus.submitted:
            task.init("rid-1")
        elif action == "complete" and status is NodeStatus.active:
            task.complete()
        elif action == "requeue":
            task.requeue()
        if save_after:
            assert manager.write_checkpoint()

    selected = checkpoint
    if corrupt_primary and manager.backup_file.exists():
        selected = manager.backup_file
        checkpoint.write_text("{corrupt", encoding="utf-8")
    persisted = json.loads(selected.read_text(encoding="utf-8"))
    expected = persisted["runtime"]["nodes"]["/flow/task"]
    secret = persisted["secrets"]["job_passwords"].get("/flow/task")

    restored = Bunch(host="127.0.0.1", port="33083")
    recovery = CheckpointManager(restored, checkpoint_file=checkpoint)
    assert recovery.restore()
    current = restored.find_node("/flow/task")
    assert current.state.node_status.value == expected["state"]["status"]
    for field in (
        "try_no",
        "task_id",
        "attempt_id",
        "submitted_at",
        "started_at",
        "ended_at",
    ):
        assert getattr(current, field) == expected.get(field)
    assert current.job_password == (None if secret is None else secret["password"])
    assert restored.server_state.status.value == "halted"
    assert recovery.restore_summary["source"] == (
        "primary" if selected == checkpoint else "backup"
    )
