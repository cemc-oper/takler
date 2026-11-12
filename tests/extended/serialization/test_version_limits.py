"""Version publication under a query journal overflow."""

from takler.core import Bunch, Flow, NodeStatus
from takler.server.scheduler import Scheduler


def test_query_journal_overflow_does_not_drop_version_changes():
    bunch = Bunch()
    flow = bunch.add_flow(Flow("f"))
    tasks = [flow.add_task(f"t{index:04d}") for index in range(2100)]
    scheduler = Scheduler(bunch)
    before = scheduler.version_store.pin()
    scheduler.query_changes.begin()
    for task in tasks:
        task.set_node_status_only(NodeStatus.complete)
    scheduler.query_changes.end()
    scheduler.version_store.flush()
    after = scheduler.version_store.pin()
    assert scheduler.query_changes.since(before.state_version)[0].changes[0].path == "/"
    assert before.public_node("/f/t2099")["state"]["status"] == NodeStatus.unknown.value
    assert after.public_node("/f/t0000")["state"]["status"] == NodeStatus.complete.value
    assert after.public_node("/f/t2099")["state"]["status"] == NodeStatus.complete.value
