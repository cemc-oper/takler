import pytest

from takler.core import Bunch, Flow
from takler.core.state import NodeStatus


@pytest.mark.parametrize("partial", [False, True])
@pytest.mark.parametrize("parent", [False, True])
@pytest.mark.parametrize("path", [None, "/f", "/missing"])
def test_missing_reference_blocks_without_partial_reservation(partial, parent, path):
    flow = Flow("f")
    task = flow.add_task("t")
    limit = task.add_limit("available", 3)
    if partial:
        task.add_in_limit("available")
    owner = flow if parent else task
    owner.add_in_limit("missing", node_path=path)
    flow.begin()
    assert not task.check_dependencies()
    assert not task.resolve_dependencies()
    errors = flow.validate_limit_references()
    assert len(errors) == 1
    assert owner.node_path in errors[0]
    assert "missing" in errors[0]
    assert (path or "self/ancestors") in errors[0]
    task.increment_in_limit(set())
    assert limit.value == 0
    assert limit.node_paths == set()
    assert task.try_no == 0


@pytest.mark.parametrize("path", [None, "/f", "."])
def test_late_definition_resolves_and_capacity_still_applies(path):
    flow = Flow("f")
    task = flow.add_task("t")
    task.add_in_limit("slots", node_path=path, tokens=2)
    assert flow.validate_limit_references()
    limit = flow.add_limit("slots", 2)
    flow.begin()
    assert flow.validate_limit_references() == []
    assert task.check_dependencies()
    task.run()
    assert limit.value == 2
    assert limit.node_paths == {task.node_path}
    task.init()
    assert limit.value == 2
    task.complete()
    assert limit.value == 0


def test_cross_flow_and_bunch_validation():
    bunch = Bunch()
    flow = bunch.add_flow("f")
    task = flow.add_task("t")
    task.add_in_limit("slots", node_path="/resources")
    assert len(bunch.validate_limit_references()) == 1
    limit = bunch.add_flow("resources").add_limit("slots", 1)
    assert bunch.validate_limit_references() == []
    assert limit.value == 0


@pytest.mark.parametrize("entry", ["init", "force"])
@pytest.mark.parametrize(
    "exit_status", [NodeStatus.complete, NodeStatus.aborted, NodeStatus.queued]
)
def test_direct_active_reserves_once_and_releases(entry, exit_status):
    flow = Flow("f")
    task = flow.add_task("t")
    limit = flow.add_limit("slots", 2)
    task.add_in_limit("slots", tokens=2)
    flow.begin()
    if entry == "init":
        task.init()
        task.init()
    else:
        task.set_node_status(NodeStatus.active)
        task.set_node_status(NodeStatus.active)
    assert limit.value == 2
    assert limit.node_paths == {task.node_path}
    task.set_node_status(exit_status)
    assert limit.value == 0
    assert limit.node_paths == set()


def test_equal_named_limits_at_different_nodes_are_both_accounted():
    flow = Flow("f")
    task = flow.add_task("t")
    first = flow.add_limit("slots", 3)
    second = task.add_limit("slots", 3)
    flow.add_in_limit("slots")
    task.add_in_limit("slots")
    flow.begin()
    task.run()
    assert first.value == second.value == 1
    task.complete()
    assert first.value == second.value == 0
