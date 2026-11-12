from takler.core.state import State, NodeStatus, SerializationType


def test_state_to_dict():
    node_state = State(NodeStatus.complete)
    assert node_state.to_dict() == dict(
        status=NodeStatus.complete.value,
        suspended=False,
    )

    node_state.node_status = NodeStatus.queued
    node_state.suspended = True
    assert node_state.to_dict() == dict(
        status=NodeStatus.queued.value,
        suspended=True,
    )
    assert node_state.to_dict(method=SerializationType.Status) == dict(
        status=NodeStatus.queued.value,
        suspended=True,
    )
    assert node_state.to_dict(method=SerializationType.Tree) == dict(
        status=NodeStatus.queued.value,
        suspended=True,
    )


def test_state_from_dict():
    node_state_dict = dict(
        status=NodeStatus.complete.value,
        suspended=False,
    )

    node_state = State.from_dict(node_state_dict)
    assert node_state.node_status == NodeStatus.complete
    assert node_state.suspended is False
