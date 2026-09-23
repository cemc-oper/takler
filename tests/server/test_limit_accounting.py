import asyncio

import pytest

from takler.core.state import NodeStatus
from takler.protocol.commands import InitCommand
from takler.server.connect_config import AuthMode, ZombiePolicy
from takler.server.zombie import ZombieDetector
from tests.server.test_zombie_detector import make_scheduler, TASK_PATH


@pytest.mark.parametrize("recursive", [False, True])
@pytest.mark.parametrize("target", ["/flow1", TASK_PATH])
def test_force_token_accounting(recursive, target):
    scheduler = make_scheduler(None)
    task = scheduler.bunch.find_node(TASK_PATH)
    limit = task.add_limit("slots", 1)
    task.add_in_limit("slots")
    scheduler._force_path(target, "active", recursive)
    expected = 1 if recursive or target == TASK_PATH else 0
    assert limit.value == expected
    scheduler._force_path(target, "complete", recursive)
    assert limit.value == 0
    assert limit.node_paths == set()


def test_adopt_init_without_submission_reserves_once():
    scheduler = make_scheduler(
        ZombieDetector(auth_mode=AuthMode.DISABLED, zombie_policy=ZombiePolicy.ADOPT)
    )
    task = scheduler.bunch.find_node(TASK_PATH)
    limit = task.add_limit("slots", 1)
    task.add_in_limit("slots")
    for _ in range(2):
        asyncio.run(
            scheduler.run_command_init(InitCommand(node_path=TASK_PATH, task_id="job"))
        )
        assert task.state.node_status == NodeStatus.active
        assert limit.value == 1
        assert limit.node_paths == {TASK_PATH}
    task.abort()
    assert limit.value == 0
