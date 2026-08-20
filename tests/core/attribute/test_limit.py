import pytest

from takler.core import Limit, InLimit, Flow, Task
from takler.core.limit import InLimitManager

from ..conftest import SimpleFlow

# ------------------
# Limit
# ------------------


def test_limit_create():
    limit_count = 20
    limit_name = "post_limit"
    limit = Limit(limit_name, limit_count)
    assert limit.name == limit_name
    assert limit.limit == limit_count
    assert limit.value == 0
    assert limit.node is None
    assert len(limit.node_paths) == 0


def test_limit_increment():
    limit = Limit("post_limit", 2)
    assert len(limit.node_paths) == 0

    limit.increment(1, "/flow1/task1")
    assert len(limit.node_paths) == 1

    limit.increment(1, "/flow1/task2")
    assert len(limit.node_paths) == 2


def test_limit_decrement():
    limit = Limit("post_limit", 2)
    assert len(limit.node_paths) == 0

    limit.increment(1, "/flow1/task1")
    limit.increment(1, "/flow1/task2")
    assert len(limit.node_paths) == 2

    limit.decrement(1, "/flow1/task1")
    assert len(limit.node_paths) == 1
    limit.decrement(1, "/flow1/task2")
    assert len(limit.node_paths) == 0


def test_limit_in_limit():
    limit = Limit("post_limit", 2)
    assert limit.in_limit(1)
    assert limit.in_limit(2)
    assert not limit.in_limit(3)

    limit.increment(1, "/flow1/task1")
    assert limit.in_limit(1)
    assert not limit.in_limit(2)
    assert not limit.in_limit(3)

    limit.increment(1, "/flow1/task2")
    assert not limit.in_limit(1)
    assert not limit.in_limit(2)
    assert not limit.in_limit(3)


def test_limit_reset():
    limit = Limit("post_limit", 2)
    limit.increment(1, "/flow1/task1")
    limit.increment(1, "/flow1/task2")
    assert limit.value == 2

    limit.reset()
    assert limit.value == 0


# ---------------------
# InLimit
# ---------------------


def test_in_limit_create():
    in_limit = InLimit("post_limit")
    assert in_limit.limit_name == "post_limit"
    assert in_limit.tokens == 1
    assert in_limit.node_path is None
    assert in_limit.limit is None


def test_in_limit_set_limit():
    limit = Limit("post_limit", 2)
    in_limit = InLimit("post_limit")
    in_limit.set_limit(limit)
    assert in_limit.limit == limit


# -------------------------------
# InLimitManager
# -------------------------------


def test_in_limit_manager_create():
    task = Task("task1")
    in_limit_manager = InLimitManager(task)
    assert in_limit_manager.node == task
    assert in_limit_manager.in_limit_list == []


def test_in_limit_manager_add_in_limit():
    task = Task("task1")
    in_limit_manager = InLimitManager(task)
    in_limit = InLimit("limit1")
    in_limit_manager.add_in_limit(in_limit)
    assert in_limit_manager.in_limit_list == [in_limit]

    another_limit = InLimit("limit2")
    in_limit_manager.add_in_limit(another_limit)
    assert in_limit_manager.in_limit_list == [in_limit, another_limit]


def test_in_limit_manager_add_in_limit_duplicate():
    task = Task("task1")
    in_limit_manager = InLimitManager(task)
    in_limit = InLimit("limit1")
    in_limit_manager.add_in_limit(in_limit)
    with pytest.raises(RuntimeError):
        in_limit_manager.add_in_limit(in_limit)


def test_in_limit_manager_has_in_limit():
    task = Task("task1")
    in_limit_manager = InLimitManager(task)
    in_limit = InLimit("limit1", node_path="/flow1/family1")
    in_limit_manager.add_in_limit(in_limit)

    in_limit_with_path = InLimit("limit1", node_path="/flow1/family1")

    assert in_limit_manager.has_in_limit(in_limit_with_path)


def test_in_limit_manager_has_in_limit_with_no_path():
    task = Task("task1")
    in_limit_manager = InLimitManager(task)
    in_limit = InLimit("limit1", node_path=None)
    in_limit_manager.add_in_limit(in_limit)

    in_limit_with_path = InLimit("limit1", node_path=None)

    assert in_limit_manager.has_in_limit(in_limit_with_path)


def test_in_limit_manager_has_in_limit_with_different_path():
    task = Task("task1")
    in_limit_manager = InLimitManager(task)
    in_limit = InLimit("limit1", node_path="/flow/family1")
    in_limit_manager.add_in_limit(in_limit)

    in_limit_with_path = InLimit("limit1", node_path="/flow/family2")

    assert not in_limit_manager.has_in_limit(in_limit_with_path)


def test_in_limit_manager_in_limit():
    limit_one = Limit("limit1", 2)
    limit_two = Limit("limit2", 1)

    in_limit_manager = InLimitManager(Task("task1"))
    in_limit_one = InLimit("limit1")
    in_limit_one.limit = limit_one
    in_limit_manager.add_in_limit(in_limit_one)
    in_limit_two = InLimit("limit2")
    in_limit_two.limit = limit_two
    in_limit_manager.add_in_limit(in_limit_two)

    assert in_limit_manager.in_limit()

    limit_one.increment(1, "/flow1/task1")
    assert in_limit_manager.in_limit()

    limit_one.increment(1, "/flow1/task2")
    assert not in_limit_manager.in_limit()

    limit_two.increment(1, "/flow1/task3")
    assert not in_limit_manager.in_limit()


def test_in_limit_manager_increment_in_limit():
    limit_one = Limit("limit1", 2)
    limit_two = Limit("limit2", 3)

    in_limit_manager = InLimitManager(Task("task1"))
    in_limit_one = InLimit("limit1")
    in_limit_one.set_limit(limit_one)
    in_limit_manager.add_in_limit(in_limit_one)
    in_limit_two = InLimit("limit2")
    in_limit_two.set_limit(limit_two)
    in_limit_manager.add_in_limit(in_limit_two)

    limit_set = set()
    in_limit_manager.increment_in_limit(limit_set, "/flow/task1")
    assert limit_set == {limit_one, limit_two}
    assert limit_one.value == 1
    assert limit_two.value == 1

    in_limit_manager.increment_in_limit(limit_set, "/flow/task1")
    assert limit_set == {limit_one, limit_two}
    assert limit_one.value == 1
    assert limit_two.value == 1

    limit_three = Limit("limit3", 4)
    in_limit_manager_for_task2 = InLimitManager(Task("task2"))
    in_limit_three = InLimit("limit3")
    in_limit_three.set_limit(limit_three)
    in_limit_manager_for_task2.add_in_limit(in_limit_three)
    in_limit_manager_for_task2.add_in_limit(in_limit_two)

    in_limit_manager_for_task2.increment_in_limit(limit_set, "/flow/task1")
    assert limit_set == {limit_one, limit_two, limit_three}
    assert limit_one.value == 1
    assert limit_two.value == 1
    assert limit_three.value == 1


def test_in_limit_manager_decrement_in_limit():
    limit_one = Limit("limit1", 2)
    limit_two = Limit("limit2", 3)
    limit_three = Limit("limit3", 4)

    in_limit_manager = InLimitManager(Task("task1"))
    in_limit_one = InLimit("limit1")
    in_limit_one.set_limit(limit_one)
    in_limit_manager.add_in_limit(in_limit_one)
    in_limit_two = InLimit("limit2")
    in_limit_two.set_limit(limit_two)
    in_limit_manager.add_in_limit(in_limit_two)

    in_limit_manager_for_task2 = InLimitManager(Task("task2"))
    in_limit_three = InLimit("limit3")
    in_limit_three.set_limit(limit_three)
    in_limit_manager_for_task2.add_in_limit(in_limit_three)
    in_limit_manager_for_task2.add_in_limit(in_limit_two)

    limit_set = set()
    in_limit_manager.increment_in_limit(limit_set, "/flow/task1")
    in_limit_manager_for_task2.increment_in_limit(limit_set, "/flow/task1")

    assert limit_set == {limit_one, limit_two, limit_three}
    assert limit_one.value == 1
    assert limit_two.value == 1
    assert limit_three.value == 1

    limit_set = set()
    in_limit_manager_for_task2.decrement_in_limit(limit_set, "/flow/task1")
    assert limit_set == {limit_two, limit_three}
    assert limit_one.value == 1
    assert limit_two.value == 0
    assert limit_three.value == 0

    in_limit_manager.decrement_in_limit(limit_set, "/flow/task1")
    assert limit_set == {limit_one, limit_two, limit_three}
    assert limit_one.value == 0
    assert limit_two.value == 0
    assert limit_three.value == 0


# -------------------------------
# Flow
# -------------------------------


def test_flow_add_limit_duplicate(simple_flow):
    flow1 = simple_flow.flow1
    flow1.add_limit("limit1", 10)

    with pytest.raises(RuntimeError):
        flow1.add_limit("limit1", 20)


class FlowWithLimit(SimpleFlow):
    total_limit: Limit
    section_limit: Limit
    total_limit_in_limit: InLimit
    section_limit_in_limit: InLimit


@pytest.fixture
def flow_with_limit(simple_flow):
    """
    Flow:

        |- flow1
          limit total_limit 3
          limit section_limit 2
          in_limit total_limit
          |- container1
            in_limit section_limit
            |- task1
            |- container2
              |- task2
              |- task3
          |- task4
          |- container3
            |- task5
          |- task6

    """

    flow1 = simple_flow.flow1
    total_limit = flow1.add_limit("total_limit", 3)
    section_limit = flow1.add_limit("section_limit", 2)

    total_limit_in_limit = flow1.add_in_limit("total_limit")
    container1 = simple_flow.container1
    section_limit_in_limit = container1.add_in_limit("section_limit")

    flow = FlowWithLimit(
        **simple_flow.model_dump(),
        total_limit=total_limit,
        section_limit=section_limit,
        total_limit_in_limit=total_limit_in_limit,
        section_limit_in_limit=section_limit_in_limit,
    )

    return flow


def test_node_find_limit(flow_with_limit):
    total_limit = flow_with_limit.total_limit
    section_limit = flow_with_limit.section_limit
    flow1 = flow_with_limit.flow1
    task1 = flow_with_limit.task1

    assert total_limit == flow1.find_limit("total_limit")
    assert section_limit == flow1.find_limit("section_limit")

    assert flow1.find_limit("nonexist_limit") is None
    assert task1.find_limit("total_limit") is None


def test_node_find_limit_up(flow_with_limit):
    task1 = flow_with_limit.task1
    flow1 = flow_with_limit.flow1
    total_limit = flow_with_limit.total_limit
    section_limit = flow_with_limit.section_limit

    assert task1.find_limit_up("total_limit") == total_limit
    assert task1.find_limit_up("section_limit") == section_limit
    assert task1.find_limit_up("nonexist_limit") is None

    assert flow1.find_limit_up("total_limit") == total_limit
    assert flow1.find_limit_up("section_limit") == section_limit
    assert flow1.find_limit_up("nonexist_limit") is None


def test_node_check_in_limit_up(flow_with_limit):
    task1 = flow_with_limit.task1
    flow1 = flow_with_limit.flow1
    task6 = flow_with_limit.task6
    total_limit = flow_with_limit.total_limit
    section_limit = flow_with_limit.section_limit

    assert task1.check_in_limit_up()
    total_limit.increment(1, "/flow1/task4")
    total_limit.increment(1, "/flow1/container3/task5")
    total_limit.increment(1, "/flow1/task6")
    assert not flow1.check_in_limit_up()

    total_limit.reset()
    section_limit.increment(1, "/flow1/container1/container2/task2")
    section_limit.increment(1, "/flow1/container1/container2/task3")
    assert not task1.check_in_limit_up()
    assert task6.check_in_limit_up()


def test_node_increment_in_limit(flow_with_limit):
    task1 = flow_with_limit.task1
    task6 = flow_with_limit.task6
    total_limit = flow_with_limit.total_limit
    section_limit = flow_with_limit.section_limit

    limit_set = set()
    task1.increment_in_limit(limit_set)
    assert limit_set == {total_limit, section_limit}
    assert total_limit.value == 1
    assert section_limit.value == 1

    limit_set = set()
    task6.increment_in_limit(limit_set)
    assert limit_set == {total_limit}
    assert total_limit.value == 2
    assert section_limit.value == 1


def test_node_decrement_in_limit(flow_with_limit):
    task1 = flow_with_limit.task1
    task6 = flow_with_limit.task6
    total_limit = flow_with_limit.total_limit
    section_limit = flow_with_limit.section_limit

    limit_set = set()
    task1.increment_in_limit(limit_set)
    limit_set = set()
    task6.increment_in_limit(limit_set)

    limit_set = set()
    task1.decrement_in_limit(limit_set)
    assert limit_set == {total_limit, section_limit}
    assert total_limit.value == 1
    assert section_limit.value == 0

    limit_set = set()
    task6.decrement_in_limit(limit_set)
    assert limit_set == {total_limit}
    assert total_limit.value == 0
    assert section_limit.value == 0


def test_flow_limit(flow_with_limit):
    flow1: Flow = flow_with_limit.flow1
    task1: Task = flow_with_limit.task1
    task2: Task = flow_with_limit.task2
    task3: Task = flow_with_limit.task3
    task4: Task = flow_with_limit.task4
    task5: Task = flow_with_limit.task5
    task6: Task = flow_with_limit.task6
    total_limit: Limit = flow_with_limit.total_limit
    section_limit: Limit = flow_with_limit.section_limit

    flow1.requeue()

    assert total_limit.value == 0
    assert section_limit.value == 0

    # Task1 run
    assert task1.check_in_limit_up()
    task1.run()
    assert total_limit.value == 1
    assert section_limit.value == 1
    task1.init("1001")
    assert total_limit.value == 1
    assert section_limit.value == 1

    # Task2 run
    task2.check_in_limit_up()
    task2.run()
    assert total_limit.value == 2
    assert section_limit.value == 2

    assert not task3.check_in_limit_up()

    # Task4 run
    assert task4.check_in_limit_up()
    task4.run()
    assert total_limit.value == 3
    assert section_limit.value == 2

    assert not task5.check_in_limit_up()

    # Task1 finish
    task1.complete()
    assert total_limit.value == 2
    assert section_limit.value == 1

    assert task3.check_in_limit_up()

    # Task5 run
    task5.run()
    assert total_limit.value == 3
    assert section_limit.value == 1

    assert not task3.check_in_limit_up()
    assert not task6.check_in_limit_up()

    # Task2 complete
    task2.complete()
    assert total_limit.value == 2
    assert section_limit.value == 0

    assert task3.check_in_limit_up()
    assert task6.check_in_limit_up()

    # Task3 run
    task3.run()
    assert total_limit.value == 3
    assert section_limit.value == 1

    assert not task6.check_in_limit_up()

    # Task4 complete
    task4.complete()
    assert total_limit.value == 2
    assert section_limit.value == 1

    # Task3 complete
    task3.complete()
    assert total_limit.value == 1
    assert section_limit.value == 0

    # Task5 complete
    task5.complete()
    assert total_limit.value == 0
    assert section_limit.value == 0


# -------------------------------
# InLimitManager.delete_in_limit
# -------------------------------


def test_in_limit_manager_delete_in_limit_without_tokens():
    limit_one = Limit("limit1", 2)

    in_limit_manager = InLimitManager(Task("task1"))
    in_limit_one = InLimit("limit1")
    in_limit_one.set_limit(limit_one)
    in_limit_manager.add_in_limit(in_limit_one)
    in_limit_two = InLimit("limit2")
    in_limit_manager.add_in_limit(in_limit_two)

    assert in_limit_manager.delete_in_limit("limit1")
    assert in_limit_manager.in_limit_list == [in_limit_two]
    assert limit_one.value == 0
    assert len(limit_one.node_paths) == 0


def test_in_limit_manager_delete_in_limit_not_found():
    in_limit_manager = InLimitManager(Task("task1"))
    in_limit_one = InLimit("limit1")
    in_limit_manager.add_in_limit(in_limit_one)
    in_limit_two = InLimit("limit2")
    in_limit_manager.add_in_limit(in_limit_two)

    assert not in_limit_manager.delete_in_limit("nonexist_limit")
    assert in_limit_manager.in_limit_list == [in_limit_one, in_limit_two]


def test_in_limit_manager_delete_in_limit_with_tokens():
    limit_one = Limit("limit1", 4)
    limit_two = Limit("limit2", 4)

    task = Task("task1")
    node_path = task.node_path
    in_limit_manager = InLimitManager(task)
    in_limit_one = InLimit("limit1", tokens=2)
    in_limit_one.set_limit(limit_one)
    in_limit_manager.add_in_limit(in_limit_one)
    in_limit_two = InLimit("limit2")
    in_limit_two.set_limit(limit_two)
    in_limit_manager.add_in_limit(in_limit_two)

    in_limit_manager.increment_in_limit(set(), node_path)
    assert limit_one.value == 2
    assert node_path in limit_one.node_paths

    assert in_limit_manager.delete_in_limit("limit1")
    assert in_limit_manager.in_limit_list == [in_limit_two]

    # tokens of the deleted InLimit are released
    assert limit_one.value == 0
    assert node_path not in limit_one.node_paths

    # the other Limit is untouched
    assert limit_two.value == 1
    assert node_path in limit_two.node_paths


def test_node_delete_in_limit_releases_tokens(flow_with_limit):
    flow1 = flow_with_limit.flow1
    container1 = flow_with_limit.container1
    task1 = flow_with_limit.task1
    total_limit = flow_with_limit.total_limit
    section_limit = flow_with_limit.section_limit

    flow1.requeue()
    task1.run()
    assert total_limit.value == 1
    assert section_limit.value == 1

    # container1 holds the section_limit InLimit, but the token is occupied with task1's path,
    # so deleting on container1 does not release task1's token.
    assert container1.in_limit_manager.delete_in_limit("section_limit")
    assert container1.in_limit_manager.in_limit_list == []
    assert section_limit.value == 1

    assert not container1.in_limit_manager.delete_in_limit("section_limit")
