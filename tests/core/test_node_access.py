from dataclasses import dataclass
from typing import Optional

from takler.core import Bunch


def test_node_node_path(simple_flow):
    assert simple_flow.flow1.node_path == "/flow1"
    assert simple_flow.container1.node_path == "/flow1/container1"
    assert simple_flow.task1.node_path == "/flow1/container1/task1"
    assert simple_flow.container2.node_path == "/flow1/container1/container2"
    assert simple_flow.task2.node_path == "/flow1/container1/container2/task2"
    assert simple_flow.task3.node_path == "/flow1/container1/container2/task3"
    assert simple_flow.task4.node_path == "/flow1/task4"
    assert simple_flow.container3.node_path == "/flow1/container3"
    assert simple_flow.task5.node_path == "/flow1/container3/task5"
    assert simple_flow.task6.node_path == "/flow1/task6"


def test_node_is_leaf_node(simple_flow):
    assert not simple_flow.flow1.is_leaf_node()
    assert not simple_flow.container1.is_leaf_node()
    assert simple_flow.task1.is_leaf_node()
    assert not simple_flow.container2.is_leaf_node()
    assert simple_flow.task2.is_leaf_node()
    assert simple_flow.task3.is_leaf_node()
    assert simple_flow.task4.is_leaf_node()
    assert not simple_flow.container3.is_leaf_node()
    assert simple_flow.task5.is_leaf_node()
    assert simple_flow.task6.is_leaf_node()


def test_node_get_root(simple_flow):
    flow1 = simple_flow.flow1

    assert simple_flow.flow1.get_root() == flow1
    assert simple_flow.container1.get_root() == flow1
    assert simple_flow.task1.get_root() == flow1
    assert simple_flow.container2.get_root() == flow1
    assert simple_flow.task2.get_root() == flow1
    assert simple_flow.task3.get_root() == flow1
    assert simple_flow.task4.get_root() == flow1
    assert simple_flow.container3.get_root() == flow1
    assert simple_flow.task5.get_root() == flow1
    assert simple_flow.task6.get_root() == flow1


def test_node_get_bunch(simple_flow):
    flow1 = simple_flow.flow1

    bunch = flow1.get_bunch()
    assert bunch is None

    bunch = Bunch()
    bunch.add_flow(flow1)
    assert flow1.get_bunch() == bunch
    assert simple_flow.container1.get_bunch() == bunch
    assert simple_flow.task1.get_bunch() == bunch


def test_node_find_node(simple_flow):
    @dataclass
    class QueryOption:
        node_name: str
        node_path: str

    @dataclass
    class TestCase:
        query: QueryOption
        expected_node_name: Optional[str]

    def do_tests(cases: list[TestCase]):
        for test_case in cases:
            node = getattr(simple_flow, test_case.query.node_name)
            if test_case.expected_node_name is None:
                expected_node = None
            else:
                expected_node = getattr(simple_flow, test_case.expected_node_name)
            if expected_node is None:
                assert node.find_node(test_case.query.node_path) is None
            else:
                assert node.find_node(test_case.query.node_path) == expected_node

    test_cases = [
        TestCase(
            query=QueryOption(node_name="flow1", node_path="flow1/container1"),
            expected_node_name="container1",
        ),
        TestCase(
            query=QueryOption(node_name="flow1", node_path="/flow1/container1"),
            expected_node_name="container1",
        ),
        TestCase(
            query=QueryOption(node_name="flow1", node_path="/flow1/container3/task5"),
            expected_node_name="task5",
        ),
        TestCase(
            query=QueryOption(node_name="flow1", node_path="container1"),
            expected_node_name=None,
        ),
    ]

    do_tests(cases=test_cases)

    test_cases = [
        TestCase(
            query=QueryOption(node_name="container1", node_path="container1/task1"),
            expected_node_name="task1",
        ),
        TestCase(
            query=QueryOption(node_name="container1", node_path="task4"),
            expected_node_name="task4",
        ),
        TestCase(
            query=QueryOption(node_name="container1", node_path="/flow1/task4"),
            expected_node_name="task4",
        ),
        TestCase(
            query=QueryOption(node_name="task4", node_path="./container3"),
            expected_node_name="container3",
        ),
        TestCase(
            query=QueryOption(node_name="task4", node_path="/flow1/container3"),
            expected_node_name="container3",
        ),
    ]

    do_tests(cases=test_cases)

    test_cases = [
        TestCase(
            query=QueryOption(node_name="task2", node_path="./task3"),
            expected_node_name="task3",
        ),
        TestCase(
            query=QueryOption(node_name="task2", node_path="../task1"),
            expected_node_name="task1",
        ),
        TestCase(
            query=QueryOption(node_name="task2", node_path="../../task4"),
            expected_node_name="task4",
        ),
        TestCase(
            query=QueryOption(node_name="task2", node_path="./task1"),
            expected_node_name=None,
        ),
        TestCase(
            query=QueryOption(node_name="task2", node_path="../task5"),
            expected_node_name=None,
        ),
    ]
    do_tests(cases=test_cases)


#------------------------
# Bunch
#------------------------


def test_bunch_check_absolute_node_path():
    @dataclass
    class TestCase:
        node_path: str
        check_result: bool

    test_cases = [
        TestCase(node_path="/flow1", check_result=True),
        TestCase(node_path="/flow1/", check_result=True),
        TestCase(node_path="flow1", check_result=False),
        TestCase(node_path="flow1/", check_result=False),

        TestCase(node_path="/", check_result=False),
        TestCase(node_path="", check_result=False),

        TestCase(node_path="flow1/container1", check_result=False),
        TestCase(node_path="flow1/container1/", check_result=False),
        TestCase(node_path="/flow1/container1", check_result=True),
        TestCase(node_path="/flow1/container1/", check_result=True),

        TestCase(node_path="./container1", check_result=False),
        TestCase(node_path="./container1/", check_result=False),
        TestCase(node_path="./container1/task1", check_result=False),
        TestCase(node_path="./container1/task1/", check_result=False),
        TestCase(node_path="../container1", check_result=False),
        TestCase(node_path="../container1/", check_result=False),
        TestCase(node_path="../container1/task1", check_result=False),
        TestCase(node_path="../container1/task1/", check_result=False),

        TestCase(node_path='./', check_result=False),
        TestCase(node_path='../', check_result=False),
    ]

    for test_case in test_cases:
        if test_case.check_result:
            assert Bunch.check_absolute_node_path(test_case.node_path)
        else:
            assert not Bunch.check_absolute_node_path(test_case.node_path)


def test_bunch_check_node_path():
    @dataclass
    class TestCase:
        node_path: str
        check_result: bool

    test_cases = [
        TestCase(node_path="/flow1", check_result=True),
        TestCase(node_path="/flow1/", check_result=True),
        TestCase(node_path="flow1", check_result=False),
        TestCase(node_path="flow1/", check_result=False),

        TestCase(node_path="/", check_result=False),
        TestCase(node_path="", check_result=False),

        TestCase(node_path="flow1/container1", check_result=False),
        TestCase(node_path="flow1/container1/", check_result=False),
        TestCase(node_path="/flow1/container1", check_result=True),
        TestCase(node_path="/flow1/container1/", check_result=True),

        TestCase(node_path='.', check_result=False),
        TestCase(node_path='./', check_result=False),
        TestCase(node_path='..', check_result=False),
        TestCase(node_path='../', check_result=False),

        TestCase(node_path='./container1', check_result=True),
        TestCase(node_path='./container1/', check_result=True),
        TestCase(node_path='./container1/task1', check_result=True),
        TestCase(node_path='./container1/task1/', check_result=True),
        TestCase(node_path='../container1', check_result=True),
        TestCase(node_path='../container1/', check_result=True),
        TestCase(node_path='../container1/task1', check_result=True),
        TestCase(node_path='../container1/task1/', check_result=True),
    ]

    for test_case in test_cases:
        if test_case.check_result:
            assert Bunch.check_node_path(test_case.node_path)
        else:
            assert not Bunch.check_node_path(test_case.node_path)
