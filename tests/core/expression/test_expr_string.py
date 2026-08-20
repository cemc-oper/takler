from dataclasses import dataclass
from typing import Union, Type

import pytest
from lark.exceptions import LarkError

from takler.core import NodeStatus
from takler.exceptions import ExpressionSyntaxError
from takler.core.expression_parser import parse_trigger
from takler.core.expression_ast import (
    AstOpEq, AstOpGt, AstOpGe,
    AstOpOr, AstOpAnd,
    AstNodePath, AstVariablePath, AstNodeStatus, AstInteger,
    AstMathAdd,
)


def test_node_path():
    expr_cases = [
        "/flow1/task1 == complete",
        "./task1 == aborted",
        "../container1/task1 == complete",
        "../../container1/task_001 == complete",
        "/flow1/00/container1/000_task == complete",
        "/flow1/00/001/002_task03 == complete",
    ]
    for expr_string in expr_cases:
        ast = parse_trigger(expr_string)
        assert isinstance(ast.left, AstNodePath)
        assert isinstance(ast.right, AstNodeStatus)


def test_invalid_node_path():
    expr_cases = [
        # expression string, reported line, reported column
        ("task1 == complete", 1, 1),
    ]
    for expr_string, line, column in expr_cases:
        with pytest.raises(ExpressionSyntaxError) as exc_info:
            parse_trigger(expr_string)
        error = exc_info.value
        assert not isinstance(error, LarkError)
        assert error.expression == expr_string
        assert error.line == line
        assert error.column == column


def test_node_status():
    expr_cases = [
        ("./task1 == aborted", NodeStatus.aborted),
        ("./task1 == Aborted", NodeStatus.aborted),
        ("./task1 == abORTed", NodeStatus.aborted),
        ("./task1 == complete", NodeStatus.complete),
        ("./task1 == COMPLETE", NodeStatus.complete),
    ]
    for expr_string, status in expr_cases:
        ast = parse_trigger(expr_string)
        assert isinstance(ast.left, AstNodePath)
        assert isinstance(ast.right, AstNodeStatus)
        assert ast.right.node_status == status


def test_invalid_node_status():
    expr_cases = [
        # expression string, reported line, reported column
        ("./task1 == unknown", 1, 12),
        ("./task1 == queued", 1, 12),
        ("./task1 == submitted", 1, 12),
    ]
    for expr_string, line, column in expr_cases:
        with pytest.raises(ExpressionSyntaxError) as exc_info:
            parse_trigger(expr_string)
        error = exc_info.value
        assert not isinstance(error, LarkError)
        assert error.expression == expr_string
        assert error.line == line
        assert error.column == column


def test_invalid_expression_without_position():
    """
    The underlying parser reports no position for an unexpected end of input,
    so ``line`` / ``column`` are ``None`` while ``expression`` is still kept.
    """
    expr_cases = [
        "/flow1/task1 ==",
        "(/flow1/task1 == complete",
        "",
    ]
    for expr_string in expr_cases:
        with pytest.raises(ExpressionSyntaxError) as exc_info:
            parse_trigger(expr_string)
        error = exc_info.value
        assert not isinstance(error, LarkError)
        assert error.expression == expr_string
        assert error.line is None
        assert error.column is None


def test_variable_path():
    @dataclass
    class TestCase:
        expr_string: str
        node_path: str
        variable_name: str

    expr_cases = [
        TestCase("/flow1/task1:event1 == set", "/flow1/task1", "event1"),
        TestCase("./task1:meter1 >= 20", "./task1", "meter1"),
        TestCase("../container1/task1:event2 == set", "../container1/task1", "event2"),
        TestCase("../../container1/task1:meter2 > 10", "../../container1/task1", "meter2"),
    ]
    for test_case in expr_cases:
        ast = parse_trigger(test_case.expr_string)
        assert isinstance(ast.left, AstVariablePath)
        assert ast.left.node.node_path == test_case.node_path
        assert ast.left.variable_name == test_case.variable_name


def test_op_eq():
    @dataclass
    class TestCase:
        expr_string: str
        left: Union[AstNodePath, AstVariablePath]
        right: Union[NodeStatus, int]
    expr_cases = [
        TestCase("/flow1/task1 == complete", AstNodePath("/flow1/task1"), NodeStatus.complete),
        TestCase("/flow1/task1 eq complete", AstNodePath("/flow1/task1"), NodeStatus.complete),
        TestCase("/flow1/task1 EQ complete", AstNodePath("/flow1/task1"), NodeStatus.complete),
        TestCase("/flow1/task1 eQ complete", AstNodePath("/flow1/task1"), NodeStatus.complete),
        TestCase("/flow1/task1 Eq complete", AstNodePath("/flow1/task1"), NodeStatus.complete),
        TestCase(
            "/flow1/task1:event1 == set",
            AstVariablePath(AstNodePath("/flow1/task1"), "event1"),
            1
        ),
        TestCase(
            "/flow1/task1:meter1 == 10", # not suggested.
            AstVariablePath(AstNodePath("/flow1/task1"), "meter1"),
            10
        ),
    ]
    for test_case in expr_cases:
        ast = parse_trigger(test_case.expr_string)
        assert isinstance(ast, AstOpEq)

        left = ast.left
        if isinstance(left, AstNodePath):
            assert left.node_path == test_case.left.node_path
        elif isinstance(left, AstVariablePath):
            assert left.node.node_path == test_case.left.node.node_path
            assert left.variable_name == test_case.left.variable_name
        else:
            raise ValueError(f"left type is not supported: {type(left).__name__}")

        right = ast.right
        if isinstance(right, AstNodeStatus):
            assert right.node_status == test_case.right
        elif isinstance(right, AstInteger):
            assert right.value() == test_case.right
        else:
            raise ValueError(f"right type is not supported: {type(right).__name__}")


def test_event_set():
    expr_cases = [
        "/flow1/task1:event1 == set",
        "/flow1/task1:event1 == SET",
    ]
    for expr_string in expr_cases:
        ast = parse_trigger(expr_string)
        ast_right = ast.right
        assert isinstance(ast_right, AstInteger)
        assert ast_right.value() == 1


def test_event_unset():
    expr_cases = [
        "/flow1/task1:event1 == unset",
        "/flow1/task1:event1 == UNSET",
    ]
    for expr_string in expr_cases:
        ast = parse_trigger(expr_string)
        ast_right = ast.right
        assert isinstance(ast_right, AstInteger)
        assert ast_right.value() == 0


def test_meter_value():
    @dataclass
    class TestCase:
        expr_string: str
        left: AstVariablePath
        right: int
        op: Type[Union[AstOpEq, AstOpGt, AstOpGe]]

    expr_cases = [
        TestCase(
            "/flow1/task1:meter1 == 10",
            AstVariablePath(AstNodePath("/flow1/task1"), "meter1"),
            10,
            AstOpEq),
        TestCase(
            "/flow1/task1:meter1 > 10",
            AstVariablePath(AstNodePath("/flow1/task1"), "meter1"),
            10,
            AstOpGt),
        TestCase(
            "/flow1/task1:meter1 >= 10",
            AstVariablePath(AstNodePath("/flow1/task1"), "meter1"),
            10,
            AstOpGe),
    ]
    for test_case in expr_cases:
        ast = parse_trigger(test_case.expr_string)
        assert isinstance(ast, test_case.op)

        left = ast.left
        assert left.node.node_path == test_case.left.node.node_path
        assert left.variable_name == test_case.left.variable_name

        ast_right = ast.right
        assert isinstance(ast_right, AstInteger)
        assert ast_right.value() == 10

def test_op_gt():
    @dataclass
    class TestCase:
        expr_string: str
        left: AstVariablePath
        right: int

    expr_cases = [
        TestCase("/flow1/task1:meter1 > 20", AstVariablePath(AstNodePath("/flow1/task1"), "meter1"), 20),
    ]
    for test_case in expr_cases:
        ast = parse_trigger(test_case.expr_string)
        assert isinstance(ast, AstOpGt)


def test_op_ge():
    @dataclass
    class TestCase:
        expr_string: str
        left: AstVariablePath
        right: int

    expr_cases = [
        TestCase(
            "/flow1/task1:meter1 >= 20",
            AstVariablePath(AstNodePath("/flow1/task1"), "meter1"),
            20,
        ),
    ]
    for test_case in expr_cases:
        ast = parse_trigger(test_case.expr_string)
        assert isinstance(ast, AstOpGe)
        assert ast.left.node.node_path == test_case.left.node.node_path
        assert ast.left.variable_name == test_case.left.variable_name
        assert ast.right.value() == test_case.right


def test_op_and():
    expr_cases = [
        "/flow1/task1 == complete and /flow1/task2 == complete",
        "/flow1/task1 == complete AND /flow1/task2 == complete",
    ]
    for expr_string in expr_cases:
        ast = parse_trigger(expr_string)
        assert isinstance(ast, AstOpAnd)


def test_op_or():
    expr_cases = [
        "/flow1/task1 == complete or /flow1/task2 == complete",
        "/flow1/task1 == complete OR /flow1/task2 == complete",
        "/flow1/task1 == complete OR /flow1/task2:meter1 >= 10",
    ]
    for expr_string in expr_cases:
        ast = parse_trigger(expr_string)
        assert isinstance(ast, AstOpOr)


def test_multi_op():
    @dataclass
    class TestCase:
        expr_string: str
        op: Type[Union[AstOpOr, AstOpAnd]]
        left: Type[Union[AstOpOr, AstOpAnd, AstOpEq, AstOpGt, AstOpGe]]
        right: Type[Union[AstOpOr, AstOpAnd, AstOpEq, AstOpGt, AstOpGe]]

    expr_cases = [
        TestCase(
            "(/flow1/task1 == complete and /flow1/task2:meter1 >= 20) or /flow1/task3 == complete",
            AstOpOr,
            AstOpAnd,
            AstOpEq,
        ),
        TestCase(
            "/flow1/task1 == complete and (/flow1/task2:event1 == set or /flow1/task3 == complete)",
            AstOpAnd,
            AstOpEq,
            AstOpOr,
        ),
    ]
    for test_case in expr_cases:
        ast = parse_trigger(test_case.expr_string)
        assert isinstance(ast, test_case.op)
        assert isinstance(ast.left, test_case.left)
        assert isinstance(ast.right, test_case.right)


def test_math_add():
    @dataclass
    class TestCase:
        expr_string: str
        op: Type[AstMathAdd]
        left: AstVariablePath
        right: AstVariablePath

    expr_cases = [
        TestCase(
            "/flow1/container1:YMD + /flow1/container2:LAG_DATE",
            AstMathAdd,
            AstVariablePath(AstNodePath("/flow1/container1"), "YMD"),
            AstVariablePath(AstNodePath("/flow1/container2"), "LAG_DATE"),
        ),
    ]
    for test_case in expr_cases:
        ast = parse_trigger(test_case.expr_string)
        assert isinstance(ast, test_case.op)
        assert ast.left.node.node_path == test_case.left.node.node_path
        assert ast.left.variable_name == test_case.left.variable_name

        assert ast.right.node.node_path == test_case.right.node.node_path
        assert ast.right.variable_name == test_case.right.variable_name
