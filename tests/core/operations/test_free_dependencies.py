import pytest


def test_free_dependencies_with_error_type(simple_flow_for_operation):
    task1 = simple_flow_for_operation.task1

    with pytest.raises(ValueError):
        task1.free_dependencies("error_type")
