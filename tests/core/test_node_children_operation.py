import pytest

from takler.core.node import Node


def test_node_append_child():
    node1 = Node("node1")
    child1 = node1.append_child("child1")
    assert node1.children == [child1]

    child2 = Node("child2")
    node1.append_child(child2)
    assert node1.children == [child1, child2]


def test_node_append_child_error_type():
    node = Node("node")

    with pytest.raises(TypeError):
        node.append_child(None)

    with pytest.raises(TypeError):
        node.append_child(4)


def test_node_find_child_index():
    node1 = Node("node1")
    child1 = node1.append_child("child1")
    child2 = node1.append_child("child2")

    assert node1.find_child_index("child1") == 0
    assert node1.find_child_index(child1) == 0
    assert node1.find_child_index("child2") == 1
    assert node1.find_child_index(child2) == 1

    child2_standalone = Node("child2")
    assert node1.find_child_index(child2_standalone) == 1

    child3 = Node("child3")
    assert node1.find_child_index(child3) == -1
    assert node1.find_child_index("child3") == -1


def test_node_update_child():
    node1 = Node("node1")
    child1 = node1.append_child("child1")
    child2 = node1.append_child("child2")

    assert node1.children == [child1, child2]

    new_child1 = Node("new_child1")
    node1.update_child(child1, new_child1)
    assert node1.children == [new_child1, child2]

    new_child2 = Node("new_child2")
    node1.update_child("child2", new_child2)
    assert node1.children == [new_child1, new_child2]

    child3 = Node("child3")
    with pytest.raises(ValueError):
        node1.update_child("child3", new_child2)
    with pytest.raises(ValueError):
        node1.update_child(child3, new_child2)

    with pytest.raises(TypeError):
        node1.update_child(new_child2, None)
    with pytest.raises(TypeError):
        node1.update_child(None, new_child1)
    with pytest.raises(TypeError):
        node1.update_child(None, None)


def test_node_delete_children():
    node1 = Node("node1")
    child1 = node1.append_child("child1")
    child2 = node1.append_child("child2")

    assert node1.children == [child1, child2]

    node1.delete_children()
    assert node1.children == []

    node1.delete_children()
    assert node1.children == []


def test_node_delete_child():
    node1 = Node("node1")
    child1 = node1.append_child("child1")
    child2 = node1.append_child("child2")
    child2.append_child("child2_1")
    child2.append_child("child2_2")

    assert node1.children == [child1, child2]

    node1.delete_child(child1)
    assert node1.children == [child2]

    deleted_child2 = node1.delete_child("child2")
    assert node1.children == []
    assert deleted_child2.children == []

    with pytest.raises(ValueError):
        node1.delete_child("child3")

    with pytest.raises(TypeError):
        node1.delete_child(None)
