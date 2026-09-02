from typing import TYPE_CHECKING, Optional, Set, List, Dict

from .util import SerializationType


if TYPE_CHECKING:
    from .node import Node


class Limit:
    """
    Limit is used to control task running, and it is an attribution attached to some Node.
    The number of running Tasks with some Limit should not be large than Limit's limit.

    Attributes
    ----------
    name
        Limit's name. Limits in one Node should have different names.
    limit
        total number of tokens.
    value
        current occupied tokens.
    node
        node who holds the Limit.
    node_paths
        list of node path that is occupying the Limit.

    """

    def __init__(self, name: str, limit: int):
        self.name: str = name
        self.limit: int = limit
        self.value: int = 0
        self.node: Optional["Node"] = None
        self.node_paths: Set[str] = set()

    def __eq__(self, other):
        if not isinstance(other, Limit):
            return False
        return self.name == other.name and self.limit == other.limit

    def __hash__(self):
        return hash((self.name, self.limit))

    def __repr__(self):
        return f"Limit(name='{self.name}', limit={self.limit}, value={self.value}, node_paths={sorted(self.node_paths)})"

    def set_node(self, node: "Node"):
        """
        set reference node for Limit.
        """
        self.node = node

    def in_limit(self, tokens: int) -> bool:
        """
        check if there has enough tokens.
        """
        return self.value + tokens <= self.limit

    def increment(self, tokens: int, node_path: str):
        """
        Occupy ``tokens`` for some node by increment Limit value.

        Parameters
        ----------
        tokens
            number of tokens for node, usually is 1.
        node_path
            full node path. See ``Node.node_path``

        """
        if node_path in self.node_paths:
            return
        self.node_paths.add(node_path)
        self.value += tokens

    def decrement(self, tokens: int, node_path: str):
        """
        Release ``tokens`` for some node by decrement Limit value.

        Parameters
        ----------
        tokens
            number of tokens for node, usually is 1.
        node_path
            full node path. See ``Node.node_path``

        """
        if node_path not in self.node_paths:
            return
        self.node_paths.remove(node_path)
        self.value -= tokens
        if self.value < 0:
            self.value = 0
            self.node_paths.clear()

    def reset(self):
        """
        reset the limit. Clear node path list and set current value to 0.
        """
        self.node_paths.clear()
        self.value = 0

    # Serialization ----------------------------------------------------------

    def to_dict(self) -> Dict:
        result = dict(
            name=self.name,
            limit=self.limit,
            node_paths=sorted(list(self.node_paths)),
            value=self.value,
        )
        return result

    @classmethod
    def from_dict(
        cls, d: Dict, method: SerializationType = SerializationType.Status
    ) -> "Limit":
        name = d["name"]
        limit = d["limit"]
        limit = Limit(name=name, limit=limit)
        if method == SerializationType.Status:
            value = d["value"]
            node_paths = d["node_paths"]
            limit.value = value
            limit.node_paths = set(node_paths)
        return limit


class InLimit:
    """
    An attribution of Node to mark current Node and all its children to use some Limit.
    One Node could have multiply :py:class:`~takler.core.limit.InLimit` objects.
    :py:class:`~takler.core.limit.InLimit` is managed by :py:class:`~takler.core.limit.InLimitManager`.

    Attributes
    ----------
    limit_name
        name of the Limit.
    tokens
        number of tokens for one node. Default is 1.
    node_path
        reference node of the Limit. If None, :py:class:`~takler.core.limit.InLimitManager` will search up the node tree.
    limit
        the :py:class:`~takler.core.limit.Limit` object
    """

    def __init__(
        self, limit_name: str, node_path: Optional[str] = None, tokens: int = 1
    ):
        self.limit_name: str = limit_name
        self.tokens: int = tokens

        # self.node: Optional[Node] = None
        self.node_path: Optional[str] = node_path
        self.limit: Optional[Limit] = None

    def __eq__(self, other):
        return (
            self.limit_name == other.limit_name
            and self.tokens == other.tokens
            and self.node_path == other.node_path
            and self.limit == other.limit
        )

    def __repr__(self):
        return f"InLimit(limit_name='{self.limit_name}', tokens={self.tokens}, node_path={self.node_path!r})"

    def set_limit(self, limit: Limit):
        self.limit = limit

    # Serialization -----------------------

    def to_dict(self) -> Dict:
        result = dict(
            limit_name=self.limit_name,
            tokens=self.tokens,
            node_path=self.node_path,
        )
        return result

    @classmethod
    def from_dict(
        cls, d: Dict, method: SerializationType = SerializationType.Status
    ) -> "InLimit":
        limit_name = d["limit_name"]
        tokens = d["tokens"]
        node_path = d["node_path"]
        in_limit = InLimit(limit_name=limit_name, node_path=node_path, tokens=tokens)
        return in_limit


class InLimitManager:
    """
    Manager :py:class:`~takler.core.limit.InLimit` objects in one :py:class:`~takler.core.node.Node`.
    Deal with Limit increment and decrement.

    Attributes
    ----------
    node : Node
        reference node who has the :py:class:`~takler.core.limit.InLimitManager`
    in_limit_list : List[InLimit]
        list of :py:class:`~takler.core.limit.InLimit`
    """

    def __init__(self, node: "Node"):
        self.node: "Node" = node

        self.in_limit_list: List[InLimit] = list()

    def __eq__(self, other):
        return all([a == b for a, b in zip(self.in_limit_list, other.in_limit_list)])

    # Access ----------------------------------------

    def add_in_limit(self, in_limit: InLimit):
        if self.has_in_limit(in_limit):
            raise RuntimeError(
                f"add_in_limit failed: duplicate InLimit in node: {self.node.node_path}"
            )
        self.in_limit_list.append(in_limit)

    def delete_in_limit(self, name: str) -> bool:
        """
        Delete the :py:class:`~takler.core.limit.InLimit` with limit name ``name``.

        The matched ``InLimit`` is removed from ``in_limit_list`` **first**, and then the tokens
        occupied by the current node on the corresponding ``Limit`` are released. Doing it in this
        order keeps ``in_limit()`` from seeing an item whose tokens have already been released while
        the item itself is still in the list.

        Parameters
        ----------
        name
            name of the Limit used by the ``InLimit``.

        Returns
        -------
        bool
            ``True`` if an ``InLimit`` is found and removed, no matter whether it is occupying
            tokens of the ``Limit``. ``False`` if there is no such ``InLimit``, and in that case
            ``in_limit_list`` is left unchanged.
        """
        target: Optional[InLimit] = None
        for index, item in enumerate(self.in_limit_list):
            if item.limit_name == name:
                target = self.in_limit_list.pop(index)
                break

        if target is None:
            return False

        if target.limit is None:
            self.resolve_in_limit(target)

        if target.limit is not None:
            target.limit.decrement(target.tokens, self.node.node_path)

        return True

    def has_in_limit(self, in_limit: InLimit) -> bool:
        """
        Check whether InLimit is in this manager.

        Same InLimits should have same limit_name and same node_path (equal if not None)

        Parameters
        ----------
        in_limit

        Returns
        -------
        bool
        """
        for item in self.in_limit_list:
            if (
                (item.limit_name is None and in_limit.limit_name is None)
                or (item.limit_name == in_limit.limit_name)
            ) and (
                (item.node_path is None and in_limit.node_path is None)
                or (item.node_path == in_limit.node_path)
            ):
                return True
        return False

    # Check ------------------------------

    def in_limit(self) -> bool:
        """
        Check if there are enough tokens in all ``Limit``s.

        Returns
        -------
        bool
        """
        if len(self.in_limit_list) == 0:
            return True

        self.resolve_in_limit_references()

        valid_in_limit_count = 0
        fitted_in_limit_count = 0
        for item in self.in_limit_list:
            if item.limit is None:
                continue

            valid_in_limit_count += 1
            if item.limit.in_limit(item.tokens):
                fitted_in_limit_count += 1

        return valid_in_limit_count == fitted_in_limit_count

    # Change ------------------------------------------

    def increment_in_limit(self, limit_set: Set[Limit], node_path: str):
        """
        Occupy all ``Limits`` with each ``InLimit``'s token for some node.

        One ``Limit`` should only be occupied once even if there are multiply ``InLimit`` using it.

        Parameters
        ----------
        limit_set
            A Limit set shared with in ``Node.increment_in_limit`` method to make sure each ``Limit`` will be occupied once.
        node_path
            full node path, see ``Node.node_path``
        """
        if len(self.in_limit_list) == 0:
            return

        self.resolve_in_limit_references()

        for item in self.in_limit_list:
            current_limit = item.limit
            if current_limit is None:
                continue
            if current_limit in limit_set:
                continue

            limit_set.add(current_limit)
            current_limit.increment(item.tokens, node_path)

    def decrement_in_limit(self, limit_set: Set[Limit], node_path: str):
        """
        Release all ``Limits`` with each ``InLimit``'s token for some node.

        One ``Limit`` should only be released once even if there are multiply ``InLimit`` using it.

        Parameters
        ----------
        limit_set
            see ``InLimitManager.increment_in_limit``
        node_path
            see ``InLimitManager.increment_in_limit``
        """
        if len(self.in_limit_list) == 0:
            return

        self.resolve_in_limit_references()

        for item in self.in_limit_list:
            current_limit = item.limit
            if current_limit is None:
                continue
            if current_limit in limit_set:
                continue

            limit_set.add(current_limit)
            current_limit.decrement(item.tokens, node_path)

    # Inner use ---------------------------------------

    def resolve_in_limit_references(self):
        """
        Find ``Limit`` for all ``InLimit`` in this manager.
        """
        for item in self.in_limit_list:
            self.resolve_in_limit(item)

    def resolve_in_limit(self, in_limit: InLimit):
        """
        Find ``Limit`` for some ``InLimit``.
        """
        if in_limit.limit is not None:
            return

        # find limit
        if in_limit.node_path is None:
            limit = self.node.find_limit_up(in_limit.limit_name)
            if limit is not None:
                in_limit.set_limit(limit)
            return
        else:
            reference_node = self.node.find_node(in_limit.node_path)
            if reference_node is None:
                return

            limit = reference_node.find_limit(in_limit.limit_name)
            if limit is not None:
                in_limit.set_limit(limit)
            return

    # Serialization ----------------------------------------

    def to_dict(self) -> Dict:
        result = dict(
            in_limit_list=[in_limit.to_dict() for in_limit in self.in_limit_list]
        )
        return result

    @classmethod
    def fill_from_dict(
        cls, d: Dict, node: "Node", method: SerializationType = SerializationType.Status
    ) -> "InLimitManager":
        in_limit_list = d["in_limit_list"]
        for in_limit in in_limit_list:
            node.add_in_limit(
                limit_name=in_limit["limit_name"],
                tokens=in_limit["tokens"],
                node_path=in_limit["node_path"],
            )
        return node.in_limit_manager
