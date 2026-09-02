from typing import Dict

from .util import SerializationType


class Event:
    """
    Event is an attribute for Task node to set some flag while task is running.

    Attributes
    ----------
    name : str
        Event's name
    initial_value : bool
        initial value of Event, default is False
    _value : bool
        current value of Event

    """

    def __init__(self, name: str, initial_value: bool = False):
        self.name: str = name
        self.initial_value: bool = initial_value
        self._value: bool = self.initial_value

    def __eq__(self, other: "Event") -> bool:
        return (
            self.name == other.name
            and self.value == other.value
            and self.initial_value == other.initial_value
        )

    def __repr__(self):
        if self.value:
            return f"Event({self.name}, set)"
        else:
            return f"Event({self.name}, unset)"

    @property
    def value(self) -> bool:
        """
        str: current value of Event.
        """
        return self._value

    @value.setter
    def value(self, value: bool):
        self._value = value

    def reset(self):
        """
        Set Event to its initial value, which is typically False.
        """
        self.value = self.initial_value

    # Serialization --------------------------------------------------------

    def to_dict(self) -> Dict:
        """
        Convert Event to dict object. For Example:

        .. code-block::

            {
                "name": "event1",
                "initial_value": False,
                "value": True
            }

        Returns
        -------
        Dict
            dict form of Event.

        Examples
        --------

        >>> event = Event("event1")
        >>> event.to_dict()
        {'name': 'event1', 'initial_value': False, 'value': False}
        >>> event.value = True
        >>> event.to_dict()
        {'name': 'event1', 'initial_value': False, 'value': True}

        """
        result = dict(
            name=self.name,
            initial_value=self.initial_value,
            value=self.value,
        )
        return result

    @classmethod
    def from_dict(
        cls, d: Dict, method: SerializationType = SerializationType.Status
    ) -> "Event":
        """
        Create an Event from dict object.

        If ``method`` is :py:obj:`~takler.core.SerializationType.Status`, set Event's value to ``d["value"]``.

        If ``method`` is :py:obj:`~takler.core.SerializationType.Tree`, only set initial value.

        Parameters
        ----------
        d
            dict object for an Event
        method
            serialization type.

        Returns
        -------
        Event

        Examples
        --------
        >>> d = {'name': 'event1', 'initial_value': False, 'value': True}
        >>> Event.from_dict(d, method=SerializationType.Status)
        Event(event1, set)
        >>> Event.from_dict(d, method=SerializationType.Tree)
        Event(event1, unset)

        If ``Tree`` method is used, value key is not neccessary.

        >>> d = {'name': 'event1', 'initial_value': False}
        >>> Event.from_dict(d, method=SerializationType.Tree)
        Event(event1, unset)

        """
        name = d["name"]
        initial_value = d["initial_value"]
        event = Event(name=name, initial_value=initial_value)
        if method == SerializationType.Status:
            value = d["value"]
            event.value = value

        return event
