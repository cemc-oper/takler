from typing import Dict

from .util import SerializationType


class Meter:
    """
    An integer attribute with value range.

    Attributes
    ----------
    name : str
        meter name
    min_value : int
        min value, meter's initial value is min value
    max_value : int
        max value
    _value : int
        current value
    """

    def __init__(self, name: str, min_value: int, max_value: int):
        self.name: str = name
        self.min_value: int = min_value
        self.max_value: int = max_value
        self._value: int = self.min_value

    def __repr__(self):
        return f"Meter({self.name}, [{self.min_value}, {self.max_value}], {self.value})"

    def __eq__(self, other: "Meter") -> bool:
        return (
            self.name == other.name
            and self.min_value == other.min_value
            and self.max_value == other.max_value
            and self.value == other.value
        )

    @property
    def value(self) -> int:
        """
        int: current value of meter
        """
        return self._value

    @value.setter
    def value(self, value: int):
        if self.is_invalid(value):
            raise ValueError(f"value must be in [{self.min_value}, {self.max_value}]")
        self._value = value

    def reset(self):
        """
        set meter to its initial value, which is the min value.
        """
        self.value = self.min_value

    def is_invalid(self, value: int) -> bool:
        """
        Check whether some value is in the range.

        Parameters
        ----------
        value
            some value to be checked

        Returns
        -------
        bool
        """
        return value < self.min_value or value > self.max_value

    # Serialization -----------------------------------------------------------

    def to_dict(self) -> Dict:
        """
        Convert Meter to dict object. For example:

        .. code-block::

            {
                "name": "meter1",
                "min_value": 0,
                "max_value": 100,
                "value": 10
            }

        Returns
        -------
        Dict
            dict form of Meter

        Examples
        --------

        >>> meter = Meter("meter1", 0, 100)
        >>> meter.to_dict()
        {'name': 'meter1', 'min_value': 0, 'max_value': 100, 'value': 0}
        >>> meter.value = 10
        >>> meter.to_dict()
        {'name': 'meter1', 'min_value': 0, 'max_value': 100, 'value': 10}

        """
        result = dict(
            name=self.name,
            min_value=self.min_value,
            max_value=self.max_value,
            value=self.value,
        )
        return result

    @classmethod
    def from_dict(
        cls, d: Dict, method: SerializationType = SerializationType.Status
    ) -> "Meter":
        """
        Create a Meter from dict object.

        If ``method`` is :py:obj:`~takler.core.SerializationType.Status`, set Meter's value to ``d["value"]``.

        If ``method`` is :py:obj:`~takler.core.SerializationType.Tree`, only set min and max value.


        Parameters
        ----------
        d
            dict object for a Meter
        method
            serialization type.

        Returns
        -------
        Meter

        Examples
        --------
        >>> d = {'name': 'meter1', 'min_value': 0, 'max_value': 100, 'value': 10}
        >>> Meter.from_dict(d, method=SerializationType.Status)
        Meter(meter1, [0, 100], 10)
        >>> Meter.from_dict(d, method=SerializationType.Tree)
        Meter(meter1, [0, 100], 0)

        If ``Tree`` method is used, value key is not neccessary.

        >>> d = {'name': 'meter1', 'min_value': 0, 'max_value': 100}
        >>> Meter.from_dict(d, method=SerializationType.Tree)
        Meter(meter1, [0, 100], 0)

        """
        name = d["name"]
        min_value = d["min_value"]
        max_value = d["max_value"]
        meter = Meter(
            name=name,
            min_value=min_value,
            max_value=max_value,
        )
        if method == SerializationType.Status:
            value = d["value"]
            meter.value = value
        return meter
