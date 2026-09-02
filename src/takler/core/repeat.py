from abc import ABC, abstractmethod
from typing import Union, Optional, Dict, TypeVar
from datetime import datetime, date, timedelta

from .parameter import Parameter
from .util import SerializationType


T = TypeVar("T")


class RepeatBase(ABC):
    def __init__(self, name: str):
        self.name: str = name

    @property
    @abstractmethod
    def start(self) -> T:
        """
        The first (start) value.
        """

    @property
    @abstractmethod
    def end(self) -> T:
        """
        The last (end) value.
        """

    @property
    @abstractmethod
    def step(self):
        """
        step for each increment, rely on Instance.
        """

    @property
    @abstractmethod
    def value(self) -> T:
        """
        Return current value for Repeat.
        """

    @value.setter
    @abstractmethod
    def value(self, value):
        """
        Set value without raise exception.
        """

    @abstractmethod
    def valid(self) -> bool:
        """
        Check whether current value is valid, such as .

        Returns
        -------
        bool
        """
        return True

    @abstractmethod
    def increment(self) -> bool:
        """
        Increment Repeat's current value to next value.
        Return True if there has a next value, or False if current value is the last one.

        Returns
        -------
        bool
            where the increment is successful.
        """
        return True

    @abstractmethod
    def change(self, value):
        """
        Set value, and will raise exception if value is not valid.

        Raises
        ------
        ValueError
            If ``value`` is not valid.
        """

    @abstractmethod
    def reset(self):
        """
        Set value to the first (start) value
        """

    @abstractmethod
    def generated_parameters(self) -> Dict[str, Parameter]:
        """
        Return generated parameters to use in Node.

        Returns
        -------
        Dict[str, Parameter]
            Generated parameters.
        """

    @abstractmethod
    def to_dict(self) -> Dict: ...

    @staticmethod
    def from_dict(
        cls, d: Dict, method: SerializationType = SerializationType.Status
    ) -> "RepeatBase": ...


class RepeatDate(RepeatBase):
    """
    Repeat dates, from start_date to end_date step by step_day.

    Attributes
    ----------
    start_date
        Start date
    end_date
        End date
    step_day
        Step of days
    _value
        Current date
    """

    DATE_FORMAT = "%Y%m%d"

    def __init__(
        self,
        name: str,
        start_date: Union[str, int],
        end_date: Union[str, int],
        step: int = 1,
    ):
        super(RepeatDate, self).__init__(name=name)
        self.start_date: date = datetime.strptime(
            str(start_date), RepeatDate.DATE_FORMAT
        ).date()
        self.end_date: date = datetime.strptime(
            str(end_date), RepeatDate.DATE_FORMAT
        ).date()
        self.step_day: timedelta = timedelta(days=step)
        self._value: date = self.start_date

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.start_date == other.start_date
            and self.end_date == other.end_date
            and self.step_day == other.step_day
            and self.value == other.value
        )

    @property
    def start(self) -> int:
        return int(self.start_date.strftime(RepeatDate.DATE_FORMAT))

    @property
    def end(self) -> int:
        return int(self.end_date.strftime(RepeatDate.DATE_FORMAT))

    @property
    def step(self) -> int:
        return int(self.step_day / timedelta(days=1))

    @property
    def value(self) -> int:
        return int(self._value.strftime(RepeatDate.DATE_FORMAT))

    @value.setter
    def value(self, value: Union[str, int, date]):
        if isinstance(value, int):
            value = str(value)
        if isinstance(value, str):
            value = datetime.strptime(value, RepeatDate.DATE_FORMAT).date()
        self._value = value

    def valid(self) -> bool:
        return self.start_date <= self._value <= self.end_date

    def increment(self) -> bool:
        value = self._value + self.step_day
        if value <= self.end_date:
            self.value = value
            return True
        else:
            return False

    def change(self, value: Union[str, int, date]):
        """
        change RepeatDate with valid value.
        If value is not valid, raise ValueError.

        Parameters
        ----------
        value

        """
        if isinstance(value, int):
            value = str(value)
        if isinstance(value, str):
            value = datetime.strptime(value, RepeatDate.DATE_FORMAT).date()

        # check range
        if value < self.start_date or value > self.end_date:
            raise ValueError(
                f"value must be in range [{self.start_date}, {self.end_date}], but current is {value}"
            )

        # check step
        diff = (value - self.start_date) / self.step_day
        if not diff.is_integer():
            raise ValueError(
                f"value must be in multiply step {self.step} from {self.start_date}, but current is {value}"
            )

        self.value = value

    def reset(self):
        self._value = self.start_date

    def generated_parameters(self) -> Dict[str, Parameter]:
        return {self.name: Parameter(self.name, self.value)}

    # Serialization ---------------------------------------

    def to_dict(self) -> Dict:
        result = dict(
            name=self.name,
            start_date=self.start_date.strftime(self.DATE_FORMAT),
            end_date=self.end_date.strftime(self.DATE_FORMAT),
            step=self.step,
            value=self.value,
            class_type=self.__class__.__name__,
        )
        return result

    @classmethod
    def from_dict(
        cls, d: Dict, method: SerializationType = SerializationType.Status
    ) -> "RepeatDate":
        name = d["name"]
        start_date = d["start_date"]
        end_date = d["end_date"]
        step = d["step"]
        repeat_date = RepeatDate(
            name=name, start_date=start_date, end_date=end_date, step=step
        )

        if method == SerializationType.Status:
            value = d["value"]
            repeat_date.value = value

        return repeat_date


class Repeat:
    """
    An attribute for Node to run tasks repeatedly under this Node.

    Attributes
    ----------
    r
        inner ``RepeatBase`` object. If ``None``, ``empty`` returns ``False``.
    """

    def __init__(self, r: Optional[RepeatBase] = None):
        self.r = r

    def __eq__(self, other):
        return other.r == self.r

    def empty(self) -> bool:
        """
        Check if Repeat is empty.

        Returns
        -------
        bool
        """
        return self.r is not None

    def clear(self):
        """
        Clear Repeat and make it empty.
        """
        self.r = None

    def start(self) -> T:
        """
        Return the first (start) value.
        """
        return self.r.start

    def end(self) -> T:
        """
        Return the last (end) value.
        """
        return self.r.end

    def step(self):
        """
        Return step.
        """
        return self.r.step

    def value(self) -> T:
        """
        Return current value.
        """
        return self.r.value

    def valid(self) -> bool:
        """
        Check if current value if valid.
        """
        return self.r.valid()

    def reset(self):
        """
        Reset Repeat to the first value.
        """
        return self.r.reset()

    def increment(self) -> bool:
        """
        Increment Repeat to next value, return True if successful.
        """
        return self.r.increment()

    def change(self, value):
        """
        Change Repeat current value and will raise exception if value is not valid.
        """
        self.r.change(value)

    def set_value(self, value):
        """
        Set Repeat current value without raising exception.
        """
        self.r.value = value

    def generated_parameters(self) -> Dict[str, Parameter]:
        """
        Return generated parameters.
        """
        return self.r.generated_parameters()

    # Serialization -----------------------------------------------------

    def to_dict(self) -> Dict:
        result = dict(r=self.r.to_dict())
        return result

    @classmethod
    def from_dict(
        cls, d: Dict, method: SerializationType = SerializationType.Status
    ) -> "Repeat":
        r = d["r"]
        class_name = r["class_type"]
        class_type = REPEAT_ATTR_MAP[class_name]
        repeat_r = class_type.from_dict(d=r, method=method)
        repeat = Repeat(r=repeat_r)
        return repeat


REPEAT_ATTR_MAP = dict(RepeatDate=RepeatDate)
