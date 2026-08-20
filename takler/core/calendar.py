import datetime
from typing import Dict, Optional

from .util import SerializationType


def _datetime_to_dict_value(time: Optional[datetime.datetime]) -> Optional[str]:
    """
    Convert a datetime to an ISO 8601 string. ``None`` is kept as ``None`` (``null`` in JSON).
    """
    if time is None:
        return None
    return time.isoformat()


def _datetime_from_dict_value(value: Optional[str]) -> Optional[datetime.datetime]:
    """
    Convert an ISO 8601 string back to a datetime. ``None`` is kept as ``None``.
    """
    if value is None:
        return None
    return datetime.datetime.fromisoformat(value)


def _timedelta_to_dict_value(duration: Optional[datetime.timedelta]) -> Optional[float]:
    """
    Convert a timedelta to total seconds. ``None`` is kept as ``None`` (``null`` in JSON).
    """
    if duration is None:
        return None
    return duration.total_seconds()


def _timedelta_from_dict_value(value: Optional[float]) -> Optional[datetime.timedelta]:
    """
    Convert seconds back to a timedelta. ``None`` is kept as ``None``.
    """
    if value is None:
        return None
    return datetime.timedelta(seconds=value)


class Calendar:
    """
    Save current time and initial time for Flow.

    Attributes
    ----------
    initial_time
        工作流启动时间，逻辑时间
    flow_time
        工作流当前时间，逻辑时间
    duration
        从 initial_real_time 到当前的时间间隔
    increment
        相邻两次更新日历的间隔
    initial_real_time
        启动的真实时间
    last_real_time
        最后一次更新的真实时间
    """

    def __init__(self):
        self.initial_time: Optional[datetime.datetime] = None
        self.flow_time: Optional[datetime.datetime] = None
        self.duration: Optional[datetime.timedelta] = None
        self.increment: Optional[datetime.timedelta] = None

        self.initial_real_time: Optional[datetime.datetime] = None
        self.last_real_time: Optional[datetime.datetime] = None

    # generated variables

    @property
    def year(self) -> int:
        """
        year of current flow time.

        Returns
        -------
        int
        """
        if self.flow_time is None:
            return -1
        else:
            return self.flow_time.year

    @property
    def month(self) -> int:
        """
        month of current flow time.

        Returns
        -------
        int
        """
        if self.flow_time is None:
            return -1
        else:
            return self.flow_time.month

    @property
    def day_of_month(self) -> int:
        """
        day of current flow time.

        Returns
        -------
        int
        """
        if self.flow_time is None:
            return -1
        else:
            return self.flow_time.day

    @property
    def day_of_week(self) -> int:
        """
        week day number of current flow time.
        Monday is 1, Sunday is 7

        Returns
        -------
        int
        """
        if self.flow_time is None:
            return -1
        else:
            return self.flow_time.isoweekday()

    @property
    def day_of_year(self) -> int:
        """
        day number in year of current flow time

        Returns
        -------
        int
        """
        if self.flow_time is None:
            return -1
        else:
            return self.flow_time.timetuple().tm_yday

    def begin(self, time: datetime.datetime):
        """
        Start to run calendar, set initial_time and clear duration.
        Get current time and set it to initial_real_time and last_real_time.

        Parameters
        ----------
        time
        """
        self.initial_time = time
        self.flow_time = time
        self.duration = datetime.timedelta()
        self.increment = datetime.timedelta()
        self.initial_real_time = datetime.datetime.now()
        self.last_real_time = self.initial_real_time

    def update(self, time: datetime.datetime):
        """
        Update calendar's flow_time to a new time.

        Parameters
        ----------
        time
        """
        time_now = time
        self.increment = time_now - self.last_real_time
        self.duration = time_now - self.initial_real_time
        self.flow_time += self.increment
        self.last_real_time = time_now

    # Serialization ----------------------------------------------

    def to_dict(self) -> Dict:
        """
        Serialize the calendar's six fields.

        ``initial_time`` / ``flow_time`` / ``initial_real_time`` / ``last_real_time`` are written as
        ISO 8601 strings, ``duration`` / ``increment`` as seconds in float.
        Each field is written as ``null`` when it is ``None``.

        Returns
        -------
        Dict
        """
        result = dict(
            initial_time=_datetime_to_dict_value(self.initial_time),
            flow_time=_datetime_to_dict_value(self.flow_time),
            duration=_timedelta_to_dict_value(self.duration),
            increment=_timedelta_to_dict_value(self.increment),
            initial_real_time=_datetime_to_dict_value(self.initial_real_time),
            last_real_time=_datetime_to_dict_value(self.last_real_time),
        )
        return result

    @classmethod
    def from_dict(
        cls, d: Dict, method: SerializationType = SerializationType.Status
    ) -> "Calendar":
        """
        Create a ``Calendar`` from a dictionary.

        All the six fields are runtime state, so they are only restored when ``method`` is
        ``SerializationType.Status``. Otherwise a fresh calendar (all fields ``None``) is returned.
        Missing keys are tolerated and treated as ``None``.

        Parameters
        ----------
        d
        method

        Returns
        -------
        Calendar
        """
        calendar = cls()
        if method != SerializationType.Status:
            return calendar

        calendar.initial_time = _datetime_from_dict_value(d.get("initial_time"))
        calendar.flow_time = _datetime_from_dict_value(d.get("flow_time"))
        calendar.duration = _timedelta_from_dict_value(d.get("duration"))
        calendar.increment = _timedelta_from_dict_value(d.get("increment"))
        calendar.initial_real_time = _datetime_from_dict_value(
            d.get("initial_real_time")
        )
        calendar.last_real_time = _datetime_from_dict_value(d.get("last_real_time"))

        return calendar
