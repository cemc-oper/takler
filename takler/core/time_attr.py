import datetime
from typing import Union, Dict

from .calendar import Calendar
from .util import SerializationType


class TimeAttribute:
    """
    Time dependency for a Node.

    Node should only be run until time dependency is satisfied.

    Attributes
    ----------
    time
        time point when the node should begin to run.
    free
        if marked true, time attributes is ignored.
    """

    def __init__(self, time: Union[datetime.time, str]):
        if isinstance(time, str):
            time = datetime.datetime.strptime(time, "%H:%M").time()
        self.time: datetime.time = time
        self.free: bool = False

    def __eq__(self, other):
        return other.time == self.time and other.free == self.free

    def is_free(self, calendar: Calendar) -> bool:
        """
        Check whether TimeAttribute is satisfied.

        Time attribute is satisfied when:

        (1) Flow calendar's time (HH:MM) is equal to time attribute's time (HH:MM).
        (2) Or free flag is marked.

        Parameters
        ----------
        calendar

        Returns
        -------
        bool
        """
        if self.free:
            return True

        flow_time = calendar.flow_time
        hour = flow_time.hour
        minute = flow_time.minute
        if hour == self.time.hour and minute == self.time.minute:
            return True
        else:
            return False

    def reset(self):
        """
        Clear free flag
        """
        self.clear_free()

    def set_free(self):
        """
        Mark TimeAttribute's free flag.
        """
        self.free = True

    def clear_free(self):
        """
        Clear TimeAttribute's free flag.
        """
        self.free = False

    def calendar_changed(self, calendar: Calendar):
        """
        When calendar equals TimeAttribute's time, the TimeAttribute is satisfied.
        After this time, TimeAttribute should always be satisfied until Node is requeue.
        So when calendar changed, TimeAttributes should be checked.
        If it is satisfied, it should be marked free to prevent future time check.

        Parameters
        ----------
        calendar
            The calendar of a Flow
        """
        if self.free:
            return
        if self.is_free(calendar):
            self.set_free()

    # Serialization ----------------------------------------------

    def to_dict(self) -> Dict:
        result = dict(
            time=self.time.strftime("%H:%M"),
            free=self.free,
        )
        return result

    @classmethod
    def from_dict(
        cls, d: Dict, method: SerializationType = SerializationType.Status
    ) -> "TimeAttribute":
        time_string = d["time"]
        time_attr = TimeAttribute(time=time_string)
        if method == SerializationType.Status:
            is_free = d["free"]
            if is_free:
                time_attr.set_free()

        return time_attr
