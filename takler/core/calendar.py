import datetime
from typing import Optional


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
