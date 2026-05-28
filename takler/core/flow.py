import datetime
from typing import TYPE_CHECKING, Optional, Dict

from pydantic import BaseModel, ConfigDict

from .node_container import NodeContainer
from .calendar import Calendar
from .parameter import Parameter, DATE, TIME

if TYPE_CHECKING:
    from .bunch import Bunch


class Flow(NodeContainer):
    def __init__(self, name: str):
        super(Flow, self).__init__(name)

        self.bunch: Optional[Bunch] = None
        self.calendar: Calendar = Calendar()

        self.generated_parameters: FlowGeneratedParameters = FlowGeneratedParameters(flow=self)

    # Node access --------------------------------------

    def get_bunch(self) -> "Bunch":
        return self.bunch

    # Parameter ----------------------------------------

    def find_parent_parameter(self, name: str) -> Optional[Parameter]:
        p = super(Flow, self).find_parent_parameter(name)
        if p is not None:
            return p

        if self.bunch is None:
            return None

        return self.bunch.find_parent_parameter(name)

    # Calendar ----------------------------------------

    def requeue_calendar(self):
        """
        Requeue calendar with current time.
        """
        suite_time = datetime.datetime.now()
        self.calendar.begin(suite_time)

    def update_calendar(self, time: datetime.datetime):
        """
        Update calendar using given time. Used in scheduler's main loop.

        After generated parameters are updated, ``calendar_changed`` is called to update time attributes.

        Parameters
        ----------
        time
        """
        self.calendar.update(time)
        self.update_generated_parameters()
        self.calendar_changed(self.calendar)

    # Parameter ---------------------------------------------------

    def find_generated_parameter(self, name: str) -> Optional[Parameter]:
        param = self.generated_parameters.find_parameter(name)
        if param is not None:
            return param

        return super(Flow, self).find_generated_parameter(name)

    def update_generated_parameters(self):
        self.generated_parameters.update_parameters()
        super(Flow, self).update_generated_parameters()

    def generated_parameters_only(self) -> Dict[str, Parameter]:
        params = super(Flow, self).generated_parameters_only()
        params.update(self.generated_parameters.generated_parameters())
        return params

    # Node Operation ---------------------------------------
    def requeue(self, reset_repeat: bool = True):
        self.requeue_calendar()
        super(Flow, self).requeue(reset_repeat=reset_repeat)


class FlowGeneratedParameters(BaseModel):
    """
    Generated parameters for a Flow.

    Attributes
    -----------
    flow
        parent Flow object for parameters.
    date
        current date
    time
        current time
    """
    flow: Flow
    date: Parameter = Parameter(DATE, None)
    time: Parameter = Parameter(TIME, None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

    def update_parameters(self):
        """
        Update generated parameters from Flow node's attrs.
        """
        flow_time = self.flow.calendar.flow_time
        self.date.value = flow_time.strftime("%Y-%m-%d")
        self.time.value = flow_time.strftime("%H:%M")

    def find_parameter(self, name: str) -> Optional[Parameter]:
        if name == DATE:
            return self.date
        elif name == TIME:
            return self.time
        else:
            return None

    def generated_parameters(self) -> Dict[str, Parameter]:
        return {
            DATE: self.date,
            TIME: self.time,
        }
