from typing import Union, Optional, Dict

from .util import SerializationType

# Bunch level
TAKLER_HOST = "TAKLER_HOST"
TAKLER_PORT = "TAKLER_PORT"
TAKLER_HOME = "TAKLER_HOME"

# Flow level
FLOW = "FLOW"
TAKLER_DATE = "TAKLER_DATE"
TAKLER_TIME = "TAKLER_TIME"

TIME = "TIME"
DATE = "DATE"

# Task level
TASK = "TASK"
TAKLER_NAME = "TAKLER_NAME"
TAKLER_RID = "TAKLER_RID"
TAKLER_TRY_NO = "TAKLER_TRY_NO"
TAKLER_PASS = "TAKLER_PASS"

TAKLER_TRIES = "TAKLER_TRIES"


class Parameter(object):
    def __init__(self, name: str, value: Optional[Union[str, int, float, bool]] = None):
        self.name: str = name
        self._value: Optional[Union[str, int, float, bool]] = value

    def __repr__(self):
        return f"Parameter<{self.name}, {self.value}>"

    def __eq__(self, other):
        return (
            type(other) is type(self)
            and other.name == self.name
            and other.value == self.value
        )

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, v: Optional[Union[str, int, float, bool]]):
        self._value = v

    # Serialization ---------------------------------------

    def to_dict(self) -> Dict:
        """

        Returns
        -------
        Dict
            ::

                {
                    "name": parameter name,
                    "value": parameter value, str or int or float or boolean
                }

        """
        result = dict(name=self.name, value=self._value)
        return result

    @classmethod
    def from_dict(
        cls, d: Dict, method: SerializationType = SerializationType.Status
    ) -> "Parameter":
        """

        Parameters
        ----------
        d
            ::

                {
                    "name": parameter name,
                    "value": parameter value, str or int or float or boolean
                }

        method

            - ``SerializationType.Status``
            - ``SerializationType.Tree``

        Returns
        -------
        Parameter
        """
        name = d["name"]
        value = d["value"]
        param = Parameter(name, value)
        return param
