属性
==================================

本页收录 ``takler.core`` 的节点属性对象：状态、参数、时间、触发器、
限制、重复、事件、标尺，以及序列化模式枚举。

State
----------

.. autosummary::
   :toctree: generated

   takler.core.State
   takler.core.NodeStatus

Parameter
----------

.. autosummary::
   :toctree: generated

   takler.core.Parameter

Time
------

.. autosummary::
   :toctree: generated

   takler.core.TimeAttribute

Trigger
--------

.. autosummary::
   :toctree: generated

   takler.core.expression.Expression

Limit
------

.. autosummary::
   :toctree: generated

   takler.core.Limit
   takler.core.InLimit
   takler.core.limit.InLimitManager

Repeat
-------

.. autosummary::
   :toctree: generated

   takler.core.Repeat
   takler.core.repeat.RepeatBase
   takler.core.RepeatDate

Event
-------

.. autosummary::
   :toctree: generated

   takler.core.Event

Meter
------

.. autosummary::
   :toctree: generated

   takler.core.Meter

序列化
------

``SerializationType`` 区分 ``to_dict`` / ``from_dict`` 的两种模式
（ ``Tree`` 与 ``Status`` ），机制见 :doc:`/develop/core-design` 的序
列化一节。

.. autosummary::
   :toctree: generated

   takler.core.SerializationType
