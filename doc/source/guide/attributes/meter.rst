标尺 (meter)
============

标尺是一个带取值范围的命名整数，类型为
:py:class:`~takler.core.Meter` ，用于让运行中的任务向服务上报进度
（百分比、已处理记录数等），下游任务可以按标尺取值放行。

定义
----

用 :py:meth:`Node.add_meter <takler.core.node.Node.add_meter>` 添加，
必须给出取值范围，初值是范围下限 ``min_value`` ：

.. code-block:: python

    task1.add_meter("step", 0, 100)     # 范围 [0, 100]，初值 0

给 :py:attr:`Meter.value <takler.core.Meter.value>` 赋超出
``[min_value, max_value]`` 的值会抛出 ``ValueError`` ——因此任务用
``meter`` child 命令上报越界值时，服务端会拒绝这次上报并返回失败响应。
``node.set_meter(name, value)`` 在标尺不存在时返回 ``False`` 。

示例
----

下面的例子中 ``t1`` 带有标尺 ``step`` （第 21 行）， ``t2`` 的触发器
在标尺达到 50 时满足（第 26 行）：

.. literalinclude:: /../examples/getting_started/step9_meters.py
    :language: python
    :linenos:
    :emphasize-lines: 21,26

``t1`` 的脚本在运行过程中用 ``meter`` child 命令多次上报进度
（第 3~4、7~8、11~12 行）：

.. literalinclude:: /../examples/getting_started/test/task1_with_meter.takler
    :language: bash
    :linenos:
    :emphasize-lines: 3,4,7,8,11,12

在触发器中引用
--------------

标尺用 ``路径:标尺名`` 引用并与整数比较（ ``==`` 、 ``>`` 、 ``>=`` 、
``<`` 、 ``<=`` ），例如 ``./t1:step >= 50`` ；完整语法见
:doc:`/guide/trigger-expression` 。

requeue 与序列化
----------------

* :py:meth:`Node.requeue <takler.core.node.Node.requeue>` 把标尺重置回
  ``min_value``
* 保存状态时序列化带当前值；只保存定义时恢复为 ``min_value``
