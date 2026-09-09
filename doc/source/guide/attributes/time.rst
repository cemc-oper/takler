时间依赖 (time 与日历)
======================

time 依赖让节点等到 flow 的逻辑时钟到达一天中的某个时刻才可运行，
类型为 :py:class:`~takler.core.TimeAttribute` 。

定义
----

用 :py:meth:`Node.add_time <takler.core.node.Node.add_time>` 添加，
参数是 ``"HH:MM"`` 格式的字符串或 ``datetime.time`` 对象：

.. code-block:: python

    task2.add_time("12:00")                     # "HH:MM" 字符串
    task2.add_time(datetime.time(18, 30))       # 或 datetime.time

一个节点可以挂多个 time 依赖，**任意一个** 满足即可运行。

示例
----

下面的例子给 ``t2`` 加了一个 12:00 的时间依赖（第 26 行）：

.. literalinclude:: /../examples/getting_started/step9_repeat_and_time.py
    :language: python
    :linenos:
    :emphasize-lines: 26

判定规则
--------

time 依赖在 flow 逻辑时钟的「时：分」与设定值相等时满足——比较精确
到分钟，忽略秒。满足的瞬间，日历更新回调会给该 time 依赖置位一个
``free`` 闩锁，此后它**一直保持满足**，直到节点被 requeue 时清除。
因此「 12:00 的依赖」在 12:00 这一分钟里被闩锁， 12:01 之后检查
仍然通过。

.. important::

    闩锁只在日历更新恰好经过那一分钟时置位。如果 time 依赖是在其
    时刻过去之后才添加的（或服务在该分钟没有运行），当天不会再
    满足，要等到下一天同一时刻。 ``free-dep`` 命令（
    :py:meth:`Node.free_dependencies <takler.core.node.Node.free_dependencies>`
    的 ``"time"`` 类型）可以手动置位闩锁，立即解除时间依赖。

对不在任何 flow 中的节点检查时间依赖会抛出 ``RuntimeError`` ——
time 依赖离不开 flow 的日历。

日历 (Calendar)
---------------

每个 flow 有一个逻辑日历
:py:class:`~takler.core.calendar.Calendar` ：

* :py:meth:`Flow.begin <takler.core.Flow.begin>` 以当前真实时间启动
  逻辑时钟
* 调度器主循环周期性调用
  :py:meth:`Flow.update_calendar <takler.core.Flow.update_calendar>` ，
  按真实时间的流逝推进逻辑时间，随后更新 ``DATE`` / ``TIME`` 变量
  （见 :doc:`/guide/variables` ）并检查各节点 time 依赖的闩锁
* :py:meth:`Flow.requeue <takler.core.NodeContainer.requeue>` 只重置
  节点树， **不** 触碰日历；重新计时要用 ``begin(force=True)``

requeue 与序列化
----------------

* :py:meth:`Node.requeue <takler.core.node.Node.requeue>` 清除 time
  依赖的 ``free`` 闩锁，节点重新等待下一次时刻匹配
* 保存状态时序列化带 ``free`` 闩锁（恢复后已满足的时刻不需要再等）；
  只保存定义时闩锁清除
