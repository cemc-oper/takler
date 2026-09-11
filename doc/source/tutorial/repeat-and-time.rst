Repeat 与时间
===============

本节介绍两种与时间有关的属性：

* **repeat** ：让一个节点（及其子树）按日期循环运行，常用于「每天跑一遍」
  的业务流程
* **time** ：让节点等到一天中的某个时刻才允许运行

Repeat：按日期循环
--------------------

用 :py:meth:`~takler.core.node.Node.add_repeat` 给节点添加一个
:py:class:`~takler.core.RepeatDate`：节点每次运行完一轮后，日期自动
推进一天并重新排队，直到超出结束日期。下面的例子让容器 ``daily``
从 2024-01-01 到 2024-01-03 每天运行一轮，同时给 ``t2`` 加了一个
时间依赖：

.. literalinclude:: /../examples/getting_started/step9_repeat_and_time.py
    :language: python
    :linenos:

关键代码是第 18 行的 repeat 定义与第 26 行的时间依赖。

``RepeatDate`` 的第一个参数是变量名。repeat 会为节点**生成一个同名变量**，
存放当前循环值（``YYYYMMDD`` 格式的整数），节点子树中的任务都可以使用，
包括在任务脚本中。``t1`` 的脚本通过 Jinja2 模板引用当前日期：

.. literalinclude:: /../examples/getting_started/test/task1_with_repeat.takler
    :language: bash
    :linenos:
    :emphasize-lines: 3

运行上述脚本，打印结果中 ``daily`` 节点下会带有 repeat 行，
依次是当前值与取值范围 ``[起始, 结束]``：

.. code-block::

    |- test [unknown]
      |- daily [unknown]
          repeat TAKLER_DATE 20240101 [20240101, 20240103]
        |- t1 [unknown]
      |- t2 [unknown]
          time 12:00

循环如何推进
--------------

带有 repeat 的节点变为 ``complete`` 时，调度器会把 repeat 的值推进到
下一天，并把该节点**重新排队** （子树中的任务随之重置），于是同一批任务
用新的日期再跑一轮；当日期推进到超出结束值时，节点保持 ``complete``
不再循环。本例中 ``daily`` 会分别以 ``TAKLER_DATE`` 为 20240101、
20240102、20240103 运行三轮，然后整个工作流完成。

.. important::

    手动对节点执行 ``requeue`` 会把它的 repeat **重置回起始值**
    （自动推进循环时的内部重新排队不会重置）。如果想从循环中间的某天
    继续，requeue 之后需要再手动调整 repeat 的当前值。

时间依赖 (time)
------------------

用 :py:meth:`~takler.core.node.Node.add_time` 给节点添加时间依赖，
参数是 ``"HH:MM"`` 格式的字符串（或 ``datetime.time`` 对象）。
带有时间依赖的节点，只有当 **flow 的逻辑时钟** 到达该时刻后才允许运行。

每个 flow 都有一个 :py:class:`~takler.core.calendar.Calendar`
（逻辑日历）：

* :py:meth:`~takler.core.Flow.begin` 启动日历时把逻辑时间设为
  当前真实时间
* 调度器的主循环不断调用
  :py:meth:`~takler.core.Flow.update_calendar`，按真实时间的流逝
  推进逻辑时间，并检查各节点的时间依赖

逻辑时间一旦到达 ``add_time`` 设定的时刻，该时间依赖就被满足，并且
**保持满足** （内部置位一个 ``free`` 标记），直到节点被 requeue 时重置。
因此任务不会因为「错过了那一分钟」而永远等待。一个节点可以添加多个
时间依赖，任意一个满足即可运行。

此外，flow 会根据逻辑日历生成 ``DATE`` （如 ``2024-01-01``）与 ``TIME``
（如 ``12:00``）两个变量，任务脚本中可以像普通变量一样引用。

.. note::

    takler 的时间属性只有本节介绍的 ``RepeatDate`` 与 ``time`` 两种：
    没有 ecFlow 的 ``cron``、``date``、``day`` 属性，也没有整数、枚举等
    其他 repeat 变体。复杂的定时需求可以用多个 ``time`` 依赖组合表达。
    完整的差异清单见 :doc:`/guide/ecflow-differences` 。

repeat 与 time 的完整参考（取值校验、闩锁语义、与日历的交互）见用户
指南的 :doc:`/guide/attributes/repeat` 与 :doc:`/guide/attributes/time` 。

练习
-----

1. 修改 **test.py**，给 ``group1`` 添加从上周一到上周五的 ``RepeatDate``，
   并在任务脚本中打印生成的日期变量
2. 启动服务并运行工作流，用 ``show`` 观察三轮循环中 repeat 值的变化
3. 给某个任务添加一个几分钟后到期的 ``time`` 依赖，观察它在到期前保持
   排队、到期后自动运行
4. 在循环跑到中间某天时 requeue 工作流，确认 repeat 被重置回起始日期
