时间依赖 (time)
==================

上一节介绍的 repeat 控制任务「哪天运行」。本节介绍 **time** 属性：
让节点等到一天中的某个时刻才允许运行。

定义时间依赖
------------------

用 :py:meth:`~takler.core.node.Node.add_time` 给节点添加时间依赖，
参数是 ``"HH:MM"`` 格式的字符串（或 ``datetime.time`` 对象）。
下面的例子给 ``t2`` 加了一个 12:00 的时间依赖：

.. literalinclude:: /../examples/getting_started/step11_time.py
    :language: python
    :linenos:

关键代码是第 22 行的时间依赖。带有时间依赖的节点，只有当
**flow 的逻辑时钟** 到达该时刻后才允许运行。

运行上述脚本，打印结果中 ``t2`` 节点下会带有 time 行：

.. code-block::

    |- test [unknown]
      |- t1 [unknown]
      |- t2 [unknown]
          time 12:00

逻辑日历
--------------

每个 flow 都有一个 :py:class:`~takler.core.calendar.Calendar`
（逻辑日历）：

* :py:meth:`~takler.core.Flow.begin` 启动日历时把逻辑时间设为
  当前真实时间
* 调度器的主循环不断调用
  :py:meth:`~takler.core.Flow.update_calendar` ，按真实时间的流逝
  推进逻辑时间，并检查各节点的时间依赖

逻辑时间一旦到达 ``add_time`` 设定的时刻，该时间依赖就被满足，并且
**保持满足** （内部置位一个 ``free`` 标记），直到节点被 requeue 时
重置。因此任务不会因为「错过了那一分钟」而永远等待。一个节点可以添加
多个时间依赖，任意一个满足即可运行。

此外，flow 会根据逻辑日历生成 ``DATE`` （如 ``2024-01-01`` ）与
``TIME`` （如 ``12:00`` ）两个变量，任务脚本中可以像普通变量一样引用。

.. note::

    takler 的时间属性只有 ``time`` 一种（每日固定时刻）：没有 ecFlow
    的 ``cron`` 、 ``date`` 、 ``day`` 属性，也不支持 ``time`` 的
    区间形式。复杂的定时需求可以用多个 ``time`` 依赖组合表达。
    完整的差异清单见 :doc:`/guide/ecflow-differences` 。

时间依赖的完整参考（取值校验、闩锁语义、与日历的交互）见用户指南的
:doc:`/guide/attributes/time` 。运行中可以用 ``free-dep`` 命令手动
释放时间依赖，见 :doc:`controlling-the-flow` 一节。

练习
-----

1. 修改 **test.py** ，给某个任务添加一个几分钟后到期的 ``time``
   依赖，观察它在到期前保持排队、到期后自动运行
2. 给同一任务添加两个 ``time`` 依赖，确认任意一个到期即可运行
3. 在任务脚本中打印 ``DATE`` 与 ``TIME`` 两个生成变量
4. requeue 带有已满足时间依赖的任务，确认它重新开始等待（``free``
   闩锁被重置）
