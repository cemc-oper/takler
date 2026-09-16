Repeat：按日期循环
==================

本节介绍 **repeat** 属性：让一个节点（及其子树）按日期循环运行，
常用于「每天跑一遍」的业务流程。

定义 repeat
--------------

用 :py:meth:`~takler.core.node.Node.add_repeat` 给节点添加一个
:py:class:`~takler.core.RepeatDate` ：节点每次运行完一轮后，日期自动
推进一天并重新排队，直到超出结束日期。下面的例子让容器 ``daily``
从 2024-01-01 到 2024-01-03 每天运行一轮：

.. literalinclude:: /../examples/getting_started/step10_repeat.py
    :language: python
    :linenos:

关键代码是第 18 行的 repeat 定义。

``RepeatDate`` 的第一个参数是变量名。repeat 会为节点**生成一个同名
变量**，存放当前循环值（``YYYYMMDD`` 格式的整数），节点子树中的任务
都可以使用，包括在任务脚本中。``t1`` 的脚本通过 Jinja2 模板引用当前
日期：

.. literalinclude:: /../examples/getting_started/test/task1_with_repeat.takler
    :language: bash
    :linenos:
    :emphasize-lines: 3

运行上述脚本，打印结果中 ``daily`` 节点下会带有 repeat 行，
依次是当前值与取值范围 ``[起始, 结束]`` ：

.. code-block::

    |- test [unknown]
      |- daily [unknown]
          repeat TAKLER_DATE 20240101 [20240101, 20240103]
        |- t1 [unknown]

循环如何推进
--------------

带有 repeat 的节点变为 ``complete`` 时，调度器会把 repeat 的值推进到
下一天，并把该节点**重新排队** （子树中的任务随之重置），于是同一批
任务用新的日期再跑一轮；当日期推进到超出结束值时，节点保持
``complete`` 不再循环。本例中 ``daily`` 会分别以 ``TAKLER_DATE`` 为
20240101、20240102、20240103 运行三轮，然后整个工作流完成。

.. important::

    手动对节点执行 ``requeue`` 会把它的 repeat **重置回起始值**
    （自动推进循环时的内部重新排队不会重置）。如果想从循环中间的某天
    继续，requeue 之后需要再手动调整 repeat 的当前值。

.. note::

    takler 的 repeat 只有 ``RepeatDate`` 一种：没有 ecFlow 的整数、
    枚举等其他 repeat 变体。枚举式循环的替代写法是在 Python 里用
    ``for`` 循环直接生成一组任务或容器，完整的差异清单见
    :doc:`/guide/ecflow-differences` 。

repeat 的完整参考（取值校验、与 requeue 的交互）见用户指南的
:doc:`/guide/attributes/repeat` 。让节点等到一天中某个时刻才运行的
时间依赖见下一节 :doc:`time` 。

练习
-----

1. 修改 **test.py** ，给 ``daily`` 容器添加从上周一到上周五的
   ``RepeatDate`` ，并在任务脚本中打印生成的日期变量
2. 启动服务并运行工作流，用 ``show`` 观察三轮循环中 repeat 值的变化
3. 在循环跑到中间某天时 requeue 工作流，确认 repeat 被重置回起始日期
