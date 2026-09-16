日期循环 (RepeatDate)
=====================

repeat 让节点（及其子树）按取值序列循环运行，常用于「每天跑一遍」的
业务流程。 takler 目前只有一种 repeat 变体——按日期循环的
:py:class:`~takler.core.RepeatDate` 。

定义
----

用 :py:meth:`Node.add_repeat <takler.core.node.Node.add_repeat>`
添加：

.. code-block:: python

    daily.add_repeat(RepeatDate("TAKLER_DATE", 20240101, 20240103, step=1))

构造参数：

.. list-table::
    :header-rows: 1
    :widths: 25 75

    * - 参数
      - 说明
    * - ``name``
      - repeat 的名字，同时是生成的变量名
    * - ``start_date`` 、 ``end_date``
      - 起止日期（含端点）， ``YYYYMMDD`` 格式的 ``str`` 或 ``int``
    * - ``step``
      - 步进天数，默认 1

一个节点至多有一个 repeat ；再次调用 ``add_repeat`` 会替换掉已有的
repeat 。

示例
----

下面的例子让容器 ``daily`` 从 2024-01-01 到 2024-01-03 每天运行一轮
（第 18 行）：

.. literalinclude:: /../examples/getting_started/step10_repeat.py
    :language: python
    :linenos:
    :emphasize-lines: 18

repeat 为节点**生成一个同名变量**，取值为当前循环值（ ``YYYYMMDD``
格式的整数），节点子树中的任务可以在脚本里用 ``{{ TAKLER_DATE }}``
引用（第 3 行）：

.. literalinclude:: /../examples/getting_started/test/task1_with_repeat.takler
    :language: jinja
    :linenos:
    :emphasize-lines: 3

该变量也可以在触发器中比较，例如 ``./daily:TAKLER_DATE >= 20240102``
（语法见 :doc:`/guide/trigger-expression` ，变量生成规则见
:doc:`/guide/variables` ）。

循环推进
--------

带 repeat 的节点变为 ``complete`` 时，调度器把 repeat 推进到下一个
取值，并把该节点重新排队——这次内部 requeue **不会** 重置 repeat ，
于是子树用新的取值再跑一轮；推进到超出 ``end_date`` 时不再排队，
节点保持 ``complete`` 。手动执行
:py:meth:`Node.requeue <takler.core.node.Node.requeue>` 默认把 repeat
**重置回起始值** （ ``reset_repeat=True`` ）。

修改当前值
----------

想在循环中途跳到某天，用
:py:meth:`RepeatDate.change <takler.core.RepeatDate.change>` ，它会
校验取值：超出 ``[start_date, end_date]`` 或不在步进网格上（不是从
起始日起步进的整数倍）都抛出 ``ValueError`` 。直接给
:py:attr:`RepeatDate.value <takler.core.RepeatDate.value>` 赋值则
**不做校验** ，可能留下不在网格上的取值，应优先使用 ``change`` 。

requeue 与序列化
----------------

* 手动 requeue 把 repeat 重置回 ``start_date`` ；循环推进时的内部
  requeue 不重置
* 保存状态时序列化带当前值；只保存定义时当前值回到 ``start_date``
