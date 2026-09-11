事件 (event)
============

事件是一个命名的布尔开关，类型为 :py:class:`~takler.core.Event` ，
用于让运行中的任务向服务上报「某个关键点已到达」，从而提前放行下游
任务。任何节点（不限于任务）都可以挂事件。

定义
----

用 :py:meth:`Node.add_event <takler.core.node.Node.add_event>` 添加：

.. code-block:: python

    task1.add_event("a")                            # 初值 unset
    task1.add_event("b", initial_value=True)        # 初值 set

构造参数：

.. list-table::
    :header-rows: 1
    :widths: 25 75

    * - 参数
      - 说明
    * - ``name``
      - 事件名。同一节点上事件名不能重复；默认（ ``check=True`` ）
        重复添加抛出 ``RuntimeError`` ，传 ``check=False`` 可跳过检查
    * - ``initial_value``
      - 初始值，默认 ``False`` （ unset ）。 requeue 重置时回到这个值

示例
----

下面的例子中 ``t1`` 带有事件 ``a`` （第 19 行）， ``t2`` 的触发器在
事件置位时满足（第 25 行），不必等 ``t1`` 完成：

.. literalinclude:: /../examples/getting_started/step7_events_and_meters.py
    :language: python
    :linenos:
    :emphasize-lines: 19,25

``t1`` 的脚本在跑到一半时用 ``event`` child 命令置位事件
（第 9~10 行）：

.. literalinclude:: /../examples/getting_started/test/task1_with_events.takler
    :language: bash
    :linenos:
    :emphasize-lines: 9,10

取值与上报
----------

事件的值只能由外部改变：任务脚本中的 ``event`` child 命令把事件置为
set ，控制命令 ``force`` 可以置位或清回 unset （见
:doc:`/tutorial/going-further/controlling-the-flow` ）。调度器自己从不修改事件的值。
API 层面可用 ``node.set_event(name, value)`` 直接赋值；事件不存在时
该方法返回 ``False`` ，不报错。

在触发器中引用
--------------

事件用 ``路径:事件名`` 引用，与 ``set`` / ``unset`` 比较，例如
``./t1:a == set`` ；完整语法见 :doc:`/guide/trigger-expression` 。
事件与标尺、参数同名时查找顺序为事件 → 标尺 → 参数。

requeue 与序列化
----------------

* :py:meth:`Node.requeue <takler.core.node.Node.requeue>` 把事件重置回
  ``initial_value``
* 保存状态时序列化带当前值；只保存定义时恢复为 ``initial_value``
