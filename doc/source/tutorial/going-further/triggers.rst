触发器
========

前面章节中的任务是相互独立的，没有先后顺序。本节介绍触发器 (trigger)，
让一个任务的运行依赖于其他节点的状态、事件或标尺。

给任务添加触发器
------------------

使用 :py:meth:`~takler.core.node.Node.add_trigger` 给任意节点添加触发器。
下面的例子让任务 ``t2`` 依赖任务 ``t1`` 完成：

.. literalinclude:: /../examples/getting_started/step6_triggers.py
    :language: python
    :linenos:

第 21 行是新增的关键代码： ``task2.add_trigger("./t1 == complete")`` 。
``./t1`` 是相对路径，表示与 ``task2`` 同一层级的节点 ``t1`` （写法详见下一节）。

运行上述脚本，打印结果中会带上触发器表达式：

.. code-block::

    |- test [unknown]
      |- t1 [unknown]
      |- t2 [unknown]
          trigger ./t1 == complete

在服务运行过程中，调度器每次检查任务是否可以运行时都会重新求值触发器：
只要 ``t1`` 还没有变成 ``complete``，``t2`` 的触发器就不满足，调度器不会
提交 ``t2``；``t1`` 一旦完成，下一次检查时 ``t2`` 的触发器立即满足。

触发器表达式写法
------------------

触发器表达式的语法专门为 takler 设计（不是 ecFlow 的 ``.def`` 语言），
本节先给出常见写法，完整参考见用户指南的 :doc:`/guide/trigger-expression` 。

**节点路径**：

* ``/flow1/task1``：绝对路径，从根节点开始
* ``./task1``：相对路径，``.`` 表示当前节点所在的层级
* ``../task1``：相对路径，``..`` 表示上一级

**状态比较**：使用 ``==`` 或 ``eq`` （大小写不敏感），例如：

.. code-block::

    ./task1 == complete
    ../task1 eq aborted

.. important::

    触发器表达式里可用的状态词只有三个：``complete``、``aborted``、``active``。
    **没有** ``queued`` 或 ``submitted``：这两个状态都是任务尚未真正开始运行时
    经过的中间状态，触发器语义上只关心「有没有跑完」「跑没跑」「有没有出错」，
    所以不支持用它们做触发条件。

**逻辑运算符**：``and``、``or`` （大小写不敏感），可以用括号分组：

.. code-block::

    ./task1 == complete and ./task2 == complete
    (./task1 == aborted or ./task2 == aborted) and ./task3 == complete

**变量比较**：用 ``路径:变量名`` 的写法引用某个节点的变量、事件或标尺，
配合比较运算符 ``==``、``>``、``>=``、``<``、``<=`` 使用，例如：

.. code-block::

    ./task1:meter1 >= 4
    ./task1:event1 == set

事件与标尺的写法会在 :doc:`events` 与 :doc:`meters` 两节详细介绍。

.. note::

    除普通触发器外还有**完成触发器** (complete trigger)：表达式满足时
    节点直接判定为 ``complete`` 而不真正运行，详见下一节
    :doc:`complete-triggers` 。

练习
-----

1. 修改 **test.py**，给 ``t2`` 添加触发器，使其依赖 ``t1`` 的完成状态
2. 启动服务并 requeue 工作流，观察 ``t2`` 是否在 ``t1`` 完成之前保持等待
3. 尝试在触发器表达式中使用 ``and``、``or`` 与括号组合多个条件
