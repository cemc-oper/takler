状态模型
========

本页说明 takler 的节点状态：六个状态的含义、状态如何变化、容器状态如何
从子节点聚合，以及与状态正交的 ``suspended`` 标记。

节点状态
--------

每个节点在任何时刻都处于 :py:class:`~takler.core.NodeStatus` 的六个状态
之一：

* ``unknown`` ：初始状态。flow 尚未 ``begin`` 时，所有节点都是
  ``unknown``
* ``queued`` ：排队中，等待依赖（触发器、时间、限额）满足
* ``submitted`` ：作业已提交，尚未开始运行
* ``active`` ：作业正在运行
* ``complete`` ：成功完成
* ``aborted`` ：异常终止（作业报错、脚本调用了 ``abort`` 等）

``NodeStatus`` 是有序枚举，数值关系为：

.. code-block::

    unknown < complete < queued < submitted < active < aborted

这个顺序在容器状态聚合中起决定性作用（见下文「容器状态聚合」）。

任务的生命周期
--------------

一个任务的状态通常沿下图迁移：

.. mermaid::

    flowchart LR
        unknown -->|begin| queued
        queued -->|依赖满足，提交作业| submitted
        submitted -->|init| active
        active -->|complete| complete
        active -->|abort 或作业失败| aborted
        complete -->|requeue| queued
        aborted -->|requeue| queued

状态变化有两个来源：

* **child 命令上报** ：作业脚本中的 ``init`` / ``complete`` / ``abort``
  把任务置为 ``active`` / ``complete`` / ``aborted`` （见
  :doc:`/tutorial/getting-started/understanding-includes` ）
* **控制命令** ： ``requeue`` 把节点重置回默认状态； ``force`` 直接改写
  状态； ``run`` 触发一次作业提交（见
  :doc:`/tutorial/advanced-topics/controlling-the-flow` ）

容器状态聚合
------------

容器（以及 flow）没有独立决定的状态：它的状态等于全部子节点状态中数值
最大——即「最重要」——的那一个：

.. code-block::

    aborted > active > submitted > queued > complete > unknown

举几个例子：

* 子节点是 ``complete`` 与 ``active`` 各一个时，容器显示 ``active``
* 只要有一个子节点是 ``aborted`` ，容器就显示 ``aborted`` ，即使其余
  子节点都已经 ``complete``
* 全部子节点都 ``complete`` 时，容器才是 ``complete``

这个规则沿树递归：多层容器嵌套时，从最深层开始逐层取最大值，一直汇总
到 flow 层。

.. note::

    注意 ``complete`` 在顺序中排在 ``queued`` 之前：一个子节点完成后，
    只要还有兄弟节点在排队（ ``queued`` ），容器就显示 ``queued`` 而不是
    ``complete`` 。

状态传播：sink 与 swim
----------------------

状态变化沿节点树向两个方向传播：

* **swim（上浮）** ：任务状态变化后，其父容器按上面的规则重新聚合，
  变化一路向上直到 flow 。聚合结果为 ``complete`` 且节点带 repeat 时，
  还会顺带推进 repeat 并自动 requeue 该节点（见
  :doc:`/tutorial/advanced-topics/repeat` ）
* **sink（下沉）** ： ``requeue`` 一个容器时，重置操作向下作用到整棵
  子树； ``force --recursive`` 置状态同理

与状态正交的 suspended
--------------------------

节点的完整状态是 :py:class:`~takler.core.State` ，包含 ``node_status``
与 ``suspended`` 两部分，二者正交：挂起一个节点**不改变它的状态**，只是
让调度器跳过它——挂起的节点不会被自动调度， ``show`` 输出中显示为
``suspend (状态)`` 。挂起容器时调度器不再向下遍历，因此整棵子树都停止
调度。详见 :doc:`/tutorial/advanced-topics/controlling-the-flow` 。

默认状态：default_node_status
------------------------------------

``requeue`` 把节点重置回它的 ``default_node_status`` ，默认为
``queued`` 。可以通过
:py:meth:`Node.set_default_node_status <takler.core.node.Node.set_default_node_status>`
修改，但只允许 ``queued`` 与 ``complete`` 两个取值—— ``submitted`` 、
``active`` 、 ``aborted`` 是运行中的瞬态，不能作为重置目标，设置时会
抛出 ``UnsupportedValueError`` 。

把默认状态设为 ``complete`` 的效果： ``requeue`` 之后该节点直接显示为
完成而不是重新排队。若容器（或 flow）的默认状态是 ``complete`` ，
``requeue`` 它时整个子树都会被下沉为 ``complete`` ——适合用来标记
「默认不需要运行、需要时才手动 run」的分支。
