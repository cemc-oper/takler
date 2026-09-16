事件
======

前面章节介绍了用其他节点的**状态**做触发条件。任务在运行过程中还可以
主动向服务上报自己的进度，让下游任务不必等到上游完全结束就可以开始
运行。本节介绍第一种进度属性：**事件 (event)** ——一个布尔开关，
任务在跑到某个关键点时把它置位。

与状态不同，事件的值完全由任务自己上报，调度器不会自动修改它。

定义事件
----------

使用 :py:meth:`~takler.core.node.Node.add_event` 给节点添加事件。
下面的例子中 ``t1`` 带有一个事件 ``a`` ；``t2`` 用它做触发条件：

.. literalinclude:: /../examples/getting_started/step8_events.py
    :language: python
    :linenos:

关键代码是第 21 行的事件定义与第 26 行的触发器表达式
``./t1:a == set`` ：``t1`` 的事件 ``a`` 被置位时满足。事件只有
``set`` 和 ``unset`` 两种取值，``./t1:a == unset`` 则表示事件尚未
被置位。

事件默认初值为 ``unset`` ，可以用 ``add_event("a", initial_value=True)``
让它从 ``set`` 开始。

运行上述脚本，打印结果中会带上事件与触发器表达式：

.. code-block::

    |- test [unknown]
      |- t1 [unknown]
          event a [unset]
      |- t2 [unknown]
          trigger ./t1:a == set

在脚本中上报事件
----------------------

事件不会自己变化，需要在任务脚本中用 child 命令上报。``t1`` 的脚本
在运行到一半时置位事件：

.. literalinclude:: /../examples/getting_started/test/task1_with_event.takler
    :language: bash
    :linenos:
    :emphasize-lines: 5,6

第 5~6 行的 ``event`` child 命令把事件 ``a`` 置为 ``set`` ——此刻
``t1`` 还没有运行结束，但 ``t2`` 的触发器已经满足，调度器下一次检查
时就会放行 ``t2`` 。这就是「``t1`` 跑到一半就放行 ``t2`` 」。

脚本中使用的 child 命令写法如下（``takler-client-py`` 会从环境变量
``TAKLER_NAME`` 读取节点路径，无需显式传 ``--node-path`` ）：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client event --host ${TAKLER_HOST} --port ${TAKLER_PORT} \
                  --node-path ${TAKLER_NAME} --event-name a

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py event --host ${TAKLER_HOST} --port ${TAKLER_PORT} \
                  --event-name a

``event`` 命令只负责置位；如需把事件清回 ``unset`` ，要用控制命令
``force`` （见高级话题的
:doc:`/tutorial/advanced-topics/controlling-the-flow` 一节）。

查看事件
--------------

运行中的工作流可以用 ``--show-event`` 查看每个节点事件的当前值
（该选项默认开启，这里显式写出）：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client show --show-event

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py show --show-event

事件的生命周期
--------------------

.. important::

    对节点执行 ``requeue`` 时，它的事件会被**重置回初始值**
    （ ``initial_value`` ）。因此重新排队一轮工作流后，下游基于事件的
    触发器会重新等待上游再次上报。

事件的完整行为（参数、边界情况、与 requeue 和序列化的交互）见用户指南的
:doc:`/guide/attributes/event` 。

练习
-----

1. 修改 **test.py** ，给 ``t1`` 添加事件 ``a`` ，并让 ``t2`` 依赖
   ``./t1:a == set``
2. 在 ``t1`` 的脚本中插入 ``event`` 命令，启动服务并 requeue 工作流，
   用 ``show --show-event`` 观察上报过程
3. requeue 工作流，确认事件被重置回初始值
4. 把 ``t2`` 的触发条件改为 ``./t1:a == unset`` ，观察它在 ``t1``
   上报之前就运行的效果
