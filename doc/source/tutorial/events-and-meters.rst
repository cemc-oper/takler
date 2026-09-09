事件与标尺
==========

上一节介绍了用其他节点的**状态**做触发条件。任务在运行过程中还可以主动向
服务上报自己的进度，让下游任务不必等到上游完全结束就可以开始运行。
本节介绍两种进度属性：

* **事件 (event)** ：一个布尔开关，任务在跑到某个关键点时把它置位
* **标尺 (meter)** ：一个有取值范围的整数，任务在运行中不断更新它

与状态不同，事件和标尺的值完全由任务自己上报，调度器不会自动修改它们。

定义事件与标尺
--------------

使用 :py:meth:`~takler.core.node.Node.add_event` 和
:py:meth:`~takler.core.node.Node.add_meter` 给节点添加事件与标尺。
下面的例子中 ``t1`` 带有一个事件 ``a`` 和一个取值范围 0~100 的标尺
``step``；``t2`` 和 ``t3`` 分别用它们做触发条件：

.. literalinclude:: /../examples/getting_started/step7_events_and_meters.py
    :language: python
    :linenos:

关键代码是第 18~19 行的属性定义，以及第 24 行与第 29 行的触发器表达式：

* ``./t1:a == set``：``t1`` 的事件 ``a`` 被置位时满足。事件只有 ``set``
  和 ``unset`` 两种取值，``./t1:a == unset`` 则表示事件尚未被置位
* ``./t1:step >= 50``：``t1`` 的标尺 ``step`` 达到 50 时满足。标尺支持
  ``==``、``>``、``>=``、``<``、``<=`` 等数值比较

标尺在创建时必须给出取值范围，初值是范围的下限；上报超出范围的值会被拒绝。
事件默认初值为 ``unset``，可以用 ``add_event("a", initial_value=True)``
让它从 ``set`` 开始。

运行上述脚本，打印结果中会带上事件、标尺与触发器表达式：

.. code-block::

    |- test [unknown]
      |- t1 [unknown]
          event a [unset]
          meter step 0 100 [0]
      |- t2 [unknown]
          trigger ./t1:a == set
      |- t3 [unknown]
          trigger ./t1:step >= 50

在脚本中上报事件与标尺
----------------------

事件和标尺不会自己变化，需要在任务脚本中用 child 命令上报。``t1`` 的脚本
在运行过程中更新标尺、并在完成一半时置位事件：

.. literalinclude:: /../examples/getting_started/test/task1_with_events.takler
    :language: bash
    :linenos:
    :emphasize-lines: 3,8,10,14

第 10 行的 ``event`` child 命令把事件 ``a`` 置为 ``set``——此刻 ``t1``
还没有运行结束，但 ``t2`` 的触发器已经满足，调度器下一次检查时就会放行
``t2``。这就是「``t1`` 跑到一半就放行 ``t2``」。

脚本中使用的两个 child 命令写法如下（``takler-client-py`` 会从环境变量
``TAKLER_NAME`` 读取节点路径，无需显式传 ``--node-path``）：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client event --host ${TAKLER_HOST} --port ${TAKLER_PORT} \
                  --node-path ${TAKLER_NAME} --event-name a
            takler_client meter --host ${TAKLER_HOST} --port ${TAKLER_PORT} \
                  --node-path ${TAKLER_NAME} --meter-name step --meter-value 50

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py event --host ${TAKLER_HOST} --port ${TAKLER_PORT} \
                  --event-name a
            takler-client-py meter --host ${TAKLER_HOST} --port ${TAKLER_PORT} \
                  --meter-name step --meter-value 50

``event`` 命令只负责置位；如需把事件清回 ``unset``，要用控制命令
``force`` （后续章节介绍）。

查看事件与标尺
--------------

运行中的工作流可以用 ``--show-event`` 与 ``--show-meter`` 查看每个节点
事件与标尺的当前值（两个选项默认开启，这里显式写出）：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client show --show-event --show-meter

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py show --show-event --show-meter

事件与标尺的完整行为（参数、边界情况、与 requeue 和序列化的交互）见
用户指南的 :doc:`/guide/attributes/event` 与
:doc:`/guide/attributes/meter` 。

事件与标尺的生命周期
--------------------

.. important::

    对节点执行 ``requeue`` 时，它的事件和标尺会被**重置回初始值**：
    事件回到 ``initial_value``，标尺回到取值范围下限。因此重新排队一轮
    工作流后，下游基于事件与标尺的触发器会重新等待上游再次上报。

.. note::

    ecFlow 中用于展示文本信息的 ``label`` 属性在 takler 中**不存在**，
    也没有对应的 child 命令。简单的进度信息可以用事件与标尺表达；
    更复杂的信息可以写进作业输出文件再查看。

练习
-----

1. 修改 **test.py**，给 ``t1`` 添加事件 ``a`` 与标尺 ``step``，
   并让 ``t2`` 依赖 ``./t1:a == set``
2. 在 ``t1`` 的脚本中插入 ``event`` 与 ``meter`` 命令，启动服务并
   requeue 工作流，用 ``show --show-event --show-meter`` 观察上报过程
3. 把 ``t3`` 的触发条件改为标尺等于 ``100``，观察它与 ``t1`` 完成的先后
4. requeue 工作流，确认事件与标尺都被重置回初始值
