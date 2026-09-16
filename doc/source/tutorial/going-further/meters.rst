标尺
======

上一节介绍的事件是一个布尔开关，只能表达「到了/还没到」。本节介绍
**标尺 (meter)** ——一个有取值范围的整数，任务在运行中不断更新它，
下游任务可以按进度数值决定何时启动。典型场景是「上游处理到一半的数据
已经可用，下游先开始处理这部分」。

与状态不同，标尺的值完全由任务自己上报，调度器不会自动修改它。

定义标尺
----------

使用 :py:meth:`~takler.core.node.Node.add_meter` 给节点添加标尺。
下面的例子中 ``t1`` 带有一个取值范围 0~100 的标尺 ``step`` ；
``t2`` 用它做触发条件：

.. literalinclude:: /../examples/getting_started/step9_meters.py
    :language: python
    :linenos:

关键代码是第 21 行的标尺定义与第 26 行的触发器表达式
``./t1:step >= 50`` ：``t1`` 的标尺 ``step`` 达到 50 时满足。标尺
支持 ``==`` 、 ``>`` 、 ``>=`` 、 ``<`` 、 ``<=`` 等数值比较。

标尺在创建时必须给出取值范围，初值是范围的下限；上报超出范围的值会被
拒绝。

运行上述脚本，打印结果中会带上标尺与触发器表达式：

.. code-block::

    |- test [unknown]
      |- t1 [unknown]
          meter step 0 100 [0]
      |- t2 [unknown]
          trigger ./t1:step >= 50

在脚本中上报标尺
----------------------

标尺不会自己变化，需要在任务脚本中用 child 命令上报。``t1`` 的脚本
在运行过程中不断更新标尺：

.. literalinclude:: /../examples/getting_started/test/task1_with_meter.takler
    :language: bash
    :linenos:
    :emphasize-lines: 3,7,11

第 7~8 行把标尺更新为 50 ——此刻 ``t1`` 还没有运行结束，但 ``t2`` 的
触发器已经满足，调度器下一次检查时就会放行 ``t2`` 。

脚本中使用的 child 命令写法如下（``takler-client-py`` 会从环境变量
``TAKLER_NAME`` 读取节点路径，无需显式传 ``--node-path`` ）：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client meter --host ${TAKLER_HOST} --port ${TAKLER_PORT} \
                  --node-path ${TAKLER_NAME} --meter-name step --meter-value 50

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py meter --host ${TAKLER_HOST} --port ${TAKLER_PORT} \
                  --meter-name step --meter-value 50

查看标尺
--------------

运行中的工作流可以用 ``--show-meter`` 查看每个节点标尺的当前值
（该选项默认开启，这里显式写出）：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client show --show-meter

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py show --show-meter

标尺的生命周期
--------------------

.. important::

    对节点执行 ``requeue`` 时，它的标尺会被**重置回取值范围下限**。
    因此重新排队一轮工作流后，下游基于标尺的触发器会重新等待上游再次
    上报。

.. note::

    ecFlow 中用于展示文本信息的 ``label`` 属性在 takler 中**不存在**，
    也没有对应的 child 命令。简单的进度信息可以用事件与标尺表达；
    更复杂的信息可以写进作业输出文件再查看。takler 与 ecFlow 的完整
    差异清单见 :doc:`/guide/ecflow-differences` 。

标尺的完整行为（参数、边界情况、与 requeue 和序列化的交互）见用户指南的
:doc:`/guide/attributes/meter` 。

练习
-----

1. 修改 **test.py** ，给 ``t1`` 添加标尺 ``step`` （0~100），并让
   ``t2`` 依赖 ``./t1:step >= 50``
2. 在 ``t1`` 的脚本中插入 ``meter`` 命令，启动服务并 requeue 工作流，
   用 ``show --show-meter`` 观察上报过程
3. 把 ``t2`` 的触发条件改为标尺等于 ``100`` ，观察它与 ``t1`` 完成的
   先后
4. 在脚本中尝试上报超出范围的值（如 ``101`` ），观察命令报错
