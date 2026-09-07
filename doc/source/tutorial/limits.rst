限额
======

前面几节介绍的触发器、事件与标尺控制的是任务之间的**先后依赖**。本节介绍
限额 (limit)，它控制的是**并发数量**：最多允许多少个任务同时运行。
典型场景是底层资源有限——比如只有两台计算节点可用，或者某个服务最多
承受两个并发连接。

定义限额
----------

限额分两步设置：

1. 用 :py:meth:`~takler.core.node.Node.add_limit` 在某个节点上定义一个
   **限额** （一个命名的 :py:class:`~takler.core.Limit` 令牌池），
   给出令牌总数
2. 用 :py:meth:`~takler.core.node.Node.add_in_limit` 给要受限的任务添加
   **in-limit 标记**，声明它在运行期间占用该限额的令牌（默认 1 个）

下面的例子在容器 ``group1`` 上定义了最多 2 个令牌的限额 ``work``，
组内 4 个任务都声明占用它：

.. literalinclude:: /../examples/getting_started/step8_limits.py
    :language: python
    :linenos:

关键代码是第 18 行的限额定义，以及第 23、27、31、35 行的 in-limit 标记。

运行上述脚本，打印结果中 ``group1`` 节点下会带有限额行，
``[0/2]`` 表示当前已占用 0 个、共 2 个令牌：

.. code-block::

    |- test [unknown]
      |- group1 [unknown]
          limit work [0/2]
        |- t1 [unknown]
        |- t2 [unknown]
        |- t3 [unknown]
        |- t4 [unknown]

限额如何生效
--------------

调度器在提交任务前会检查该任务（及其所有祖先节点）声明的每一个限额是否
还有剩余令牌；令牌耗尽时任务保持 ``queued``，直到有令牌被释放。

令牌的占用与释放跟随任务状态自动变化：

* 任务进入 ``submitted`` （提交运行）时占用令牌
* 任务变为 ``complete`` 或 ``aborted`` 时释放令牌

因此本例中 ``t1`` 和 ``t2`` 会先运行，``t3``、``t4`` 排队等待；
``t1`` 完成后释放一个令牌，``t3`` 才得以运行，依此类推——同一时刻最多
只有 2 个任务在运行。

.. note::

    对占用着令牌的任务执行 ``requeue`` 并不会释放它的令牌；
    令牌只在任务进入 ``complete`` 或 ``aborted`` 状态时释放。

限额定义在哪一级
------------------

限额定义在哪个节点上，决定了它的**作用范围**。``add_in_limit`` 不指定
节点路径时，会从当前节点开始沿父链向上查找 **最近的** 同名限额
（与变量的查找规则一致），所以通常把限额定义在要限制的那组任务的
公共祖先上：

* 定义在容器上：限制该容器内的任务
* 定义在 flow 上：限制整个工作流中的任务

也可以用 ``node_path`` 参数显式指定限额所在的节点，此时不做父链查找：

.. code-block:: python

    task1.add_in_limit("work", node_path="/test/group1")

一个节点可以声明多个 in-limit 标记（全部满足才能运行）；一个任务也可以
用 ``tokens`` 参数一次占用多个令牌，例如 ``add_in_limit("work", tokens=2)``
表示该任务运行期间独占整个 ``work`` 限额。

查看限额
----------

运行中的工作流可以用 ``--show-limit`` 查看每个限额的当前占用情况
（该选项默认开启，这里显式写出）：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client show --show-limit

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py show --show-limit

限额行打印在 **定义** 限额的节点上，例如 ``limit work [2/2]`` 表示 2 个
令牌已全部占用。in-limit 标记本身不会打印。

练习
-----

1. 修改 **test.py**，在 ``group1`` 上定义限额 ``work`` （2 个令牌），
   并让组内所有任务声明占用它
2. 启动服务并 requeue 工作流，用 ``show --show-limit`` 观察
   ``t3``、``t4`` 在令牌耗尽时保持排队、在前置任务完成后依次运行
3. 把限额改为 1 个令牌，观察任务完全串行执行
4. 给某个任务设置 ``tokens=2``，观察它运行时其他任务都无法启动
