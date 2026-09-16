控制工作流
==========

前面几节中，工作流的运行完全由调度器按依赖关系驱动。本节介绍人工干预
工作流的控制命令：

* ``begin`` / ``requeue`` ：启动与重新排队
* ``suspend`` / ``resume`` ：挂起与恢复
* ``run`` ：手动运行任务
* ``force`` ：强制设置节点状态或事件取值
* ``free-dep`` ：手动释放依赖
* ``load`` ：向运行中的服务加载新工作流

这些命令通过客户端发给运行中的 takler 服务，目标用节点路径指定。

.. note::

    Go 版 ``takler_client`` 目前只实现了 ``requeue`` 、 ``suspend`` 、
    ``resume`` 、 ``run`` 四个控制命令；``begin`` 、 ``force`` 、
    ``free-dep`` 、 ``load`` 请使用 Python 版 ``takler-client-py`` 。

示例工作流
----------

本节使用下面的工作流：``t1`` 带有一个事件 ``a`` ；``t2`` 等待 ``t1``
完成；``t3`` 带有一个时间依赖：

.. literalinclude:: /../examples/getting_started/step13_control.py
    :language: python
    :linenos:

运行上述脚本，打印结果如下：

.. code-block::

    |- test [unknown]
      |- t1 [unknown]
          event a [unset]
      |- t2 [unknown]
          trigger ./t1 == complete
      |- t3 [unknown]
          time 12:00

参照 :doc:`../getting-started/starting-a-server` 一节把该工作流挂到服务上
（修改 **test.py** 的 ``create_flow`` 函数即可），下面的命令都对这个
运行中的服务执行。

启动与重启：begin 与 requeue
------------------------------

每个 flow 都有一个「是否已启动 (begun)」的标记，**只有 begun 的 flow 才会
被调度器处理**。「挂到服务上但尚未 begun」是工作流的正常状态——例如刚
用 ``load`` 加载的 flow 就停在这里。

``begin`` 启动 flow：把逻辑日历设为当前时间、把节点树重置为 ``queued``
并标记 begun。不带 flow 名时启动服务中的所有 flow：

.. code-block:: bash

    takler-client-py begin              # 启动所有 flow
    takler-client-py begin test         # 只启动 flow test

对已 begun 的 flow 再执行 ``begin`` 会报错；加 ``--force`` 可以强制重新
启动（日历重置、节点树重新排队）：

.. code-block:: bash

    takler-client-py begin --force test

``requeue`` 只重置状态，不触碰日历与 begun 标记。它可以作用于任意节点
路径——整个 flow、某个容器或单个任务——被重置的子树回到 ``queued``
等待重新调度：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client requeue /test

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py requeue /test

.. note::

    对未 begun 的 flow 执行 ``requeue`` 会被拒绝，首次启动请用 ``begin`` 。
    另外，手动 ``requeue`` 会把 repeat 重置回起始值，详见
    :doc:`repeat` 一节。

挂起与恢复：suspend 与 resume
---------------------------------

``suspend`` 挂起一个节点：挂起的节点不会被自动调度。挂起容器时，其整个
子树都不会被调度——调度器检查到容器已挂起后就不再向下遍历。
``suspended`` 是与节点状态正交的标记，不改变状态本身；``show`` 输出中
挂起的节点显示为 ``suspend (状态)`` ：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client suspend /test/t3
            takler_client show

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py suspend /test/t3
            takler-client-py show

``t3`` 一行会显示为 ``|- t3 [suspend (queued)]`` 。挂起不影响已经在
运行的作业，只阻止新的作业提交。用 ``resume`` 恢复：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client resume /test/t3

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py resume /test/t3

手动运行任务：run
--------------------

``run`` 让调度器立即提交一个任务的作业，常用于调试——不等工作流走到
它就先把作业跑起来看看：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client run /test/t1

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py run /test/t1

``run`` 只对任务有效；任务正处于 ``submitted`` 或 ``active`` 状态时
命令被忽略（避免同一任务同时跑两份作业）。加 ``--force`` 则无视当前
状态强制再提交一次：

.. code-block:: bash

    takler-client-py run --force /test/t1

强制置状态：force
--------------------

``force`` 直接改写节点状态或事件取值，不管它当前是什么。典型用途是
跳过已经在线下确认过的任务：把 ``t1`` 强制设为 ``complete`` 后，
``t2`` 的触发器随即满足：

.. code-block:: bash

    takler-client-py force complete /test/t1

对节点使用时，``--recursive`` 默认开启：目标节点的所有后代一并设为
该状态（例如 ``force complete /test`` 直接把整个工作流标记完成）。
加 ``--no-recursive`` 则只改目标节点自己——注意此时父容器的状态仍按
「取子节点中最重要状态」的规则重新聚合。

对事件使用时，路径写成 ``节点路径:事件名`` ，状态取 ``set`` 或
``clear`` ：

.. code-block:: bash

    takler-client-py force set /test/t1:a      # 置位事件 a
    takler-client-py force clear /test/t1:a    # 清除事件 a

``state`` 参数不是合法的 ``NodeStatus`` 名称（对节点）或不是
``set`` / ``clear`` （对事件）时命令报错，节点保持不变。

释放依赖：free-dep
--------------------

``free-dep`` 把节点上的依赖标记为「视为满足」，节点不再等待它：

* ``--dep-type time`` ：时间依赖视为满足（置位 ``free`` 闩锁）
* ``--dep-type trigger`` ：触发器视为满足
* ``--dep-type all`` ：以上两者都释放

例如让 ``t3`` 不再等到 12:00 、让 ``t2`` 不再等 ``t1`` ：

.. code-block:: bash

    takler-client-py free-dep --dep-type time /test/t3
    takler-client-py free-dep --dep-type trigger /test/t2

被释放的依赖只在当前这一轮运行中有效；节点被 ``requeue`` 后依赖重新
生效，等待下一次满足（或被再次释放）。

加载新工作流：load
--------------------

``load`` 把工作流定义文件加载进运行中的服务，无需重启服务。目前支持
JSON 格式（:py:meth:`Flow.to_dict <takler.core.Flow.to_dict>` 的
输出）。先用 Python 把工作流导出成 JSON 文件：

.. code-block:: python

    import json

    # flow 是构造好的 Flow 对象
    with open("my_flow.json", "w") as f:
        json.dump(flow.to_dict(), f)

然后加载并启动：

.. code-block:: bash

    takler-client-py load my_flow.json
    takler-client-py begin my_flow

``load`` 只注册定义：加载进来的 flow 尚未 begun，调度器不会处理它，
需要显式 ``begin`` 才会开始运行。

使用 takler-tui
------------------

上面的控制操作也可以在交互式终端界面 ``takler-tui`` 中完成。连接到
服务（地址解析顺序为 ``--connect-file`` / ``TAKLER_CONNECT_FILE`` →
``--host`` / ``--port`` / 环境变量 → 默认值）：

.. code-block:: bash

    takler-tui --host localhost --port 33083

界面左侧是节点树，选中节点后可用按键：

* ``r`` ：刷新节点树（默认不自动刷新）
* ``m`` ：打开当前节点的操作菜单（也可右键点击节点）
* 空格：展开/折叠节点
* ``p`` ：ping 服务
* ``q`` ：退出

对选中节点的控制操作：

* ``Ctrl+R`` ：run（仅任务节点）
* ``Ctrl+Q`` ：requeue
* ``Ctrl+S`` ：suspend
* ``Ctrl+U`` ：resume
* ``Ctrl+F`` ：force complete（弹确认框）
* ``Ctrl+D`` ：free dependencies

其他 ``force`` 目标状态（``queued`` 、 ``aborted`` 等）从菜单中的
``Force…`` 二级菜单选择。控制命令执行成功后界面会自动刷新。

.. note::

    在开启 XON/XOFF 流控的终端里，``Ctrl+S`` / ``Ctrl+Q`` 可能被终端
    拦截而到不了程序。如遇按键无响应，先执行 ``stty -ixon`` 或换用
    默认关闭流控的终端。

练习
-----

1. 修改 **test.py** 挂示例工作流，启动服务后用 ``begin`` 启动它，
   再尝试不带 ``--force`` 重复 ``begin`` ，观察报错
2. 在 ``t2`` 等待触发器时 ``suspend /test`` ，确认整个工作流停止调度；
   ``resume`` 后观察运行继续
3. 用 ``force complete /test/t1`` 放行 ``t2`` ，再用 ``requeue /test``
   把工作流重置回起点
4. 打开 ``takler-tui`` ，用快捷键完成一次 suspend / resume 和一次
   force complete
