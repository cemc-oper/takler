增加任务与节点容器
=====================

之前的章节只定义了一个任务 *t1*。本节把工作流扩展成多个任务，
并引入节点容器 (*NodeContainer*) 来组织任务的层级关系。

.. note::

    与 ecFlow 不同，takler 没有专门的 ``Family`` 类。ecFlow 中的 *family*
    在 takler 中就是 :py:class:`~takler.core.NodeContainer`：容器既可以挂在
    工作流 (:py:class:`~takler.core.Flow`) 下，也可以挂在另一个容器下，
    没有单独的 family 概念，只有统一的容器。

增加更多任务
--------------

在工作流 *test* 中增加一个容器 *group1*，并在其中放两个新任务 *t2* 与 *t3*：

.. literalinclude:: /../examples/getting_started/step4_add_tasks_and_containers.py
    :language: python
    :linenos:

逐行解释新增部分：

- 使用 :py:meth:`~takler.core.NodeContainer.add_container` 创建容器 ``group1``，
  该方法在 :py:class:`~takler.core.Flow` 与 :py:class:`~takler.core.NodeContainer`
  上都可用（``Flow`` 继承自 ``NodeContainer``）
- 在 ``group1`` 上调用 :py:meth:`~takler.core.NodeContainer.add_task` 添加任务
  ``t2``、``t3``，与直接在 ``flow`` 上添加任务的写法完全一致
- ``task2.takler``、``task3.takler`` 与 ``task1.takler`` 一样，复用
  ``head.takler`` / ``tail.takler`` 两个头文件

运行上述脚本，会打印如下树形结构：

.. code-block::

    |- test [unknown]
      |- t1 [unknown]
      |- group1 [unknown]
        |- t2 [unknown]
        |- t3 [unknown]

节点路径
---------

每个节点都有一个从根节点开始、以 ``/`` 分隔的绝对路径，即 ``node_path``：

* ``t1`` 的路径是 ``/test/t1``
* ``group1`` 的路径是 ``/test/group1``
* ``t2`` 的路径是 ``/test/group1/t2``

命令行客户端与触发器表达式中都使用这种路径来定位节点，例如：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client show /test/group1/t2

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py show /test/group1/t2

触发器表达式中还可以使用相对路径，``.`` 表示当前节点，``..`` 表示上一级，
例如在 ``t3`` 的触发器里写 ``../t2 == complete`` 就是指 ``/test/group1/t2``。
相对路径的写法会在后续「触发器」一节详细介绍。

容器状态由子节点聚合而来
----------------------------

容器 (``NodeContainer``) 没有自己独立决定的状态：它的状态是由子节点的状态
按下表的顺序取「最重要」的一个聚合出来的。:py:class:`~takler.core.NodeStatus`
是一个有序枚举：

.. mermaid::

    flowchart LR
        unknown --> queued --> submitted --> active --> aborted
        active --> complete

    %% aborted 与 complete 都比 active 更重要；
    %% aborted 在这五个状态里最重要，unknown 最不重要。

具体规则是：容器的状态等于其全部子节点状态中数值最大的那一个
（``aborted`` > ``complete`` > ``active`` > ``submitted`` > ``queued`` > ``unknown``）。
只要有一个子节点是 ``aborted``，容器（以及更上层的容器、整个工作流）就会显示为
``aborted``，即使其余子节点都已经 ``complete``。

.. literalinclude:: /../examples/getting_started/test/task2.takler
    :caption: 无需运行，仅用于对照上一节的任务脚本结构
    :language: jinja

拿本节的例子验证：``group1`` 下有 ``t2`` 与 ``t3`` 两个任务。

* 当 ``t2`` 变为 ``active``、``t3`` 变为 ``complete`` 时，``group1`` 显示为
  ``active``（``active`` 比 ``complete`` 更重要）
* ``group1`` 的状态变化会继续向上传播：只要 ``t1`` 不是更高状态，
  工作流 ``test`` 也会显示为 ``active``

这个「取子节点最大值」的规则是递归的：多层容器嵌套时，从最深层的任务开始，
逐层取最大值一直汇总到工作流层。状态变化向上传播的过程叫做 swim（上浮）；
服务主动把状态下发给子节点（比如 ``requeue`` 整个容器）的过程叫做 sink（下沉）。
完整的状态模型将在用户指南的「状态模型」一节中详细说明。

练习
-----

1. 在 ``$TAKLER_HOME/test`` 目录中创建 **task2.takler** 与 **task3.takler**，
   内容与 **task1.takler** 类似
2. 修改 **test.py**，增加容器 ``group1`` 与任务 ``t2``、``t3``
3. 运行脚本，检查打印出的树形结构
