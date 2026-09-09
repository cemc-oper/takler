限额 (limit 与 inlimit)
=======================

限额控制并发：一个 **限额** 是挂在某个节点上的命名令牌池
（ :py:class:`~takler.core.Limit` ），一个 **in-limit 标记**
（ :py:class:`~takler.core.limit.InLimit` ）声明某个节点运行期间要
占用哪个限额的令牌。

定义
----

用 :py:meth:`Node.add_limit <takler.core.node.Node.add_limit>` 定义
限额，参数为 ``name`` 与 ``limit`` （令牌总数）。同一节点上限额名
不能重复，重复添加抛出 ``RuntimeError`` 。

用 :py:meth:`Node.add_in_limit <takler.core.node.Node.add_in_limit>`
声明占用：

.. code-block:: python

    group1.add_limit("work", 2)                 # 定义：2 个令牌
    task1.add_in_limit("work")                  # 声明：运行时占 1 个
    task2.add_in_limit("work", tokens=2)        # 声明：运行时占 2 个
    task3.add_in_limit("work", node_path="/test/group1")  # 显式指定位置

``add_in_limit`` 的参数：

.. list-table::
    :header-rows: 1
    :widths: 25 75

    * - 参数
      - 说明
    * - ``limit_name``
      - 要占用的限额名
    * - ``node_path``
      - 限额所在节点的路径，默认 ``None``
    * - ``tokens``
      - 占用的令牌数，默认 1

同一节点上 ``limit_name`` 与 ``node_path`` 都相同的 in-limit 标记不能
重复，重复添加抛出 ``RuntimeError`` 。

示例
----

下面的例子在容器 ``group1`` 上定义了最多 2 个令牌的限额 ``work``
（第 18 行），组内 4 个任务都声明占用它（第 23、27、31、35 行），
因此同一时刻最多只有 2 个任务在运行：

.. literalinclude:: /../examples/getting_started/step8_limits.py
    :language: python
    :linenos:
    :emphasize-lines: 18,23,27,31,35

引用解析
--------

in-limit 标记并不保存限额的位置，而是在首次参与检查时由
:py:class:`~takler.core.limit.InLimitManager` 惰性解析并缓存：

* ``node_path=None`` （默认）：从当前节点开始沿父链向上，使用
  **最近的** 同名限额（
  :py:meth:`Node.find_limit_up <takler.core.node.Node.find_limit_up>`
  ），查找规则与变量一致
* 指定 ``node_path`` ：只在该节点上查找同名限额， **不做** 父链查找

.. important::

    解析不到限额的 in-limit 标记会被**静默忽略**，既不报错也不阻塞
    任务运行。拼错限额名或路径的结果是该限制完全不生效，定义后应
    用 ``show --show-limit`` 确认占用情况符合预期。

占用与释放
----------

调度器提交任务前检查该任务**及其所有祖先节点**上的全部 in-limit 标记
（ :py:meth:`Node.check_in_limit_up <takler.core.node.Node.check_in_limit_up>`
），所有标记都有足够令牌才允许运行，任一不足则任务保持 ``queued`` 。
因此把 in-limit 标记加在容器上等价于让容器内所有任务共享占用。

令牌的占用与释放跟随任务状态自动变化：

* 任务进入 ``submitted`` 时占用令牌
* 任务进入 ``complete`` 或 ``aborted`` 时释放令牌
* 对占用着令牌的任务执行 requeue **不会** 释放令牌

同一个限额在同一次任务运行中只被占用一次，即使该任务通过多个
in-limit 标记（例如自身一个、祖先容器一个）指向同一个限额。

requeue 与序列化
----------------

* requeue 不改变限额的占用（见上）
* 保存状态时限额带当前占用量与占用者路径列表，恢复后占用关系保持；
  只保存定义时占用清零。 in-limit 标记只有定义（限额名、路径、令牌数），
  没有运行期状态
