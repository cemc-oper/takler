变量
======

前面章节中已经多次使用变量，例如给任务设置 ``TAKLER_SCRIPT``、给工作流设置
``TAKLER_HOME``。本节系统介绍变量的定义方式、作用域规则，以及在脚本中的使用方法。

定义变量
---------

使用 :py:meth:`~takler.core.node.Node.add_parameter` 给任意节点（工作流、容器、任务）
添加变量。变量在 takler 中的类型是 :py:class:`~takler.core.Parameter`。

.. literalinclude:: /../examples/getting_started/step5_variables.py
    :language: python
    :linenos:

上述代码在四个层级上都定义了变量：

* 工作流 ``test`` 上的 ``TAKLER_HOME`` 与 ``GREETING``
* 容器 ``group1`` 上的 ``GREETING`` （与工作流同名）
* 任务 ``t2`` 上的 ``GREETING`` （与容器同名）
* 任务 ``t1``、``t3`` 没有自己的 ``GREETING``

变量作用域：沿父链向上查找
------------------------------

查找变量时，使用 :py:meth:`~takler.core.node.Node.find_parent_parameter`：
先在节点自己身上找，找不到就沿着父节点链一路向上找（容器 → 工作流 → bunch），
使用第一个找到的取值。运行上面的脚本会打印：

.. code-block::

    t1 sees GREETING: hello from flow
    t2 sees GREETING: hello from t2
    t3 sees GREETING: hello from group1

* ``t1`` 没有自己的 ``GREETING``，也不在 ``group1`` 里，往上找到工作流层的取值
* ``t2`` 自己定义了 ``GREETING``，直接使用自己的取值，不会继续往上找
* ``t3`` 没有自己的 ``GREETING``，往上找到容器 ``group1`` 的取值，
  不会再继续往上找到工作流层的 ``hello from flow``

这就是变量遮蔽 (shadowing)：离节点更近的定义会遮蔽更远的同名定义。
容器和工作流常用来定义一批任务共享的默认值，个别任务再按需覆盖。

user 参数与 generated 参数
-------------------------------

变量分两类：

* **user 参数**：通过 :py:meth:`~takler.core.node.Node.add_parameter` 显式定义的变量，
  即上面例子中的 ``GREETING``、``TAKLER_HOME``、``TAKLER_SCRIPT``
* **generated 参数**：由 takler 根据节点当前状态自动计算和更新的变量，
  不需要（也不能通过 ``add_parameter``）手动设置，例如 ``TAKLER_JOB``、
  ``TAKLER_NAME``、``TAKLER_RID``

上面脚本的输出中，以 ``#`` 开头的行就是 generated 参数，例如：

.. code-block::

    |- t1 [unknown]
        param TAKLER_SCRIPT '...task1.takler'
        # param TAKLER_SCRIPT 'None'
        # param TAKLER_JOB 'None'
        # param TAKLER_JOBOUT 'None'
        # param TASK 'None'
        # param TAKLER_NAME 'None'
        # param TAKLER_RID 'None'
        # param TAKLER_TRY_NO 'None'
        # param TAKLER_PASS 'None'

查找变量时（:py:meth:`~takler.core.node.Node.find_parameter`），
同名的 user 参数优先于 generated 参数：例子中 ``t1`` 同时有 user 参数
``TAKLER_SCRIPT``（脚本路径）与同名的 generated 参数（尚未生成，取值为 ``None``），
实际生效的是 user 参数的取值。这些 generated 参数只有在任务真正开始生成作业
（作业生成、运行阶段）之后才会被填上实际取值，在此之前显示为 ``None``。

常用保留变量
--------------

takler 会在不同层级自动生成一批「保留变量」，可以直接在脚本或触发器中使用：

.. list-table::
    :header-rows: 1
    :widths: 15 30 55

    * - 层级
      - 变量
      - 说明
    * - bunch
      - ``TAKLER_HOST``、``TAKLER_PORT``、``TAKLER_HOME``
      - 服务连接信息与默认根目录
    * - flow
      - ``FLOW``、``TAKLER_DATE``、``TAKLER_TIME``、``TIME``、``DATE``
      - 工作流名称与日历相关变量
    * - task
      - ``TASK``、``TAKLER_NAME``、``TAKLER_RID``、``TAKLER_TRY_NO``、
        ``TAKLER_PASS``、``TAKLER_TRIES``
      - 任务名称、节点路径、运行标识、重试次数、作业口令等

Shell 脚本任务还会额外生成 ``TAKLER_SCRIPT``、``TAKLER_JOB``、``TAKLER_JOBOUT``
三个变量，用于定位脚本文件、生成的作业文件与输出文件，详见
:doc:`getting-started/checking-job-creation`。

在脚本中使用变量
------------------

takler 脚本使用 Jinja2 语法引用变量，即 ``{{ 变量名 }}``。下面的 task2 脚本
引用了本节定义的 ``GREETING`` 变量：

.. literalinclude:: /../examples/getting_started/test/task2_with_greeting.takler
    :language: jinja

由于 ``t2`` 自己定义了 ``GREETING`` 为 ``"hello from t2"``，渲染后这一行会变成：

.. code-block:: bash

    echo "hello from t2"

如果把这个模板用在 ``t3`` 上（``t3`` 没有自己的 ``GREETING``），
会渲染成容器 ``group1`` 的取值：

.. code-block:: bash

    echo "hello from group1"

用命令行检查变量
------------------

运行中的工作流可以用 ``--show-parameter`` 查看每个节点当前解析到的变量：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client show --show-parameter

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py show --show-parameter

输出格式与本节脚本打印的树形结构一致：不带 ``#`` 前缀的是 user 参数，
带 ``#`` 前缀的是 generated 参数。

练习
-----

1. 在工作流、容器、任务三个层级分别定义一个同名变量，验证遮蔽规则
2. 在某个任务脚本中使用 ``{{ 变量名 }}`` 引用一个定义在容器上的变量
3. 用 ``show --show-parameter`` 查看解析结果
