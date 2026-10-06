检查结果
=========

检查工作流的运行情况，使用命令行客户端查询，``--show-all`` 选项用于打印更多信息：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client show --show-all

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py show --show-all

命令会从 takler 服务检索当前状态。下面只保留树形部分；实际输出还包含
独立的服务状态、尝试 UUID、RID、时间和允许展示的本轮文件引用：

.. code-block::

    |- test [complete]
      |- t1 [complete]

如果输出与上面一样，任务 *t1* 是完成状态 (``complete``)，工作流 *test* 也是完成状态，表示工作流运行成功。

.. note::

    如果输出不是这样的，可能会看到出错状态 (``aborted``)，请首先检查 takler 脚本。

服务创建的作业文件 (*job file*) 与 takler 文件在同一目录中
（即 ``${TAKLER_HOME}/test``），默认名为 ``t1.job<attempt_id>``。
比较 **task1.takler**、**head.takler**、**tail.takler** 和本轮作业文件。

输出文件 (*output file*) 默认名为 ``t1.<attempt_id>``。可从 ``show``
的当前文件引用确认实际路径；显式覆盖路径时以该引用为准。

练习
-----

1. 定位作业文件和输出文件
2. 从服务检索工作流运行状态，检查运行结果
