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

命令会从 Takler 服务检索工作流运行状态，打印每个节点的运行状态。输出如下所示（``${TAKLER_HOME}`` 替换为你实际设置的目录）：

.. code-block::

    |- test [complete]
        param TAKLER_HOME '${TAKLER_HOME}'
      |- t1 [complete]
          param TAKLER_SCRIPT '${TAKLER_HOME}/test/task1.takler'
          # param TAKLER_SCRIPT 'None'
          # param TAKLER_JOB '${TAKLER_HOME}/test/t1.job'
          # param TAKLER_JOBOUT '${TAKLER_HOME}/test/t1.out'
          # param TASK 't1'
          # param TAKLER_NAME '/test/t1'
          # param TAKLER_RID 'None'

如果输出与上面一样，任务 *t1* 是完成状态 (``complete``)，工作流 *test* 也是完成状态，表示工作流运行成功。

.. note::

    如果输出不是这样的，可能会看到出错状态 (``aborted``)，请首先检查 takler 脚本。

服务创建的作业文件 (*job file*) 与 takler 文件在同一目录中 (即 ``${TAKLER_HOME}/test``)，名为 **t1.job**。
比较 **task1.takler**、**head.takler**、**tail.takler** 和 **t1.job** 文件。

输出文件 (*output file*) 与 takler 文件在同一目录中，名为 **t1.out**。

练习
-----

1. 定位作业文件和输出文件
2. 从服务检索工作流运行状态，检查运行结果