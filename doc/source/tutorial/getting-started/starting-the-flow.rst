启动工作流
==========

启动 Takler 服务后，工作流处于 ``unknown`` 状态，调度器不会自动运行 ``unknown`` 状态的工作流。
如果想要启动工作流，需要 ``requeue`` 工作流。

在之前章节中，我们已经启动了工作流 *test* 的 Takler 服务。
如果 Takler 服务尚未启动，请运行 **test.py** 脚本。

.. code-block:: bash

    python test.py

查询状态
---------

使用下面的命令查询 Takler 服务中工作流的运行状态：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client show

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py show

输出如下所示：

.. code-block::

    localhost:33083 show
    |- test [unknown]
      |- t1 [unknown]

工作流 *test* 处于 ``unknown`` 状态。

启动工作流
----------

Takler 目前没有专门用于启动工作流的 API，可以使用 ``requeue`` 命令将工作流设为 ``queued`` 状态，让调度器开始调度工作流。

使用客户端执行 ``requeue`` 操作：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client requeue /test

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py requeue /test

检查运行情况
--------------

在终端中检查 **test.py** 的运行输出，输出日志类似如下所示：

.. code-block::

    2022-07-05 08:46:05.785 | INFO     | takler.server.scheduler:main_loop:56 - main loop...
    2022-07-05 08:46:08.119 | INFO     | takler.server.network_service:RunRequeueCommand:129 - Requeue: /test
    2022-07-05 08:46:15.787 | INFO     | takler.server.scheduler:main_loop:56 - main loop...
    2022-07-05 08:46:15.920 | INFO     | takler.tasks.shell.shell_script_task:create_job_script:113 - Job generation success: ${TAKLER_HOME}/test/t1.job
    2022-07-05 08:46:15.921 | INFO     | takler.tasks.shell.shell_script_task:create_job_script:117 - Render run command success: ${TAKLER_HOME}/test/t1.job 1> ${TAKLER_HOME}/test/t1.out 2>&1
    2022-07-05 08:46:15.921 | INFO     | takler.core.task_node:run:130 - run: /test/t1
    2022-07-05 08:46:16.157 | INFO     | takler.server.network_service:RunInitCommand:73 - Init: /test/t1 with 46453
    2022-07-05 08:46:16.157 | INFO     | takler.core.task_node:init:144 - init: /test/t1
    2022-07-05 08:46:16.169 | INFO     | takler.server.network_service:RunCompleteCommand:82 - Complete: /test/t1
    2022-07-05 08:46:16.170 | INFO     | takler.core.task_node:complete:149 - complete: /test/t1
    2022-07-05 08:46:25.797 | INFO     | takler.server.scheduler:main_loop:56 - main loop...
    2022-07-05 08:46:35.808 | INFO     | takler.server.scheduler:main_loop:56 - main loop...

可以看到：

* Takler 服务接收到了客户端发送的 ``requeue`` 命令。
* 调度器在下一次 main loop 时，发现 */test/t1* 任务满足运行条件，随即生成作业脚本 **t1.job**，并在本机运行该脚本。
* 脚本运行时会调用 child 命令 ``init`` 通知服务任务已启动，随后调用 ``complete`` 通知服务任务已完成。
* 在下一次 main loop 时，工作流 *test* 下所有任务已完成，处于 ``complete`` 状态，所以调度器不再提交任务。

练习
------

1. 运行 **test.py**，启动 Takler 服务
2. 运行 ``requeue /test``（``takler_client requeue /test`` 或 ``takler-client-py requeue /test``），启动工作流 *test*
3. 检查 **test.py** 运行输出信息