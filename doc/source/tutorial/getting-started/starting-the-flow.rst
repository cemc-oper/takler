启动工作流
==========

启动 takler 服务后，工作流尚未 ``begin``，处于 ``unknown`` 状态，
调度器不会自动运行它。使用 ``begin test`` 启动日历并将任务排队。
``requeue`` 只重置节点状态，不能代替 ``begin``。

在之前章节中，我们已经启动了工作流 *test* 的 takler 服务。
如果 takler 服务尚未启动，请运行 **test.py** 脚本。

.. code-block:: bash

    python test.py

查询状态
---------

使用下面的命令查询 takler 服务中工作流的运行状态：

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

使用 ``begin`` 命令启动工作流：

使用客户端执行：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client begin test

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py begin test

检查运行情况
--------------

在终端中检查 **test.py** 的运行输出。下面只列关键事件；实际时间、
``attempt_id`` 和作业文件名每次都不同：

.. code-block::

    Job generation success: ${TAKLER_HOME}/test/t1.job<attempt-uuid>
    run: /test/t1 with try no 1
    init: /test/t1
    complete: /test/t1

可以看到：

* ``begin`` 启动了工作流日历并重排队任务。
* 调度器在下一次 main loop 时，发现 */test/t1* 任务满足运行条件，随即生成文件名带本轮 UUID 的作业脚本，并按默认命令运行。
* 脚本运行时会调用 child 命令 ``init`` 通知服务任务已启动，随后调用 ``complete`` 通知服务任务已完成。
* 在下一次 main loop 时，工作流 *test* 下所有任务已完成，处于 ``complete`` 状态，所以调度器不再提交任务。

练习
------

1. 运行 **test.py**，启动 takler 服务
2. 运行 ``begin test``（``takler_client begin test`` 或 ``takler-client-py begin test``），启动工作流 *test*
3. 检查 **test.py** 运行输出信息
