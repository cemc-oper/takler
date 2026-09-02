检查作业
=========

在前一节中，我们看到一个任务脚本如何被转换为作业文件。

如果我们直接手动运行作业文件，运行会出错

.. code-block:: bash

    $TAKLER_HOME/test/t1.job

出错信息类似：

.. code-block::

    + takler_client init --host localhost --port 33083 --task-id 132970 --node-path /test/t1
    localhost:33083 init /test/t1 with 132970
    2022/06/30 08:53:50 could not init: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing dial tcp 127.0.0.1:33083: connect: connection refused"

原因是作业脚本中集成的 takler_client（或 takler-client-py）命令无法与服务 Takler 通讯。

``TAKLER_HOST`` 与 ``TAKLER_PORT`` 由任务脚本生成，因为当前主机上没有启动 Takler 服务，
所以 child 命令因无法连接服务而报错。

无论作业是如何生成的，即通过 Python 脚本或 Takler 服务，我们需要一种独立于 Takler 服务的检查作业方法。
可以通过设置环境变量 ``NO_TAKLER`` 来完成。

.. code-block:: bash

    export NO_TAKLER=1
    $TAKLER_HOME/test/t1.job

部分输出如下：

.. code-block::

    + wait
    + takler_client complete --host localhost --port 33083 --node-path /test/t1
    ignore because NO_TAKLER is set.
    + trap 0
    + exit 0

设置 ``NO_TAKLER`` 时，child 命令立即返回，返回值为 0（即成功），不再尝试连接服务。
这使您可以独立于 Takler 来检查您的脚本和作业。两个客户端都识别该环境变量。

练习
-----

1. ``export NO_TAKLER=1``
2. 运行作业脚本 ``$TAKLER_HOME/test/t1.job``