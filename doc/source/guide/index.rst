用户指南
==================================

本章是主题式参考手册，按主题查阅 takler 的概念、配置项与用法，
不要求按顺序阅读。如果你是第一次接触 takler，建议先完成 :doc:`/tutorial/index`。

一次 job 的典型生命周期如下：

.. mermaid::

    flowchart LR
        queued --> submitted --> active --> complete
        active --> aborted

客户端命令通常同时提供 Go 客户端 ``takler_client`` 与 Python 客户端
``takler-client-py`` 两种写法：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client ping

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py ping

.. toctree::
   :hidden:
   :maxdepth: 2
