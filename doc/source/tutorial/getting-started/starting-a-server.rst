启动服务
=========

工作流需要在 Takler 服务中才能自动运行，Takler 可以将工作流定义和 Takler 服务启动集成到同一个脚本文件中。

Takler 服务以异步方式运行网络监听服务和调度器：

* 网络监听服务 (``NetworkService``) 用于响应从客户端发送的请求，执行相应操作
* 调度器 (``Scheduler``) 用于定时检查工作流任务状态，运行符合条件的任务

基本步骤
-------------------------

使用 Takler 运行单个工作流的基本步骤：

* 创建工作流
* 创建 Takler 服务对象，添加工作流到服务
* 启动服务

修改 Python 脚本
----------------

修改 **test.py** 文件：

.. literalinclude:: /../examples/getting_started/step3_start_server.py
    :language: python
    :linenos:

逐行介绍新增代码：

* 1：导入 Python 异步包 asyncio
* 6：导入 Takler 服务类 ``TaklerServer``
* 22：异步函数 ``run_takler`` 用于启动 Takler 服务并运行调度器
* 23：启动 Takler 服务
* 24：运行 Takler 服务，直到服务被终止
* 31：创建 Takler 服务对象 ``server``
* 32：将工作流 ``flow`` 添加到 Takler 服务 ``server``
* 33：运行异步函数 ``run_takler``，直到函数运行结束，即 Takler 服务被终止

运行 Takler 服务
-----------------

执行 **test.py** 代码，启动并运行 Takler 服务

.. code-block:: bash

    python test.py

脚本输出日志如下：

.. code-block::

    |- test [unknown]
      |- t1 [unknown]
    2022-07-05 03:09:52.853 | INFO     | takler.server:start:35 - start server...
    2022-07-05 03:09:52.902 | INFO     | takler.server.network_service:start:51 - service started: [::]:33083
    2022-07-05 03:09:52.902 | INFO     | takler.server:start:38 - start server...done
    2022-07-05 03:09:52.902 | INFO     | takler.server.scheduler:main_loop:56 - main loop...
    2022-07-05 03:10:02.911 | INFO     | takler.server.scheduler:main_loop:56 - main loop...
    2022-07-05 03:10:12.939 | INFO     | takler.server.scheduler:main_loop:56 - main loop...

Takler 服务监听 ``[::]:33083`` 端口。
Takler 的调度器每隔 10 秒检查工作流状态，自动提交符合触发器条件的任务。
默认设置下，工作流处于 ``unknown`` 状态，任务不会被自动提交。

练习
------

1. 修改 **test.py**
2. 运行 **test.py**，检查命令行输出