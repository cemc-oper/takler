检查作业生成
=============

在前一节，我们已经实现第一个任务 ( task1.takler 文件 )。
task1.takler 脚本需要经过预处理生成作业文件 (job file)。
这个过程由 Takler 服务在任务运行前自动完成。

可以在工作流运行前手动检查作业生成。

检查作业
-----------

在工作流运行前可以检查作业生成过程，检查包括：

* 定位 takler 脚本文件，对应工作流定义中的每个任务 (``ShellScriptTask``)。
  本例中使用 ``TAKLER_SCRIPT`` 直接指定脚本路径。
* 进行预处理

当工作流较复杂且包含许多 takler 脚本时，这种检查可以节省大量时间。

检查作业生成时需要注意以下几点：

1. 作业检查 **独立** 于 Takler 服务，因此作业文件中的 ``TAKLER_HOST`` 和 ``TAKLER_PORT`` 被替换为默认值 (``localhost`` 和 ``33083``)。
2. 作业文件扩展名是 **.job**。
3. 默认情况下，作业文件在 ``TAKLER_HOME`` 目录下生成。

使用 ``takler.tasks.shell.check_job_creation`` 函数完成检查。

更新 **test.py**

.. literalinclude:: /../examples/getting_started/step2_check_job_creation.py
    :language: python
    :linenos:

运行脚本

.. code-block:: bash

    python test.py

显示如下结果（注：删掉部分日志信息）

.. code-block::

    |- test [unknown]
      |- t1 [unknown]
    Job generation success: ${TAKLER_HOME}/test/t1.job
    Render run command success: ${TAKLER_HOME}/test/t1.job 1> ${TAKLER_HOME}/test/t1.out 2>&1
    check job creation results: 1 total, 1 success, 0 failed.


练习
-----

1. 在 ``$TAKLER_HOME/test.py`` 文件中增加作业创建检查。
2. 运行修改后的 **test.py**
3. 检查生成的作业文件 ``$TAKLER_HOME/test/t1.job``