定义新工作流
===================

本节介绍如何定义只有一个任务的工作流。

创建工作流文件
---------------

创建一个 Python 文件 **test.py**。

本教程在目录 ``${TAKLER_HOME}`` 中创建该文件：

.. literalinclude:: /../examples/getting_started/step1_define_flow.py
    :language: python
    :linenos:

上述代码定义了一个叫做 `test` 的工作流，包含一个叫做 `t1` 的任务。

逐行解释上述代码：

1-2：导入 Python 自带包

4-6：导入 takler 包中对象

- :py:class:`~takler.core.Flow`：工作流类
- :py:class:`~takler.core.Bunch`：工作流集合类，可以包含多个工作流 (``Flow``) 对象
- :py:class:`~takler.tasks.shell.ShellScriptTask`：Shell 脚本任务类
- :py:func:`~takler.visitor.pre_order_travel` 函数：前序遍历工作流
- :py:class:`~takler.visitor.PrintVisitor` 类：打印节点信息

9：``create_flow()`` 函数创建只有一个任务 (task1) 的工作流 (flow1)

13：定义工作流 ``flow``

14：为 ``flow`` 定义变量 ``TAKLER_HOME``，该变量定义工作流生成的作业文件的保存目录，本例中是脚本当前目录

15：定义 Shell 脚本任务 ``task1``

16：为 ``task1`` 定义变量 ``TAKLER_SCRIPT``，该变量定义 Shell 脚本任务对应的脚本目录，本例中为

.. code-block::

    ${TAKLER_HOME}/test/task1.takler

17：``create_flow()`` 函数返回工作流 ``flow`` 对象

20：定义直接运行脚本会执行的代码

21：创建工作流 ``flow``

22：创建工作流集合对象 ``bunch``。:py:class:`~takler.core.Bunch` 对象用于保存 Takler 服务中的工作流。

23：将工作流 ``flow`` 添加到 ``bunch`` 中

24：``pre_order_travel`` 与 ``PrintVisitor`` 结合会打印工作流的树形结构

运行上述脚本

.. code-block:: bash

    python test.py


会打印工作流结构

.. code-block::

    |- test [unknown]
      |- t1 [unknown]


另一种定义方式
---------------

为了表现工作流节点的层次关系，可以使用 Python 的 ``with`` 语句创建工作流，上述代码中的 ``create_flow()`` 函数可以修改为：

.. code-block:: python

    def create_flow():
       with Flow("test") as flow
           flow.add_parameter("TAKLER_HOME", TAKLER_HOME)
           with flow.add_task(ShellScriptTask("t1")) as task1:
               task1.add_parameter("TAKLER_SCRIPT", Path(TAKLER_HOME, "test/task1.takler"))
       return flow


.. note::

    在大型工作流中可以使用 ``with`` 方式合理缩进代码，方便后续修改维护。

练习
-----

1. 创建工作流定义文件 **test.py**
2. 运行脚本