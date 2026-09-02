定义第一个任务
===============

接下来，我们需要为任务 t1 编写 takler 脚本。

在工作流定义中，我们为 t1 设置变量 ``TAKLER_SCRIPT``，该变量设置任务 t1 对应的脚本路径

.. code-block:: bash

    ${TAKLER_HOME}/test/task1.takler

创建任务脚本
------------

在 ``$TAKLER_HOME`` 目录中创建目录 test

.. code-block:: bash

    cd ${TAKLER_HOME}
    mkdir test
    cd test

在 test 目录中创建 task1.takler 文件：

.. literalinclude:: /../examples/getting_started/test/task1.takler
    :language: jinja


作业生成
---------

在提交任务前，Takler 服务会将 takler 脚本 (takler script) 转化为作业文件 (job file)，这个过程叫做作业生成 (job creation)。
作业生成过程中会调用 Jinja2 引擎对 takler 脚本进行渲染，通常包括：

- 处理头文件指令
- 执行变量替换

作业生成步骤会创建一个以 `.job` 结尾的文件，Takler 服务将该文件提交给你的系统。

在当前示例中：

- ``{% include "head.takler" %}`` 会被替换为 **head.takler** 文件的内容。

  注意头文件路径相对于 **task1.takler** 所在目录，本例中的 **head.takler** 与 **task1.takler** 在同一个目录中。

  .. warning::

    Jinja2 默认仅支持加载模板目录内或子目录内的模板文件。
    Takler 在解析任务脚本时默认将该脚本所在目录放到模板搜索列表中。
    所以如果没有额外设置头文件路径，Takler 无法加载脚本所在目录之外的头文件，比如 ``../head.takler``。

- ``{{ TAKLER_HOME }}`` 被 ``TAKLER_HOME`` 变量的值替换
- ``{% include "tail.takler" %}`` 会被替换为 **tail.takler** 文件的内容

练习
------

1. 在 ``$TAKLER_HOME/test`` 目录中创建 takler 脚本 **task1.takler**