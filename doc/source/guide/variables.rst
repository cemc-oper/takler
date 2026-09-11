变量参考
========

变量是挂在节点上的键值对，类型为 :py:class:`~takler.core.Parameter` ，
用于参数化作业脚本与触发器表达式。入门介绍见 :doc:`/tutorial/going-further/variables` ；
本页是完整参考：定义方式、作用域与解析顺序、 user 与 generated 两类
参数的区别，以及全部保留变量。

定义变量
--------

用 :py:meth:`Node.add_parameter <takler.core.node.Node.add_parameter>`
给任意节点（ bunch 、 flow 、容器、任务）添加变量，支持四种写法：

.. code-block:: python

    node.add_parameter("THRESHOLD", 4)                # 名字 + 值
    node.add_parameter({"A": 1, "B": "x"})            # dict 批量
    node.add_parameter([Parameter("C", True)])        # Parameter 列表
    node.add_parameter(Parameter("D", 1.5))           # 单个 Parameter

取值类型限定为 ``str`` 、 ``int`` 、 ``float`` 、 ``bool`` 。同名变量
重复添加时后添加的覆盖先前的取值。

作用域与解析顺序
----------------

查找变量用
:py:meth:`Node.find_parent_parameter <takler.core.node.Node.find_parent_parameter>`
，顺序是：

1. 节点自己
2. 沿父节点链向上：容器 → flow
3. bunch （服务级参数）

第一个找到的取值生效——离节点近的定义**遮蔽** (shadow) 远处的同名
定义。容器和 flow 常用来放一批任务共享的默认值，个别任务按需覆盖。

同一节点内部，同名的 user 参数优先于 generated 参数（见下一节）。

user 参数与 generated 参数
--------------------------

变量分两类：

* **user 参数** ：通过 ``add_parameter`` 显式定义的变量。序列化
  （ ``to_dict`` ）只保存 user 参数， load 与 checkpoint 恢复的都是
  它们
* **generated 参数** ： takler 根据节点当前状态自动计算的变量（任务名、
  日历日期、作业口令等）。它们不会被序列化，也**不应该** 用
  ``add_parameter`` 手动设置——同名 user 参数会遮蔽 generated 参数，
  使自动计算失效

多数 generated 参数在节点尚未运行时取值为 ``None`` ：任务级参数在首次
生成作业时填入， flow 的日历参数在 ``begin`` 后第一次日历时钟跳动时
填入。

.. note::

    ``TAKLER_PASS`` 只能是 generated 参数：作业口令是 child 命令上报
    的凭据，保持 generated 使它不会出现在 ``show`` 输出与 checkpoint
    文件中。

保留变量全表
------------

takler 在各层级自动生成的变量如下。

**bunch 级** （服务级，来自 :py:class:`~takler.core.Bunch` 的
``ServerState`` ，沿继承链对所有节点可见）：

.. list-table::
    :header-rows: 1
    :widths: 25 60

    * - 变量
      - 含义
    * - ``TAKLER_HOST``
      - 服务监听地址，来自 ``Bunch(host=...)``
    * - ``TAKLER_PORT``
      - 服务端口，来自 ``Bunch(port=...)``
    * - ``TAKLER_HOME``
      - 作业文件根目录，默认 ``"."`` 。实际使用中几乎总在 flow 上
        用同名 user 参数覆盖（见 :doc:`/tutorial/going-further/variables` ）

**flow 级** （来自 flow 的日历）：

.. list-table::
    :header-rows: 1
    :widths: 25 60

    * - 变量
      - 含义
    * - ``DATE``
      - 日历当前日期， ``%Y-%m-%d`` 格式
    * - ``TIME``
      - 日历当前时间， ``%H:%M`` 格式

**task 级** （任务首次生成作业时填入实际取值，此前为 ``None`` ）：

.. list-table::
    :header-rows: 1
    :widths: 25 60

    * - 变量
      - 含义
    * - ``TASK``
      - 任务名（节点名）
    * - ``TAKLER_NAME``
      - 节点的完整路径，如 ``/test/group1/t2``
    * - ``TAKLER_RID``
      - 作业标识（本地 shell 作业即进程号）， child 命令上报与
        ``kill`` 时定位作业用
    * - ``TAKLER_TRY_NO``
      - 第几次尝试运行，每次重试加一
    * - ``TAKLER_PASS``
      - 作业口令， child 命令上报的凭据（不入 checkpoint ，见上文）

**ShellScriptTask 额外生成** （作业文件相关）：

.. list-table::
    :header-rows: 1
    :widths: 25 60

    * - 变量
      - 含义
    * - ``TAKLER_SCRIPT``
      - 任务脚本路径，取 ``script_path`` 。定义同名 user 参数可以
        覆盖（ user 优先于 generated ）
    * - ``TAKLER_JOB``
      - 生成的作业文件路径：
        ``{TAKLER_HOME}{节点路径}.job{try_no}``
    * - ``TAKLER_JOBOUT``
      - 作业输出文件路径： ``{TAKLER_HOME}{节点路径}.{try_no}``

**repeat 生成**：节点带 repeat 时，以 repeat 的名字为变量名生成一个
参数，取值为 repeat 的当前值，随 repeat 推进自动更新（见
:doc:`/tutorial/going-further/repeat-and-time` ）。

shell 任务识别的 user 变量
--------------------------

下列变量不会自动生成，定义后会改变 shell 任务的行为（详见
:doc:`/tutorial/getting-started/checking-job-creation` ）：

.. list-table::
    :header-rows: 1
    :widths: 30 55

    * - 变量
      - 含义
    * - ``TAKLER_INCLUDE``
      - 模板搜索路径列表，用 ``:`` 分隔；脚本中的
        ``{% include %}`` 在这些目录下查找
    * - ``TAKLER_SHELL_JOB_CMD``
      - 作业提交命令模板，默认
        ``{{TAKLER_JOB}} 1> {{TAKLER_JOBOUT}} 2>&1``
    * - ``TAKLER_SHELL_KILL_CMD``
      - 终止作业的命令模板，默认 ``kill -15 {{TAKLER_RID}}``

在脚本中引用变量
----------------

takler 脚本用 Jinja2 语法 ``{{ 变量名 }}`` 引用变量。渲染上下文是
:py:meth:`Node.parameters <takler.core.node.Node.parameters>` 返回的
合并视图：节点自己、各级祖先直到 bunch 的全部变量，同名时近处优先、
user 优先于 generated 。

.. warning::

    引用**未定义**的变量不会报错： Jinja2 默认把未定义变量渲染为空
    字符串， ``check_job_creation`` 也检查不出来。 ``echo {{ THRESHOLD }}``
    在 ``THRESHOLD`` 未定义时会渲染成 ``echo`` 。定义变量时注意层级
    与拼写。

在触发器中引用变量
------------------

触发器表达式用 ``路径:变量名`` 引用事件、标尺或参数，例如
``./t1:meter1 >= ./t1:THRESHOLD`` 。完整语法见
:doc:`/guide/trigger-expression` 。
