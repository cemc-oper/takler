概念与术语
==========

本页定义用户指南与教程中使用的术语，并说明 takler 的节点模型。
如果你来自 ecFlow，文末给出了两套术语的对应关系。

节点模型
--------

takler 把工作流组织成一棵节点树，共四层：

.. code-block::

    Bunch                 服务端的全局容器，容纳所有 flow
    └─ Flow               一份独立的工作流，带有自己的日历
       └─ NodeContainer   容器，可嵌套，用来组织层级
          └─ Task         任务，叶子节点，对应一个作业脚本

* :py:class:`~takler.core.Bunch` ：一个服务进程持有一个 ``Bunch`` ，
  它是所有工作流的容器，同时持有服务级参数（ ``TAKLER_HOST`` 、
  ``TAKLER_PORT`` 、 ``TAKLER_HOME`` ），这些参数沿继承链对所有节点
  可见（见 :doc:`/tutorial/going-further/variables` ）。
* :py:class:`~takler.core.Flow` ：一份完整的工作流定义，是调度的基本
  单位： ``begin`` 、 ``suspend`` 等控制命令以节点路径为边界，每个
  flow 有自己独立的逻辑日历，驱动时间依赖与 repeat（见
  :doc:`/tutorial/advanced-topics/repeat` ）。
* :py:class:`~takler.core.NodeContainer` ：容器节点，可以挂在 flow 或
  另一个容器下，层数不限，用来把相关任务组织在一起。
* :py:class:`~takler.core.Task` ：叶子节点。实际运行中使用其子类
  :py:class:`~takler.tasks.shell.ShellScriptTask` ，对应一个 shell
  作业脚本。

每个节点都有从根开始、以 ``/`` 分隔的绝对路径（如
``/test/group1/t2`` ），客户端命令与触发器表达式都用它定位节点。
容器没有独立的状态：它的状态由子节点聚合而来，详见 :doc:`node-status` 。

术语表
------

.. glossary::

    节点 (node)
        节点树中的任意元素：flow、容器、任务统称节点。基类为
        :py:class:`~takler.core.node.Node` 。

    节点路径 (node path)
        节点在树中的绝对路径，以 ``/`` 开头、按层级分隔，如
        ``/test/group1/t2`` 。引用事件等变量时在路径后加冒号与变量名，
        如 ``/test/t1:a`` 。

    工作流 (flow)
        一份完整的工作流定义，即节点树中顶层的一棵子树。一个服务可以
        同时运行多个 flow，见 :doc:`defining-flows` 。

    容器 (container)
        :py:class:`~takler.core.NodeContainer` ，可以容纳任务与其他
        容器的中间节点。ecFlow 中的 family 在 takler 中就是容器。

    任务 (task)
        叶子节点，代表一步具体的工作。运行时对应一次作业提交。

    Bunch
        服务端的全局容器 :py:class:`~takler.core.Bunch` ，容纳所有
        flow 并持有服务级参数。

    作业 (job)
        任务的一次实际运行：调度器把任务脚本渲染成作业脚本
        （ ``<节点路径>.job<try_no>`` ）并提交执行，产出输出文件
        （ ``<节点路径>.<try_no>`` ）。见
        :doc:`/tutorial/getting-started/checking-the-job` 。

    child 命令
        作业脚本内向服务上报状态的命令： ``init`` （开始运行）、
        ``complete`` （成功完成）、 ``abort`` （异常终止）、
        ``event`` （置事件）、 ``meter`` （更新标尺）。通常由
        ``head.takler`` / ``tail.takler`` 自动调用，见
        :doc:`/tutorial/getting-started/understanding-includes` 。

    触发器 (trigger)
        节点运行的前置条件表达式，如 ``./t1 == complete`` ，见
        :doc:`/tutorial/going-further/triggers` 。

    事件 (event)
        任务运行中置位的布尔信号，下游任务可以据此提前启动，见
        :doc:`/tutorial/going-further/events` 。

    标尺 (meter)
        任务运行中更新的整数值，可在触发器中参与比较，见
        :doc:`/tutorial/going-further/meters` 。

    限额 (limit / inlimit)
        限制同时运行的任务数： ``limit`` 定义令牌总数，
        ``inlimit`` 声明占用，见 :doc:`/tutorial/advanced-topics/limits` 。

    重复 (repeat)
        让节点按日期序列重复运行的属性，见
        :doc:`/tutorial/advanced-topics/repeat` 。

    时间依赖 (time)
        让节点等到 flow 日历到达某个时刻才可运行的依赖，见
        :doc:`/tutorial/advanced-topics/time` 。

    日历 (calendar)
        每个 flow 的逻辑时钟， ``begin`` 时以当前时间启动，驱动时间
        依赖与 repeat 的取值。

    参数 (parameter)
        挂在节点上的键值对（也称变量），渲染作业脚本时沿节点树继承，
        见 :doc:`/tutorial/going-further/variables` 。

    begun
        flow 的「已启动」标记：只有 begun 的 flow 才会被调度器处理，
        见 :doc:`/tutorial/advanced-topics/controlling-the-flow` 。

    try_no
        任务的运行次序号，每次提交加 1， ``requeue`` 清零；作业脚本与
        输出文件以它编号，见 :doc:`/tutorial/advanced-topics/zombies` 。

    僵尸 (zombie)
        不属于任务当前运行实例的上报，服务端按配置的策略拒绝或接管，
        见 :doc:`/tutorial/advanced-topics/zombies` 。

    检查点 (checkpoint)
        服务端周期性保存的快照文件，用于进程重启后恢复工作流状态，
        见 :doc:`/tutorial/advanced-topics/restart` 。

与 ecFlow 术语的对应
----------------------

takler 的概念体系源自 ecFlow，常用术语的对应关系如下：

.. list-table::
    :header-rows: 1

    * - ecFlow
      - takler
      - 说明
    * - suite
      - flow
      - 顶层工作流，带自己的日历
    * - family
      - 容器（ ``NodeContainer`` ）
      - takler 没有单独的 Family 类，容器可任意嵌套
    * - task
      - task
      - 叶子节点；takler 用 ``ShellScriptTask`` 运行 shell 脚本
    * - defs 定义文件
      - Python 代码 + ``Bunch``
      - takler 用 Python API 定义工作流（见 :doc:`defining-flows` ），
        用 JSON 做序列化与热加载
    * - variable
      - parameter
      - 用户变量，渲染时沿节点树继承
    * - event / meter / limit / inlimit / repeat / time / trigger
      - 同名
      - 概念一致，语法细节见各主题页
    * - child 命令（ ``init`` / ``complete`` / ``abort`` / ``event`` /
        ``meter`` ）
      - 同名
      - 作业脚本内上报状态的命令

两套系统在能力上的完整差异会在「与 ecFlow 的差异与限制」一页集中说明。
