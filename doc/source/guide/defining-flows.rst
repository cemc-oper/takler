定义工作流
==========

takler 用 Python 代码定义工作流，不需要独立的定义文件格式。本页汇总
定义、组织、校验与加载工作流的常用写法；各属性的详细用法见教程的对应
章节。

构建节点树
----------

通过 :py:meth:`~takler.core.NodeContainer.add_task` 与
:py:meth:`~takler.core.NodeContainer.add_container` 逐级搭建节点树：

.. code-block:: python

    from takler.core import Flow
    from takler.tasks.shell import ShellScriptTask

    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", "/path/to/takler-tutorial")

    task1 = flow.add_task(ShellScriptTask("t1", script_path="test/task1.takler"))

    group1 = flow.add_container("group1")
    task2 = group1.add_task(ShellScriptTask("t2", script_path="test/task2.takler"))

也可以用 ``with`` 语句按缩进分组，层级关系一目了然。 ``with`` 只起
分组作用，进入与退出时没有额外副作用：

.. code-block:: python

    with Flow("test") as flow:
        flow.add_parameter("TAKLER_HOME", "/path/to/takler-tutorial")

        with flow.add_task(ShellScriptTask("t1", script_path="test/task1.takler")):
            pass

        with flow.add_container("group1") as group1:
            with group1.add_task(ShellScriptTask("t2", script_path="test/task2.takler")):
                pass

两种写法构建出的节点树完全等价。

``add_task`` / ``add_container`` 既接受对象也接受名字字符串。传字符串
时创建的是基类 :py:class:`~takler.core.Task` /
:py:class:`~takler.core.NodeContainer` ——基类 ``Task`` 不含任何作业
逻辑，实际运行应使用
:py:class:`~takler.tasks.shell.ShellScriptTask` 或自定义的 ``Task``
子类。

Bunch 与多 flow
---------------

服务端用 :py:class:`~takler.core.Bunch` 容纳所有 flow ；定义脚本通常
在末尾把 flow 挂上去：

.. code-block:: python

    from takler.core import Bunch

    bunch = Bunch()
    bunch.add_flow(flow)

几点注意：

* 一个服务进程持有一个 ``Bunch`` ，其中可以同时存在多个 flow
* flow 按名字索引， ``add_flow`` 同名 flow 会覆盖已有定义
* ``Bunch`` 持有服务级参数（ ``TAKLER_HOST`` 、 ``TAKLER_PORT`` 、
  ``TAKLER_HOME`` ），沿参数继承链对所有节点可见
* 节点路径的第一段就是 flow 名， ``bunch.find_node("/test/t1")`` 可以
  定位任意 flow 下的任意节点

序列化与加载
------------

节点树可以序列化为 dict ，进而存成 JSON：

.. code-block:: python

    import json

    from takler.core import Flow, SerializationType

    # 导出定义
    with open("my_flow.json", "w") as f:
        json.dump(flow.to_dict(), f)

    # 从 JSON 重建
    with open("my_flow.json") as f:
        flow2 = Flow.from_dict(json.load(f), method=SerializationType.Tree)

反序列化有两种模式（ ``SerializationType`` 枚举）：

* ``Tree`` ：只恢复定义（结构、参数、依赖、属性），所有节点回到初始
  状态 ``unknown`` ， flow 未 begun、日历为空。客户端的 ``load`` 命令
  用这种模式——加载得到的是一份全新定义，需要显式 ``begin`` 才会开始
  运行（见 :doc:`/tutorial/controlling-the-flow` ）
* ``Status`` ：连同运行时状态一起恢复（节点状态、 ``suspended`` 、
  事件与标尺取值、 ``try_no`` 、日历、 begun 标记等）。服务端的
  checkpoint 用这种模式（见 :doc:`/tutorial/zombies-and-restart` ）

``Bunch.to_dict()`` 把整个 :py:class:`~takler.core.Bunch`
（所有 flow 加上服务参数）序列化为一个 dict ，是 checkpoint 文件的
内容来源。

校验：check_job_creation
---------------------------

不启动服务就能验证一份工作流定义：
``takler.tasks.shell.check_job_creation(flow)`` 遍历树中所有
``ShellScriptTask`` ，把每个任务的作业脚本渲染到 ``TAKLER_HOME`` 下，
但不提交运行。渲染失败（脚本不存在、 include 无法解析、变量未定义等）
会抛出 ``JobSubmissionError`` 。换言之，渲染通过说明这份定义在结构上
是可运行的。

教程的 :doc:`/tutorial/getting-started/checking-job-creation` 一节有
完整示例。注意这是一次「干跑」：它会在 ``TAKLER_HOME`` 下真实生成
``*.job*`` 文件，只是不执行。

轻量任务：task 装饰器
-------------------------------------------

``takler.core.task`` 把一个函数包装成 ``Task`` 子类：依赖满足时在
**服务进程内** 执行函数体，执行前后自动调用 ``init`` / ``complete``
上报状态。被装饰的函数接收一个 ``self`` 参数，即任务节点本身：

.. code-block:: python

    from takler.core import task

    @task("notify")
    def notify(self):
        print(f"{self.node_path} finished upstream work")

    flow.add_task(notify())

.. warning::

    函数体在调度器主循环中同步执行，执行期间整个服务都暂停调度。因此
    装饰器任务只适合轻量操作（置事件、发通知、小计算）；运行 shell
    脚本或耗时计算请使用 :py:class:`~takler.tasks.shell.ShellScriptTask` 。

.. note::

    ``takler.core.async_task`` 是 ``task`` 的协程变体，但它生成的
    ``run`` 方法是协程函数，而调度器以同步方式调用 ``run()`` ——
    协程永远不会被 await，函数体不会执行。在当前版本请使用
    ``task`` ，不要用 ``async_task`` 。
