扩展指南
========

本页讲三条扩展路径： 自定义任务类型、 ``task`` / ``async_task`` 装饰
器、自定义 logging backend 。前提知识在 :doc:`architecture` 与
:doc:`core-design` 。

自定义任务类型
--------------

``run()`` 的模板方法结构
~~~~~~~~~~~~~~~~~~~~~~~~

:py:class:`~takler.core.Task` 的 ``run()`` 是一个三段模板：

#. ``before_run()`` —— 调 ``increment_try_no`` ： ``try_no`` 加一、清
   空 ``task_id`` 与 ``aborted_reason`` 、轮换一次性
   ``job_password`` 、刷新 generated 参数；
#. ``do_run()`` —— **子类的扩展点** ，做真正的提交。返回 ``False``
   则中止本次运行， ``after_run`` 不会执行（提交失败走这条路，任务不
   会被置成 ``submitted`` ）；
#. ``after_run()`` —— 把任务置为 ``submitted`` 并触发 swim 状态传播
   。

:py:class:`~takler.tasks.shell.ShellScriptTask` 就是照这个形状实现
的： ``do_run`` 里渲染脚本、写作业文件、派生 ``/bin/sh -c`` 子进程，
之后的状态推进（ ``active`` → ``complete`` / ``aborted`` ）由作业进
程经 child 命令上报驱动（见 :doc:`architecture` 的时序图）。

两种语义模式
~~~~~~~~~~~~

* **派生外部工作** （ ``ShellScriptTask`` 模式）：覆写 ``do_run`` ，在
  里面把作业交出去就返回 ``True`` ； ``after_run`` 置
  ``submitted`` ，之后等外部工作经 child 命令上报。**不要** 在
  ``do_run`` 里自己调 ``complete()`` —— 它返回后 ``after_run`` 会把
  状态盖回 ``submitted`` 。
* **进程内即时完成** （装饰器模式）：整个覆写 ``run()`` ，先
  ``Task.run(self)`` 走标准的三段（落 ``submitted`` ），再自己
  ``init()`` → 干活 → ``complete()`` 。函数体跑在 **服务端进程** 的
  调度主循环里，耗时操作会卡住所有 flow 的调度，只适合秒级以内的任
  务。

后一种的最小例子（这也是 ``task`` 装饰器生成的代码形状）：

.. code-block:: python

    from pathlib import Path

    from takler.core import Task


    class MarkerTask(Task):
        """运行即在 marker_path 写一行节点路径的内联任务。"""

        def __init__(self, name: str, marker_path=None):
            super().__init__(name)
            self.marker_path = marker_path

        def run(self):
            Task.run(self)  # before_run / do_run / after_run -> submitted
            self.init()     # -> active
            Path(self.marker_path).write_text(self.node_path + "\n")
            self.complete() # -> complete

受信任注册与独立 codec
~~~~~~~~~~~~~~~~~~~~~~

在服务启动代码中显式注册类型。上传定义只能引用已经注册的稳定 ID，
不能指定模块路径、函数源码或自动发现入口。构造器由注册项提供，
可以接收定义参数；构建阶段不得读取文件或执行任务。

.. code-block:: python

    from takler.schema.definition import DefinitionModel
    from takler.serialization import NodeRegistration, get_registry

    class MarkerDefinition(DefinitionModel):
        marker_path: str

    class MarkerRuntime(DefinitionModel):
        pass

    get_registry().register(NodeRegistration(
        type_id="example.marker",
        python_type=MarkerTask,
        kind="task",
        definition_schema=MarkerDefinition,
        export_definition=lambda node: {"marker_path": str(node.marker_path)},
        construct=lambda name, data: MarkerTask(name, data.marker_path),
        runtime_schema=MarkerRuntime,
        export_runtime=lambda node: {},
        restore_runtime=lambda node, data: None,
        query=lambda node: {"marker_path": str(node.marker_path)},
    ))

定义字段位于 ``type_data``，运行扩展字段位于 checkpoint 的
``runtime_data``。schema 必须 strict 且 extra=forbid；禁止重复 ID、
重复精确 Python 类型或覆盖内建 ``takler.*`` 命名空间。缺少某个入口
需要的 codec 会报 ``missing_codec``，未知类型报 ``unknown_type`` /
``unregistered_type``。查询投影接口供后续只读查询接入，必须剔除
``secret_fields``，不能返回执行对象。

注册是受信任 Python 代码，codec 不是沙箱。不要在定义 schema 中声明
运行身份、口令或认证字段；业务机密也不能进入查询投影。
``job_password`` 只由 checkpoint 独立的在途口令映射保存。

生成变量
~~~~~~~~

任务自带的 generated 参数（ ``TAKLER_NAME`` / ``TAKLER_PASS`` 等五个）
由 ``TaskNodeGeneratedParameters`` 提供。自定义类型要加自己的生成变
量时，照 ``ShellScriptTask`` 的三件套抄：一个 pydantic 模型集中声明
字段与 ``update_parameters`` ，再覆写任务上的
``update_generated_parameters`` / ``find_generated_parameter`` /
``generated_parameters_only`` 三个方法做合并与查找。规则细节见
:doc:`/guide/variables` 。

``task`` 与 ``async_task`` 装饰器
---------------------------------

``takler.core.task(name)`` 把一个函数变成内联任务： 调用被装饰函数即
得到一个 ``Task`` 子类实例，其 ``run()`` 按上面「进程内即时完成」模
式执行 —— ``init()`` → 函数体（ ``self`` 注入为关键字参数） →
``complete()`` 。适合本地示例、冒烟测试与秒级任务。生成的局部类型没有稳定身份，
未经显式注册可重建工厂和 codec 不得导出或恢复。

``async_task`` **目前不可用于自动调度** ： 它生成的 ``run`` 是协程函
数，而调度器同步调用 ``run()`` ，函数体永远不会执行。该限制已在
:doc:`/guide/defining-flows` 中记录并有测试钉住；在此修复之前请用
``task`` 。

自定义 logging backend
----------------------

日志子系统（配置项见 :doc:`/operation/logging` ）的后端抽象是两层的
ABC ，都在 ``takler/logging/backends/__init__.py`` ：

* ``LoggingBackend`` 三个抽象方法： ``map_level`` （把 canonical 级别
  映射到底层库的表示；不支持的级别向 **更详细** 的方向就近回落，模
  块级 ``map_level`` 助手已实现在这个逻辑）、 ``apply_config`` （安
  装 sink 并设级别；必须幂等 —— 先拆掉自己上次装的 sink 再装新
  的； **不向调用方抛异常** ，应用失败的设置经返回的
  ``ApplyResult`` 上报）、 ``get_named_logger`` ；
* ``NamedLogger`` 只需实现 ``log`` 一个抽象方法，
  ``trace`` ~ ``critical`` 六个便捷方法已在其上搭好。

**选择机制是探测，不是注册表** ： ``select_backend()`` 尝试
``import loguru`` ，成功用 ``LoguruBackend`` ，否则用
``StdlibBackend`` ，结果按进程缓存。目前没有注册自定义后端的公开
API —— 要装自己的实现，只能在首次 ``get_logger`` 之前替换
``takler.logging.backends`` 里的模块级私有单例 ``_BACKEND`` ，这属于
内部接缝，不享受接口稳定性保证； ``reset_backend()`` 是测试专用，生产代码不应调
用。

验证清单
--------

一个自定义任务类型落地前，建议逐项过一遍：

#. 单元测试： ``run()`` 模板顺序（ ``before_run`` → ``do_run`` →
   ``after_run`` ）、 ``do_run`` 返回 ``False`` 不进
   ``submitted`` ；
#. 定义与 runtime 分别往返，确认 type_id 还原原类型与自定义字段，
   并验证未注册/缺 codec 时明确失败；
#. 重启恢复： 把 flow 载入服务、快照、重启，确认恢复成功且在途状态
   正确（步骤参考 :doc:`/tutorial/advanced-topics/restart` ）。
