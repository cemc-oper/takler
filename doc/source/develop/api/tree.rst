结构
==================================

本页收录 ``takler.core`` 的树形结构对象与 ``takler.visitor`` 的遍历工
具。每个条目由 autosummary 生成独立页面，列出全部公开成员。

树形结构
---------

.. autosummary::
   :toctree: generated

   takler.core.Bunch
   takler.core.Flow
   takler.core.NodeContainer
   takler.core.Task
   takler.core.calendar.Calendar

.. autoclass:: takler.core.node.Node
   :members:

.. 注释： ``Node`` 不用 autosummary 收录是有意为之 —— 它的类 docstring 带
   numpy 风格 ``Attributes`` 一节， napoleon 会把它转换成一串
   ``.. attribute::`` 指令； autosummary 提取摘要时会再次解析这些指令，
   在页面上注册出重复的 ``name`` 目标（ Sphinx 9.1 的已知行为）。直接用
   ``autoclass`` 则没有摘要提取这一步，目标只注册一次。

任务装饰器
----------

``task`` 与 ``async_task`` 把函数变成内联任务，用法与限制见
:doc:`/develop/extending` 。

.. autosummary::
   :toctree: generated
   :nosignatures:

   takler.core.task
   takler.core.async_task

访问工具
----------

.. autosummary::
   :toctree: generated

   takler.visitor.NodeVisitor
   takler.visitor.SimplePrintVisitor
   takler.visitor.PrintVisitor
   takler.visitor.pre_order_travel
