开发者
==================================

本章面向阅读或参与 takler 开发的读者。

**架构** ：:doc:`architecture` 给出进程视图、包分层与依赖方向、服务
端进程的组装，以及一次作业从提交到 ``complete`` 的完整时序；
:doc:`core-design` 深入 ``takler.core`` 包内 —— 节点继承体系、
sink / swim 状态传播、依赖解析入口、触发器表达式管线与序列化双模式
。

**API 参考** ： :doc:`api/index` 是由 docstring 生成的类与函数文档。

.. toctree::
   :hidden:
   :maxdepth: 2

   architecture
   core-design
   API <api/index>
