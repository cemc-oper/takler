开发者
==================================

本章面向阅读或参与 takler 开发的读者。

**架构** ：:doc:`architecture` 给出进程视图、包分层与依赖方向、服务
端进程的组装，以及一次作业从提交到 ``complete`` 的完整时序；
:doc:`core-design` 深入 ``takler.core`` 包内 —— 节点继承体系、
sink / swim 状态传播、依赖解析入口、触发器表达式管线与序列化双模式
。

**协议与工程** ： :doc:`protocol` 面向客户端实现者 —— 全部 RPC 、
``ServiceResponse.flag`` 的 error_code 语义、凭据 metadata 、
``ForceState`` / ``DepType`` 枚举、 stub 重新生成与跨语言契约测试；
:doc:`contributing` 是环境、风格、测试、覆盖率与文档构建的贡献者指
南； :doc:`extending` 讲自定义任务类型（含 class_type 反射约束）、
``task`` 装饰器与自定义 logging backend 。

**API 参考** ： :doc:`api/index` 是由 docstring 生成的类与函数文档。

.. toctree::
   :hidden:
   :maxdepth: 2

   architecture
   core-design
   protocol
   contributing
   extending
   API <api/index>
