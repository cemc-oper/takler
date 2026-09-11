服务端
======

本页收录 ``takler.server`` 包的主要模块。**这些模块是服务的内部实
现** ，接口稳定性弱于 ``takler.core`` 与 ``takler.client`` —— 模块
内的类与函数可能在不发 major 版本的情况下调整，外部代码只应依赖前两者
。

收录采用 ``automodule`` 粗粒度方式：每个模块一页一节，列出全部公开成
员。生成的 gRPC stub （ ``takler.server.protocol.takler_pb2*`` ）不
在收录范围，其内容以 ``takler.proto`` 与 :doc:`/develop/protocol` 为
准。

调度与网络服务
--------------

``Scheduler`` 是调度主循环的载体； ``TaklerService`` 把 16 个 RPC 接
进节点树。机制见 :doc:`/develop/architecture` 。

.. automodule:: takler.server.scheduler
   :members:

.. automodule:: takler.server.network_service
   :members:

鉴权与僵尸检测
--------------

``auth`` 模块负责凭据解析、权限分级与拒绝分类； ``zombie`` 模块实现
Z1/Z2/Z3 僵尸判定。语义见 :doc:`/operation/security` 与
:doc:`/operation/zombie` 。

.. automodule:: takler.server.auth
   :members:

.. automodule:: takler.server.zombie
   :members:

审计、检查点与 TLS
------------------

.. automodule:: takler.server.audit
   :members:

.. automodule:: takler.server.checkpoint
   :members:

.. automodule:: takler.server.tls
   :members:

连接配置
--------

``connect_config`` 定义 ``.takler_connect.json`` 的 pydantic 模型与环
境变量解析，配置项全表见 :doc:`/operation/reference` 。

.. automodule:: takler.server.connect_config
   :members:

协议错误码
----------

``error_code`` 模块刻意不 import 生成的 stub ，客户端可以独立使用
（见 :doc:`/develop/protocol` ）。

.. automodule:: takler.server.protocol.error_code
   :members:
