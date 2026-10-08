协议层
======

``takler.protocol`` 是传输中立的协议模型： 16 个命令的请求/响应 DTO
（ ``commands`` ）、信封（ ``envelope`` ）与 error_code 表
（ ``error_code`` ）。它只依赖 pydantic 与 ``takler.exceptions`` ，
服务端与两个客户端都建立在它之上。协议语义见 :doc:`/develop/protocol` 。

.. automodule:: takler.protocol

命令 DTO
--------

.. automodule:: takler.protocol.commands
   :members:

信封
----

.. automodule:: takler.protocol.envelope
   :members:

错误码
------

``error_code`` 模块在 M3 任务 4 从 ``takler.server.protocol`` 迁入本
包，是 ``ServiceResponse.flag`` 取值的唯一事实来源。

.. automodule:: takler.protocol.error_code
   :members:

查询 DTO
--------

``QueryDocument v1`` 的 Python 基类由 HTTP 与 gRPC 共用；初始页与详情
的完整版本、字段和预算契约见 R2 任务文档。

.. autoclass:: takler.protocol.query_v1.QueryModel
