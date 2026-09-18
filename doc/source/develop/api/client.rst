客户端
======

本页收录 Python 客户端包 ``takler.client`` 的编程接口。命令行用法见
:doc:`/guide/cli` ；协议层面的语义（超时、重试窗口、凭据 metadata ）
见 :doc:`/develop/protocol` 。

服务客户端
----------

``TaklerServiceClient`` 封装全部 16 个命令，每个公开方法对应一条客
户端命令；重试与凭据在内部装配，调用方只需要 host 与 port 。

线路相关的一切 —— 连接生命周期、请求/响应编码、凭据 metadata 、重
试循环 —— 都在 ``ClientTransport`` 抽象（ ``takler.client.transport``
）后面，默认实现是 gRPC 的
``takler.client.grpc_transport.GrpcTransport`` ；HTTP transport 随
``takler[http]`` extra 提供（ M3 任务 8 ）。命令方法只收发
``takler.protocol`` 的 DTO 与 payload 字典，不接触生成类。

.. autosummary::
   :toctree: generated

   takler.client.TaklerServiceClient

凭据
----

``takler.client.credentials`` 解析 child 与 operator 两类凭据：环境变
量名与 metadata 键的全表见 :doc:`/operation/reference` 。

.. autosummary::
   :toctree: generated

   takler.client.credentials.current_user_name
   takler.client.credentials.resolve_ca_file
   takler.client.credentials.resolve_server_name
   takler.client.credentials.resolve_secret_file
   takler.client.credentials.read_first_secret

重试
----

``takler.client.retry`` 实现 :doc:`/develop/protocol` 的超时与重试契
约： ``CommandKind`` 区分三类命令， ``RetryPolicy`` 记账重试窗口，
``backoff_seconds`` 给出退避序列， ``resolve_retry_window`` 叠加
``TAKLER_TIMEOUT`` 环境变量的覆盖。

.. autosummary::
   :toctree: generated

   takler.client.retry.CommandKind
   takler.client.retry.RetryPolicy
   takler.client.retry.backoff_seconds
   takler.client.retry.resolve_retry_window

退出码
------

``takler.client.exit_code`` 把 ``ServiceResponse.flag`` 分类码或本地
异常翻译成进程退出码（ ``0`` / ``1`` / ``3`` / ``4`` ），对照表见
:doc:`/operation/reference` 。

.. autosummary::
   :toctree: generated

   takler.client.exit_code.exit_code_for_error_code
   takler.client.exit_code.exit_code_for_exception
