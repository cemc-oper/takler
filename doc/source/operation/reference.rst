全局参考表
==========

本页把运维中需要查阅的四类参考信息汇总在四张总表里：环境变量、
``connect.yaml`` 配置项、 ``error_code`` 分类、客户端退出码。每张表
只给出一行结论，语义细节与配置示例跟随链接到各自的详解页。

环境变量
--------

按**读取方**分三组。注意服务端**不读** ``TAKLER_HOST`` /
``TAKLER_PORT`` ——它的地址来自 ``--host`` / ``--port`` 或
``connect.yaml`` 。

服务端进程
~~~~~~~~~~

.. list-table::
    :header-rows: 1
    :widths: 24 44 14 18

    * - 变量
      - 含义与取值
      - 未设置时
      - 详解
    * - ``TAKLER_CONNECT_FILE``
      - ``connect.yaml`` 的路径。读不到时只记一条 WARNING 并照常启动
        （与 ``--config`` 读不到即启动失败不同）
      - 不带配置文件启动
      - :doc:`/operation/connect-config`
    * - ``TAKLER_LOG_LEVEL``
      - 日志级别， ``TRACE`` ~ ``CRITICAL`` 大小写不敏感；无法识别的
        取值记 WARNING 后按 ``INFO``
      - ``INFO``
      - :doc:`/operation/logging`
    * - ``TAKLER_LOG_FILE``
      - 常规日志文件路径，父目录自动创建
      - 仅控制台
      - :doc:`/operation/logging`
    * - ``TAKLER_AUDIT_FILE``
      - 审计日志文件路径，优先于 ``security.audit_file``
      - 审计记录写入常规日志目标
      - :doc:`/operation/audit`
    * - ``TAKLER_EXCEPTION_POLICY``
      - 未预期异常的处理策略， ``resilient`` / ``fail_fast``
        （ ``-`` 与 ``_`` 等价）；无法识别的取值记 WARNING 后按
        ``resilient``
      - ``resilient``
      - :doc:`/operation/resilience`
    * - ``TAKLER_AUTH_MODE``
      - 鉴权模式， ``disabled`` / ``enabled`` ；无法识别的取值记
        WARNING 后按 ``disabled``
      - ``disabled``
      - :doc:`/operation/security`
    * - ``TAKLER_ZOMBIE_POLICY``
      - zombie 处置策略， ``fail`` / ``fob`` / ``adopt`` ；无法识别
        的取值记 WARNING 后按 ``fail``
      - ``fail``
      - :doc:`/operation/zombie`

客户端
~~~~~~

适用于 ``takler-client-py`` 、 ``takler-tui`` 与作业脚本里的 child
命令（ ``takler_client`` 的对应选项见 :doc:`/guide/cli` ）。

.. list-table::
    :header-rows: 1
    :widths: 24 44 14 18

    * - 变量
      - 含义与取值
      - 未设置时
      - 详解
    * - ``TAKLER_CONNECT_FILE``
      - ``connect.yaml`` 的路径，提供地址与 ``security`` 段设置。
        读不到时按失败契约处理： stderr 一行，退出码 ``3``
      - 不用配置文件
      - :doc:`/operation/connect-config`
    * - ``TAKLER_HOST`` / ``TAKLER_PORT``
      - 服务地址。优先级低于命令行选项与 ``TAKLER_CONNECT_FILE``
        指向的文件
      - ``localhost:33083``
      - :doc:`/guide/cli`
    * - ``TAKLER_TIMEOUT``
      - 重试窗口秒数，须为非负整数字符串， ``0`` 表示只试一次；非法
        取值记 WARNING 后按命令类默认
      - child 命令 ``86400`` ，其余 ``60``
      - :doc:`/guide/cli`
    * - ``TAKLER_TRANSPORT``
      - 客户端使用的 transport ， ``grpc`` / ``http`` ；无法识别的取值
        记 WARNING 后按 ``grpc`` 。优先级低于显式参数与
        ``connect.yaml`` 的 ``server.transport``
      - ``grpc``
      - :doc:`/operation/deployment`
    * - ``TAKLER_TLS_CA_FILE``
      - 客户端信任的 CA 证书。文件不可用时报错于**请求发出之前**
        （退出码 ``1`` ）
      - 明文连接
      - :doc:`/operation/security`
    * - ``TAKLER_TLS_SERVER_NAME``
      - 校验证书主机名时使用的覆盖名
      - 用连接的主机名校验
      - :doc:`/operation/security`
    * - ``TAKLER_SECRET_FILE``
      - 运维共享密钥文件。读不到时记 WARNING 并**不带密钥**发出请求，
        由服务端决定是否拒绝
      - 不携带 ``takler-secret``
      - :doc:`/operation/security`
    * - ``TAKLER_PASS``
      - 作业口令，仅 child 命令发送
      - 不携带 ``takler-pass``
      - :doc:`/guide/job-management`
    * - ``TAKLER_NAME``
      - child 命令 ``--node-path`` 的默认来源；作业文件中已导出
      - 无（缺失时用法错误，退出码 ``2`` ）
      - :doc:`/guide/cli`
    * - ``NO_TAKLER``
      - **已设置** （不看取值）时 child 命令不发起请求、直接成功返回，
        供脱离服务调试脚本
      - 未设置
      - :doc:`/guide/task-script`
    * - ``TAKLER_LOG_LEVEL`` / ``TAKLER_LOG_FILE``
      - 客户端日志走同一个日志子系统，规则与服务端相同
      - ``INFO`` / 仅控制台
      - :doc:`/operation/logging`
    * - ``LOGNAME`` / ``USER``
      - ``takler-user`` 元数据（审计记录里的 ``user`` ）的回退来源
      - 依次探测，都不可得则不携带
      - :doc:`/operation/audit`

.. note::

    ``takler-tui`` 的地址解析顺序与 CLI 相反——``connect.yaml`` 的
    优先级**高于** ``--host`` / ``--port`` ，见 :doc:`/guide/tui`
    。TUI 与 Python 客户端走同一条解析链： ``security`` 段与
    ``server.transport`` 对 TUI 同样生效。

作业脚本变量
~~~~~~~~~~~~

``TAKLER_HOST`` 、 ``TAKLER_PASS`` 等变量出现在作业脚本里，但它们
**不是** 服务读写的环境变量：它们是渲染时替换进作业文件的模板参数，
由脚本的头文件（ ``head.takler`` ） ``export`` 成作业进程的环境
变量。完整清单（含 ``DATE`` / ``TIME`` / ``TAKLER_JOB`` 等生成变量
的取值规则）见 :doc:`/guide/variables` 的保留变量全表，生命周期语义
见 :doc:`/guide/job-management` 。

connect.yaml 配置项
-------------------

全部字段如下；字段语义与共用布局见 :doc:`/operation/connect-config`
， ``security`` 段细节见 :doc:`/operation/security` 。

.. list-table::
    :header-rows: 1
    :widths: 34 20 46

    * - 字段
      - 默认值
      - 覆盖来源（优先级从高到低）
    * - ``server.address.hostname``
      - 无（必填）
      - ``--host`` > 本字段 > ``localhost``
    * - ``server.address.ip``
      - 无（必填）
      - 仅作记录，不参与地址解析
    * - ``server.address.port``
      - 无（必填）
      - ``--port`` > 本字段 > ``33083``
    * - ``server.http.host`` / ``server.http.port``
      - ``0.0.0.0`` / 无（ ``http`` 小节存在时必填）
      - 无； ``http`` 小节存在即在独立端口挂 HTTP transport （需
        ``takler[http]`` ），省略则保持 gRPC-only
    * - ``server.http.tls_cert_file`` / ``server.http.tls_key_file``
      - 回落到 gRPC 证书对
      - 无；成对配置，只配置其一终止启动
    * - ``server.transport``
      - ``grpc``
      - **客户端** 侧： 显式参数 > 本字段 > ``TAKLER_TRANSPORT`` >
        ``grpc`` ；服务端忽略本字段
    * - ``checkpoint.interval``
      - ``120`` 秒
      - ``--checkpoint-interval`` > 本字段（小于 ``10`` 回退 ``120`` ）
    * - ``checkpoint.file``
      - ``takler.check``
      - ``--checkpoint-file`` > 本字段
    * - ``security.server_cert_file`` / ``security.server_key_file``
      - 未配置（明文）
      - ``--tls-cert`` / ``--tls-key`` > 本字段；两者必须成对
    * - ``security.client_ca_file``
      - 未配置
      - 无（ mTLS 预留，本版本不校验客户端证书）
    * - ``security.ca_file``
      - 未配置
      - 客户端侧： ``TAKLER_TLS_CA_FILE`` > 本字段
    * - ``security.server_name``
      - 不覆盖
      - 客户端侧： ``TAKLER_TLS_SERVER_NAME`` > 本字段
    * - ``security.auth_mode``
      - ``disabled``
      - ``TAKLER_AUTH_MODE`` > 本字段
    * - ``security.operator_secret_file``
      - 未配置
      - 服务端无覆盖来源；客户端经 ``TAKLER_SECRET_FILE`` 读同一文件
    * - ``security.operator_whitelist_file``
      - 未配置（不校验白名单）
      - 无
    * - ``security.zombie_policy``
      - ``fail``
      - ``TAKLER_ZOMBIE_POLICY`` > 本字段
    * - ``security.audit_file``
      - 未配置
      - ``TAKLER_AUDIT_FILE`` > 本字段

异常策略 ``--exception-policy`` / ``TAKLER_EXCEPTION_POLICY``
**不在** ``connect.yaml`` 中配置，见 :doc:`/operation/resilience`
。

error_code 分类表
-----------------

服务响应的 ``flag`` 字段： ``0`` 表示成功，任何非零值表示失败——只
判断 ``flag != 0`` 的客户端始终兼容。非零时 ``flag`` 携带下表的分
类码，客户端 stderr 上的``错误分类名``即来自此表（如
``node_not_found: ...`` ）。

.. list-table::
    :header-rows: 1
    :widths: 8 22 26 32 12

    * - 码
      - 分类名
      - 异常类
      - 典型场景
      - 退出码
    * - ``0``
      - ``success``
      - —
      - 命令成功
      - ``0``
    * - ``1``
      - ``takler_error``
      - ``TaklerError``
      - 没有专属码的 takler 错误（兜底）
      - ``1``
    * - ``10``
      - ``node_not_found``
      - ``NodeNotFoundError``
      - 请求路径上不存在节点
      - ``1``
    * - ``11``
      - ``invalid_node_path``
      - ``InvalidNodePathError``
      - 节点路径格式错误（如非绝对路径）
      - ``1``
    * - ``12``
      - ``node_type``
      - ``NodeTypeError``
      - 节点存在但类型与操作不符
      - ``1``
    * - ``13``
      - ``unsupported_value``
      - ``UnsupportedValueError``
      - 取值超出支持集
      - ``1``
    * - ``14``
      - ``flow_state``
      - ``FlowStateError``
      - flow 状态不允许该操作
      - ``1``
    * - ``15``
      - ``invalid_request``
      - ``InvalidRequestError``
      - 其他请求内容错误
      - ``1``
    * - ``20``
      - ``expression_syntax``
      - ``ExpressionSyntaxError``
      - 触发器表达式解析失败
      - ``1``
    * - ``30``
      - ``job_submission``
      - ``JobSubmissionError``
      - 作业脚本渲染或提交失败
      - ``3``
    * - ``31``
      - ``zombie``
      - ``ZombieError``
      - 上报被判定为 zombie 且策略为 ``fail``
      - ``3``
    * - ``40``
      - ``transport``
      - ``TransportError``
      - 传输层失败（客户端本地抛出）
      - ``4``
    * - ``41``
      - ``client_connection``
      - ``ClientConnectionError``
      - 重试窗口耗尽仍连不上（客户端本地抛出）
      - ``4``
    * - ``42``
      - ``server_response``
      - ``ServerResponseError``
      - 响应无法解析（客户端本地抛出）
      - ``3``
    * - ``43``
      - ``permission_denied``
      - ``PermissionDeniedError``
      - 鉴权拒绝
      - ``1``
    * - ``99``
      - ``internal_error``
      - 任何非 ``TaklerError`` 异常
      - 服务内部错误
      - ``3``

码的分组规律： ``10`` ~ ``15`` 是请求内容错误， ``20`` 是表达式，
``30`` ~ ``31`` 是作业生命周期， ``40`` ~ ``43`` 是传输与凭据，
``99`` 是服务内部错误。映射按**确切异常类型**查找、不沿继承链：
没有专属码的 ``TaklerError`` 子类落到 ``1`` ，非 takler 异常落到
``99`` ；未注册的码显示为 ``unknown`` 。 ``40`` / ``41`` / ``42``
三类由客户端本地抛出，不会出现在服务响应里。审计记录的
``error_code`` 键取同一组取值，见 :doc:`/operation/audit` 。

客户端退出码
------------

``takler-client-py`` 与 ``takler_client`` 共用同一套退出码，作业脚本
配合 ``set -e`` 依赖它判断成败：

.. list-table::
    :header-rows: 1
    :widths: 10 90

    * - 退出码
      - 含义
    * - ``0``
      - 成功（ child 命令在 ``NO_TAKLER`` 已设置时短路，也以 ``0``
        退出）
    * - ``1``
      - 请求不被接受：节点不存在、路径格式错误、取值不支持、表达式
        语法错误、鉴权拒绝、 TLS CA 文件不可用等（对应上表退出码
        ``1`` 的分类）
    * - ``2``
      - 命令行用法错误：缺必需选项、未知子命令等，在发起任何请求之
        前由参数解析拒绝
    * - ``3``
      - 服务端执行失败（作业提交失败、 zombie 拒绝上报等），或服务
        返回了客户端无法解析的响应；客户端本地读到损坏的
        ``TAKLER_CONNECT_FILE`` 也归此类
    * - ``4``
      - 重试窗口耗尽仍连不上服务，或其他传输层失败

失败时 stderr 恰好一行：服务端拒绝是 ``错误分类名: 服务端消息`` ，
客户端本地失败是 ``异常类型名: 描述`` ；未预期异常的 traceback 只
写入 ``TAKLER_LOG_FILE`` 。命令级细节见 :doc:`/guide/cli` 。
