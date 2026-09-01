安全部署
============

本页面向运维人员，说明如何为 Takler 服务启用 TLS 传输加密与命令鉴权，
以及作业一次性口令、zombie 处置策略与审计日志的配置方式。

Takler 的默认部署是**不加密、不鉴权**的：

* 未配置证书时，服务以明文方式绑定监听地址，并在启动时记录一条 WARNING
* 鉴权模式 (``auth_mode``) 默认为 ``disabled``，此时任何能访问到监听端口的调用方都可以执行
  ``requeue``、``suspend``、``force``、``load`` 等控制命令，也可以读取整棵节点树，
  服务在启动时同样记录一条 WARNING 说明这一点

因此升级到本版本不会改变既有部署的通讯方式，启用安全项都是显式操作。

.. note::

    启用鉴权之前请先读完「作业脚本权限与 umask」与「从 M1 部署升级到启用鉴权」两节。
    口令注入与 umask 收紧都必须在服务端开始校验之前就位。


连接配置文件的 security 段
----------------------------

全部安全配置项集中在 ``connect.yaml`` 的 ``security`` 段。
该段的每一项都可以省略，省略即「未配置」，此时使用内置默认值，
因此只含 ``server`` 段（或 ``server`` 加 ``checkpoint`` 段）的旧配置文件仍然可以加载。

.. code-block:: yaml
    :linenos:

    server:
      address:
        hostname: login01
        ip: 10.0.0.9
        port: "33083"

    checkpoint:
      interval: 120
      file: /home/oper/takler/takler.check

    security:
      # TLS：服务端
      server_cert_file: /home/oper/takler/tls/server.crt
      server_key_file: /home/oper/takler/tls/server.key
      # mTLS 预留扩展位，本版本不校验客户端证书
      client_ca_file: null
      # TLS：客户端
      ca_file: /home/oper/takler/tls/ca.crt
      server_name: null
      # 鉴权
      auth_mode: enabled
      operator_secret_file: /home/oper/takler/secret
      operator_whitelist_file: /home/oper/takler/whitelist
      # zombie 处置与审计
      zombie_policy: fail
      audit_file: /home/oper/takler/audit.jsonl

各配置项的含义与默认值：

.. list-table::
    :header-rows: 1
    :widths: 25 12 63

    * - 配置项
      - 默认值
      - 说明
    * - ``server_cert_file``
      - 未配置
      - 服务端证书文件路径，须与 ``server_key_file`` 同时配置
    * - ``server_key_file``
      - 未配置
      - 服务端私钥文件路径，须与 ``server_cert_file`` 同时配置
    * - ``client_ca_file``
      - 未配置
      - 校验客户端证书使用的 CA 证书，mTLS 预留扩展位，本版本不使用
    * - ``ca_file``
      - 未配置
      - 客户端信任的 CA 证书文件路径，未配置时客户端建立不加密连接
    * - ``server_name``
      - 不覆盖
      - 客户端校验服务端证书主机名时使用的名称，用于证书 CN / SAN 与连接地址不一致的情况
    * - ``auth_mode``
      - ``disabled``
      - 鉴权模式，取值 ``disabled`` 或 ``enabled``
    * - ``operator_secret_file``
      - 未配置
      - 运维共享密钥文件路径
    * - ``operator_whitelist_file``
      - 未配置
      - 运维用户白名单文件路径
    * - ``zombie_policy``
      - ``fail``
      - zombie 处置策略，取值 ``fail``、``fob`` 或 ``adopt``
    * - ``audit_file``
      - 未配置
      - 审计日志文件路径，未配置时审计记录写入常规日志目标

服务端与客户端都通过环境变量 ``TAKLER_CONNECT_FILE`` 找到该文件；
服务端也可以用 ``takler-server --config`` 指定。


环境变量
------------

本版本引入七个环境变量。取值优先级为
**命令行选项 > 环境变量 > connect.yaml 的 security 段 > 内置默认值**，
其中空串与纯空白视为「未提供」，逐级向下回落。

.. list-table::
    :header-rows: 1
    :widths: 30 15 55

    * - 环境变量
      - 使用方
      - 说明
    * - ``TAKLER_PASS``
      - 客户端
      - 作业一次性口令。由服务端生成并作为同名变量注入作业脚本，child 命令从该环境变量读取
    * - ``TAKLER_SECRET_FILE``
      - 客户端
      - 运维共享密钥文件路径，对应 ``operator_secret_file``
    * - ``TAKLER_TLS_CA_FILE``
      - 客户端
      - 客户端信任的 CA 证书文件路径，对应 ``ca_file``
    * - ``TAKLER_TLS_SERVER_NAME``
      - 客户端
      - 证书主机名校验的目标名称，对应 ``server_name``
    * - ``TAKLER_AUTH_MODE``
      - 服务端
      - 鉴权模式，对应 ``auth_mode``
    * - ``TAKLER_ZOMBIE_POLICY``
      - 服务端
      - zombie 处置策略，对应 ``zombie_policy``
    * - ``TAKLER_AUDIT_FILE``
      - 服务端
      - 审计日志文件路径，对应 ``audit_file``

Python 客户端 (``takler-client-py``) 与 Go 客户端 (``takler_client``) 识别同一组客户端环境变量。
服务端的 ``auth_mode``、``zombie_policy`` 与 ``audit_file`` 没有对应的命令行选项，
只能通过环境变量或配置文件设置；TLS 的证书与私钥另有 ``--tls-cert`` 与 ``--tls-key`` 两个选项，
其取值优先于配置文件。


TLS 传输加密
----------------

服务端
^^^^^^^^^^

服务端的证书与私钥必须**同时**配置：

* 两者都已配置：以 TLS 绑定监听地址，并记录一条含监听地址与证书路径的 INFO
* 两者都未配置：以明文绑定监听地址，并记录一条含监听地址与「传输未加密」说明的 WARNING
* 只配置其中一个：记录 ERROR 并终止启动，``takler-server`` 向标准错误输出一行说明并以退出码 1 结束
* 文件不存在、不可读或不能被解析为证书与私钥：同样记录 ERROR 并以退出码 1 结束

半配置与文件错误都不会降级为明文启动：运维要求了 TLS 却静默得到明文端口，
比启动失败更难发现。

.. code-block:: bash

    takler-server \
      --tls-cert /home/oper/takler/tls/server.crt \
      --tls-key /home/oper/takler/tls/server.key

客户端
^^^^^^^^^^

客户端以 ``ca_file`` 指定的 CA 证书为根信任建立 TLS 连接，未配置时建立不加密连接。

.. code-block:: bash

    export TAKLER_TLS_CA_FILE=/home/oper/takler/tls/ca.crt

如果证书的 CN / SAN 与客户端连接使用的主机名不一致，用 ``TAKLER_TLS_SERVER_NAME``
（或 ``server_name``）指定校验用的名称。

以不加密通道连接一个已启用 TLS 的服务时，客户端会在重试窗口耗尽后以退出码 4 结束，
并输出服务端地址与总尝试次数。因此配置 TLS 这一步会中断尚未配置 CA 的客户端，
需要一个统一的切换窗口。

.. note::

    mTLS（校验客户端证书）不在本版本实现范围内。
    配置项 ``client_ca_file`` 是为其预留的扩展位：配置了它，服务仍然可以启动，
    但会记录一条 WARNING 说明「本版本不校验客户端证书」。


密钥文件与白名单文件
------------------------

文件格式
^^^^^^^^^^

两个文件使用同一套解析规则：

* 每个非空白行是一个取值
* 忽略空行，忽略去除首尾空白后以 ``#`` 开头的注释行
* 每行去除首尾空白

密钥文件 (``operator_secret_file``) 的**全部**有效行都是有效的运维共享密钥，
这是不停机轮换的基础；客户端只发送其密钥文件中的**第一个**有效行。

.. code-block:: text

    # 2026-07 轮换后的新密钥
    lQ4mR0tXo9nZ2pJ1sVw7cYbA3dFgH6kL
    # 旧密钥，客户端全部更新后删除
    aB9cD2eF4gH6iJ8kL0mN2oP4qR6sT8uV

白名单文件 (``operator_whitelist_file``) 的每个有效行是一个允许执行运维命令的 OS 用户名。
用户名按字节序列完全相等比较，不做大小写折叠、不做前后缀匹配。

.. code-block:: text

    # 值班账户
    oper
    wangdp

两个文件的内容变更**不需要重启服务**：服务按文件的修改时间与大小判断是否重新读取，
保存后对后续请求即时生效。

文件权限
^^^^^^^^^^

密钥文件的机密性完全取决于文件权限，建议设为仅所有者可读写：

.. code-block:: bash

    chmod 0600 /home/oper/takler/secret

如果密钥文件允许所有者之外的用户读或写，服务在启动时记录一条含路径与实际权限位的 WARNING
并继续启动。

启动期校验
^^^^^^^^^^^^

``auth_mode`` 为 ``enabled`` 时：

* 密钥文件路径未配置：记 ERROR 并终止启动
* 密钥文件不存在、不可读或没有任何有效行：记 ERROR（含路径与原因）并终止启动
* 白名单文件路径未配置：记 WARNING 说明「任一持有有效密钥的用户名均被接受」，只用密钥校验运维命令

运行期读不到密钥文件或白名单文件时（被误删、权限被改、共享文件系统抖动），
服务记 ERROR 并拒绝该运维命令，而不是放行。


鉴权模式
------------

``auth_mode`` 取两个值：

* ``disabled``\ （默认）：拦截器放行全部请求，完全不校验凭据
* ``enabled``：按命令分级校验凭据

凭据经 gRPC metadata 的三个键传递，不改动 protobuf 定义：
``takler-pass``\ （作业一次性口令）、``takler-secret``\ （运维共享密钥）、
``takler-user``\ （调用方 OS 用户名）。客户端自动注入，作业脚本与运维命令的写法不变。

命令分级：

.. list-table::
    :header-rows: 1
    :widths: 20 32 48

    * - 级别
      - 命令
      - ``enabled`` 时所需凭据
    * - child
      - ``init``、``complete``、``abort``、``event``、``meter``
      - ``takler-pass``
    * - 运维
      - ``requeue``、``suspend``、``resume``、``run``、``force``、``free-dep``、``load``、``begin``、``show``、``coroutine``
      - ``takler-secret`` 加在白名单中的 ``takler-user``
    * - 公开
      - ``ping``
      - 无

``show`` 与 ``coroutine`` 虽然只读，但都会返回整份工作流定义（节点路径、变量、触发器表达式），
因此与控制命令同级，需要运维凭据。TUI (``takler-tui``) 是它们的主要使用方，
所以启用鉴权后，运行 TUI 的账户也需要能读到密钥文件。

``ping`` 不需要任何凭据，可以直接用于健康检查与监控。

child 命令只校验 ``takler-pass`` 是否存在，取值是否与目标 task 当前的口令一致由 zombie 检测判定。

被拒绝的请求不会进入命令处理逻辑，节点树状态保持不变；客户端以退出码 1 结束，
并输出 ``PermissionDeniedError`` 与服务端返回的说明文本。日志、审计记录与 gRPC 状态说明中
都不会出现任何凭据取值。


作业脚本权限与 umask
------------------------

服务为 task 的每次运行生成一个一次性口令，作为生成变量 ``TAKLER_PASS`` 注入作业脚本。
``head.takler`` 中需要有 ``export TAKLER_PASS={{TAKLER_PASS}}`` 一行，
详见 :doc:`/tutorial/getting-started/understanding-includes`。

.. warning::

    作业脚本文件里含有明文口令，而它的读写权限位**由服务端进程的 umask 决定**
    （takler 只额外添加所有者可执行位，不显式设定读写位）。
    在常见的默认 umask ``0022`` 下，作业脚本对同组用户与其他用户可读，
    在共享文件系统上意味着任何账户都能读到每个在途作业的口令，鉴权因此形同虚设。

    启用鉴权前，请把服务端进程的 umask 设为 ``0077`` 或等价取值：

    .. code-block:: bash

        umask 0077
        takler-server

``auth_mode`` 为 ``enabled`` 且当前 umask 允许所有者之外的用户读取新建文件时，
服务在启动时记录恰好一条 WARNING，内容包含当前 umask、风险说明与建议取值 ``0077``。
该检查在启动时执行一次，不随每次生成作业脚本重复执行。


zombie 检测与处置
---------------------

一个 child 命令如果不属于服务当前记录的运行实例，就是 zombie，
典型情形是 task 被 requeue 之后，旧作业才上报 ``complete``。

三个判定条件按顺序检查，命中第一个即停止：

.. list-table::
    :header-rows: 1
    :widths: 12 88

    * - 条件
      - 含义
    * - ``Z1``
      - 命令携带的口令与目标 task 当前口令不一致，或目标 task 没有口令。仅在 ``auth_mode`` 为 ``enabled`` 时判定
    * - ``Z2``
      - 目标 task 既不是 submitted 也不是 active 状态，两种鉴权模式下都判定
    * - ``Z3``
      - ``init`` 命令携带的 ``task_id`` 与 active 状态目标 task 已记录的取值不一致，两种鉴权模式下都判定

``zombie_policy`` 是服务端全局设置，取三个值：

.. list-table::
    :header-rows: 1
    :widths: 15 85

    * - 取值
      - 处置方式
    * - ``fail``
      - 默认值。不改变目标 task 的任何状态，返回 ``flag=31``，客户端以退出码 3 结束并输出分类名 ``zombie``
    * - ``fob``
      - 不改变目标 task 的任何状态，但返回成功，旧作业静默继续
    * - ``adopt``
      - 执行该命令，并把命令携带的口令与 ``task_id`` 收养为目标 task 的取值

每次处置都记录一条含节点路径、命令名、命中的条件、生效的策略与目标 task 当前状态的 WARNING，
并写出一条审计记录。日志与审计记录都不含口令取值。

.. note::

    ``Z2`` 与 ``Z3`` 在 ``auth_mode`` 为 ``disabled`` 时同样生效，
    这是本版本对既有部署可见的行为改变：requeue 之后旧作业上报的 child 命令会被拒绝，
    而不再静默污染新实例的状态。升级后如需临时保留旧行为，可以把 ``zombie_policy`` 设为 ``fob``。


审计日志
------------

以下三类事件各写出一条审计记录：运维命令执行结束、鉴权拒绝、zombie 处置。

每条记录是一行 JSON 对象（JSON Lines），含八个键：
``timestamp``、``event``、``command``、``user``、``peer``、``target``、``outcome``、``error_code``。

.. code-block:: json

    {"timestamp": "2026-07-15T10:30:00.123456", "event": "control", "command": "requeue", "user": "oper", "peer": "ipv4:10.0.0.9:51234", "target": ["/flow1/family1/task1"], "outcome": "success", "error_code": 0}

* ``event`` 取值 ``control``、``denied``、``zombie``
* ``outcome`` 取值 ``success``、``error``、``denied``、``zombie``
* ``error_code`` 为该请求返回的 ``flag``，鉴权拒绝时固定为 43
* ``user`` 为请求携带的 ``takler-user``，未携带时为 ``unknown``
* 记录中不含口令取值与共享密钥取值

配置了 ``audit_file``（或 ``TAKLER_AUDIT_FILE``）时，审计记录**只**写入该文件，
不进入 ``TAKLER_LOG_FILE`` 配置的常规日志文件与控制台；
未配置时，审计记录写入常规日志目标。

审计文件由服务创建，权限为仅所有者可读写 (``0600``)，父目录不存在时自动创建。
审计文件每行是完整的 JSON，没有时间戳前缀，可以直接交给 ``jq`` 处理：

.. code-block:: bash

    # 今天被拒绝的运维命令，以及是谁发起的
    jq -r 'select(.event == "denied") | [.timestamp, .user, .command, .peer] | @tsv' audit.jsonl

写审计文件失败时，服务向常规日志记一条含路径与原因的 WARNING，请求的响应不受影响：
审计是观测手段，不是可用性单点。


从 M1 部署升级到启用鉴权
----------------------------

推荐按以下顺序分五步进行，每一步都可以单独验证：

1. **升级到本版本但不启用任何安全项。**
   服务端与两个客户端都升级，``auth_mode`` 保持 ``disabled``、不配 TLS。
   此时验证 ``Z2`` / ``Z3`` zombie 检测与作业脚本权限变化没有打破既有流程。
2. **在 head.takler 中加入 ``export TAKLER_PASS={{TAKLER_PASS}}``。**
   此时服务端还没校验，加了不生效但无害，可以逐个 flow 灰度。
3. **把服务端进程 umask 设为 ``0077``。**
   在启用鉴权之前完成，否则口令一启用即泄露。
4. **配置 TLS。**
   服务端配证书与私钥，客户端配 CA。这一步会中断未配 CA 的客户端，需要一个统一切换窗口。
5. **配置密钥与白名单文件，最后把 ``auth_mode`` 切到 ``enabled``。**
   切换前用启动日志中的 WARNING 确认没有「在途但无口令」的任务，或先把在途任务跑完再切。
   密钥文件可以先只写一行，后续轮换按「不停机轮换运维共享密钥」一节进行，不需要再停机。

顺序的关键约束是**第 2 步必须早于第 5 步、第 3 步必须早于第 5 步**：
口令注入与 umask 收紧都要在服务端开始校验之前就位。

``connect.yaml`` 与旧的快照文件都不需要改动或迁移：
快照中缺少口令映射时按空映射处理，在途任务在 ``auth_mode`` 为 ``disabled`` 下照常上报。


不停机轮换运维共享密钥
--------------------------

服务端接受密钥文件中的全部取值，客户端只发送第一个有效行，
因此轮换不需要一个所有客户端同时切换的时间窗口：

1. **在服务端密钥文件中加入新取值。**
   文件保存后即时生效，此时新旧取值同时有效。服务端不关心行的顺序。
2. **逐个更新客户端的密钥文件，把新取值放在第一个有效行。**
   这一步可以持续任意长时间，已更新与未更新的客户端都能正常工作。
3. **从服务端密钥文件中删除旧取值。**
   此后仍在使用旧取值的客户端会被拒绝，拒绝原因分类为 ``invalid_credential``。

第 3 步之后如果还有残留的旧客户端，审计日志中 ``event`` 为 ``denied`` 的记录带 ``user`` 字段，
可以直接看出还有谁没更新完。


排查
--------

``ping`` 通而其他命令全挂
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

这是「服务端已启用鉴权、客户端未升级」的特征症状，不是网络问题：
``ping`` 免认证因此照常返回，而未升级的客户端不发送任何凭据，
child 命令与运维命令都因缺少凭据被拒绝。
检查服务端启动日志中的鉴权模式记录，以及客户端版本与 ``TAKLER_SECRET_FILE`` 配置。

把快照文件发给他人排查
^^^^^^^^^^^^^^^^^^^^^^^^^

快照文件 (``takler.check``) 的顶层 ``job_passwords`` 键保存着全部在途作业的一次性口令，
文件权限也因此收紧为 ``0600``。需要把快照发给他人分析时，先做脱敏：

.. code-block:: bash

    jq 'del(.job_passwords)' takler.check > takler.check.shared
