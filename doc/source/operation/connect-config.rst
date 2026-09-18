连接配置 connect.yaml
=====================

``connect.yaml`` 是服务端与客户端**共用**的一份 YAML 配置文件：一份
文件同时描述「服务在哪里」（ ``server`` 段）、「快照怎么写」
（ ``checkpoint`` 段）与「安全态势」（ ``security`` 段），服务端与
两个客户端各自从中读取自己需要的部分。本页列出全部字段、使用方与
取值优先级链； ``security`` 段各字段的语义细节见
:doc:`/operation/security` 。

如何找到这份文件
----------------

服务端按 ``--config`` > 环境变量 ``TAKLER_CONNECT_FILE`` 的顺序查找
，两者都没有则不带配置文件启动。两种来源的失败处理不同：

* ``--config`` 是运维显式给出的，读不到（不存在、不可读、 YAML 或
  字段校验失败）会让启动**直接失败**，错误必须被看到
* ``TAKLER_CONNECT_FILE`` 可能是环境里残留的过期取值，读不到时只记
  一条 WARNING 并忽略，照常启动

客户端（ ``takler-client-py`` 与 ``takler_client`` ）没有 ``--config``
选项，只认 ``TAKLER_CONNECT_FILE`` ；文件读不到时按客户端的失败契约
处理（ stderr 一行 ``FileNotFoundError: ...`` ，退出码 ``3`` ，见
:doc:`/guide/cli` ）。
TUI (``takler-tui``) 只把该文件用于解析地址，不读取 ``security``
段，见 :doc:`/guide/tui` 。

文件结构
--------

最小文件只含 ``server`` 段； ``checkpoint`` 与 ``security`` 两段
整体可省略，段内每一项也可单独省略，省略即「未配置」：

.. code-block:: yaml

    server:
      address:
        hostname: login01
        ip: 10.0.0.9
        port: "33083"

server 段
---------

.. list-table::
    :header-rows: 1
    :widths: 26 14 60

    * - 字段
      - 默认值
      - 说明
    * - ``server.address.hostname``
      - 无（必填）
      - 服务主机名。服务端与客户端都以此加 ``port`` 为连接地址；
        服务端同时把它宣告给作业脚本（ ``TAKLER_HOST`` ）
    * - ``server.address.ip``
      - 无（必填）
      - 服务 IP 。仅作记录， **不参与** 连接地址解析——两端读的都是
        ``hostname``
    * - ``server.address.port``
      - 无（必填）
      - 服务端口， YAML 中写作字符串
    * - ``server.http``
      - 未配置
      - HTTP 监听小节（ M3 ）。配置即在 gRPC 端口之外再挂一个 HTTP
        transport（需要 ``takler[http]`` extra ）；省略则服务保持
        gRPC-only
    * - ``server.http.host``
      - ``0.0.0.0``
      - HTTP 监听网卡
    * - ``server.http.port``
      - 无（ ``http`` 小节存在时必填）
      - HTTP 监听端口，独立于 gRPC 端口， YAML 中写作字符串
    * - ``server.http.tls_cert_file``
      - 回落到 gRPC 证书对
      - HTTP 监听自身的证书，与 ``tls_key_file`` 成对配置；两者都不配
        则继承 gRPC 监听解析出的证书对（命令行 ``--tls-cert`` /
        ``--tls-key`` > ``security`` 段），仍无则为明文——推荐由前置
        反向代理终止 TLS ，见 :doc:`/operation/deployment`
    * - ``server.http.tls_key_file``
      - 回落到 gRPC 证书对
      - 上述证书的私钥；只配置两者之一会终止启动

checkpoint 段
-------------

.. list-table::
    :header-rows: 1
    :widths: 26 14 60

    * - 字段
      - 默认值
      - 说明
    * - ``checkpoint.interval``
      - ``120``
      - 两次快照之间的秒数。小于 ``10`` 的取值被拒绝并回退到
        ``120``
    * - ``checkpoint.file``
      - ``takler.check``
      - 检查点文件路径；相对路径按服务进程的工作目录解析。备份文件
        为其加 ``.b`` 后缀

security 段
-----------

.. list-table::
    :header-rows: 1
    :widths: 30 14 56

    * - 字段
      - 默认值
      - 说明
    * - ``security.server_cert_file``
      - 未配置
      - 服务端证书，与 ``server_key_file`` 同时配置即启用 TLS
    * - ``security.server_key_file``
      - 未配置
      - 服务端私钥；只配置两者之一会终止启动
    * - ``security.client_ca_file``
      - 未配置
      - mTLS 预留扩展位，本版本不校验客户端证书
    * - ``security.ca_file``
      - 未配置
      - 客户端信任的 CA 证书
    * - ``security.server_name``
      - 不覆盖
      - 客户端校验证书主机名时使用的名称
    * - ``security.auth_mode``
      - ``disabled``
      - 鉴权模式， ``disabled`` 或 ``enabled``
    * - ``security.operator_secret_file``
      - 未配置
      - 运维共享密钥文件路径（服务端校验用，客户端也读它取密钥）
    * - ``security.operator_whitelist_file``
      - 未配置
      - 运维用户白名单文件路径
    * - ``security.zombie_policy``
      - ``fail``
      - zombie 处置策略， ``fail`` / ``fob`` / ``adopt``
    * - ``security.audit_file``
      - 未配置
      - 审计日志文件路径；未配置时审计记录写入常规日志目标

同一字段的服务端 / 客户端分工、文件格式与语义（密钥轮换、白名单匹配
规则、启动期校验等）见 :doc:`/operation/security` 。

取值优先级链
------------

通用规则：空串与纯空白一律视为「未提供」，逐级向下回落；枚举类取值
（ ``auth_mode`` 、 ``zombie_policy`` 、 ``exception_policy`` ）不区分
大小写并容忍 ``-`` 与 ``_`` 互换，无法识别的取值记一条 WARNING 后
回退到内置默认值，绝不因配置笔误让服务变得更脆弱。

**文件位置** ： ``--config`` > ``TAKLER_CONNECT_FILE`` > 无配置文件
（仅服务端有 ``--config`` ）。

**地址** （服务端）：

1. ``--host`` / ``--port``
2. ``connect.yaml`` 的 ``server.address.hostname`` / ``port``
3. 内置默认 ``localhost`` / ``33083``

**地址** （客户端）：与 :doc:`/guide/cli` 一致——``--host`` /
``--port`` > ``TAKLER_CONNECT_FILE`` > ``TAKLER_HOST`` /
``TAKLER_PORT`` > 内置默认。注意客户端的 connect 文件优先级**低于**
命令行选项但**高于** ``TAKLER_HOST`` / ``TAKLER_PORT`` 环境变量，而
服务端根本不读这两个环境变量。

**检查点** ： ``--checkpoint-file`` / ``--checkpoint-interval`` >
``connect.yaml`` 的 ``checkpoint`` 段 > 内置默认（ ``takler.check``
/ ``120`` 秒）。

**TLS 证书对** （服务端）： ``--tls-cert`` / ``--tls-key`` >
``connect.yaml`` 的 ``security.server_cert_file`` /
``server_key_file`` 。命令行是最高优先级，配置文件只补命令行未给的
部分；证书与私钥必须成对出现，与来源无关。

**其余 security 字段** ：环境变量 > ``connect.yaml`` 的
``security`` 段 > 内置默认。服务端侧的 ``auth_mode`` 、
``zombie_policy`` 、 ``audit_file`` 没有命令行选项，对应环境变量
``TAKLER_AUTH_MODE`` 、 ``TAKLER_ZOMBIE_POLICY`` 、
``TAKLER_AUDIT_FILE`` ；客户端侧的 ``ca_file`` 、 ``server_name`` 、
``operator_secret_file`` 对应 ``TAKLER_TLS_CA_FILE`` 、
``TAKLER_TLS_SERVER_NAME`` 、 ``TAKLER_SECRET_FILE`` 。

**异常策略** ： ``--exception-policy`` >
``TAKLER_EXCEPTION_POLICY`` > ``resilient`` 。它不属于
``connect.yaml`` ——配置文件的 ``checkpoint`` 段之外，服务端运行
参数里只有它不进入配置文件。

共用同一份配置的典型布局
------------------------

服务端与客户端读同一份文件的不同部分，因此一台登录节点上的常见布局
是：

.. code-block:: bash

    # 服务端（systemd 单元或启动脚本中）
    takler-server --config /home/oper/takler/connect.yaml

    # 运维账户的客户端环境
    export TAKLER_CONNECT_FILE=/home/oper/takler/connect.yaml
    takler-client-py show
    takler-tui

作业脚本不在此列：作业由服务在同一台机器上拉起，地址经
``TAKLER_HOST`` / ``TAKLER_PORT`` 注入，口令经 ``TAKLER_PASS``
注入，child 命令无需读 ``connect.yaml`` 。
