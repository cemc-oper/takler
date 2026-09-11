审计日志
========

审计日志回答的问题是「谁在什么时候改了什么」。服务上有三个记录点，
各自为一次事件写出恰好一条审计记录：

* **运维命令执行结束** ：八个控制命令（ ``requeue`` 、 ``suspend`` 、
  ``resume`` 、 ``run`` 、 ``force`` 、 ``free_dep`` 、 ``load`` 、
  ``begin`` ）每处理完一个请求写一条
* **鉴权拒绝** ：鉴权拦截器每拒绝一个请求写一条，见
  :doc:`/operation/security`
* **zombie 处置** ：每次 zombie 处置写一条，无论生效的是哪种策略，见
  :doc:`/operation/zombie`

只读的 ``show`` 与 ``coroutine`` 不产生审计记录——TUI 会持续轮询它们，
记录的话每次刷新就是一条； ``ping`` 与 child 命令也不产生（ child 命令
被判定为 zombie 时由 zombie 记录点单独写一条）。

记录格式
--------

每条记录是一行 JSON 对象（JSON Lines），含八个键，键的顺序固定：

.. code-block:: json

    {"timestamp": "2026-07-15T10:30:00.123456", "event": "control", "command": "requeue", "user": "oper", "peer": "ipv4:10.0.0.9:51234", "target": ["/flow1/group1/task1"], "outcome": "success", "error_code": 0}

.. list-table::
    :header-rows: 1
    :widths: 18 82

    * - 键
      - 含义
    * - ``timestamp``
      - 本地时间的 ISO 8601 字符串，不带时区后缀，与服务常规日志和
        运维的 shell 历史对照时不需要换算时区
    * - ``event``
      - 记录点： ``control`` 、 ``denied`` 、 ``zombie``
    * - ``command``
      - 命令的短名，从 RPC 方法名推导：
        ``RunCommandFreeDep`` → ``free_dep`` ，与运维实际输入的子命令一致
    * - ``user``
      - 请求携带的 ``takler-user`` 凭据；未携带时为 ``unknown``
    * - ``peer``
      - 调用方的网络地址，形如 ``ipv4:10.0.0.9:51234`` ；读不到地址时
        为 ``unknown`` （进程内调用、单元测试等未经 RPC 栈的场景）
    * - ``target``
      - 命令作用的节点路径或 flow 名列表；没有得到目标的请求（例如
        在解析请求之前就被拒绝）为空列表
    * - ``outcome``
      - ``success`` 、 ``error`` 、 ``denied`` 、 ``zombie``
    * - ``error_code``
      - 该请求返回的 ``flag`` 取值，与客户端看到的一致： ``flag=0``
        即 ``success`` ，非零即 ``error`` 。鉴权拒绝走不到命令处理，
        没有 ``flag`` 可复制，固定为 ``43``
        （ ``permission_denied`` ）

一行一条记录的保证
------------------

审计文件按行读取，因此一条记录**永远只占一行**，无论节点路径里有什么
字符： JSON 转义处理 ASCII 控制字符，三个 Unicode 换行符
（ ``U+0085`` 、 ``U+2028`` 、 ``U+2029`` ）也会被显式转义；
非 ASCII 字符（如中文节点路径）不转义，保持可读。把任何一行喂给
``json.loads`` 都能还原出完整的八键对象。

记录中**不含口令取值与共享密钥取值** ——八个键里没有可以放凭据的字段，
各记录点也不会把凭据塞进 ``target`` 或 ``command`` 。

写入去向
--------

配置了 ``audit_file`` （ ``connect.yaml`` 的 ``security`` 段，见
:doc:`/operation/security` ）或环境变量 ``TAKLER_AUDIT_FILE`` 时，
审计记录**只**写入该文件，不进入 ``TAKLER_LOG_FILE`` 配置的常规日志
文件与控制台——隔离是双向的，其他组件的记录也不会混进审计文件。
未配置时审计记录写入常规日志目标（控制台与常规日志文件），以常规的
「时间戳 级别 组件 消息」格式出现，组件名为 ``audit`` 。

取值优先级为环境变量 ``TAKLER_AUDIT_FILE`` > ``connect.yaml`` 的
``security.audit_file`` > 不启用审计文件，空串与纯空白视为未配置；
完整的字段参考见 :doc:`/operation/connect-config` 。审计 sink 由日志
子系统实现，分流机制见 :doc:`/operation/logging` 。

文件权限与创建
--------------

审计文件由服务在写出第一条记录时创建：父目录不存在时自动创建，文件
权限为仅所有者可读写 (``0600``) 。文件先以 ``0600`` **创建** 、再交给
日志后端追加，避免后端按进程 umask 创建后再收紧权限、中间留下一个
所有用户可读的窗口——审计记录里有执行命令的用户名与来源地址，不适合
共享登录节点上的每个账户可读。已经存在的审计文件保持其当前权限，服务
不会改写运维有意放置的文件的权限位。

审计文件每行是完整的 JSON ，没有时间戳前缀，可以直接交给 ``jq`` 处理：

.. code-block:: bash

    # 今天被拒绝的运维命令，以及是谁发起的
    jq -r 'select(.event == "denied") | [.timestamp, .user, .command, .peer] | @tsv' audit.jsonl

    # zombie 处置记录：fob 策略下客户端看到的是成功，这里是唯一的痕迹
    jq -r 'select(.event == "zombie") | [.timestamp, .command, .target[0], .error_code] | @tsv' audit.jsonl

    # 一个节点的全部被操作历史
    jq -r 'select(.target | index("/flow1/group1/task1")) | [.timestamp, .user, .command, .outcome] | @tsv' audit.jsonl

写失败的降级
------------

写审计记录失败时（磁盘满、权限被改、目录被删），服务向常规日志记一条
含路径与原因的 WARNING （组件名 ``server.audit`` ），**请求的响应不受
任何影响**：审计是观测手段，不是可用性单点。审计磁盘写满的服务照常
处理命令，只是暂时无法证明自己处理过什么。
