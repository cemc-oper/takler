故障排查
========

本页按**症状**组织，每条给出最可能的原因、定位命令与参考页链接。
通用的第一步永远是看日志：服务日志（控制台或 ``TAKLER_LOG_FILE``
，级别由 ``TAKLER_LOG_LEVEL`` 控制）与审计文件（见
:doc:`/operation/logging` 与 :doc:`/operation/audit` ）。

连不上服务（退出码 4 ）
-------------------------

命令在重试窗口内反复重试（默认 control / query 命令 ``60`` 秒，
child 命令 ``86400`` 秒），最终 stderr 一行 ``ClientConnectionError:
server ... is unreachable ...`` ，退出码 ``4`` 。按顺序检查：

#. **服务在不在。** 服务进程是否存活、监听端口是否是预期的
   ``33083`` （ ``ss -tlnp | grep 33083`` ）；服务日志里应有启动
   记录。
#. **地址对不对。** 客户端地址解析链为 ``--host`` / ``--port`` >
   ``TAKLER_CONNECT_FILE`` 指向的 ``connect.yaml`` >
   ``TAKLER_HOST`` / ``TAKLER_PORT`` > ``localhost:33083`` ——
   环境里残留的 ``TAKLER_CONNECT_FILE`` 指向旧文件是常见原因，用
   ``echo $TAKLER_CONNECT_FILE`` 确认。参考 :doc:`/guide/cli`
   与 :doc:`/operation/connect-config` 。
#. **是不是 TLS 握手失败。** 服务端启用了 TLS 而客户端没有配置
   ``TAKLER_TLS_CA_FILE`` （或反之）时，握手失败在客户端表现
   为 gRPC ``UNAVAILABLE`` ——**与「服务不在线」无法区分**，会
   一直重试到窗口耗尽。两边 TLS 配置要成对，见下文「 TLS 握手
   失败」。

可以用 ``TAKLER_TIMEOUT=5`` 缩短重试窗口让失败更快暴露（ ``0``
表示只试一次）。

ping 通但其他命令全被拒绝（退出码 1 ）
--------------------------------------

``ping`` 是唯一免鉴权的命令，它能通只说明网络与 TLS 正常。其他命令
报 ``PermissionDeniedError: ... UNAUTHENTICATED: ... refused:
missing_credential`` 或 ``PERMISSION_DENIED: ... refused:
invalid_credential`` / ``not_in_whitelist`` 时，问题在凭据：

* ``missing_credential`` ：服务端 ``auth_mode: enabled`` 而客户端
  没带上 ``takler-secret`` ——检查 ``TAKLER_SECRET_FILE`` 是否设置、
  指向的文件是否可读。客户端读不到密钥文件时**不会报错**，只记一条
  WARNING 后不带密钥发请求，所以先看客户端日志里有没有
  ``Cannot read Operator_Secret_File ...`` 。
* ``invalid_credential`` ：密钥对不上——客户端与服务端读的
  ``operator_secret_file`` 内容不一致（常见于密钥轮换只改了一端，
  见 :doc:`/operation/security` 的轮换一节）。
* ``not_in_whitelist`` ：密钥正确但用户不在
  ``operator_whitelist_file`` 里。审计文件（ ``event`` 为
  ``denied`` 的记录）能查到被拒绝的用户与来源地址，见
  :doc:`/operation/audit` 。

配置细节见 :doc:`/operation/security` 。

任务卡在 queued
---------------

``queued`` 表示依赖未满足。任务只在以下检查全部通过时才会提交：
未 suspended 、时间依赖已到、 trigger 表达式为真（含事件、标尺、
变量条件）、限额令牌可用，且状态不是 ``aborted`` （ aborted 的任务
**不会** 自动重跑）。

定位： ``takler-client-py show --node-path /flow/task --show-all``
（或在 TUI 的属性 / 参数页，见 :doc:`/guide/tui` ）看 trigger
表达式、时间依赖、事件 / 标尺当前值与限额占用；对照
:doc:`/guide/trigger-expression` 检查表达式引用的节点路径与状态。确认依赖确实该放行却仍卡住时，运维手段
是 ``free_dep`` （按类型解除依赖）或 ``run --force`` （跳过检查
直接提交），见 :doc:`/guide/cli` 。

注意 takler 没有 date / day / cron 依赖（见
:doc:`/guide/ecflow-differences` ），排查时不用考虑日历日期条件。

任务卡在 submitted
------------------

``submitted`` 表示作业命令已启动、但 ``init`` 上报尚未到达。
**服务对这一转移没有任何超时或看门狗** ，任务可以无限期停在这里。
渲染失败与进程创建失败不会让任务停在 submitted ——它们直接使任务
``aborted`` ，所以卡在 submitted 说明作业命令本身退出码为 ``0``
却没有拉起作业：

#. **看作业输出文件。** 默认路径 ``{TAKLER_HOME}{节点路径}.{try_no}``
   （如 ``t1.1`` ），作业文件为 ``{TAKLER_HOME}{节点路径}.job{try_no}``
   。文件不存在或为空 → 作业命令根本没执行作业文件。
#. **检查 ``TAKLER_SHELL_JOB_CMD`` 。** 自定义的作业命令如果把默认
   模板包了一层且吞掉了真实退出码 / 没有真正执行 ``{{TAKLER_JOB}}``
   ，命令「成功」返回而作业从未运行，见
   :doc:`/guide/job-management` 的警告。
#. **检查执行权限。** 服务只给作业文件加**所有者**执行位，其余权限
   位来自服务进程的 umask ；以其他账户提交时可能无权执行。
#. **作业跑了但 ``init`` 没回来** → 见下一条。

作业在跑但状态不动
------------------

作业进程活着（输出文件在增长），服务侧状态却停在 ``submitted`` /
``active`` ——child 上报没有到达或被拒绝。定位：

#. **直接在作业输出里找 child 命令的报错。** child 命令连接失败会
   重试最长一天后退出码 ``4`` ；被拒绝则退出码非零并打印一行原因。
#. **地址是渲染时写死的。** 作业文件里的 ``TAKLER_HOST`` /
   ``TAKLER_PORT`` 来自服务启动时宣告的地址；服务重启换了地址后，
   旧作业的上报继续发往旧地址。恢复时地址不一致会在日志里留下分级
   记录，见 :doc:`/operation/checkpoint` 。
#. **口令不匹配即 zombie 。** 启用鉴权时 ``TAKLER_PASS`` 缺失或
   不匹配、未启用鉴权时上报到一个不在 submitted / active 状态的
   任务、 ``init`` 携带的 task id 不一致，分别命中 zombie 判定
   Z1 / Z2 / Z3 ——服务日志有一条 WARNING ，审计文件有一条
   ``event`` 为 ``zombie`` 的记录。判定条件与处置见
   :doc:`/operation/zombie` 。

zombie 记录刷日志
-----------------

服务日志反复出现 zombie WARNING 、审计文件里 ``event`` 为
``zombie`` 的记录持续增长，几乎总是**同一批旧作业在重试**： child
命令的默认重试窗口长达一天，被 ``fail`` 策略拒绝的上报（退出码
``3`` ）不会让作业停下。典型场景是 requeue 之后旧作业仍在运行并
继续上报，见 :doc:`/operation/zombie` 的「典型场景」一节。

处置：先按审计记录里的 ``target`` 与 ``peer`` 找到旧作业并确认它
该不该活着（该杀就杀）；确认上报无害后可以临时把
``security.zombie_policy`` 调为 ``fob`` （静默收下丢弃）或
``adopt`` （收下并采用新口令）止血，但这两种策略都会掩盖真实的
状态漂移，用完调回 ``fail`` 。三种策略的语义见
:doc:`/operation/zombie` 。

快照恢复失败
------------

服务启动后节点树是空的或少了某些 flow 。恢复的全部结论都在启动日志
里： ``failed to parse checkpoint file ...`` 表示某级文件不可用并
已回退， ``could not restore from the checkpoint file ... nor from
the backup file`` 表示两级都失败、以空 bunch 启动， ``failed to
restore flow ...`` 表示单个 flow 被跳过（常见于自定义 Task 子类不
满足恢复要求）。逐条对照 :doc:`/operation/checkpoint` 的「恢复失败
时怎么查」一节。

TLS 握手失败
------------

**客户端侧** 的表现分两种：

* CA 证书文件本身不可用（不存在、为空、不是合法 CA ）：报错发生在
  **请求发出之前** ， stderr 一行 ``InvalidRequestError: cannot read
  TLS CA certificate file ...`` ，退出码 ``1`` ——这是配置错误，
  修文件即可。
* 握手在连接时失败（证书不受信任、主机名不匹配）：表现与「连不上
  服务」完全相同——重试到窗口耗尽，退出码 ``4`` 。主机名不匹配
  时用 ``TAKLER_TLS_SERVER_NAME`` 指定证书上的名字（例如证书签给
  短主机名而客户端按全名连接）。

**服务端侧** 的配置错误在启动时暴露：证书与私钥只配置了一个、文件
不可读、证书与私钥不匹配，都会打印一行原因并以退出码 ``1`` 拒绝
启动，不会降级为明文。完整校验规则见 :doc:`/operation/security`
的 TLS 一节。

作业脚本渲染失败
----------------

症状是任务直接变成 ``aborted`` （不是卡在 queued / submitted ），
服务日志有一条 ``job_submission`` 相关的 ERROR 。渲染失败的三类
原因：脚本文件（ ``TAKLER_SCRIPT`` ）缺失、 ``{% include %}``
在搜索路径（脚本所在目录 + ``TAKLER_INCLUDE`` 列出的目录）里找
不到、模板语法错误。注意**引用未定义的变量不算渲染失败**——
Jinja2 默认渲染为空字符串， ``check_job_creation`` 也检查不出来，
作业会带着空参数跑起来。渲染模型与预防手段见
:doc:`/guide/task-script` ，提交失败到 aborted 的完整路径见
:doc:`/guide/job-management` 。
