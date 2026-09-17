协议与跨语言契约
================

本页面向客户端实现者： Go 客户端 ``takler_client`` 是参照实现，任何新
语言的客户端都应能与它对齐。协议的唯一权威来源是
``src/takler/server/protocol/takler.proto`` ，本页解释字段背后的语义与
两边的常量契约，不逐字段抄 proto 定义。服务端如何把 RPC 接进节点树见
:doc:`architecture` 与 :doc:`core-design` 。

服务与方法
----------

包 ``takler_protocol`` 里只有一个服务 ``TaklerServer`` ，共 16 个 RPC，
全部是 unary-unary （一次请求一次应答，没有流式调用）：

.. list-table::
    :header-rows: 1
    :widths: 30 26 44

    * - 方法
      - 请求 → 应答
      - 用途
    * - ``RunCommandInit``
      - ``InitCommand`` → ``ServiceResponse``
      - 作业启动上报； ``task_id`` 写入 ``TAKLER_RID``
    * - ``RunCommandComplete``
      - ``CompleteCommand`` → ``ServiceResponse``
      - 作业正常结束
    * - ``RunCommandAbort``
      - ``AbortCommand`` → ``ServiceResponse``
      - 作业失败； ``reason`` 记入 ``aborted_reason``
    * - ``RunCommandEvent``
      - ``EventCommand`` → ``ServiceResponse``
      - 置位事件（只能 set ，清除见下方 ForceCommand ）
    * - ``RunCommandMeter``
      - ``MeterCommand`` → ``ServiceResponse``
      - 更新标尺； ``meter_value`` 线上是 **字符串**
    * - ``RunCommandRequeue``
      - ``RequeueCommand`` → ``ServiceResponse``
      - 重排队
    * - ``RunCommandSuspend`` / ``RunCommandResume``
      - ``SuspendCommand`` → ``ServiceResponse``
      - 挂起 / 恢复（两个方法共用同一消息类型）
    * - ``RunCommandRun``
      - ``RunCommand`` → ``ServiceResponse``
      - 手动提交任务； ``force`` 跳过状态检查
    * - ``RunCommandForce``
      - ``ForceCommand`` → ``ServiceResponse``
      - 强制置节点状态或置 / 清事件
    * - ``RunCommandFreeDep``
      - ``FreeDepCommand`` → ``ServiceResponse``
      - 手动释放依赖
    * - ``RunCommandLoad``
      - ``LoadCommand`` → ``ServiceResponse``
      - 载入 flow 定义
    * - ``RunCommandBegin``
      - ``BeginCommand`` → ``ServiceResponse``
      - 启动日历
    * - ``RunRequestShow``
      - ``ShowRequest`` → ``ShowResponse``
      - 查询节点树
    * - ``RunRequestPing``
      - ``PingRequest`` → ``PingResponse``
      - 健康检查（唯一免凭据的方法）
    * - ``QueryCoroutine``
      - ``CoroutineRequest`` → ``CoroutineResponse``
      - 列出服务端事件循环上的协程

请求消息里需要实现者注意的语义：

* 五个 child 命令共用 ``ChildCommandOptions`` 消息里的
  ``node_path`` 定位目标节点。
  ``event`` 只能置位；清除事件走 ``RunCommandForce`` ，把 ``path`` 写成
  ``节点路径:事件名`` 、 ``state`` 用 ``clear`` （ ``set`` 亦可）。
* ``MeterCommand.meter_value`` 在 proto 里是 ``string`` ，服务端用
  ``int()`` 转换后再校验取值范围；一个非整数的字符串会以内部错误收场
  （ ``int()`` 抛出的 ``ValueError`` 不是 takler 异常，分类为
  ``internal_error`` ）。
* ``ForceCommand.path`` 接受节点路径与 ``节点:事件`` 两种形式；
  ``recursive`` 只对节点有意义（语义见 :doc:`/guide/node-status` 的
  sink 一节）。
* ``LoadCommand.flow`` 是 ``bytes`` ，内容为
  :py:class:`~takler.core.Flow` 的 ``to_dict`` JSON ； ``flow_type``
  目前只接受 ``"json"`` ，其他取值与坏 JSON 分别以
  ``unsupported_value`` / ``invalid_request`` 拒绝。载入按 Tree 模式
  恢复：状态归零、未 begun （见 :doc:`core-design` 的序列化一节）。
* ``BeginCommand.flow_name`` 为空串表示对 Bunch 里全部 flow 执行
  begin —— 空串就是线上形式，不是省略字段。
* ``ShowResponse.output`` 是服务端排版好的文本，客户端直接打印，不再
  解析。

``ServiceResponse`` 与 error_code
---------------------------------

除三个查询方法外，所有 RPC 都返回 ``ServiceResponse{flag, message}`` ：

* ``flag`` 是错误分类码（ Error_Code ）： ``0`` 表示成功，非 ``0`` 表
  示失败并标识分类。 ``message`` 形如 ``{异常类名}: {异常消息}`` 。
* **字段形状是冻结的** ： ``flag`` 保持 ``int32`` 、字段号 ``1`` ，
  ``message`` 保持 ``string`` 、字段号 ``2`` ，不允许增删字段。分类
  码是复用既有 ``flag`` 字段而不是新增字段引入的，只判断
  ``flag != 0`` 的老客户端因此永远兼容。
  ``tests/server/test_proto_contract.py`` 同时钉住 descriptor 的形状与
  proto 注释里指向分类码表的说明。
* 分类码表在 ``protocol/error_code.py`` （ M3 任务 4 起从
  ``server/protocol/`` 迁入传输中立的 ``takler/protocol`` 包），不是
  proto 枚举 —— 这个模块只依赖 ``takler.exceptions`` ，客户端不用拉入
  protobuf 生成代码或服务端包就能完成映射。完整的十六码表（码值、分类名、
  异常类、客户端退出码）
  见 :doc:`/operation/reference` ，这里只说查找规则：按 **确切异常类
  型** 查表，不沿继承链；没有专属码的 takler 异常归 ``1``
  （ ``takler_error`` ），非 takler 异常归 ``99`` （
  ``internal_error`` ）；客户端遇到表中不存在的码，分类名读作
  ``unknown`` ，退出码取最保守的 ``3`` 。
* 分类名按码值分组： ``0`` 是 ``success`` ； ``10`` ~ ``15`` 是请
  求与节点树（ ``node_not_found`` 、 ``invalid_node_path`` 、
  ``node_type`` 、 ``unsupported_value`` 、 ``flow_state`` 、
  ``invalid_request`` ）； ``20`` 是 ``expression_syntax`` ；
  ``30`` / ``31`` 是 ``job_submission`` 与 ``zombie`` ； ``40`` ~
  ``43`` 是传输与鉴权（ ``transport`` 、 ``client_connection`` 、
  ``server_response`` 、 ``permission_denied`` ）—— 其中 ``40`` /
  ``41`` / ``42`` 只由客户端本地抛出，永远不会出现在服务端返回的
  ``flag`` 里； ``1`` 与 ``99`` 分别是 ``takler_error`` 与
  ``internal_error`` 两个兜底。
* 三个查询方法没有 ``flag`` 字段，错误复用各自的应答类型： ``show``
  把错误写进 ``output`` （ ``error: {类型}: {消息}`` ）， ``ping`` 与
  ``coroutine`` 返回空应答。鉴权拒绝不走应答体，见下节。

凭据 metadata 与权限分级
------------------------

凭据走 gRPC metadata ，一共三个键，全小写、都不带 ``-bin`` 后缀（值是
ASCII 文本， ``-bin`` 会把值声明为二进制）：

.. list-table::
    :header-rows: 1
    :widths: 22 78

    * - 键
      - 语义
    * - ``takler-pass``
      - child 命令携带的一次性作业口令（ ``TAKLER_PASS`` ）。拦截器只检
        查它在场，值与目标任务的口令是否一致由 ``ZombieDetector`` 的
        Z1 判定（见 :doc:`/operation/zombie` ）—— 缺失是「没有凭据」，
        不匹配是「另一个作业实例的凭据」，后者可能按僵尸策略放行。
    * - ``takler-secret``
      - 运维命令携带的共享口令，与 ``Operator_Secret_Set`` 做常数时间比
        对（见 :doc:`/operation/security` ）。
    * - ``takler-user``
      - 运维命令携带的 OS 用户名，须命中白名单；同时进入审计记录。

服务端解析规则： 空白值视为未携带；同一键重复出现时 **第一个出现生效**
（ HTTP/2 允许同名头重复，取首个是确定性的、且不受追加方影响）。键名
在 ``auth.py`` 里是三个常量（ ``METADATA_KEY_JOB_PASSWORD`` 等），客户
端应复用同样的拼写。

每个方法按 ``PRIVILEGE_BY_METHOD`` 归入三级之一，表以全限定方法名
（ ``/takler_protocol.TaklerServer/<方法名>`` ）为键：

* ``CHILD`` —— 五个 child 命令；
* ``OPERATOR`` —— 八个控制命令，外加 ``RunRequestShow`` 与
  ``QueryCoroutine`` 两个只读方法（它们返回整份流程定义，侦察价值与写
  操作相当）；
* ``PUBLIC`` —— 仅 ``RunRequestPing`` 。

**表里没有的方法一律按 ``OPERATOR`` 处理** —— 故意 fail-closed ，新增
RPC 忘了分类的结果是要凭据，而不是一个匿名的写入口。
``tests/server/test_privilege_table_property.py`` 遍历 descriptor 断言
每个已声明方法都有显式条目。 ``auth_mode`` 为 ``disabled`` 时不做任何
检查，但凭据仍被解析并发布给僵尸判定与审计。

鉴权拒绝的应答也是契约的一部分，分类字符串三个：
``missing_credential`` 以 gRPC 状态码 ``UNAUTHENTICATED`` 拒绝（你没带
凭据）， ``invalid_credential`` 与 ``not_in_whitelist`` 以
``PERMISSION_DENIED`` 拒绝（带了但不成立）。拒绝细节里只有方法名与分
类，不含任何凭据值，也不区分「密钥错了」与「服务端文件读不出」。

枚举： ``ForceState`` 与 ``DepType``
------------------------------------

``ForceCommand.ForceState`` 的八个取值：
``unknown=0`` 、 ``complete=1`` 、 ``queued=2`` 、 ``submitted=3`` 、
``active=4`` 、 ``aborted=5`` 、 ``clear=6`` 、 ``set=7`` 。注意两点：
它的编号 **与** core 的 :py:class:`~takler.core.NodeStatus` 枚举值
（ ``unknown=1`` 到 ``aborted=6`` ） **不一致** ，两套编号不能互转；
末尾的 ``clear`` / ``set`` 不是节点状态，是事件的清除与置位（配合
``节点:事件`` 路径使用）。

``FreeDepCommand.DepType`` 的三个取值： ``all=0`` 、 ``trigger=1`` 、
``time=2`` 。 proto3 下缺省即零值 ``all`` 。

Python 客户端按枚举 **名字符串** 转换（
``takler_pb2.ForceCommand.ForceState.Value(state)`` ），所以线上的枚举
名就是命令行接受的字符串，如 ``complete`` 、 ``set`` 。

超时与重试契约
--------------

以下常量同属跨语言契约，两个客户端必须一致：

* 单次尝试超时 ``10`` 秒；
* 重试窗口： child 命令 ``86400`` 秒（一天，作业可以比服务活得久），
  控制与查询命令 ``60`` 秒；环境变量 ``TAKLER_TIMEOUT`` 覆盖窗口（见
  :doc:`/operation/reference` ）；
* 退避公式 ``min(2**(n-1), 60)`` 秒， ``n`` 为第几次尝试；
* 可重试的 gRPC 状态码： ``UNAVAILABLE`` 、 ``DEADLINE_EXCEEDED`` 、
  ``RESOURCE_EXHAUSTED`` 、 ``UNKNOWN`` ；不可重试：
  ``INVALID_ARGUMENT`` 、 ``NOT_FOUND`` 、 ``PERMISSION_DENIED`` 、
  ``UNAUTHENTICATED`` （请求本身错了，重试不会改变结果）；
* **业务失败不重试** ： ``flag != 0`` 的应答是正常返回，不是传输故
  障，客户端直接把它交给调用方，既不重试也不抛异常。

重新生成 stub
-------------

Python stub （ ``takler_pb2.py`` 与 ``takler_pb2_grpc.py`` ）在仓库根
目录用 dev 依赖组里的 ``grpcio-tools`` 重新生成：

.. code-block:: console

    $ python -m grpc_tools.protoc -Isrc --python_out=src --grpc_python_out=src \
        takler/server/protocol/takler.proto

``grpcio-tools`` 固定在 ``>=1.83`` 不是随手写的：更低的版本把
protobuf 上限钉在 ``<7`` ，与本项目自己的 ``protobuf>=7.35`` 冲突，生
成物会不可复现。生成的 ``*_pb2*.py`` 被 ruff 与覆盖率同时排除，不要手
工编辑。

Go 侧的生成命令写在 ``takler.proto`` 头部注释里（ ``protoc --go_out``
系列），产物落在 ``takler-client`` 仓库的 ``takler_protocol/`` 目录；
proto 的 ``go_package`` 选项已指向该仓库。

跨语言契约测试
--------------

常量契约靠一对「漂移哨兵」测试维持： Python 半是
``tests/client/test_cross_language_contract.py`` ， Go 半是
``takler-client`` 仓库的 ``common/errorcode_test.go`` 。两边各自 **手
抄** 同一份期望值 —— error_code 十六行、退出码映射、重试常量、状态码
集合 —— 刻意不从生产代码的表里推导：读着自己要监督的表的测试永远发
现不了那张表被改错。任何一侧改了常量而另一侧没跟上，失败的是测试而
不是线上协议。

另有两个相关哨兵： ``tests/server/test_proto_contract.py`` 钉住
``ServiceResponse`` 的字段形状与注释；
``tests/server/test_privilege_table_property.py`` 断言权限表覆盖
descriptor 里的全部方法。

新增一个命令要同步改的地方
--------------------------

以新增一个控制命令为例，清单按依赖顺序：

#. ``server/protocol/takler.proto`` ：定义请求消息与 rpc 。
   ``ServiceResponse`` 的两个字段保持不动。
#. 重新生成 Python stub （上节命令）与 Go stub （ ``takler-client`` 仓
   库）。
#. ``server/auth.py`` 的 ``PRIVILEGE_BY_METHOD`` 加一条显式分类。不加
   也会安全地回落到 ``OPERATOR`` ，但完备性测试会失败。
#. ``server/network_service.py`` 的 ``TaklerService`` 加处理器，并把核
   心调用包进 ``_handle_command`` （见 :doc:`architecture` 的 RPC 主
   线）。 ``OPERATOR`` 级的写操作从被分类起就自动进审计；只读方法要
   列进 ``_READ_ONLY_OPERATOR_METHODS`` 才不会被记审计。
#. ``server/scheduler.py`` 加对应的 ``run_command_*`` 。
#. ``client/service_client.py`` 加客户端方法， ``client/cli.py`` 加命
   令； Go 侧对应改 ``common/client_*.go`` 与 ``cmd/cmd_*.go`` 。
#. 只有引入了新的异常类型才动 ``protocol/error_code.py`` ，并
   同步 Go 侧 ``common/errorcode.go`` 与两边的契约测试。
#. 文档： :doc:`/guide/cli` 的命令表，动了 error_code 或环境变量时连
   带 :doc:`/operation/reference` 。

协议本身的错误形态（退出码、stderr 契约、重试表现）以
:doc:`/guide/cli` 与 :doc:`/operation/reference` 为准。
